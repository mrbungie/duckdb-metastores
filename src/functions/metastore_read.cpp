#include "metastore_functions.hpp"
#include "metastore_runtime.hpp"
#include "connector/metastore_connector.hpp"
#include "metastore_partition_predicate.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "metastore_utils.hpp"
#include "metastore_scan_plan.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "formats/format_reader.hpp"

namespace duckdb {

static constexpr const idx_t METASTORE_INVALID_INDEX = idx_t(-1);

struct MetastoreReadBindData : public TableFunctionData {
	std::string catalog;
	std::string schema;
	std::string table_name;
	MetastoreTable table;
	duckdb::unique_ptr<IMetastoreConnector> connector;

	// Partitioning and filters
	mutable vector<string> selected_partitions;
	mutable vector<string> scan_files;
	mutable vector<MetastorePartitionValue> partitions;
	bool is_partitioned;
	mutable bool needs_planning;
	mutable string last_predicate;

	// Wrapping the underlying scan (e.g. read_parquet)
	mutable duckdb::unique_ptr<FunctionData> underlying_bind_data;
	mutable TableFunction underlying_function;

	mutable vector<LogicalType> return_types;
	mutable vector<string> names;
	mutable vector<LogicalType> underlying_return_types;
	mutable vector<string> underlying_names;

	mutable std::unordered_map<string, idx_t> file_to_part_idx;
	mutable idx_t filename_underlying_idx = METASTORE_INVALID_INDEX;
	mutable idx_t underlying_col_count = 0;
	std::optional<MetastoreFormat> forced_format;

	MetastoreReadBindData(std::string catalog_p, std::string schema_p, std::string table_name_p,
	                     std::optional<MetastoreFormat> forced_format_p = std::nullopt)
	    : catalog(std::move(catalog_p)), schema(std::move(schema_p)), table_name(std::move(table_name_p)),
	      is_partitioned(false), needs_planning(false), forced_format(forced_format_p) {
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<MetastoreReadBindData>();
		return catalog == other.catalog && schema == other.schema && table_name == other.table_name &&
		       forced_format == other.forced_format;
	}

	duckdb::unique_ptr<FunctionData> Copy() const override {
		auto copy = duckdb::make_uniq<MetastoreReadBindData>(catalog, schema, table_name, forced_format);
		copy->table = table;

		copy->selected_partitions = selected_partitions;
		copy->scan_files = scan_files;
		copy->partitions = partitions;
		copy->is_partitioned = is_partitioned;
		copy->needs_planning = needs_planning;
		copy->last_predicate = last_predicate;
		copy->underlying_function = underlying_function;
		if (underlying_bind_data) {
			copy->underlying_bind_data = underlying_bind_data->Copy();
		}
		copy->return_types = return_types;
		copy->names = names;
		copy->underlying_return_types = underlying_return_types;
		copy->underlying_names = underlying_names;
		copy->file_to_part_idx = file_to_part_idx;
		copy->filename_underlying_idx = filename_underlying_idx;
		copy->underlying_col_count = underlying_col_count;
		copy->forced_format = forced_format;
		return std::move(copy);
	}
};

static const char *ScanFunctionName(MetastoreFormat format) {
	switch (format) {
	case MetastoreFormat::Parquet:
		return "metastore_parquet_scan";
	case MetastoreFormat::CSV:
		return "metastore_csv_scan";
	case MetastoreFormat::JSON:
		return "metastore_json_scan";
	default:
		return "metastore_read";
	}
}

// Helpers moved to metastore_scan_plan.cpp

static void AddNamedParameter(named_parameter_map_t &named_parameters, const std::string &name, const Value &value) {
	named_parameters[name] = value;
}

static idx_t GetMaxPartitions(ClientContext &context) {
	Value val;
	if (context.TryGetCurrentSetting("metastore_max_partitions", val)) {
		return val.GetValue<idx_t>();
	}
	return 100000;
}

static bool TouchesPartitionColumns(const Expression &expr, const MetastoreTable &table, const vector<string> &names) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &col_ref = expr.Cast<BoundColumnRefExpression>();
		if (col_ref.binding.column_index < names.size()) {
			const string &col_name = names[col_ref.binding.column_index];
			for (auto &part_col : table.partition_spec.columns) {
				if (part_col.name == col_name) {
					return true;
				}
			}
		}
	}
	bool touches = false;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (TouchesPartitionColumns(child, table, names)) {
			touches = true;
		}
	});
	return touches;
}

static bool IsPartitionColumnName(const MetastoreTable &table, const string &col_name) {
	for (auto &part_col : table.partition_spec.columns) {
		if (part_col.name == col_name) {
			return true;
		}
	}
	return false;
}

static bool GetNameIndex(const vector<string> &names, const string &name, idx_t &out_idx) {
	for (idx_t i = 0; i < names.size(); i++) {
		if (names[i] == name) {
			out_idx = i;
			return true;
		}
	}
	return false;
}

static duckdb::unique_ptr<TableFilterSet>
BuildUnderlyingFilterSet(const MetastoreReadBindData &bind_data, const vector<column_t> &input_column_ids,
                         const optional_ptr<TableFilterSet> input_filters, const vector<idx_t> &underlying_column_ids) {
	if (!input_filters) {
		return nullptr;
	}

	auto result = duckdb::make_uniq<TableFilterSet>();
	for (auto &entry : input_filters->filters) {
		auto output_filter_idx = entry.first;
		if (output_filter_idx >= input_column_ids.size()) {
			continue;
		}
		auto col_id = input_column_ids[output_filter_idx];
		if (col_id == METASTORE_INVALID_INDEX || col_id >= bind_data.names.size()) {
			continue;
		}

		const auto &col_name = bind_data.names[col_id];
		if (IsPartitionColumnName(bind_data.table, col_name)) {
			continue;
		}

		idx_t underlying_col_id = METASTORE_INVALID_INDEX;
		for (idx_t u = 0; u < bind_data.underlying_names.size(); u++) {
			if (bind_data.underlying_names[u] == col_name) {
				underlying_col_id = u;
				break;
			}
		}
		if (underlying_col_id == METASTORE_INVALID_INDEX) {
			continue;
		}

		idx_t projected_underlying_idx = METASTORE_INVALID_INDEX;
		for (idx_t i = 0; i < underlying_column_ids.size(); i++) {
			if (underlying_column_ids[i] == underlying_col_id) {
				projected_underlying_idx = i;
				break;
			}
		}
		if (projected_underlying_idx == METASTORE_INVALID_INDEX) {
			continue;
		}

		result->filters[projected_underlying_idx] = entry.second->Copy();
	}

	if (result->filters.empty()) {
		return nullptr;
	}
	return result;
}

static bool RewriteLocalPartitionFilter(unique_ptr<Expression> &expr, const LogicalGet &get,
	                                  const MetastoreReadBindData &bind_data, bool &has_partition_ref,
	                                  bool &only_partition_refs) {
	if (!expr) {
		return false;
	}

	if (expr->GetExpressionClass() == ExpressionClass::BOUND_REF) {
		auto idx = expr->Cast<BoundReferenceExpression>().index;
		idx_t col_id = METASTORE_INVALID_INDEX;
		if (idx < get.GetColumnIds().size()) {
			col_id = get.GetColumnIds()[idx].GetPrimaryIndex();
		} else if (idx < bind_data.names.size()) {
			col_id = idx;
		} else {
			return false;
		}
		if (col_id >= bind_data.names.size()) {
			return false;
		}
		const auto &name = bind_data.names[col_id];
		has_partition_ref = has_partition_ref || IsPartitionColumnName(bind_data.table, name);
		if (!IsPartitionColumnName(bind_data.table, name)) {
			only_partition_refs = false;
		}
		expr = make_uniq<BoundReferenceExpression>(bind_data.return_types[col_id], col_id);
	}

	if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &col_ref = expr->Cast<BoundColumnRefExpression>();
		if (col_ref.binding.table_index != get.table_index) {
			only_partition_refs = false;
			return true;
		}
		auto output_col_idx = col_ref.binding.column_index;
		if (output_col_idx >= get.GetColumnIds().size()) {
			return false;
		}
		auto col_id = get.GetColumnIds()[output_col_idx].GetPrimaryIndex();
		if (col_id >= bind_data.names.size()) {
			return false;
		}
		const auto &name = bind_data.names[col_id];
		has_partition_ref = has_partition_ref || IsPartitionColumnName(bind_data.table, name);
		if (!IsPartitionColumnName(bind_data.table, name)) {
			only_partition_refs = false;
		}
		expr = make_uniq<BoundReferenceExpression>(bind_data.return_types[col_id], col_id);
	}

	bool ok = true;
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
		if (!RewriteLocalPartitionFilter(child, get, bind_data, has_partition_ref, only_partition_refs)) {
			ok = false;
		}
	});
	return ok;
}

static void AppendLocalPartitionFilters(const Expression &expr, const LogicalGet &get,
	                                 const MetastoreReadBindData &bind_data,
	                                 vector<unique_ptr<Expression>> &out_filters) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
	    expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		for (auto &child : conjunction.children) {
			AppendLocalPartitionFilters(*child, get, bind_data, out_filters);
		}
		return;
	}

	auto local_filter = expr.Copy();
	bool has_partition_ref = false;
	bool only_partition_refs = true;
	if (!RewriteLocalPartitionFilter(local_filter, get, bind_data, has_partition_ref, only_partition_refs)) {
		return;
	}
	if (!has_partition_ref || !only_partition_refs) {
		return;
	}
	out_filters.push_back(std::move(local_filter));
}

static std::vector<std::string> GetPartitionNames(const MetastoreTable &table,
                                                  const std::vector<MetastorePartitionValue> &partitions) {
	std::vector<std::string> names;
	for (auto &part : partitions) {
		std::string name;
		for (idx_t i = 0; i < part.values.size() && i < table.partition_spec.columns.size(); i++) {
			if (!name.empty()) {
				name += "/";
			}
			name += table.partition_spec.columns[i].name + "=" + part.values[i];
		}
		if (!name.empty()) {
			names.push_back(std::move(name));
		}
	}
	sort(names.begin(), names.end());
	names.erase(unique(names.begin(), names.end()), names.end());
	return names;
}

static bool EvaluatePartitionValueFilter(const string &value, const TableFilter &filter) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &cmp = filter.Cast<ConstantFilter>();
		auto rhs = cmp.constant.ToString();
		switch (cmp.comparison_type) {
		case ExpressionType::COMPARE_EQUAL:
			return value == rhs;
		case ExpressionType::COMPARE_NOTEQUAL:
			return value != rhs;
		case ExpressionType::COMPARE_GREATERTHAN:
			return value > rhs;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			return value >= rhs;
		case ExpressionType::COMPARE_LESSTHAN:
			return value < rhs;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			return value <= rhs;
		default:
			return true;
		}
	}
	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		for (auto &candidate : in_filter.values) {
			if (value == candidate.ToString()) {
				return true;
			}
		}
		return false;
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &and_filter = filter.Cast<ConjunctionAndFilter>();
		for (auto &child : and_filter.child_filters) {
			if (!EvaluatePartitionValueFilter(value, *child)) {
				return false;
			}
		}
		return true;
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &or_filter = filter.Cast<ConjunctionOrFilter>();
		for (auto &child : or_filter.child_filters) {
			if (EvaluatePartitionValueFilter(value, *child)) {
				return true;
			}
		}
		return false;
	}
	case TableFilterType::IS_NULL:
		return StringUtil::Lower(value) == "null" || value.empty();
	case TableFilterType::IS_NOT_NULL:
		return !(StringUtil::Lower(value) == "null" || value.empty());
	default:
		return true;
	}
}

static void PrunePartitionsByTableFilters(const MetastoreTable &table, const TableFilterSet &filter_set,
                                          const vector<ColumnIndex> &column_ids, const vector<string> &names,
                                          MetastoreScanPlan &plan) {
	if (plan.partitions.empty()) {
		return;
	}

	vector<bool> keep(plan.partitions.size(), true);
	bool has_partition_filter = false;

	for (auto &entry : filter_set.filters) {
		auto col_id = entry.first;
		if (col_id >= names.size()) {
			continue;
		}
		const auto &col_name = names[col_id];

		idx_t part_idx = METASTORE_INVALID_INDEX;
		for (idx_t i = 0; i < table.partition_spec.columns.size(); i++) {
			if (table.partition_spec.columns[i].name == col_name) {
				part_idx = i;
				break;
			}
		}
		if (part_idx == METASTORE_INVALID_INDEX) {
			continue;
		}

		has_partition_filter = true;
		for (idx_t p = 0; p < plan.partitions.size(); p++) {
			if (!keep[p]) {
				continue;
			}
			if (part_idx >= plan.partitions[p].values.size()) {
				keep[p] = false;
				continue;
			}
			if (!EvaluatePartitionValueFilter(plan.partitions[p].values[part_idx], *entry.second)) {
				keep[p] = false;
			}
		}
	}

	if (!has_partition_filter) {
		return;
	}

	vector<idx_t> old_to_new(plan.partitions.size(), METASTORE_INVALID_INDEX);
	vector<MetastorePartitionValue> filtered_partitions;
	for (idx_t i = 0; i < plan.partitions.size(); i++) {
		if (!keep[i]) {
			continue;
		}
		old_to_new[i] = filtered_partitions.size();
		filtered_partitions.push_back(std::move(plan.partitions[i]));
	}

	vector<string> filtered_files;
	vector<idx_t> filtered_file_partition_indices;
	for (idx_t i = 0; i < plan.files.size(); i++) {
		auto old_idx = plan.file_partition_indices[i];
		if (old_idx >= old_to_new.size()) {
			continue;
		}
		auto new_idx = old_to_new[old_idx];
		if (new_idx == METASTORE_INVALID_INDEX) {
			continue;
		}
		filtered_files.push_back(plan.files[i]);
		filtered_file_partition_indices.push_back(new_idx);
	}

	plan.partitions = std::move(filtered_partitions);
	plan.files = std::move(filtered_files);
	plan.file_partition_indices = std::move(filtered_file_partition_indices);
	plan.selected_partitions = GetPartitionNames(table, plan.partitions);
}

static void PrunePartitionsByExpressions(ClientContext &context, const MetastoreReadBindData &bind_data,
	                                     const LogicalGet &get,
	                                     const vector<duckdb::unique_ptr<Expression>> &filters,
	                                     MetastoreScanPlan &plan) {
	if (plan.partitions.empty() || filters.empty()) {
		return;
	}

	vector<idx_t> partition_name_indices(bind_data.table.partition_spec.columns.size(), METASTORE_INVALID_INDEX);
	for (idx_t p = 0; p < bind_data.table.partition_spec.columns.size(); p++) {
		idx_t name_idx;
		if (GetNameIndex(bind_data.names, bind_data.table.partition_spec.columns[p].name, name_idx)) {
			partition_name_indices[p] = name_idx;
		}
	}

	DataChunk chunk;
	chunk.Initialize(context, bind_data.return_types, plan.partitions.size());
	for (idx_t p = 0; p < plan.partitions.size(); p++) {
		for (idx_t part_idx = 0; part_idx < bind_data.table.partition_spec.columns.size(); part_idx++) {
			auto name_idx = partition_name_indices[part_idx];
			if (name_idx == METASTORE_INVALID_INDEX || part_idx >= plan.partitions[p].values.size()) {
				continue;
			}
			try {
				chunk.data[name_idx].SetValue(
				    p, Value(plan.partitions[p].values[part_idx]).CastAs(context, bind_data.return_types[name_idx]));
			} catch (...) {
				FlatVector::SetNull(chunk.data[name_idx], p, true);
			}
		}
	}
	chunk.SetCardinality(plan.partitions.size());

	vector<bool> keep(plan.partitions.size(), true);
	vector<unique_ptr<Expression>> local_partition_filters;
	for (auto &filter : filters) {
		AppendLocalPartitionFilters(*filter, get, bind_data, local_partition_filters);
	}

	if (local_partition_filters.empty()) {
		return;
	}

	for (auto &local_filter : local_partition_filters) {
		SelectionVector sel(plan.partitions.size());
		ExpressionExecutor executor(context, *local_filter);
		auto count = executor.SelectExpression(chunk, sel);
		vector<bool> pass(plan.partitions.size(), false);
		for (idx_t i = 0; i < count; i++) {
			pass[sel.get_index(i)] = true;
		}
		for (idx_t i = 0; i < keep.size(); i++) {
			keep[i] = keep[i] && pass[i];
		}
	}

	vector<idx_t> old_to_new(plan.partitions.size(), METASTORE_INVALID_INDEX);
	vector<MetastorePartitionValue> filtered_partitions;
	for (idx_t i = 0; i < plan.partitions.size(); i++) {
		if (!keep[i]) {
			continue;
		}
		old_to_new[i] = filtered_partitions.size();
		filtered_partitions.push_back(std::move(plan.partitions[i]));
	}

	vector<string> filtered_files;
	vector<idx_t> filtered_file_partition_indices;
	for (idx_t i = 0; i < plan.files.size(); i++) {
		auto old_idx = plan.file_partition_indices[i];
		if (old_idx >= old_to_new.size()) {
			continue;
		}
		auto new_idx = old_to_new[old_idx];
		if (new_idx == METASTORE_INVALID_INDEX) {
			continue;
		}
		filtered_files.push_back(plan.files[i]);
		filtered_file_partition_indices.push_back(new_idx);
	}

	plan.partitions = std::move(filtered_partitions);
	plan.files = std::move(filtered_files);
	plan.file_partition_indices = std::move(filtered_file_partition_indices);
	plan.selected_partitions = GetPartitionNames(bind_data.table, plan.partitions);
}

//! Ensure all partition columns are present in bind_data.names/return_types.
static void EnsurePartitionColumnsInSchema(const MetastoreReadBindData &bind_data) {
	for (auto &pcol : bind_data.table.partition_spec.columns) {
		const auto ptype =
		    TransformStringToLogicalType(MetastoreUtils::MapHiveTypeToDuckDB(pcol.type));
		idx_t found_idx = METASTORE_INVALID_INDEX;
		for (idx_t i = 0; i < bind_data.names.size(); i++) {
			if (StringUtil::CIEquals(bind_data.names[i], pcol.name)) {
				found_idx = i;
				break;
			}
		}
		if (found_idx == METASTORE_INVALID_INDEX) {
			bind_data.names.push_back(pcol.name);
			bind_data.return_types.push_back(ptype);
		} else if (found_idx < bind_data.return_types.size()) {
			// Ensure partition columns always use the partition-spec type
			bind_data.return_types[found_idx] = ptype;
		}
	}
}


static void BindUnderlyingFunction(ClientContext &context, const MetastoreReadBindData &bind_data) {
	auto selected_format = bind_data.table.storage_descriptor.format;
	if (bind_data.forced_format.has_value()) {
		if (selected_format != bind_data.forced_format.value()) {
			throw BinderException("%s can only bind %s tables (got %s)",
			                      ScanFunctionName(bind_data.forced_format.value()),
			                      MetastoreFormatToString(bind_data.forced_format.value()),
			                      MetastoreFormatToString(selected_format));
		}
		selected_format = bind_data.forced_format.value();
	}

	auto &reader = GetFormatReader(selected_format);
	if (!reader.GetRequiredExtension().empty()) {
		Catalog::TryAutoLoad(context, reader.GetRequiredExtension());
	}
	auto scan_function_name = reader.GetScanFunctionName();

	auto &func_catalog = Catalog::GetEntry(context, CatalogType::TABLE_FUNCTION_ENTRY, SYSTEM_CATALOG, DEFAULT_SCHEMA,
	                                       scan_function_name)
	                         .Cast<TableFunctionCatalogEntry>();
	bind_data.underlying_function =
	    func_catalog.functions.GetFunctionByArguments(context, {LogicalType::LIST(LogicalType::VARCHAR)});

	vector<Value> file_list;
	auto &fs = FileSystem::GetFileSystem(context);
	auto is_ignored_file = [](const string &file_path) {
		auto slash_pos = file_path.find_last_of("/\\");
		auto file_name = slash_pos == string::npos ? file_path : file_path.substr(slash_pos + 1);
		return StringUtil::StartsWith(file_name, ".") || StringUtil::StartsWith(file_name, "_") ||
		       StringUtil::EndsWith(file_name, ".crc");
	};
	auto append_scan_target = [&](const string &raw_location) {
		auto scan_path = reader.BuildScanPath(raw_location);
		if (scan_path.empty()) {
			return;
		}
		if (FileSystem::HasGlob(scan_path)) {
			auto expanded = fs.GlobFiles(scan_path, context, FileGlobOptions::ALLOW_EMPTY);
			for (auto &file : expanded) {
				if (!is_ignored_file(file.path)) {
					file_list.push_back(Value(file.path));
				}
			}
			if (!expanded.empty()) {
				return;
			}
		}
		if (!is_ignored_file(scan_path)) {
			file_list.push_back(Value(scan_path));
		}
	};

	if (bind_data.scan_files.empty()) {
		append_scan_target(bind_data.table.storage_descriptor.location);
	} else {
		for (auto &file : bind_data.scan_files) {
			append_scan_target(file);
		}
	}

	auto named_parameters = reader.BuildNamedParameters(bind_data.table.storage_descriptor,
	                                                    bind_data.table.partition_spec, bind_data.is_partitioned);

	vector<LogicalType> input_table_types;
	vector<string> input_table_names;
	vector<Value> bind_inputs;
	bind_inputs.push_back(Value::LIST(LogicalType::VARCHAR, file_list));
	auto table_func_ref = duckdb::make_uniq<TableFunctionRef>();
	TableFunctionBindInput bind_input(bind_inputs, named_parameters, input_table_types, input_table_names, nullptr,
	                                  nullptr, bind_data.underlying_function, *table_func_ref);

	try {
		bind_data.underlying_return_types.clear();
		bind_data.underlying_names.clear();
		bind_data.underlying_bind_data = bind_data.underlying_function.bind(
		    context, bind_input, bind_data.underlying_return_types, bind_data.underlying_names);
		bind_data.underlying_col_count = bind_data.underlying_names.size();

		bind_data.names.clear();
		bind_data.return_types.clear();
		if (!bind_data.table.storage_descriptor.columns.empty()) {
			for (auto &col : bind_data.table.storage_descriptor.columns) {
				bind_data.names.push_back(col.name);
				bool matched = false;
				for (idx_t i = 0; i < bind_data.underlying_names.size(); i++) {
					if (bind_data.underlying_names[i] == col.name) {
						bind_data.return_types.push_back(bind_data.underlying_return_types[i]);
						matched = true;
						break;
					}
				}
				if (!matched) {
					bind_data.return_types.push_back(
					    TransformStringToLogicalType(MetastoreUtils::MapHiveTypeToDuckDB(col.type)));
				}
			}
		} else {
			for (idx_t i = 0; i < bind_data.underlying_names.size(); i++) {
				auto &col_name = bind_data.underlying_names[i];
				if (col_name == "filename") {
					continue;
				}
				bool is_partition_col = false;
				for (auto &part_col : bind_data.table.partition_spec.columns) {
					if (part_col.name == col_name) {
						is_partition_col = true;
						break;
					}
				}
				if (is_partition_col) {
					continue;
				}
				bind_data.names.push_back(col_name);
				bind_data.return_types.push_back(bind_data.underlying_return_types[i]);
			}
		}

		for (idx_t i = 0; i < bind_data.underlying_names.size(); i++) {
			if (bind_data.underlying_names[i] == "filename") {
				bind_data.filename_underlying_idx = i;
				break;
			}
		}

		EnsurePartitionColumnsInSchema(bind_data);
	} catch (std::exception &e) {
		bind_data.names.clear();
		bind_data.return_types.clear();
		if (bind_data.scan_files.empty() && bind_data.is_partitioned) {
			for (auto &col : bind_data.table.storage_descriptor.columns) {
				bind_data.names.push_back(col.name);
				bind_data.return_types.push_back(
				    TransformStringToLogicalType(MetastoreUtils::MapHiveTypeToDuckDB(col.type)));
			}
			for (auto &col : bind_data.table.partition_spec.columns) {
				bind_data.names.push_back(col.name);
				bind_data.return_types.push_back(
				    TransformStringToLogicalType(MetastoreUtils::MapHiveTypeToDuckDB(col.type)));
			}
		} else {
			throw;
		}
	}
}

static duckdb::unique_ptr<FunctionData> MetastoreReadBindInternal(ClientContext &context, TableFunctionBindInput &input,
                                                                  vector<LogicalType> &return_types,
                                                                  vector<string> &names,
                                                                  std::optional<MetastoreFormat> forced_format,
                                                                  const string &function_name) {
	if (input.inputs.size() < 3) {
		throw BinderException("%s requires at least 3 arguments: catalog, schema, table_name", function_name);
	}

	auto catalog = input.inputs[0].GetValue<std::string>();
	auto schema = input.inputs[1].GetValue<std::string>();
	auto table_name = input.inputs[2].GetValue<std::string>();

	auto bind_data = duckdb::make_uniq<MetastoreReadBindData>(catalog, schema, table_name, forced_format);

	bind_data->connector = CreateConnector(catalog, schema);

	auto table_result = bind_data->connector->GetTable(table_name);
	if (!table_result.IsOk()) {
		throw BinderException("Failed to get table metadata for %s.%s: %s", schema, table_name,
		                      table_result.error.message);
	}
	bind_data->table = table_result.value;
	bind_data->is_partitioned = bind_data->table.IsPartitioned();
	for (auto &col : bind_data->table.storage_descriptor.columns) {
		bind_data->names.push_back(col.name);
		bind_data->return_types.push_back(
		    TransformStringToLogicalType(MetastoreUtils::MapHiveTypeToDuckDB(col.type)));
	}
	EnsurePartitionColumnsInSchema(*bind_data);

	MetastorePlanOptions opt;
	opt.table_name = table_name;
	opt.predicate = "";
	opt.max_partitions = GetMaxPartitions(context);
	opt.allow_expand_paths = true;

	auto plan = PlanScan(context, *bind_data->connector, bind_data->table, opt);
	bind_data->scan_files = std::move(plan.files);
	bind_data->selected_partitions = std::move(plan.selected_partitions);
	bind_data->partitions = std::move(plan.partitions);
	auto &fs = FileSystem::GetFileSystem(context);
	for (idx_t i = 0; i < bind_data->scan_files.size(); i++) {
		auto norm_path = MetastoreUtils::NormalizeLocation(fs.ExpandPath(bind_data->scan_files[i]));
		bind_data->file_to_part_idx[norm_path] = plan.file_partition_indices[i];
	}
	bind_data->needs_planning = bind_data->is_partitioned;

	BindUnderlyingFunction(context, *bind_data);
	return_types = bind_data->return_types;
	names = bind_data->names;

	return std::move(bind_data);
}

duckdb::unique_ptr<FunctionData> MetastoreReadBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	return MetastoreReadBindInternal(context, input, return_types, names, std::nullopt,
	                                 "metastore_read");
}

duckdb::unique_ptr<FunctionData> MetastoreParquetScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types, vector<string> &names) {
	return MetastoreReadBindInternal(context, input, return_types, names, MetastoreFormat::Parquet,
	                                 "metastore_parquet_scan");
}

duckdb::unique_ptr<FunctionData> MetastoreCsvScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	return MetastoreReadBindInternal(context, input, return_types, names, MetastoreFormat::CSV, "metastore_csv_scan");
}

duckdb::unique_ptr<FunctionData> MetastoreJsonScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	return MetastoreReadBindInternal(context, input, return_types, names, MetastoreFormat::JSON,
	                                 "metastore_json_scan");
}

void MetastoreReadPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                        vector<duckdb::unique_ptr<Expression>> &filters) {
	auto &bind_data = bind_data_p->Cast<MetastoreReadBindData>();
	if (!bind_data.is_partitioned) {
		return;
	}

	FilterCombiner combiner(context);
	for (auto &filter : filters) {
		combiner.AddFilter(filter->Copy());
	}
	vector<FilterPushdownResult> pushdown_results;
	TableFilterSet filter_set = combiner.GenerateTableScanFilters(get.GetColumnIds(), pushdown_results);
	std::string predicate =
	    MetastorePartitionPredicate::FromTableFilters(bind_data.table, filter_set, get.GetColumnIds(), bind_data.names);

	MetastorePlanOptions opt;
	opt.table_name = bind_data.table_name;
	opt.predicate = predicate;
	opt.max_partitions = GetMaxPartitions(context);
	opt.allow_expand_paths = true;

	if (!predicate.empty() && bind_data.last_predicate == predicate && !bind_data.scan_files.empty()) {
		// Already planned with this predicate
		return;
	}

	auto plan = PlanScan(context, *bind_data.connector, bind_data.table, opt);
	PrunePartitionsByTableFilters(bind_data.table, filter_set, get.GetColumnIds(), bind_data.names, plan);
	PrunePartitionsByExpressions(context, bind_data, get, filters, plan);

	bool changed = (plan.files != bind_data.scan_files);
	bind_data.scan_files = std::move(plan.files);
	bind_data.partitions = std::move(plan.partitions);
	bind_data.selected_partitions = std::move(plan.selected_partitions);

	auto &fs = FileSystem::GetFileSystem(context);
	bind_data.file_to_part_idx.clear();
	for (idx_t i = 0; i < bind_data.scan_files.size(); i++) {
		auto norm_path = MetastoreUtils::NormalizeLocation(fs.ExpandPath(bind_data.scan_files[i]));
		bind_data.file_to_part_idx[norm_path] = plan.file_partition_indices[i];
	}

	bind_data.last_predicate = predicate;
	bind_data.needs_planning = false;

	if (changed) {
		BindUnderlyingFunction(context, bind_data);
	}

}

struct MetastoreReadGlobalState : public GlobalTableFunctionState {
	duckdb::unique_ptr<GlobalTableFunctionState> underlying_state;
	vector<idx_t> output_to_underlying_idx;
	vector<idx_t> output_to_partition_idx;
	vector<LogicalType> underlying_types;
	vector<idx_t> underlying_column_ids;
	idx_t filename_col_in_underlying_chunk;
	duckdb::unique_ptr<TableFilterSet> underlying_filters;
};

//! Ensure scan has been planned; called from InitGlobal when pushdown hasn't fired.
static void EnsureScanPlanned(ClientContext &context, const MetastoreReadBindData &bind_data) {
	if (!bind_data.is_partitioned || !bind_data.needs_planning || !bind_data.scan_files.empty()) {
		return;
	}

	MetastorePlanOptions opt;
	opt.table_name = bind_data.table_name;
	opt.predicate = "";
	opt.max_partitions = GetMaxPartitions(context);
	opt.allow_expand_paths = true;

	auto plan = PlanScan(context, *bind_data.connector, bind_data.table, opt);
	bind_data.scan_files = std::move(plan.files);
	bind_data.selected_partitions = std::move(plan.selected_partitions);
	bind_data.partitions = std::move(plan.partitions);
	bind_data.needs_planning = false;

	auto &fs = FileSystem::GetFileSystem(context);
	for (idx_t i = 0; i < bind_data.scan_files.size(); i++) {
		auto norm_path = MetastoreUtils::NormalizeLocation(fs.ExpandPath(bind_data.scan_files[i]));
		bind_data.file_to_part_idx[norm_path] = plan.file_partition_indices[i];
	}

	BindUnderlyingFunction(context, bind_data);
}

duckdb::unique_ptr<GlobalTableFunctionState> MetastoreReadInitGlobal(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<MetastoreReadBindData>();

	EnsureScanPlanned(context, bind_data);

	auto gstate = duckdb::make_uniq<MetastoreReadGlobalState>();
	gstate->filename_col_in_underlying_chunk = METASTORE_INVALID_INDEX;

	auto data_col_count = bind_data.table.storage_descriptor.columns.size();
	auto part_cols_count = bind_data.table.partition_spec.columns.size();

	vector<idx_t> underlying_column_ids;
	for (idx_t i = 0; i < input.column_ids.size(); i++) {
		auto col_id = input.column_ids[i];
		const string &col_name = (col_id == METASTORE_INVALID_INDEX) ? "filename" : bind_data.names[col_id];

		bool is_partition_col = false;
		idx_t p_idx = 0;
		for (idx_t j = 0; j < bind_data.table.partition_spec.columns.size(); j++) {
			if (bind_data.table.partition_spec.columns[j].name == col_name) {
				is_partition_col = true;
				p_idx = j;
				break;
			}
		}

		if (is_partition_col) {
			gstate->output_to_underlying_idx.push_back(METASTORE_INVALID_INDEX);
			gstate->output_to_partition_idx.push_back(p_idx);
		} else {
			idx_t u_id = METASTORE_INVALID_INDEX;
			for (idx_t u = 0; u < bind_data.underlying_names.size(); u++) {
				if (bind_data.underlying_names[u] == col_name) {
					u_id = u;
					break;
				}
			}
			if (u_id != METASTORE_INVALID_INDEX) {
				gstate->output_to_underlying_idx.push_back(underlying_column_ids.size());
				gstate->output_to_partition_idx.push_back(METASTORE_INVALID_INDEX);
				underlying_column_ids.push_back(u_id);
			} else {
				gstate->output_to_underlying_idx.push_back(METASTORE_INVALID_INDEX);
				gstate->output_to_partition_idx.push_back(METASTORE_INVALID_INDEX);
			}
		}
	}

	if (bind_data.is_partitioned && bind_data.filename_underlying_idx != METASTORE_INVALID_INDEX) {
		gstate->filename_col_in_underlying_chunk = underlying_column_ids.size();
		underlying_column_ids.push_back(bind_data.filename_underlying_idx);
	}

	if (input.filters) {
		for (auto &filter : input.filters->filters) {
			if (filter.first >= input.column_ids.size()) {
				continue;
			}
			auto col_id = input.column_ids[filter.first];
			if (col_id == METASTORE_INVALID_INDEX || col_id >= bind_data.names.size()) {
				continue;
			}
			const auto &col_name = bind_data.names[col_id];
			if (IsPartitionColumnName(bind_data.table, col_name)) {
				continue;
			}
			for (idx_t u = 0; u < bind_data.underlying_names.size(); u++) {
				if (bind_data.underlying_names[u] != col_name) {
					continue;
				}
				bool already_projected = false;
				for (auto existing : underlying_column_ids) {
					if (existing == u) {
						already_projected = true;
						break;
					}
				}
				if (!already_projected) {
					underlying_column_ids.push_back(u);
				}
				break;
			}
		}
	}

	gstate->underlying_column_ids = underlying_column_ids;
	for (auto &id : underlying_column_ids) {
		gstate->underlying_types.push_back(bind_data.underlying_return_types[id]);
	}
	gstate->underlying_filters =
	    BuildUnderlyingFilterSet(bind_data, input.column_ids, input.filters, gstate->underlying_column_ids);

	if (bind_data.underlying_function.init_global) {
		vector<idx_t> underlying_projection_ids;
		for (idx_t i = 0; i < underlying_column_ids.size(); i++) {
			underlying_projection_ids.push_back(i);
		}

		TableFunctionInitInput underlying_input(bind_data.underlying_bind_data.get(), underlying_column_ids,
		                                        underlying_projection_ids, gstate->underlying_filters.get());
		gstate->underlying_state = bind_data.underlying_function.init_global(context, underlying_input);
	}
	return std::move(gstate);
}

struct MetastoreReadLocalState : public LocalTableFunctionState {
	duckdb::unique_ptr<LocalTableFunctionState> underlying_state;
	DataChunk underlying_chunk;
	duckdb::unique_ptr<TableFilterSet> underlying_filters;
};

duckdb::unique_ptr<LocalTableFunctionState> MetastoreReadInitLocal(ExecutionContext &context,
                                                                   TableFunctionInitInput &input,
                                                                   GlobalTableFunctionState *global_state) {
	auto &bind_data = input.bind_data->Cast<MetastoreReadBindData>();
	auto &gstate = global_state->Cast<MetastoreReadGlobalState>();
	auto lstate = duckdb::make_uniq<MetastoreReadLocalState>();
	lstate->underlying_chunk.Initialize(context.client, gstate.underlying_types);
	if (bind_data.underlying_function.init_local) {
		vector<idx_t> underlying_projection_ids;
		for (idx_t i = 0; i < gstate.underlying_column_ids.size(); i++) {
			underlying_projection_ids.push_back(i);
		}

		TableFunctionInitInput underlying_input(bind_data.underlying_bind_data.get(), gstate.underlying_column_ids,
		                                        underlying_projection_ids, gstate.underlying_filters.get());
		lstate->underlying_state =
		    bind_data.underlying_function.init_local(context, underlying_input, gstate.underlying_state.get());
	}
	return std::move(lstate);
}

//! Resolve a partition column value for a single row by looking up the filename
//! in the file→partition index map (exact match first, fuzzy fallback).
static Value ResolvePartitionValue(const MetastoreReadBindData &bind_data, FileSystem &fs, const string &raw_filename,
                                   idx_t p_idx) {
	auto fname = MetastoreUtils::NormalizeLocation(fs.ExpandPath(raw_filename));

	// 1. Try exact match
	auto it = bind_data.file_to_part_idx.find(fname);

	// 2. Try matching the directory part if the stored path is a directory or glob
	if (it == bind_data.file_to_part_idx.end()) {
		for (auto &entry : bind_data.file_to_part_idx) {
			auto &candidate = entry.first;
			string base = candidate;
			// Strip trailing glob if present
			if (StringUtil::EndsWith(base, "*")) {
				base = base.substr(0, base.size() - 1);
			}
			if (StringUtil::EndsWith(base, "/")) {
				base = base.substr(0, base.size() - 1);
			}

			// If the actual file is inside this directory, we have a match
			if (StringUtil::StartsWith(fname, base)) {
				it = bind_data.file_to_part_idx.find(candidate);
				break;
			}
		}
	}

	// 3. Fallback to fuzzy match (least reliable)
	if (it == bind_data.file_to_part_idx.end()) {
		for (auto &entry : bind_data.file_to_part_idx) {
			if (StringUtil::EndsWith(fname, entry.first) || StringUtil::EndsWith(entry.first, fname)) {
				it = bind_data.file_to_part_idx.find(entry.first);
				break;
			}
		}
	}

	if (it != bind_data.file_to_part_idx.end() && it->second < bind_data.partitions.size()) {
		auto &p_val = bind_data.partitions[it->second];
		if (p_idx < p_val.values.size()) {
			return Value(p_val.values[p_idx]);
		}
	}
	return Value();
}

void MetastoreReadExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<MetastoreReadBindData>();
	auto &gstate = data.global_state->Cast<MetastoreReadGlobalState>();
	auto &lstate = data.local_state->Cast<MetastoreReadLocalState>();

	if (bind_data.scan_files.empty()) {
		output.SetCardinality(0);
		return;
	}

	TableFunctionInput underlying_input(bind_data.underlying_bind_data.get(), lstate.underlying_state.get(),
	                                    gstate.underlying_state.get());
	bind_data.underlying_function.function(context, underlying_input, lstate.underlying_chunk);

	auto N = lstate.underlying_chunk.size();
	output.SetCardinality(N);

	for (idx_t i = 0; i < output.ColumnCount(); i++) {
		auto u_idx = gstate.output_to_underlying_idx[i];
		if (u_idx != METASTORE_INVALID_INDEX) {
			if (u_idx < lstate.underlying_chunk.ColumnCount()) {
				output.data[i].Reference(lstate.underlying_chunk.data[u_idx]);
			} else {
				FlatVector::Validity(output.data[i]).SetAllInvalid(N);
			}
		} else {
			auto p_idx = gstate.output_to_partition_idx[i];
			if (p_idx != METASTORE_INVALID_INDEX &&
			    gstate.filename_col_in_underlying_chunk != METASTORE_INVALID_INDEX &&
			    gstate.filename_col_in_underlying_chunk < lstate.underlying_chunk.ColumnCount()) {
				auto &filename_col = lstate.underlying_chunk.data[gstate.filename_col_in_underlying_chunk];
				auto &dest = output.data[i];
				auto &fs = FileSystem::GetFileSystem(context);

				for (idx_t r = 0; r < N; r++) {
					auto val = ResolvePartitionValue(bind_data, fs, filename_col.GetValue(r).ToString(), p_idx);
					if (val.IsNull()) {
						FlatVector::SetNull(dest, r, true);
					} else {
						dest.SetValue(r, val);
					}
				}
			} else {
				FlatVector::Validity(output.data[i]).SetAllInvalid(N);
			}
		}
	}
}

InsertionOrderPreservingMap<std::string> MetastoreReadToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<std::string> result;
	auto &bind_data = input.bind_data->Cast<MetastoreReadBindData>();
	result["Metastore"] = bind_data.catalog;
	result["Table"] = bind_data.table_name;
	result["Underlying Scan"] = bind_data.underlying_function.name;
	if (bind_data.is_partitioned) {
		result["Partitions Selected"] = std::to_string(bind_data.selected_partitions.size());
	}
	return result;
}

TableFunctionSet MetastoreFunctions::GetMetastoreReadFunction() {
	TableFunctionSet function_set("metastore_read");

	// TODO: Fix state management (i.e. look at
	// https://github.com/duckdb/duckdb-iceberg/blob/main/src/iceberg_functions/iceberg_table_properties_functions.cpp)
	auto func = TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, MetastoreReadExecute,
	                          MetastoreReadBind, MetastoreReadInitGlobal, MetastoreReadInitLocal);
	func.filter_pushdown = false;
	func.pushdown_complex_filter = MetastoreReadPushdownComplexFilter;
	func.projection_pushdown = true;
	func.to_string = MetastoreReadToString;

	function_set.AddFunction(func);

	return function_set;
}

TableFunctionSet MetastoreFunctions::GetMetastoreParquetScanFunction() {
	TableFunctionSet function_set("metastore_parquet_scan");
	auto func = TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, MetastoreReadExecute,
	                          MetastoreParquetScanBind, MetastoreReadInitGlobal, MetastoreReadInitLocal);
	func.filter_pushdown = false;
	func.pushdown_complex_filter = MetastoreReadPushdownComplexFilter;
	func.projection_pushdown = true;
	func.to_string = MetastoreReadToString;
	function_set.AddFunction(func);
	return function_set;
}

TableFunctionSet MetastoreFunctions::GetMetastoreCsvScanFunction() {
	TableFunctionSet function_set("metastore_csv_scan");
	auto func = TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, MetastoreReadExecute,
	                          MetastoreCsvScanBind, MetastoreReadInitGlobal, MetastoreReadInitLocal);
	func.filter_pushdown = false;
	func.pushdown_complex_filter = MetastoreReadPushdownComplexFilter;
	func.projection_pushdown = true;
	func.to_string = MetastoreReadToString;
	function_set.AddFunction(func);
	return function_set;
}

TableFunctionSet MetastoreFunctions::GetMetastoreJsonScanFunction() {
	TableFunctionSet function_set("metastore_json_scan");
	auto func = TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, MetastoreReadExecute,
	                          MetastoreJsonScanBind, MetastoreReadInitGlobal, MetastoreReadInitLocal);
	func.filter_pushdown = false;
	func.pushdown_complex_filter = MetastoreReadPushdownComplexFilter;
	func.projection_pushdown = true;
	func.to_string = MetastoreReadToString;
	function_set.AddFunction(func);
	return function_set;
}

} // namespace duckdb
