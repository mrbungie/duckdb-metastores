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
#include "metastore_utils.hpp"
#include "metastore_scan_plan.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
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

	mutable std::unordered_map<string, idx_t> file_to_part_idx;
	mutable idx_t filename_underlying_idx = METASTORE_INVALID_INDEX;
	mutable idx_t underlying_col_count = 0;

	MetastoreReadBindData(std::string catalog_p, std::string schema_p, std::string table_name_p)
	    : catalog(std::move(catalog_p)), schema(std::move(schema_p)), table_name(std::move(table_name_p)),
	      is_partitioned(false), needs_planning(false) {
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<MetastoreReadBindData>();
		return catalog == other.catalog && schema == other.schema && table_name == other.table_name;
	}

	duckdb::unique_ptr<FunctionData> Copy() const override {
		auto copy = duckdb::make_uniq<MetastoreReadBindData>(catalog, schema, table_name);
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
		copy->file_to_part_idx = file_to_part_idx;
		copy->filename_underlying_idx = filename_underlying_idx;
		copy->underlying_col_count = underlying_col_count;
		return std::move(copy);
	}
};

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

static void PrunePartitionsLocally(ClientContext &context, const MetastoreTable &table, const vector<string> &names,
                                   const vector<duckdb::unique_ptr<Expression>> &filters, MetastoreScanPlan &plan) {
	if (plan.partitions.empty()) {
		return;
	}

	// 1. Prepare types and chunk
	vector<LogicalType> types;
	vector<idx_t> partition_col_indices;
	for (auto &part_col : table.partition_spec.columns) {
		types.push_back(TransformStringToLogicalType(MetastoreUtils::MapHiveTypeToDuckDB(part_col.type)));
		for (idx_t i = 0; i < names.size(); i++) {
			if (names[i] == part_col.name) {
				partition_col_indices.push_back(i);
				break;
			}
		}
	}
	if (types.empty()) {
		return;
	}

	DataChunk chunk;
	chunk.Initialize(context, types, plan.partitions.size());
	for (idx_t p_idx = 0; p_idx < plan.partitions.size(); p_idx++) {
		auto &part = plan.partitions[p_idx];
		for (idx_t c_idx = 0; c_idx < types.size(); c_idx++) {
			if (c_idx < part.values.size()) {
				chunk.data[c_idx].SetValue(p_idx, Value(part.values[c_idx]).CastAs(context, types[c_idx]));
			}
		}
	}
	chunk.SetCardinality(plan.partitions.size());

	// 2. Evaluate filters
	SelectionVector sel(plan.partitions.size());
	idx_t count = plan.partitions.size();
	for (auto &filter : filters) {
		if (TouchesPartitionColumns(*filter, table, names)) {
			ExpressionExecutor executor(context, *filter);
			count = executor.SelectExpression(chunk, sel);
			if (count == 0) {
				break;
			}
			// Update chunk for next filter if needed? SelectExpression returns a new selection vector.
			// For multiple filters, we should probably combine them or use a temporary selection.
			// But DuckDB SelectExpression typically applies on top of existing selection?
			// Actually, executor.SelectExpression(chunk, sel) returns the count and updates sel.
		}
	}

	if (count < plan.partitions.size()) {
		vector<MetastorePartitionValue> filtered_partitions;
		vector<string> filtered_files;
		for (idx_t i = 0; i < count; i++) {
			idx_t idx = sel.get_index(i);
			filtered_partitions.push_back(std::move(plan.partitions[idx]));
			// We need to re-resolve files for these partitions too?
			// To keep it simple, we'll just re-resolve or assume plan.files can be filtered if we knew which file came
			// from which partition. Since PlanScan already built plan.files, it's easier to just re-build file list
			// here from locations.
			auto &reader = GetFormatReader(table.storage_descriptor.format);
			auto scan_path = reader.BuildScanPath(filtered_partitions.back().location);
			if (!scan_path.empty()) {
				filtered_files.push_back(std::move(scan_path));
			}
		}
		plan.partitions = std::move(filtered_partitions);
		plan.files = std::move(filtered_files);
		plan.selected_partitions = GetPartitionNames(table, plan.partitions);
	}
}

//! Ensure all partition columns are present in bind_data.names/return_types.
static void EnsurePartitionColumnsInSchema(const MetastoreReadBindData &bind_data) {
	for (auto &pcol : bind_data.table.partition_spec.columns) {
		bool found = false;
		for (auto &name : bind_data.names) {
			if (name == pcol.name) {
				found = true;
				break;
			}
		}
		if (!found) {
			bind_data.names.push_back(pcol.name);
			bind_data.return_types.push_back(
			    TransformStringToLogicalType(MetastoreUtils::MapHiveTypeToDuckDB(pcol.type)));
		}
	}
}

static void BindUnderlyingFunction(ClientContext &context, const MetastoreReadBindData &bind_data) {
	auto &reader = GetFormatReader(bind_data.table.storage_descriptor.format);
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
	if (bind_data.scan_files.empty()) {
		file_list.push_back(Value(reader.BuildScanPath(bind_data.table.storage_descriptor.location)));
	} else {
		for (auto &file : bind_data.scan_files) {
			file_list.push_back(Value(file));
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

	// If partitioned, we MUST have the filename to resolve partition values
	bool has_filename = false;
	for (const auto &name : bind_data.names) {
		if (name == "filename") {
			has_filename = true;
			break;
		}
	}
	if (bind_data.is_partitioned && !has_filename) {
		bind_data.names.push_back("filename");
		bind_data.return_types.push_back(LogicalType::VARCHAR);
	}

	try {
		bind_data.underlying_bind_data =
		    bind_data.underlying_function.bind(context, bind_input, bind_data.return_types, bind_data.names);
		bind_data.underlying_col_count = bind_data.names.size();

		for (idx_t i = 0; i < bind_data.names.size(); i++) {
			if (bind_data.names[i] == "filename") {
				bind_data.filename_underlying_idx = i;
				break;
			}
		}

		EnsurePartitionColumnsInSchema(bind_data);
	} catch (std::exception &e) {
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

duckdb::unique_ptr<FunctionData> MetastoreReadBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() < 3) {
		throw BinderException("metastore_read requires at least 3 arguments: catalog, schema, table_name");
	}

	auto catalog = input.inputs[0].GetValue<std::string>();
	auto schema = input.inputs[1].GetValue<std::string>();
	auto table_name = input.inputs[2].GetValue<std::string>();

	auto bind_data = duckdb::make_uniq<MetastoreReadBindData>(catalog, schema, table_name);

	bind_data->connector = CreateConnector(catalog);
	if (schema != bind_data->connector->GetNamespace()) {
		throw BinderException(
		    "Metastore connector for catalog '%s' is bound to namespace '%s', but requested schema is '%s'", catalog,
		    bind_data->connector->GetNamespace(), schema);
	}

	auto table_result = bind_data->connector->GetTable(table_name);
	if (!table_result.IsOk()) {
		throw BinderException("Failed to get table metadata for %s.%s: %s", schema, table_name,
		                      table_result.error.message);
	}
	bind_data->table = table_result.value;
	bind_data->is_partitioned = bind_data->table.IsPartitioned();

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

	if (predicate.empty()) {
		// No metastore pushdown possible, see if we need local prune
		bool touches = false;
		for (auto &filter : filters) {
			if (TouchesPartitionColumns(*filter, bind_data.table, bind_data.names)) {
				touches = true;
				break;
			}
		}
		if (!touches && !bind_data.needs_planning) {
			// No partition filters and already planned, nothing to do
			return;
		}
	}

	if (bind_data.last_predicate == predicate && !bind_data.scan_files.empty()) {
		// Already planned with this predicate
		return;
	}

	auto plan = PlanScan(context, *bind_data.connector, bind_data.table, opt);

	if (predicate.empty()) {
		PrunePartitionsLocally(context, bind_data.table, bind_data.names, filters, plan);
	}

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

	if (bind_data.underlying_function.pushdown_complex_filter) {
		bind_data.underlying_function.pushdown_complex_filter(context, get, bind_data.underlying_bind_data.get(),
		                                                      filters);
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
			gstate->output_to_underlying_idx.push_back(underlying_column_ids.size());
			gstate->output_to_partition_idx.push_back(METASTORE_INVALID_INDEX);
			underlying_column_ids.push_back(col_id);
		}
	}

	if (bind_data.is_partitioned && bind_data.filename_underlying_idx != METASTORE_INVALID_INDEX) {
		gstate->filename_col_in_underlying_chunk = underlying_column_ids.size();
		underlying_column_ids.push_back(bind_data.filename_underlying_idx);
	}

	gstate->underlying_column_ids = underlying_column_ids;
	for (auto &id : underlying_column_ids) {
		gstate->underlying_types.push_back(bind_data.return_types[id]);
	}

	if (bind_data.underlying_function.init_global) {
		vector<idx_t> underlying_projection_ids;
		for (idx_t i = 0; i < underlying_column_ids.size(); i++) {
			underlying_projection_ids.push_back(i);
		}

		gstate->underlying_filters = duckdb::make_uniq<TableFilterSet>();
		if (input.filters) {
			for (auto &filter : input.filters->filters) {
				if (filter.first < data_col_count) {
					gstate->underlying_filters->filters[filter.first] = filter.second->Copy();
				}
			}
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
		lstate->underlying_filters = duckdb::make_uniq<TableFilterSet>();
		if (input.filters) {
			auto data_col_count = bind_data.table.storage_descriptor.columns.size();
			for (auto &filter : input.filters->filters) {
				if (filter.first < data_col_count) {
					lstate->underlying_filters->filters[filter.first] = filter.second->Copy();
				}
			}
		}

		vector<idx_t> underlying_projection_ids;
		for (idx_t i = 0; i < gstate.underlying_column_ids.size(); i++) {
			underlying_projection_ids.push_back(i);
		}

		TableFunctionInitInput underlying_input(bind_data.underlying_bind_data.get(), gstate.underlying_column_ids,
		                                        underlying_projection_ids, lstate->underlying_filters.get());
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
	func.filter_pushdown = true;
	func.pushdown_complex_filter = MetastoreReadPushdownComplexFilter;
	func.projection_pushdown = true;
	func.to_string = MetastoreReadToString;

	function_set.AddFunction(func);

	return function_set;
}

} // namespace duckdb
