#include "duckdb/table_functions/metastore_functions.hpp"
#include "runtime/metastore_runtime.hpp"
#include "core/connector/metastore_connector.hpp"
#include "core/planner/metastore_planner.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/metastore_utils.hpp"
#include <filesystem>
#include <algorithm>

namespace duckdb {

struct MetastoreReadBindData : public TableFunctionData {
	std::string catalog;
	std::string schema;
	std::string table_name;
	MetastoreTable table;
	duckdb::unique_ptr<IMetastoreConnector> connector;

	// Partitioning and filters
	vector<MetastorePartitionPredicate> partition_predicates;
	vector<string> selected_partitions;
	vector<string> scan_files;
	bool is_partitioned;

	// Wrapping the underlying scan (e.g. read_parquet)
	duckdb::unique_ptr<FunctionData> underlying_bind_data;
	TableFunction underlying_function;

	vector<LogicalType> return_types;
	vector<string> names;

	MetastoreReadBindData(std::string catalog_p, std::string schema_p, std::string table_name_p)
	    : catalog(std::move(catalog_p)), schema(std::move(schema_p)), table_name(std::move(table_name_p)),
	      is_partitioned(false) {
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<MetastoreReadBindData>();
		return catalog == other.catalog && schema == other.schema && table_name == other.table_name;
	}

	duckdb::unique_ptr<FunctionData> Copy() const override {
		auto copy = duckdb::make_uniq<MetastoreReadBindData>(catalog, schema, table_name);
		copy->table = table;
		copy->partition_predicates = partition_predicates;
		copy->selected_partitions = selected_partitions;
		copy->scan_files = scan_files;
		copy->is_partitioned = is_partitioned;
		if (underlying_bind_data) {
			copy->underlying_bind_data = underlying_bind_data->Copy();
		}
		copy->underlying_function = underlying_function;
		copy->return_types = return_types;
		copy->names = names;
		return std::move(copy);
	}
};

static std::vector<std::string> ResolveScanFiles(const std::vector<std::string> &scan_files) {
	std::vector<std::string> resolved;
	for (auto &scan_file : scan_files) {
		if (StringUtil::Contains(scan_file, "[!._]*")) {
			auto marker = scan_file.find("[!._]*");
			auto base = scan_file.substr(0, marker);
			std::error_code ec;
			if (std::filesystem::exists(base, ec)) {
				for (auto &entry : std::filesystem::directory_iterator(base, ec)) {
					if (ec || !entry.is_regular_file()) {
						continue;
					}
					auto file_name = entry.path().filename().string();
					if (!file_name.empty() && file_name[0] != '.' && file_name[0] != '_') {
						resolved.push_back(entry.path().string());
					}
				}
			}
			if (!ec) {
				continue;
			}
		}
		resolved.push_back(scan_file);
	}
	sort(resolved.begin(), resolved.end());
	resolved.erase(unique(resolved.begin(), resolved.end()), resolved.end());
	return resolved;
}

static std::string JoinPreview(const std::vector<std::string> &values, idx_t limit = 8) {
	if (values.empty()) {
		return "";
	}
	std::string result;
	for (idx_t i = 0; i < values.size() && i < limit; i++) {
		if (!result.empty()) {
			result += ", ";
		}
		result += values[i];
	}
	if (values.size() > limit) {
		result += ", ...";
	}
	return result;
}

static std::vector<std::string> PartitionNames(const MetastoreTable &table,
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

static void AddNamedParameter(named_parameter_map_t &named_parameters, const std::string &name, const Value &value) {
	named_parameters[name] = value;
}

static void BindUnderlyingFunction(ClientContext &context, MetastoreReadBindData &bind_data) {
	std::string scan_function_name;
	switch (bind_data.table.storage_descriptor.format) {
	case MetastoreFormat::JSON:
		Catalog::TryAutoLoad(context, "json");
		scan_function_name = "read_json_auto";
		break;
	case MetastoreFormat::CSV:
		scan_function_name = "read_csv";
		break;
	case MetastoreFormat::Parquet:
		Catalog::TryAutoLoad(context, "parquet");
		scan_function_name = "read_parquet";
		break;
	default:
		throw BinderException("Unsupported metastore table format for direct query: %s", bind_data.table.name);
	}

	auto &func_catalog = Catalog::GetEntry(context, CatalogType::TABLE_FUNCTION_ENTRY, SYSTEM_CATALOG, DEFAULT_SCHEMA,
	                                       scan_function_name)
	                         .Cast<TableFunctionCatalogEntry>();
	bind_data.underlying_function =
	    func_catalog.functions.GetFunctionByArguments(context, {LogicalType::LIST(LogicalType::VARCHAR)});

	vector<Value> file_list;
	if (bind_data.scan_files.empty()) {
		file_list.push_back(Value(MetastoreUtils::BuildScanPath(bind_data.table.storage_descriptor.location,
		                                                        bind_data.table.storage_descriptor.format)));
	} else {
		for (auto &file : bind_data.scan_files) {
			file_list.push_back(Value(file));
		}
	}

	named_parameter_map_t named_parameters;

	if (bind_data.table.storage_descriptor.format == MetastoreFormat::JSON) {
		if (!bind_data.table.storage_descriptor.columns.empty()) {
			child_list_t<Value> column_types;
			for (auto &column : bind_data.table.storage_descriptor.columns) {
				column_types.emplace_back(column.name, Value(MetastoreUtils::MapHiveTypeToDuckDB(column.type)));
			}
			if (bind_data.is_partitioned) {
				for (auto &col : bind_data.table.partition_spec.columns) {
					column_types.emplace_back(col.name, Value(MetastoreUtils::MapHiveTypeToDuckDB(col.type)));
				}
			}
			AddNamedParameter(named_parameters, "columns", Value::STRUCT(std::move(column_types)));
		}
	}
	if (bind_data.table.storage_descriptor.format == MetastoreFormat::CSV) {
		AddNamedParameter(named_parameters, "header", Value::BOOLEAN(false));
		auto serde_it = bind_data.table.storage_descriptor.serde_parameters.find("field.delim");
		if (serde_it == bind_data.table.storage_descriptor.serde_parameters.end()) {
			serde_it = bind_data.table.storage_descriptor.serde_parameters.find("serialization.format");
		}
		if (serde_it != bind_data.table.storage_descriptor.serde_parameters.end() && !serde_it->second.empty()) {
			AddNamedParameter(named_parameters, "delim", Value(serde_it->second));
		}
		if (!bind_data.table.storage_descriptor.columns.empty()) {
			child_list_t<Value> column_types;
			for (auto &column : bind_data.table.storage_descriptor.columns) {
				column_types.emplace_back(column.name, Value(MetastoreUtils::MapHiveTypeToDuckDB(column.type)));
			}
			AddNamedParameter(named_parameters, "columns", Value::STRUCT(std::move(column_types)));
		}
	}
	if (bind_data.is_partitioned) {
		AddNamedParameter(named_parameters, "hive_partitioning", Value::BOOLEAN(true));
	}

	vector<LogicalType> input_table_types;
	vector<string> input_table_names;
	vector<Value> bind_inputs;
	bind_inputs.push_back(Value::LIST(LogicalType::VARCHAR, file_list));
	auto table_func_ref = duckdb::make_uniq<TableFunctionRef>();
	TableFunctionBindInput bind_input(bind_inputs, named_parameters, input_table_types, input_table_names, nullptr,
	                                  nullptr, bind_data.underlying_function, *table_func_ref);

	try {
		bind_data.underlying_bind_data =
		    bind_data.underlying_function.bind(context, bind_input, bind_data.return_types, bind_data.names);
		if (bind_data.is_partitioned && !bind_data.table.storage_descriptor.columns.empty()) {
			auto data_col_count = bind_data.table.storage_descriptor.columns.size();
			if (bind_data.names.size() >= data_col_count) {
				for (idx_t i = 0; i < data_col_count; i++) {
					bind_data.names[i] = bind_data.table.storage_descriptor.columns[i].name;
				}
			}
		}
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

	auto config_opt = LookupMetastoreAttachConfig(catalog);
	if (!config_opt.has_value()) {
		throw BinderException("Metastore catalog %s not found", catalog);
	}

	auto factory = ProviderRegistry::GetFactory(StringUtil::Lower(MetastoreProviderTypeToString(config_opt->provider)));
	if (!factory) {
		throw BinderException("No factory found for provider type: %s",
		                      MetastoreProviderTypeToString(config_opt->provider));
	}

	bind_data->connector = factory->CreateConnector(*config_opt);

	auto table_result = bind_data->connector->GetTable(schema, table_name);
	if (!table_result.IsOk()) {
		throw BinderException("Failed to get table metadata for %s.%s: %s", schema, table_name,
		                      table_result.error.message);
	}
	bind_data->table = table_result.value;
	bind_data->is_partitioned = bind_data->table.IsPartitioned();

	if (!bind_data->is_partitioned) {
		bind_data->scan_files.push_back(MetastoreUtils::BuildScanPath(bind_data->table.storage_descriptor.location,
		                                                              bind_data->table.storage_descriptor.format));
	} else {
		auto parts_result = bind_data->connector->ListPartitions(schema, table_name, "");
		if (parts_result.IsOk()) {
			bind_data->selected_partitions = PartitionNames(bind_data->table, parts_result.value);
			for (auto &part : parts_result.value) {
				auto scan_path =
				    MetastoreUtils::BuildScanPath(part.location, bind_data->table.storage_descriptor.format);
				if (!scan_path.empty()) {
					bind_data->scan_files.push_back(std::move(scan_path));
				}
			}
			if (bind_data->scan_files.empty()) {
				bind_data->scan_files.push_back(MetastoreUtils::BuildScanPath(
				    bind_data->table.storage_descriptor.location, bind_data->table.storage_descriptor.format));
			}
		} else {
			throw BinderException("Failed to list partitions: %s", parts_result.error.message);
		}
	}

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
	    MetastorePlanner::GeneratePartitionPredicate(bind_data.table, filter_set, get.GetColumnIds(), bind_data.names);

	auto parts_result = bind_data.connector->ListPartitions(bind_data.schema, bind_data.table_name, predicate);
	if (parts_result.IsOk()) {
		bind_data.selected_partitions = PartitionNames(bind_data.table, parts_result.value);
		bind_data.scan_files.clear();
		for (auto &part : parts_result.value) {
			auto scan_path = MetastoreUtils::BuildScanPath(part.location, bind_data.table.storage_descriptor.format);
			if (!scan_path.empty()) {
				bind_data.scan_files.push_back(std::move(scan_path));
			}
		}
		if (bind_data.scan_files.empty()) {
			bind_data.scan_files.push_back(MetastoreUtils::BuildScanPath(bind_data.table.storage_descriptor.location,
			                                                             bind_data.table.storage_descriptor.format));
		}
	}

	BindUnderlyingFunction(context, bind_data);
	if (bind_data.underlying_function.pushdown_complex_filter) {
		bind_data.underlying_function.pushdown_complex_filter(context, get, bind_data.underlying_bind_data.get(),
		                                                      filters);
	}
}

struct MetastoreReadGlobalState : public GlobalTableFunctionState {
	duckdb::unique_ptr<GlobalTableFunctionState> underlying_state;
};

duckdb::unique_ptr<GlobalTableFunctionState> MetastoreReadInitGlobal(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<MetastoreReadBindData>();
	auto gstate = duckdb::make_uniq<MetastoreReadGlobalState>();
	if (bind_data.underlying_function.init_global) {
		TableFunctionInitInput underlying_input(bind_data.underlying_bind_data.get(), input.column_ids,
		                                        input.projection_ids, input.filters);
		gstate->underlying_state = bind_data.underlying_function.init_global(context, underlying_input);
	}
	return std::move(gstate);
}

struct MetastoreReadLocalState : public LocalTableFunctionState {
	duckdb::unique_ptr<LocalTableFunctionState> underlying_state;
};

duckdb::unique_ptr<LocalTableFunctionState> MetastoreReadInitLocal(ExecutionContext &context,
                                                                   TableFunctionInitInput &input,
                                                                   GlobalTableFunctionState *global_state) {
	auto &bind_data = input.bind_data->Cast<MetastoreReadBindData>();
	auto &gstate = global_state->Cast<MetastoreReadGlobalState>();
	auto lstate = duckdb::make_uniq<MetastoreReadLocalState>();
	if (bind_data.underlying_function.init_local) {
		TableFunctionInitInput underlying_input(bind_data.underlying_bind_data.get(), input.column_ids,
		                                        input.projection_ids, input.filters);
		lstate->underlying_state =
		    bind_data.underlying_function.init_local(context, underlying_input, gstate.underlying_state.get());
	}
	return std::move(lstate);
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
	bind_data.underlying_function.function(context, underlying_input, output);
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

TableFunction GetMetastoreReadFunction() {
	TableFunction func("metastore_read", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                   MetastoreReadExecute, MetastoreReadBind, MetastoreReadInitGlobal, MetastoreReadInitLocal);
	func.filter_pushdown = true;
	func.pushdown_complex_filter = MetastoreReadPushdownComplexFilter;
	func.projection_pushdown = true;
	func.to_string = MetastoreReadToString;
	return func;
}

} // namespace duckdb
