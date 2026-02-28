#include "metastore_functions.hpp"
#include "metastore_runtime.hpp"
#include "metastore_connector.hpp"
#include "planner/metastore_planner.hpp"
#include "hms/hms_config.hpp"
#include "hms/hms_connector.hpp"
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"

namespace duckdb {

// Add metastore_read table function implementation here

struct MetastoreReadBindData : public TableFunctionData {
	std::string catalog;
	std::string schema;
	std::string table_name;
	MetastoreTable table;
	std::unique_ptr<IMetastoreConnector> connector;

	// Partitioning and filters
	std::vector<MetastorePartitionPredicate> partition_predicates;
	std::vector<std::string> scan_files;
	bool is_partitioned;

	// Wrapping the underlying scan (e.g. read_parquet)
	unique_ptr<FunctionData> underlying_bind_data;
	TableFunction underlying_function;
	
	vector<LogicalType> return_types;
	vector<string> names;

	MetastoreReadBindData(std::string catalog_p, std::string schema_p, std::string table_name_p)
	    : catalog(std::move(catalog_p)), schema(std::move(schema_p)), table_name(std::move(table_name_p)), is_partitioned(false) {
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<MetastoreReadBindData>();
		return catalog == other.catalog && schema == other.schema && table_name == other.table_name;
	}

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<MetastoreReadBindData>(catalog, schema, table_name);
		copy->table = table;
		// Connector can't be copied easily, so we leave it null in the copy and re-init if needed.
		copy->partition_predicates = partition_predicates;
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

static string TrimTypeSuffix(string hive_type) {
	auto pos = hive_type.find('(');
	if (pos != string::npos) {
		hive_type = hive_type.substr(0, pos);
	}
	return StringUtil::Lower(hive_type);
}

static string MapHiveTypeToDuckDB(const string &hive_type) {
	auto normalized = TrimTypeSuffix(hive_type);
	if (normalized == "tinyint") return "TINYINT";
	if (normalized == "smallint") return "SMALLINT";
	if (normalized == "int" || normalized == "integer") return "INTEGER";
	if (normalized == "bigint") return "BIGINT";
	if (normalized == "float") return "FLOAT";
	if (normalized == "double") return "DOUBLE";
	if (normalized == "boolean") return "BOOLEAN";
	if (normalized == "date") return "DATE";
	if (normalized == "timestamp") return "TIMESTAMP";
	if (normalized == "string" || normalized == "varchar" || normalized == "char") return "VARCHAR";
	if (normalized == "binary") return "BLOB";
	return "VARCHAR";
}

static string NormalizeHmsLocation(const string &location) {
	if (StringUtil::StartsWith(location, "file://")) {
		return location.substr(7);
	}
	if (StringUtil::StartsWith(location, "file:")) {
		return location.substr(5);
	}
	return location;
}

static string BuildScanPath(const string &raw_location, MetastoreFormat format) {
	auto location = NormalizeHmsLocation(raw_location);
	if (location.empty()) {
		return location;
	}
	if (StringUtil::Contains(location, "*") || StringUtil::Contains(location, "?")) {
		return location;
	}
	if (format == MetastoreFormat::CSV || format == MetastoreFormat::Parquet || format == MetastoreFormat::JSON) {
		if (!StringUtil::EndsWith(location, "/")) {
			return location + "/[!._]*";
		}
		return location + "[!._]*";
	}
	return location;
}

static void AddNamedConstant(vector<unique_ptr<ParsedExpression>> &arguments, const string &name, Value value) {
	auto named_arg = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL,
	                                                 make_uniq<ColumnRefExpression>(name),
	                                                 make_uniq<ConstantExpression>(std::move(value)));
	arguments.push_back(std::move(named_arg));
}

static void BindUnderlyingFunction(ClientContext &context, MetastoreReadBindData &bind_data) {
	string scan_function_name;
	switch (bind_data.table.storage_descriptor.format) {
	case MetastoreFormat::JSON:
		Catalog::TryAutoLoad(context, "json");
		scan_function_name = "read_json_auto";
		break;
	case MetastoreFormat::CSV:
		scan_function_name = "read_csv_auto";
		break;
	case MetastoreFormat::Parquet:
		Catalog::TryAutoLoad(context, "parquet");
		scan_function_name = "read_parquet";
		break;
	default:
		throw BinderException("Unsupported HMS table format for direct query: %s", bind_data.table.name);
	}

	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &func_catalog = Catalog::GetEntry(context, CatalogType::TABLE_FUNCTION_ENTRY, SYSTEM_CATALOG,
	                                      DEFAULT_SCHEMA, scan_function_name).Cast<TableFunctionCatalogEntry>();
	bind_data.underlying_function = func_catalog.functions.GetFunctionByArguments(context, {LogicalType::LIST(LogicalType::VARCHAR)});

	vector<Value> file_list;
	if (bind_data.scan_files.empty()) {
		// Just a dummy so it won't crash
		file_list.push_back(Value(BuildScanPath(bind_data.table.storage_descriptor.location, bind_data.table.storage_descriptor.format)));
	} else {
		for (auto &file : bind_data.scan_files) {
			file_list.push_back(Value(file));
		}
	}

	vector<unique_ptr<ParsedExpression>> arguments;
	arguments.push_back(make_uniq<ConstantExpression>(Value::LIST(LogicalType::VARCHAR, file_list)));

	if (bind_data.table.storage_descriptor.format == MetastoreFormat::JSON) {
		if (!bind_data.table.storage_descriptor.columns.empty()) {
			child_list_t<Value> column_types;
			for (auto &column : bind_data.table.storage_descriptor.columns) {
				column_types.emplace_back(column.name, Value(MapHiveTypeToDuckDB(column.type)));
			}
			if (bind_data.is_partitioned) {
				for (auto &col : bind_data.table.partition_spec.columns) {
					column_types.emplace_back(col.name, Value(MapHiveTypeToDuckDB(col.type)));
				}
			}
			AddNamedConstant(arguments, "columns", Value::STRUCT(std::move(column_types)));
		}
	}
	if (bind_data.table.storage_descriptor.format == MetastoreFormat::CSV) {
		AddNamedConstant(arguments, "header", Value::BOOLEAN(false));
		auto serde_it = bind_data.table.storage_descriptor.serde_parameters.find("field.delim");
		if (serde_it == bind_data.table.storage_descriptor.serde_parameters.end()) {
			serde_it = bind_data.table.storage_descriptor.serde_parameters.find("serialization.format");
		}
		if (serde_it != bind_data.table.storage_descriptor.serde_parameters.end() && !serde_it->second.empty()) {
			AddNamedConstant(arguments, "delim", Value(serde_it->second));
		}
		if (!bind_data.table.storage_descriptor.columns.empty()) {
			child_list_t<Value> column_types;
			for (auto &column : bind_data.table.storage_descriptor.columns) {
				column_types.emplace_back(column.name, Value(MapHiveTypeToDuckDB(column.type)));
			}
			if (bind_data.is_partitioned) {
				for (auto &col : bind_data.table.partition_spec.columns) {
					column_types.emplace_back(col.name, Value(MapHiveTypeToDuckDB(col.type)));
				}
			}
			AddNamedConstant(arguments, "columns", Value::STRUCT(std::move(column_types)));
		}
	}
	if (bind_data.is_partitioned) {
		AddNamedConstant(arguments, "hive_partitioning", Value::BOOLEAN(true));
	}

	named_parameter_map_t named_parameters;
	vector<LogicalType> input_table_types;
	vector<string> input_table_names;
	auto table_func_ref = make_uniq<TableFunctionRef>();
	TableFunctionBindInput bind_input(file_list, named_parameters, input_table_types, input_table_names, nullptr, nullptr, bind_data.underlying_function, *table_func_ref);
	
	try {
		bind_data.underlying_bind_data = bind_data.underlying_function.bind(context, bind_input, bind_data.return_types, bind_data.names);
	} catch (std::exception &e) {
		if (bind_data.scan_files.empty() && bind_data.is_partitioned) {
			for (auto &col : bind_data.table.storage_descriptor.columns) {
				bind_data.names.push_back(col.name);
				bind_data.return_types.push_back(TransformStringToLogicalType(MapHiveTypeToDuckDB(col.type)));
			}
			for (auto &col : bind_data.table.partition_spec.columns) {
				bind_data.names.push_back(col.name);
				bind_data.return_types.push_back(TransformStringToLogicalType(MapHiveTypeToDuckDB(col.type)));
			}
		} else {
			throw;
		}
	}
}

unique_ptr<FunctionData> MetastoreReadBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() < 3) {
		throw BinderException("metastore_read requires at least 3 arguments: catalog, schema, table_name");
	}

	auto catalog = input.inputs[0].GetValue<string>();
	auto schema = input.inputs[1].GetValue<string>();
	auto table_name = input.inputs[2].GetValue<string>();

	auto bind_data = make_uniq<MetastoreReadBindData>(catalog, schema, table_name);

	auto config_opt = LookupMetastoreAttachConfig(catalog);
	if (!config_opt.has_value() || config_opt->provider != MetastoreProviderType::HMS) {
		throw BinderException("Metastore catalog %s not found or unsupported", catalog);
	}

	auto hms_config = ParseHmsEndpoint(config_opt->endpoint);
	bind_data->connector = make_uniq<HmsConnector>(std::move(hms_config));

	auto table_result = bind_data->connector->GetTable(schema, table_name);
	if (!table_result.IsOk()) {
		throw BinderException("Failed to get table metadata for %s.%s: %s", schema, table_name,
		                      table_result.error.message);
	}
	bind_data->table = table_result.value;
	bind_data->is_partitioned = bind_data->table.IsPartitioned();

	if (!bind_data->is_partitioned) {
		bind_data->scan_files.push_back(BuildScanPath(bind_data->table.storage_descriptor.location, bind_data->table.storage_descriptor.format));
	} else {
		// Initialize with empty scan files, wait for pushdown to provide the filter to list partitions
		// If pushdown doesn't prune, we will list all. Wait, if we list them now, duckdb types might infer correctly.
		auto parts_result = bind_data->connector->ListPartitions(schema, table_name, "");
		if (parts_result.IsOk()) {
			for (auto &part : parts_result.value) {
				bind_data->scan_files.push_back(BuildScanPath(part.location, bind_data->table.storage_descriptor.format));
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
                                        vector<unique_ptr<Expression>> &filters) {
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
	std::string hms_predicate = MetastorePlanner::GeneratePartitionPredicate(bind_data.table, filter_set, get.GetColumnIds(), bind_data.names);

	auto parts_result = bind_data.connector->ListPartitions(bind_data.schema, bind_data.table_name, hms_predicate);
	if (parts_result.IsOk()) {
		bind_data.scan_files.clear();
		for (auto &part : parts_result.value) {
			bind_data.scan_files.push_back(BuildScanPath(part.location, bind_data.table.storage_descriptor.format));
		}
	}

	BindUnderlyingFunction(context, bind_data);

	// Delegate to DuckDB's native multi-file reader to execute complex functions in-memory against the partitions
	if (bind_data.underlying_function.pushdown_complex_filter) {
		bind_data.underlying_function.pushdown_complex_filter(context, get, bind_data.underlying_bind_data.get(), filters);
	}
}



struct MetastoreReadGlobalState : public GlobalTableFunctionState {
	unique_ptr<GlobalTableFunctionState> underlying_state;
};

unique_ptr<GlobalTableFunctionState> MetastoreReadInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<MetastoreReadBindData>();
	auto gstate = make_uniq<MetastoreReadGlobalState>();
	if (bind_data.underlying_function.init_global) {
		TableFunctionInitInput underlying_input(bind_data.underlying_bind_data.get(), input.column_ids, input.projection_ids, input.filters);
		gstate->underlying_state = bind_data.underlying_function.init_global(context, underlying_input);
	}
	return std::move(gstate);
}

struct MetastoreReadLocalState : public LocalTableFunctionState {
	unique_ptr<LocalTableFunctionState> underlying_state;
};

unique_ptr<LocalTableFunctionState> MetastoreReadInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                           GlobalTableFunctionState *global_state) {
	auto &bind_data = input.bind_data->Cast<MetastoreReadBindData>();
	auto &gstate = global_state->Cast<MetastoreReadGlobalState>();
	auto lstate = make_uniq<MetastoreReadLocalState>();
	if (bind_data.underlying_function.init_local) {
		TableFunctionInitInput underlying_input(bind_data.underlying_bind_data.get(), input.column_ids, input.projection_ids, input.filters);
		lstate->underlying_state = bind_data.underlying_function.init_local(context, underlying_input, gstate.underlying_state.get());
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

	TableFunctionInput underlying_input(bind_data.underlying_bind_data.get(), lstate.underlying_state.get(), gstate.underlying_state.get());
	bind_data.underlying_function.function(context, underlying_input, output);
}

InsertionOrderPreservingMap<string> MetastoreReadToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<MetastoreReadBindData>();
	result["Metastore"] = bind_data.catalog;
	result["Table"] = bind_data.table_name;
	result["Underlying Scan"] = bind_data.underlying_function.name;
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
