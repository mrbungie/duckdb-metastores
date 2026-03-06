#include "metastore_functions.hpp"
#include "metastore_runtime.hpp"
#include "metastore_partition_cache.hpp"
#include "connector/metastore_connector.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/main/materialized_query_result.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Helper: extract columns/partitions from ANY named parameter
//===--------------------------------------------------------------------===//
static void ExtractColumns(const Value &val, std::vector<MetastoreColumn> &out_cols) {
	if (val.IsNull() || val.type().id() != LogicalTypeId::STRUCT) {
		throw BinderException("columns requires a STRUCT");
	}
	auto &children = StructValue::GetChildren(val);
	for (idx_t i = 0; i < children.size(); i++) {
		MetastoreColumn mc;
		mc.name = StructType::GetChildName(val.type(), i);
		mc.type = children[i].ToString();
		out_cols.push_back(std::move(mc));
	}
}

static void ExtractPartitions(const Value &val, std::vector<MetastorePartitionColumn> &out_cols) {
	if (val.IsNull() || val.type().id() != LogicalTypeId::STRUCT) {
		throw BinderException("partitions requires a STRUCT");
	}
	auto &children = StructValue::GetChildren(val);
	for (idx_t i = 0; i < children.size(); i++) {
		MetastorePartitionColumn mpc;
		mpc.name = StructType::GetChildName(val.type(), i);
		mpc.type = children[i].ToString();
		out_cols.push_back(std::move(mpc));
	}
}

//===--------------------------------------------------------------------===//
// Bind & Data Structures
//===--------------------------------------------------------------------===//

struct MetastoreWriteBindData : public TableFunctionData {
	string catalog;
	string schema;
	string table_name;

	// For create table
	MetastoreTable new_table;

	// For insert / create partition
	vector<string> partition_values;
	string location;
	string query;
};

struct MetastoreWriteGlobalState : public GlobalTableFunctionState {
	duckdb::unique_ptr<IMetastoreConnector> connector;
	bool success = false;
};

static void InvalidatePartitionCacheForTable(ClientContext &context, const string &catalog, const string &schema,
	                                         const string &table_name) {
	auto cache = MetastorePartitionCache::GetOrCreate(context);
	cache->InvalidateTable(catalog, schema, table_name);
}

static duckdb::unique_ptr<GlobalTableFunctionState> MetastoreWriteInitGlobal(ClientContext &context,
                                                                             TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<MetastoreWriteBindData>();
	auto gstate = duckdb::make_uniq<MetastoreWriteGlobalState>();
	gstate->connector = CreateConnector(bind_data.catalog, bind_data.schema);
	return std::move(gstate);
}

//===--------------------------------------------------------------------===//
// metastore_create_table
//===--------------------------------------------------------------------===//

static duckdb::unique_ptr<FunctionData> MetastoreCreateTableBind(ClientContext &context, TableFunctionBindInput &input,
                                                                 vector<LogicalType> &return_types,
                                                                 vector<string> &names) {
	auto bind_data = duckdb::make_uniq<MetastoreWriteBindData>();

	bind_data->catalog = input.inputs[0].ToString();
	bind_data->schema = input.inputs[1].ToString();
	bind_data->table_name = input.inputs[2].ToString();

	MetastoreTable tbl;
	tbl.catalog = bind_data->catalog;
	tbl.namespace_name = bind_data->schema;
	tbl.name = bind_data->table_name;

	auto loc_it = input.named_parameters.find("location");
	if (loc_it == input.named_parameters.end()) {
		throw BinderException("location is required");
	}
	tbl.storage_descriptor.location = loc_it->second.ToString();

	auto format_it = input.named_parameters.find("format");
	if (format_it != input.named_parameters.end()) {
		auto fmt_str = StringUtil::Lower(format_it->second.ToString());
		if (fmt_str == "parquet") {
			tbl.storage_descriptor.format = MetastoreFormat::Parquet;
		} else if (fmt_str == "csv") {
			tbl.storage_descriptor.format = MetastoreFormat::CSV;
		} else if (fmt_str == "json") {
			tbl.storage_descriptor.format = MetastoreFormat::JSON;
		} else {
			tbl.storage_descriptor.format = MetastoreFormat::Unknown;
		}
	} else {
		tbl.storage_descriptor.format = MetastoreFormat::Parquet;
	}

	auto cols_it = input.named_parameters.find("columns");
	if (cols_it != input.named_parameters.end()) {
		ExtractColumns(cols_it->second, tbl.storage_descriptor.columns);
	}

	auto parts_it = input.named_parameters.find("partitions");
	if (parts_it != input.named_parameters.end()) {
		ExtractPartitions(parts_it->second, tbl.partition_spec.columns);
	}

	bind_data->new_table = std::move(tbl);

	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("success");

	return std::move(bind_data);
}

static void MetastoreCreateTableExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<MetastoreWriteGlobalState>();
	if (gstate.success) {
		output.SetCardinality(0);
		return;
	}
	auto &bind_data = data.bind_data->Cast<MetastoreWriteBindData>();

	auto result = gstate.connector->CreateTable(bind_data.new_table);
	if (!result.IsOk()) {
		throw IOException("Failed to create table: %s", result.error.message);
	}

	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BOOLEAN(true));
	InvalidatePartitionCacheForTable(context, bind_data.catalog, bind_data.schema, bind_data.table_name);
	gstate.success = true;
}

//===--------------------------------------------------------------------===//
// metastore_create_partition
//===--------------------------------------------------------------------===//
static duckdb::unique_ptr<FunctionData> MetastoreCreatePartitionBind(ClientContext &context,
                                                                     TableFunctionBindInput &input,
                                                                     vector<LogicalType> &return_types,
                                                                     vector<string> &names) {
	auto bind_data = duckdb::make_uniq<MetastoreWriteBindData>();

	bind_data->catalog = input.inputs[0].ToString();
	bind_data->schema = input.inputs[1].ToString();
	bind_data->table_name = input.inputs[2].ToString();

	auto loc_it = input.named_parameters.find("location");
	if (loc_it == input.named_parameters.end()) {
		throw BinderException("location is required");
	}
	bind_data->location = loc_it->second.ToString();

	auto val_it = input.named_parameters.find("values");
	if (val_it == input.named_parameters.end() || val_it->second.type().id() != LogicalTypeId::LIST) {
		throw BinderException("values requires a LIST of VARCHAR");
	}
	auto &list_children = ListValue::GetChildren(val_it->second);
	for (auto &child : list_children) {
		bind_data->partition_values.push_back(child.ToString());
	}

	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("success");

	return std::move(bind_data);
}

static void MetastoreCreatePartitionExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<MetastoreWriteGlobalState>();
	if (gstate.success) {
		output.SetCardinality(0);
		return;
	}
	auto &bind_data = data.bind_data->Cast<MetastoreWriteBindData>();

	MetastorePartitionValue mp;
	mp.location = bind_data.location;
	for (auto &v : bind_data.partition_values) {
		mp.values.push_back(v);
	}

	auto result = gstate.connector->AddPartition(bind_data.table_name, mp);
	if (!result.IsOk()) {
		throw IOException("Failed to add partition: %s", result.error.message);
	}

	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BOOLEAN(true));
	InvalidatePartitionCacheForTable(context, bind_data.catalog, bind_data.schema, bind_data.table_name);
	gstate.success = true;
}

//===--------------------------------------------------------------------===//
// metastore_insert
//===--------------------------------------------------------------------===//
static duckdb::unique_ptr<FunctionData> MetastoreInsertBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = duckdb::make_uniq<MetastoreWriteBindData>();

	bind_data->catalog = input.inputs[0].ToString();
	bind_data->schema = input.inputs[1].ToString();
	bind_data->table_name = input.inputs[2].ToString();

	auto q_it = input.named_parameters.find("query");
	if (q_it == input.named_parameters.end()) {
		throw BinderException("query is required");
	}
	bind_data->query = q_it->second.ToString();

	auto val_it = input.named_parameters.find("values");
	if (val_it != input.named_parameters.end() && val_it->second.type().id() == LogicalTypeId::LIST) {
		auto &list_children = ListValue::GetChildren(val_it->second);
		for (auto &child : list_children) {
			bind_data->partition_values.push_back(child.ToString());
		}
	}

	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("success");

	return std::move(bind_data);
}

struct DynamicInsertPlan {
	string partition_columns_sql;
	vector<string> partition_column_names;
	duckdb::unique_ptr<MaterializedQueryResult> discovered_partitions;
};

static MetastoreTable ResolveInsertTarget(IMetastoreConnector &connector, const string &table_name) {
	auto table_res = connector.GetTable(table_name);
	if (!table_res.IsOk()) {
		throw IOException("Failed to fetch table metadata: %s", table_res.error.message);
	}
	return std::move(table_res.value);
}

static string ResolveInsertFormat(const MetastoreTable &table) {
	string format_str = "parquet";
	if (table.storage_descriptor.format == MetastoreFormat::CSV) {
		format_str = "csv";
	} else if (table.storage_descriptor.format == MetastoreFormat::JSON) {
		format_str = "json";
	}
	return format_str;
}

static DynamicInsertPlan PlanDynamicInsert(ClientContext &context, const MetastoreTable &table, const string &query) {
	if (table.storage_descriptor.format == MetastoreFormat::Unknown) {
		throw BinderException("Dynamic partitioning not supported for Unknown format");
	}

	auto &fs = FileSystem::GetFileSystem(context);
	if (!fs.DirectoryExists(table.storage_descriptor.location)) {
		fs.CreateDirectoriesRecursive(table.storage_descriptor.location);
	}

	DynamicInsertPlan plan;
	for (idx_t i = 0; i < table.partition_spec.columns.size(); i++) {
		if (i > 0) {
			plan.partition_columns_sql += ", ";
		}
		auto name = table.partition_spec.columns[i].name;
		plan.partition_columns_sql += KeywordHelper::WriteOptionallyQuoted(name);
		plan.partition_column_names.push_back(name);
	}

	string discovery_sql = "SELECT DISTINCT " + plan.partition_columns_sql + " FROM (" + query + ")";
	auto discovery_res = MetastoreRuntime::GetConnection().Query(discovery_sql);
	if (discovery_res->HasError()) {
		throw IOException("Failed to discover partitions (is your query missing partition columns?): %s",
		                  discovery_res->GetError());
	}
	plan.discovered_partitions = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(discovery_res));
	return plan;
}

static void ExecuteDynamicPhysicalWrite(const string &query, const string &location, const string &format,
	                                     const DynamicInsertPlan &plan) {
	if (plan.discovered_partitions->RowCount() == 0) {
		return;
	}

	string copy_sql =
	    "COPY (" + query + ") TO '" + location + "' (FORMAT " + format + ", PARTITION_BY (" + plan.partition_columns_sql +
	    "), OVERWRITE TRUE);";
	auto res = MetastoreRuntime::GetConnection().Query(copy_sql);
	if (res->HasError()) {
		throw IOException("Failed to execute dynamic COPY: %s", res->GetError());
	}
}

static void RegisterDiscoveredPartitions(IMetastoreConnector &connector, const string &table_name, const string &location,
	                                     const DynamicInsertPlan &plan) {
	if (plan.discovered_partitions->RowCount() == 0) {
		return;
	}

	for (idx_t r = 0; r < plan.discovered_partitions->RowCount(); r++) {
		MetastorePartitionValue mp;
		string part_path = location;
		for (idx_t c = 0; c < plan.discovered_partitions->ColumnCount(); c++) {
			auto val_obj = plan.discovered_partitions->GetValue(c, r);
			string val;
			string path_val;
			if (val_obj.IsNull()) {
				val = "NULL";
				path_val = "NULL";
			} else {
				val = val_obj.ToString();
				path_val = val;
			}
			mp.values.push_back(val);
			if (!StringUtil::EndsWith(part_path, "/")) {
				part_path += "/";
			}
			part_path += plan.partition_column_names[c] + "=" + path_val;
		}
		mp.location = part_path;
		auto p_res = connector.AddPartition(table_name, mp);
		if (!p_res.IsOk()) {
			throw IOException("Failed to add discovered partition metadata: %s", p_res.error.message);
		}
	}
}

static string PlanTargetedInsertLocation(ClientContext &context, const MetastoreTable &table,
	                                     const vector<string> &partition_values) {
	string target_dir = table.storage_descriptor.location;
	if (table.IsPartitioned()) {
		if (partition_values.size() != table.partition_spec.columns.size()) {
			throw InvalidInputException("Partition value count mismatch. Expected %d, got %d",
			                            static_cast<int>(table.partition_spec.columns.size()),
			                            static_cast<int>(partition_values.size()));
		}
		for (idx_t i = 0; i < table.partition_spec.columns.size(); i++) {
			if (!StringUtil::EndsWith(target_dir, "/")) {
				target_dir += "/";
			}
			target_dir += table.partition_spec.columns[i].name + "=" + partition_values[i];
		}
		if (!StringUtil::EndsWith(target_dir, ".parquet") && table.storage_descriptor.format == MetastoreFormat::Parquet) {
			target_dir += ".parquet";
		}
	}

	if (table.IsPartitioned()) {
		auto &fs = FileSystem::GetFileSystem(context);
		auto last_sep = target_dir.find_last_of("/\\");
		if (last_sep != string::npos) {
			auto parent_dir = target_dir.substr(0, last_sep);
			if (!fs.DirectoryExists(parent_dir)) {
				fs.CreateDirectoriesRecursive(parent_dir);
			}
		}
	}

	return target_dir;
}

static void ExecuteTargetedPhysicalWrite(const string &query, const string &target_dir, const string &format) {
	string copy_sql = "COPY (" + query + ") TO '" + target_dir + "' (FORMAT " + format + ", OVERWRITE TRUE);";
	auto res = MetastoreRuntime::GetConnection().Query(copy_sql);
	if (res->HasError()) {
		throw IOException("Failed to execute COPY: %s", res->GetError());
	}
}

static void RegisterTargetedPartition(IMetastoreConnector &connector, const string &table_name, const MetastoreTable &table,
	                                  const string &target_dir, const vector<string> &partition_values) {
	if (!table.IsPartitioned()) {
		return;
	}

	MetastorePartitionValue mp;
	mp.location = target_dir;
	for (auto &v : partition_values) {
		mp.values.push_back(v);
	}
	auto p_res = connector.AddPartition(table_name, mp);
	if (!p_res.IsOk()) {
		throw IOException("Failed to add partition metadata: %s", p_res.error.message);
	}
}

static bool ShouldUseDynamicInsert(const MetastoreTable &table, const vector<string> &partition_values) {
	return table.IsPartitioned() && partition_values.empty();
}

static void ExecuteDynamicInsert(IMetastoreConnector &connector, ClientContext &context, const string &table_name,
	                             const MetastoreTable &table, const string &query, const string &format) {
	auto dynamic_plan = PlanDynamicInsert(context, table, query);
	ExecuteDynamicPhysicalWrite(query, table.storage_descriptor.location, format, dynamic_plan);
	RegisterDiscoveredPartitions(connector, table_name, table.storage_descriptor.location, dynamic_plan);
}

static void ExecuteTargetedInsert(IMetastoreConnector &connector, ClientContext &context, const string &table_name,
	                              const MetastoreTable &table, const vector<string> &partition_values,
	                              const string &query, const string &format) {
	auto target_dir = PlanTargetedInsertLocation(context, table, partition_values);
	ExecuteTargetedPhysicalWrite(query, target_dir, format);
	RegisterTargetedPartition(connector, table_name, table, target_dir, partition_values);
}

static void MetastoreInsertExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<MetastoreWriteGlobalState>();
	if (gstate.success) {
		output.SetCardinality(0);
		return;
	}
	auto &bind_data = data.bind_data->Cast<MetastoreWriteBindData>();

	auto table = ResolveInsertTarget(*gstate.connector, bind_data.table_name);
	auto format_str = ResolveInsertFormat(table);
	if (ShouldUseDynamicInsert(table, bind_data.partition_values)) {
		ExecuteDynamicInsert(*gstate.connector, context, bind_data.table_name, table, bind_data.query, format_str);
	} else {
		ExecuteTargetedInsert(*gstate.connector, context, bind_data.table_name, table, bind_data.partition_values,
		                    bind_data.query, format_str);
	}

	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BOOLEAN(true));
	InvalidatePartitionCacheForTable(context, bind_data.catalog, bind_data.schema, bind_data.table_name);
	gstate.success = true;
}

//===--------------------------------------------------------------------===//
// Registration
//===--------------------------------------------------------------------===//
TableFunctionSet MetastoreFunctions::GetMetastoreCreateTableFunction() {
	TableFunction func("metastore_create_table", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                   MetastoreCreateTableExecute, MetastoreCreateTableBind, MetastoreWriteInitGlobal);
	func.named_parameters["columns"] = LogicalType::ANY;
	func.named_parameters["partitions"] = LogicalType::ANY;
	func.named_parameters["location"] = LogicalType::VARCHAR;
	func.named_parameters["format"] = LogicalType::VARCHAR;

	TableFunctionSet set("metastore_create_table");
	set.AddFunction(func);
	return set;
}

TableFunctionSet MetastoreFunctions::GetMetastoreCreatePartitionFunction() {
	TableFunction func("metastore_create_partition", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                   MetastoreCreatePartitionExecute, MetastoreCreatePartitionBind, MetastoreWriteInitGlobal);
	func.named_parameters["values"] = LogicalType::ANY;
	func.named_parameters["location"] = LogicalType::VARCHAR;

	TableFunctionSet set("metastore_create_partition");
	set.AddFunction(func);
	return set;
}

TableFunctionSet MetastoreFunctions::GetMetastoreInsertFunction() {
	TableFunction func("metastore_insert", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                   MetastoreInsertExecute, MetastoreInsertBind, MetastoreWriteInitGlobal);
	func.named_parameters["query"] = LogicalType::VARCHAR;
	func.named_parameters["values"] = LogicalType::ANY;

	TableFunctionSet set("metastore_insert");
	set.AddFunction(func);
	return set;
}

} // namespace duckdb
