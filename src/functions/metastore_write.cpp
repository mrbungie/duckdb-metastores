#include "metastore_functions.hpp"
#include "metastore_runtime.hpp"
#include "connector/metastore_connector.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/connection.hpp"

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
	mutable duckdb::unique_ptr<IMetastoreConnector> connector;

	// For create table
	MetastoreTable new_table;

	// For insert / create partition
	vector<string> partition_values;
	string location;
	string query;
};

struct MetastoreWriteGlobalState : public GlobalTableFunctionState {
	bool success = false;
};

static duckdb::unique_ptr<GlobalTableFunctionState> MetastoreWriteInitGlobal(ClientContext &context,
                                                                             TableFunctionInitInput &input) {
	return duckdb::make_uniq<MetastoreWriteGlobalState>();
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
	bind_data->connector = CreateConnector(bind_data->catalog);

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

	auto result = bind_data.connector->CreateTable(bind_data.new_table);
	if (!result.IsOk()) {
		throw IOException("Failed to create table: %s", result.error.message);
	}

	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BOOLEAN(true));
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
	bind_data->connector = CreateConnector(bind_data->catalog);

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

	auto result = bind_data.connector->AddPartition(bind_data.table_name, mp);
	if (!result.IsOk()) {
		throw IOException("Failed to add partition: %s", result.error.message);
	}

	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BOOLEAN(true));
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
	bind_data->connector = CreateConnector(bind_data->catalog);

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

static void MetastoreInsertExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<MetastoreWriteGlobalState>();
	if (gstate.success) {
		output.SetCardinality(0);
		return;
	}
	auto &bind_data = data.bind_data->Cast<MetastoreWriteBindData>();

	// 1. Fetch table metadata to determine target format and location
	auto table_res = bind_data.connector->GetTable(bind_data.table_name);
	if (!table_res.IsOk()) {
		throw IOException("Failed to fetch table metadata: %s", table_res.error.message);
	}
	auto &table = table_res.value;

	string target_dir = table.storage_descriptor.location;

	// 2. Build partition path if partitioned
	if (table.IsPartitioned()) {
		if (bind_data.partition_values.size() != table.partition_spec.columns.size()) {
			throw InvalidInputException("Partition value count mismatch. Expected %d, got %d",
			                            static_cast<int>(table.partition_spec.columns.size()),
			                            static_cast<int>(bind_data.partition_values.size()));
		}
		for (idx_t i = 0; i < table.partition_spec.columns.size(); i++) {
			if (!StringUtil::EndsWith(target_dir, "/")) {
				target_dir += "/";
			}
			target_dir += table.partition_spec.columns[i].name + "=" + bind_data.partition_values[i];
		}
	}

	// 3. Perform COPY
	string format_str = "parquet";
	string extension = ".parquet";
	if (table.storage_descriptor.format == MetastoreFormat::CSV) {
		format_str = "csv";
		extension = ".csv";
	} else if (table.storage_descriptor.format == MetastoreFormat::JSON) {
		format_str = "json";
		extension = ".json";
	}

	string write_path = target_dir;
	if (table.IsPartitioned()) {
		// If it's a directory-style path generated from base location,
		// ensure it's a file path for the COPY command or it will fail if the dir doesn't exist.
		// However, registering it as a file path is also more robust for the mock provider.
		if (!StringUtil::EndsWith(write_path, extension)) {
			if (!StringUtil::EndsWith(write_path, "/")) {
				write_path += "/";
			}
			write_path += "data" + extension;
		}
	}

	string copy_sql = "COPY (" + bind_data.query + ") TO '" + write_path + "' (FORMAT " + format_str + ");";
	// We use an isolated connection to run the copy query avoiding context re-entrance issues
	auto res = MetastoreRuntime::GetConnection().Query(copy_sql);
	if (res->HasError()) {
		throw IOException("Failed to execute COPY to %s: %s", write_path, res->GetError());
	}

	// 4. Register partition if partitioned
	if (table.IsPartitioned()) {
		MetastorePartitionValue mp;
		mp.location = write_path;
		for (auto &v : bind_data.partition_values) {
			mp.values.push_back(v);
		}
		auto p_res = bind_data.connector->AddPartition(bind_data.table_name, mp);
		if (!p_res.IsOk()) {
			throw IOException("Failed to add partition metadata: %s", p_res.error.message);
		}
	}

	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BOOLEAN(true));
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
