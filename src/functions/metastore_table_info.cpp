#include "metastore_functions.hpp"
#include "metastore_runtime.hpp"
#include "connector/metastore_connector.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

struct MetastoreTableInfoBindData : public TableFunctionData {
	string catalog;
	string schema;
	string table_name;

	duckdb::unique_ptr<FunctionData> Copy() const override {
		auto copy = duckdb::make_uniq<MetastoreTableInfoBindData>();
		copy->catalog = catalog;
		copy->schema = schema;
		copy->table_name = table_name;
		return std::move(copy);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<MetastoreTableInfoBindData>();
		return catalog == other.catalog && schema == other.schema && table_name == other.table_name;
	}
};

static duckdb::unique_ptr<FunctionData> MetastoreTableInfoBind(ClientContext &context, TableFunctionBindInput &input,
                                                               vector<LogicalType> &return_types,
                                                               vector<string> &names) {

	// Validate argument count (3 required: catalog, schema, table_name)
	if (input.inputs.size() != 3) {
		throw BinderException("metastore_table_info requires 3 arguments: catalog, schema, table_name");
	}

	// Validate that all 3 input arguments are non-empty strings
	for (idx_t i = 0; i < 3; i++) {
		if (input.inputs[i].IsNull()) {
			throw InvalidInputException("Argument " + to_string(i) + " cannot be NULL");
		}
		std::string arg_val = input.inputs[i].GetValue<std::string>();
		if (arg_val.empty()) {
			throw InvalidInputException("Argument " + to_string(i) + " cannot be empty string");
		}
	}

	// Set return schema: 5 VARCHAR columns
	return_types = {
	    LogicalType::VARCHAR, // table_catalog
	    LogicalType::VARCHAR, // table_schema
	    LogicalType::VARCHAR, // table_name
	    LogicalType::VARCHAR, // location
	    LogicalType::VARCHAR  // format
	};

	names = {"table_catalog", "table_schema", "table_name", "location", "format"};

	auto bind_data = duckdb::make_uniq<MetastoreTableInfoBindData>();
	bind_data->catalog = input.inputs[0].GetValue<std::string>();
	bind_data->schema = input.inputs[1].GetValue<std::string>();
	bind_data->table_name = input.inputs[2].GetValue<std::string>();
	return std::move(bind_data);
}

// Global state for metastore_table_info
struct MetastoreTableInfoGlobalState : public GlobalTableFunctionState {
	bool finished = false;
};

// Initialize global state
static duckdb::unique_ptr<GlobalTableFunctionState> MetastoreTableInfoInitGlobal(ClientContext &context,
                                                                                 TableFunctionInitInput &input) {
	return duckdb::make_uniq<MetastoreTableInfoGlobalState>();
}

static void MetastoreTableInfoExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<MetastoreTableInfoGlobalState>();

	if (gstate.finished) {
		output.SetCardinality(0);
		return;
	}
	auto &bind_data = data.bind_data->Cast<MetastoreTableInfoBindData>();

	duckdb::unique_ptr<IMetastoreConnector> connector = CreateConnector(bind_data.catalog);
	if (bind_data.schema != connector->GetNamespace()) {
		output.SetCardinality(0);
		gstate.finished = true;
		return;
	}

	auto table_result = connector->GetTable(bind_data.table_name);
	if (!table_result.IsOk()) {
		throw InvalidInputException(table_result.error.message);
	}
	output.SetCardinality(1);
	output.SetValue(0, 0, Value(table_result.value.catalog));
	output.SetValue(1, 0, Value(table_result.value.namespace_name));
	output.SetValue(2, 0, Value(table_result.value.name));
	output.SetValue(3, 0, Value(table_result.value.storage_descriptor.location));
	output.SetValue(4, 0, Value(MetastoreFormatToString(table_result.value.storage_descriptor.format)));
	gstate.finished = true;
}

TableFunctionSet MetastoreFunctions::GetMetastoreTableInfoFunction() {
	TableFunctionSet function_set("metastore_table_info");

	// TODO: Fix state management (i.e. look at
	// https://github.com/duckdb/duckdb-iceberg/blob/main/src/iceberg_functions/iceberg_table_properties_functions.cpp)
	auto fun = TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                         MetastoreTableInfoExecute, MetastoreTableInfoBind, MetastoreTableInfoInitGlobal);

	function_set.AddFunction(fun);

	return function_set;
}

} // namespace duckdb
