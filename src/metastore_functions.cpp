#include "metastore_functions.hpp"
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

// Bind function for metastore_scan table function
static unique_ptr<FunctionData> MetastoreScanBind(
		ClientContext &context, TableFunctionBindInput &input,
		vector<LogicalType> &return_types, vector<string> &names) {

	// Validate argument count (3 required: catalog, schema, table_name)
	if (input.inputs.size() < 3) {
		throw BinderException("metastore_scan requires at least 3 arguments: catalog, schema, table_name");
	}

	// Validate that all 3 input arguments are non-empty strings
	for (idx_t i = 0; i < 3; i++) {
		if (input.inputs[i].IsNull()) {
			throw InvalidInputException("Argument " + to_string(i) + " cannot be NULL");
		}
		string arg_val = input.inputs[i].GetValue<string>();
		if (arg_val.empty()) {
			throw InvalidInputException("Argument " + to_string(i) + " cannot be empty");
		}
	}

	// Set return schema: 5 VARCHAR columns
	return_types = {
		LogicalType::VARCHAR,  // table_catalog
		LogicalType::VARCHAR,  // table_schema
		LogicalType::VARCHAR,  // table_name
		LogicalType::VARCHAR,  // location
		LogicalType::VARCHAR   // format
	};

	names = {
		"table_catalog",
		"table_schema",
		"table_name",
		"location",
		"format"
	};

	// Return empty bind data (no state needed for empty result)
	return nullptr;
}

// Global state for metastore_scan
struct MetastoreScanGlobalState : public GlobalTableFunctionState {
	bool finished = false;
};

// Initialize global state
static unique_ptr<GlobalTableFunctionState> MetastoreScanInitGlobal(
		ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<MetastoreScanGlobalState>();
}

// Execute function - returns empty result set
static void MetastoreScanExecute(
		ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<MetastoreScanGlobalState>();

	// Return empty result set on first call
	if (gstate.finished) {
		output.SetCardinality(0);
		return;
	}
	gstate.finished = true;
	output.SetCardinality(0);
}

void RegisterMetastoreFunctions(ExtensionLoader &loader) {
	// Register metastore_scan table function
	// Signature: metastore_scan(catalog VARCHAR, schema VARCHAR, table_name VARCHAR)
	loader.RegisterFunction(TableFunction(
			"metastore_scan",
			{LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
			MetastoreScanExecute,
			MetastoreScanBind,
			MetastoreScanInitGlobal
	));
}

} // namespace duckdb
