#include "functions/metastore_functions.hpp"
#include "metastore_runtime.hpp"
#include "connector/metastore_connector.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

struct MetastoreScanBindData : public TableFunctionData {
	string catalog;
	string schema;
	string table_name;

	duckdb::unique_ptr<FunctionData> Copy() const override {
		auto copy = duckdb::make_uniq<MetastoreScanBindData>();
		copy->catalog = catalog;
		copy->schema = schema;
		copy->table_name = table_name;
		return std::move(copy);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<MetastoreScanBindData>();
		return catalog == other.catalog && schema == other.schema && table_name == other.table_name;
	}
};

static duckdb::unique_ptr<FunctionData> MetastoreScanBind(ClientContext &context, TableFunctionBindInput &input,
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
		std::string arg_val = input.inputs[i].GetValue<std::string>();
		if (arg_val.empty()) {
			throw InvalidInputException("Argument " + to_string(i) + " cannot be empty std::string");
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

	auto bind_data = duckdb::make_uniq<MetastoreScanBindData>();
	bind_data->catalog = input.inputs[0].GetValue<std::string>();
	bind_data->schema = input.inputs[1].GetValue<std::string>();
	bind_data->table_name = input.inputs[2].GetValue<std::string>();
	return std::move(bind_data);
}

// Global state for metastore_scan
struct MetastoreScanGlobalState : public GlobalTableFunctionState {
	bool finished = false;
};

// Initialize global state
static duckdb::unique_ptr<GlobalTableFunctionState> MetastoreScanInitGlobal(ClientContext &context,
                                                                            TableFunctionInitInput &input) {
	return duckdb::make_uniq<MetastoreScanGlobalState>();
}

static void MetastoreScanExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<MetastoreScanGlobalState>();

	if (gstate.finished) {
		output.SetCardinality(0);
		return;
	}
	auto &bind_data = data.bind_data->Cast<MetastoreScanBindData>();
	auto config_opt = LookupMetastoreAttachConfig(bind_data.catalog);
	if (!config_opt.has_value()) {
		throw InvalidInputException("Catalog is not attached as metastore: " + bind_data.catalog);
	}

	if (config_opt->provider != MetastoreProviderType::HMS) {
		throw InvalidInputException("Only HMS provider is supported in this build");
	}
	auto parsed_uri = ParsedUri::Parse(config_opt->endpoint);
	auto factory = ProviderRegistry::ResolveProvider(parsed_uri);
	if (!factory) {
		throw InvalidInputException(std::string("Provider factory not found for endpoint: ") + config_opt->endpoint);
	}

	duckdb::unique_ptr<IMetastoreConnector> connector = factory->CreateConnector(*config_opt);
	auto table_result = connector->GetTable(bind_data.schema, bind_data.table_name);
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

void RegisterMetastoreFunctions(ExtensionLoader &loader) {
	TableFunction scan_func("metastore_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                        MetastoreScanExecute, MetastoreScanBind, MetastoreScanInitGlobal);
	loader.RegisterFunction(scan_func);
	loader.RegisterFunction(GetMetastoreReadFunction());
}

} // namespace duckdb
