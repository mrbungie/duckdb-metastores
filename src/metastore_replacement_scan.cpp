#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "metastore_runtime.hpp"
#include "connector/metastore_connector.hpp"

namespace duckdb {

duckdb::unique_ptr<TableRef> MetastoreReplacementScan(ClientContext &context, ReplacementScanInput &input,
                                                      optional_ptr<ReplacementScanData> data) {
	(void)data;
	if (input.catalog_name.empty()) {
		return nullptr;
	}

	// Check if this catalog is attached as a metastore
	auto config_opt = LookupMetastoreAttachConfig(input.catalog_name);
	if (!config_opt.has_value() || input.schema_name.empty()) {
		return nullptr;
	}

	// Try to resolve the provider factory
	auto parsed_uri = ParsedUri::Parse(config_opt->endpoint);
	auto factory = ProviderRegistry::ResolveProvider(parsed_uri);
	if (!factory) {
		return nullptr;
	}

	// Check if the table exists in the metastore
	auto scoped_config = *config_opt;
	scoped_config.extra_params["namespace"] = input.schema_name;
	duckdb::unique_ptr<IMetastoreConnector> connector = factory->CreateConnector(scoped_config);

	auto table_result = connector->GetTable(input.table_name);
	if (!table_result.IsOk()) {
		// If not found, return nullptr so DuckDB can try other options or throw a normal "table not found"
		return nullptr;
	}

	const auto format = table_result.value.storage_descriptor.format;
	string scan_function_name;
	switch (format) {
	case MetastoreFormat::Parquet:
		scan_function_name = "metastore_parquet_scan";
		break;
	case MetastoreFormat::CSV:
		scan_function_name = "metastore_csv_scan";
		break;
	case MetastoreFormat::JSON:
		scan_function_name = "metastore_json_scan";
		break;
	default:
		scan_function_name = "metastore_read";
		break;
	}

	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> arguments;
	arguments.push_back(make_uniq<ConstantExpression>(Value(input.catalog_name)));
	arguments.push_back(make_uniq<ConstantExpression>(Value(input.schema_name)));
	arguments.push_back(make_uniq<ConstantExpression>(Value(input.table_name)));

	table_function->function = make_uniq<FunctionExpression>(scan_function_name, std::move(arguments));
	table_function->alias = input.table_name;
	return std::move(table_function);
}

} // namespace duckdb
