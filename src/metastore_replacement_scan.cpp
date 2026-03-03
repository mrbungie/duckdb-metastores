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
	if (config_opt->provider != MetastoreProviderType::HMS) {
		return nullptr;
	}
	auto parsed_uri = ParsedUri::Parse(config_opt->endpoint);
	auto factory = ProviderRegistry::ResolveProvider(parsed_uri);
	if (!factory) {
		return nullptr;
	}

	// Check if the table exists in the metastore
	duckdb::unique_ptr<IMetastoreConnector> connector = factory->CreateConnector(*config_opt);
	auto table_result = connector->GetTable(input.schema_name, input.table_name);
	if (!table_result.IsOk()) {
		// If not found, return nullptr so DuckDB can try other options or throw a normal "table not found"
		return nullptr;
	}

	// Table exists! Delegate all the heavy lifting (mapping, partitioning, format detection) to metastore_read
	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> arguments;
	arguments.push_back(make_uniq<ConstantExpression>(Value(input.catalog_name)));
	arguments.push_back(make_uniq<ConstantExpression>(Value(input.schema_name)));
	arguments.push_back(make_uniq<ConstantExpression>(Value(input.table_name)));

	table_function->function = make_uniq<FunctionExpression>("metastore_read", std::move(arguments));
	table_function->alias = input.table_name;
	return std::move(table_function);
}

} // namespace duckdb
