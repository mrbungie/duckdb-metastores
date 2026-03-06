#include "metastore_functions.hpp"

class ExtensionLoader;

namespace duckdb {

vector<TableFunctionSet> MetastoreFunctions::GetTableFunctions(ExtensionLoader &loader) {
	vector<TableFunctionSet> functions;

	functions.push_back(std::move(GetMetastoreTableInfoFunction()));
	functions.push_back(std::move(GetMetastoreParquetScanFunction()));
	functions.push_back(std::move(GetMetastoreCsvScanFunction()));
	functions.push_back(std::move(GetMetastoreJsonScanFunction()));
	functions.push_back(std::move(GetMetastoreReadFunction()));
	functions.push_back(std::move(GetMetastoreCreateTableFunction()));
	functions.push_back(std::move(GetMetastoreCreatePartitionFunction()));
	functions.push_back(std::move(GetMetastoreInsertFunction()));

	return functions;
}

vector<ScalarFunction> MetastoreFunctions::GetScalarFunctions() {
	return {};
}

} // namespace duckdb
