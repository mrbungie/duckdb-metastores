#pragma once

#include "duckdb.hpp"

namespace duckdb {

class MetastoreFunctions {
public:
	static vector<TableFunctionSet> GetTableFunctions(ExtensionLoader &loader);
	static vector<ScalarFunction> GetScalarFunctions();

private:
	static TableFunctionSet GetMetastoreTableInfoFunction();
	static TableFunctionSet GetMetastoreReadFunction();
};

} // namespace duckdb