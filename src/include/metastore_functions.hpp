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

	// Write functions
	static TableFunctionSet GetMetastoreCreateTableFunction();
	static TableFunctionSet GetMetastoreCreatePartitionFunction();
	static TableFunctionSet GetMetastoreInsertFunction();
};

} // namespace duckdb