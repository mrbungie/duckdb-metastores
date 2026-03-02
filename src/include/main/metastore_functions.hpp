#pragma once

#include "duckdb.hpp"

namespace duckdb {

// Register metastore table functions
void RegisterMetastoreFunctions(ExtensionLoader &loader);

TableFunction GetMetastoreReadFunction();

} // namespace duckdb
