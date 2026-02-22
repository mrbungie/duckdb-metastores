#pragma once

#include "duckdb.hpp"
#include "metastore_runtime.hpp"

namespace duckdb {

// Register metastore table functions.
// ms_info is the per-DatabaseInstance storage info used for catalog config lookup.
void RegisterMetastoreFunctions(ExtensionLoader &loader, MetastoreStorageInfo *ms_info);

} // namespace duckdb
