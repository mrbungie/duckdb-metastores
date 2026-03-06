#pragma once

#include "duckdb.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

duckdb::unique_ptr<TableRef> MetastoreReplacementScan(ClientContext &context, ReplacementScanInput &input,
                                                      optional_ptr<ReplacementScanData> data);

duckdb::unique_ptr<StorageExtension> CreateMetastoreStorageExtension();

void RegisterMetastoreFunctions(ExtensionLoader &loader);

} // namespace duckdb
