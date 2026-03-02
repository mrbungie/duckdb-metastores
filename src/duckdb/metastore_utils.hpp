#pragma once

#include "duckdb.hpp"
#include "core/models/metastore_types.hpp"

namespace duckdb {

struct MetastoreUtils {
	static std::string TrimTypeSuffix(std::string hive_type);
	static std::string MapHiveTypeToDuckDB(const std::string &hive_type);
	static std::string NormalizeLocation(const std::string &location);
	static std::string BuildScanPath(const std::string &raw_location, MetastoreFormat format);
};

} // namespace duckdb
