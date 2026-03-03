#pragma once

#include "duckdb.hpp"
#include "metastore_types.hpp"

namespace duckdb {

struct MetastoreUtils {
	static std::string TrimTypeSuffix(std::string hive_type);
	static std::string MapHiveTypeToDuckDB(const std::string &hive_type);
	static std::string NormalizeLocation(const std::string &location);
};

} // namespace duckdb
