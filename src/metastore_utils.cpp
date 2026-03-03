#include "metastore_utils.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

std::string MetastoreUtils::TrimTypeSuffix(std::string hive_type) {
	auto pos = hive_type.find('(');
	if (pos != std::string::npos) {
		hive_type = hive_type.substr(0, pos);
	}
	return StringUtil::Lower(hive_type);
}

std::string MetastoreUtils::MapHiveTypeToDuckDB(const std::string &hive_type) {
	auto normalized = TrimTypeSuffix(hive_type);
	if (normalized == "tinyint") {
		return "TINYINT";
	}
	if (normalized == "smallint") {
		return "SMALLINT";
	}
	if (normalized == "int" || normalized == "integer") {
		return "INTEGER";
	}
	if (normalized == "bigint") {
		return "BIGINT";
	}
	if (normalized == "float") {
		return "FLOAT";
	}
	if (normalized == "double") {
		return "DOUBLE";
	}
	if (normalized == "boolean") {
		return "BOOLEAN";
	}
	if (normalized == "date") {
		return "DATE";
	}
	if (normalized == "timestamp") {
		return "TIMESTAMP";
	}
	if (normalized == "std::string" || normalized == "varchar" || normalized == "char") {
		return "VARCHAR";
	}
	if (normalized == "binary") {
		return "BLOB";
	}
	return "VARCHAR";
}

std::string MetastoreUtils::NormalizeLocation(const std::string &location) {
	if (StringUtil::StartsWith(location, "file://")) {
		return location.substr(7);
	}
	if (StringUtil::StartsWith(location, "file:")) {
		return location.substr(5);
	}
	return location;
}

std::string MetastoreUtils::BuildScanPath(const std::string &raw_location, MetastoreFormat format) {
	auto location = NormalizeLocation(raw_location);
	if (location.empty()) {
		return location;
	}
	if (StringUtil::Contains(location, "*") || StringUtil::Contains(location, "?")) {
		return location;
	}
	if (format == MetastoreFormat::CSV || format == MetastoreFormat::Parquet || format == MetastoreFormat::JSON) {
		if (!StringUtil::EndsWith(location, "/")) {
			return location + "/[!._]*";
		}
		return location + "[!._]*";
	}
	return location;
}

} // namespace duckdb
