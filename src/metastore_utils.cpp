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
	std::string res = location;
	if (StringUtil::StartsWith(res, "file://")) {
		res = res.substr(7);
	} else if (StringUtil::StartsWith(res, "file:")) {
		res = res.substr(5);
	}
	// Replace all "\\" with "/"
	for (size_t i = 0; i < res.size(); i++) {
		if (res[i] == '\\') {
			res[i] = '/';
		}
	}
	// Replace "//" with "/"
	size_t pos;
	while ((pos = res.find("//")) != std::string::npos) {
		res.replace(pos, 2, "/");
	}
	while (res.size() > 1 && res.back() == '/') {
		res.pop_back();
	}
	return StringUtil::Lower(res);
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
		auto lower = StringUtil::Lower(location);
		if (StringUtil::EndsWith(lower, ".parquet") || StringUtil::EndsWith(lower, ".csv") ||
		    StringUtil::EndsWith(lower, ".json") || StringUtil::EndsWith(lower, ".gz") ||
		    StringUtil::EndsWith(lower, ".zst")) {
			return location;
		}
		if (!StringUtil::EndsWith(location, "/")) {
			return location + "/[!._]*";
		}
		return location + "[!._]*";
	}
	return location;
}

} // namespace duckdb
