#include "metastore_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include <unordered_map>

namespace duckdb {

// Strips parenthesized suffix and lowercases: "DECIMAL(10,2)" -> "decimal"
std::string MetastoreUtils::TrimTypeSuffix(std::string hive_type) {
	auto pos = hive_type.find('(');
	if (pos != std::string::npos) {
		hive_type = hive_type.substr(0, pos);
	}
	return StringUtil::Lower(hive_type);
}

// Static lookup table mapping normalized Hive base types to DuckDB type strings.
// Used for types that do not carry parameters (or whose parameters are ignored).
static const std::unordered_map<std::string, std::string> &GetHiveTypeLookup() {
	static const std::unordered_map<std::string, std::string> lookup = {
	    {"tinyint", "TINYINT"},
	    {"smallint", "SMALLINT"},
	    {"int", "INTEGER"},
	    {"integer", "INTEGER"},
	    {"bigint", "BIGINT"},
	    {"float", "FLOAT"},
	    {"double", "DOUBLE"},
	    {"boolean", "BOOLEAN"},
	    {"date", "DATE"},
	    {"timestamp", "TIMESTAMP"},
	    {"string", "VARCHAR"},
	    {"std::string", "VARCHAR"},
	    {"varchar", "VARCHAR"},
	    {"char", "VARCHAR"},
	    {"binary", "BLOB"},
	};
	return lookup;
}

// Extracts the parenthesized parameter substring from a raw Hive type.
// Returns the content between '(' and ')' if present, or an empty string.
// Example: "decimal(10,2)" -> "10,2", "int" -> ""
static std::string ExtractTypeParams(const std::string &raw_type) {
	auto open = raw_type.find('(');
	if (open == std::string::npos) {
		return "";
	}
	auto close = raw_type.find(')', open);
	if (close == std::string::npos) {
		return "";
	}
	return raw_type.substr(open + 1, close - open - 1);
}

// Handles parameterized Hive types that need parameter passthrough.
// Currently only "decimal" carries parameters to DuckDB; char/varchar parameters
// are intentionally discarded (DuckDB VARCHAR is unbounded).
static std::string MapParameterizedType(const std::string &base_type, const std::string &params) {
	if (base_type == "decimal" && !params.empty()) {
		return "DECIMAL(" + params + ")";
	}
	// decimal without params gets default precision
	if (base_type == "decimal") {
		return "DECIMAL";
	}
	return "";
}

std::string MetastoreUtils::MapHiveTypeToDuckDB(const std::string &hive_type) {
	auto base_type = TrimTypeSuffix(hive_type);
	const auto &lookup = GetHiveTypeLookup();
	auto it = lookup.find(base_type);
	if (it != lookup.end()) {
		return it->second;
	}
	// Check parameterized types not in the simple lookup table
	auto params = ExtractTypeParams(hive_type);
	auto result = MapParameterizedType(base_type, params);
	if (!result.empty()) {
		return result;
	}
	// Unknown types default to VARCHAR
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
	return res;
}

} // namespace duckdb
