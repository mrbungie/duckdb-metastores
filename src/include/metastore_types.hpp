#pragma once

#include "duckdb.hpp"

#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace duckdb {

enum class MetastoreFormat : uint8_t { Parquet, JSON, ORC, CSV, Delta, Iceberg, Unknown };

inline const char *MetastoreFormatToString(MetastoreFormat fmt) {
	switch (fmt) {
	case MetastoreFormat::Parquet:
		return "Parquet";
	case MetastoreFormat::ORC:
		return "ORC";
	case MetastoreFormat::JSON:
		return "JSON";
	case MetastoreFormat::CSV:
		return "CSV";
	case MetastoreFormat::Delta:
		return "Delta";
	case MetastoreFormat::Iceberg:
		return "Iceberg";
	case MetastoreFormat::Unknown:
	default:
		return "Unknown";
	}
}

using MetastoreTableProperties = std::unordered_map<std::string, std::string>;

struct MetastoreColumn {
	std::string name;
	std::string type;
};

struct MetastoreStorageDescriptor {
	std::string location;
	MetastoreFormat format = MetastoreFormat::Unknown;
	std::vector<MetastoreColumn> columns;
	std::unordered_map<std::string, std::string> serde_parameters;
	std::optional<std::string> serde_class;
	std::optional<std::string> input_format;
	std::optional<std::string> output_format;
};

struct MetastorePartitionColumn {
	std::string name;
	//! Type string as reported by the metastore (e.g. "string", "int", "date")
	std::string type;
};

struct MetastorePartitionSpec {
	std::vector<MetastorePartitionColumn> columns;

	bool IsPartitioned() const {
		return !columns.empty();
	}
};

struct MetastorePartitionValue {
	//! Values in the same order as MetastorePartitionSpec::columns
	std::vector<std::string> values;
	std::string location;
};

struct MetastoreCatalog {
	std::string name;
	std::optional<std::string> description;
	std::optional<std::string> location;
};

struct MetastoreNamespace {
	std::string name;
	std::string catalog;
	std::optional<std::string> description;
	std::optional<std::string> location;
	std::unordered_map<std::string, std::string> properties;
};

struct MetastoreTable {
	std::string catalog;
	std::string namespace_name;
	std::string name;

	MetastoreStorageDescriptor storage_descriptor;
	//! Empty columns in partition_spec means unpartitioned
	MetastorePartitionSpec partition_spec;
	MetastoreTableProperties properties;

	std::optional<std::string> owner;

	bool IsPartitioned() const {
		return partition_spec.IsPartitioned();
	}
};

} // namespace duckdb
