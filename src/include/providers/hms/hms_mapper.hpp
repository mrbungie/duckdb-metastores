#pragma once

#include "connector/metastore_connector.hpp"
#include "metastore_types.hpp"

#include <string>

namespace duckdb {

//===--------------------------------------------------------------------===//
// HmsMapper — maps HMS storage descriptor fields to domain model types
//===--------------------------------------------------------------------===//
class HmsMapper {
public:
	//! Detect MetastoreFormat from HMS serde_class / input_format / output_format fields.
	//! Returns MetastoreFormat::Unknown if no known pattern matches.
	static MetastoreFormat DetectFormat(const MetastoreStorageDescriptor &sd);

	static MetastoreResult<MetastoreTable> MapTable(const std::string &catalog, const std::string &namespace_name,
	                                                const std::string &table_name, MetastoreStorageDescriptor sd,
	                                                MetastorePartitionSpec partition_spec,
	                                                MetastoreTableProperties properties);

	//! Map MetastoreTable back to HMS Thrift format (Internal Hive structure)
	static void ToHmsTable(const MetastoreTable &table, void *out_hms_table);

	//! Map MetastorePartitionValue back to HMS Thrift format
	static void ToHmsPartition(const std::string &table_name, const MetastorePartitionValue &partition,
	                           void *out_hms_partition);
};

} // namespace duckdb
