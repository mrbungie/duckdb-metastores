#pragma once

#include "hms/hms_config.hpp"
#include "metastore_connector.hpp"
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

	//! Map HMS raw table metadata into a MetastoreTable.
	//! Returns Error(Unsupported) if the storage format is unrecognized and cannot be coerced.
	//! Returns Error(InvalidConfig) if required fields (location) are missing.
	static MetastoreResult<MetastoreTable> MapTable(const std::string &catalog, const std::string &namespace_name,
	                                                const std::string &table_name, MetastoreStorageDescriptor sd,
	                                                MetastorePartitionSpec partition_spec,
	                                                MetastoreTableProperties properties);
};

} // namespace duckdb
