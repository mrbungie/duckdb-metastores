#pragma once

#include "duckdb.hpp"
#include "metastore_types.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

//! Interface for format-specific scan logic.
class IFormatReader {
public:
	virtual ~IFormatReader() = default;

	virtual string GetScanFunctionName() const = 0;
	virtual string GetRequiredExtension() const {
		return "";
	}
	virtual named_parameter_map_t BuildNamedParameters(const MetastoreStorageDescriptor &sd,
	                                                   const MetastorePartitionSpec &partition_spec,
	                                                   bool is_partitioned) const = 0;
	virtual string BuildScanPath(const string &raw_location) const = 0;
};

class ParquetFormatReader : public IFormatReader {
public:
	string GetScanFunctionName() const override;
	string GetRequiredExtension() const override;
	named_parameter_map_t BuildNamedParameters(const MetastoreStorageDescriptor &sd,
	                                           const MetastorePartitionSpec &partition_spec,
	                                           bool is_partitioned) const override;
	string BuildScanPath(const string &raw_location) const override;
};

class CsvFormatReader : public IFormatReader {
public:
	string GetScanFunctionName() const override;
	named_parameter_map_t BuildNamedParameters(const MetastoreStorageDescriptor &sd,
	                                           const MetastorePartitionSpec &partition_spec,
	                                           bool is_partitioned) const override;
	string BuildScanPath(const string &raw_location) const override;
};

class JsonFormatReader : public IFormatReader {
public:
	string GetScanFunctionName() const override;
	string GetRequiredExtension() const override;
	named_parameter_map_t BuildNamedParameters(const MetastoreStorageDescriptor &sd,
	                                           const MetastorePartitionSpec &partition_spec,
	                                           bool is_partitioned) const override;
	string BuildScanPath(const string &raw_location) const override;
};

IFormatReader &GetFormatReader(MetastoreFormat format);

} // namespace duckdb
