#include "formats/format_reader.hpp"
#include "metastore_utils.hpp"

namespace duckdb {

string ParquetFormatReader::GetScanFunctionName() const {
	return "read_parquet";
}

string ParquetFormatReader::GetRequiredExtension() const {
	return "parquet";
}

named_parameter_map_t ParquetFormatReader::BuildNamedParameters(const MetastoreStorageDescriptor &sd,
                                                                const MetastorePartitionSpec &partition_spec,
                                                                bool is_partitioned) const {
	named_parameter_map_t params;
	if (is_partitioned) {
		params["filename"] = Value::BOOLEAN(true);
	}
	return params;
}

string ParquetFormatReader::BuildScanPath(const string &raw_location) const {
	auto location = MetastoreUtils::NormalizeLocation(raw_location);
	if (location.empty() || StringUtil::Contains(location, "*") || StringUtil::Contains(location, "?")) {
		return location;
	}
	auto lower = StringUtil::Lower(location);
	if (StringUtil::EndsWith(lower, ".parquet") || StringUtil::EndsWith(lower, ".gz") ||
	    StringUtil::EndsWith(lower, ".zst")) {
		return location;
	}
	return location + (StringUtil::EndsWith(location, "/") ? "" : "/") + "[!._]*";
}

} // namespace duckdb
