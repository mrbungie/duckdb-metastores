#include "hms/hms_mapper.hpp"

#include <algorithm>
#include <cctype>
#include <initializer_list>
#include <utility>

namespace duckdb {

namespace {

std::string ToLower(std::string value) {
	std::transform(value.begin(), value.end(), value.begin(),
	               [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
	return value;
}

bool ContainsAny(const std::string &value, const std::initializer_list<const char *> &needles) {
	for (const auto *needle : needles) {
		if (value.find(needle) != std::string::npos) {
			return true;
		}
	}
	return false;
}

MetastoreFormat DetectFromPattern(const std::optional<std::string> &field) {
	if (!field.has_value()) {
		return MetastoreFormat::Unknown;
	}
	auto lower = ToLower(*field);
	if (ContainsAny(lower, {"mapredparquetinputformat", "parquet"})) {
		return MetastoreFormat::Parquet;
	}
	if (ContainsAny(lower, {"jsoninputformat", "json"})) {
		return MetastoreFormat::JSON;
	}
	if (ContainsAny(lower, {"orcinputformat", "orc"})) {
		return MetastoreFormat::ORC;
	}
	if (ContainsAny(lower, {"textinputformat", "csv", "text"})) {
		return MetastoreFormat::CSV;
	}
	return MetastoreFormat::Unknown;
}

MetastoreFormat DetectFromSerde(const std::optional<std::string> &field) {
	if (!field.has_value()) {
		return MetastoreFormat::Unknown;
	}
	auto lower = ToLower(*field);
	if (ContainsAny(lower, {"parquethiveserde", "parquet"})) {
		return MetastoreFormat::Parquet;
	}
	if (ContainsAny(lower, {"jsonserde", "json"})) {
		return MetastoreFormat::JSON;
	}
	if (ContainsAny(lower, {"orcserde", "orc"})) {
		return MetastoreFormat::ORC;
	}
	if (ContainsAny(lower, {"lazysimpleserde", "csv", "text"})) {
		return MetastoreFormat::CSV;
	}
	return MetastoreFormat::Unknown;
}

} // namespace

MetastoreFormat HmsMapper::DetectFormat(const MetastoreStorageDescriptor &sd) {
	if (sd.format != MetastoreFormat::Unknown) {
		return sd.format;
	}

	auto input_format = DetectFromPattern(sd.input_format);
	if (input_format != MetastoreFormat::Unknown) {
		return input_format;
	}

	auto output_format = DetectFromPattern(sd.output_format);
	if (output_format != MetastoreFormat::Unknown) {
		return output_format;
	}

	return DetectFromSerde(sd.serde_class);
}

MetastoreResult<MetastoreTable> HmsMapper::MapTable(const std::string &catalog, const std::string &namespace_name,
                                                    const std::string &table_name, MetastoreStorageDescriptor sd,
                                                    MetastorePartitionSpec partition_spec,
                                                    MetastoreTableProperties properties) {
	if (sd.location.empty()) {
		return MetastoreResult<MetastoreTable>::Error(MetastoreErrorCode::InvalidConfig,
		                                              "HMS table location is missing", table_name, false);
	}

	sd.format = DetectFormat(sd);
	if (sd.format == MetastoreFormat::Unknown) {
		return MetastoreResult<MetastoreTable>::Error(
		    MetastoreErrorCode::Unsupported, "Unsupported HMS serde format for table: " + table_name,
		    sd.serde_class.value_or(sd.input_format.value_or("unknown")), false);
	}

	MetastoreTable table;
	table.catalog = catalog;
	table.namespace_name = namespace_name;
	table.name = table_name;
	table.storage_descriptor = std::move(sd);
	table.partition_spec = std::move(partition_spec);
	table.properties = std::move(properties);

	return MetastoreResult<MetastoreTable>::Success(std::move(table));
}

} // namespace duckdb
