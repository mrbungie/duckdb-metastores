#include "providers/hms/hms_mapper.hpp"
#include "ThriftHiveMetastore.h"
#include <string>
#include <algorithm>
#include <cctype>
#include <initializer_list>
#include <utility>

namespace duckdb {

using Apache::Hadoop::Hive::FieldSchema;
using Apache::Hadoop::Hive::Partition;
using Apache::Hadoop::Hive::SerDeInfo;
using Apache::Hadoop::Hive::StorageDescriptor;
using Apache::Hadoop::Hive::Table;

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

void HmsMapper::ToHmsTable(const MetastoreTable &table, void *out_hms_table) {
	auto &hms_table = *static_cast<Table *>(out_hms_table);
	hms_table.dbName = table.namespace_name;
	hms_table.tableName = table.name;
	hms_table.tableType = "EXTERNAL_TABLE";

	// Convert unordered_map to map
	for (const auto &pair : table.properties) {
		hms_table.parameters[pair.first] = pair.second;
	}
	hms_table.parameters["EXTERNAL"] = "TRUE";

	StorageDescriptor hms_sd;
	hms_sd.location = table.storage_descriptor.location;

	// Map format to SerDe/InputFormat
	switch (table.storage_descriptor.format) {
	case MetastoreFormat::Parquet:
		hms_sd.inputFormat = "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetInputFormat";
		hms_sd.outputFormat = "org.apache.hadoop.hive.ql.io.parquet.MapRedParquetOutputFormat";
		hms_sd.serdeInfo.serializationLib = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
		break;
	case MetastoreFormat::CSV:
		hms_sd.inputFormat = "org.apache.hadoop.mapred.TextInputFormat";
		hms_sd.outputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
		hms_sd.serdeInfo.serializationLib = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
		hms_sd.serdeInfo.parameters["field.delim"] = ",";
		break;
	case MetastoreFormat::JSON:
		hms_sd.inputFormat = "org.apache.hadoop.mapred.TextInputFormat";
		hms_sd.outputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
		hms_sd.serdeInfo.serializationLib = "org.apache.hive.hcatalog.data.JsonSerDe";
		break;
	default:
		break;
	}

	for (const auto &col : table.storage_descriptor.columns) {
		FieldSchema fs;
		fs.name = col.name;
		fs.type = col.type;
		hms_sd.cols.push_back(std::move(fs));
	}
	hms_table.sd = std::move(hms_sd);

	for (const auto &pcol : table.partition_spec.columns) {
		FieldSchema fs;
		fs.name = pcol.name;
		fs.type = pcol.type;
		hms_table.partitionKeys.push_back(std::move(fs));
	}
}

void HmsMapper::ToHmsPartition(const std::string &table_name, const MetastorePartitionValue &partition,
                               void *out_hms_partition) {
	auto &hms_part = *static_cast<Partition *>(out_hms_partition);
	hms_part.tableName = table_name;
	hms_part.values = partition.values;
	hms_part.sd.location = partition.location;
}

} // namespace duckdb
