#include "formats/format_reader.hpp"
#include "metastore_utils.hpp"

namespace duckdb {

static void AddNamedParameter(named_parameter_map_t &named_parameters, const std::string &name, const Value &value) {
	named_parameters[name] = value;
}

string CsvFormatReader::GetScanFunctionName() const {
	return "read_csv";
}

named_parameter_map_t CsvFormatReader::BuildNamedParameters(const MetastoreStorageDescriptor &sd,
                                                            const MetastorePartitionSpec &partition_spec,
                                                            bool is_partitioned) const {
	named_parameter_map_t params;
	AddNamedParameter(params, "auto_detect", Value::BOOLEAN(true));
	AddNamedParameter(params, "header", Value::BOOLEAN(true));
	auto serde_it = sd.serde_parameters.find("field.delim");
	if (serde_it == sd.serde_parameters.end()) {
		serde_it = sd.serde_parameters.find("serialization.format");
	}
	if (serde_it != sd.serde_parameters.end() && !serde_it->second.empty()) {
		AddNamedParameter(params, "delim", Value(serde_it->second));
	}
	if (!sd.columns.empty()) {
		child_list_t<Value> column_types;
		for (auto &column : sd.columns) {
			column_types.emplace_back(column.name, Value(MetastoreUtils::MapHiveTypeToDuckDB(column.type)));
		}
		AddNamedParameter(params, "columns", Value::STRUCT(std::move(column_types)));
	}
	if (is_partitioned) {
		AddNamedParameter(params, "filename", Value::BOOLEAN(true));
	}
	return params;
}

string CsvFormatReader::BuildScanPath(const string &raw_location) const {
	auto location = MetastoreUtils::NormalizeLocation(raw_location);
	if (location.empty() || StringUtil::Contains(location, "*") || StringUtil::Contains(location, "?")) {
		return location;
	}
	auto lower = StringUtil::Lower(location);
	if (StringUtil::EndsWith(lower, ".csv") || StringUtil::EndsWith(lower, ".gz") ||
	    StringUtil::EndsWith(lower, ".zst")) {
		return location;
	}
	return location + (StringUtil::EndsWith(location, "/") ? "" : "/") + "[!._]*";
}

} // namespace duckdb
