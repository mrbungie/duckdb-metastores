#include "formats/format_reader.hpp"
#include "metastore_utils.hpp"

namespace duckdb {

static void AddNamedParameter(named_parameter_map_t &named_parameters, const std::string &name, const Value &value) {
	named_parameters[name] = value;
}

string JsonFormatReader::GetScanFunctionName() const {
	return "read_json_auto";
}

string JsonFormatReader::GetRequiredExtension() const {
	return "json";
}

named_parameter_map_t JsonFormatReader::BuildNamedParameters(const MetastoreStorageDescriptor &sd,
                                                             const MetastorePartitionSpec &partition_spec,
                                                             bool is_partitioned) const {
	named_parameter_map_t params;
	if (sd.columns.empty()) {
		AddNamedParameter(params, "auto_detect", Value::BOOLEAN(true));
	} else {
		child_list_t<Value> column_types;
		for (auto &column : sd.columns) {
			column_types.emplace_back(column.name, Value(MetastoreUtils::MapHiveTypeToDuckDB(column.type)));
		}
		if (is_partitioned) {
			for (auto &col : partition_spec.columns) {
				column_types.emplace_back(col.name, Value(MetastoreUtils::MapHiveTypeToDuckDB(col.type)));
			}
		}
		AddNamedParameter(params, "columns", Value::STRUCT(std::move(column_types)));
	}
	if (is_partitioned) {
		AddNamedParameter(params, "filename", Value::BOOLEAN(true));
	}
	return params;
}

string JsonFormatReader::BuildScanPath(const string &raw_location) const {
	auto location = MetastoreUtils::NormalizeLocation(raw_location);
	if (location.empty() || StringUtil::Contains(location, "*") || StringUtil::Contains(location, "?")) {
		return location;
	}
	auto lower = StringUtil::Lower(location);
	if (StringUtil::EndsWith(lower, ".json") || StringUtil::EndsWith(lower, ".gz") ||
	    StringUtil::EndsWith(lower, ".zst")) {
		return location;
	}
	return location + (StringUtil::EndsWith(location, "/") ? "" : "/") + "[!._]*";
}

} // namespace duckdb
