#include "metastore_connector.hpp"
#include "metastore_ffi.h"

#include <memory>
#include <string>
#include <vector>

namespace duckdb {

namespace {

std::string SafeStr(const char *s) {
	return s ? std::string(s) : std::string();
}

std::optional<std::string> OptionalStr(const char *s) {
	return s ? std::optional<std::string>(std::string(s)) : std::nullopt;
}

MetastoreError ConvertFFIError(const MetastoreFFIError &ffi_err) {
	if (ffi_err.code == METASTORE_OK) {
		return MetastoreError();
	}
	return MetastoreError(static_cast<MetastoreErrorCode>(ffi_err.code), SafeStr(ffi_err.message),
	                      SafeStr(ffi_err.detail), ffi_err.retryable);
}

MetastoreFormat ParseFormat(const char *fmt) {
	if (!fmt) {
		return MetastoreFormat::Unknown;
	}
	std::string s(fmt);
	if (s == "Parquet") {
		return MetastoreFormat::Parquet;
	}
	if (s == "ORC") {
		return MetastoreFormat::ORC;
	}
	if (s == "CSV") {
		return MetastoreFormat::CSV;
	}
	if (s == "Delta") {
		return MetastoreFormat::Delta;
	}
	if (s == "Iceberg") {
		return MetastoreFormat::Iceberg;
	}
	return MetastoreFormat::Unknown;
}

} // namespace

class FFIMetastoreConnector : public IMetastoreConnector {
public:
	explicit FFIMetastoreConnector(MetastoreConnectorHandle handle_p) : handle(handle_p) {
	}

	~FFIMetastoreConnector() override {
		if (handle) {
			metastore_connector_free(handle);
			handle = nullptr;
		}
	}

	FFIMetastoreConnector(const FFIMetastoreConnector &) = delete;
	FFIMetastoreConnector &operator=(const FFIMetastoreConnector &) = delete;

	MetastoreResult<std::vector<MetastoreNamespace>> ListNamespaces() override {
		MetastoreFFINamespaceList ffi_list = {};
		MetastoreFFIError ffi_err = metastore_list_namespaces(handle, &ffi_list);

		if (ffi_err.code != METASTORE_OK) {
			auto err = ConvertFFIError(ffi_err);
			metastore_error_free(&ffi_err);
			return MetastoreResult<std::vector<MetastoreNamespace>>::Error(err.code, std::move(err.message),
			                                                              std::move(err.detail), err.retryable);
		}
		metastore_error_free(&ffi_err);

		std::vector<MetastoreNamespace> result;
		result.reserve(ffi_list.count);
		for (size_t i = 0; i < ffi_list.count; i++) {
			MetastoreNamespace ns;
			ns.name = SafeStr(ffi_list.items[i].name);
			ns.catalog = SafeStr(ffi_list.items[i].catalog);
			ns.description = OptionalStr(ffi_list.items[i].description);
			ns.location = OptionalStr(ffi_list.items[i].location);
			result.push_back(std::move(ns));
		}
		metastore_namespace_list_free(&ffi_list);
		return MetastoreResult<std::vector<MetastoreNamespace>>::Success(std::move(result));
	}

	MetastoreResult<std::vector<std::string>> ListTables(const std::string &namespace_name) override {
		MetastoreFFIStringList ffi_list = {};
		MetastoreFFIError ffi_err = metastore_list_tables(handle, namespace_name.c_str(), &ffi_list);

		if (ffi_err.code != METASTORE_OK) {
			auto err = ConvertFFIError(ffi_err);
			metastore_error_free(&ffi_err);
			return MetastoreResult<std::vector<std::string>>::Error(err.code, std::move(err.message),
			                                                       std::move(err.detail), err.retryable);
		}
		metastore_error_free(&ffi_err);

		std::vector<std::string> result;
		result.reserve(ffi_list.count);
		for (size_t i = 0; i < ffi_list.count; i++) {
			result.push_back(SafeStr(ffi_list.items[i]));
		}
		metastore_string_list_free(&ffi_list);
		return MetastoreResult<std::vector<std::string>>::Success(std::move(result));
	}

	MetastoreResult<MetastoreTable> GetTable(const std::string &namespace_name,
	                                         const std::string &table_name) override {
		MetastoreFFITable ffi_table = {};
		MetastoreFFIError ffi_err = metastore_get_table(handle, namespace_name.c_str(), table_name.c_str(), &ffi_table);

		if (ffi_err.code != METASTORE_OK) {
			auto err = ConvertFFIError(ffi_err);
			metastore_error_free(&ffi_err);
			return MetastoreResult<MetastoreTable>::Error(err.code, std::move(err.message), std::move(err.detail),
			                                             err.retryable);
		}
		metastore_error_free(&ffi_err);

		MetastoreTable table;
		table.catalog = SafeStr(ffi_table.catalog);
		table.namespace_name = SafeStr(ffi_table.namespace_name);
		table.name = SafeStr(ffi_table.name);

		table.storage_descriptor.location = SafeStr(ffi_table.storage_descriptor.location);
		table.storage_descriptor.format = ParseFormat(ffi_table.storage_descriptor.format);
		table.storage_descriptor.serde_class = OptionalStr(ffi_table.storage_descriptor.serde_class);
		table.storage_descriptor.input_format = OptionalStr(ffi_table.storage_descriptor.input_format);
		table.storage_descriptor.output_format = OptionalStr(ffi_table.storage_descriptor.output_format);

		for (size_t i = 0; i < ffi_table.partition_column_count; i++) {
			MetastorePartitionColumn col;
			col.name = SafeStr(ffi_table.partition_columns[i].name);
			col.type = SafeStr(ffi_table.partition_columns[i].type);
			table.partition_spec.columns.push_back(std::move(col));
		}

		for (size_t i = 0; i < ffi_table.property_count; i++) {
			table.properties[SafeStr(ffi_table.property_keys[i])] = SafeStr(ffi_table.property_values[i]);
		}

		if (ffi_table.owner) {
			table.owner = std::string(ffi_table.owner);
		}

		metastore_table_free(&ffi_table);
		return MetastoreResult<MetastoreTable>::Success(std::move(table));
	}

	MetastoreResult<std::vector<MetastorePartitionValue>>
	ListPartitions(const std::string &namespace_name, const std::string &table_name,
	               const std::string &predicate) override {
		MetastoreFFIPartitionValueList ffi_list = {};
		const char *pred = predicate.empty() ? nullptr : predicate.c_str();
		MetastoreFFIError ffi_err =
		    metastore_list_partitions(handle, namespace_name.c_str(), table_name.c_str(), pred, &ffi_list);

		if (ffi_err.code != METASTORE_OK) {
			auto err = ConvertFFIError(ffi_err);
			metastore_error_free(&ffi_err);
			return MetastoreResult<std::vector<MetastorePartitionValue>>::Error(
			    err.code, std::move(err.message), std::move(err.detail), err.retryable);
		}
		metastore_error_free(&ffi_err);

		std::vector<MetastorePartitionValue> result;
		result.reserve(ffi_list.count);
		for (size_t i = 0; i < ffi_list.count; i++) {
			MetastorePartitionValue pv;
			pv.location = SafeStr(ffi_list.items[i].location);
			for (size_t j = 0; j < ffi_list.items[i].value_count; j++) {
				pv.values.push_back(SafeStr(ffi_list.items[i].values[j]));
			}
			result.push_back(std::move(pv));
		}
		metastore_partition_value_list_free(&ffi_list);
		return MetastoreResult<std::vector<MetastorePartitionValue>>::Success(std::move(result));
	}

	MetastoreResult<MetastoreTableProperties> GetTableStats(const std::string &namespace_name,
	                                                        const std::string &table_name) override {
		MetastoreFFIKeyValueList ffi_list = {};
		MetastoreFFIError ffi_err = metastore_get_table_stats(handle, namespace_name.c_str(), table_name.c_str(), &ffi_list);

		if (ffi_err.code != METASTORE_OK) {
			auto err = ConvertFFIError(ffi_err);
			metastore_error_free(&ffi_err);
			return MetastoreResult<MetastoreTableProperties>::Error(err.code, std::move(err.message),
			                                                       std::move(err.detail), err.retryable);
		}
		metastore_error_free(&ffi_err);

		MetastoreTableProperties result;
		for (size_t i = 0; i < ffi_list.count; i++) {
			result[SafeStr(ffi_list.keys[i])] = SafeStr(ffi_list.values[i]);
		}
		metastore_key_value_list_free(&ffi_list);
		return MetastoreResult<MetastoreTableProperties>::Success(std::move(result));
	}

private:
	MetastoreConnectorHandle handle;
};

std::unique_ptr<IMetastoreConnector> CreateFFIConnector(MetastoreConnectorHandle handle) {
	return std::unique_ptr<IMetastoreConnector>(new FFIMetastoreConnector(handle));
}

} // namespace duckdb
