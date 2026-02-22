#define DUCKDB_EXTENSION_MAIN

#include "metastore_extension.hpp"
#include "metastore_errors.hpp"
#include "metastore_functions.hpp"
#include "metastore_runtime.hpp"
#include "metastore_connector.hpp"
#include "auth/metastore_secret_bridge.hpp"
#include "hms/hms_config.hpp"
#include "hms/hms_connector.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include <duckdb/storage/storage_extension.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

static string TrimTypeSuffix(string hive_type) {
	auto pos = hive_type.find('(');
	if (pos != string::npos) {
		hive_type = hive_type.substr(0, pos);
	}
	return StringUtil::Lower(hive_type);
}

static string MapHiveTypeToDuckDB(const string &hive_type) {
	auto normalized = TrimTypeSuffix(hive_type);
	if (normalized == "tinyint") {
		return "TINYINT";
	}
	if (normalized == "smallint") {
		return "SMALLINT";
	}
	if (normalized == "int" || normalized == "integer") {
		return "INTEGER";
	}
	if (normalized == "bigint") {
		return "BIGINT";
	}
	if (normalized == "float") {
		return "FLOAT";
	}
	if (normalized == "double") {
		return "DOUBLE";
	}
	if (normalized == "boolean") {
		return "BOOLEAN";
	}
	if (normalized == "date") {
		return "DATE";
	}
	if (normalized == "timestamp") {
		return "TIMESTAMP";
	}
	if (normalized == "string" || normalized == "varchar" || normalized == "char") {
		return "VARCHAR";
	}
	if (normalized == "binary") {
		return "BLOB";
	}
	return "VARCHAR";
}

static void AddNamedConstant(vector<unique_ptr<ParsedExpression>> &arguments, const string &name, Value value) {
	auto named_arg = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL,
	                                                 make_uniq<ColumnRefExpression>(name),
	                                                 make_uniq<ConstantExpression>(std::move(value)));
	arguments.push_back(std::move(named_arg));
}

static string NormalizeHmsLocation(const string &location) {
	if (StringUtil::StartsWith(location, "file://")) {
		return location.substr(7);
	}
	if (StringUtil::StartsWith(location, "file:")) {
		return location.substr(5);
	}
	return location;
}

static string BuildScanPath(const string &raw_location, MetastoreFormat format) {
	auto location = NormalizeHmsLocation(raw_location);
	if (location.empty()) {
		return location;
	}
	if (StringUtil::Contains(location, "*") || StringUtil::Contains(location, "?")) {
		return location;
	}
	if (format == MetastoreFormat::CSV || format == MetastoreFormat::Parquet) {
		if (!StringUtil::EndsWith(location, "/")) {
			return location + "/[!._]*";
		}
		return location + "[!._]*";
	}
	return location;
}

static unique_ptr<TableRef> MetastoreReplacementScan(ClientContext &context, ReplacementScanInput &input,
                                                     optional_ptr<ReplacementScanData> data) {
	(void)data;
	if (input.catalog_name.empty()) {
		return nullptr;
	}
	auto config_opt = LookupMetastoreAttachConfig(input.catalog_name);
	if (!config_opt.has_value() || config_opt->provider != MetastoreProviderType::HMS || input.schema_name.empty()) {
		return nullptr;
	}
	auto hms_config = ParseHmsEndpoint(config_opt->endpoint);
	std::unique_ptr<IMetastoreConnector> connector = make_uniq<HmsConnector>(std::move(hms_config));
	auto table_result = connector->GetTable(input.schema_name, input.table_name);
	if (!table_result.IsOk()) {
		if (table_result.error.code == MetastoreErrorCode::NotFound) {
			return nullptr;
		}
		throw BinderException("Failed to resolve HMS table %s.%s.%s: %s", input.catalog_name, input.schema_name,
		                      input.table_name, table_result.error.message);
	}
	if (table_result.value.storage_descriptor.location.empty()) {
		return nullptr;
	}
	string scan_function;
	switch (table_result.value.storage_descriptor.format) {
	case MetastoreFormat::CSV:
		scan_function = "read_csv_auto";
		break;
	case MetastoreFormat::Parquet:
		Catalog::TryAutoLoad(context, "parquet");
		scan_function = "read_parquet";
		break;
	default:
		throw BinderException("Unsupported HMS table format for direct query: %s", input.table_name);
	}
	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> arguments;
	arguments.push_back(make_uniq<ConstantExpression>(
	    Value(BuildScanPath(table_result.value.storage_descriptor.location, table_result.value.storage_descriptor.format))));
	if (table_result.value.storage_descriptor.format == MetastoreFormat::CSV) {
		AddNamedConstant(arguments, "header", Value::BOOLEAN(false));
		auto serde_it = table_result.value.storage_descriptor.serde_parameters.find("field.delim");
		if (serde_it == table_result.value.storage_descriptor.serde_parameters.end()) {
			serde_it = table_result.value.storage_descriptor.serde_parameters.find("serialization.format");
		}
		if (serde_it != table_result.value.storage_descriptor.serde_parameters.end() && !serde_it->second.empty()) {
			AddNamedConstant(arguments, "delim", Value(serde_it->second));
		}
		if (!table_result.value.storage_descriptor.columns.empty()) {
			child_list_t<Value> column_types;
			for (auto &column : table_result.value.storage_descriptor.columns) {
				column_types.emplace_back(column.name, Value(MapHiveTypeToDuckDB(column.type)));
			}
			AddNamedConstant(arguments, "columns", Value::STRUCT(std::move(column_types)));
			AddNamedConstant(arguments, "auto_detect", Value::BOOLEAN(false));
		}
	}
	table_function->function = make_uniq<FunctionExpression>(scan_function, std::move(arguments));
	table_function->alias = input.table_name;
	return std::move(table_function);
}

static unique_ptr<Catalog> MetastoreAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
	                                        AttachedDatabase &db, const string &name, AttachInfo &info,
	                                        AttachOptions &attach_options) {
	case_insensitive_map_t<Value> attach_kv;
	for (auto &entry : info.options) {
		attach_kv[entry.first] = entry.second;
	}
	if (attach_kv.find("PROVIDER") == attach_kv.end() && !info.path.empty() && info.path != ":memory:") {
		attach_kv["PROVIDER"] = Value("hms");
		attach_kv["ENDPOINT"] = Value(info.path);
	}
	auto connector_config = ResolveConnectorConfig(attach_kv);
	if (connector_config.provider != MetastoreProviderType::HMS) {
		throw InvalidInputException("Only HMS provider is supported in this build");
	}
	RegisterMetastoreAttachConfig(name, std::move(connector_config));
	info.path = ":memory:";
	auto catalog = make_uniq<DuckCatalog>(db);
	catalog->Initialize(false);
	return std::move(catalog);
}

static unique_ptr<TransactionManager>
MetastoreCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info, AttachedDatabase &db,
	                              Catalog &catalog) {
	return make_uniq<DuckTransactionManager>(db);
}

static unique_ptr<StorageExtension> CreateMetastoreStorageExtension() {
	auto storage_extension = make_uniq<StorageExtension>();
	storage_extension->attach = MetastoreAttach;
	storage_extension->create_transaction_manager = MetastoreCreateTransactionManager;
	return storage_extension;
}

static void LoadInternal(ExtensionLoader &loader) {
	auto &db_instance = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db_instance);
	config.storage_extensions["metastore"] = CreateMetastoreStorageExtension();
	config.replacement_scans.emplace_back(MetastoreReplacementScan);
	config.AddExtensionOption("metastore_debug", "Enable diagnostic mode for metastore operations",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));

	RegisterMetastoreFunctions(loader);
}

void MetastoreExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string MetastoreExtension::Name() {
	return "metastore";
}

std::string MetastoreExtension::Version() const {
#ifdef EXT_VERSION_METASTORE
	return EXT_VERSION_METASTORE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(metastore, loader) {
	duckdb::LoadInternal(loader);
}
}
