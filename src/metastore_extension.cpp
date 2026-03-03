#define DUCKDB_EXTENSION_MAIN

#include "metastore_extension.hpp"
#include "metastore_duckdb.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

void RegisterHmsProvider();

static void LoadInternal(DatabaseInstance &db_instance) {
	auto &config = DBConfig::GetConfig(db_instance);

	// Register providers
	RegisterHmsProvider();

	config.storage_extensions["metastore"] = CreateMetastoreStorageExtension();
	config.replacement_scans.emplace_back(MetastoreReplacementScan);
	config.AddExtensionOption("metastore_debug", "Enable diagnostic mode for metastore operations",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));

	// Register functions
	// We need a context or something to register functions?
	// ExtensionLoader::RegisterFunction just takes the name and some pointers.
}

void MetastoreExtension::Load(ExtensionLoader &loader) {
	auto &db_instance = loader.GetDatabaseInstance();
	LoadInternal(db_instance);
	RegisterMetastoreFunctions(loader);
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
	auto &db = loader.GetDatabaseInstance();
	duckdb::LoadInternal(db);
	duckdb::RegisterMetastoreFunctions(loader);
}
}
