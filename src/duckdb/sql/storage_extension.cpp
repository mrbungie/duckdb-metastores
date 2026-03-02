#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/storage/storage_extension.hpp"

#include "runtime/metastore_runtime.hpp"
#include "core/connector/metastore_connector.hpp"

namespace duckdb {

static duckdb::unique_ptr<Catalog> MetastoreAttach(optional_ptr<StorageExtensionInfo> storage_info,
                                                   ClientContext &context, AttachedDatabase &db,
                                                   const std::string &name, AttachInfo &info,
                                                   AttachOptions &attach_options) {
	case_insensitive_map_t<Value> attach_kv;
	for (auto &entry : info.options) {
		attach_kv[entry.first] = entry.second;
	}

	std::string path = info.path;
	if (path.empty() || path == ":memory:") {
		auto it = attach_kv.find("ENDPOINT");
		if (it != attach_kv.end()) {
			path = it->second.ToString();
		}
	}

	if (path.empty() || path == ":memory:") {
		throw InvalidInputException("ENDPOINT or path required for metastore attach");
	}

	auto parsed_uri = ParsedUri::Parse(path);
	auto factory = ProviderRegistry::ResolveProvider(parsed_uri);
	if (!factory) {
		throw InvalidInputException("No metastore provider found for URI: " + path);
	}

	auto connector_config = factory->NormalizeConfig(name, parsed_uri, attach_kv);
	RegisterMetastoreAttachConfig(name, std::move(connector_config));

	info.path = ":memory:";
	auto catalog = duckdb::make_uniq<DuckCatalog>(db);
	catalog->Initialize(false);
	return std::move(catalog);
}

static duckdb::unique_ptr<TransactionManager>
MetastoreCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info, AttachedDatabase &db,
                                  Catalog &catalog) {
	return duckdb::make_uniq<DuckTransactionManager>(db);
}

duckdb::unique_ptr<StorageExtension> CreateMetastoreStorageExtension() {
	auto storage_extension = duckdb::make_uniq<StorageExtension>();
	storage_extension->attach = MetastoreAttach;
	storage_extension->create_transaction_manager = MetastoreCreateTransactionManager;
	return storage_extension;
}

} // namespace duckdb
