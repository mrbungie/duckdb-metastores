#pragma once

#include "core/connector/metastore_connector.hpp"
#include <optional>
#include <string>

namespace duckdb {

void RegisterMetastoreAttachConfig(const std::string &catalog_name, MetastoreCatalogConfig config);
std::optional<MetastoreCatalogConfig> LookupMetastoreAttachConfig(const std::string &catalog_name);

class ProviderRegistry {
public:
	static void Register(const std::string &provider_id, duckdb::unique_ptr<IConnectorFactory> factory);
	static IConnectorFactory *GetFactory(const std::string &provider_id);
	static IConnectorFactory *ResolveProvider(const ParsedUri &uri);
};

} // namespace duckdb
