#pragma once

#include "metastore_types.hpp"
#include "connector/metastore_connector.hpp"
#include <optional>
#include <string>

#include "duckdb/main/connection.hpp"

namespace duckdb {

void RegisterMetastoreAttachConfig(const std::string &catalog_name, MetastoreCatalogConfig config);
std::optional<MetastoreCatalogConfig> LookupMetastoreAttachConfig(const std::string &catalog_name);

class ProviderRegistry {
public:
	static void Register(duckdb::unique_ptr<IConnectorFactory> factory);
	static IConnectorFactory *ResolveProvider(const ParsedUri &uri);
	static void Initialize();
};

class MetastoreRuntime {
public:
	static void SetDatabase(DatabaseInstance &db);
	static Connection &GetConnection();
};

duckdb::unique_ptr<IMetastoreConnector> CreateConnector(const std::string &catalog_name,
                                                        const std::optional<std::string> &namespace_override =
                                                            std::nullopt);

} // namespace duckdb
