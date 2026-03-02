#pragma once

#include "providers/hms/hms_config.hpp"
#include "main/metastore_connector.hpp"

namespace duckdb {

class HmsConnector : public IMetastoreConnector {
public:
	explicit HmsConnector(HmsConfig config);
	~HmsConnector() override = default;

	MetastoreResult<std::vector<MetastoreNamespace>> ListNamespaces() override;
	MetastoreResult<std::vector<std::string>> ListTables(const std::string &namespace_name) override;
	MetastoreResult<MetastoreTable> GetTable(const std::string &namespace_name, const std::string &table_name) override;
	MetastoreResult<std::vector<MetastorePartitionValue>> ListPartitions(const std::string &namespace_name,
	                                                                     const std::string &table_name,
	                                                                     const std::string &predicate = "") override;
	MetastoreResult<MetastoreTableProperties> GetTableStats(const std::string &namespace_name,
	                                                        const std::string &table_name) override;

private:
	HmsConfig config_;
	std::vector<std::string> namespaces_cache;
};

} // namespace duckdb
