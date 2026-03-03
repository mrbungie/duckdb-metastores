#pragma once

#include "hms_config.hpp"
#include "connector/metastore_connector.hpp"

namespace duckdb {

class HmsConnector : public IMetastoreConnector {
public:
	explicit HmsConnector(string bound_namespace, HmsConfig config);
	~HmsConnector() override = default;

	string GetNamespace() override {
		return bound_namespace_;
	}

	MetastoreResult<std::vector<std::string>> ListTables() override;
	MetastoreResult<MetastoreTable> GetTable(const std::string &table_name) override;
	MetastoreResult<std::vector<MetastorePartitionValue>> ListPartitions(const std::string &table_name,
	                                                                     const std::string &predicate = "") override;
	MetastoreResult<MetastoreTableProperties> GetTableStats(const std::string &table_name) override;

private:
	string bound_namespace_;
	HmsConfig config_;
};

} // namespace duckdb
