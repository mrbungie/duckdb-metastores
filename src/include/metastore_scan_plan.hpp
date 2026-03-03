#pragma once

#include "duckdb.hpp"
#include "metastore_types.hpp"
#include "connector/metastore_connector.hpp"

namespace duckdb {

struct MetastoreScanPlan {
	vector<string> files;
	vector<string> selected_partitions;
	vector<MetastorePartitionValue> partitions;
	bool is_partitioned;
	bool used_predicate;
	idx_t partitions_examined;
};

struct MetastorePlanOptions {
	string schema;
	string table_name;
	string predicate;
	idx_t max_partitions;
	bool allow_expand_paths;
};

MetastoreScanPlan PlanScan(ClientContext &context, IMetastoreConnector &connector, const MetastoreTable &table,
                           const MetastorePlanOptions &opt);

} // namespace duckdb
