#pragma once

#include "metastore_types.hpp"
#include "duckdb/common/column_index.hpp"
#include "duckdb/planner/table_filter.hpp"

#include <optional>
#include <string>
#include <vector>

namespace duckdb {

struct MetastorePartitionPredicate {
	std::string column;
	std::string value;
};

struct MetastoreScanFilter {
	std::optional<std::string> namespace_filter;
	std::optional<std::string> table_filter;
	std::vector<MetastorePartitionPredicate> partition_predicates;
};

struct MetastorePlannerResult {
	MetastoreScanFilter scan_filter;
	bool partition_pruning_enabled = false;
	std::string reason;
};

class MetastorePlanner {
public:
	static MetastorePlannerResult Plan(const MetastoreTable &table, const std::vector<std::string> &requested_namespaces,
	                                  const std::vector<std::string> &requested_tables);

	static bool CanPrunePartitions(const MetastoreTable &table);

	static std::string GeneratePartitionPredicate(const MetastoreTable &table, const TableFilterSet &filter_set, const vector<ColumnIndex> &column_ids, const std::vector<std::string> &names);
};

}
