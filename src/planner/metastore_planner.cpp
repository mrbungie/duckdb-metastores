#include "planner/metastore_planner.hpp"

namespace duckdb {

MetastorePlannerResult MetastorePlanner::Plan(const MetastoreTable &table,
	                                          const std::vector<std::string> &requested_namespaces,
	                                          const std::vector<std::string> &requested_tables) {
	MetastorePlannerResult result;

	if (requested_namespaces.size() == 1) {
		result.scan_filter.namespace_filter = requested_namespaces.front();
	}

	if (requested_tables.size() == 1) {
		result.scan_filter.table_filter = requested_tables.front();
	}

	result.partition_pruning_enabled = CanPrunePartitions(table);
	if (result.partition_pruning_enabled) {
		result.reason = "Partition pruning enabled: table has explicit non-empty partition spec.";
	} else {
		result.reason = "Partition pruning disabled: table has no explicit non-empty partition spec.";
	}

	return result;
}

bool MetastorePlanner::CanPrunePartitions(const MetastoreTable &table) {
	if (table.partition_spec.columns.empty()) {
		return false;
	}
	return table.partition_spec.IsPartitioned() && table.IsPartitioned();
}

}
