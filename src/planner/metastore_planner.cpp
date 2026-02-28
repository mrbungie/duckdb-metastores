#include "planner/metastore_planner.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/common/string_util.hpp"

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

std::string MetastorePlanner::GeneratePartitionPredicate(const MetastoreTable &table, const TableFilterSet &filter_set, const std::vector<column_t> &column_ids, const std::vector<std::string> &names) {
	if (!CanPrunePartitions(table)) {
		return "";
	}
	
	std::string predicate = "";
	bool first = true;

	for (auto &entry : filter_set.filters) {
		idx_t filter_idx = entry.first; // This is the index into column_ids
		auto &filter = *entry.second;
		
		if (filter_idx >= column_ids.size()) continue;
		idx_t column_id = column_ids[filter_idx];
		
		if (column_id >= names.size()) continue;
		std::string col_name = names[column_id];

		// Check if col_name is a partition column
		bool is_partition_col = false;
		for (auto &part_col : table.partition_spec.columns) {
			if (part_col.name == col_name) {
				is_partition_col = true;
				break;
			}
		}

		if (!is_partition_col) continue;

		// Handle CONSTANT_COMPARISON (which is the most common pushdown)
		if (filter.filter_type == TableFilterType::CONSTANT_COMPARISON) {
			auto &constant_filter = filter.Cast<ConstantFilter>();
			if (constant_filter.comparison_type == ExpressionType::COMPARE_EQUAL) {
				if (!first) {
					predicate += " and ";
				}
				first = false;
				predicate += col_name + "='" + constant_filter.constant.ToString() + "'";
			}
		}
	}

	return predicate;
}

}
