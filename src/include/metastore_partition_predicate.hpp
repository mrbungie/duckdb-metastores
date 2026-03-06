#pragma once

#include "metastore_types.hpp"
#include "duckdb/common/column_index.hpp"
#include "duckdb/planner/table_filter.hpp"

#include <string>
#include <vector>

namespace duckdb {

class MetastorePartitionPredicate {
public:
	static bool CanPrune(const MetastoreTable &table);

	static std::string FromTableFilters(const MetastoreTable &table, const TableFilterSet &filter_set,
	                                    const std::vector<ColumnIndex> &column_ids,
	                                    const std::vector<std::string> &names);
};

} // namespace duckdb
