#include "metastore_partition_predicate.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

bool MetastorePartitionPredicate::CanPrune(const MetastoreTable &table) {
	if (table.partition_spec.columns.empty()) {
		return false;
	}
	return table.partition_spec.IsPartitioned() && table.IsPartitioned();
}

static std::string EscapeString(const std::string &input) {
	return StringUtil::Replace(input, "'", "''");
}

static std::string FilterToPredicate(const std::string &col_name, const TableFilter &filter) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		std::string op;
		switch (constant_filter.comparison_type) {
		case ExpressionType::COMPARE_EQUAL:
			op = "=";
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
			op = "!=";
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			op = ">";
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			op = ">=";
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			op = "<";
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			op = "<=";
			break;
		default:
			return "";
		}
		return col_name + " " + op + " '" + EscapeString(constant_filter.constant.ToString()) + "'";
	}
	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		if (in_filter.values.empty()) {
			return "";
		}
		std::string in_list;
		for (size_t i = 0; i < in_filter.values.size(); i++) {
			if (i > 0) {
				in_list += ", ";
			}
			in_list += "'" + EscapeString(in_filter.values[i].ToString()) + "'";
		}
		return col_name + " IN (" + in_list + ")";
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &and_filter = filter.Cast<ConjunctionAndFilter>();
		std::string result;
		for (size_t i = 0; i < and_filter.child_filters.size(); i++) {
			auto child_pred = FilterToPredicate(col_name, *and_filter.child_filters[i]);
			if (child_pred.empty()) {
				return ""; // If any child is unsupported, we can't safely pushdown the AND block
			}
			if (i > 0) {
				result += " AND ";
			}
			result += "(" + child_pred + ")";
		}
		return result;
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &or_filter = filter.Cast<ConjunctionOrFilter>();
		std::string result;
		for (size_t i = 0; i < or_filter.child_filters.size(); i++) {
			auto child_pred = FilterToPredicate(col_name, *or_filter.child_filters[i]);
			if (child_pred.empty()) {
				return ""; // If any child is unsupported, the entire OR fails pushdown
			}
			if (i > 0) {
				result += " OR ";
			}
			result += "(" + child_pred + ")";
		}
		return result;
	}
	case TableFilterType::IS_NULL:
		// HMS supports checking for __HIVE_DEFAULT_PARTITION__ but standard SQL IS NULL is tricky
		return "";
	case TableFilterType::IS_NOT_NULL:
		return col_name + " != '__HIVE_DEFAULT_PARTITION__'";
	default:
		return "";
	}
}

std::string MetastorePartitionPredicate::FromTableFilters(const MetastoreTable &table, const TableFilterSet &filter_set,
                                                          const std::vector<ColumnIndex> &column_ids,
                                                          const std::vector<std::string> &names) {
	if (!CanPrune(table)) {
		return "";
	}

	std::string predicate = "";
	bool first = true;

	for (auto &entry : filter_set.filters) {
		idx_t filter_idx = entry.first; // This is the index into column_ids
		auto &filter = *entry.second;

		if (filter_idx >= column_ids.size()) {
			continue;
		}
		idx_t column_id = column_ids[filter_idx].GetPrimaryIndex();

		if (column_id >= names.size()) {
			continue;
		}
		const std::string &col_name = names[column_id];

		// Check if col_name is a partition column
		bool is_partition_col = false;
		for (auto &part_col : table.partition_spec.columns) {
			if (part_col.name == col_name) {
				is_partition_col = true;
				break;
			}
		}

		if (!is_partition_col) {
			continue;
		}

		std::string col_pred = FilterToPredicate(col_name, filter);
		if (!col_pred.empty()) {
			if (!first) {
				predicate += " AND ";
			}
			first = false;
			predicate += col_pred;
		}
	}

	return predicate;
}

} // namespace duckdb
