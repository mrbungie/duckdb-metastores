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

static std::string HandleConstantComparison(const std::string &col_name, const ConstantFilter &filter) {
	std::string op;
	switch (filter.comparison_type) {
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
	return col_name + " " + op + " '" + EscapeString(filter.constant.ToString()) + "'";
}

static std::string HandleInFilter(const std::string &col_name, const InFilter &filter) {
	if (filter.values.empty()) {
		return "";
	}
	std::string in_list;
	for (size_t i = 0; i < filter.values.size(); i++) {
		if (i > 0) {
			in_list += ", ";
		}
		in_list += "'" + EscapeString(filter.values[i].ToString()) + "'";
	}
	return col_name + " IN (" + in_list + ")";
}

static std::string FilterToPredicate(const std::string &col_name, const TableFilter &filter);

static std::string HandleConjunction(const std::string &col_name, const ConjunctionFilter &filter,
                                     const std::string &op) {
	std::string result;
	for (size_t i = 0; i < filter.child_filters.size(); i++) {
		auto child_pred = FilterToPredicate(col_name, *filter.child_filters[i]);
		if (child_pred.empty()) {
			return "";
		}
		if (i > 0) {
			result += " " + op + " ";
		}
		result += "(" + child_pred + ")";
	}
	return result;
}

static std::string FilterToPredicate(const std::string &col_name, const TableFilter &filter) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON:
		return HandleConstantComparison(col_name, filter.Cast<ConstantFilter>());
	case TableFilterType::IN_FILTER:
		return HandleInFilter(col_name, filter.Cast<InFilter>());
	case TableFilterType::CONJUNCTION_AND:
		return HandleConjunction(col_name, filter.Cast<ConjunctionAndFilter>(), "AND");
	case TableFilterType::CONJUNCTION_OR:
		return HandleConjunction(col_name, filter.Cast<ConjunctionOrFilter>(), "OR");
	case TableFilterType::IS_NOT_NULL:
		return col_name + " != '__HIVE_DEFAULT_PARTITION__'";
	case TableFilterType::IS_NULL:
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
		idx_t column_id = entry.first;
		auto &filter = *entry.second;

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
