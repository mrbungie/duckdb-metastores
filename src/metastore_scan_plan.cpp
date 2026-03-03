#include "metastore_scan_plan.hpp"
#include "metastore_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/file_system.hpp"
#include <algorithm>

namespace duckdb {

static std::vector<std::string> PartitionNames(const MetastoreTable &table,
                                               const std::vector<MetastorePartitionValue> &partitions) {
	std::vector<std::string> names;
	for (auto &part : partitions) {
		std::string name;
		for (idx_t i = 0; i < part.values.size() && i < table.partition_spec.columns.size(); i++) {
			if (!name.empty()) {
				name += "/";
			}
			name += table.partition_spec.columns[i].name + "=" + part.values[i];
		}
		if (!name.empty()) {
			names.push_back(std::move(name));
		}
	}
	sort(names.begin(), names.end());
	names.erase(unique(names.begin(), names.end()), names.end());
	return names;
}

MetastoreScanPlan PlanScan(ClientContext &context, IMetastoreConnector &connector, const MetastoreTable &table,
                           const MetastorePlanOptions &opt) {
	MetastoreScanPlan plan;
	plan.is_partitioned = table.IsPartitioned();
	plan.used_predicate = !opt.predicate.empty();
	plan.partitions_examined = 0;

	auto &fs = FileSystem::GetFileSystem(context);

	auto AddResolvedFiles = [&](const string &raw_path) {
		auto path = MetastoreUtils::BuildScanPath(raw_path, table.storage_descriptor.format);
		if (path.empty()) {
			return;
		}

		// Replace the [!._]* logic with standard DuckDB globbing if needed
		// For now, if it ends with [!._]*, we'll just treat it as a directory or simple glob if supported
		if (StringUtil::EndsWith(path, "[!._]*")) {
			path = path.substr(0, path.size() - 6);
			if (!StringUtil::EndsWith(path, "/")) {
				path += "/";
			}
			path += "*";
		}

		if (FileSystem::HasGlob(path)) {
			try {
				auto expanded = fs.GlobFiles(path, context);
				for (auto &file : expanded) {
					plan.files.push_back(file.path);
				}
			} catch (...) {
				// If glob fails, just add as is? Or skip?
				// Fallback to the original path if glob expansion fails
				plan.files.push_back(path);
			}
		} else {
			plan.files.push_back(path);
		}
	};

	if (!plan.is_partitioned) {
		AddResolvedFiles(table.storage_descriptor.location);
	} else {
		auto parts_result = connector.ListPartitions(opt.schema, opt.table_name, opt.predicate);
		if (!parts_result.IsOk()) {
			throw BinderException("Failed to list partitions for %s.%s: %s", opt.schema, opt.table_name,
			                      parts_result.error.message);
		}

		plan.partitions_examined = parts_result.value.size();
		if (plan.partitions_examined > opt.max_partitions) {
			throw BinderException("Too many partitions (%s) for table %s.%s. Add a simple predicate on partition "
			                      "columns or increase metastore_max_partitions.",
			                      to_string(plan.partitions_examined), opt.schema, opt.table_name);
		}

		plan.partitions = std::move(parts_result.value);
		plan.selected_partitions = PartitionNames(table, plan.partitions);

		for (auto &part : plan.partitions) {
			AddResolvedFiles(part.location);
		}

		if (plan.files.empty()) {
			AddResolvedFiles(table.storage_descriptor.location);
		}
	}

	sort(plan.files.begin(), plan.files.end());
	plan.files.erase(unique(plan.files.begin(), plan.files.end()), plan.files.end());

	return plan;
}

} // namespace duckdb
