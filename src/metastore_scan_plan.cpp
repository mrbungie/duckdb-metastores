#include "metastore_scan_plan.hpp"
#include "connector/metastore_connector.hpp"
#include "formats/format_reader.hpp"
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
	auto &reader = GetFormatReader(table.storage_descriptor.format);

	auto AddResolvedFiles = [&](const string &raw_path, idx_t part_idx) {
		auto IsIgnoredFile = [](const string &file_path) {
			auto slash_pos = file_path.find_last_of("/\\");
			auto file_name = slash_pos == string::npos ? file_path : file_path.substr(slash_pos + 1);
			return StringUtil::StartsWith(file_name, ".") || StringUtil::StartsWith(file_name, "_") ||
			       StringUtil::EndsWith(file_name, ".crc");
		};

		auto path = reader.BuildScanPath(raw_path);
		if (path.empty()) {
			return;
		}
		// glob expansion
		if (StringUtil::EndsWith(path, "[!._]*")) {
			path = path.substr(0, path.size() - 6);
			if (!StringUtil::EndsWith(path, "/")) {
				path += "/";
			}
			path += "*";
		}

		if (FileSystem::HasGlob(path)) {
			try {
				auto expanded = fs.GlobFiles(path, context, FileGlobOptions::ALLOW_EMPTY);
				expanded.erase(std::remove_if(expanded.begin(), expanded.end(),
				                              [&](const auto &file) { return IsIgnoredFile(file.path); }),
				               expanded.end());
				if (expanded.empty()) {
					plan.files.push_back(path);
					plan.file_partition_indices.push_back(part_idx);
				} else {
					for (auto &file : expanded) {
						if (!IsIgnoredFile(file.path)) {
							plan.files.push_back(file.path);
							plan.file_partition_indices.push_back(part_idx);
						}
					}
				}
			} catch (...) {
				plan.files.push_back(path);
				plan.file_partition_indices.push_back(part_idx);
			}
		} else {
			if (!IsIgnoredFile(path)) {
				plan.files.push_back(path);
				plan.file_partition_indices.push_back(part_idx);
			}
		}
	};

	if (!plan.is_partitioned) {
		AddResolvedFiles(table.storage_descriptor.location, 0);
	} else {
		auto parts_result = connector.ListPartitions(opt.table_name, opt.predicate);
		if (!parts_result.IsOk()) {
			throw BinderException("Failed to list partitions for %s.%s: %s", connector.GetNamespace(), opt.table_name,
			                      parts_result.error.message);
		}

		plan.partitions_examined = parts_result.value.size();
		if (plan.partitions_examined > opt.max_partitions) {
			throw BinderException("Too many partitions (%s) for table %s.%s. Add a simple predicate on partition "
			                      "columns or increase metastore_max_partitions.",
			                      to_string(plan.partitions_examined), connector.GetNamespace(), opt.table_name);
		}

		plan.partitions = std::move(parts_result.value);
		plan.selected_partitions = PartitionNames(table, plan.partitions);

		for (idx_t i = 0; i < plan.partitions.size(); i++) {
			AddResolvedFiles(plan.partitions[i].location, i);
		}

		if (plan.files.empty()) {
			AddResolvedFiles(table.storage_descriptor.location, 0);
		}
	}

	return plan;
}

} // namespace duckdb
