#include "providers/hms/hms_connector.hpp"
#include "providers/hms/hms_mapper.hpp"
#ifdef _WIN32
#include "thrift_msvc_shim.h"
#endif
#include "ThriftHiveMetastore.h"

#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

#include <functional>
#include <filesystem>
#include <memory>

namespace duckdb {

namespace {

using Apache::Hadoop::Hive::GetTableRequest;
using Apache::Hadoop::Hive::GetTableResult;
using Apache::Hadoop::Hive::MetaException;
using Apache::Hadoop::Hive::NoSuchObjectException;
using Apache::Hadoop::Hive::Table;
using Apache::Hadoop::Hive::ThriftHiveMetastoreClient;
using apache::thrift::TException;
using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::protocol::TProtocol;
using apache::thrift::transport::TBufferedTransport;
using apache::thrift::transport::TSocket;
using apache::thrift::transport::TTransport;
using apache::thrift::transport::TTransportException;

struct HmsClientContext {
	std::shared_ptr<TTransport> transport;
	duckdb::unique_ptr<ThriftHiveMetastoreClient> client;

	HmsClientContext() = default;
	HmsClientContext(HmsClientContext &&) = default;
	HmsClientContext &operator=(HmsClientContext &&) = default;
	HmsClientContext(const HmsClientContext &) = delete;
	HmsClientContext &operator=(const HmsClientContext &) = delete;

	~HmsClientContext() {
		if (transport && transport->isOpen()) {
			transport->close();
		}
	}
};

MetastoreResult<HmsClientContext> ConnectHms(const HmsConfig &config) {
	try {
		std::shared_ptr<TTransport> socket(new TSocket(config.endpoint, config.port));
		std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
		std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));

		transport->open();

		HmsClientContext ctx;
		ctx.transport = transport;
		ctx.client = duckdb::make_uniq<ThriftHiveMetastoreClient>(protocol);
		return MetastoreResult<HmsClientContext>::Success(std::move(ctx));
	} catch (const TException &tx) {
		return MetastoreResult<HmsClientContext>::Error(MetastoreErrorCode::Transient, "HMS socket connect failed",
		                                                tx.what(), true);
	}
}

std::vector<std::string> ParsePartitionNameValues(const std::string &partition_name) {
	std::vector<std::string> values;
	stringstream ss(partition_name);
	std::string segment;
	while (getline(ss, segment, '/')) {
		auto eq_pos = segment.find('=');
		if (eq_pos == std::string::npos || eq_pos + 1 >= segment.size()) {
			values.push_back(segment);
		} else {
			values.push_back(segment.substr(eq_pos + 1));
		}
	}
	return values;
}

std::string NormalizeFileLocation(std::string location) {
	if (location.rfind("file://", 0) == 0) {
		location = location.substr(7);
	} else if (location.rfind("file:", 0) == 0) {
		location = location.substr(5);
	}
	while (!location.empty() && location.back() == '/') {
		location.pop_back();
	}
	return location;
}

std::vector<std::string> DiscoverLocalPartitionNames(const std::string &table_location, idx_t partition_depth) {
	std::vector<std::string> names;
	if (partition_depth == 0) {
		return names;
	}
	auto root = NormalizeFileLocation(table_location);
	if (root.empty()) {
		return names;
	}
	if (!std::filesystem::exists(root)) {
		return names;
	}

	std::function<void(const std::string &, idx_t, std::vector<std::string> &)> walk;
	walk = [&](const std::string &path, idx_t depth, std::vector<std::string> &segments) {
		if (depth == partition_depth) {
			std::string partition_name;
			for (idx_t i = 0; i < segments.size(); i++) {
				if (i > 0) {
					partition_name += "/";
				}
				partition_name += segments[i];
			}
			names.push_back(std::move(partition_name));
			return;
		}
		for (auto &entry : std::filesystem::directory_iterator(path)) {
			if (!entry.is_directory()) {
				continue;
			}
			auto segment = entry.path().filename().string();
			if (segment.find('=') == std::string::npos) {
				continue;
			}
			segments.push_back(std::move(segment));
			walk(entry.path().string(), depth + 1, segments);
			segments.pop_back();
		}
	};

	std::vector<std::string> segments;
	try {
		walk(root, 0, segments);
	} catch (...) {
		return {};
	}
	std::sort(names.begin(), names.end());
	names.erase(std::unique(names.begin(), names.end()), names.end());
	return names;
}

} // namespace

HmsConnector::HmsConnector(string ns, HmsConfig config) : bound_namespace_(std::move(ns)), config_(std::move(config)) {
}

MetastoreResult<std::vector<std::string>> HmsConnector::ListTables() {
	auto conn_res = ConnectHms(config_);
	if (!conn_res.IsOk()) {
		return MetastoreResult<std::vector<std::string>>::Error(conn_res.error.code, conn_res.error.message,
		                                                        conn_res.error.detail, conn_res.error.retryable);
	}

	std::vector<std::string> tables;
	try {
		conn_res.value.client->get_all_tables(tables, bound_namespace_);
	} catch (const MetaException &e) {
		return MetastoreResult<std::vector<std::string>>::Error(MetastoreErrorCode::Transient, "HMS retrieve error",
		                                                        e.message, true);
	} catch (const TException &tx) {
		return MetastoreResult<std::vector<std::string>>::Error(MetastoreErrorCode::Transient, "HMS network error",
		                                                        tx.what(), true);
	}

	return MetastoreResult<std::vector<std::string>>::Success(std::move(tables));
}

MetastoreResult<MetastoreTable> HmsConnector::GetTable(const std::string &table_name) {
	auto conn_res = ConnectHms(config_);
	if (!conn_res.IsOk()) {
		return MetastoreResult<MetastoreTable>::Error(conn_res.error.code, conn_res.error.message,
		                                              conn_res.error.detail, conn_res.error.retryable);
	}

	GetTableResult res;
	GetTableRequest req;
	req.__set_dbName(bound_namespace_);
	req.__set_tblName(table_name);
	try {
		conn_res.value.client->get_table_req(res, req);
	} catch (const NoSuchObjectException &e) {
		return MetastoreResult<MetastoreTable>::Error(MetastoreErrorCode::NotFound, "HMS table not found", e.message,
		                                              false);
	} catch (const MetaException &e) {
		return MetastoreResult<MetastoreTable>::Error(MetastoreErrorCode::Transient, "HMS retrieve error", e.message,
		                                              true);
	} catch (const TException &tx) {
		return MetastoreResult<MetastoreTable>::Error(MetastoreErrorCode::Transient, "HMS network error", tx.what(),
		                                              true);
	}

	Table &hms_table = res.table;

	MetastoreStorageDescriptor sd;
	if (hms_table.__isset.sd) {
		sd.location = hms_table.sd.location;
		sd.input_format = hms_table.sd.inputFormat;
		sd.output_format = hms_table.sd.outputFormat;
		if (hms_table.sd.__isset.serdeInfo) {
			sd.serde_class = hms_table.sd.serdeInfo.serializationLib;
			sd.serde_parameters = std::unordered_map<std::string, std::string>(
			    hms_table.sd.serdeInfo.parameters.begin(), hms_table.sd.serdeInfo.parameters.end());
		}
		for (const auto &col : hms_table.sd.cols) {
			MetastoreColumn c;
			c.name = col.name;
			c.type = col.type;
			sd.columns.push_back(std::move(c));
		}
	}

	MetastorePartitionSpec p_spec;
	for (const auto &pcol : hms_table.partitionKeys) {
		MetastorePartitionColumn pc;
		pc.name = pcol.name;
		pc.type = pcol.type;
		p_spec.columns.push_back(std::move(pc));
	}

	MetastoreTableProperties properties(hms_table.parameters.begin(), hms_table.parameters.end());
	auto mapped = HmsMapper::MapTable("hms", bound_namespace_, table_name, std::move(sd), std::move(p_spec),
	                                  std::move(properties));
	if (!mapped.IsOk()) {
		return mapped;
	}

	auto final_table = std::move(mapped.value);
	final_table.owner = std::move(hms_table.owner);
	return MetastoreResult<MetastoreTable>::Success(std::move(final_table));
}

MetastoreResult<std::vector<MetastorePartitionValue>> HmsConnector::ListPartitions(const std::string &table_name,
                                                                                   const std::string &predicate) {
	(void)predicate;

	auto conn_res = ConnectHms(config_);
	if (!conn_res.IsOk()) {
		return MetastoreResult<std::vector<MetastorePartitionValue>>::Error(
		    conn_res.error.code, conn_res.error.message, conn_res.error.detail, conn_res.error.retryable);
	}

	std::vector<std::string> partition_names;
	try {
		conn_res.value.client->get_partition_names(partition_names, bound_namespace_, table_name, -1);
	} catch (const NoSuchObjectException &) {
		partition_names.clear();
	} catch (const MetaException &e) {
		return MetastoreResult<std::vector<MetastorePartitionValue>>::Error(MetastoreErrorCode::Transient,
		                                                                    "HMS retrieve error", e.message, true);
	} catch (const TException &tx) {
		return MetastoreResult<std::vector<MetastorePartitionValue>>::Error(MetastoreErrorCode::Transient,
		                                                                    "HMS network error", tx.what(), true);
	}

	auto table_result = GetTable(table_name);
	if (!table_result.IsOk()) {
		return MetastoreResult<std::vector<MetastorePartitionValue>>::Error(
		    table_result.error.code, std::move(table_result.error.message), std::move(table_result.error.detail),
		    table_result.error.retryable);
	}

	const auto &table_location = table_result.value.storage_descriptor.location;
	const bool table_location_has_trailing_slash = !table_location.empty() && table_location.back() == '/';

	if (table_result.value.IsPartitioned()) {
		auto discovered = DiscoverLocalPartitionNames(table_location, table_result.value.partition_spec.columns.size());
		if (!discovered.empty()) {
			partition_names = std::move(discovered);
		}
	}

	std::vector<MetastorePartitionValue> result;
	result.reserve(partition_names.size());
	for (auto &name : partition_names) {
		MetastorePartitionValue pv;
		pv.values = ParsePartitionNameValues(name);
		if (!table_location.empty()) {
			if (table_location_has_trailing_slash) {
				pv.location = table_location + name;
			} else {
				pv.location = table_location + "/" + name;
			}
		}
		result.push_back(std::move(pv));
	}
	return MetastoreResult<std::vector<MetastorePartitionValue>>::Success(std::move(result));
}

MetastoreResult<MetastoreTableProperties> HmsConnector::GetTableStats(const std::string &table_name) {
	auto table_result = GetTable(table_name);
	if (!table_result.IsOk()) {
		return MetastoreResult<MetastoreTableProperties>::Error(
		    table_result.error.code, std::move(table_result.error.message), std::move(table_result.error.detail),
		    table_result.error.retryable);
	}
	return MetastoreResult<MetastoreTableProperties>::Success(std::move(table_result.value.properties));
}

} // namespace duckdb
