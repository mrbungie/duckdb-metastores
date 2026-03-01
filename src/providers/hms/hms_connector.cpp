#include "providers/hms/hms_connector.hpp"
#include "providers/hms/hms_mapper.hpp"
#include "ThriftHiveMetastore.h"

#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

#include <optional>
#include <sstream>
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
	std::unique_ptr<ThriftHiveMetastoreClient> client;

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

		// We could configure timeout on TSocket if needed.
		// auto tsocket = std::dynamic_pointer_cast<TSocket>(socket);
		// tsocket->setConnTimeout(10000);
		// tsocket->setRecvTimeout(10000);
		// tsocket->setSendTimeout(10000);

		transport->open();

		HmsClientContext ctx;
		ctx.transport = transport;
		ctx.client = std::make_unique<ThriftHiveMetastoreClient>(protocol);
		return MetastoreResult<HmsClientContext>::Success(std::move(ctx));
	} catch (const TException &tx) {
		return MetastoreResult<HmsClientContext>::Error(MetastoreErrorCode::Transient, "HMS socket connect failed",
		                                                tx.what(), true);
	}
}

std::vector<std::string> ParsePartitionNameValues(const std::string &partition_name) {
	std::vector<std::string> values;
	std::stringstream ss(partition_name);
	std::string segment;
	while (std::getline(ss, segment, '/')) {
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

HmsConnector::HmsConnector(HmsConfig config) : config_(std::move(config)) {
}

MetastoreResult<std::vector<MetastoreNamespace>> HmsConnector::ListNamespaces() {
	auto conn_res = ConnectHms(config_);
	if (!conn_res.IsOk()) {
		return MetastoreResult<std::vector<MetastoreNamespace>>::Error(conn_res.error.code, conn_res.error.message,
		                                                               conn_res.error.detail, conn_res.error.retryable);
	}

	std::vector<std::string> db_names;
	try {
		conn_res.value.client->get_all_databases(db_names);
	} catch (const MetaException &e) {
		return MetastoreResult<std::vector<MetastoreNamespace>>::Error(MetastoreErrorCode::Transient,
		                                                               "HMS retrieve error", e.message, true);
	} catch (const TException &tx) {
		return MetastoreResult<std::vector<MetastoreNamespace>>::Error(MetastoreErrorCode::Transient,
		                                                               "HMS network error", tx.what(), true);
	}

	namespaces_cache = db_names;
	std::vector<MetastoreNamespace> result;
	result.reserve(db_names.size());
	for (const auto &name : db_names) {
		MetastoreNamespace ns;
		ns.name = name;
		ns.catalog = "hms";
		result.push_back(std::move(ns));
	}
	return MetastoreResult<std::vector<MetastoreNamespace>>::Success(std::move(result));
}

MetastoreResult<std::vector<std::string>> HmsConnector::ListTables(const std::string &namespace_name) {
	auto conn_res = ConnectHms(config_);
	if (!conn_res.IsOk()) {
		return MetastoreResult<std::vector<std::string>>::Error(conn_res.error.code, conn_res.error.message,
		                                                        conn_res.error.detail, conn_res.error.retryable);
	}

	std::vector<std::string> tables;
	try {
		conn_res.value.client->get_all_tables(tables, namespace_name);
	} catch (const MetaException &e) {
		return MetastoreResult<std::vector<std::string>>::Error(MetastoreErrorCode::Transient, "HMS retrieve error",
		                                                        e.message, true);
	} catch (const TException &tx) {
		return MetastoreResult<std::vector<std::string>>::Error(MetastoreErrorCode::Transient, "HMS network error",
		                                                        tx.what(), true);
	}

	return MetastoreResult<std::vector<std::string>>::Success(std::move(tables));
}

MetastoreResult<MetastoreTable> HmsConnector::GetTable(const std::string &namespace_name,
                                                       const std::string &table_name) {
	auto conn_res = ConnectHms(config_);
	if (!conn_res.IsOk()) {
		return MetastoreResult<MetastoreTable>::Error(conn_res.error.code, conn_res.error.message,
		                                              conn_res.error.detail, conn_res.error.retryable);
	}

	GetTableResult res;
	GetTableRequest req;
	req.__set_dbName(namespace_name);
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
	auto mapped =
	    HmsMapper::MapTable("hms", namespace_name, table_name, std::move(sd), std::move(p_spec), std::move(properties));
	if (!mapped.IsOk()) {
		return mapped;
	}

	auto final_table = std::move(mapped.value);
	final_table.owner = std::move(hms_table.owner);
	return MetastoreResult<MetastoreTable>::Success(std::move(final_table));
}

MetastoreResult<std::vector<MetastorePartitionValue>> HmsConnector::ListPartitions(const std::string &namespace_name,
                                                                                   const std::string &table_name,
                                                                                   const std::string &predicate) {
	(void)predicate;

	auto conn_res = ConnectHms(config_);
	if (!conn_res.IsOk()) {
		return MetastoreResult<std::vector<MetastorePartitionValue>>::Error(
		    conn_res.error.code, conn_res.error.message, conn_res.error.detail, conn_res.error.retryable);
	}

	std::vector<std::string> partition_names;
	try {
		conn_res.value.client->get_partition_names(partition_names, namespace_name, table_name, -1);
	} catch (const NoSuchObjectException &) {
		// Ignored, we just yield an empty list
		partition_names.clear();
	} catch (const MetaException &e) {
		return MetastoreResult<std::vector<MetastorePartitionValue>>::Error(MetastoreErrorCode::Transient,
		                                                                    "HMS retrieve error", e.message, true);
	} catch (const TException &tx) {
		return MetastoreResult<std::vector<MetastorePartitionValue>>::Error(MetastoreErrorCode::Transient,
		                                                                    "HMS network error", tx.what(), true);
	}

	auto table_result = GetTable(namespace_name, table_name);
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

MetastoreResult<MetastoreTableProperties> HmsConnector::GetTableStats(const std::string &namespace_name,
                                                                      const std::string &table_name) {
	auto table_result = GetTable(namespace_name, table_name);
	if (!table_result.IsOk()) {
		return MetastoreResult<MetastoreTableProperties>::Error(
		    table_result.error.code, std::move(table_result.error.message), std::move(table_result.error.detail),
		    table_result.error.retryable);
	}
	return MetastoreResult<MetastoreTableProperties>::Success(std::move(table_result.value.properties));
}

//===--------------------------------------------------------------------===//
// ParseHmsEndpoint
//===--------------------------------------------------------------------===//
static bool ParsePort(const std::string &port_str, uint16_t &port_out) {
	if (port_str.empty()) {
		return false;
	}
	for (char c : port_str) {
		if (c < '0' || c > '9') {
			return false;
		}
	}
	uint64_t val;
	try {
		val = std::stoul(port_str);
	} catch (...) {
		return false;
	}
	if (val == 0 || val > 65535) {
		return false;
	}
	port_out = static_cast<uint16_t>(val);
	return true;
}

HmsConfig ParseHmsEndpoint(const std::string &endpoint) {
	MetastoreErrorTag tag {"hms", "ParseHmsEndpoint", false};

	if (endpoint.empty()) {
		throw MetastoreException(MetastoreErrorCode::InvalidConfig, tag, "HMS endpoint URI is empty");
	}

	HmsConfig config;
	std::string remainder;

	// Detect and strip scheme
	const std::string thrift_ssl_scheme = "thrift+ssl://";
	const std::string thrift_scheme = "thrift://";

	if (endpoint.size() >= thrift_ssl_scheme.size() &&
	    endpoint.substr(0, thrift_ssl_scheme.size()) == thrift_ssl_scheme) {
		config.transport = HmsTransport::ThriftTLS;
		remainder = endpoint.substr(thrift_ssl_scheme.size());
	} else if (endpoint.size() >= thrift_scheme.size() && endpoint.substr(0, thrift_scheme.size()) == thrift_scheme) {
		config.transport = HmsTransport::Thrift;
		remainder = endpoint.substr(thrift_scheme.size());
	} else {
		config.transport = HmsTransport::Thrift;
		remainder = endpoint;
	}

	if (remainder.empty()) {
		throw MetastoreException(MetastoreErrorCode::InvalidConfig, tag,
		                         "HMS endpoint URI has no host: '" + endpoint + "'");
	}

	// Split host:port
	auto colon_pos = remainder.rfind(':');
	if (colon_pos != std::string::npos && colon_pos > 0) {
		std::string host_part = remainder.substr(0, colon_pos);
		std::string port_part = remainder.substr(colon_pos + 1);

		uint16_t parsed_port;
		if (ParsePort(port_part, parsed_port)) {
			config.endpoint = host_part;
			config.port = parsed_port;
		} else {
			throw MetastoreException(MetastoreErrorCode::InvalidConfig, tag,
			                         "Invalid port in HMS endpoint URI: '" + endpoint + "'");
		}
	} else {
		config.endpoint = remainder;
		config.port = 9083;
	}

	if (config.endpoint.empty()) {
		throw MetastoreException(MetastoreErrorCode::InvalidConfig, tag,
		                         "HMS endpoint URI has empty host: '" + endpoint + "'");
	}

	return config;
}

} // namespace duckdb
