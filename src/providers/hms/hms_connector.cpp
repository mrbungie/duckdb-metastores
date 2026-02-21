#include "hms/hms_connector.hpp"

namespace duckdb {

static const char *HMS_STUB_MESSAGE = "HMS connector not yet implemented — Thrift client pending";

HmsConnector::HmsConnector(HmsConfig config) : config_(std::move(config)) {
}

MetastoreResult<std::vector<MetastoreNamespace>> HmsConnector::ListNamespaces() {
	return MetastoreResult<std::vector<MetastoreNamespace>>::Error(
	    MetastoreErrorCode::Unsupported, HMS_STUB_MESSAGE, "", true);
}

MetastoreResult<std::vector<std::string>> HmsConnector::ListTables(const std::string &namespace_name) {
	return MetastoreResult<std::vector<std::string>>::Error(
	    MetastoreErrorCode::Unsupported, HMS_STUB_MESSAGE, "", true);
}

MetastoreResult<MetastoreTable> HmsConnector::GetTable(const std::string &namespace_name,
                                                       const std::string &table_name) {
	return MetastoreResult<MetastoreTable>::Error(
	    MetastoreErrorCode::Unsupported, HMS_STUB_MESSAGE, "", true);
}

MetastoreResult<std::vector<MetastorePartitionValue>>
HmsConnector::ListPartitions(const std::string &namespace_name, const std::string &table_name,
                             const std::string &predicate) {
	return MetastoreResult<std::vector<MetastorePartitionValue>>::Error(
	    MetastoreErrorCode::Unsupported, HMS_STUB_MESSAGE, "", true);
}

MetastoreResult<MetastoreTableProperties> HmsConnector::GetTableStats(const std::string &namespace_name,
                                                                      const std::string &table_name) {
	return MetastoreResult<MetastoreTableProperties>::Error(
	    MetastoreErrorCode::Unsupported, HMS_STUB_MESSAGE, "", true);
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
	unsigned long val;
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
		throw MetastoreException(MetastoreErrorCode::InvalidConfig, tag,
		                         "HMS endpoint URI is empty");
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
	} else if (endpoint.size() >= thrift_scheme.size() &&
	           endpoint.substr(0, thrift_scheme.size()) == thrift_scheme) {
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
