#include "providers/hms/hms_config.hpp"
#include "connector/metastore_errors.hpp"

namespace duckdb {

static bool ValidatePort(const std::string &port_str, uint16_t &port_out) {
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

static std::pair<HmsTransport, std::string> StripScheme(const std::string &endpoint) {
	const std::string thrift_ssl_scheme = "thrift+ssl://";
	const std::string thrift_scheme = "thrift://";

	if (endpoint.size() >= thrift_ssl_scheme.size() &&
	    endpoint.substr(0, thrift_ssl_scheme.size()) == thrift_ssl_scheme) {
		return {HmsTransport::ThriftTLS, endpoint.substr(thrift_ssl_scheme.size())};
	} else if (endpoint.size() >= thrift_scheme.size() &&
	           endpoint.substr(0, thrift_scheme.size()) == thrift_scheme) {
		return {HmsTransport::Thrift, endpoint.substr(thrift_scheme.size())};
	} else {
		return {HmsTransport::Thrift, endpoint};
	}
}

static std::pair<std::string, std::string> SplitHostPort(const std::string &remainder) {
	auto colon_pos = remainder.rfind(':');
	if (colon_pos != std::string::npos && colon_pos > 0) {
		std::string host = remainder.substr(0, colon_pos);
		std::string port = remainder.substr(colon_pos + 1);
		return {host, port};
	}
	return {remainder, ""};
}

HmsConfig ParseHmsEndpoint(const std::string &endpoint) {
	MetastoreErrorTag tag {"hms", "ParseHmsEndpoint", false};

	if (endpoint.empty()) {
		throw MetastoreException(MetastoreErrorCode::InvalidConfig, tag, "HMS endpoint URI is empty");
	}

	HmsConfig config;


	auto [transport, remainder] = StripScheme(endpoint);
	config.transport = transport;

	if (remainder.empty()) {
		throw MetastoreException(MetastoreErrorCode::InvalidConfig, tag,
		                         "HMS endpoint URI has no host: '" + endpoint + "'");
	}

	auto [host, port_str] = SplitHostPort(remainder);
	if (port_str.empty()) {
		// No port specified, use default
		config.endpoint = host;
		config.port = 9083;
	} else {
		uint16_t parsed_port;
		if (ValidatePort(port_str, parsed_port)) {
			config.endpoint = host;
			config.port = parsed_port;
		} else {
			throw MetastoreException(MetastoreErrorCode::InvalidConfig, tag,
			                         "Invalid port in HMS endpoint URI: '" + endpoint + "'");
		}
	}

	if (config.endpoint.empty()) {
		throw MetastoreException(MetastoreErrorCode::InvalidConfig, tag,
		                         "HMS endpoint URI has empty host: '" + endpoint + "'");
	}

	return config;
}

} // namespace duckdb
