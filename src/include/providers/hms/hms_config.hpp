#pragma once

#include "duckdb.hpp"

#include <cstdint>
#include <string>

namespace duckdb {

//===--------------------------------------------------------------------===//
// HmsTransport — wire transport for the Thrift connection
//===--------------------------------------------------------------------===//
enum class HmsTransport : uint8_t {
	Thrift = 0,   //! Plain Thrift (no TLS)
	ThriftTLS = 1 //! Thrift over TLS
};

inline const char *HmsTransportToString(HmsTransport transport) {
	switch (transport) {
	case HmsTransport::Thrift:
		return "thrift";
	case HmsTransport::ThriftTLS:
		return "thrift+ssl";
	default:
		return "unknown";
	}
}

//===--------------------------------------------------------------------===//
// HmsConfig — parsed HMS endpoint configuration
//===--------------------------------------------------------------------===//
struct HmsConfig {
	//! Hostname or IP of the HMS Thrift server
	std::string endpoint;
	//! Wire transport (plain Thrift or TLS)
	HmsTransport transport = HmsTransport::Thrift;
	//! Connection timeout in milliseconds
	uint32_t connection_timeout_ms = 30000;
	//! HMS Thrift port (default: 9083)
	uint16_t port = 9083;
};

//===--------------------------------------------------------------------===//
// Internal Helpers for HMS Endpoint Parsing
//===--------------------------------------------------------------------===//

//! Internal helper: validate port string; returns false if invalid
static bool ValidatePort(const std::string &port_str, uint16_t &port_out);

//! Internal helper: strip scheme from HMS URI; returns (transport, remainder)
static std::pair<HmsTransport, std::string> StripScheme(const std::string &endpoint);

//! Internal helper: split host:port string; returns (host, port) or (input, empty) if no colon
static std::pair<std::string, std::string> SplitHostPort(const std::string &remainder);

//===--------------------------------------------------------------------===//
// ParseHmsEndpoint — parse an HMS URI into HmsConfig
HmsConfig ParseHmsEndpoint(const std::string &endpoint);

} // namespace duckdb
