#pragma once

#include "metastore_errors.hpp"

#include <cstdint>
#include <string>

namespace duckdb {

//===--------------------------------------------------------------------===//
// HmsTransport — wire transport for the Thrift connection
//===--------------------------------------------------------------------===//
enum class HmsTransport : uint8_t {
	Thrift = 0,    //! Plain Thrift (no TLS)
	ThriftTLS = 1  //! Thrift over TLS
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
// ParseHmsEndpoint — parse an HMS URI into HmsConfig
//
// Supported URI forms:
//   thrift://hostname:9083       -> Thrift transport
//   thrift+ssl://hostname:9083   -> ThriftTLS transport
//   hostname:9083                -> bare host:port, defaults to Thrift
//   hostname                     -> bare host, defaults to Thrift + port 9083
//
// Throws MetastoreException with InvalidConfig on malformed URI.
//===--------------------------------------------------------------------===//
HmsConfig ParseHmsEndpoint(const std::string &endpoint);

} // namespace duckdb
