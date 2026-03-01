#pragma once

#include "metastore_connector.hpp"
#include "duckdb.hpp"
#include <stdexcept>
#include <string>

namespace duckdb {

inline const char *MetastoreErrorCodeToString(MetastoreErrorCode code) {
	switch (code) {
	case MetastoreErrorCode::Ok:
		return "Ok";
	case MetastoreErrorCode::NotFound:
		return "NotFound";
	case MetastoreErrorCode::PermissionDenied:
		return "PermissionDenied";
	case MetastoreErrorCode::Transient:
		return "Transient";
	case MetastoreErrorCode::InvalidConfig:
		return "InvalidConfig";
	case MetastoreErrorCode::Unsupported:
		return "Unsupported";
	default:
		return "Unknown";
	}
}

//! Diagnostic information (redacted, safe for logging)
struct MetastoreDiagnosticInfo {
	//! Redacted provider type (e.g., "HMS", "Glue", "Dataproc")
	std::string provider_type;
	//! Endpoint mode (e.g., "thrift", "rest")
	std::string endpoint_mode;
	//! Auth strategy class (e.g., "StaticKeys", "Chain", "AssumeRole")
	std::string auth_strategy_class;
};

//! Structured error tag for context
struct MetastoreErrorTag {
	//! Which provider: "hms", "glue", "dataproc"
	std::string provider;
	//! Which operation: "ListNamespaces", "GetTable", "ListTables", etc.
	std::string operation;
	//! Whether the error is potentially transient and safe to retry
	bool retryable = false;
};

//! Exception class for metastore operations
class MetastoreException : public std::runtime_error {
public:
	MetastoreException(MetastoreErrorCode code, const MetastoreErrorTag &tag, const std::string &message)
	    : std::runtime_error(message), error_code_(code), error_tag_(tag) {
	}

	MetastoreErrorCode GetErrorCode() const {
		return error_code_;
	}

	const MetastoreErrorTag &GetErrorTag() const {
		return error_tag_;
	}

private:
	MetastoreErrorCode error_code_;
	MetastoreErrorTag error_tag_;
};

//! Helper function to throw a metastore error
inline void throw_metastore_error(MetastoreErrorCode code, const MetastoreErrorTag &tag, const std::string &message) {
	throw MetastoreException(code, tag, message);
}

} // namespace duckdb
