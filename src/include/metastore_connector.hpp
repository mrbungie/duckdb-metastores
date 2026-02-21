#pragma once

#include "metastore_types.hpp"

#include <memory>
#include <string>
#include <vector>

namespace duckdb {

//===--------------------------------------------------------------------===//
// MetastoreErrorCode — error classification for connector operations
//===--------------------------------------------------------------------===//
enum class MetastoreErrorCode : int32_t {
	Ok = 0,
	NotFound = 1,
	PermissionDenied = 2,
	Transient = 3,
	InvalidConfig = 4,
	Unsupported = 5
};

//===--------------------------------------------------------------------===//
// MetastoreResult<T> — result-or-error envelope for connector operations
//===--------------------------------------------------------------------===//
struct MetastoreError {
	MetastoreErrorCode code;
	std::string message;
	std::string detail;
	bool retryable;

	MetastoreError() : code(MetastoreErrorCode::Ok), retryable(false) {
	}
	MetastoreError(MetastoreErrorCode code_p, std::string message_p, std::string detail_p = "",
	               bool retryable_p = false)
	    : code(code_p), message(std::move(message_p)), detail(std::move(detail_p)), retryable(retryable_p) {
	}

	bool IsOk() const {
		return code == MetastoreErrorCode::Ok;
	}
};

template <typename T>
struct MetastoreResult {
	T value;
	MetastoreError error;

	//! Check whether the operation succeeded
	bool IsOk() const {
		return error.IsOk();
	}

	//! Construct a success result
	static MetastoreResult Success(T val) {
		MetastoreResult r;
		r.value = std::move(val);
		return r;
	}

	//! Construct an error result
	static MetastoreResult Error(MetastoreErrorCode code, std::string message, std::string detail = "",
	                             bool retryable = false) {
		MetastoreResult r;
		r.error = MetastoreError(code, std::move(message), std::move(detail), retryable);
		return r;
	}
};

//===--------------------------------------------------------------------===//
// IMetastoreConnector — abstract interface for metastore backends
//
// All metastore providers (HMS, Glue, Dataproc, etc.) implement this
// interface. The C++ side consumes it; the Rust side produces it through
// the FFI bridge in bridge_ffi.cpp / metastore_ffi.h.
//===--------------------------------------------------------------------===//
class IMetastoreConnector {
public:
	virtual ~IMetastoreConnector() = default;

	//! List all namespaces (databases/schemas) available in the metastore.
	virtual MetastoreResult<std::vector<MetastoreNamespace>> ListNamespaces() = 0;

	//! List all tables within a given namespace.
	virtual MetastoreResult<std::vector<std::string>> ListTables(const std::string &namespace_name) = 0;

	//! Get full table metadata for a specific table.
	virtual MetastoreResult<MetastoreTable> GetTable(const std::string &namespace_name,
	                                                 const std::string &table_name) = 0;

	//! List partition values for a partitioned table.
	//! @param predicate  Optional filter expression to push down to the metastore.
	//!                   Empty string means "all partitions".
	virtual MetastoreResult<std::vector<MetastorePartitionValue>>
	ListPartitions(const std::string &namespace_name, const std::string &table_name,
	               const std::string &predicate = "") = 0;

	//! (Optional) Retrieve table-level statistics if the metastore supports them.
	//! Default implementation returns Unsupported.
	virtual MetastoreResult<MetastoreTableProperties> GetTableStats(const std::string &namespace_name,
	                                                                const std::string &table_name) {
		return MetastoreResult<MetastoreTableProperties>::Error(MetastoreErrorCode::Unsupported,
		                                                       "GetTableStats not supported by this connector");
	}
};

} // namespace duckdb
