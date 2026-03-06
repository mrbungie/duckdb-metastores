#pragma once

#include "metastore_types.hpp"

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
// interface.
//===--------------------------------------------------------------------===//
class IMetastoreConnector {
public:
	virtual ~IMetastoreConnector() = default;

	//! Get the namespace this connector is bound to.
	virtual std::string GetNamespace() = 0;

	//! List all tables within the bound namespace.
	virtual MetastoreResult<std::vector<std::string>> ListTables() = 0;

	//! Get full table metadata for a specific table in the bound namespace.
	virtual MetastoreResult<MetastoreTable> GetTable(const std::string &table_name) = 0;

	//! List partition values for a partitioned table in the bound namespace.
	//! @param predicate  Optional filter expression to push down to the metastore.
	//!                   Empty std::string means "all partitions".
	virtual MetastoreResult<std::vector<MetastorePartitionValue>> ListPartitions(const std::string &table_name,
	                                                                             const std::string &predicate = "") = 0;

	//! (Optional) Retrieve table-level statistics if the metastore supports them.
	//! Default implementation returns Unsupported.
	virtual MetastoreResult<MetastoreTableProperties> GetTableStats(const std::string &table_name) {
		return MetastoreResult<MetastoreTableProperties>::Error(MetastoreErrorCode::Unsupported,
		                                                        "GetTableStats not supported by this connector");
	}

	//! Create a new table in the bound namespace.
	virtual MetastoreResult<bool> CreateTable(const MetastoreTable &table) {
		return MetastoreResult<bool>::Error(MetastoreErrorCode::Unsupported,
		                                    "CreateTable not supported by this connector");
	}

	//! Drop an existing table in the bound namespace.
	virtual MetastoreResult<bool> DropTable(const std::string &table_name, bool cascade = false) {
		return MetastoreResult<bool>::Error(MetastoreErrorCode::Unsupported,
		                                    "DropTable not supported by this connector");
	}

	//! Add a partition to a table in the bound namespace.
	virtual MetastoreResult<bool> AddPartition(const std::string &table_name,
	                                           const MetastorePartitionValue &partition) {
		return MetastoreResult<bool>::Error(MetastoreErrorCode::Unsupported,
		                                    "AddPartition not supported by this connector");
	}

	//! Drop a partition from a table in the bound namespace.
	virtual MetastoreResult<bool> DropPartition(const std::string &table_name, const std::vector<std::string> &values) {
		return MetastoreResult<bool>::Error(MetastoreErrorCode::Unsupported,
		                                    "DropPartition not supported by this connector");
	}
};

class IConnectorFactory {
public:
	virtual ~IConnectorFactory() = default;
	virtual bool CanHandle(const ParsedUri &uri) const = 0;
	virtual MetastoreCatalogConfig NormalizeConfig(const std::string &catalog_name, const ParsedUri &uri,
	                                               const case_insensitive_map_t<Value> &options) = 0;
	virtual duckdb::unique_ptr<IMetastoreConnector> CreateConnector(const MetastoreCatalogConfig &config) = 0;
};

} // namespace duckdb
