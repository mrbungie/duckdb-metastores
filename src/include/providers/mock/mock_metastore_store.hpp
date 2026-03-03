#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

// Keep this aligned with your MetastoreFormat enum
enum class MockFormat : uint8_t { Parquet, Csv, Json, Orc, Delta, Iceberg, Unknown };

struct MockColumn {
	string name;
	string type; // keep as string to match HMS/Glue style
	bool nullable = true;
	string comment;
};

struct MockPartitionColumn {
	string name;
	string type;
};

struct MockPartitionSpec {
	vector<MockPartitionColumn> columns; // canonical order
};

struct MockStorageDescriptor {
	string location; // root table location
	MockFormat format = MockFormat::Unknown;
	string serde;
	string compression;
};

struct MockFileStatistics {
	// Key is column name, value is string-serialized to keep it simple
	unordered_map<string, string> min_values;
	unordered_map<string, string> max_values;
	unordered_map<string, idx_t> null_counts;
};

struct MockFileEntry {
	string path;
	idx_t file_size = 0;
	idx_t row_count = 0;
	shared_ptr<MockFileStatistics> stats; // optional, can be null
};

struct MockTableStatistics {
	idx_t row_count = 0;
	idx_t total_size_bytes = 0;
	bool has_row_count = false;
	bool has_total_size = false;
};

struct MockPartitionStatistics {
	idx_t row_count = 0;
	idx_t total_size_bytes = 0;
	bool has_row_count = false;
	bool has_total_size = false;
};

struct MockPartition {
	vector<string> values; // ordered list, matches partition_spec.columns
	string location;

	vector<MockFileEntry> files;

	MockPartitionStatistics statistics;
	bool has_statistics = false;

	string created_at_iso; // optional
};

struct MockTable {
	string name;

	vector<MockColumn> columns;
	MockPartitionSpec partition_spec;
	MockStorageDescriptor storage;

	unordered_map<string, string> properties;

	MockTableStatistics statistics;
	bool has_statistics = false;

	vector<MockPartition> partitions; // empty means unpartitioned

	string created_at_iso; // optional
};

struct MockSchema {
	string name;
	unordered_map<string, string> properties;
	vector<MockTable> tables;
};

struct MockMetastoreState {
	string version; // optional
	vector<MockSchema> schemas;
};

// A simple in-memory store with CRUD and indexes.
// This is the "metastore" behind your mock connector.
class MockMetastoreStore {
public:
	MockMetastoreStore();

	// Schema operations
	void CreateSchema(const string &schema_name);
	bool HasSchema(const string &schema_name) const;
	vector<string> ListSchemas() const;

	// Table operations
	void CreateTable(const string &schema_name, MockTable table);
	bool HasTable(const string &schema_name, const string &table_name) const;
	const MockTable &GetTable(const string &schema_name, const string &table_name) const;
	MockTable &GetTableMutable(const string &schema_name, const string &table_name);
	vector<string> ListTables(const string &schema_name) const;
	void DropTable(const string &schema_name, const string &table_name);

	// Partition operations (by values in canonical order)
	void AddPartition(const string &schema_name, const string &table_name, MockPartition part);
	bool HasPartition(const string &schema_name, const string &table_name, const vector<string> &values) const;
	const MockPartition &GetPartition(const string &schema_name, const string &table_name,
	                                  const vector<string> &values) const;
	vector<MockPartition> ListPartitions(const string &schema_name, const string &table_name) const;
	void DropPartition(const string &schema_name, const string &table_name, const vector<string> &values);

private:
	// Indexes for fast lookup, keys are lowercased
	struct TableKey {
		string schema;
		string table;
		bool operator==(const TableKey &o) const {
			return schema == o.schema && table == o.table;
		}
	};

	struct TableKeyHash {
		size_t operator()(const TableKey &k) const {
			return std::hash<string>()(k.schema) ^ (std::hash<string>()(k.table) << 1);
		}
	};

	MockMetastoreState state;

	// Indices map to positions inside state.schemas / schema.tables / table.partitions
	unordered_map<string, idx_t> schema_idx;                // schema -> index in state.schemas
	unordered_map<TableKey, idx_t, TableKeyHash> table_idx; // (schema, table) -> table index within schema.tables

	// Helpers
	static string L(const string &s);
	void RebuildSchemaIndex();
	void RebuildTableIndex(const string &schema_name);
	void EnsureSchemaExists(const string &schema_name);
	void ValidatePartitionValues(const MockTable &tbl, const vector<string> &values) const;
	idx_t FindPartitionIndex(const MockTable &tbl, const vector<string> &values) const;
};

} // namespace duckdb
