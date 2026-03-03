#include "providers/mock/mock_metastore.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

MockMetastoreStore::MockMetastoreStore() {
}

string MockMetastoreStore::L(const string &s) {
	return StringUtil::Lower(s);
}

void MockMetastoreStore::CreateSchema(const string &schema_name) {
	if (HasSchema(schema_name)) {
		return;
	}
	MockSchema schema;
	schema.name = schema_name;
	state.schemas.push_back(std::move(schema));
	RebuildSchemaIndex();
}

bool MockMetastoreStore::HasSchema(const string &schema_name) const {
	return schema_idx.count(L(schema_name)) > 0;
}

vector<string> MockMetastoreStore::ListSchemas() const {
	vector<string> names;
	for (auto &s : state.schemas) {
		names.push_back(s.name);
	}
	return names;
}

void MockMetastoreStore::DropSchema(const string &schema_name) {
	if (!HasSchema(schema_name)) {
		return;
	}
	auto s_idx = schema_idx[L(schema_name)];
	state.schemas.erase(state.schemas.begin() + static_cast<ptrdiff_t>(s_idx));

	// Clear entries from table_idx that belong to this schema
	for (auto it = table_idx.begin(); it != table_idx.end();) {
		if (it->first.schema == L(schema_name)) {
			it = table_idx.erase(it);
		} else {
			++it;
		}
	}

	RebuildSchemaIndex();
}

void MockMetastoreStore::CreateTable(const string &schema_name, MockTable table) {
	EnsureSchemaExists(schema_name);
	auto s_idx = schema_idx[L(schema_name)];
	auto &schema = state.schemas[s_idx];

	if (HasTable(schema_name, table.name)) {
		throw InvalidInputException("mock store: table '%s' already exists in schema '%s'", table.name, schema_name);
	}

	schema.tables.push_back(std::move(table));
	RebuildTableIndex(schema_name);
}

bool MockMetastoreStore::HasTable(const string &schema_name, const string &table_name) const {
	TableKey key {L(schema_name), L(table_name)};
	return table_idx.count(key) > 0;
}

const MockTable &MockMetastoreStore::GetTable(const string &schema_name, const string &table_name) const {
	TableKey key {L(schema_name), L(table_name)};
	auto it = table_idx.find(key);
	if (it == table_idx.end()) {
		throw InvalidInputException("mock store: table '%s' not found in schema '%s'", table_name, schema_name);
	}
	auto s_idx = schema_idx.at(L(schema_name));
	return state.schemas[s_idx].tables[it->second];
}

MockTable &MockMetastoreStore::GetTableMutable(const string &schema_name, const string &table_name) {
	TableKey key {L(schema_name), L(table_name)};
	auto it = table_idx.find(key);
	if (it == table_idx.end()) {
		throw InvalidInputException("mock store: table '%s' not found in schema '%s'", table_name, schema_name);
	}
	auto s_idx = schema_idx.at(L(schema_name));
	return state.schemas[s_idx].tables[it->second];
}

vector<string> MockMetastoreStore::ListTables(const string &schema_name) const {
	if (!HasSchema(schema_name)) {
		return {};
	}
	auto s_idx = schema_idx.at(L(schema_name));
	vector<string> names;
	for (auto &tbl : state.schemas[s_idx].tables) {
		names.push_back(tbl.name);
	}
	return names;
}

void MockMetastoreStore::DropTable(const string &schema_name, const string &table_name) {
	if (!HasTable(schema_name, table_name)) {
		return;
	}
	auto s_idx = schema_idx.at(L(schema_name));
	TableKey key {L(schema_name), L(table_name)};
	auto t_pos = table_idx[key];

	auto &schema = state.schemas[s_idx];
	schema.tables.erase(schema.tables.begin() + t_pos);
	RebuildTableIndex(schema_name);
}

void MockMetastoreStore::AddPartition(const string &schema_name, const string &table_name, MockPartition part) {
	auto &tbl = GetTableMutable(schema_name, table_name);
	ValidatePartitionValues(tbl, part.values);

	if (HasPartition(schema_name, table_name, part.values)) {
		throw InvalidInputException("mock store: partition already exists");
	}

	tbl.partitions.push_back(std::move(part));
}

bool MockMetastoreStore::HasPartition(const string &schema_name, const string &table_name,
                                      const vector<string> &values) const {
	auto &tbl = GetTable(schema_name, table_name);
	return FindPartitionIndex(tbl, values) != DConstants::INVALID_INDEX;
}

const MockPartition &MockMetastoreStore::GetPartition(const string &schema_name, const string &table_name,
                                                      const vector<string> &values) const {
	auto &tbl = GetTable(schema_name, table_name);
	auto p_idx = FindPartitionIndex(tbl, values);
	if (p_idx == DConstants::INVALID_INDEX) {
		throw InvalidInputException("mock store: partition not found");
	}
	return tbl.partitions[p_idx];
}

vector<MockPartition> MockMetastoreStore::ListPartitions(const string &schema_name, const string &table_name) const {
	auto &tbl = GetTable(schema_name, table_name);
	return tbl.partitions;
}

void MockMetastoreStore::DropPartition(const string &schema_name, const string &table_name,
                                       const vector<string> &values) {
	auto &tbl = GetTableMutable(schema_name, table_name);
	auto p_idx = FindPartitionIndex(tbl, values);
	if (p_idx != DConstants::INVALID_INDEX) {
		tbl.partitions.erase(tbl.partitions.begin() + p_idx);
	}
}

void MockMetastoreStore::RebuildSchemaIndex() {
	schema_idx.clear();
	for (idx_t i = 0; i < state.schemas.size(); i++) {
		schema_idx[L(state.schemas[i].name)] = i;
	}
}

void MockMetastoreStore::RebuildTableIndex(const string &schema_name) {
	if (!HasSchema(schema_name)) {
		return;
	}
	auto s_idx = schema_idx[L(schema_name)];
	auto &schema = state.schemas[s_idx];

	// Clear only entries for this schema
	for (auto it = table_idx.begin(); it != table_idx.end();) {
		if (it->first.schema == L(schema_name)) {
			it = table_idx.erase(it);
		} else {
			++it;
		}
	}

	for (idx_t i = 0; i < schema.tables.size(); i++) {
		TableKey key {L(schema_name), L(schema.tables[i].name)};
		table_idx[key] = i;
	}
}

void MockMetastoreStore::EnsureSchemaExists(const string &schema_name) {
	if (!HasSchema(schema_name)) {
		CreateSchema(schema_name);
	}
}

void MockMetastoreStore::ValidatePartitionValues(const MockTable &tbl, const vector<string> &values) const {
	if (values.size() != tbl.partition_spec.columns.size()) {
		throw InvalidInputException("mock store: partition values count mismatch. Expected %d, got %d",
		                            tbl.partition_spec.columns.size(), values.size());
	}
}

idx_t MockMetastoreStore::FindPartitionIndex(const MockTable &tbl, const vector<string> &values) const {
	for (idx_t i = 0; i < tbl.partitions.size(); i++) {
		if (tbl.partitions[i].values == values) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

} // namespace duckdb
