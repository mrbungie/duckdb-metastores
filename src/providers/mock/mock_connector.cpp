#include "metastore_runtime.hpp"
#include "connector/metastore_connector.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/common/exception.hpp"
#include "providers/mock/mock_metastore.hpp"

namespace duckdb {

// Mode 2 only: state is stored in DuckDB tables. The options point to those tables.
// The values are SQL table references like:
//   temp.mock_ms_namespaces
//   temp.mock_ms_tables
//   temp.mock_ms_partitions
//
// Required schemas:
//
// namespaces table:
//   (name VARCHAR)
//
// tables table:
//   (schema_name VARCHAR, table_name VARCHAR, location VARCHAR, format VARCHAR, partition_key VARCHAR)
//
// partitions table:
//   (schema_name VARCHAR, table_name VARCHAR, part_value VARCHAR, location VARCHAR)
//
// columns table (optional):
//   (schema_name VARCHAR, table_name VARCHAR, column_name VARCHAR, column_type VARCHAR, column_index INTEGER)

static Connection &GetRuntimeConnection() {
	return MetastoreRuntime::GetConnection();
}

static unique_ptr<MaterializedQueryResult> Q(Connection &con, const string &sql) {
	auto res = con.Query(sql);
	if (!res) {
		throw InternalException("mock provider: query returned null");
	}
	if (res->HasError()) {
		throw InternalException("mock provider: query error: %s", res->GetError());
	}
	return unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(res));
}

// using MetastoreExtraOptions from metastore_types.hpp for state pointers

class MockMetastoreConnector : public IMetastoreConnector {
private:
	MetastoreExtraOptions ptrs;
	MockMetastoreStore store;
	string bound_namespace;

	// minimal escaping for test usage (single quotes)
	static string Escape(const string &s) {
		string out;
		out.reserve(s.size());
		for (auto c : s) {
			if (c == '\'') {
				out.push_back('\'');
				out.push_back('\'');
			} else {
				out.push_back(c);
			}
		}
		return out;
	}

	void LoadFromTables() {
		auto &con = GetRuntimeConnection();

		// 1. Load Schemas/Namespaces
		auto ns_res = Q(con, "SELECT name FROM " + ptrs.namespaces_table);
		for (idx_t i = 0; i < ns_res->RowCount(); i++) {
			store.CreateSchema(ns_res->GetValue(0, i).ToString());
		}

		// 2. Load Tables
		auto tbl_res =
		    Q(con, "SELECT schema_name, table_name, location, format, partition_key FROM " + ptrs.tables_table);
		for (idx_t i = 0; i < tbl_res->RowCount(); i++) {
			auto schema = tbl_res->GetValue(0, i).ToString();
			MockTable tbl;
			tbl.name = tbl_res->GetValue(1, i).ToString();
			tbl.storage.location = tbl_res->GetValue(2, i).ToString();
			auto fmt = StringUtil::Lower(tbl_res->GetValue(3, i).ToString());
			if (fmt == "parquet") {
				tbl.storage.format = MockFormat::Parquet;
			} else if (fmt == "csv") {
				tbl.storage.format = MockFormat::Csv;
			} else if (fmt == "json") {
				tbl.storage.format = MockFormat::Json;
			} else {
				tbl.storage.format = MockFormat::Unknown;
			}

			auto pkeys = tbl_res->GetValue(4, i).ToString();
			if (!pkeys.empty()) {
				auto parts = StringUtil::Split(pkeys, ",");
				for (auto &pkey : parts) {
					StringUtil::Trim(pkey);
					MockPartitionColumn col;
					col.name = pkey;
					col.type = "string";
					tbl.partition_spec.columns.push_back(std::move(col));
				}
			}

			// Load columns if available
			if (!ptrs.columns_table.empty()) {
				auto col_sql = "SELECT column_name, column_type FROM " + ptrs.columns_table + " WHERE schema_name = '" +
				               Escape(schema) + "' AND table_name = '" + Escape(tbl.name) + "' ORDER BY column_index";
				auto col_res = Q(con, col_sql);
				for (idx_t j = 0; j < col_res->RowCount(); j++) {
					MockColumn col;
					col.name = col_res->GetValue(0, j).ToString();
					col.type = col_res->GetValue(1, j).ToString();
					tbl.columns.push_back(std::move(col));
				}
			}

			store.CreateTable(schema, std::move(tbl));
		}

		// 3. Load Partitions
		auto part_res = Q(con, "SELECT schema_name, table_name, part_value, location FROM " + ptrs.partitions_table);
		for (idx_t i = 0; i < part_res->RowCount(); i++) {
			auto schema = part_res->GetValue(0, i).ToString();
			auto table = part_res->GetValue(1, i).ToString();

			MockPartition part;
			auto pvals = StringUtil::Split(part_res->GetValue(2, i).ToString(), ",");
			for (auto &v : pvals) {
				StringUtil::Trim(v);
				part.values.push_back(v);
			}
			part.location = part_res->GetValue(3, i).ToString();

			store.AddPartition(schema, table, std::move(part));
		}
	}

public:
	explicit MockMetastoreConnector(string ns, MetastoreExtraOptions ptrs_p)
	    : ptrs(std::move(ptrs_p)), bound_namespace(std::move(ns)) {
		if (ptrs.namespaces_table.empty() || ptrs.tables_table.empty() || ptrs.partitions_table.empty()) {
			throw InvalidInputException("mock provider: missing required table pointers. "
			                            "Provide namespaces_table, tables_table, partitions_table in ATTACH options.");
		}
		LoadFromTables();
	}

	string GetNamespace() override {
		return bound_namespace;
	}

	MetastoreResult<MetastoreTable> GetTable(const std::string &table_name) override {
		try {
			if (!store.HasTable(bound_namespace, table_name)) {
				// Re-load once if not found, in case it was created by another connector instance
				LoadFromTables();
			}
			if (!store.HasTable(bound_namespace, table_name)) {
				return MetastoreResult<MetastoreTable>::Error(MetastoreErrorCode::NotFound,
				                                              "mock provider: table '" + bound_namespace + "." +
				                                                  table_name + "' not found");
			}
			auto &mock_table = store.GetTable(bound_namespace, table_name);
			MetastoreTable out;
			out.catalog = "mock_catalog";
			out.namespace_name = bound_namespace;
			out.name = mock_table.name;
			out.storage_descriptor.location = mock_table.storage.location;

			switch (mock_table.storage.format) {
			case MockFormat::Parquet:
				out.storage_descriptor.format = MetastoreFormat::Parquet;
				break;
			case MockFormat::Csv:
				out.storage_descriptor.format = MetastoreFormat::CSV;
				break;
			case MockFormat::Json:
				out.storage_descriptor.format = MetastoreFormat::JSON;
				break;
			default:
				out.storage_descriptor.format = MetastoreFormat::Unknown;
				break;
			}

			for (auto &col : mock_table.columns) {
				MetastoreColumn mc;
				mc.name = col.name;
				mc.type = col.type;
				out.storage_descriptor.columns.push_back(std::move(mc));
			}

			for (auto &pcol : mock_table.partition_spec.columns) {
				MetastorePartitionColumn mpc;
				mpc.name = pcol.name;
				mpc.type = pcol.type;
				out.partition_spec.columns.push_back(std::move(mpc));
			}

			for (auto &prop : mock_table.properties) {
				out.properties[prop.first] = prop.second;
			}

			return MetastoreResult<MetastoreTable>::Success(std::move(out));
		} catch (const std::exception &ex) {
			return MetastoreResult<MetastoreTable>::Error(MetastoreErrorCode::Transient, ex.what());
		}
	}

	MetastoreResult<std::vector<MetastorePartitionValue>> ListPartitions(const std::string &table,
	                                                                     const std::string &predicate) override {
		(void)predicate;
		try {
			if (!store.HasTable(bound_namespace, table)) {
				LoadFromTables();
			}
			if (!store.HasTable(bound_namespace, table)) {
				return MetastoreResult<std::vector<MetastorePartitionValue>>::Success({});
			}
			auto partitions = store.ListPartitions(bound_namespace, table);
			std::vector<MetastorePartitionValue> out;
			for (auto &p : partitions) {
				MetastorePartitionValue mp;
				for (auto &v : p.values) {
					mp.values.push_back(v);
				}
				mp.location = p.location;
				out.push_back(std::move(mp));
			}
			return MetastoreResult<std::vector<MetastorePartitionValue>>::Success(std::move(out));
		} catch (const std::exception &ex) {
			return MetastoreResult<std::vector<MetastorePartitionValue>>::Error(MetastoreErrorCode::Transient,
			                                                                    ex.what());
		}
	}

	MetastoreResult<std::vector<std::string>> ListTables() override {
		try {
			auto names = store.ListTables(bound_namespace);
			return MetastoreResult<std::vector<std::string>>::Success(std::move(names));
		} catch (const std::exception &ex) {
			return MetastoreResult<std::vector<std::string>>::Error(MetastoreErrorCode::Transient, ex.what());
		}
	}

	MetastoreResult<MetastoreTableProperties> GetTableStats(const std::string &table_name) override {
		try {
			if (!store.HasTable(bound_namespace, table_name)) {
				LoadFromTables();
			}
			if (!store.HasTable(bound_namespace, table_name)) {
				return MetastoreResult<MetastoreTableProperties>::Error(MetastoreErrorCode::NotFound,
				                                                      "mock provider: table '" + bound_namespace + "." +
				                                                          table_name + "' not found");
			}

			auto &table = store.GetTable(bound_namespace, table_name);
			MetastoreTableProperties props;

			if (table.has_statistics && table.statistics.has_row_count) {
				props["row_count"] = std::to_string(table.statistics.row_count);
			}
			if (table.has_statistics && table.statistics.has_total_size) {
				props["total_size_bytes"] = std::to_string(table.statistics.total_size_bytes);
			}

			auto row_count_it = table.properties.find("row_count");
			if (props.find("row_count") == props.end() && row_count_it != table.properties.end() && !row_count_it->second.empty()) {
				props["row_count"] = row_count_it->second;
			}
			auto total_size_it = table.properties.find("total_size_bytes");
			if (props.find("total_size_bytes") == props.end() && total_size_it != table.properties.end() &&
			    !total_size_it->second.empty()) {
				props["total_size_bytes"] = total_size_it->second;
			}

			if (!table.partition_spec.columns.empty()) {
				props["partition_count"] = std::to_string(table.partitions.size());
			}

			if (props.empty()) {
				return MetastoreResult<MetastoreTableProperties>::Error(MetastoreErrorCode::Unsupported,
				                                                      "mock provider: table statistics not available");
			}

			return MetastoreResult<MetastoreTableProperties>::Success(std::move(props));
		} catch (const std::exception &ex) {
			return MetastoreResult<MetastoreTableProperties>::Error(MetastoreErrorCode::Transient, ex.what());
		}
	}

	MetastoreResult<bool> CreateTable(const MetastoreTable &table) override {
		try {
			MockTable mt;
			mt.name = table.name;
			mt.storage.location = table.storage_descriptor.location;
			switch (table.storage_descriptor.format) {
			case MetastoreFormat::Parquet:
				mt.storage.format = MockFormat::Parquet;
				break;
			case MetastoreFormat::JSON:
				mt.storage.format = MockFormat::Json;
				break;
			case MetastoreFormat::CSV:
				mt.storage.format = MockFormat::Csv;
				break;
			default:
				mt.storage.format = MockFormat::Unknown;
				break;
			}

			for (auto &col : table.storage_descriptor.columns) {
				MockColumn mc;
				mc.name = col.name;
				mc.type = col.type;
				mt.columns.push_back(std::move(mc));
			}

			for (auto &pcol : table.partition_spec.columns) {
				MockPartitionColumn mpc;
				mpc.name = pcol.name;
				mpc.type = pcol.type;
				mt.partition_spec.columns.push_back(std::move(mpc));
			}

			for (auto &prop : table.properties) {
				mt.properties[prop.first] = prop.second;
			}

			store.CreateTable(bound_namespace, std::move(mt));

			// Persist to underlying tables
			auto &con = GetRuntimeConnection();
			string fmt_str = "parquet";
			if (table.storage_descriptor.format == MetastoreFormat::CSV) {
				fmt_str = "csv";
			} else if (table.storage_descriptor.format == MetastoreFormat::JSON) {
				fmt_str = "json";
			}

			string pkeys;
			for (idx_t i = 0; i < table.partition_spec.columns.size(); i++) {
				if (i > 0) {
					pkeys += ",";
				}
				pkeys += table.partition_spec.columns[i].name;
			}

			string tbl_sql = "INSERT INTO " + ptrs.tables_table + " VALUES ('" + Escape(bound_namespace) + "', '" +
			                 Escape(table.name) + "', '" + Escape(table.storage_descriptor.location) + "', '" +
			                 fmt_str + "', '" + Escape(pkeys) + "')";
			Q(con, tbl_sql);

			if (!ptrs.columns_table.empty()) {
				for (idx_t i = 0; i < table.storage_descriptor.columns.size(); i++) {
					auto &col = table.storage_descriptor.columns[i];
					string col_sql = "INSERT INTO " + ptrs.columns_table + " VALUES ('" + Escape(bound_namespace) +
					                 "', '" + Escape(table.name) + "', '" + Escape(col.name) + "', '" +
					                 Escape(col.type) + "', " + std::to_string(i) + ")";
					Q(con, col_sql);
				}
			}

			return MetastoreResult<bool>::Success(true);
		} catch (const std::exception &ex) {
			return MetastoreResult<bool>::Error(MetastoreErrorCode::Transient, ex.what());
		}
	}

	MetastoreResult<bool> DropTable(const std::string &table_name, bool cascade = false) override {
		try {
			store.DropTable(bound_namespace, table_name);
			return MetastoreResult<bool>::Success(true);
		} catch (const std::exception &ex) {
			return MetastoreResult<bool>::Error(MetastoreErrorCode::Transient, ex.what());
		}
	}

	MetastoreResult<bool> AddPartition(const std::string &table_name,
	                                   const MetastorePartitionValue &partition) override {
		try {
			MockPartition mp;
			for (auto &v : partition.values) {
				mp.values.push_back(v);
			}
			mp.location = partition.location;
			store.AddPartition(bound_namespace, table_name, std::move(mp));

			// Persist to underlying tables
			auto &con = GetRuntimeConnection();
			string pvals;
			for (idx_t i = 0; i < partition.values.size(); i++) {
				if (i > 0) {
					pvals += ",";
				}
				pvals += partition.values[i];
			}
			string part_sql = "INSERT INTO " + ptrs.partitions_table + " VALUES ('" + Escape(bound_namespace) + "', '" +
			                  Escape(table_name) + "', '" + Escape(pvals) + "', '" + Escape(partition.location) + "')";
			Q(con, part_sql);

			return MetastoreResult<bool>::Success(true);
		} catch (const std::exception &ex) {
			return MetastoreResult<bool>::Error(MetastoreErrorCode::Transient, ex.what());
		}
	}

	MetastoreResult<bool> DropPartition(const std::string &table_name,
	                                    const std::vector<std::string> &values) override {
		try {
			vector<string> duckdb_values;
			for (auto &v : values) {
				duckdb_values.push_back(v);
			}
			store.DropPartition(bound_namespace, table_name, duckdb_values);
			return MetastoreResult<bool>::Success(true);
		} catch (const std::exception &ex) {
			return MetastoreResult<bool>::Error(MetastoreErrorCode::Transient, ex.what());
		}
	}
};

class MockMetastoreConnectorFactory : public IConnectorFactory {
public:
	bool CanHandle(const ParsedUri &uri) const override {
		return uri.scheme == "mock";
	}

	MetastoreCatalogConfig NormalizeConfig(const std::string &catalog_name, const ParsedUri &uri,
	                                       const case_insensitive_map_t<Value> &options) override {
		MetastoreCatalogConfig config;
		config.catalog_name = catalog_name;
		config.endpoint = uri.ToString();
		config.provider = MetastoreProviderType::Unknown;
		config.options = options;

		// Extract namespace from path
		string ns = uri.path;
		if (ns.empty() || ns == "/") {
			ns = "default";
		} else if (ns[0] == '/') {
			ns = ns.substr(1);
		}
		config.extra_params["namespace"] = ns;

		auto it = options.find("extra_options");
		if (it == options.end() || it->second.type().id() != LogicalTypeId::STRUCT) {
			throw InvalidInputException("mock provider: extra_options (STRUCT) is required in ATTACH options. "
			                            "Example: extra_options {namespaces_table: '...', tables_table: '...', "
			                            "partitions_table: '...'}");
		}

		auto &struct_val = it->second;
		auto &children = StructValue::GetChildren(struct_val);
		auto &stype = struct_val.type();

		for (idx_t i = 0; i < children.size(); i++) {
			auto name = StringUtil::Lower(StructType::GetChildName(stype, i));
			if (name == "namespaces_table") {
				config.extra_options.namespaces_table = children[i].ToString();
			} else if (name == "tables_table") {
				config.extra_options.tables_table = children[i].ToString();
			} else if (name == "partitions_table") {
				config.extra_options.partitions_table = children[i].ToString();
			} else if (name == "columns_table") {
				config.extra_options.columns_table = children[i].ToString();
			}
		}

		if (config.extra_options.namespaces_table.empty() || config.extra_options.tables_table.empty() ||
		    config.extra_options.partitions_table.empty()) {
			throw InvalidInputException("mock provider: extra_options struct must contain namespaces_table, "
			                            "tables_table, and partitions_table.");
		}

		return config;
	}

	duckdb::unique_ptr<IMetastoreConnector> CreateConnector(const MetastoreCatalogConfig &config) override {
		string ns = "default";
		auto it = config.extra_params.find("namespace");
		if (it != config.extra_params.end()) {
			ns = it->second;
		}
		return duckdb::make_uniq<MockMetastoreConnector>(ns, config.extra_options);
	}
};

void RegisterMockProvider() {
	ProviderRegistry::Register(duckdb::make_uniq<MockMetastoreConnectorFactory>());
}

} // namespace duckdb
