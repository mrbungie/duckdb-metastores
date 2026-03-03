#include "metastore_runtime.hpp"
#include "connector/metastore_connector.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/common/exception.hpp"

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

static bool AnyRow(Connection &con, const string &table_ref) {
	auto res = Q(con, "select 1 from " + table_ref + " limit 1");
	return res->RowCount() > 0;
}

// using MetastoreExtraOptions from metastore_types.hpp for state pointers

class MockConnector : public IMetastoreConnector {
public:
	explicit MockConnector(MetastoreExtraOptions ptrs_p) : ptrs(std::move(ptrs_p)) {
		if (ptrs.namespaces_table.empty() || ptrs.tables_table.empty() || ptrs.partitions_table.empty()) {
			throw InvalidInputException("mock provider: missing required table pointers. "
			                            "Provide namespaces_table, tables_table, partitions_table in ATTACH options.");
		}
	}

	MetastoreResult<MetastoreTable> GetTable(const std::string &schema, const std::string &table) override {
		try {
			auto &con = GetRuntimeConnection();

			auto sql = "select location, format, partition_key from " + ptrs.tables_table + " where schema_name = '" +
			           Escape(schema) + "' and table_name = '" + Escape(table) + "' limit 1";
			auto res = Q(con, sql);
			if (res->RowCount() == 0) {
				return MetastoreResult<MetastoreTable>::Error(
				    MetastoreErrorCode::NotFound, "mock provider: table '" + schema + "." + table + "' not found in " +
				                                      ptrs.tables_table + " (Query: " + sql + ")");
			}

			MetastoreTable out;
			out.catalog = "mock_catalog";
			out.namespace_name = schema;
			out.name = table;

			out.storage_descriptor.location = res->GetValue(0, 0).ToString();
			auto fmt = StringUtil::Lower(res->GetValue(1, 0).ToString());
			if (fmt == "parquet") {
				out.storage_descriptor.format = MetastoreFormat::Parquet;
			} else if (fmt == "csv") {
				out.storage_descriptor.format = MetastoreFormat::CSV;
			} else if (fmt == "json") {
				out.storage_descriptor.format = MetastoreFormat::JSON;
			} else {
				out.storage_descriptor.format = MetastoreFormat::Unknown;
			}

			auto pkeys = res->GetValue(2, 0).ToString();
			if (!pkeys.empty()) {
				auto parts = StringUtil::Split(pkeys, ",");
				for (auto &pkey : parts) {
					MetastorePartitionColumn col;
					StringUtil::Trim(pkey);
					col.name = pkey;
					col.type = "string";
					out.partition_spec.columns.push_back(std::move(col));
				}
			}

			// fetch columns if table is provided
			if (!ptrs.columns_table.empty()) {
				auto col_sql = "select column_name, column_type from " + ptrs.columns_table + " where schema_name = '" +
				               Escape(schema) + "' and table_name = '" + Escape(table) + "' order by column_index";
				auto col_res = Q(con, col_sql);
				for (idx_t i = 0; i < col_res->RowCount(); i++) {
					MetastoreColumn col;
					col.name = col_res->GetValue(0, i).ToString();
					col.type = col_res->GetValue(1, i).ToString();
					out.storage_descriptor.columns.push_back(std::move(col));
				}
			}

			return MetastoreResult<MetastoreTable>::Success(std::move(out));
		} catch (const std::exception &ex) {
			return MetastoreResult<MetastoreTable>::Error(MetastoreErrorCode::Transient, ex.what());
		}
	}

	MetastoreResult<std::vector<MetastorePartitionValue>>
	ListPartitions(const std::string &schema, const std::string &table, const std::string &predicate) override {
		(void)predicate;
		try {
			auto &con = GetRuntimeConnection();

			// If there are no rows for that table, return empty partitions.
			auto sql = "select part_value, location from " + ptrs.partitions_table + " where schema_name = '" +
			           Escape(schema) + "' and table_name = '" + Escape(table) + "'";
			auto res = Q(con, sql);

			std::vector<MetastorePartitionValue> out;
			out.reserve(res->RowCount());

			for (idx_t r = 0; r < res->RowCount(); r++) {
				MetastorePartitionValue p;
				auto pvals = StringUtil::Split(res->GetValue(0, r).ToString(), ",");
				for (auto &v : pvals) {
					StringUtil::Trim(v);
					p.values.push_back(v);
				}
				p.location = res->GetValue(1, r).ToString();
				out.push_back(std::move(p));
			}

			return MetastoreResult<std::vector<MetastorePartitionValue>>::Success(std::move(out));
		} catch (const std::exception &ex) {
			return MetastoreResult<std::vector<MetastorePartitionValue>>::Error(MetastoreErrorCode::Transient,
			                                                                    ex.what());
		}
	}

	MetastoreResult<std::vector<MetastoreNamespace>> ListNamespaces() override {
		try {
			auto &con = GetRuntimeConnection();

			auto res = Q(con, "select name from " + ptrs.namespaces_table);

			std::vector<MetastoreNamespace> out;
			out.reserve(res->RowCount());

			for (idx_t r = 0; r < res->RowCount(); r++) {
				MetastoreNamespace ns;
				ns.catalog = "mock_catalog";
				ns.name = res->GetValue(0, r).ToString();
				out.push_back(std::move(ns));
			}

			return MetastoreResult<std::vector<MetastoreNamespace>>::Success(std::move(out));
		} catch (const std::exception &ex) {
			return MetastoreResult<std::vector<MetastoreNamespace>>::Error(MetastoreErrorCode::Transient, ex.what());
		}
	}

	MetastoreResult<std::vector<std::string>> ListTables(const std::string &schema) override {
		try {
			auto &con = GetRuntimeConnection();

			auto sql = "select table_name from " + ptrs.tables_table + " where schema_name = '" + Escape(schema) + "'";
			auto res = Q(con, sql);

			std::vector<std::string> out;
			out.reserve(res->RowCount());

			for (idx_t r = 0; r < res->RowCount(); r++) {
				out.push_back(res->GetValue(0, r).ToString());
			}

			return MetastoreResult<std::vector<std::string>>::Success(std::move(out));
		} catch (const std::exception &ex) {
			return MetastoreResult<std::vector<std::string>>::Error(MetastoreErrorCode::Transient, ex.what());
		}
	}

private:
	MetastoreExtraOptions ptrs;

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
};

class MockConnectorFactory : public IConnectorFactory {
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
		return duckdb::make_uniq<MockConnector>(config.extra_options);
	}
};

void RegisterMockProvider() {
	ProviderRegistry::Register(duckdb::make_uniq<MockConnectorFactory>());
}

} // namespace duckdb
