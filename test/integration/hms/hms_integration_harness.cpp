#include "hms/hms_config.hpp"
#include "hms/hms_connector.hpp"
#include "hms/hms_mapper.hpp"
#include "hms/hms_retry.hpp"

#include <iostream>
#include <string>
#include <utility>

namespace {

using namespace duckdb;

void Assert(bool condition, const std::string &message) {
	if (!condition) {
		std::cerr << "[FAIL] " << message << std::endl;
		std::exit(1);
	}
}

void TestEndpointParsing() {
	auto config = ParseHmsEndpoint("thrift://localhost:9083");
	Assert(config.endpoint == "localhost", "endpoint host should parse");
	Assert(config.port == 9083, "endpoint port should parse");
	Assert(config.transport == HmsTransport::Thrift, "endpoint transport should parse");

	auto tls_config = ParseHmsEndpoint("thrift+ssl://hms.example.com:10000");
	Assert(tls_config.endpoint == "hms.example.com", "tls endpoint host should parse");
	Assert(tls_config.port == 10000, "tls endpoint port should parse");
	Assert(tls_config.transport == HmsTransport::ThriftTLS, "tls endpoint transport should parse");

	bool invalid_error = false;
	try {
		(void)ParseHmsEndpoint("thrift://:9083");
	} catch (const MetastoreException &ex) {
		invalid_error = ex.GetErrorCode() == MetastoreErrorCode::InvalidConfig;
	}
	Assert(invalid_error, "invalid endpoint must raise InvalidConfig");
}

void TestMapperBehavior() {
	MetastorePartitionSpec heavy_partition_spec;
	for (int i = 0; i < 64; i++) {
		MetastorePartitionColumn col;
		col.name = "p" + std::to_string(i);
		col.type = "string";
		heavy_partition_spec.columns.push_back(std::move(col));
	}

	MetastoreStorageDescriptor parquet_sd;
	parquet_sd.location = "s3://warehouse/db/table";
	parquet_sd.input_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
	auto parquet_result =
	    HmsMapper::MapTable("main", "db", "parquet_tbl", std::move(parquet_sd), std::move(heavy_partition_spec), {});
	Assert(parquet_result.IsOk(), "parquet mapping should succeed");
	Assert(parquet_result.value.storage_descriptor.format == MetastoreFormat::Parquet,
	       "parquet format should be detected");
	Assert(parquet_result.value.partition_spec.columns.size() == 64,
	       "partition-heavy fixture should preserve partition columns");
	Assert(parquet_result.value.IsPartitioned(), "partition-heavy fixture should remain partitioned");

	MetastoreStorageDescriptor orc_sd;
	orc_sd.location = "s3://warehouse/db/orc_tbl";
	orc_sd.serde_class = "org.apache.hadoop.hive.ql.io.orc.OrcSerde";
	auto orc_result = HmsMapper::MapTable("main", "db", "orc_tbl", std::move(orc_sd), {}, {});
	Assert(orc_result.IsOk(), "orc mapping should succeed");
	Assert(orc_result.value.storage_descriptor.format == MetastoreFormat::ORC, "orc format should be detected");

	MetastoreStorageDescriptor missing_location_sd;
	missing_location_sd.serde_class = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
	auto missing_location_result =
	    HmsMapper::MapTable("main", "db", "missing_loc", std::move(missing_location_sd), {}, {});
	Assert(!missing_location_result.IsOk(), "missing location should fail");
	Assert(missing_location_result.error.code == MetastoreErrorCode::InvalidConfig,
	       "missing location must return InvalidConfig");

	MetastoreStorageDescriptor unknown_sd;
	unknown_sd.location = "s3://warehouse/db/unknown";
	unknown_sd.serde_class = "com.example.UnknownSerde";
	auto unknown_result = HmsMapper::MapTable("main", "db", "unknown_tbl", std::move(unknown_sd), {}, {});
	Assert(!unknown_result.IsOk(), "unknown serde should fail");
	Assert(unknown_result.error.code == MetastoreErrorCode::Unsupported,
	       "unknown serde must return Unsupported");
	Assert(unknown_result.error.retryable == false, "unknown serde must be non-retryable");
}

void TestRetryPolicy() {
	HmsRetryPolicy retry;
	retry.max_attempts = 4;
	retry.initial_delay_ms = 100;
	retry.max_delay_ms = 450;
	retry.backoff_multiplier = 2.0;

	Assert(retry.ComputeDelay(0) == 0, "attempt zero should not retry");
	Assert(retry.ComputeDelay(1) == 100, "attempt 1 delay mismatch");
	Assert(retry.ComputeDelay(2) == 200, "attempt 2 delay mismatch");
	Assert(retry.ComputeDelay(3) == 400, "attempt 3 delay mismatch");
	Assert(retry.ComputeDelay(4) == 0, "attempt 4 should exceed retry budget");
	Assert(retry.ShouldRetry(1), "attempt 1 should allow retry");
	Assert(retry.ShouldRetry(3), "attempt 3 should allow retry");
	Assert(!retry.ShouldRetry(4), "attempt 4 should not allow retry");
}

void TestConnectorStubContract() {
	HmsConfig config;
	config.endpoint = "localhost";
	config.port = 9083;
	HmsConnector connector(config);

	auto ns_result = connector.ListNamespaces();
	Assert(!ns_result.IsOk(), "stub ListNamespaces should fail");
	Assert(ns_result.error.code == MetastoreErrorCode::Unsupported, "stub should return Unsupported");
	Assert(ns_result.error.retryable, "stub error should be retryable");
}

}

int main() {
	TestEndpointParsing();
	TestMapperBehavior();
	TestRetryPolicy();
	TestConnectorStubContract();
	std::cout << "[PASS] HMS integration harness checks completed" << std::endl;
	return 0;
}
