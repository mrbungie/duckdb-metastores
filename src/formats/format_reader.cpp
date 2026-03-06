#include "formats/format_reader.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

IFormatReader &GetFormatReader(MetastoreFormat format) {
	static ParquetFormatReader parquet_reader;
	static CsvFormatReader csv_reader;
	static JsonFormatReader json_reader;

	switch (format) {
	case MetastoreFormat::Parquet:
		return parquet_reader;
	case MetastoreFormat::CSV:
		return csv_reader;
	case MetastoreFormat::JSON:
		return json_reader;
	default:
		throw BinderException("Unsupported metastore table format");
	}
}

} // namespace duckdb
