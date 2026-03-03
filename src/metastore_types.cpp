#include "metastore_types.hpp"

namespace duckdb {

ParsedUri ParsedUri::Parse(const std::string &uri) {
	ParsedUri result;
	auto scheme_pos = uri.find("://");
	if (scheme_pos != std::string::npos) {
		result.scheme = uri.substr(0, scheme_pos);
		auto authority_start = scheme_pos + 3;
		auto path_start = uri.find('/', authority_start);
		if (path_start != std::string::npos) {
			result.authority = uri.substr(authority_start, path_start - authority_start);
			result.path = uri.substr(path_start);
		} else {
			result.authority = uri.substr(authority_start);
		}
	} else {
		result.path = uri;
	}
	return result;
}

} // namespace duckdb
