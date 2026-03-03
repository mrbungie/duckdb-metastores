#include "metastore_types.hpp"

namespace duckdb {

ParsedUri ParsedUri::Parse(const std::string &uri_p) {
	ParsedUri result;
	std::string uri = StringUtil::Replace(uri_p, " ", ""); // Basic whitespace removal for connection strings
	auto scheme_pos = uri.find(':');
	if (scheme_pos != std::string::npos && scheme_pos > 0) {
		result.scheme = StringUtil::Lower(uri.substr(0, scheme_pos));
		auto after_scheme = scheme_pos + 1;
		if (uri.size() >= after_scheme + 2 && uri.substr(after_scheme, 2) == "//") {
			auto authority_start = after_scheme + 2;
			auto path_start = uri.find('/', authority_start);
			if (path_start != std::string::npos) {
				result.authority = uri.substr(authority_start, path_start - authority_start);
				result.path = uri.substr(path_start);
			} else {
				result.authority = uri.substr(authority_start);
			}
		} else {
			result.path = uri.substr(after_scheme);
		}
	} else {
		result.path = uri;
	}
	return result;
}

std::string ParsedUri::ToString() const {
	if (scheme.empty()) {
		return path;
	}
	return scheme + "://" + authority + path;
}

} // namespace duckdb
