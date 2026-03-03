#pragma once
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-includes"

// IWYU pragma: keep
#include "duckdb.hpp" // NOLINT(clangd-unused-includes)

#pragma clang diagnostic pop

namespace duckdb {

class MetastoreExtension : public Extension {
public:
	void Load(ExtensionLoader &db) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb
