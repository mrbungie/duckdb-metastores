PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=quack
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile
# Overwrite EXT_FLAGS to enable unittests
EXT_FLAGS=-DENABLE_UNITTEST_CPP_TESTS=1

test_compile:
	cmake --build build/release --target unittest

