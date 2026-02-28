#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

exec env HMS_TEST_MODE=all "${ROOT_DIR}/test/integration/hms/run_hms_tests.sh" "$@"
