#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>

namespace duckdb {

//===--------------------------------------------------------------------===//
// HmsRetryPolicy — exponential backoff retry configuration for HMS calls
//===--------------------------------------------------------------------===//
struct HmsRetryPolicy {
	//! Maximum number of attempts (including the first call)
	uint32_t max_attempts = 3;
	//! Delay before the first retry, in milliseconds
	uint32_t initial_delay_ms = 100;
	//! Maximum delay cap, in milliseconds
	uint32_t max_delay_ms = 5000;
	//! Multiplicative backoff factor applied per retry
	double backoff_multiplier = 2.0;

	//! Compute the retry delay for attempt number `attempt` (1-indexed: attempt=1 is the first retry).
	//! Returns 0 if attempt > max_attempts - 1 (no more retries).
	//! Returns min(initial_delay_ms * backoff_multiplier^(attempt-1), max_delay_ms).
	uint32_t ComputeDelay(uint32_t attempt) const {
		if (attempt == 0 || attempt >= max_attempts) {
			return 0;
		}
		double raw_delay = static_cast<double>(initial_delay_ms) * std::pow(backoff_multiplier, attempt - 1);
		double bounded_delay = std::min(raw_delay, static_cast<double>(max_delay_ms));
		return static_cast<uint32_t>(bounded_delay);
	}

	//! Returns true if another attempt should be made.
	bool ShouldRetry(uint32_t attempts_made) const {
		return attempts_made < max_attempts;
	}
};

} // namespace duckdb
