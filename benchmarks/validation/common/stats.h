// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef BENCHMARKS_VALIDATION_COMMON_STATS_H_
#define BENCHMARKS_VALIDATION_COMMON_STATS_H_

#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <vector>

namespace mino::benchmarks::validation {

struct Distribution {
    size_t samples = 0;
    uint64_t p50 = 0;
    uint64_t p95 = 0;
    uint64_t p99 = 0;
    uint64_t maximum = 0;
};

Distribution Summarize(std::vector<uint64_t> samples);
void WriteDistribution(std::ostream& output, const Distribution& value);

}  // namespace mino::benchmarks::validation

#endif  // BENCHMARKS_VALIDATION_COMMON_STATS_H_
