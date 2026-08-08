// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/validation/common/stats.h"

#include <algorithm>
#include <cmath>
#include <ostream>
#include <utility>

namespace mino::benchmarks::validation {

Distribution Summarize(std::vector<uint64_t> samples) {
    Distribution result;
    result.samples = samples.size();
    if (samples.empty()) return result;
    std::sort(samples.begin(), samples.end());
    const auto nearest_rank = [&](double percentile) {
        size_t rank = static_cast<size_t>(
            std::ceil(percentile * static_cast<double>(samples.size())));
        rank = std::max<size_t>(1, std::min(rank, samples.size()));
        return samples[rank - 1];
    };
    result.p50 = nearest_rank(0.50);
    result.p95 = nearest_rank(0.95);
    result.p99 = nearest_rank(0.99);
    result.maximum = samples.back();
    return result;
}

void WriteDistribution(std::ostream& output, const Distribution& value) {
    output << "{\"samples\":" << value.samples << ",\"p50\":" << value.p50
           << ",\"p95\":" << value.p95 << ",\"p99\":" << value.p99
           << ",\"max\":" << value.maximum << "}";
}

}  // namespace mino::benchmarks::validation
