// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef BENCHMARKS_VALIDATION_REPORT_REPORT_H_
#define BENCHMARKS_VALIDATION_REPORT_REPORT_H_

#include <optional>
#include <string>
#include <string_view>

#include "benchmarks/validation/common/config.h"
#include "benchmarks/validation/common/temporary_directory.h"

namespace mino::benchmarks::validation {

struct ValidationResults {
    std::string broadcast_ack;
    std::string dynamic_view;
    std::string topic_writer_scaling;
    std::string sync_policy;
    std::string buffer_capacity;
    std::string pin_lease;
};

std::string BuildJson(
    const Config& config, const ValidationResults& results,
    const std::optional<OwnedTemporaryDirectory>& temporary);
std::string BuildFailureJson(int argc, char** argv, std::string_view reason);

}  // namespace mino::benchmarks::validation

#endif  // BENCHMARKS_VALIDATION_REPORT_REPORT_H_
