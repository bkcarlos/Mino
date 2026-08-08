// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef BENCHMARKS_VALIDATION_COMMON_PROVENANCE_H_
#define BENCHMARKS_VALIDATION_COMMON_PROVENANCE_H_

#include <cstdint>
#include <string>

namespace mino::benchmarks::validation {

std::string RunTimestampUtc();
std::string OperatingSystem();
uint64_t PhysicalMemoryBytes();
unsigned int LogicalCpuCount();

}  // namespace mino::benchmarks::validation

#endif  // BENCHMARKS_VALIDATION_COMMON_PROVENANCE_H_
