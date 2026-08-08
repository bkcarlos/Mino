// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef BENCHMARKS_VALIDATION_VALIDATIONS_TOPIC_WRITER_SCALING_H_
#define BENCHMARKS_VALIDATION_VALIDATIONS_TOPIC_WRITER_SCALING_H_

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>

namespace mino::benchmarks::validation {

std::string RunTopicWriterScaling(const std::filesystem::path& root,
                   uint64_t records_per_writer, size_t payload_bytes);

}  // namespace mino::benchmarks::validation

#endif  // BENCHMARKS_VALIDATION_VALIDATIONS_TOPIC_WRITER_SCALING_H_
