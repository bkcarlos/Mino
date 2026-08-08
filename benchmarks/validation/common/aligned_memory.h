// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef BENCHMARKS_VALIDATION_COMMON_ALIGNED_MEMORY_H_
#define BENCHMARKS_VALIDATION_COMMON_ALIGNED_MEMORY_H_

#include <cstddef>
#include <memory>

namespace mino::benchmarks::validation {

struct AlignedBytesDeleter {
    void operator()(std::byte* pointer) const;
};

using AlignedBytes = std::unique_ptr<std::byte[], AlignedBytesDeleter>;

AlignedBytes AllocateAligned(size_t bytes);

}  // namespace mino::benchmarks::validation

#endif  // BENCHMARKS_VALIDATION_COMMON_ALIGNED_MEMORY_H_
