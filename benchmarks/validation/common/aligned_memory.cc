// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/validation/common/aligned_memory.h"

#include <cstring>
#include <new>

namespace mino::benchmarks::validation {

void AlignedBytesDeleter::operator()(std::byte* pointer) const {
    ::operator delete[](pointer, std::align_val_t(64));
}

AlignedBytes AllocateAligned(size_t bytes) {
    AlignedBytes memory(new (std::align_val_t(64)) std::byte[bytes]);
    std::memset(memory.get(), 0, bytes);
    return memory;
}

}  // namespace mino::benchmarks::validation
