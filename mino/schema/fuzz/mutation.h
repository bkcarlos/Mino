// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_SCHEMA_FUZZ_MUTATION_H_
#define MINO_SCHEMA_FUZZ_MUTATION_H_

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "mino/common/result.h"

namespace mino::schema::fuzz {

enum class MutationKind : uint8_t {
    kTruncate,
    kBitFlip,
    kInsert,
    kRepeat,
    kLengthBomb,
};

inline constexpr size_t kMutationKindCount = 5;
inline constexpr size_t kMaxMutationOutputBytes = 32u << 10;

// Deterministic bounded mutator shared by the quick corpus test and standalone
// driver. The result never exceeds min(max_output_bytes,
// kMaxMutationOutputBytes), and allocation failure is reported as a Status.
Result<std::vector<std::byte>> Mutate(std::span<const std::byte> seed,
                                      MutationKind kind, uint64_t entropy,
                                      size_t max_output_bytes) noexcept;

}  // namespace mino::schema::fuzz

#endif  // MINO_SCHEMA_FUZZ_MUTATION_H_
