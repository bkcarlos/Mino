// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#ifndef MINO_SCHEMA_CANONICAL_H_
#define MINO_SCHEMA_CANONICAL_H_

#include <cstddef>
#include <span>
#include <string>
#include <string_view>

#include "mino/common/result.h"
#include "mino/schema/descriptor.h"

namespace mino::schema {

struct CanonicalOptions {
    size_t max_output_bytes = 4u << 20;
};

class CanonicalForm {
public:
    CanonicalForm(std::string text, CanonicalDigest digest,
                  uint64_t short_id) noexcept;

    std::string_view text() const noexcept { return text_; }
    const CanonicalDigest& digest() const noexcept { return digest_; }
    uint64_t short_id() const noexcept { return short_id_; }

private:
    std::string text_;
    CanonicalDigest digest_{};
    uint64_t short_id_ = 0;
};

class Canonicalizer {
public:
    static Result<CanonicalForm> Canonicalize(
        const AggregateDescriptor& aggregate,
        std::span<const DependencyDescriptor> dependency_closure = {},
        const CanonicalOptions& options = {}) noexcept;
};

// Stable internal SHA-256 used by Canonicalization v1.
CanonicalDigest Sha256(std::string_view bytes) noexcept;
std::string DigestHex(const CanonicalDigest& digest);

// The short ID is exactly digest bytes [0, 8), interpreted as an unsigned
// little-endian integer: digest[0] is bits 0..7 and digest[7] is bits 56..63.
uint64_t DigestShortId(const CanonicalDigest& digest) noexcept;

}  // namespace mino::schema

#endif  // MINO_SCHEMA_CANONICAL_H_
