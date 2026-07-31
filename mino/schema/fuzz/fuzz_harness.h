// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_SCHEMA_FUZZ_FUZZ_HARNESS_H_
#define MINO_SCHEMA_FUZZ_FUZZ_HARNESS_H_

#include <cstddef>
#include <span>

#include "mino/common/status.h"

namespace mino::schema::fuzz {

// These limits are deliberately small enough for sanitizer runs and apply
// before an input is copied or parsed.
inline constexpr size_t kMaxIdlInputBytes = 16u << 10;
inline constexpr size_t kMaxDescriptorInputBytes = 32u << 10;
inline constexpr size_t kMaxCanonicalPayloadBytes = 16u << 10;

// Stable, engine-neutral harness boundaries. Arbitrary input is an expected
// condition: malformed data is reported as a Status and no exception escapes.
//
// FuzzIdl returns OK, InvalidArgument, or ResourceExhausted for input outcomes.
// FuzzDescriptor exercises both MINODSC2 DecodeAndValidate and the Registry byte
// API; malformed inputs may also report Corruption or SchemaMismatch.
// FuzzCanonicalPayload returns OK, Corruption, SchemaMismatch, or
// ResourceExhausted. Internal indicates a harness/API bug, not a malformed-input
// outcome.
Status FuzzIdl(std::span<const std::byte> input) noexcept;
Status FuzzDescriptor(std::span<const std::byte> input) noexcept;
Status FuzzCanonicalPayload(std::span<const std::byte> input) noexcept;

// Selector-based entry point for engines that expose one byte-stream callback.
// The first byte selects IDL, descriptor, or canonical payload; the remainder
// is passed unchanged to the selected stable harness. Empty input exercises IDL.
Status FuzzOneInput(std::span<const std::byte> input) noexcept;

}  // namespace mino::schema::fuzz

#endif  // MINO_SCHEMA_FUZZ_FUZZ_HARNESS_H_
