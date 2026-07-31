// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_SCHEMA_WIRE_H_
#define MINO_SCHEMA_WIRE_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/schema/descriptor.h"
#include "mino/schema/dynamic_value.h"

namespace mino::schema {

enum class WireType : uint8_t {
    kVarint = 0,
    kI64 = 1,
    kLengthDelimited = 2,
    kI32 = 5,
};

struct WireLimits {
    size_t max_frame_bytes = 16u << 20;
    size_t max_length_bytes = 16u << 20;
    size_t max_container_elements = 1u << 20;
    size_t max_depth = 32;
    UnknownFieldLimits unknown_fields;
};

uint64_t ZigZagEncode(int64_t value) noexcept;
int64_t ZigZagDecode(uint64_t value) noexcept;

// Appends/reads the shortest unsigned LEB128 representation. Decode rejects
// overlong, non-minimal and >64-bit encodings with kCorruption.
Status EncodeLeb128(uint64_t value, std::vector<std::byte>& output) noexcept;
Result<uint64_t> DecodeLeb128(std::span<const std::byte> input,
                              size_t& offset) noexcept;

class CanonicalWireCodec {
public:
    static Result<std::vector<std::byte>> Encode(
        const SchemaDescriptor& descriptor, const DynamicMessage& message,
        std::span<const std::shared_ptr<const SchemaDescriptor>> descriptors = {},
        const WireLimits& limits = {}) noexcept;

    static Result<DynamicMessage> Decode(
        const SchemaDescriptor& descriptor, std::span<const std::byte> bytes,
        std::span<const std::shared_ptr<const SchemaDescriptor>> descriptors = {},
        const WireLimits& limits = {}) noexcept;
};

}  // namespace mino::schema

#endif  // MINO_SCHEMA_WIRE_H_
