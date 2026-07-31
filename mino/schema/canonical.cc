// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#include "mino/schema/canonical.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <new>
#include <string>
#include <utility>
#include <vector>

#include "mino/common/status.h"

namespace mino::schema {
namespace {

constexpr std::array<uint32_t, 64> kRoundConstants = {
    0x428a2f98u, 0x71374491u, 0xb5c0fbcfu, 0xe9b5dba5u,
    0x3956c25bu, 0x59f111f1u, 0x923f82a4u, 0xab1c5ed5u,
    0xd807aa98u, 0x12835b01u, 0x243185beu, 0x550c7dc3u,
    0x72be5d74u, 0x80deb1feu, 0x9bdc06a7u, 0xc19bf174u,
    0xe49b69c1u, 0xefbe4786u, 0x0fc19dc6u, 0x240ca1ccu,
    0x2de92c6fu, 0x4a7484aau, 0x5cb0a9dcu, 0x76f988dau,
    0x983e5152u, 0xa831c66du, 0xb00327c8u, 0xbf597fc7u,
    0xc6e00bf3u, 0xd5a79147u, 0x06ca6351u, 0x14292967u,
    0x27b70a85u, 0x2e1b2138u, 0x4d2c6dfcu, 0x53380d13u,
    0x650a7354u, 0x766a0abbu, 0x81c2c92eu, 0x92722c85u,
    0xa2bfe8a1u, 0xa81a664bu, 0xc24b8b70u, 0xc76c51a3u,
    0xd192e819u, 0xd6990624u, 0xf40e3585u, 0x106aa070u,
    0x19a4c116u, 0x1e376c08u, 0x2748774cu, 0x34b0bcb5u,
    0x391c0cb3u, 0x4ed8aa4au, 0x5b9cca4fu, 0x682e6ff3u,
    0x748f82eeu, 0x78a5636fu, 0x84c87814u, 0x8cc70208u,
    0x90befffau, 0xa4506cebu, 0xbef9a3f7u, 0xc67178f2u,
};

void Compress(const std::byte* block, std::array<uint32_t, 8>& state) noexcept {
    std::array<uint32_t, 64> words{};
    for (size_t i = 0; i < 16; ++i) {
        words[i] = (static_cast<uint32_t>(block[i * 4]) << 24) |
                   (static_cast<uint32_t>(block[i * 4 + 1]) << 16) |
                   (static_cast<uint32_t>(block[i * 4 + 2]) << 8) |
                   static_cast<uint32_t>(block[i * 4 + 3]);
    }
    for (size_t i = 16; i < words.size(); ++i) {
        const uint32_t s0 = std::rotr(words[i - 15], 7) ^
                            std::rotr(words[i - 15], 18) ^
                            (words[i - 15] >> 3);
        const uint32_t s1 = std::rotr(words[i - 2], 17) ^
                            std::rotr(words[i - 2], 19) ^
                            (words[i - 2] >> 10);
        words[i] = words[i - 16] + s0 + words[i - 7] + s1;
    }

    uint32_t a = state[0];
    uint32_t b = state[1];
    uint32_t c = state[2];
    uint32_t d = state[3];
    uint32_t e = state[4];
    uint32_t f = state[5];
    uint32_t g = state[6];
    uint32_t h = state[7];
    for (size_t i = 0; i < words.size(); ++i) {
        const uint32_t sum1 = std::rotr(e, 6) ^ std::rotr(e, 11) ^
                              std::rotr(e, 25);
        const uint32_t choose = (e & f) ^ (~e & g);
        const uint32_t temp1 =
            h + sum1 + choose + kRoundConstants[i] + words[i];
        const uint32_t sum0 = std::rotr(a, 2) ^ std::rotr(a, 13) ^
                              std::rotr(a, 22);
        const uint32_t majority = (a & b) ^ (a & c) ^ (b & c);
        const uint32_t temp2 = sum0 + majority;
        h = g;
        g = f;
        f = e;
        e = d + temp1;
        d = c;
        c = b;
        b = a;
        a = temp1 + temp2;
    }
    state[0] += a;
    state[1] += b;
    state[2] += c;
    state[3] += d;
    state[4] += e;
    state[5] += f;
    state[6] += g;
    state[7] += h;
}

std::string TypeText(const TypeDescriptor& type) {
    if (type.kind() == TypeDescriptor::Kind::kScalar) {
        return std::string(type.name());
    }
    if (type.kind() == TypeDescriptor::Kind::kUserDefined) {
        return "type(" + std::to_string(type.name().size()) + "):" +
               std::string(type.name());
    }
    return "vector<" + TypeText(*type.element_type()) + ">";
}

std::string CardinalityText(FieldCardinality cardinality) {
    switch (cardinality) {
        case FieldCardinality::kUnspecified:
            return "unspecified";
        case FieldCardinality::kOptional:
            return "optional";
        case FieldCardinality::kRequired:
            return "required";
    }
    return "unspecified";
}

std::string ConstraintText(const FieldDescriptor& field) {
    std::vector<std::string> constraints;
    constraints.push_back("cardinality=" +
                          CardinalityText(field.cardinality()));
    if (field.constraints().max_bytes().has_value()) {
        constraints.push_back(
            "max_bytes=" +
            std::to_string(*field.constraints().max_bytes()));
    }
    if (field.constraints().max_capacity().has_value()) {
        constraints.push_back(
            "max_capacity=" +
            std::to_string(*field.constraints().max_capacity()));
    }
    if (field.constraints().snapshot_key()) {
        constraints.emplace_back("snapshot_key");
    }
    std::sort(constraints.begin(), constraints.end());
    std::string result;
    for (size_t i = 0; i < constraints.size(); ++i) {
        if (i != 0) result.push_back(',');
        result.append(constraints[i]);
    }
    return result;
}

std::string HexBytes(std::string_view bytes) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string result;
    result.reserve(bytes.size() * 2);
    for (char byte : bytes) {
        const uint8_t value = static_cast<uint8_t>(byte);
        result.push_back(kHex[value >> 4]);
        result.push_back(kHex[value & 0x0fu]);
    }
    return result;
}

std::string DefaultText(const std::optional<DefaultValue>& value) {
    if (!value.has_value()) return "-";
    switch (value->kind()) {
        case DefaultValue::Kind::kInteger:
            return "integer:" + std::string(value->canonical_value());
        case DefaultValue::Kind::kFloat32:
            return "float32:" + std::string(value->canonical_value());
        case DefaultValue::Kind::kFloat64:
            return "float64:" + std::string(value->canonical_value());
        case DefaultValue::Kind::kBoolean:
            return "boolean:" + std::string(value->canonical_value());
        case DefaultValue::Kind::kString:
            return "string(" +
                   std::to_string(value->canonical_value().size()) + "):" +
                   std::string(value->canonical_value());
        case DefaultValue::Kind::kBytes:
            return "bytes(" +
                   std::to_string(value->canonical_value().size()) + "):" +
                   HexBytes(value->canonical_value());
    }
    return "-";
}

Result<std::vector<DependencyDescriptor>> NormalizeDependencies(
    std::span<const DependencyDescriptor> dependencies) {
    std::vector<DependencyDescriptor> normalized(dependencies.begin(),
                                                 dependencies.end());
    std::sort(normalized.begin(), normalized.end(),
              [](const auto& lhs, const auto& rhs) {
                  return lhs.full_name() < rhs.full_name();
              });
    for (size_t i = 1; i < normalized.size(); ++i) {
        if (normalized[i - 1].full_name() == normalized[i].full_name()) {
            if (normalized[i - 1].digest() != normalized[i].digest()) {
                return Status::Error(
                    StatusCode::kInvalidArgument,
                    "dependency name maps to multiple digests");
            }
            normalized.erase(normalized.begin() +
                             static_cast<std::ptrdiff_t>(i));
            --i;
        }
    }
    return normalized;
}

}  // namespace

CanonicalForm::CanonicalForm(std::string text, CanonicalDigest digest,
                             uint64_t short_id) noexcept
    : text_(std::move(text)), digest_(digest), short_id_(short_id) {}

CanonicalDigest Sha256(std::string_view bytes) noexcept {
    std::array<uint32_t, 8> state = {
        0x6a09e667u, 0xbb67ae85u, 0x3c6ef372u, 0xa54ff53au,
        0x510e527fu, 0x9b05688cu, 0x1f83d9abu, 0x5be0cd19u,
    };
    const auto* data = reinterpret_cast<const std::byte*>(bytes.data());
    size_t offset = 0;
    while (bytes.size() - offset >= 64) {
        Compress(data + offset, state);
        offset += 64;
    }

    std::array<std::byte, 128> final_blocks{};
    const size_t remainder = bytes.size() - offset;
    if (remainder != 0) {
        std::memcpy(final_blocks.data(), data + offset, remainder);
    }
    final_blocks[remainder] = std::byte{0x80};
    const size_t final_size = remainder < 56 ? 64 : 128;
    const uint64_t bit_length = static_cast<uint64_t>(bytes.size()) * 8u;
    for (size_t i = 0; i < 8; ++i) {
        final_blocks[final_size - 1 - i] =
            static_cast<std::byte>((bit_length >> (i * 8)) & 0xffu);
    }
    Compress(final_blocks.data(), state);
    if (final_size == 128) Compress(final_blocks.data() + 64, state);

    CanonicalDigest digest{};
    for (size_t i = 0; i < state.size(); ++i) {
        digest[i * 4] = static_cast<std::byte>(state[i] >> 24);
        digest[i * 4 + 1] = static_cast<std::byte>(state[i] >> 16);
        digest[i * 4 + 2] = static_cast<std::byte>(state[i] >> 8);
        digest[i * 4 + 3] = static_cast<std::byte>(state[i]);
    }
    return digest;
}

std::string DigestHex(const CanonicalDigest& digest) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string result;
    result.reserve(digest.size() * 2);
    for (std::byte byte : digest) {
        const uint8_t value = static_cast<uint8_t>(byte);
        result.push_back(kHex[value >> 4]);
        result.push_back(kHex[value & 0x0f]);
    }
    return result;
}

uint64_t DigestShortId(const CanonicalDigest& digest) noexcept {
    uint64_t result = 0;
    for (size_t i = 0; i < 8; ++i) {
        result |= static_cast<uint64_t>(digest[i]) << (i * 8);
    }
    return result;
}

Result<CanonicalForm> Canonicalizer::Canonicalize(
    const AggregateDescriptor& aggregate,
    std::span<const DependencyDescriptor> dependency_closure,
    const CanonicalOptions& options) noexcept {
    try {
        auto dependencies = NormalizeDependencies(dependency_closure);
        if (!dependencies.ok()) return dependencies.status();

        std::string text("mino-canonical-v1");
        text.push_back('\0');
        text.append("type:");
        text.append(aggregate.kind() == AggregateKind::kMessage ? "message:"
                                                                : "struct:");
        text.append(std::to_string(aggregate.full_name().size()));
        text.push_back(':');
        text.append(aggregate.full_name());
        text.push_back('\n');
        for (const FieldDescriptor& field : aggregate.fields()) {
            text.append(std::to_string(field.id()));
            text.push_back(':');
            text.append(TypeText(field.type()));
            text.push_back(':');
            text.append(ConstraintText(field));
            text.push_back(':');
            text.append(DefaultText(field.default_value()));
            text.push_back('\n');
        }

        if (!aggregate.reserved_ranges().empty()) {
            std::vector<std::pair<uint32_t, uint32_t>> ranges;
            for (const ReservedRangeDescriptor& range :
                 aggregate.reserved_ranges()) {
                if (!ranges.empty() &&
                    ranges.back().second != std::numeric_limits<uint32_t>::max() &&
                    ranges.back().second + 1 == range.first()) {
                    ranges.back().second = range.last();
                } else {
                    ranges.emplace_back(range.first(), range.last());
                }
            }
            text.append("reserved:");
            for (size_t i = 0; i < ranges.size(); ++i) {
                if (i != 0) text.push_back(',');
                text.append(std::to_string(ranges[i].first));
                text.push_back('-');
                text.append(std::to_string(ranges[i].second));
            }
            text.push_back('\n');
        }

        for (const DependencyDescriptor& dependency : *dependencies) {
            text.append("dependency:");
            text.append(std::to_string(dependency.full_name().size()));
            text.push_back(':');
            text.append(dependency.full_name());
            text.push_back(':');
            text.append(DigestHex(dependency.digest()));
            text.push_back('\n');
        }
        if (text.size() > options.max_output_bytes) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "canonical schema exceeds max_output_bytes");
        }
        const CanonicalDigest digest = Sha256(text);
        return CanonicalForm(std::move(text), digest, DigestShortId(digest));
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

}  // namespace mino::schema
