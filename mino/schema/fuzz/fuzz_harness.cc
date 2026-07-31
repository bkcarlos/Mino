// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/schema/fuzz/fuzz_harness.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <span>
#include <string_view>

#include "mino/common/status.h"
#include "mino/schema/canonical.h"
#include "mino/schema/codegen/artifact_codec.h"
#include "mino/schema/compiler.h"
#include "mino/schema/descriptor.h"
#include "mino/schema/registry.h"
#include "mino/schema/wire.h"

namespace mino::schema::fuzz {
namespace {

constexpr std::string_view kCanonicalPayloadIdl = R"idl(
syntax = "v1";
package fuzz;
option schema_version = "1.0";
message Payload {
  int32 delta = 1;
  uint64 sequence = 2;
  fixed32 code = 3;
  float sample = 4;
  optional string label = 5 [max_bytes = 32];
  vector<uint32> values = 6 [max_capacity = 4];
}
)idl";

CompileOptions BoundedCompileOptions() {
    CompileOptions options;
    options.max_input_bytes = kMaxIdlInputBytes;
    options.max_tokens = 2048;
    options.max_token_bytes = 1024;
    options.max_types = 32;
    options.max_fields = 256;
    options.max_reserved_ranges = 128;
    options.max_annotations = 256;
    options.max_nesting_depth = 8;
    options.max_name_bytes = 256;
    options.max_total_capacity = 1u << 20;
    options.max_canonical_bytes = 64u << 10;
    options.layout_version = 1;
    return options;
}

WireLimits BoundedWireLimits() {
    WireLimits limits;
    limits.max_frame_bytes = kMaxCanonicalPayloadBytes;
    limits.max_length_bytes = 4u << 10;
    limits.max_container_elements = 64;
    limits.max_depth = 8;
    limits.unknown_fields.max_bytes = 4u << 10;
    limits.unknown_fields.max_fields = 64;
    return limits;
}

const Result<CompiledSchema>& CanonicalPayloadSchema() {
    static const Result<CompiledSchema> schema = [] {
        CompileOptions options = BoundedCompileOptions();
        options.max_input_bytes = 1024;
        options.max_tokens = 256;
        options.max_types = 1;
        options.max_fields = 16;
        options.max_annotations = 16;
        options.max_total_capacity = 1024;
        options.unknown_fields.max_bytes = 0;
        options.unknown_fields.max_fields = 0;
        options.max_canonical_bytes = 4096;
        return SchemaCompiler::Compile(kCanonicalPayloadIdl, options);
    }();
    return schema;
}

Status Unexpected(std::string_view message) {
    return Status::Error(StatusCode::kInternal, message);
}

}  // namespace

Status FuzzIdl(std::span<const std::byte> input) noexcept {
    try {
        if (input.size() > kMaxIdlInputBytes) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "IDL fuzz input exceeds max bytes");
        }
        const char* data = input.empty()
                               ? ""
                               : reinterpret_cast<const char*>(input.data());
        auto compiled = SchemaCompiler::Compile(
            std::string_view(data, input.size()), BoundedCompileOptions());
        if (!compiled.ok()) return compiled.status();

        CanonicalOptions canonical_options;
        canonical_options.max_output_bytes = 64u << 10;
        for (const auto& descriptor : compiled->types()) {
            if (descriptor == nullptr) {
                return Unexpected("compiler returned a null descriptor");
            }
            auto canonical = Canonicalizer::Canonicalize(
                descriptor->aggregate(), descriptor->dependencies(),
                canonical_options);
            if (!canonical.ok()) return canonical.status();
            if (canonical->text() != descriptor->canonical_schema() ||
                canonical->digest() !=
                    descriptor->identity().canonical_digest() ||
                canonical->short_id() != descriptor->identity().short_id()) {
                return Status::Error(StatusCode::kSchemaMismatch,
                                     "compiler canonical identity is unstable");
            }
        }

        SchemaRegistry registry;
        auto registered = registry.RegisterCompiled(*compiled);
        return registered.ok() ? Status::Ok() : registered.status();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Status FuzzDescriptor(std::span<const std::byte> input) noexcept {
    try {
        if (input.size() > kMaxDescriptorInputBytes) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "descriptor fuzz input exceeds max bytes");
        }
        const char* data = input.empty()
                               ? ""
                               : reinterpret_cast<const char*>(input.data());
        auto decoded = codegen::DecodeAndValidate(
            std::string_view(data, input.size()));
        if (!decoded.ok()) return decoded.status();

        SchemaRegistry registry;
        auto registered = registry.RegisterDescriptor(input);
        if (!registered.ok()) return registered.status();
        if (decoded->types.empty() || registry.size() != decoded->types.size()) {
            return Unexpected(
                "descriptor codec and registry disagree on artifact contents");
        }
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Status FuzzCanonicalPayload(std::span<const std::byte> input) noexcept {
    try {
        if (input.size() > kMaxCanonicalPayloadBytes) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "canonical payload exceeds max bytes");
        }
        const auto& schema = CanonicalPayloadSchema();
        if (!schema.ok()) return schema.status();
        const SchemaDescriptor* descriptor = schema->FindType("fuzz.Payload");
        if (descriptor == nullptr) {
            return Unexpected("canonical payload descriptor is unavailable");
        }

        const WireLimits limits = BoundedWireLimits();
        auto decoded = CanonicalWireCodec::Decode(
            *descriptor, input, schema->types(), limits);
        if (!decoded.ok()) return decoded.status();
        auto encoded = CanonicalWireCodec::Encode(
            *descriptor, *decoded, schema->types(), limits);
        if (!encoded.ok()) return encoded.status();
        if (encoded->size() != input.size() ||
            !std::equal(encoded->begin(), encoded->end(), input.begin())) {
            return Status::Error(StatusCode::kCorruption,
                                 "payload is not canonical on round trip");
        }
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Status FuzzOneInput(std::span<const std::byte> input) noexcept {
    try {
        if (input.empty()) return FuzzIdl(input);
        const uint8_t selector = static_cast<uint8_t>(input.front()) % 3;
        const auto payload = input.subspan(1);
        if (selector == 0) return FuzzIdl(payload);
        if (selector == 1) return FuzzDescriptor(payload);
        return FuzzCanonicalPayload(payload);
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

}  // namespace mino::schema::fuzz
