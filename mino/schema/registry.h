// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_SCHEMA_REGISTRY_H_
#define MINO_SCHEMA_REGISTRY_H_

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <shared_mutex>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "mino/common/result.h"
#include "mino/schema/compatibility.h"
#include "mino/schema/compiler.h"
#include "mino/schema/descriptor.h"

namespace mino::schema {

using SchemaHandle = std::shared_ptr<const SchemaDescriptor>;

class SchemaRegistry {
public:
    using ShortIdIndexProviderForTesting =
        uint64_t (*)(const CanonicalDigest&) noexcept;

    SchemaRegistry() = default;
    // Test-only collision injection. Descriptor identities are still validated
    // with the production DigestShortId rule; this hook affects only indexing.
    explicit SchemaRegistry(
        ShortIdIndexProviderForTesting short_id_index_provider_for_testing)
        : short_id_index_provider_for_testing_(
              short_id_index_provider_for_testing) {}

    // Compilation and dependency resolution happen before the short write-lock
    // commit. One handle is returned for each type in the IDL.
    Result<std::vector<SchemaHandle>> RegisterIdl(
        std::string_view idl, const CompileOptions& options = {}) noexcept;

    // Structured descriptor registration is the v1 descriptor API.
    Result<SchemaHandle> RegisterDescriptor(SchemaHandle descriptor) noexcept;
    // Decodes a versioned minoc descriptor artifact and atomically registers
    // every type it contains. Dependencies must be present in the same artifact
    // or already registered. Returns the published handle for the artifact's
    // first encoded type (not an inferred root); empty, malformed, or invalid
    // artifacts are rejected.
    Result<SchemaHandle> RegisterDescriptor(
        std::span<const std::byte> descriptor_bytes) noexcept;
    Result<std::vector<SchemaHandle>> RegisterCompiled(
        const CompiledSchema& schema) noexcept;

    Result<SchemaHandle> Find(const CanonicalDigest& digest) const noexcept;
    Result<SchemaHandle> Find(const SchemaIdentity& identity) const noexcept;
    // Short IDs are only unambiguous indexes. A collision returns
    // kSchemaMismatch and requires Find(full_digest).
    Result<SchemaHandle> FindByShortId(uint64_t short_id) const noexcept;

    Result<Compatibility> CheckCompatibility(
        const CanonicalDigest& from_digest,
        const CanonicalDigest& to_digest) const noexcept;

    size_t size() const noexcept;

private:
    struct TypeVersionKey {
        std::string full_name;
        uint32_t schema_version = 0;

        friend bool operator<(const TypeVersionKey& lhs,
                              const TypeVersionKey& rhs) noexcept {
            if (lhs.full_name != rhs.full_name) {
                return lhs.full_name < rhs.full_name;
            }
            return lhs.schema_version < rhs.schema_version;
        }
    };

    Result<std::vector<SchemaHandle>> RegisterDescriptors(
        std::span<const SchemaHandle> descriptors) noexcept;
    uint64_t ShortIdIndex(const CanonicalDigest& digest) const noexcept;

    mutable std::shared_mutex mutex_;
    ShortIdIndexProviderForTesting short_id_index_provider_for_testing_ = nullptr;
    std::map<CanonicalDigest, SchemaHandle> by_digest_;
    std::map<TypeVersionKey, CanonicalDigest> by_type_version_;
    std::map<uint64_t, std::vector<CanonicalDigest>> by_short_id_;
};

}  // namespace mino::schema

#endif  // MINO_SCHEMA_REGISTRY_H_
