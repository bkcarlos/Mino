// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_STORAGE_SCHEMA_STORE_H_
#define MINO_STORAGE_SCHEMA_STORE_H_

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <span>

#include "mino/common/result.h"
#include "mino/schema/descriptor.h"

namespace mino::schema {
class SchemaRegistry;
}  // namespace mino::schema

namespace mino::storage {

using SchemaRef = uint32_t;
inline constexpr SchemaRef kInvalidSchemaRef = 0;
inline constexpr uint16_t kSchemaManifestVersion = 1;
inline constexpr size_t kSchemaManifestHeaderSize = 36;

enum class SchemaStoreFaultPoint : uint8_t {
    kAfterDescriptorTempWrite,
    kAfterDescriptorSync,
    kAfterDescriptorRename,
    kAfterDescriptorDirectorySync,
    kAfterManifestTempWrite,
    kAfterManifestSync,
    kAfterManifestRename,
    kAfterManifestDirectorySync,
};

using SchemaStoreFaultHook =
    Status (*)(SchemaStoreFaultPoint point, void* context) noexcept;

struct SchemaStoreOptions {
    size_t max_entries = 65536;
    size_t max_manifest_bytes = 16u * 1024u * 1024u;
    size_t max_descriptor_bytes = 16u * 1024u * 1024u;
    SchemaStoreFaultHook fault_hook = nullptr;
    void* fault_hook_context = nullptr;
};

struct SchemaStoreEntry {
    SchemaRef ref;
    schema::SchemaIdentity identity;
    // Absolute or root-relative according to the path passed to Open().
    std::filesystem::path descriptor_path;
};

// Per-recording-session immutable descriptor store. SchemaStore is deliberately
// single-owner and not thread-safe. Open() also takes a non-blocking advisory
// lock so two store instances cannot update the same schemas directory.
class SchemaStore final {
public:
    static Result<std::unique_ptr<SchemaStore>> Open(
        const std::filesystem::path& session_root,
        schema::SchemaRegistry* registry,
        const SchemaStoreOptions& options = {}) noexcept;

    ~SchemaStore();

    SchemaStore(const SchemaStore&) = delete;
    SchemaStore& operator=(const SchemaStore&) = delete;
    SchemaStore(SchemaStore&&) = delete;
    SchemaStore& operator=(SchemaStore&&) = delete;

    // The raw bytes must be a complete MINODSC artifact containing identity.
    // A returned ref is durable and may be written into durable Records.
    Result<SchemaRef> Persist(
        const schema::SchemaIdentity& identity,
        std::span<const std::byte> descriptor_artifact) noexcept;

    Result<SchemaStoreEntry> Resolve(SchemaRef ref) const noexcept;
    Result<SchemaRef> FindRef(
        const schema::SchemaIdentity& identity) const noexcept;
    Result<SchemaRef> FindRef(
        const schema::CanonicalDigest& digest) const noexcept;

    // Revalidates and publishes every durable descriptor into the registry
    // supplied to Open(). Safe to call repeatedly; publication is idempotent.
    Status HydrateRegistry() noexcept;

    size_t size() const noexcept { return by_ref_.size(); }
    SchemaRef high_watermark() const noexcept { return high_watermark_; }

private:
    SchemaStore(std::filesystem::path session_root,
                std::filesystem::path schemas_directory,
                schema::SchemaRegistry* registry, SchemaStoreOptions options,
                int owner_lock_fd) noexcept;

    std::filesystem::path session_root_;
    std::filesystem::path schemas_directory_;
    schema::SchemaRegistry* registry_ = nullptr;
    SchemaStoreOptions options_;
    int owner_lock_fd_ = -1;
    bool poisoned_ = false;
    SchemaRef high_watermark_ = kInvalidSchemaRef;
    std::map<SchemaRef, SchemaStoreEntry> by_ref_;
    std::map<schema::CanonicalDigest, SchemaRef> by_digest_;
};

}  // namespace mino::storage

#endif  // MINO_STORAGE_SCHEMA_STORE_H_
