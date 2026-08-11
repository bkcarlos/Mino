// Copyright 2026 The Mino Authors

#ifndef MINO_UPGRADE_MANIFEST_H_
#define MINO_UPGRADE_MANIFEST_H_

#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

#include "mino/common/result.h"
#include "mino/upgrade/upgrade.h"

namespace mino::upgrade {

enum class UpgradePersistenceFaultPoint : uint8_t {
    kAfterTemporaryWrite = 1,
    kAfterTemporaryDataSync = 2,
    kAfterAtomicRename = 3,
    kAfterParentDirectorySync = 4,
};

using UpgradePersistenceFaultHook =
    Status (*)(UpgradePersistenceFaultPoint point, void* context) noexcept;

struct UpgradeManifestOptions {
    size_t maximum_file_bytes = kMaximumUpgradeFileBytes;
    size_t maximum_journal_entries = kMaximumUpgradeJournalEntries;
    UpgradePersistenceFaultHook fault_hook = nullptr;
    void* fault_hook_context = nullptr;
};

Result<std::vector<std::byte>> EncodeUpgradeSnapshot(
    const UpgradeSnapshot& snapshot,
    const UpgradeManifestOptions& options = {}) noexcept;
Result<UpgradeSnapshot> DecodeUpgradeSnapshot(
    std::span<const std::byte> encoded,
    const UpgradeManifestOptions& options = {}) noexcept;

// Owns a non-blocking advisory owner lock next to the manifest. Every successful
// mutation is durable before return: bounded write, data sync, atomic rename,
// then parent-directory fsync. The object is deliberately not thread-safe.
class UpgradeManifestStore final {
public:
    static Result<std::unique_ptr<UpgradeManifestStore>> Create(
        const std::filesystem::path& manifest_path, UpgradePlan plan,
        uint64_t now_ns, const UpgradeManifestOptions& options = {}) noexcept;
    static Result<std::unique_ptr<UpgradeManifestStore>> Open(
        const std::filesystem::path& manifest_path,
        const UpgradeManifestOptions& options = {}) noexcept;

    ~UpgradeManifestStore();
    UpgradeManifestStore(const UpgradeManifestStore&) = delete;
    UpgradeManifestStore& operator=(const UpgradeManifestStore&) = delete;

    const UpgradeSnapshot& snapshot() const noexcept { return snapshot_; }
    const std::filesystem::path& path() const noexcept { return path_; }
    bool poisoned() const noexcept { return poisoned_; }

    Status Advance(UpgradePhase next, uint64_t now_ns,
                   std::string_view detail = {}) noexcept;
    Status Fail(uint64_t now_ns, std::string_view reason) noexcept;

private:
    UpgradeManifestStore(std::filesystem::path path,
                         UpgradeManifestOptions options, int lock_fd,
                         UpgradeSnapshot snapshot) noexcept;
    Status Persist(UpgradeSnapshot next) noexcept;

    std::filesystem::path path_;
    UpgradeManifestOptions options_;
    int lock_fd_ = -1;
    bool poisoned_ = false;
    UpgradeSnapshot snapshot_;
};

}  // namespace mino::upgrade

#endif  // MINO_UPGRADE_MANIFEST_H_
