// Copyright 2026 The Mino Authors

#ifndef MINO_UPGRADE_ROUTING_CATALOG_H_
#define MINO_UPGRADE_ROUTING_CATALOG_H_

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "mino/common/result.h"
#include "mino/upgrade/upgrade.h"

namespace mino::upgrade {

inline constexpr size_t kMaximumRoutingCatalogBytes = 64u * 1024u;

struct RegionRoutingSnapshot {
    uint64_t generation = 0;
    RegionIdentity active_region;
    std::string commit_token;
    bool source_fenced = false;

    friend bool operator==(const RegionRoutingSnapshot&,
                           const RegionRoutingSnapshot&) = default;
};

Result<std::vector<std::byte>> EncodeRegionRoutingSnapshot(
    const RegionRoutingSnapshot& snapshot) noexcept;
Result<RegionRoutingSnapshot> DecodeRegionRoutingSnapshot(
    std::span<const std::byte> encoded) noexcept;
// Lock-free reader for endpoint creation/route refresh. Atomic rename ensures a
// reader observes one complete CRC-valid generation.
Result<RegionRoutingSnapshot> LoadRegionRoutingSnapshot(
    const std::filesystem::path& path) noexcept;

// Durable source of truth for new local/transport endpoints. The catalog owns a
// non-blocking advisory writer lock. Every mutation is CRC-protected and
// durable after temporary write + data sync + atomic rename + parent fsync.
class RegionRoutingCatalog final {
public:
    static Result<std::unique_ptr<RegionRoutingCatalog>> Create(
        const std::filesystem::path& path,
        const RegionIdentity& initial_region) noexcept;
    static Result<std::unique_ptr<RegionRoutingCatalog>> Open(
        const std::filesystem::path& path) noexcept;

    ~RegionRoutingCatalog();
    RegionRoutingCatalog(const RegionRoutingCatalog&) = delete;
    RegionRoutingCatalog& operator=(const RegionRoutingCatalog&) = delete;

    const RegionRoutingSnapshot& snapshot() const noexcept { return snapshot_; }
    const std::filesystem::path& path() const noexcept { return path_; }

    // Generic generation CAS. expected_active prevents an ABA-style switch
    // even if an operator supplied a stale generation from another catalog.
    Result<RegionRoutingSnapshot> CompareExchange(
        uint64_t expected_generation, const RegionIdentity& expected_active,
        RegionRoutingSnapshot replacement) noexcept;

    // Token-idempotent upgrade operations used by the production control plane.
    Status FenceSource(const UpgradePlan& plan) noexcept;
    Status Cutover(const UpgradePlan& plan) noexcept;
    Status RestoreSource(const UpgradePlan& plan) noexcept;

private:
    RegionRoutingCatalog(std::filesystem::path path, int lock_fd,
                         RegionRoutingSnapshot snapshot) noexcept;
    Status Persist(RegionRoutingSnapshot next) noexcept;

    std::filesystem::path path_;
    int lock_fd_ = -1;
    RegionRoutingSnapshot snapshot_;
};

}  // namespace mino::upgrade

#endif  // MINO_UPGRADE_ROUTING_CATALOG_H_
