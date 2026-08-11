// Copyright 2026 The Mino Authors

#ifndef MINO_RUNTIME_DEPLOYMENT_UPGRADE_SUPERVISOR_H_
#define MINO_RUNTIME_DEPLOYMENT_UPGRADE_SUPERVISOR_H_

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>

#include "mino/common/result.h"
#include "mino/upgrade/upgrade.h"

namespace mino::deployment {

inline constexpr size_t kMaximumUpgradeControlRequestBytes = 16u * 1024u;

// Owner-only Unix control socket hosted by the production process that owns the
// real Coordinator/Bus/Region composition. ServeOne is bounded and intended to
// be called by the supervisor event loop; it never accepts evidence fields.
class ProductionUpgradeSupervisor final {
public:
    static Result<std::unique_ptr<ProductionUpgradeSupervisor>> Create(
        std::filesystem::path socket_path,
        upgrade::UpgradeControlPlane* production_control) noexcept;

    ~ProductionUpgradeSupervisor();
    ProductionUpgradeSupervisor(const ProductionUpgradeSupervisor&) = delete;
    ProductionUpgradeSupervisor& operator=(const ProductionUpgradeSupervisor&) =
        delete;

    Status ServeOne(uint64_t now_ns) noexcept;
    const std::filesystem::path& socket_path() const noexcept {
        return socket_path_;
    }

private:
    ProductionUpgradeSupervisor(std::filesystem::path socket_path,
                                upgrade::UpgradeControlPlane* control,
                                int listen_fd) noexcept;

    std::filesystem::path socket_path_;
    upgrade::UpgradeControlPlane* control_ = nullptr;
    int listen_fd_ = -1;
};

}  // namespace mino::deployment

#endif  // MINO_RUNTIME_DEPLOYMENT_UPGRADE_SUPERVISOR_H_
