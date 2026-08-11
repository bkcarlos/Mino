// Copyright 2026 The Mino Authors

#ifndef MINO_UPGRADE_ORCHESTRATOR_H_
#define MINO_UPGRADE_ORCHESTRATOR_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "mino/common/result.h"
#include "mino/upgrade/manifest.h"
#include "mino/upgrade/upgrade.h"

namespace mino::upgrade {

struct UpgradeStepPreview {
    UpgradePhase current = UpgradePhase::kPrepare;
    UpgradePhase next = UpgradePhase::kPrepare;
    bool terminal = false;
    std::string action;
};

class UpgradeOrchestrator final {
public:
    UpgradeOrchestrator(UpgradeManifestStore* store,
                        UpgradeControlPlane* control) noexcept
        : store_(store), control_(control) {}

    UpgradeStepPreview Preview() const;
    // Executes one idempotent side effect and one durable phase transition.
    // kWouldBlock means proof is not complete; no timer can override it.
    Status Step(uint64_t now_ns);
    // Progresses until terminal, blocked, or max_steps is reached.
    Status Execute(uint64_t now_ns, size_t max_steps = 16);
    Status Rollback(uint64_t now_ns);
    Status Fail(uint64_t now_ns, std::string reason);

private:
    Status ObserveAndCommit(uint64_t now_ns);

    UpgradeManifestStore* store_ = nullptr;
    UpgradeControlPlane* control_ = nullptr;
};

}  // namespace mino::upgrade

#endif  // MINO_UPGRADE_ORCHESTRATOR_H_
