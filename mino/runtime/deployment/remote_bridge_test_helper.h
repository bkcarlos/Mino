// Copyright 2026 The Mino Authors

#ifndef MINO_RUNTIME_DEPLOYMENT_REMOTE_BRIDGE_TEST_HELPER_H_
#define MINO_RUNTIME_DEPLOYMENT_REMOTE_BRIDGE_TEST_HELPER_H_

#include <memory>
#include <optional>

#include "mino/runtime/deployment/remote_bridge.h"

namespace mino::deployment::testing {

// Test-only composition seam. This target may construct a plaintext driver;
// production targets cannot depend on it because its Bazel target is testonly.
class RemoteBridgeTestFactory final {
public:
    static Result<std::unique_ptr<RemoteBridge>> Create(
        RemoteBridgeConfig config, bridge::BridgeIngressPort* ingress,
        std::shared_ptr<bridge::DescriptorAuth> descriptor_auth,
        std::shared_ptr<capacity::CapacityController> capacity_controller = {},
        std::optional<capacity::ResourceVector> capacity_charge =
            std::nullopt) noexcept;

    static Result<capacity::ResourceVector> EstimateResources(
        const RemoteBridgeConfig& config) noexcept;
};

}  // namespace mino::deployment::testing

#endif  // MINO_RUNTIME_DEPLOYMENT_REMOTE_BRIDGE_TEST_HELPER_H_
