// Copyright 2026 The Mino Authors

#include "mino/runtime/deployment/remote_bridge_test_helper.h"

#include <utility>

namespace mino::deployment {

Result<capacity::ResourceVector> EstimateRemoteBridgeResourcesForTesting(
    const RemoteBridgeConfig& config) noexcept;

namespace testing {

Result<std::unique_ptr<RemoteBridge>> RemoteBridgeTestFactory::Create(
    RemoteBridgeConfig config, bridge::BridgeIngressPort* ingress,
    std::shared_ptr<bridge::DescriptorAuth> descriptor_auth,
    std::shared_ptr<capacity::CapacityController> capacity_controller,
    std::optional<capacity::ResourceVector> capacity_charge) noexcept {
    return RemoteBridge::CreateImpl(
        std::move(config), ingress, std::move(descriptor_auth),
        std::move(capacity_controller), capacity_charge,
        /*allow_plaintext_for_testing=*/true);
}

Result<capacity::ResourceVector> RemoteBridgeTestFactory::EstimateResources(
    const RemoteBridgeConfig& config) noexcept {
    return EstimateRemoteBridgeResourcesForTesting(config);
}

}  // namespace testing
}  // namespace mino::deployment
