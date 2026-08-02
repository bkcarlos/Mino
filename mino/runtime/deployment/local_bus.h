// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_RUNTIME_DEPLOYMENT_LOCAL_BUS_H_
#define MINO_RUNTIME_DEPLOYMENT_LOCAL_BUS_H_

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "mino/common/result.h"
#include "mino/runtime/bus.h"
#include "mino/schema/descriptor.h"

namespace mino::deployment {

inline constexpr size_t kMaximumLocalDeploymentTopics = 1024;
inline constexpr uint64_t kMaximumLocalRegionBytes = 1ull << 30;
inline constexpr uint32_t kMaximumLocalChannelCapacity = 1u << 16;

struct LocalTopicConfig {
    std::string name;
    schema::SchemaIdentity schema{0, {}, 0, 0};
    uint32_t channel_capacity = 256;
    uint32_t max_subscribers = 16;
    size_t max_payload_bytes = 1024u * 1024u;
};

struct LocalBusConfig {
    NodeId node_id{};
    uint64_t lease_epoch = 1;
    uint64_t lease_duration_ns = 60ull * 1'000'000'000ull;
    uint32_t region_id = 1;
    uint64_t region_bytes = 64u * 1024u * 1024u;
    std::filesystem::path topic_id_state_path;
    std::vector<LocalTopicConfig> topics;
};

// Production, local-only Bus assembly. Each configured topic owns a real
// BroadcastChannel and a fixed, bounded canonical-payload region retained by
// LocalPublicationBinding. The deployment intentionally supports one publisher
// per topic, matching BroadcastChannel's single-publisher contract.
class LocalBusDeployment final {
public:
    static Result<std::unique_ptr<LocalBusDeployment>> Create(
        LocalBusConfig config) noexcept;

    ~LocalBusDeployment();
    LocalBusDeployment(const LocalBusDeployment&) = delete;
    LocalBusDeployment& operator=(const LocalBusDeployment&) = delete;
    LocalBusDeployment(LocalBusDeployment&&) = delete;
    LocalBusDeployment& operator=(LocalBusDeployment&&) = delete;

    Bus& bus() noexcept;
    const Bus& bus() const noexcept;

private:
    class Impl;
    explicit LocalBusDeployment(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mino::deployment

#endif  // MINO_RUNTIME_DEPLOYMENT_LOCAL_BUS_H_
