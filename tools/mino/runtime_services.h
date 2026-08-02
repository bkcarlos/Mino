// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef TOOLS_MINO_RUNTIME_SERVICES_H_
#define TOOLS_MINO_RUNTIME_SERVICES_H_

#include <atomic>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "mino/common/result.h"
#include "mino/runtime/deployment/local_bus.h"
#include "mino/storage/replay_engine.h"
#include "tools/mino/storage_commands.h"

namespace mino::tools {

struct RuntimeTopicConfig {
    deployment::LocalTopicConfig bus;
    std::vector<std::byte> descriptor_artifact;
    uint32_t record_partitions = 1;
};

struct RuntimeConfig {
    deployment::LocalBusConfig bus;
    std::vector<RuntimeTopicConfig> topics;
    uint64_t record_stop_after_records = 0;
    uint64_t record_max_runtime_ms = 0;
    uint64_t record_poll_interval_ms = 1;
    size_t recorder_buffer_bytes = 64u * 1024u * 1024u;
    size_t recorder_queue_capacity = 4096;
    uint64_t max_segment_bytes = 64u * 1024u * 1024u;
};

Result<RuntimeConfig> LoadRuntimeConfig(
    const std::filesystem::path& path) noexcept;

class BusRecorderServiceLauncher final : public RecorderServiceLauncher {
public:
    BusRecorderServiceLauncher(deployment::LocalBusDeployment& deployment,
                               RuntimeConfig config,
                               volatile std::sig_atomic_t* signal_stop = nullptr);

    Status Run(const std::filesystem::path& session_root) noexcept override;
    void RequestStop() noexcept;
    bool running() const noexcept;

private:
    deployment::LocalBusDeployment* deployment_ = nullptr;
    RuntimeConfig config_;
    volatile std::sig_atomic_t* signal_stop_ = nullptr;
    std::atomic<bool> stop_requested_{false};
    std::atomic<bool> running_{false};
    std::atomic<bool> run_claimed_{false};
};

class BusReplayPublisherAdapter final
    : public storage::ReplayPublisherAdapter {
public:
    explicit BusReplayPublisherAdapter(
        deployment::LocalBusDeployment& deployment) noexcept;

    Status Publish(
        const storage::ReplayPublishRequest& request) noexcept override;

private:
    deployment::LocalBusDeployment* deployment_ = nullptr;
    std::mutex mutex_;
    std::unordered_map<std::string, std::unique_ptr<BusPublisher>> publishers_;
};

// Owns the complete process assembly. Dependencies, Bus endpoints, cached replay
// publishers, and recorder launcher all remain alive until command completion.
class RuntimeCommandServices final {
public:
    static Result<std::unique_ptr<RuntimeCommandServices>> Create(
        const std::filesystem::path& config_path,
        volatile std::sig_atomic_t* signal_stop = nullptr) noexcept;

    ~RuntimeCommandServices();
    RuntimeCommandServices(const RuntimeCommandServices&) = delete;
    RuntimeCommandServices& operator=(const RuntimeCommandServices&) = delete;

    StorageCommandServices services() noexcept;
    Bus& bus() noexcept;
    BusRecorderServiceLauncher& recorder_launcher() noexcept;
    const RuntimeConfig& config() const noexcept { return config_; }

private:
    RuntimeCommandServices(
        RuntimeConfig config,
        std::unique_ptr<deployment::LocalBusDeployment> deployment,
        std::unique_ptr<BusRecorderServiceLauncher> recorder,
        std::unique_ptr<BusReplayPublisherAdapter> replay) noexcept;

    RuntimeConfig config_;
    std::unique_ptr<deployment::LocalBusDeployment> deployment_;
    std::unique_ptr<BusRecorderServiceLauncher> recorder_;
    std::unique_ptr<BusReplayPublisherAdapter> replay_;
};

}  // namespace mino::tools

#endif  // TOOLS_MINO_RUNTIME_SERVICES_H_
