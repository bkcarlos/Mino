// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include <csignal>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "mino/common/status.h"
#include "mino/config/deployment_config.h"
#include "mino/runtime/deployment/local_bus.h"
#include "mino/runtime/deployment/monitoring.h"
#include "mino/schema/codegen/artifact_codec.h"
#include "mino/schema/compiler.h"
#include "mino/schema/layout.h"
#include "mino/storage/recorder.h"

namespace {

using mino::Status;
using mino::StatusCode;
using mino::config::NodeDeploymentConfig;

volatile std::sig_atomic_t g_stop_requested = 0;

void RequestStop(int) { g_stop_requested = 1; }

uint64_t MonotonicNowNs() noexcept {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

uint64_t UnixNowNs() noexcept {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

Status Invalid(std::string message) {
    return Status::Error(StatusCode::kInvalidArgument, std::move(message));
}

struct ControlSchema {
    mino::schema::SchemaIdentity identity{0, {}, 0, 0};
    std::vector<std::byte> artifact;
};

mino::Result<ControlSchema> CompileControlSchema() {
    auto compiled = mino::schema::SchemaCompiler::Compile(
        "option schema_version = \"1.0\"; package mino.deployment; "
        "message ControlEvent { uint64 sequence = 1; }");
    if (!compiled.ok()) return compiled.status();
    if (compiled->types().size() != 1) {
        return Invalid("built-in control schema must contain exactly one type");
    }
    std::vector<mino::schema::LayoutPlan> layouts;
    layouts.reserve(compiled->types().size());
    for (const auto& descriptor : compiled->types()) {
        auto layout = mino::schema::LayoutPlanner::Plan(*descriptor, {});
        if (!layout.ok()) return layout.status();
        layouts.push_back(std::move(*layout));
    }
    auto encoded = mino::schema::codegen::EncodeDescriptorArtifact(*compiled,
                                                                    layouts);
    if (!encoded.ok()) return encoded.status();
    const std::span<const std::byte> bytes = std::as_bytes(
        std::span<const char>(encoded->data(), encoded->size()));
    return ControlSchema{
        .identity = compiled->types().front()->identity(),
        .artifact = std::vector<std::byte>(bytes.begin(), bytes.end()),
    };
}

mino::deployment::MonitoringConfig MonitoringConfigFrom(
    const NodeDeploymentConfig& config) {
    return mino::deployment::MonitoringConfig{
        .prometheus_enabled = config.monitoring.enabled,
        .otlp_enabled = config.monitoring.otlp_enabled,
        .aggregate_interval_ms = config.monitoring.aggregate_interval_ms,
        .process_start_unix_ns = UnixNowNs(),
        .prometheus =
            mino::observability::PrometheusHttpOptions{
                .bind_address = config.monitoring.bind_address,
                .port = config.monitoring.port,
                .request_bytes_limit = config.monitoring.request_bytes_limit,
                .header_count_limit = config.monitoring.header_count_limit,
                .response_bytes_limit = config.monitoring.response_bytes_limit,
                .connection_limit = config.monitoring.connection_limit,
                .worker_threads = config.monitoring.worker_threads,
                .read_timeout_ms = config.monitoring.read_timeout_ms,
                .write_timeout_ms = config.monitoring.write_timeout_ms,
                .accept_poll_ms = config.monitoring.accept_poll_ms,
            },
    };
}

mino::deployment::LocalBusConfig LocalBusConfigFrom(
    const NodeDeploymentConfig& config, const ControlSchema& schema) {
    mino::deployment::LocalBusConfig bus;
    bus.node_id = mino::NodeId{config.node.id};
    bus.security_domain_id = mino::SecurityDomainId{config.security_domain.id};
    bus.lease_epoch = 1;
    bus.lease_duration_ns = 60ull * 1'000'000'000ull;
    bus.region_id = config.region.id;
    bus.region_bytes = config.region.bytes;
    bus.topic_id_state_path = config.storage.runtime_dir / "topic_ids.state";
    bus.topics.push_back(mino::deployment::LocalTopicConfig{
        .name = config.supervisor.control_topic,
        .schema = schema.identity,
        .channel_capacity = config.supervisor.channel_capacity,
        .max_subscribers = config.supervisor.max_subscribers,
        .max_payload_bytes = config.supervisor.max_payload_bytes,
    });
    return bus;
}

mino::Result<std::unique_ptr<mino::storage::Recorder>> CreateRecorder(
    const NodeDeploymentConfig& config, const ControlSchema& schema) {
    const std::filesystem::path session_root =
        config.storage.data_dir / "recorder";
    mino::Result<std::unique_ptr<mino::storage::Recorder>> recorder =
        std::filesystem::exists(session_root)
            ? mino::storage::Recorder::Open(session_root)
            : mino::storage::Recorder::Create(
                  session_root,
                  mino::storage::RecordingSessionMetadata{
                      .recording_id = config.supervisor.recording_id,
                      .created_at_ns = UnixNowNs(),
                      .owner_id = config.node.id,
                      .owner_epoch = 1,
                      .config_version = 1,
                  });
    if (!recorder.ok()) return recorder.status();

    mino::storage::RecorderTopicConfig topic;
    topic.topic_id = mino::TopicId{1};
    topic.topic_name = config.supervisor.control_topic;
    topic.config_version = 1;
    topic.schemas.push_back(mino::storage::RecorderTopicSchema{
        .identity = schema.identity,
        .descriptor_artifact = schema.artifact,
    });
    const Status added = (*recorder)->AddTopic(topic);
    if (!added.ok()) return added;
    const Status started = (*recorder)->Start(MonotonicNowNs());
    if (!started.ok()) return started;
    return recorder;
}

Status ValidateSupervisorContract(const NodeDeploymentConfig& config) {
    if (config.supervisor.mode != "local") {
        return Invalid("mino-node supports only the explicit local supervisor mode");
    }
    if (config.bridge.enabled) {
        return Invalid("local mino-node does not assemble RemoteBridge; use a "
                       "bridge-capable node composition instead of pretending the "
                       "configured bridge is running");
    }
    if (config.monitoring.otlp_enabled) {
        return Invalid("local mino-node has no bounded OTLP sink; disable OTLP or "
                       "use a composition that injects one");
    }
    return Status::Ok();
}

void PrintUsage(std::ostream& output) {
    output << "mino-node: controlled local Mino node supervisor\n\n"
              "USAGE:\n"
              "  mino-node --config <deployment.toml> [--dry-run|--oneshot]\n";
}

}  // namespace

int main(int argc, char** argv) {
    std::string config_path;
    bool dry_run = false;
    bool oneshot = false;
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (argument == "--config") {
            if (++index >= argc || !config_path.empty()) {
                std::cerr << "mino-node: --config requires one value\n";
                return 2;
            }
            config_path = argv[index];
        } else if (argument == "--dry-run") {
            dry_run = true;
        } else if (argument == "--oneshot") {
            oneshot = true;
        } else if (argument == "--help" || argument == "-h") {
            PrintUsage(std::cout);
            return 0;
        } else {
            std::cerr << "mino-node: unknown option '" << argument << "'\n";
            return 2;
        }
    }
    if (config_path.empty() || (dry_run && oneshot)) {
        PrintUsage(std::cerr);
        return 2;
    }

    auto config = mino::config::LoadNodeDeploymentConfigFromTomlFile(config_path);
    if (!config.ok()) {
        std::cerr << "mino-node: " << config.status().ToString() << '\n';
        return 3;
    }
    const Status contract = ValidateSupervisorContract(*config);
    if (!contract.ok()) {
        std::cerr << "mino-node: " << contract.ToString() << '\n';
        return 4;
    }
    auto schema = CompileControlSchema();
    if (!schema.ok()) {
        std::cerr << "mino-node: control schema: "
                  << schema.status().ToString() << '\n';
        return 5;
    }
    if (dry_run) {
        std::cout << "mino-node dry-run ok: LocalBus + Monitoring"
                  << (config->supervisor.recorder_enabled ? " + Recorder" : "")
                  << "; RemoteBridge disabled\n";
        return 0;
    }
    if (!oneshot) {
        g_stop_requested = 0;
        if (std::signal(SIGTERM, RequestStop) == SIG_ERR ||
            std::signal(SIGINT, RequestStop) == SIG_ERR) {
            std::cerr << "mino-node: cannot install termination handlers\n";
            return 5;
        }
    }

    auto local_bus = mino::deployment::LocalBusDeployment::Create(
        LocalBusConfigFrom(*config, *schema));
    if (!local_bus.ok()) {
        std::cerr << "mino-node: LocalBus: "
                  << local_bus.status().ToString() << '\n';
        return 5;
    }
    auto monitoring = mino::deployment::MonitoringDeployment::Create(
        MonitoringConfigFrom(*config));
    if (!monitoring.ok()) {
        std::cerr << "mino-node: Monitoring: "
                  << monitoring.status().ToString() << '\n';
        return 5;
    }
    const Status monitoring_started = (*monitoring)->Start();
    if (!monitoring_started.ok()) {
        std::cerr << "mino-node: Monitoring: "
                  << monitoring_started.ToString() << '\n';
        return 5;
    }

    std::unique_ptr<mino::storage::Recorder> recorder;
    if (config->supervisor.recorder_enabled) {
        auto created = CreateRecorder(*config, *schema);
        if (!created.ok()) {
            (*monitoring)->Stop();
            std::cerr << "mino-node: Recorder: "
                      << created.status().ToString() << '\n';
            return 5;
        }
        recorder = std::move(*created);
    }

    if (!oneshot) {
        std::cout << "mino-node ready: " << config->node.name
                  << " monitoring_port=" << (*monitoring)->prometheus_port()
                  << '\n';
        while (g_stop_requested == 0) {
            if (recorder != nullptr) {
                auto pumped = recorder->Pump(MonotonicNowNs(), 64);
                if (!pumped.ok()) {
                    std::cerr << "mino-node: Recorder pump: "
                              << pumped.status().ToString() << '\n';
                    g_stop_requested = 1;
                    break;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    Status recorder_stopped = Status::Ok();
    if (recorder != nullptr) {
        recorder_stopped = recorder->Stop(MonotonicNowNs());
    }
    (*monitoring)->Stop();
    if (!recorder_stopped.ok()) {
        std::cerr << "mino-node: Recorder stop: "
                  << recorder_stopped.ToString() << '\n';
        return 5;
    }
    std::cout << "mino-node stopped cleanly\n";
    return 0;
}
