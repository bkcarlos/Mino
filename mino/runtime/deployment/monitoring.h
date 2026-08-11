// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_RUNTIME_DEPLOYMENT_MONITORING_H_
#define MINO_RUNTIME_DEPLOYMENT_MONITORING_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "mino/capacity/capacity.h"
#include "mino/common/result.h"
#include "mino/observability/metrics.h"
#include "mino/observability/operational_metrics.h"
#include "mino/observability/otlp_exporter.h"
#include "mino/observability/prometheus_http_endpoint.h"
#include "mino/runtime/deployment/local_bus.h"
#include "mino/runtime/deployment/remote_bridge.h"
#include "mino/security/tls.h"
#include "mino/shm/allocator/central_slab.h"
#include "mino/shm/allocator/large_object_pool.h"
#include "mino/storage/recorder.h"

namespace mino::deployment {

inline constexpr size_t kOtlpSnapshotQueueCapacity = 16;

struct MonitoringSources {
    // Non-owning production sources. Every source must outlive the deployment.
    // Polling occurs only on the monitoring snapshot thread.
    const LocalBusDeployment* local_bus = nullptr;
    const RemoteBridge* remote_bridge = nullptr;
    const storage::Recorder* recorder = nullptr;
    const capacity::CapacityController* capacity = nullptr;
    const security::TlsChannelFactory* tls = nullptr;
    std::vector<const CentralSlabAllocator*> slab_allocators;
    std::vector<const LargeObjectPool*> large_object_pools;
};

struct MonitoringConfig {
    bool prometheus_enabled = true;
    bool otlp_enabled = false;
    uint32_t aggregate_interval_ms = 1000;
    uint64_t process_start_unix_ns = 0;
    observability::PrometheusHttpOptions prometheus;
};

// Production monitoring assembly. It owns only cold-path threads and bounded
// queues; callers record through the returned label-free metric handles. OTLP
// network ownership stays in the caller-provided non-blocking transactional sink.
class MonitoringDeployment final {
public:
    static Result<std::unique_ptr<MonitoringDeployment>> Create(
        MonitoringConfig config,
        observability::OtlpJsonSink* otlp_sink = nullptr) noexcept;
    static Result<std::unique_ptr<MonitoringDeployment>> Create(
        MonitoringConfig config, MonitoringSources sources,
        observability::OtlpJsonSink* otlp_sink = nullptr) noexcept;

    ~MonitoringDeployment();
    MonitoringDeployment(const MonitoringDeployment&) = delete;
    MonitoringDeployment& operator=(const MonitoringDeployment&) = delete;
    MonitoringDeployment(MonitoringDeployment&&) = delete;
    MonitoringDeployment& operator=(MonitoringDeployment&&) = delete;

    Status Start();
    void Stop() noexcept;
    bool running() const noexcept;

    observability::MetricRegistry& registry() noexcept;
    const observability::MetricRegistry& registry() const noexcept;
    observability::OperationalMetrics& metrics() noexcept;
    const observability::OperationalMetrics& metrics() const noexcept;
    uint16_t prometheus_port() const noexcept;

private:
    class Impl;
    explicit MonitoringDeployment(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mino::deployment

#endif  // MINO_RUNTIME_DEPLOYMENT_MONITORING_H_
