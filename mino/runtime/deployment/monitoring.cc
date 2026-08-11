// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/deployment/monitoring.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

#include "mino/observability/exporter.h"

namespace mino::deployment {
namespace {

uint64_t UnixNowNs() noexcept {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

uint64_t MonotonicNowNs() noexcept {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

Status Validate(const MonitoringConfig& config,
                const observability::OtlpJsonSink* sink) {
    if (!config.prometheus_enabled && !config.otlp_enabled) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "at least one monitoring exporter must be enabled");
    }
    if (config.aggregate_interval_ms < 100 ||
        config.aggregate_interval_ms > 60'000) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "monitoring aggregate interval is out of bounds");
    }
    if (config.otlp_enabled && sink == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "OTLP monitoring requires a bounded sink");
    }
    return Status::Ok();
}

Status ValidateSources(const MonitoringSources& sources) {
    for (const CentralSlabAllocator* allocator : sources.slab_allocators) {
        if (allocator == nullptr) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "monitoring slab source is null");
        }
    }
    for (const LargeObjectPool* pool : sources.large_object_pools) {
        if (pool == nullptr) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "monitoring large-object source is null");
        }
    }
    return Status::Ok();
}

void Set(observability::GaugeMetric* metric, uint64_t value) noexcept {
    if (metric != nullptr) metric->gauge().Set(value, 0);
}

void Increment(observability::CounterMetric* metric) noexcept {
    if (metric != nullptr) metric->counter().Increment(0);
}

void AddDelta(observability::CounterMetric* metric, uint64_t current,
              uint64_t* previous) noexcept {
    if (previous == nullptr) return;
    const uint64_t delta = current >= *previous ? current - *previous : current;
    *previous = current;
    if (metric != nullptr && delta != 0) metric->counter().Add(delta, 0);
}

uint64_t SaturatingAdd(uint64_t lhs, uint64_t rhs) noexcept {
    return rhs > std::numeric_limits<uint64_t>::max() - lhs
               ? std::numeric_limits<uint64_t>::max()
               : lhs + rhs;
}

uint64_t ResourceBytes(const capacity::ResourceVector& value) noexcept {
    uint64_t total = value.shm_bytes;
    for (uint64_t slab : value.slab_bytes) total = SaturatingAdd(total, slab);
    total = SaturatingAdd(total, value.bridge_egress_bytes);
    total = SaturatingAdd(total, value.schema_buffer_bytes);
    total = SaturatingAdd(total, value.recorder_buffer_bytes);
    return total;
}

}  // namespace

class MonitoringDeployment::Impl final {
public:
    Impl(MonitoringConfig config, MonitoringSources sources,
         observability::OtlpJsonSink* sink, uint64_t process_start_unix_ns)
        : config_(std::move(config)), sources_(std::move(sources)),
          registry_(process_start_unix_ns), otlp_sink_(sink) {}

    Status Initialize() {
        MINO_RETURN_IF_ERROR(
            observability::RegisterOperationalMetrics(registry_, &metrics_));
        if (config_.prometheus_enabled) {
            prometheus_ =
                std::make_unique<observability::PrometheusHttpEndpoint>(
                    registry_, metrics_, config_.prometheus);
        }
        if (config_.otlp_enabled) {
            otlp_exporter_ =
                std::make_unique<observability::OtlpJsonExporter>(*otlp_sink_);
            pipeline_metrics_ = {
                .submitted_total = metrics_.otlp_export_submitted_total,
                .dropped_total = metrics_.otlp_export_dropped_total,
                .exported_total = metrics_.otlp_exported_total,
                .failures_total = metrics_.otlp_export_failures_total,
                .queue_depth = metrics_.otlp_export_queue_depth,
                .queue_capacity = metrics_.otlp_export_queue_capacity,
            };
            otlp_pipeline_ = std::make_unique<observability::ExportPipeline<
                kOtlpSnapshotQueueCapacity>>(*otlp_exporter_,
                                             &pipeline_metrics_);
        }
        return Status::Ok();
    }

    Status Start() {
        std::lock_guard lock(lifecycle_mutex_);
        if (running_.load(std::memory_order_acquire)) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "monitoring deployment is already running");
        }
        stopping_.store(false, std::memory_order_release);
        if (prometheus_ != nullptr) {
            const Status status = prometheus_->Start();
            if (!status.ok()) {
                Increment(metrics_.prometheus_http_failures_total);
                return status;
            }
        }
        try {
            snapshot_thread_ = std::thread([this] { SnapshotLoop(); });
            if (otlp_pipeline_ != nullptr) {
                export_thread_ = std::thread([this] { ExportLoop(); });
            }
        } catch (...) {
            stopping_.store(true, std::memory_order_release);
            condition_.notify_all();
            if (snapshot_thread_.joinable()) snapshot_thread_.join();
            if (export_thread_.joinable()) export_thread_.join();
            if (prometheus_ != nullptr) prometheus_->Stop();
            return Status::Error(StatusCode::kResourceExhausted,
                                 "cannot start monitoring worker threads");
        }
        Set(metrics_.monitoring_up, 1);
        running_.store(true, std::memory_order_release);
        return Status::Ok();
    }

    void Stop() noexcept {
        std::lock_guard lock(lifecycle_mutex_);
        if (!running_.exchange(false, std::memory_order_acq_rel) &&
            !snapshot_thread_.joinable() && !export_thread_.joinable()) {
            return;
        }
        stopping_.store(true, std::memory_order_release);
        condition_.notify_all();
        if (prometheus_ != nullptr) prometheus_->Stop();
        if (snapshot_thread_.joinable()) snapshot_thread_.join();
        if (export_thread_.joinable()) export_thread_.join();
        Set(metrics_.monitoring_up, 0);
        Set(metrics_.otlp_export_queue_depth, 0);
    }

    bool running() const noexcept {
        return running_.load(std::memory_order_acquire);
    }

    observability::MetricRegistry& registry() noexcept { return registry_; }
    const observability::MetricRegistry& registry() const noexcept {
        return registry_;
    }
    observability::OperationalMetrics& metrics() noexcept { return metrics_; }
    const observability::OperationalMetrics& metrics() const noexcept {
        return metrics_;
    }
    uint16_t prometheus_port() const noexcept {
        return prometheus_ == nullptr ? 0 : prometheus_->bound_port();
    }

private:
    struct SourceTotals {
        uint64_t queue_full = 0;
        uint64_t queue_dropped = 0;
        uint64_t slab_allocations = 0;
        uint64_t slab_failures = 0;
        uint64_t slab_fallback = 0;
        uint64_t lease_expirations = 0;
        uint64_t bridge_connections = 0;
        uint64_t bridge_disconnects = 0;
        uint64_t bridge_reconnects = 0;
        uint64_t bridge_reconnect_failures = 0;
        uint64_t bridge_protocol_failures = 0;
        uint64_t storage_writes = 0;
        uint64_t storage_syncs = 0;
        uint64_t storage_write_failures = 0;
        uint64_t storage_sync_failures = 0;
        uint64_t storage_errors = 0;
        uint64_t capacity_rejections = 0;
        uint64_t tls_handshake_failures = 0;
        uint64_t acl_denials = 0;
    };

    void PollSources() noexcept {
        if (sources_.local_bus != nullptr) {
            const LocalBusOperationalStats stats =
                sources_.local_bus->OperationalStats(MonotonicNowNs());
            Set(metrics_.queue_depth, stats.queue_depth);
            Set(metrics_.queue_capacity, stats.queue_capacity);
            AddDelta(metrics_.queue_full_total, stats.queue_full_events,
                     &source_totals_.queue_full);
            AddDelta(metrics_.queue_dropped_total, stats.queue_dropped,
                     &source_totals_.queue_dropped);
            AddDelta(metrics_.lease_expirations_total, stats.lease_expirations,
                     &source_totals_.lease_expirations);
            Set(metrics_.lease_oldest_heartbeat_age_seconds,
                stats.oldest_heartbeat_age_ns / 1'000'000'000ull);
            const bool near = stats.queue_capacity != 0 &&
                              stats.queue_depth >=
                                  stats.queue_capacity - stats.queue_capacity / 10;
            if (near && !queue_was_near_full_) {
                Increment(metrics_.queue_near_full_total);
            }
            queue_was_near_full_ = near;
        }

        uint64_t slab_allocations = 0;
        uint64_t slab_failures = 0;
        uint64_t slab_fallback = 0;
        for (const CentralSlabAllocator* allocator : sources_.slab_allocators) {
            const AllocatorLocalCacheStats stats = allocator->local_cache_stats();
            slab_allocations = SaturatingAdd(slab_allocations, stats.allocations);
            slab_failures = SaturatingAdd(slab_failures, stats.exhaustions);
            slab_fallback = SaturatingAdd(slab_fallback, stats.fallback_scans);
        }
        for (const LargeObjectPool* pool : sources_.large_object_pools) {
            const LargeObjectPoolMetrics stats = pool->metrics();
            const LargeObjectNumaStats numa = pool->numa_stats();
            slab_allocations = SaturatingAdd(slab_allocations, stats.allocations);
            slab_failures =
                SaturatingAdd(slab_failures, stats.allocation_failures);
            slab_fallback = SaturatingAdd(
                slab_fallback, SaturatingAdd(stats.huge_page_fallback_allocations,
                                             numa.fallback_allocations));
        }
        AddDelta(metrics_.slab_allocations_total, slab_allocations,
                 &source_totals_.slab_allocations);
        AddDelta(metrics_.slab_allocation_failures_total, slab_failures,
                 &source_totals_.slab_failures);
        AddDelta(metrics_.slab_fallback_total, slab_fallback,
                 &source_totals_.slab_fallback);

        if (sources_.remote_bridge != nullptr) {
            const RemoteBridgeOperationalStats stats =
                sources_.remote_bridge->OperationalStats();
            Set(metrics_.bridge_configured,
                stats.configured_connections == 0 ? 0 : 1);
            Set(metrics_.bridge_connected,
                stats.connected_connections == 0 ? 0 : 1);
            AddDelta(metrics_.bridge_connections_total, stats.connections,
                     &source_totals_.bridge_connections);
            AddDelta(metrics_.bridge_disconnects_total, stats.disconnects,
                     &source_totals_.bridge_disconnects);
            AddDelta(metrics_.bridge_reconnects_total, stats.reconnects,
                     &source_totals_.bridge_reconnects);
            AddDelta(metrics_.bridge_reconnect_failures_total,
                     stats.reconnect_failures,
                     &source_totals_.bridge_reconnect_failures);
            AddDelta(metrics_.bridge_protocol_failures_total,
                     stats.protocol_failures,
                     &source_totals_.bridge_protocol_failures);
            AddDelta(metrics_.acl_denied_total, stats.acl_denials,
                     &source_totals_.acl_denials);
        }

        if (sources_.recorder != nullptr) {
            const storage::RecorderMetrics stats = sources_.recorder->metrics();
            AddDelta(metrics_.storage_writes_total, stats.written_records,
                     &source_totals_.storage_writes);
            AddDelta(metrics_.storage_syncs_total, stats.durable_records,
                     &source_totals_.storage_syncs);
            AddDelta(metrics_.storage_write_failures_total, stats.write_failures,
                     &source_totals_.storage_write_failures);
            AddDelta(metrics_.storage_sync_failures_total, stats.sync_failures,
                     &source_totals_.storage_sync_failures);
            AddDelta(metrics_.storage_errors_total, stats.writer_failures,
                     &source_totals_.storage_errors);
            Set(metrics_.storage_pending_bytes, stats.pending_bytes);
        }

        if (sources_.capacity != nullptr) {
            const capacity::CapacitySnapshot snapshot =
                sources_.capacity->Snapshot();
            AddDelta(metrics_.capacity_rejections_total,
                     snapshot.rejected_reservations,
                     &source_totals_.capacity_rejections);
            Set(metrics_.capacity_headroom_bytes,
                ResourceBytes(snapshot.data_plane_headroom));
            Set(metrics_.capacity_min_headroom_bytes,
                ResourceBytes(snapshot.budget.emergency_reserve));
        }

        if (sources_.tls != nullptr) {
            const security::TlsOperationalStats stats =
                sources_.tls->OperationalStats();
            AddDelta(metrics_.tls_handshake_failures_total,
                     stats.handshake_failures,
                     &source_totals_.tls_handshake_failures);
            Set(metrics_.tls_certificate_expiry_unixtime_seconds,
                stats.certificate_expiry_unix_seconds);
        }
    }

    void SnapshotLoop() noexcept {
        const auto interval =
            std::chrono::milliseconds(config_.aggregate_interval_ms);
        while (!stopping_.load(std::memory_order_acquire)) {
            const uint64_t now_ns = UnixNowNs();
            PollSources();
            Set(metrics_.monitoring_last_snapshot_unixtime_seconds,
                now_ns / 1'000'000'000ull);
            if (otlp_pipeline_ != nullptr) {
                observability::TelemetrySnapshot snapshot;
                registry_.TakeSnapshot(now_ns, &snapshot);
                (void)otlp_pipeline_->TrySubmit(snapshot);
                condition_.notify_all();
            }
            std::unique_lock lock(wait_mutex_);
            condition_.wait_for(lock, interval, [this] {
                return stopping_.load(std::memory_order_acquire);
            });
        }
    }

    void ExportLoop() noexcept {
        while (!stopping_.load(std::memory_order_acquire)) {
            if (otlp_pipeline_->DrainOne()) continue;
            std::unique_lock lock(wait_mutex_);
            condition_.wait_for(lock, std::chrono::milliseconds(20), [this] {
                return stopping_.load(std::memory_order_acquire);
            });
        }
    }

    MonitoringConfig config_;
    MonitoringSources sources_;
    SourceTotals source_totals_;
    bool queue_was_near_full_ = false;
    observability::MetricRegistry registry_;
    observability::OperationalMetrics metrics_;
    observability::OtlpJsonSink* otlp_sink_ = nullptr;
    observability::ExportPipelineMetrics pipeline_metrics_;
    std::unique_ptr<observability::PrometheusHttpEndpoint> prometheus_;
    std::unique_ptr<observability::OtlpJsonExporter> otlp_exporter_;
    std::unique_ptr<observability::ExportPipeline<
        kOtlpSnapshotQueueCapacity>>
        otlp_pipeline_;
    std::atomic<bool> stopping_{false};
    std::atomic<bool> running_{false};
    std::thread snapshot_thread_;
    std::thread export_thread_;
    std::mutex lifecycle_mutex_;
    std::mutex wait_mutex_;
    std::condition_variable condition_;
};

Result<std::unique_ptr<MonitoringDeployment>> MonitoringDeployment::Create(
    MonitoringConfig config,
    observability::OtlpJsonSink* otlp_sink) noexcept {
    return Create(std::move(config), MonitoringSources{}, otlp_sink);
}

Result<std::unique_ptr<MonitoringDeployment>> MonitoringDeployment::Create(
    MonitoringConfig config, MonitoringSources sources,
    observability::OtlpJsonSink* otlp_sink) noexcept {
    const Status validation = Validate(config, otlp_sink);
    if (!validation.ok()) return validation;
    const Status source_validation = ValidateSources(sources);
    if (!source_validation.ok()) return source_validation;
    try {
        const uint64_t process_start =
            config.process_start_unix_ns == 0 ? UnixNowNs()
                                              : config.process_start_unix_ns;
        auto impl = std::make_unique<Impl>(std::move(config), std::move(sources),
                                          otlp_sink, process_start);
        const Status initialized = impl->Initialize();
        if (!initialized.ok()) return initialized;
        return std::unique_ptr<MonitoringDeployment>(
            new MonitoringDeployment(std::move(impl)));
    } catch (...) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "cannot allocate monitoring deployment");
    }
}

MonitoringDeployment::MonitoringDeployment(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

MonitoringDeployment::~MonitoringDeployment() { impl_->Stop(); }

Status MonitoringDeployment::Start() { return impl_->Start(); }

void MonitoringDeployment::Stop() noexcept { impl_->Stop(); }

bool MonitoringDeployment::running() const noexcept { return impl_->running(); }

observability::MetricRegistry& MonitoringDeployment::registry() noexcept {
    return impl_->registry();
}

const observability::MetricRegistry& MonitoringDeployment::registry() const
    noexcept {
    return impl_->registry();
}

observability::OperationalMetrics& MonitoringDeployment::metrics() noexcept {
    return impl_->metrics();
}

const observability::OperationalMetrics& MonitoringDeployment::metrics() const
    noexcept {
    return impl_->metrics();
}

uint16_t MonitoringDeployment::prometheus_port() const noexcept {
    return impl_->prometheus_port();
}

}  // namespace mino::deployment
