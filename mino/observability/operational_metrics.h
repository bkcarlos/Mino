// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_OBSERVABILITY_OPERATIONAL_METRICS_H_
#define MINO_OBSERVABILITY_OPERATIONAL_METRICS_H_

#include "mino/common/status.h"
#include "mino/observability/metrics.h"

namespace mino::observability {

// Process-wide, label-free operational metric contract. Node, topic, peer, and
// certificate identities are deliberately excluded: deployments that need those
// dimensions aggregate separate process endpoints instead of creating dynamic
// label sets in the data path.
struct OperationalMetrics {
    CounterMetric* queue_near_full_total = nullptr;
    CounterMetric* queue_full_total = nullptr;
    CounterMetric* queue_dropped_total = nullptr;
    GaugeMetric* queue_depth = nullptr;
    GaugeMetric* queue_capacity = nullptr;

    CounterMetric* slab_allocations_total = nullptr;
    CounterMetric* slab_allocation_failures_total = nullptr;
    CounterMetric* slab_fallback_total = nullptr;
    CounterMetric* slab_corruption_total = nullptr;

    CounterMetric* lease_expirations_total = nullptr;
    GaugeMetric* lease_oldest_heartbeat_age_seconds = nullptr;

    GaugeMetric* bridge_configured = nullptr;
    GaugeMetric* bridge_connected = nullptr;
    CounterMetric* bridge_connections_total = nullptr;
    CounterMetric* bridge_disconnects_total = nullptr;
    CounterMetric* bridge_reconnects_total = nullptr;
    CounterMetric* bridge_reconnect_failures_total = nullptr;
    CounterMetric* bridge_protocol_failures_total = nullptr;

    CounterMetric* storage_writes_total = nullptr;
    CounterMetric* storage_syncs_total = nullptr;
    CounterMetric* storage_write_failures_total = nullptr;
    CounterMetric* storage_sync_failures_total = nullptr;
    CounterMetric* storage_errors_total = nullptr;
    GaugeMetric* storage_pending_bytes = nullptr;

    CounterMetric* otlp_export_submitted_total = nullptr;
    CounterMetric* otlp_export_dropped_total = nullptr;
    CounterMetric* otlp_exported_total = nullptr;
    CounterMetric* otlp_export_failures_total = nullptr;
    GaugeMetric* otlp_export_queue_depth = nullptr;
    GaugeMetric* otlp_export_queue_capacity = nullptr;

    CounterMetric* capacity_rejections_total = nullptr;
    GaugeMetric* capacity_headroom_bytes = nullptr;
    GaugeMetric* capacity_min_headroom_bytes = nullptr;

    CounterMetric* tls_handshake_failures_total = nullptr;
    GaugeMetric* tls_certificate_expiry_unixtime_seconds = nullptr;
    CounterMetric* acl_denied_total = nullptr;

    CounterMetric* prometheus_http_accepted_total = nullptr;
    CounterMetric* prometheus_http_rejected_total = nullptr;
    CounterMetric* prometheus_http_requests_total = nullptr;
    CounterMetric* prometheus_http_failures_total = nullptr;
    GaugeMetric* prometheus_http_active_connections = nullptr;

    GaugeMetric* monitoring_up = nullptr;
    GaugeMetric* monitoring_last_snapshot_unixtime_seconds = nullptr;
};

// Registration is a cold-path operation and must complete before any snapshots
// are taken. On error, callers must discard the registry and metric handles.
Status RegisterOperationalMetrics(MetricRegistry& registry,
                                  OperationalMetrics* metrics) noexcept;

}  // namespace mino::observability

#endif  // MINO_OBSERVABILITY_OPERATIONAL_METRICS_H_
