// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/observability/operational_metrics.h"

namespace mino::observability {

Status RegisterOperationalMetrics(MetricRegistry& registry,
                                  OperationalMetrics* metrics) noexcept {
    if (metrics == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument);
    }
    OperationalMetrics registered;

#define MINO_REGISTER_COUNTER(field, name)                         \
    do {                                                           \
        const Status status =                                      \
            registry.RegisterCounter((name), &registered.field);   \
        if (!status.ok()) return status;                            \
    } while (false)
#define MINO_REGISTER_GAUGE(field, name)                           \
    do {                                                           \
        const Status status =                                      \
            registry.RegisterGauge((name), &registered.field);     \
        if (!status.ok()) return status;                            \
    } while (false)

    MINO_REGISTER_COUNTER(queue_near_full_total,
                          "mino_queue_near_full_total");
    MINO_REGISTER_COUNTER(queue_full_total, "mino_queue_full_total");
    MINO_REGISTER_COUNTER(queue_dropped_total, "mino_queue_dropped_total");
    MINO_REGISTER_GAUGE(queue_depth, "mino_queue_depth");
    MINO_REGISTER_GAUGE(queue_capacity, "mino_queue_capacity");
    MINO_REGISTER_COUNTER(slab_allocations_total,
                          "mino_slab_allocations_total");
    MINO_REGISTER_COUNTER(slab_allocation_failures_total,
                          "mino_slab_allocation_failures_total");
    MINO_REGISTER_COUNTER(slab_fallback_total, "mino_slab_fallback_total");
    MINO_REGISTER_COUNTER(slab_corruption_total,
                          "mino_slab_corruption_total");
    MINO_REGISTER_COUNTER(lease_expirations_total,
                          "mino_lease_expirations_total");
    MINO_REGISTER_GAUGE(lease_oldest_heartbeat_age_seconds,
                        "mino_lease_oldest_heartbeat_age_seconds");
    MINO_REGISTER_GAUGE(bridge_configured, "mino_bridge_configured");
    MINO_REGISTER_GAUGE(bridge_connected, "mino_bridge_connected");
    MINO_REGISTER_COUNTER(bridge_connections_total,
                          "mino_bridge_connections_total");
    MINO_REGISTER_COUNTER(bridge_disconnects_total,
                          "mino_bridge_disconnects_total");
    MINO_REGISTER_COUNTER(bridge_reconnects_total,
                          "mino_bridge_reconnects_total");
    MINO_REGISTER_COUNTER(bridge_reconnect_failures_total,
                          "mino_bridge_reconnect_failures_total");
    MINO_REGISTER_COUNTER(bridge_protocol_failures_total,
                          "mino_bridge_protocol_failures_total");
    MINO_REGISTER_COUNTER(storage_writes_total, "mino_storage_writes_total");
    MINO_REGISTER_COUNTER(storage_syncs_total, "mino_storage_syncs_total");
    MINO_REGISTER_COUNTER(storage_write_failures_total,
                          "mino_storage_write_failures_total");
    MINO_REGISTER_COUNTER(storage_sync_failures_total,
                          "mino_storage_sync_failures_total");
    MINO_REGISTER_COUNTER(storage_errors_total,
                          "mino_storage_errors_total");
    MINO_REGISTER_GAUGE(storage_pending_bytes, "mino_storage_pending_bytes");
    MINO_REGISTER_COUNTER(otlp_export_submitted_total,
                          "mino_otlp_export_submitted_total");
    MINO_REGISTER_COUNTER(otlp_export_dropped_total,
                          "mino_otlp_export_dropped_total");
    MINO_REGISTER_COUNTER(otlp_exported_total,
                          "mino_otlp_exported_total");
    MINO_REGISTER_COUNTER(otlp_export_failures_total,
                          "mino_otlp_export_failures_total");
    MINO_REGISTER_GAUGE(otlp_export_queue_depth,
                        "mino_otlp_export_queue_depth");
    MINO_REGISTER_GAUGE(otlp_export_queue_capacity,
                        "mino_otlp_export_queue_capacity");
    MINO_REGISTER_COUNTER(capacity_rejections_total,
                          "mino_capacity_rejections_total");
    MINO_REGISTER_GAUGE(capacity_headroom_bytes,
                        "mino_capacity_headroom_bytes");
    MINO_REGISTER_GAUGE(capacity_min_headroom_bytes,
                        "mino_capacity_min_headroom_bytes");
    MINO_REGISTER_COUNTER(tls_handshake_failures_total,
                          "mino_tls_handshake_failures_total");
    MINO_REGISTER_GAUGE(tls_certificate_expiry_unixtime_seconds,
                        "mino_tls_certificate_expiry_unixtime_seconds");
    MINO_REGISTER_COUNTER(acl_denied_total, "mino_acl_denied_total");
    MINO_REGISTER_COUNTER(prometheus_http_accepted_total,
                          "mino_prometheus_http_accepted_total");
    MINO_REGISTER_COUNTER(prometheus_http_rejected_total,
                          "mino_prometheus_http_rejected_total");
    MINO_REGISTER_COUNTER(prometheus_http_requests_total,
                          "mino_prometheus_http_requests_total");
    MINO_REGISTER_COUNTER(prometheus_http_failures_total,
                          "mino_prometheus_http_failures_total");
    MINO_REGISTER_GAUGE(prometheus_http_active_connections,
                        "mino_prometheus_http_active_connections");
    MINO_REGISTER_GAUGE(monitoring_up, "mino_monitoring_up");
    MINO_REGISTER_GAUGE(monitoring_last_snapshot_unixtime_seconds,
                        "mino_monitoring_last_snapshot_unixtime_seconds");

#undef MINO_REGISTER_GAUGE
#undef MINO_REGISTER_COUNTER

    *metrics = registered;
    return Status::Ok();
}

}  // namespace mino::observability
