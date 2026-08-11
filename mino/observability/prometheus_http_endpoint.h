// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_OBSERVABILITY_PROMETHEUS_HTTP_ENDPOINT_H_
#define MINO_OBSERVABILITY_PROMETHEUS_HTTP_ENDPOINT_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "mino/common/status.h"
#include "mino/observability/metrics.h"
#include "mino/observability/operational_metrics.h"

namespace mino::observability {

inline constexpr size_t kPrometheusMaximumRequestBytes = 16u * 1024u;
inline constexpr size_t kPrometheusMaximumResponseBytes = 512u * 1024u;
inline constexpr size_t kPrometheusMaximumConnections = 64;
inline constexpr size_t kPrometheusMaximumWorkerThreads = 16;

struct PrometheusHttpOptions {
    // Loopback is the safe default. Explicitly binding a non-loopback address
    // requires deployment-level firewall/authentication controls.
    std::string bind_address = "127.0.0.1";
    uint16_t port = 9464;
    size_t request_bytes_limit = 4096;
    size_t header_count_limit = 32;
    size_t response_bytes_limit = 256u * 1024u;
    size_t connection_limit = 16;
    size_t worker_threads = 2;
    uint32_t read_timeout_ms = 1000;
    uint32_t write_timeout_ms = 2000;
    uint32_t accept_poll_ms = 100;
};

// A bounded HTTP/1.0/1.1 endpoint supporting only GET /metrics and
// GET /-/healthy. Every connection serves one request and is then closed.
// Socket I/O and metric formatting run only on endpoint-owned threads.
class PrometheusHttpEndpoint final {
public:
    PrometheusHttpEndpoint(const MetricRegistry& registry,
                           OperationalMetrics& metrics,
                           PrometheusHttpOptions options = {});
    ~PrometheusHttpEndpoint();

    PrometheusHttpEndpoint(const PrometheusHttpEndpoint&) = delete;
    PrometheusHttpEndpoint& operator=(const PrometheusHttpEndpoint&) = delete;
    PrometheusHttpEndpoint(PrometheusHttpEndpoint&&) = delete;
    PrometheusHttpEndpoint& operator=(PrometheusHttpEndpoint&&) = delete;

    Status Start();
    void Stop() noexcept;

    bool running() const noexcept;
    uint16_t bound_port() const noexcept;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mino::observability

#endif  // MINO_OBSERVABILITY_PROMETHEUS_HTTP_ENDPOINT_H_
