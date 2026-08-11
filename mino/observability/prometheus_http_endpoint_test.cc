// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/observability/prometheus_http_endpoint.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <string>
#include <string_view>
#include <thread>

namespace mino::observability {
namespace {

class ScopedFd final {
public:
    explicit ScopedFd(int fd = -1) noexcept : fd_(fd) {}
    ~ScopedFd() {
        if (fd_ >= 0) (void)::close(fd_);
    }
    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;
    ScopedFd(ScopedFd&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
    ScopedFd& operator=(ScopedFd&&) = delete;
    int get() const noexcept { return fd_; }

private:
    int fd_;
};

ScopedFd Connect(uint16_t port) {
    ScopedFd fd(::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP));
    EXPECT_GE(fd.get(), 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    EXPECT_EQ(::connect(fd.get(), reinterpret_cast<sockaddr*>(&address),
                        sizeof(address)),
              0);
    return fd;
}

std::string ReceiveAll(int fd) {
    std::string response;
    char buffer[4096];
    for (;;) {
        const ssize_t received = ::recv(fd, buffer, sizeof(buffer), 0);
        if (received <= 0) break;
        response.append(buffer, static_cast<size_t>(received));
    }
    return response;
}

std::string Exchange(uint16_t port, std::string_view request) {
    ScopedFd fd = Connect(port);
    EXPECT_EQ(::send(fd.get(), request.data(), request.size(), 0),
              static_cast<ssize_t>(request.size()));
    return ReceiveAll(fd.get());
}

struct RegistryFixture {
    RegistryFixture() : registry(1000) {
        EXPECT_TRUE(RegisterOperationalMetrics(registry, &metrics).ok());
        EXPECT_TRUE(registry.RegisterCounter("mino_test_events_total", &events).ok());
        events->counter().Add(7, 0);
    }
    MetricRegistry registry;
    OperationalMetrics metrics;
    CounterMetric* events = nullptr;
};

PrometheusHttpOptions TestOptions() {
    PrometheusHttpOptions options;
    options.port = 0;
    options.worker_threads = 1;
    options.connection_limit = 2;
    options.read_timeout_ms = 200;
    options.write_timeout_ms = 500;
    options.accept_poll_ms = 20;
    return options;
}

TEST(PrometheusHttpEndpointTest, ServesOnlyMetricsAndHealthGetPaths) {
    RegistryFixture fixture;
    PrometheusHttpEndpoint endpoint(fixture.registry, fixture.metrics,
                                    TestOptions());
    ASSERT_TRUE(endpoint.Start().ok());
    ASSERT_NE(endpoint.bound_port(), 0);

    const std::string metrics =
        Exchange(endpoint.bound_port(), "GET /metrics HTTP/1.1\r\nHost: local\r\n\r\n");
    EXPECT_NE(metrics.find("HTTP/1.1 200 OK"), std::string::npos);
    EXPECT_NE(metrics.find("mino_test_events_total 7\n"), std::string::npos);
    EXPECT_NE(metrics.find("mino_prometheus_http_requests_total"),
              std::string::npos);

    const std::string health = Exchange(
        endpoint.bound_port(), "GET /-/healthy HTTP/1.0\r\n\r\n");
    EXPECT_NE(health.find("HTTP/1.1 200 OK"), std::string::npos);
    EXPECT_TRUE(health.ends_with("ok\n"));

    const std::string method =
        Exchange(endpoint.bound_port(), "POST /metrics HTTP/1.1\r\n\r\n");
    EXPECT_NE(method.find("405 Method Not Allowed"), std::string::npos);
    const std::string missing =
        Exchange(endpoint.bound_port(), "GET /debug HTTP/1.1\r\n\r\n");
    EXPECT_NE(missing.find("404 Not Found"), std::string::npos);
    endpoint.Stop();
    EXPECT_FALSE(endpoint.running());
}

TEST(PrometheusHttpEndpointTest, EnforcesHeaderBoundAndStopsIdleConnection) {
    RegistryFixture fixture;
    PrometheusHttpOptions options = TestOptions();
    options.request_bytes_limit = 128;
    options.read_timeout_ms = 5000;
    PrometheusHttpEndpoint endpoint(fixture.registry, fixture.metrics, options);
    ASSERT_TRUE(endpoint.Start().ok());

    const std::string oversized(128, 'x');
    const std::string response = Exchange(endpoint.bound_port(), oversized);
    EXPECT_NE(response.find("431 Request Header Fields Too Large"),
              std::string::npos);

    ScopedFd idle = Connect(endpoint.bound_port());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    const auto before = std::chrono::steady_clock::now();
    endpoint.Stop();
    const auto elapsed = std::chrono::steady_clock::now() - before;
    EXPECT_LT(elapsed, std::chrono::seconds(2));
}

TEST(PrometheusHttpEndpointTest, ReadDeadlineIsAbsoluteAcrossPartialInput) {
    RegistryFixture fixture;
    PrometheusHttpOptions options = TestOptions();
    options.read_timeout_ms = 150;
    PrometheusHttpEndpoint endpoint(fixture.registry, fixture.metrics, options);
    ASSERT_TRUE(endpoint.Start().ok());

    ScopedFd slow = Connect(endpoint.bound_port());
    ASSERT_EQ(::send(slow.get(), "G", 1, 0), 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_EQ(::send(slow.get(), "E", 1, 0), 1);
    const auto before = std::chrono::steady_clock::now();
    const std::string response = ReceiveAll(slow.get());
    const auto elapsed = std::chrono::steady_clock::now() - before;
    EXPECT_NE(response.find("408 Request Timeout"), std::string::npos);
    EXPECT_LT(elapsed, std::chrono::milliseconds(300));
    endpoint.Stop();
}

TEST(PrometheusHttpEndpointTest, RejectsOutOfBoundConfiguration) {
    RegistryFixture fixture;
    PrometheusHttpOptions options = TestOptions();
    options.connection_limit = kPrometheusMaximumConnections + 1;
    PrometheusHttpEndpoint endpoint(fixture.registry, fixture.metrics, options);
    const Status status = endpoint.Start();
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), StatusCode::kInvalidArgument);
}

}  // namespace
}  // namespace mino::observability
