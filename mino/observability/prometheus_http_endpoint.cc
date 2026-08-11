// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/observability/prometheus_http_endpoint.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "mino/observability/bounded_queue.h"
#include "mino/observability/prometheus_exporter.h"

namespace mino::observability {
namespace {

constexpr std::string_view kMetricsPath = "/metrics";
constexpr std::string_view kHealthPath = "/-/healthy";
constexpr std::string_view kHealthBody = "ok\n";

void Increment(CounterMetric* metric, size_t shard = 0) noexcept {
    if (metric != nullptr) metric->counter().Increment(shard);
}

void Set(GaugeMetric* metric, uint64_t value) noexcept {
    if (metric != nullptr) metric->gauge().Set(value, 0);
}

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

bool SetCloseOnExec(int fd) noexcept {
    const int flags = ::fcntl(fd, F_GETFD, 0);
    return flags >= 0 && ::fcntl(fd, F_SETFD, flags | FD_CLOEXEC) == 0;
}

void CloseSocket(int fd) noexcept {
    if (fd < 0) return;
    (void)::shutdown(fd, SHUT_RDWR);
    (void)::close(fd);
}

class FixedTextSink final : public TextSink {
public:
    explicit FixedTextSink(size_t limit) noexcept : limit_(limit) {}

    Status Write(std::string_view text) noexcept override {
        if (text.size() > limit_ - size_) {
            return Status::Error(StatusCode::kResourceExhausted);
        }
        std::memcpy(bytes_.data() + size_, text.data(), text.size());
        size_ += text.size();
        return Status::Ok();
    }

    std::string_view view() const noexcept { return {bytes_.data(), size_}; }

private:
    std::array<char, kPrometheusMaximumResponseBytes> bytes_{};
    size_t limit_ = 0;
    size_t size_ = 0;
};

enum class RequestResult {
    kMetrics,
    kHealth,
    kBadRequest,
    kMethodNotAllowed,
    kNotFound,
    kPayloadTooLarge,
    kRequestTimeout,
};

std::string_view Trim(std::string_view value) noexcept {
    while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) {
        value.remove_prefix(1);
    }
    while (!value.empty() && (value.back() == ' ' || value.back() == '\t')) {
        value.remove_suffix(1);
    }
    return value;
}

bool EqualsAsciiCaseInsensitive(std::string_view left,
                                std::string_view right) noexcept {
    if (left.size() != right.size()) return false;
    for (size_t i = 0; i < left.size(); ++i) {
        char a = left[i];
        char b = right[i];
        if (a >= 'A' && a <= 'Z') a = static_cast<char>(a - 'A' + 'a');
        if (b >= 'A' && b <= 'Z') b = static_cast<char>(b - 'A' + 'a');
        if (a != b) return false;
    }
    return true;
}

RequestResult ParseRequest(std::string_view request,
                           size_t header_count_limit) noexcept {
    const size_t header_end = request.find("\r\n\r\n");
    if (header_end == std::string_view::npos) {
        return RequestResult::kPayloadTooLarge;
    }
    const size_t request_line_end = request.find("\r\n");
    if (request_line_end == std::string_view::npos || request_line_end == 0) {
        return RequestResult::kBadRequest;
    }
    const std::string_view request_line = request.substr(0, request_line_end);
    const size_t first_space = request_line.find(' ');
    const size_t second_space = first_space == std::string_view::npos
                                    ? std::string_view::npos
                                    : request_line.find(' ', first_space + 1);
    if (first_space == std::string_view::npos ||
        second_space == std::string_view::npos ||
        request_line.find(' ', second_space + 1) != std::string_view::npos) {
        return RequestResult::kBadRequest;
    }
    const std::string_view method = request_line.substr(0, first_space);
    const std::string_view target = request_line.substr(
        first_space + 1, second_space - first_space - 1);
    const std::string_view version = request_line.substr(second_space + 1);
    if (version != "HTTP/1.0" && version != "HTTP/1.1") {
        return RequestResult::kBadRequest;
    }

    size_t header_count = 0;
    size_t cursor = request_line_end + 2;
    while (cursor < header_end) {
        const size_t line_end = request.find("\r\n", cursor);
        if (line_end == std::string_view::npos || line_end > header_end ||
            line_end == cursor || ++header_count > header_count_limit) {
            return RequestResult::kPayloadTooLarge;
        }
        const std::string_view line = request.substr(cursor, line_end - cursor);
        if (line.front() == ' ' || line.front() == '\t') {
            return RequestResult::kBadRequest;
        }
        const size_t colon = line.find(':');
        if (colon == std::string_view::npos || colon == 0) {
            return RequestResult::kBadRequest;
        }
        const std::string_view name = line.substr(0, colon);
        const std::string_view value = Trim(line.substr(colon + 1));
        if (EqualsAsciiCaseInsensitive(name, "transfer-encoding")) {
            return RequestResult::kBadRequest;
        }
        if (EqualsAsciiCaseInsensitive(name, "content-length")) {
            uint64_t content_length = 0;
            const auto parsed = std::from_chars(
                value.data(), value.data() + value.size(), content_length);
            if (value.empty() || parsed.ec != std::errc{} ||
                parsed.ptr != value.data() + value.size()) {
                return RequestResult::kBadRequest;
            }
            if (content_length != 0) return RequestResult::kPayloadTooLarge;
        }
        cursor = line_end + 2;
    }

    if (method != "GET") return RequestResult::kMethodNotAllowed;
    if (target == kMetricsPath) return RequestResult::kMetrics;
    if (target == kHealthPath) return RequestResult::kHealth;
    return RequestResult::kNotFound;
}

std::pair<std::string_view, std::string_view> StatusLineAndBody(
    RequestResult result) noexcept {
    switch (result) {
        case RequestResult::kBadRequest:
            return {"400 Bad Request", "bad request\n"};
        case RequestResult::kMethodNotAllowed:
            return {"405 Method Not Allowed", "method not allowed\n"};
        case RequestResult::kNotFound:
            return {"404 Not Found", "not found\n"};
        case RequestResult::kPayloadTooLarge:
            return {"431 Request Header Fields Too Large",
                    "request headers too large\n"};
        case RequestResult::kRequestTimeout:
            return {"408 Request Timeout", "request timeout\n"};
        case RequestResult::kMetrics:
        case RequestResult::kHealth:
            break;
    }
    return {"500 Internal Server Error", "internal error\n"};
}

bool PollUntil(int fd, short events,
               std::chrono::steady_clock::time_point deadline) noexcept {
    for (;;) {
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - std::chrono::steady_clock::now());
        if (remaining.count() <= 0) return false;
        pollfd descriptor{.fd = fd, .events = events, .revents = 0};
        const int ready =
            ::poll(&descriptor, 1, static_cast<int>(remaining.count()));
        if (ready > 0) {
            return (descriptor.revents & events) != 0;
        }
        if (ready < 0 && errno == EINTR) continue;
        return false;
    }
}

bool SendAll(int fd, std::string_view value,
             std::chrono::steady_clock::time_point deadline) noexcept {
    while (!value.empty()) {
        if (!PollUntil(fd, POLLOUT, deadline)) return false;
#ifdef MSG_NOSIGNAL
        constexpr int kSendFlags = MSG_DONTWAIT | MSG_NOSIGNAL;
#else
        constexpr int kSendFlags = MSG_DONTWAIT;
#endif
        const ssize_t sent = ::send(fd, value.data(), value.size(), kSendFlags);
        if (sent > 0) {
            value.remove_prefix(static_cast<size_t>(sent));
            continue;
        }
        if (sent < 0 && (errno == EINTR || errno == EAGAIN ||
                         errno == EWOULDBLOCK)) {
            continue;
        }
        return false;
    }
    return true;
}

bool SendResponse(int fd, std::string_view status, std::string_view content_type,
                  std::string_view body, uint32_t timeout_ms) noexcept {
    std::array<char, 512> header{};
    const int length = std::snprintf(
        header.data(), header.size(),
        "HTTP/1.1 %.*s\r\nContent-Type: %.*s\r\nContent-Length: %llu\r\n"
        "Connection: close\r\nCache-Control: no-store\r\n"
        "X-Content-Type-Options: nosniff\r\n\r\n",
        static_cast<int>(status.size()), status.data(),
        static_cast<int>(content_type.size()), content_type.data(),
        static_cast<unsigned long long>(body.size()));
    if (length < 0 || static_cast<size_t>(length) >= header.size()) return false;
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeout_ms);
    return SendAll(fd,
                   std::string_view(header.data(), static_cast<size_t>(length)),
                   deadline) &&
           SendAll(fd, body, deadline);
}

}  // namespace

class PrometheusHttpEndpoint::Impl final {
public:
    Impl(const MetricRegistry& registry, OperationalMetrics& metrics,
         PrometheusHttpOptions options)
        : registry_(registry), metrics_(metrics), options_(std::move(options)) {}

    ~Impl() { Stop(); }

    Status Start() {
        if (started_.load(std::memory_order_acquire)) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "Prometheus endpoint is already running");
        }
        const Status validation = ValidateOptions();
        if (!validation.ok()) return validation;

        const int fd = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (fd < 0 || !SetCloseOnExec(fd)) {
            if (fd >= 0) (void)::close(fd);
            return Status::Error(StatusCode::kUnavailable,
                                 "cannot create Prometheus listener");
        }
        const int enabled = 1;
        if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enabled,
                         sizeof(enabled)) != 0) {
            (void)::close(fd);
            return Status::Error(StatusCode::kUnavailable,
                                 "cannot configure Prometheus listener");
        }
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(options_.port);
        if (::inet_pton(AF_INET, options_.bind_address.c_str(),
                        &address.sin_addr) != 1 ||
            ::bind(fd, reinterpret_cast<const sockaddr*>(&address),
                   sizeof(address)) != 0 ||
            ::listen(fd, static_cast<int>(options_.connection_limit)) != 0) {
            (void)::close(fd);
            return Status::Error(StatusCode::kUnavailable,
                                 "cannot bind Prometheus listener");
        }
        socklen_t address_size = sizeof(address);
        if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address),
                          &address_size) != 0) {
            (void)::close(fd);
            return Status::Error(StatusCode::kUnavailable,
                                 "cannot inspect Prometheus listener");
        }

        listener_fd_.store(fd, std::memory_order_release);
        bound_port_.store(ntohs(address.sin_port), std::memory_order_release);
        stopping_.store(false, std::memory_order_release);
        try {
            workers_.reserve(options_.worker_threads);
            for (size_t worker = 0; worker < options_.worker_threads; ++worker) {
                workers_.emplace_back([this, worker] { WorkerLoop(worker); });
            }
            accept_thread_ = std::thread([this] { AcceptLoop(); });
        } catch (...) {
            stopping_.store(true, std::memory_order_release);
            const int listener = listener_fd_.exchange(-1);
            CloseSocket(listener);
            work_cv_.notify_all();
            for (std::thread& worker : workers_) {
                if (worker.joinable()) worker.join();
            }
            workers_.clear();
            return Status::Error(StatusCode::kResourceExhausted,
                                 "cannot start Prometheus endpoint threads");
        }
        started_.store(true, std::memory_order_release);
        return Status::Ok();
    }

    void Stop() noexcept {
        if (!started_.exchange(false, std::memory_order_acq_rel) &&
            listener_fd_.load(std::memory_order_acquire) < 0) {
            return;
        }
        stopping_.store(true, std::memory_order_release);
        const int listener = listener_fd_.exchange(-1, std::memory_order_acq_rel);
        CloseSocket(listener);
        work_cv_.notify_all();
        if (accept_thread_.joinable()) accept_thread_.join();
        {
            std::lock_guard lock(active_mutex_);
            for (int active : active_fds_) {
                if (active >= 0) (void)::shutdown(active, SHUT_RDWR);
            }
        }
        work_cv_.notify_all();
        for (std::thread& worker : workers_) {
            if (worker.joinable()) worker.join();
        }
        workers_.clear();
        int queued = -1;
        while (connections_.TryPop(&queued)) CloseSocket(queued);
        outstanding_.store(0, std::memory_order_release);
        Set(metrics_.prometheus_http_active_connections, 0);
        bound_port_.store(0, std::memory_order_release);
    }

    bool running() const noexcept {
        return started_.load(std::memory_order_acquire);
    }

    uint16_t bound_port() const noexcept {
        return bound_port_.load(std::memory_order_acquire);
    }

private:
    Status ValidateOptions() const {
        if (options_.bind_address.empty() || options_.bind_address.size() > 64 ||
            options_.request_bytes_limit < 64 ||
            options_.request_bytes_limit > kPrometheusMaximumRequestBytes ||
            options_.header_count_limit == 0 ||
            options_.header_count_limit > 128 ||
            options_.response_bytes_limit < 1024 ||
            options_.response_bytes_limit > kPrometheusMaximumResponseBytes ||
            options_.connection_limit == 0 ||
            options_.connection_limit > kPrometheusMaximumConnections ||
            options_.worker_threads == 0 ||
            options_.worker_threads > kPrometheusMaximumWorkerThreads ||
            options_.worker_threads > options_.connection_limit ||
            options_.read_timeout_ms == 0 || options_.read_timeout_ms > 10'000 ||
            options_.write_timeout_ms == 0 ||
            options_.write_timeout_ms > 10'000 ||
            options_.accept_poll_ms == 0 || options_.accept_poll_ms > 1000) {
            return Invalid("Prometheus endpoint options are out of bounds");
        }
        in_addr parsed{};
        if (::inet_pton(AF_INET, options_.bind_address.c_str(), &parsed) != 1) {
            return Invalid("Prometheus bind address must be an IPv4 literal");
        }
        return Status::Ok();
    }

    void AcceptLoop() noexcept {
        while (!stopping_.load(std::memory_order_acquire)) {
            const int listener = listener_fd_.load(std::memory_order_acquire);
            if (listener < 0) break;
            pollfd descriptor{.fd = listener, .events = POLLIN, .revents = 0};
            const int ready =
                ::poll(&descriptor, 1, static_cast<int>(options_.accept_poll_ms));
            if (ready < 0 && errno == EINTR) continue;
            if (ready <= 0) continue;
            const int accepted = ::accept(listener, nullptr, nullptr);
            if (accepted < 0) {
                if (errno == EINTR) continue;
                if (!stopping_.load(std::memory_order_acquire)) {
                    Increment(metrics_.prometheus_http_failures_total);
                }
                continue;
            }
            Increment(metrics_.prometheus_http_accepted_total);
            if (!ConfigureAcceptedSocket(accepted)) {
                Increment(metrics_.prometheus_http_rejected_total);
                CloseSocket(accepted);
                continue;
            }
            if (!ReserveConnection()) {
                Increment(metrics_.prometheus_http_rejected_total);
                CloseSocket(accepted);
                continue;
            }
            if (!connections_.TryPush(accepted)) {
                ReleaseConnection();
                Increment(metrics_.prometheus_http_rejected_total);
                CloseSocket(accepted);
                continue;
            }
            work_cv_.notify_one();
        }
    }

    bool ConfigureAcceptedSocket(int fd) const noexcept {
        if (!SetCloseOnExec(fd)) return false;
        const timeval read_timeout{
            .tv_sec = static_cast<time_t>(options_.read_timeout_ms / 1000),
            .tv_usec = static_cast<suseconds_t>(
                (options_.read_timeout_ms % 1000) * 1000)};
        const timeval write_timeout{
            .tv_sec = static_cast<time_t>(options_.write_timeout_ms / 1000),
            .tv_usec = static_cast<suseconds_t>(
                (options_.write_timeout_ms % 1000) * 1000)};
        const int receive_buffer = static_cast<int>(options_.request_bytes_limit);
        const int send_buffer = static_cast<int>(options_.response_bytes_limit);
        if (::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &read_timeout,
                         sizeof(read_timeout)) != 0 ||
            ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &write_timeout,
                         sizeof(write_timeout)) != 0 ||
            ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &receive_buffer,
                         sizeof(receive_buffer)) != 0 ||
            ::setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &send_buffer,
                         sizeof(send_buffer)) != 0) {
            return false;
        }
#ifdef SO_NOSIGPIPE
        const int no_sigpipe = 1;
        if (::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &no_sigpipe,
                         sizeof(no_sigpipe)) != 0) {
            return false;
        }
#endif
        return true;
    }

    bool ReserveConnection() noexcept {
        size_t current = outstanding_.load(std::memory_order_relaxed);
        while (current < options_.connection_limit) {
            if (outstanding_.compare_exchange_weak(
                    current, current + 1, std::memory_order_acq_rel,
                    std::memory_order_relaxed)) {
                Set(metrics_.prometheus_http_active_connections, current + 1);
                return true;
            }
        }
        return false;
    }

    void ReleaseConnection() noexcept {
        const size_t previous = outstanding_.fetch_sub(1, std::memory_order_acq_rel);
        Set(metrics_.prometheus_http_active_connections,
            previous == 0 ? 0 : previous - 1);
    }

    void WorkerLoop(size_t worker) noexcept {
        for (;;) {
            int fd = -1;
            if (!connections_.TryPop(&fd)) {
                if (stopping_.load(std::memory_order_acquire)) break;
                std::unique_lock lock(work_mutex_);
                work_cv_.wait_for(lock, std::chrono::milliseconds(20), [this] {
                    return stopping_.load(std::memory_order_acquire);
                });
                continue;
            }
            {
                std::lock_guard lock(active_mutex_);
                active_fds_[worker] = fd;
                if (stopping_.load(std::memory_order_acquire)) {
                    active_fds_[worker] = -1;
                    CloseSocket(fd);
                    ReleaseConnection();
                    continue;
                }
            }
            HandleConnection(fd, worker);
            {
                std::lock_guard lock(active_mutex_);
                active_fds_[worker] = -1;
            }
            CloseSocket(fd);
            ReleaseConnection();
        }
    }

    void HandleConnection(int fd, size_t worker) noexcept {
        std::array<char, kPrometheusMaximumRequestBytes + 1> request{};
        size_t used = 0;
        RequestResult result = RequestResult::kPayloadTooLarge;
        const auto read_deadline =
            std::chrono::steady_clock::now() +
            std::chrono::milliseconds(options_.read_timeout_ms);
        while (used < options_.request_bytes_limit) {
            if (!PollUntil(fd, POLLIN, read_deadline)) {
                result = RequestResult::kRequestTimeout;
                break;
            }
            const ssize_t received =
                ::recv(fd, request.data() + used,
                       options_.request_bytes_limit - used, MSG_DONTWAIT);
            if (received > 0) {
                used += static_cast<size_t>(received);
                const std::string_view current(request.data(), used);
                if (current.find("\r\n\r\n") != std::string_view::npos) {
                    result = ParseRequest(current, options_.header_count_limit);
                    break;
                }
                continue;
            }
            if (received < 0 && (errno == EINTR || errno == EAGAIN ||
                                 errno == EWOULDBLOCK)) {
                continue;
            }
            result = RequestResult::kBadRequest;
            break;
        }

        Increment(metrics_.prometheus_http_requests_total, worker + 1);
        if (result == RequestResult::kHealth) {
            if (!SendResponse(fd, "200 OK", "text/plain; charset=utf-8",
                              kHealthBody, options_.write_timeout_ms)) {
                Increment(metrics_.prometheus_http_failures_total, worker + 1);
            }
            return;
        }
        if (result == RequestResult::kMetrics) {
            TelemetrySnapshot snapshot;
            const uint64_t now_ns = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count());
            registry_.TakeSnapshot(now_ns, &snapshot);
            FixedTextSink sink(options_.response_bytes_limit);
            PrometheusTextExporter exporter(sink);
            const Status status = exporter.Export(snapshot);
            if (!status.ok()) {
                Increment(metrics_.prometheus_http_failures_total, worker + 1);
                Increment(metrics_.prometheus_http_rejected_total, worker + 1);
                (void)SendResponse(
                    fd, "503 Service Unavailable", "text/plain; charset=utf-8",
                    "metrics response exceeds configured limit\n",
                    options_.write_timeout_ms);
                return;
            }
            if (!SendResponse(fd, "200 OK",
                              "text/plain; version=0.0.4; charset=utf-8",
                              sink.view(), options_.write_timeout_ms)) {
                Increment(metrics_.prometheus_http_failures_total, worker + 1);
            }
            return;
        }

        Increment(metrics_.prometheus_http_rejected_total, worker + 1);
        const auto [status, body] = StatusLineAndBody(result);
        if (!SendResponse(fd, status, "text/plain; charset=utf-8", body,
                          options_.write_timeout_ms)) {
            Increment(metrics_.prometheus_http_failures_total, worker + 1);
        }
    }

    const MetricRegistry& registry_;
    OperationalMetrics& metrics_;
    PrometheusHttpOptions options_;
    BoundedQueue<int, kPrometheusMaximumConnections> connections_;
    std::atomic<int> listener_fd_{-1};
    std::atomic<bool> stopping_{false};
    std::atomic<bool> started_{false};
    std::atomic<uint16_t> bound_port_{0};
    std::atomic<size_t> outstanding_{0};
    std::thread accept_thread_;
    std::vector<std::thread> workers_;
    std::mutex work_mutex_;
    std::condition_variable work_cv_;
    std::mutex active_mutex_;
    std::array<int, kPrometheusMaximumWorkerThreads> active_fds_ = [] {
        std::array<int, kPrometheusMaximumWorkerThreads> values{};
        values.fill(-1);
        return values;
    }();
};

PrometheusHttpEndpoint::PrometheusHttpEndpoint(
    const MetricRegistry& registry, OperationalMetrics& metrics,
    PrometheusHttpOptions options)
    : impl_(std::make_unique<Impl>(registry, metrics, std::move(options))) {}

PrometheusHttpEndpoint::~PrometheusHttpEndpoint() = default;

Status PrometheusHttpEndpoint::Start() { return impl_->Start(); }

void PrometheusHttpEndpoint::Stop() noexcept { impl_->Stop(); }

bool PrometheusHttpEndpoint::running() const noexcept { return impl_->running(); }

uint16_t PrometheusHttpEndpoint::bound_port() const noexcept {
    return impl_->bound_port();
}

}  // namespace mino::observability
