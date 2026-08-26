// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.


#include <zmq.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <chrono>
#include <climits>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <sys/stat.h>
#include <sys/un.h>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <vector>

namespace {

constexpr uint32_t kDefaultMessages = 20000;
constexpr uint32_t kDefaultPayloadBytes = 256;
constexpr uint32_t kDefaultQueueDepth = 32;
constexpr uint32_t kMinPayloadBytes = 16;
constexpr uint64_t kMaxMessages = 1000000;
constexpr int kIoThreads = 1;
constexpr int kSendTimeoutMs = 30000;
constexpr int kRecvTimeoutMs = 10000;
constexpr int kLingerMs = 30000;
constexpr auto kOverallTimeout = std::chrono::seconds(90);

struct Config {
    uint64_t messages = kDefaultMessages;
    uint32_t payload_bytes = kDefaultPayloadBytes;
    uint32_t queue_depth = kDefaultQueueDepth;
};

uint64_t MonotonicNs() {
    timespec ts{};
    if (::clock_gettime(CLOCK_MONOTONIC, &ts) != 0) return 0;
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull +
           static_cast<uint64_t>(ts.tv_nsec);
}

void Usage() {
    std::cerr
        << "usage: zmq_ipc_pubsub_stress pub|sub ipc-path "
           "[--messages N] [--payload-bytes B] [--queue-depth D]\n"
        << "  pub  Connect PUSH to the ipc:// endpoint and send N payloads\n"
        << "  sub  Bind PULL, check sequences, print one JSON line\n"
        << "  path is a filesystem Unix socket (ipc:// is added if omitted)\n"
        << "  defaults: N=" << kDefaultMessages
        << " B=" << kDefaultPayloadBytes
        << " D=" << kDefaultQueueDepth << "\n";
}

bool ParseU64(std::string_view text, uint64_t* out) {
    if (text.empty() || out == nullptr) return false;
    const std::string copy(text);
    errno = 0;
    char* end = nullptr;
    const unsigned long long value = std::strtoull(copy.c_str(), &end, 10);
    if (errno != 0 || end == nullptr || *end != '\0') return false;
    *out = static_cast<uint64_t>(value);
    return true;
}

bool ParseArgs(int argc, char** argv, std::string_view* mode, std::string* name,
               Config* config) {
    if (argc < 3 || argv[1] == nullptr || argv[2] == nullptr) return false;
    *mode = argv[1];
    *name = argv[2];
    if (name->empty() || (*name)[0] == '-') return false;
    for (int i = 3; i < argc; ++i) {
        const std::string_view flag = argv[i] == nullptr ? "" : argv[i];
        if (i + 1 >= argc || argv[i + 1] == nullptr) return false;
        uint64_t value = 0;
        if (!ParseU64(argv[i + 1], &value)) return false;
        ++i;
        if (flag == "--messages") {
            if (value == 0 || value > kMaxMessages) return false;
            config->messages = value;
        } else if (flag == "--payload-bytes") {
            if (value < kMinPayloadBytes || value > 1024ull * 1024ull) {
                return false;
            }
            config->payload_bytes = static_cast<uint32_t>(value);
        } else if (flag == "--queue-depth") {
            if (value < 2 || value > 1024 || (value & (value - 1)) != 0) {
                return false;
            }
            config->queue_depth = static_cast<uint32_t>(value);
        } else {
            return false;
        }
    }
    return true;
}

std::filesystem::path SocketPath(const std::string& name) {
    if (name.rfind("ipc://", 0) == 0) {
        return std::filesystem::path(name.substr(6));
    }
    std::filesystem::path path(name);
    if (path.is_absolute()) return path;
    const char* tmp = std::getenv("TEST_TMPDIR");
    if (tmp == nullptr || *tmp == '\0') tmp = "/tmp";
    return std::filesystem::path(tmp) / path;
}

std::string IpcEndpoint(const std::filesystem::path& path) {
    return "ipc://" + path.string();
}

bool PathFitsUnix(const std::filesystem::path& path) {
    struct sockaddr_un address {};
    return path.string().size() < sizeof(address.sun_path);
}

void RemoveStaleSocket(const std::filesystem::path& path) {
    struct stat status {};
    if (lstat(path.c_str(), &status) != 0) {
        if (errno == ENOENT) return;
        throw std::system_error(errno, std::generic_category(),
                                "lstat IPC socket path");
    }
    if (!S_ISSOCK(status.st_mode)) {
        throw std::runtime_error("refusing to remove non-socket IPC path: " +
                                 path.string());
    }
    if (unlink(path.c_str()) != 0) {
        throw std::system_error(errno, std::generic_category(),
                                "remove stale IPC socket");
    }
}

void FillPayload(std::span<std::byte> buf, uint64_t seq, uint64_t origin_ns) {
    std::memcpy(buf.data(), &origin_ns, sizeof(origin_ns));
    std::memcpy(buf.data() + sizeof(origin_ns), &seq, sizeof(seq));
    for (size_t i = 16; i < buf.size(); ++i) {
        buf[i] = static_cast<std::byte>(
            static_cast<uint8_t>(seq ^ static_cast<uint64_t>(i)));
    }
}

bool PayloadMatches(std::span<const std::byte> buf, uint64_t seq) {
    for (size_t i = 16; i < buf.size(); ++i) {
        const std::byte expected = static_cast<std::byte>(
            static_cast<uint8_t>(seq ^ static_cast<uint64_t>(i)));
        if (buf[i] != expected) return false;
    }
    return true;
}

std::string ZmqError(std::string_view operation) {
    return std::string(operation) + ": " + zmq_strerror(zmq_errno());
}

bool SetInt(void* socket, int option, int value, std::string_view name) {
    if (zmq_setsockopt(socket, option, &value, sizeof(value)) != 0) {
        std::cerr << ZmqError("cannot set " + std::string(name)) << "\n";
        return false;
    }
    return true;
}

struct ZmqSession {
    void* ctx = nullptr;
    void* sock = nullptr;
    ~ZmqSession() {
        if (sock != nullptr) {
            static_cast<void>(zmq_close(sock));
            sock = nullptr;
        }
        if (ctx != nullptr) {
            while (zmq_ctx_term(ctx) != 0 && zmq_errno() == EINTR) {
            }
            ctx = nullptr;
        }
    }
};

bool OpenSocket(ZmqSession* session, int type, int hwm, int snd_ms, int rcv_ms) {
    session->ctx = zmq_ctx_new();
    if (session->ctx == nullptr) {
        std::cerr << ZmqError("zmq_ctx_new") << "\n";
        return false;
    }
    if (zmq_ctx_set(session->ctx, ZMQ_IO_THREADS, kIoThreads) != 0) {
        std::cerr << ZmqError("ZMQ_IO_THREADS") << "\n";
        return false;
    }
    session->sock = zmq_socket(session->ctx, type);
    if (session->sock == nullptr) {
        std::cerr << ZmqError("zmq_socket") << "\n";
        return false;
    }
    return SetInt(session->sock, ZMQ_LINGER, kLingerMs, "LINGER") &&
           SetInt(session->sock, ZMQ_IMMEDIATE, 1, "IMMEDIATE") &&
           SetInt(session->sock, ZMQ_SNDHWM, hwm, "SNDHWM") &&
           SetInt(session->sock, ZMQ_RCVHWM, hwm, "RCVHWM") &&
           SetInt(session->sock, ZMQ_SNDTIMEO, snd_ms, "SNDTIMEO") &&
           SetInt(session->sock, ZMQ_RCVTIMEO, rcv_ms, "RCVTIMEO");
}

int RunPub(const std::filesystem::path& path, const Config& config) {
    if (!PathFitsUnix(path)) {
        std::cerr << "Unix IPC path is too long: " << path << "\n";
        return 1;
    }
    ZmqSession session;
    if (!OpenSocket(&session, ZMQ_PUSH, static_cast<int>(config.queue_depth),
                    kSendTimeoutMs, kRecvTimeoutMs)) {
        return 1;
    }
    const std::string endpoint = IpcEndpoint(path);
    if (zmq_connect(session.sock, endpoint.c_str()) != 0) {
        std::cerr << ZmqError("zmq_connect " + endpoint) << "\n";
        return 1;
    }
    std::vector<std::byte> payload(config.payload_bytes);
    for (uint64_t seq = 0; seq < config.messages; ++seq) {
        FillPayload(payload, seq, MonotonicNs());
        if (payload.size() > static_cast<size_t>(INT_MAX)) {
            std::cerr << "payload exceeds zmq_send limit\n";
            return 1;
        }
        for (;;) {
            const int sent = zmq_send(session.sock, payload.data(),
                                      payload.size(), 0);
            if (sent == static_cast<int>(payload.size())) break;
            if (sent >= 0) {
                std::cerr << "short zmq_send at seq " << seq << "\n";
                return 1;
            }
            const int err = zmq_errno();
            if (err == EINTR) continue;
            std::cerr << "Publish failed at seq " << seq << ": "
                      << zmq_strerror(err) << "\n";
            return 1;
        }
    }
    std::cerr << "published " << config.messages << " messages via " << endpoint
              << "\n";
    return 0;
}

uint64_t PercentileNs(std::vector<uint64_t>* latencies, int percent) {
    if (latencies == nullptr || latencies->empty()) return 0;
    std::sort(latencies->begin(), latencies->end());
    const size_t n = latencies->size();
    size_t index = (static_cast<size_t>(percent) * (n - 1)) / 100;
    if (index >= n) index = n - 1;
    return (*latencies)[index];
}

int RunSub(const std::filesystem::path& path, const Config& config) {
    if (!PathFitsUnix(path)) {
        std::cerr << "Unix IPC path is too long: " << path << "\n";
        return 1;
    }
    try {
        RemoveStaleSocket(path);
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << "\n";
        return 1;
    }
    ZmqSession session;
    if (!OpenSocket(&session, ZMQ_PULL, static_cast<int>(config.queue_depth),
                    kSendTimeoutMs, kRecvTimeoutMs)) {
        return 1;
    }
    const std::string endpoint = IpcEndpoint(path);
    if (zmq_bind(session.sock, endpoint.c_str()) != 0) {
        std::cerr << ZmqError("zmq_bind " + endpoint) << "\n";
        return 1;
    }
    std::cerr << "bound " << endpoint << "\n";

    uint64_t received = 0;
    uint64_t lost = 0;
    uint64_t expected = 0;
    bool pattern_ok = true;
    std::vector<uint64_t> latencies;
    latencies.reserve(static_cast<size_t>(
        std::min(config.messages, static_cast<uint64_t>(1 << 20))));
    std::vector<std::byte> buf(config.payload_bytes);

    const uint64_t loop_start_ns = MonotonicNs();
    const auto overall = std::chrono::steady_clock::now() + kOverallTimeout;
    while (received + lost < config.messages) {
        if (std::chrono::steady_clock::now() >= overall) {
            lost += config.messages - received - lost;
            break;
        }
        const int got = zmq_recv(session.sock, buf.data(), buf.size(), 0);
        if (got < 0) {
            const int err = zmq_errno();
            if (err == EINTR) continue;
            if (err == EAGAIN) {
                lost += config.messages - received - lost;
                break;
            }
            std::cerr << ZmqError("zmq_recv") << "\n";
            return 1;
        }
        if (static_cast<size_t>(got) != config.payload_bytes ||
            static_cast<size_t>(got) < 16) {
            std::cerr << "unexpected payload size " << got << "\n";
            return 1;
        }
        uint64_t origin_ns = 0;
        uint64_t seq = 0;
        std::memcpy(&origin_ns, buf.data(), sizeof(origin_ns));
        std::memcpy(&seq, buf.data() + sizeof(origin_ns), sizeof(seq));
        if (seq < expected) {
            std::cerr << "out-of-order seq " << seq << " expected " << expected
                      << "\n";
            return 1;
        }
        if (seq > expected) lost += seq - expected;
        if (!PayloadMatches(
                std::span<const std::byte>{buf.data(), buf.size()}, seq)) {
            pattern_ok = false;
        }
        const uint64_t now_ns = MonotonicNs();
        if (origin_ns != 0 && now_ns >= origin_ns) {
            latencies.push_back(now_ns - origin_ns);
        }
        expected = seq + 1;
        ++received;
    }
    const uint64_t loop_end_ns = MonotonicNs();
    if (received + lost < config.messages) {
        lost += config.messages - received - lost;
    }
    const double elapsed_s =
        loop_end_ns > loop_start_ns
            ? static_cast<double>(loop_end_ns - loop_start_ns) / 1e9
            : 0.0;
    const double msgs_per_s =
        elapsed_s > 0.0 ? static_cast<double>(received) / elapsed_s : 0.0;
    const uint64_t p50 = PercentileNs(&latencies, 50);
    const uint64_t p95 = PercentileNs(&latencies, 95);

    std::ostringstream json;
    json << std::fixed << std::setprecision(1);
    json << "{\"codec\":\"raw\""
         << ",\"received\":" << received
         << ",\"lost\":" << lost
         << ",\"expected\":" << config.messages
         << ",\"payload_bytes\":" << config.payload_bytes
         << ",\"queue_depth\":" << config.queue_depth
         << ",\"p50_ns\":" << p50
         << ",\"p95_ns\":" << p95
         << ",\"msgs_per_s\":" << msgs_per_s
         << "}\n";
    std::cout << json.str() << std::flush;
    try {
        RemoveStaleSocket(path);
    } catch (...) {
    }
    if (!pattern_ok) {
        std::cerr << "payload pattern mismatch\n";
        return 1;
    }
    if (received != config.messages || lost != 0) return 1;
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    std::string_view mode;
    std::string name;
    Config config;
    if (!ParseArgs(argc, argv, &mode, &name, &config)) {
        Usage();
        return 2;
    }
    const std::filesystem::path path = SocketPath(name);
    if (mode == "pub") return RunPub(path, config);
    if (mode == "sub") return RunSub(path, config);
    Usage();
    return 2;
}
