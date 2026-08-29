// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.


#include "benchmarks/pipeline_comparison/pipeline_common.h"
#include "benchmarks/pipeline_comparison/semantic_protobuf_codec.h"

#include <zmq.h>

#include <algorithm>
#include <optional>
#include <cerrno>
#include <cstdlib>
#include <chrono>
#include <climits>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
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
constexpr uint32_t kHeaderAllowanceBytes = 2048;
constexpr uint64_t kMaxMessages = 1000000;
constexpr int kIoThreads = 1;
constexpr int kSendTimeoutMs = 30000;
constexpr int kRecvTimeoutMs = 10000;
constexpr int kLingerMs = 30000;
constexpr int kHandshakeTimeoutMs = 20000;
constexpr auto kOverallTimeout = std::chrono::seconds(90);
constexpr auto kHandshakeTimeout = std::chrono::seconds(20);
constexpr char kTopic[] = "camera";
constexpr int kTopicLen = static_cast<int>(sizeof(kTopic) - 1);

using mino::benchmarks::pipeline::InitializeSourceFrame;
using mino::benchmarks::pipeline::NowNs;
using mino::benchmarks::pipeline::ParseFrame;
using mino::benchmarks::pipeline::Profile;
using mino::benchmarks::pipeline::SemanticFrame;
using mino::benchmarks::pipeline::SerializeFrame;
using mino::benchmarks::pipeline::ValidateSemanticFrame;
using mino::benchmarks::pipeline::kLargePayloadBytes;
using mino::benchmarks::pipeline::kMediumPayloadBytes;
using mino::benchmarks::pipeline::kSmallPayloadBytes;

struct Config {
    uint64_t messages = kDefaultMessages;
    uint32_t payload_bytes = kDefaultPayloadBytes;
    uint32_t queue_depth = kDefaultQueueDepth;
    uint32_t hwm = kDefaultQueueDepth;
};

std::optional<Profile> ProfileFromPayload(uint32_t payload_bytes) {
    if (payload_bytes == kSmallPayloadBytes) return Profile::kSmall;
    if (payload_bytes == kMediumPayloadBytes) return Profile::kMedium;
    if (payload_bytes == kLargePayloadBytes) return Profile::kLarge;
    return std::nullopt;
}

void Usage() {
    std::cerr
        << "usage: zmq_ipc_business_stress pub|sub ipc-path "
           "[--messages N] [--payload-bytes B] [--queue-depth D] [--hwm N]\n"
        << "  pub  Connect PUB after the SUB ready handshake and send N protobuf frames\n"
        << "  sub  Bind SUB, subscribe topic camera, check sequences, print JSON\n"
        << "  path is a filesystem Unix socket (ipc:// is added if omitted)\n"
        << "  defaults: N=" << kDefaultMessages
        << " B=" << kDefaultPayloadBytes
        << " D=" << kDefaultQueueDepth
        << " HWM=D (1..1000000, not required to be power-of-two)\n";
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
    bool hwm_set = false;
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
            if (!ProfileFromPayload(static_cast<uint32_t>(value)).has_value()) {
                return false;
            }
            config->payload_bytes = static_cast<uint32_t>(value);
        } else if (flag == "--queue-depth") {
            if (value < 2 || value > 1024 || (value & (value - 1)) != 0) {
                return false;
            }
            config->queue_depth = static_cast<uint32_t>(value);
        } else if (flag == "--hwm") {
            if (value < 1 || value > kMaxMessages) return false;
            config->hwm = static_cast<uint32_t>(value);
            hwm_set = true;
        } else {
            return false;
        }
    }
    if (!hwm_set) config->hwm = config->queue_depth;
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

std::filesystem::path ReadyPath(const std::filesystem::path& path) {
    return std::filesystem::path(path.string() + ".ready");
}

std::filesystem::path PeerPath(const std::filesystem::path& path) {
    return std::filesystem::path(path.string() + ".peer");
}

std::filesystem::path DonePath(const std::filesystem::path& path) {
    return std::filesystem::path(path.string() + ".done");
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

void UnlinkQuiet(const std::filesystem::path& path) {
    if (unlink(path.c_str()) != 0 && errno != ENOENT) {
        std::cerr << "unlink " << path << ": " << std::strerror(errno) << "\n";
    }
}

bool WriteSignalFile(const std::filesystem::path& path) {
    const std::filesystem::path tmp(path.string() + ".tmp");
    const int fd =
        ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) {
        std::cerr << "open " << tmp << ": " << std::strerror(errno) << "\n";
        return false;
    }
    const char payload[] = "1\n";
    const ssize_t n = ::write(fd, payload, sizeof(payload) - 1);
    const int saved = errno;
    ::close(fd);
    if (n != static_cast<ssize_t>(sizeof(payload) - 1)) {
        UnlinkQuiet(tmp);
        std::cerr << "write " << tmp << ": " << std::strerror(saved) << "\n";
        return false;
    }
    if (::rename(tmp.c_str(), path.c_str()) != 0) {
        const int rename_err = errno;
        UnlinkQuiet(tmp);
        std::cerr << "rename " << tmp << ": " << std::strerror(rename_err)
                  << "\n";
        return false;
    }
    return true;
}

bool WaitForFile(const std::filesystem::path& path,
                 std::chrono::seconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        struct stat status {};
        if (::lstat(path.c_str(), &status) == 0 && S_ISREG(status.st_mode) &&
            status.st_size > 0) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    std::cerr << "timeout waiting for " << path << "\n";
    return false;
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
    void* monitor = nullptr;
    ~ZmqSession() {
        if (monitor != nullptr) {
            static_cast<void>(zmq_close(monitor));
            monitor = nullptr;
        }
        if (sock != nullptr) {
            static_cast<void>(zmq_socket_monitor(sock, nullptr, 0));
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

bool StartMonitor(ZmqSession* session) {
    const char* endpoint = "inproc://zmq-mon";
    const int events = ZMQ_EVENT_CONNECTED | ZMQ_EVENT_ACCEPTED |
                       ZMQ_EVENT_HANDSHAKE_SUCCEEDED |
                       ZMQ_EVENT_HANDSHAKE_FAILED_NO_DETAIL |
                       ZMQ_EVENT_HANDSHAKE_FAILED_PROTOCOL |
                       ZMQ_EVENT_HANDSHAKE_FAILED_AUTH |
                       ZMQ_EVENT_DISCONNECTED;
    if (zmq_socket_monitor(session->sock, endpoint, events) != 0) {
        std::cerr << ZmqError("zmq_socket_monitor") << "\n";
        return false;
    }
    session->monitor = zmq_socket(session->ctx, ZMQ_PAIR);
    if (session->monitor == nullptr) {
        std::cerr << ZmqError("monitor zmq_socket") << "\n";
        return false;
    }
    if (!SetInt(session->monitor, ZMQ_LINGER, 0, "monitor LINGER") ||
        !SetInt(session->monitor, ZMQ_RCVTIMEO, kHandshakeTimeoutMs,
                "monitor RCVTIMEO")) {
        return false;
    }
    if (zmq_connect(session->monitor, endpoint) != 0) {
        std::cerr << ZmqError("monitor zmq_connect") << "\n";
        return false;
    }
    return true;
}

bool WaitHandshake(void* monitor) {
    const auto deadline = std::chrono::steady_clock::now() + kHandshakeTimeout;
    while (std::chrono::steady_clock::now() < deadline) {
        zmq_msg_t event_msg;
        zmq_msg_init(&event_msg);
        const int rc = zmq_msg_recv(&event_msg, monitor, 0);
        if (rc < 0) {
            zmq_msg_close(&event_msg);
            const int err = zmq_errno();
            if (err == EINTR) continue;
            std::cerr << ZmqError("monitor recv") << "\n";
            return false;
        }
        uint16_t event = 0;
        if (zmq_msg_size(&event_msg) >= sizeof(event)) {
            std::memcpy(&event, zmq_msg_data(&event_msg), sizeof(event));
        }
        zmq_msg_close(&event_msg);
        zmq_msg_t addr_msg;
        zmq_msg_init(&addr_msg);
        static_cast<void>(zmq_msg_recv(&addr_msg, monitor, 0));
        zmq_msg_close(&addr_msg);
        if (event == ZMQ_EVENT_HANDSHAKE_SUCCEEDED) return true;
        if (event == ZMQ_EVENT_HANDSHAKE_FAILED_NO_DETAIL ||
            event == ZMQ_EVENT_HANDSHAKE_FAILED_PROTOCOL ||
            event == ZMQ_EVENT_HANDSHAKE_FAILED_AUTH ||
            event == ZMQ_EVENT_DISCONNECTED) {
            std::cerr << "PUB/SUB handshake failed, monitor event=" << event
                      << "\n";
            return false;
        }
    }
    std::cerr << "timeout waiting for ZMQ handshake\n";
    return false;
}

bool SendMultipart(void* sock, const void* payload, size_t payload_size,
                   uint64_t seq) {
    for (;;) {
        const int sent = zmq_send(sock, kTopic, static_cast<size_t>(kTopicLen),
                                  ZMQ_SNDMORE);
        if (sent == kTopicLen) break;
        if (sent >= 0) {
            std::cerr << "short topic send at seq " << seq << "\n";
            return false;
        }
        const int err = zmq_errno();
        if (err == EINTR) continue;
        std::cerr << "Publish topic failed at seq " << seq << ": "
                  << zmq_strerror(err) << "\n";
        return false;
    }
    for (;;) {
        const int sent = zmq_send(sock, payload, payload_size, 0);
        if (sent == static_cast<int>(payload_size)) return true;
        if (sent >= 0) {
            std::cerr << "short zmq_send at seq " << seq << "\n";
            return false;
        }
        const int err = zmq_errno();
        if (err == EINTR) continue;
        std::cerr << "Publish failed at seq " << seq << ": "
                  << zmq_strerror(err) << "\n";
        return false;
    }
}

int RecvPayload(void* sock, void* buf, size_t buf_size) {
    char topic[16];
    const int tgot = zmq_recv(sock, topic, sizeof(topic), 0);
    if (tgot < 0) return -1;
    if (tgot != kTopicLen || std::memcmp(topic, kTopic, kTopicLen) != 0) {
        std::cerr << "unexpected topic frame size " << tgot << "\n";
        return -2;
    }
    int more = 0;
    size_t more_size = sizeof(more);
    if (zmq_getsockopt(sock, ZMQ_RCVMORE, &more, &more_size) != 0 || more == 0) {
        std::cerr << "missing payload frame after topic\n";
        return -2;
    }
    return zmq_recv(sock, buf, buf_size, 0);
}

int RunPub(const std::filesystem::path& path, const Config& config) {
    const auto profile = ProfileFromPayload(config.payload_bytes);
    if (!profile.has_value()) {
        std::cerr << "unsupported payload-bytes for business profile\n";
        return 1;
    }
    if (!PathFitsUnix(path)) {
        std::cerr << "Unix IPC path is too long: " << path << "\n";
        return 1;
    }
    if (!WaitForFile(ReadyPath(path), kHandshakeTimeout)) return 1;
    const std::string endpoint = IpcEndpoint(path);
    {
        ZmqSession session;
        if (!OpenSocket(&session, ZMQ_PUB, static_cast<int>(config.hwm),
                        kSendTimeoutMs, kRecvTimeoutMs)) {
            return 1;
        }
        if (!StartMonitor(&session)) return 1;
        if (zmq_connect(session.sock, endpoint.c_str()) != 0) {
            std::cerr << ZmqError("zmq_connect " + endpoint) << "\n";
            return 1;
        }
        if (!WaitHandshake(session.monitor)) return 1;
        if (!WaitForFile(PeerPath(path), kHandshakeTimeout)) return 1;
        for (uint64_t seq = 0; seq < config.messages; ++seq) {
            SemanticFrame frame = InitializeSourceFrame(seq, *profile, true);
            std::string bytes;
            try {
                bytes = SerializeFrame(frame);
            } catch (const std::exception& ex) {
                std::cerr << "SerializeFrame failed at seq " << seq << ": "
                          << ex.what() << "\n";
                return 1;
            }
            if (bytes.size() > static_cast<size_t>(INT_MAX)) {
                std::cerr << "encoded frame exceeds zmq_send limit\n";
                return 1;
            }
            if (!SendMultipart(session.sock, bytes.data(), bytes.size(), seq)) {
                return 1;
            }
        }
        std::cerr << "published " << config.messages << " business frames via "
                  << endpoint << " socket=pubsub handshake=ready+monitor\n";
    }
    if (!WriteSignalFile(DonePath(path))) return 1;
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
    const auto profile = ProfileFromPayload(config.payload_bytes);
    if (!profile.has_value()) {
        std::cerr << "unsupported payload-bytes for business profile\n";
        return 1;
    }
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
    UnlinkQuiet(ReadyPath(path));
    UnlinkQuiet(PeerPath(path));
    UnlinkQuiet(DonePath(path));
    ZmqSession session;
    if (!OpenSocket(&session, ZMQ_SUB, static_cast<int>(config.hwm),
                    kSendTimeoutMs, kRecvTimeoutMs)) {
        return 1;
    }
    if (zmq_setsockopt(session.sock, ZMQ_SUBSCRIBE, kTopic,
                       static_cast<size_t>(kTopicLen)) != 0) {
        std::cerr << ZmqError("ZMQ_SUBSCRIBE camera") << "\n";
        return 1;
    }
    if (!StartMonitor(&session)) return 1;
    const std::string endpoint = IpcEndpoint(path);
    if (zmq_bind(session.sock, endpoint.c_str()) != 0) {
        std::cerr << ZmqError("zmq_bind " + endpoint) << "\n";
        return 1;
    }
    if (!WriteSignalFile(ReadyPath(path))) return 1;
    std::cerr << "bound " << endpoint << " subscribed=" << kTopic << "\n";
    if (!WaitHandshake(session.monitor)) return 1;
    if (!WriteSignalFile(PeerPath(path))) return 1;
    if (!SetInt(session.sock, ZMQ_RCVTIMEO, 100, "RCVTIMEO drain")) return 1;

    uint64_t received = 0;
    uint64_t lost = 0;
    uint64_t expected = 0;
    uint64_t first_seq = 0;
    uint64_t last_rx_ns = 0;
    bool frames_ok = true;
    std::string frame_error;
    std::vector<uint64_t> latencies;
    latencies.reserve(static_cast<size_t>(
        std::min(config.messages, static_cast<uint64_t>(1 << 20))));
    const size_t max_encoded = static_cast<size_t>(config.payload_bytes) +
                               kHeaderAllowanceBytes;
    std::vector<uint8_t> buf(max_encoded);

    const uint64_t loop_start_ns = NowNs();
    const auto overall = std::chrono::steady_clock::now() + kOverallTimeout;
    while (received + lost < config.messages) {
        if (std::chrono::steady_clock::now() >= overall) {
            lost += config.messages - received - lost;
            break;
        }
        const int got = RecvPayload(session.sock, buf.data(), buf.size());
        if (got < 0) {
            if (got == -2) return 1;
            const int err = zmq_errno();
            if (err == EINTR) continue;
            if (err == EAGAIN) {
                struct stat done_status {};
                if (::lstat(DonePath(path).c_str(), &done_status) == 0) {
                    lost += config.messages - received - lost;
                    break;
                }
                continue;
            }
            std::cerr << ZmqError("zmq_recv") << "\n";
            return 1;
        }
        if (static_cast<size_t>(got) > buf.size()) {
            std::cerr << "encoded frame truncated at " << got << " bytes\n";
            return 1;
        }
        SemanticFrame frame;
        if (!ParseFrame(std::span<const uint8_t>(buf.data(),
                                                static_cast<size_t>(got)),
                        &frame, &frame_error)) {
            std::cerr << "ParseFrame failed: " << frame_error << "\n";
            return 1;
        }
        if (frame.sample_id < expected) {
            std::cerr << "out-of-order sample_id " << frame.sample_id
                      << " expected " << expected << "\n";
            return 1;
        }
        if (received == 0) first_seq = frame.sample_id;
        if (frame.sample_id > expected) lost += frame.sample_id - expected;
        last_rx_ns = NowNs();
        if (frame.payload.size() != config.payload_bytes) {
            frames_ok = false;
            frame_error = "payload size mismatch";
        } else if (!ValidateSemanticFrame(frame, &frame_error)) {
            frames_ok = false;
        }
        const uint64_t now_ns = last_rx_ns != 0 ? last_rx_ns : NowNs();
        if (frame.origin_timestamp_ns != 0 &&
            now_ns >= frame.origin_timestamp_ns) {
            latencies.push_back(now_ns - frame.origin_timestamp_ns);
        }
        expected = frame.sample_id + 1;
        ++received;
    }
    const uint64_t loop_end_ns = NowNs();
    if (received + lost < config.messages) {
        lost += config.messages - received - lost;
    }
    const uint64_t throughput_end_ns =
        last_rx_ns != 0 ? last_rx_ns : loop_end_ns;
    const double elapsed_s =
        throughput_end_ns > loop_start_ns
            ? static_cast<double>(throughput_end_ns - loop_start_ns) / 1e9
            : 0.0;
    const double msgs_per_s =
        elapsed_s > 0.0 ? static_cast<double>(received) / elapsed_s : 0.0;
    const uint64_t p50 = PercentileNs(&latencies, 50);
    const uint64_t p95 = PercentileNs(&latencies, 95);

    std::ostringstream json;
    json << std::fixed << std::setprecision(1);
    json << "{\"codec\":\"business\""
         << ",\"socket\":\"pubsub\""
         << ",\"handshake\":\"ready+monitor\""
         << ",\"received\":" << received
         << ",\"lost\":" << lost
         << ",\"first_seq\":" << first_seq
         << ",\"expected\":" << config.messages
         << ",\"payload_bytes\":" << config.payload_bytes
         << ",\"queue_depth\":" << config.queue_depth
         << ",\"hwm\":" << config.hwm
         << ",\"p50_ns\":" << p50
         << ",\"p95_ns\":" << p95
         << ",\"msgs_per_s\":" << msgs_per_s
         << "}\n";
    std::cout << json.str() << std::flush;
    try {
        RemoveStaleSocket(path);
    } catch (...) {
    }
    UnlinkQuiet(ReadyPath(path));
    UnlinkQuiet(PeerPath(path));
    UnlinkQuiet(DonePath(path));
    if (!frames_ok) {
        std::cerr << "ValidateSemanticFrame failed: " << frame_error << "\n";
        return 1;
    }
    // PUB mute-drops when HWM is hit; lost is the measured result.
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
