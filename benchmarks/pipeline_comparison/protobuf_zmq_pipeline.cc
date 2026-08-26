// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/pipeline_comparison/autonomy_pipeline.pb.h"
#include "benchmarks/pipeline_comparison/pipeline_common.h"
#include "benchmarks/pipeline_comparison/semantic_protobuf_codec.h"

#include <zmq.h>

#include <arpa/inet.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>
#include <vector>

namespace mino::benchmarks::pipeline {
namespace {

constexpr std::string_view kBackend = "protobuf-zmq";
constexpr int kIoThreads = 1;
constexpr int kDefaultHwm = 64;
constexpr int kAckHwm = 1;
constexpr size_t kEdgeCount = 5;
constexpr size_t kAckPortOffset = kEdgeCount;
constexpr uint16_t kDefaultPortBase = 24'000;
constexpr uint16_t kMaximumPortBase = 65'526;
constexpr size_t kMaximumPayloadBytes = kLargePayloadBytes;
// The schema's non-payload fields need far less than this allowance. Keeping a
// separate encoded bound prevents an oversized ZeroMQ message from being
// accepted merely because its protobuf payload is later rejected.
constexpr size_t kMaximumEncodedBytes = kMaximumPayloadBytes + 1024;
constexpr size_t kMaximumAckBytes = 64 * 1024;
constexpr std::array<uint8_t, 8> kAckMagic = {'M', 'I', 'N', 'O',
                                               'Z', 'A', 'C', 'K'};
constexpr size_t kAckFixedBytes = kAckMagic.size() + 1 + 8 + 4;
constexpr uint64_t kNanosecondsPerSecond = 1'000'000'000ull;
constexpr uint64_t kNanosecondsPerMillisecond = 1'000'000ull;

enum class EdgeTransport : uint8_t {
    kIpc,
    kTcp,
};

struct BackendOptions {
    EdgeTransport input_transport = EdgeTransport::kIpc;
    EdgeTransport output_transport = EdgeTransport::kIpc;
    std::string listen_address = "127.0.0.1";
    std::string peer_address = "127.0.0.1";
    std::string upstream_address = "127.0.0.1";
    uint16_t port_base = kDefaultPortBase;
    int hwm = kDefaultHwm;
};

struct BoundSocketFile {
    std::filesystem::path path;
    dev_t device = 0;
    ino_t inode = 0;
    bool owned = false;
};

struct RunStatistics {
    uint64_t measured_completed = 0;
    uint64_t encoded_bytes_total = 0;
    uint64_t duplicate = 0;
    uint64_t out_of_order = 0;
    uint64_t corrupt = 0;
    uint64_t first_measured_origin_ns = 0;
    uint64_t first_measured_completion_ns = 0;
    uint64_t last_measured_completion_ns = 0;
    std::vector<uint64_t> latencies_ns;
};

std::string ZmqError(std::string_view operation) {
    const int error_number = zmq_errno();
    return std::string(operation) + ": " + zmq_strerror(error_number);
}

uint64_t AbsoluteDeadline(const CommonOptions& options) {
    const uint64_t now = NowNs();
    const uint64_t duration = options.deadline_seconds * kNanosecondsPerSecond;
    if (duration > std::numeric_limits<uint64_t>::max() - now) {
        return std::numeric_limits<uint64_t>::max();
    }
    return now + duration;
}

int RemainingTimeoutMs(uint64_t absolute_deadline_ns) {
    const uint64_t now = NowNs();
    if (now >= absolute_deadline_ns) return 0;
    const uint64_t remaining_ns = absolute_deadline_ns - now;
    const uint64_t rounded_up_ms =
        (remaining_ns + kNanosecondsPerMillisecond - 1) /
        kNanosecondsPerMillisecond;
    return static_cast<int>(
        std::min<uint64_t>(rounded_up_ms, static_cast<uint64_t>(INT_MAX)));
}

void SetIntSocketOption(void* socket, int option, int value,
                        std::string_view name) {
    if (zmq_setsockopt(socket, option, &value, sizeof(value)) != 0) {
        throw std::runtime_error(ZmqError("cannot set ZeroMQ " +
                                         std::string(name)));
    }
}

void SetInt64SocketOption(void* socket, int option, int64_t value,
                          std::string_view name) {
    if (zmq_setsockopt(socket, option, &value, sizeof(value)) != 0) {
        throw std::runtime_error(ZmqError("cannot set ZeroMQ " +
                                         std::string(name)));
    }
}

void RemoveStaleSocketFile(const std::filesystem::path& path) {
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

void RemoveOwnedSocketFile(const BoundSocketFile& bound) noexcept {
    if (!bound.owned) return;
    struct stat status {};
    if (lstat(bound.path.c_str(), &status) != 0) return;
    if (!S_ISSOCK(status.st_mode) || status.st_dev != bound.device ||
        status.st_ino != bound.inode) {
        return;
    }
    static_cast<void>(unlink(bound.path.c_str()));
}

std::optional<size_t> InputEdge(Role role) {
    switch (role) {
        case Role::kPerception: return std::nullopt;
        case Role::kPrediction: return 0;
        case Role::kPlanning: return 1;
        case Role::kControl: return 2;
        case Role::kGuardian: return 3;
        case Role::kCanbus: return 4;
    }
    throw std::invalid_argument("invalid pipeline role");
}

std::optional<size_t> OutputEdge(Role role) {
    switch (role) {
        case Role::kPerception: return 0;
        case Role::kPrediction: return 1;
        case Role::kPlanning: return 2;
        case Role::kControl: return 3;
        case Role::kGuardian: return 4;
        case Role::kCanbus: return std::nullopt;
    }
    throw std::invalid_argument("invalid pipeline role");
}

std::string_view TransportName(EdgeTransport transport) {
    switch (transport) {
        case EdgeTransport::kIpc: return "ipc";
        case EdgeTransport::kTcp: return "tcp";
    }
    throw std::invalid_argument("invalid ZeroMQ edge transport");
}

std::filesystem::path EdgePath(const CommonOptions& options, size_t edge) {
    return options.runtime_dir / ("edge-" + std::to_string(edge) + ".sock");
}

std::filesystem::path AckEdgePath(const CommonOptions& options, size_t edge) {
    return options.runtime_dir /
           ("edge-" + std::to_string(edge) + ".ack.sock");
}

std::string IpcEdgeEndpoint(const CommonOptions& options, size_t edge) {
    return "ipc://" + EdgePath(options, edge).string();
}

std::string IpcAckEndpoint(const CommonOptions& options, size_t edge) {
    return "ipc://" + AckEdgePath(options, edge).string();
}

std::string TcpEdgeEndpoint(std::string_view address, uint16_t port_base,
                            size_t edge) {
    return "tcp://" + std::string(address) + ":" +
           std::to_string(static_cast<uint32_t>(port_base) + edge);
}

std::string TcpAckEndpoint(std::string_view address, uint16_t port_base,
                           size_t edge) {
    return "tcp://" + std::string(address) + ":" +
           std::to_string(static_cast<uint32_t>(port_base) + kAckPortOffset +
                          edge);
}

std::string InputEndpoint(const CommonOptions& options,
                          const BackendOptions& backend, size_t edge) {
    if (backend.input_transport == EdgeTransport::kIpc) {
        return IpcEdgeEndpoint(options, edge);
    }
    return TcpEdgeEndpoint(backend.listen_address, backend.port_base, edge);
}

std::string OutputEndpoint(const CommonOptions& options,
                           const BackendOptions& backend, size_t edge) {
    if (backend.output_transport == EdgeTransport::kIpc) {
        return IpcEdgeEndpoint(options, edge);
    }
    return TcpEdgeEndpoint(backend.peer_address, backend.port_base, edge);
}

std::string AckInputEndpoint(const CommonOptions& options,
                             const BackendOptions& backend, size_t edge) {
    if (backend.output_transport == EdgeTransport::kIpc) {
        return IpcAckEndpoint(options, edge);
    }
    return TcpAckEndpoint(backend.listen_address, backend.port_base, edge);
}

std::string AckOutputEndpoint(const CommonOptions& options,
                              const BackendOptions& backend, size_t edge) {
    if (backend.input_transport == EdgeTransport::kIpc) {
        return IpcAckEndpoint(options, edge);
    }
    return TcpAckEndpoint(backend.upstream_address, backend.port_base, edge);
}

void ValidateIpcPath(const std::filesystem::path& socket_path) {
    struct sockaddr_un address {};
    const std::string path = socket_path.string();
    if (path.size() >= sizeof(address.sun_path)) {
        throw std::runtime_error(
            "Unix IPC path is too long (maximum " +
            std::to_string(sizeof(address.sun_path) - 1) + " bytes): " + path);
    }
}

void ValidateEndpoints(const CommonOptions& options,
                       const BackendOptions& backend) {
    if (options.run_id.size() > kMaximumAckBytes - kAckFixedBytes) {
        throw std::runtime_error(
            "--run-id exceeds the ZeroMQ completion ACK size limit");
    }
    const std::optional<size_t> input_edge = InputEdge(options.role);
    if (input_edge.has_value() &&
        backend.input_transport == EdgeTransport::kIpc) {
        ValidateIpcPath(EdgePath(options, *input_edge));
        ValidateIpcPath(AckEdgePath(options, *input_edge));
    }
    const std::optional<size_t> output_edge = OutputEdge(options.role);
    if (output_edge.has_value() &&
        backend.output_transport == EdgeTransport::kIpc) {
        ValidateIpcPath(EdgePath(options, *output_edge));
        ValidateIpcPath(AckEdgePath(options, *output_edge));
    }
}

void AppendUint32(uint32_t value, std::string* bytes) {
    for (int shift = 24; shift >= 0; shift -= 8) {
        bytes->push_back(static_cast<char>((value >> shift) & 0xffu));
    }
}

void AppendUint64(uint64_t value, std::string* bytes) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        bytes->push_back(static_cast<char>((value >> shift) & 0xffu));
    }
}

uint32_t DecodeUint32(const std::vector<uint8_t>& bytes, size_t offset) {
    uint32_t value = 0;
    for (size_t index = 0; index < 4; ++index) {
        value = (value << 8) | bytes[offset + index];
    }
    return value;
}

uint64_t DecodeUint64(const std::vector<uint8_t>& bytes, size_t offset) {
    uint64_t value = 0;
    for (size_t index = 0; index < 8; ++index) {
        value = (value << 8) | bytes[offset + index];
    }
    return value;
}

std::string EncodeCompletionAck(std::string_view run_id, size_t edge,
                                uint64_t total_frames) {
    if (edge >= kEdgeCount ||
        run_id.size() > kMaximumAckBytes - kAckFixedBytes ||
        run_id.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::invalid_argument("invalid completion ACK fields");
    }
    std::string bytes;
    bytes.reserve(kAckFixedBytes + run_id.size());
    for (uint8_t byte : kAckMagic) bytes.push_back(static_cast<char>(byte));
    bytes.push_back(static_cast<char>(edge));
    AppendUint64(total_frames, &bytes);
    AppendUint32(static_cast<uint32_t>(run_id.size()), &bytes);
    bytes.append(run_id);
    return bytes;
}

void ValidateCompletionAck(const std::vector<uint8_t>& bytes,
                           std::string_view expected_run_id,
                           size_t expected_edge, uint64_t expected_total_frames) {
    if (bytes.size() < kAckFixedBytes || bytes.size() > kMaximumAckBytes ||
        !std::equal(kAckMagic.begin(), kAckMagic.end(), bytes.begin())) {
        throw std::runtime_error("completion ACK has an invalid envelope");
    }
    size_t offset = kAckMagic.size();
    const size_t edge = bytes[offset++];
    const uint64_t total_frames = DecodeUint64(bytes, offset);
    offset += 8;
    const uint32_t run_id_size = DecodeUint32(bytes, offset);
    offset += 4;
    if (run_id_size != bytes.size() - offset) {
        throw std::runtime_error("completion ACK has an invalid run-id length");
    }
    const std::string_view run_id(
        reinterpret_cast<const char*>(bytes.data() + offset), run_id_size);
    if (run_id != expected_run_id) {
        throw std::runtime_error("completion ACK run-id mismatch");
    }
    if (edge != expected_edge) {
        throw std::runtime_error("completion ACK edge mismatch");
    }
    if (total_frames != expected_total_frames) {
        throw std::runtime_error("completion ACK total-frame count mismatch");
    }
}

class ZmqPipelineSockets {
  public:
    ZmqPipelineSockets(const CommonOptions& options,
                       const BackendOptions& backend,
                       uint64_t absolute_deadline_ns)
        : options_(options),
          backend_(backend),
          absolute_deadline_ns_(absolute_deadline_ns),
          initial_timeout_ms_(RemainingTimeoutMs(absolute_deadline_ns)) {
        try {
            Initialize();
        } catch (...) {
            Close();
            throw;
        }
    }

    ZmqPipelineSockets(const ZmqPipelineSockets&) = delete;
    ZmqPipelineSockets& operator=(const ZmqPipelineSockets&) = delete;

    ~ZmqPipelineSockets() { Close(); }

    void Send(const std::string& bytes, uint64_t absolute_deadline_ns) {
        if (data_output_ == nullptr) {
            throw std::logic_error("pipeline role has no data output socket");
        }
        SendFrame(data_output_, bytes, absolute_deadline_ns, "data");
    }

    std::vector<uint8_t> Receive(uint64_t absolute_deadline_ns) {
        if (data_input_ == nullptr) {
            throw std::logic_error("pipeline role has no data input socket");
        }
        return ReceiveFrame(data_input_, kMaximumEncodedBytes,
                            absolute_deadline_ns, "data");
    }

    void Complete(uint64_t total_frames, uint64_t absolute_deadline_ns) {
        const std::optional<size_t> output_edge = OutputEdge(options_.role);
        if (output_edge.has_value()) {
            if (ack_input_ == nullptr) {
                throw std::logic_error(
                    "pipeline role has no completion ACK input socket");
            }
            const std::vector<uint8_t> bytes =
                ReceiveFrame(ack_input_, kMaximumAckBytes,
                             absolute_deadline_ns, "completion ACK");
            ValidateCompletionAck(bytes, options_.run_id, *output_edge,
                                  total_frames);
        }

        const std::optional<size_t> input_edge = InputEdge(options_.role);
        if (input_edge.has_value()) {
            if (ack_output_ == nullptr) {
                throw std::logic_error(
                    "pipeline role has no completion ACK output socket");
            }
            const std::string bytes = EncodeCompletionAck(
                options_.run_id, *input_edge, total_frames);
            SendFrame(ack_output_, bytes, absolute_deadline_ns,
                      "completion ACK");
        }
    }

  private:
    void Initialize() {
        if (initial_timeout_ms_ == 0) {
            throw std::runtime_error("deadline expired before ZeroMQ setup");
        }
        context_ = zmq_ctx_new();
        if (context_ == nullptr) {
            throw std::runtime_error(ZmqError("cannot create ZeroMQ context"));
        }
        if (zmq_ctx_set(context_, ZMQ_IO_THREADS, kIoThreads) != 0) {
            throw std::runtime_error(
                ZmqError("cannot configure ZeroMQ context io_threads"));
        }

        const std::optional<size_t> input_edge = InputEdge(options_.role);
        if (input_edge.has_value()) {
            data_input_ = CreateSocket(ZMQ_PULL, kMaximumEncodedBytes,
                                       backend_.hwm);
            std::optional<std::filesystem::path> ipc_path;
            if (backend_.input_transport == EdgeTransport::kIpc) {
                ipc_path = EdgePath(options_, *input_edge);
            }
            BindSocket(data_input_,
                       InputEndpoint(options_, backend_, *input_edge), ipc_path,
                       &bound_data_input_);
        }

        const std::optional<size_t> output_edge = OutputEdge(options_.role);
        if (output_edge.has_value()) {
            ack_input_ =
                CreateSocket(ZMQ_PULL, kMaximumAckBytes, kAckHwm);
            std::optional<std::filesystem::path> ipc_path;
            if (backend_.output_transport == EdgeTransport::kIpc) {
                ipc_path = AckEdgePath(options_, *output_edge);
            }
            BindSocket(ack_input_,
                       AckInputEndpoint(options_, backend_, *output_edge),
                       ipc_path, &bound_ack_input_);
        }

        if (output_edge.has_value()) {
            data_output_ = CreateSocket(ZMQ_PUSH, kMaximumEncodedBytes,
                                        backend_.hwm);
            ConnectSocket(data_output_,
                          OutputEndpoint(options_, backend_, *output_edge));
        }

        if (input_edge.has_value()) {
            ack_output_ = CreateSocket(ZMQ_PUSH, kMaximumAckBytes, kAckHwm);
            ConnectSocket(ack_output_,
                          AckOutputEndpoint(options_, backend_, *input_edge));
        }
    }

    void SendFrame(void* socket, const std::string& bytes,
                   uint64_t absolute_deadline_ns, std::string_view channel) {
        if (bytes.size() > static_cast<size_t>(INT_MAX)) {
            throw std::runtime_error("ZeroMQ " + std::string(channel) +
                                     " frame exceeds zmq_send limit");
        }
        while (true) {
            const int timeout_ms = RemainingTimeoutMs(absolute_deadline_ns);
            if (timeout_ms == 0) {
                throw std::runtime_error("deadline expired before ZeroMQ " +
                                         std::string(channel) + " send");
            }
            SetIntSocketOption(socket, ZMQ_SNDTIMEO, timeout_ms, "SNDTIMEO");
            const int sent = zmq_send(socket, bytes.data(), bytes.size(), 0);
            if (sent == static_cast<int>(bytes.size())) return;
            if (sent >= 0) {
                throw std::runtime_error(
                    "ZeroMQ " + std::string(channel) +
                    " send did not send the complete frame");
            }
            const int error_number = zmq_errno();
            if (error_number == EINTR) continue;
            if (error_number == EAGAIN) {
                throw std::runtime_error("ZeroMQ " + std::string(channel) +
                                         " send timed out at deadline");
            }
            throw std::runtime_error(
                "ZeroMQ " + std::string(channel) + " send failed: " +
                std::string(zmq_strerror(error_number)));
        }
    }

    std::vector<uint8_t> ReceiveFrame(void* socket, size_t maximum_bytes,
                                      uint64_t absolute_deadline_ns,
                                      std::string_view channel) {
        std::vector<uint8_t> bytes(maximum_bytes);
        while (true) {
            const int timeout_ms = RemainingTimeoutMs(absolute_deadline_ns);
            if (timeout_ms == 0) {
                throw std::runtime_error("deadline expired before ZeroMQ " +
                                         std::string(channel) + " receive");
            }
            SetIntSocketOption(socket, ZMQ_RCVTIMEO, timeout_ms, "RCVTIMEO");
            const int received =
                zmq_recv(socket, bytes.data(), bytes.size(), 0);
            if (received < 0) {
                const int error_number = zmq_errno();
                if (error_number == EINTR) continue;
                if (error_number == EAGAIN) {
                    throw std::runtime_error(
                        "ZeroMQ " + std::string(channel) +
                        " receive timed out at deadline");
                }
                throw std::runtime_error(
                    "ZeroMQ " + std::string(channel) + " receive failed: " +
                    std::string(zmq_strerror(error_number)));
            }
            if (static_cast<size_t>(received) > bytes.size()) {
                if (channel == "data") {
                    throw std::runtime_error(
                        "received data frame exceeds encoded-size limit");
                }
                throw std::runtime_error(
                    "received completion ACK exceeds configured-size limit");
            }
            int more = 0;
            size_t more_size = sizeof(more);
            if (zmq_getsockopt(socket, ZMQ_RCVMORE, &more, &more_size) != 0) {
                throw std::runtime_error(
                    ZmqError("cannot inspect ZeroMQ multipart state"));
            }
            if (more != 0) {
                throw std::runtime_error("multipart ZeroMQ messages are not " +
                                         std::string(channel) + " frames");
            }
            bytes.resize(static_cast<size_t>(received));
            return bytes;
        }
    }

    void BindSocket(void* socket, const std::string& endpoint,
                    const std::optional<std::filesystem::path>& ipc_path,
                    BoundSocketFile* bound) {
        if (ipc_path.has_value()) RemoveStaleSocketFile(*ipc_path);
        if (zmq_bind(socket, endpoint.c_str()) != 0) {
            throw std::runtime_error(ZmqError("cannot bind " + endpoint));
        }
        if (!ipc_path.has_value()) return;
        struct stat status {};
        if (lstat(ipc_path->c_str(), &status) != 0 ||
            !S_ISSOCK(status.st_mode)) {
            throw std::runtime_error(
                "ZeroMQ bind did not create the expected IPC socket: " +
                ipc_path->string());
        }
        *bound = BoundSocketFile{*ipc_path, status.st_dev, status.st_ino, true};
    }

    void ConnectSocket(void* socket, const std::string& endpoint) {
        if (zmq_connect(socket, endpoint.c_str()) != 0) {
            throw std::runtime_error(ZmqError("cannot connect " + endpoint));
        }
    }

    void CloseSocket(void** socket, bool flush_pending) noexcept {
        if (*socket == nullptr) return;
        if (flush_pending) {
            int remaining_linger_ms = 0;
            try {
                remaining_linger_ms =
                    RemainingTimeoutMs(absolute_deadline_ns_);
            } catch (...) {
                remaining_linger_ms = 0;
            }
            static_cast<void>(zmq_setsockopt(
                *socket, ZMQ_LINGER, &remaining_linger_ms,
                sizeof(remaining_linger_ms)));
        }
        static_cast<void>(zmq_close(*socket));
        *socket = nullptr;
    }

    void Close() noexcept {
        CloseSocket(&ack_output_, true);
        CloseSocket(&data_output_, true);
        CloseSocket(&ack_input_, false);
        CloseSocket(&data_input_, false);
        RemoveOwnedSocketFile(bound_ack_input_);
        bound_ack_input_.owned = false;
        RemoveOwnedSocketFile(bound_data_input_);
        bound_data_input_.owned = false;
        if (context_ != nullptr) {
            while (zmq_ctx_term(context_) != 0 && zmq_errno() == EINTR) {
            }
            context_ = nullptr;
        }
    }

    void* CreateSocket(int type, size_t maximum_message_bytes, int hwm) {
        void* socket = zmq_socket(context_, type);
        if (socket == nullptr) {
            throw std::runtime_error(ZmqError("cannot create ZeroMQ socket"));
        }
        try {
            // PUSH sockets remain alive until downstream completion has flowed
            // back over the reverse ACK chain. Linger is still deadline-bounded
            // so a final admitted ACK cannot create an unbounded shutdown.
            SetIntSocketOption(socket, ZMQ_LINGER, initial_timeout_ms_, "LINGER");
            SetIntSocketOption(socket, ZMQ_IMMEDIATE, 1, "IMMEDIATE");
            SetIntSocketOption(socket, ZMQ_SNDHWM, hwm, "SNDHWM");
            SetIntSocketOption(socket, ZMQ_RCVHWM, hwm, "RCVHWM");
            SetIntSocketOption(socket, ZMQ_SNDTIMEO, initial_timeout_ms_,
                               "SNDTIMEO");
            SetIntSocketOption(socket, ZMQ_RCVTIMEO, initial_timeout_ms_,
                               "RCVTIMEO");
            SetInt64SocketOption(socket, ZMQ_MAXMSGSIZE,
                                 static_cast<int64_t>(maximum_message_bytes),
                                 "MAXMSGSIZE");
        } catch (...) {
            static_cast<void>(zmq_close(socket));
            throw;
        }
        return socket;
    }

    const CommonOptions& options_;
    const BackendOptions& backend_;
    uint64_t absolute_deadline_ns_;
    int initial_timeout_ms_;
    void* context_ = nullptr;
    void* data_input_ = nullptr;
    void* data_output_ = nullptr;
    void* ack_input_ = nullptr;
    void* ack_output_ = nullptr;
    BoundSocketFile bound_data_input_;
    BoundSocketFile bound_ack_input_;
};

void ValidateSequenceAndPhase(const CommonOptions& options,
                              const SemanticFrame& frame, uint64_t expected_id,
                              RunStatistics* statistics) {
    if (frame.sample_id != expected_id) {
        if (frame.sample_id < expected_id) {
            ++statistics->duplicate;
            throw std::runtime_error(
                "duplicate sample_id: expected " + std::to_string(expected_id) +
                ", got " + std::to_string(frame.sample_id));
        }
        ++statistics->out_of_order;
        throw std::runtime_error(
            "out-of-order sample_id: expected " +
            std::to_string(expected_id) + ", got " +
            std::to_string(frame.sample_id));
    }
    const bool measured = expected_id >= options.warmup_messages;
    if ((measured && frame.origin_timestamp_ns == 0) ||
        (!measured && frame.origin_timestamp_ns != 0)) {
        ++statistics->corrupt;
        throw std::runtime_error(
            measured ? "measured frame has a zero origin timestamp"
                     : "warmup frame has a non-zero origin timestamp");
    }
    if (frame.profile != static_cast<uint32_t>(options.profile)) {
        ++statistics->corrupt;
        throw std::runtime_error("frame profile does not match --profile");
    }
}

SemanticFrame ReceiveAndDecode(ZmqPipelineSockets* sockets,
                               uint64_t absolute_deadline_ns,
                               RunStatistics* statistics,
                               size_t* encoded_size) {
    std::vector<uint8_t> bytes;
    try {
        bytes = sockets->Receive(absolute_deadline_ns);
    } catch (const std::runtime_error& exception) {
        const std::string_view message(exception.what());
        if (message.find("exceeds encoded-size limit") !=
                std::string_view::npos ||
            message.find("multipart") != std::string_view::npos) {
            ++statistics->corrupt;
        }
        throw;
    }
    if (encoded_size != nullptr) *encoded_size = bytes.size();
    SemanticFrame frame;
    std::string error;
    if (!ParseFrame(bytes, &frame, &error)) {
        ++statistics->corrupt;
        throw std::runtime_error(error);
    }
    return frame;
}

uint64_t TotalFrames(const CommonOptions& options) {
    return options.warmup_messages + options.messages;
}

void RunSource(const CommonOptions& options, ZmqPipelineSockets* sockets,
               uint64_t absolute_deadline_ns, RunStatistics* statistics) {
    const uint64_t total = TotalFrames(options);
    const uint64_t schedule_start_ns = NowNs();
    for (uint64_t sample_id = 0; sample_id < total; ++sample_id) {
        PaceSource(schedule_start_ns, sample_id, options.publish_interval_us,
                   absolute_deadline_ns);
        const bool measured = sample_id >= options.warmup_messages;
        SemanticFrame frame =
            InitializeSourceFrame(sample_id, options.profile, measured);
        std::string error;
        if (!ApplyStageForClockMode(Role::kPerception, &frame,
                                    options.clock_mode, &error)) {
            throw std::runtime_error("perception stage rejected source frame: " +
                                     error);
        }
        std::string bytes = SerializeFrame(frame);
        sockets->Send(bytes, absolute_deadline_ns);
        if (measured) {
            ++statistics->measured_completed;
            statistics->encoded_bytes_total += bytes.size();
        }
    }
}

void RunForwarder(const CommonOptions& options, ZmqPipelineSockets* sockets,
                  uint64_t absolute_deadline_ns, RunStatistics* statistics) {
    const uint64_t total = TotalFrames(options);
    for (uint64_t expected_id = 0; expected_id < total; ++expected_id) {
        SemanticFrame frame = ReceiveAndDecode(
            sockets, absolute_deadline_ns, statistics, nullptr);
        ValidateSequenceAndPhase(options, frame, expected_id, statistics);
        std::string error;
        if (!ApplyStageForClockMode(options.role, &frame,
                                    options.clock_mode, &error)) {
            ++statistics->corrupt;
            throw std::runtime_error(std::string(RoleName(options.role)) +
                                     " stage rejected frame: " + error);
        }
        std::string bytes = SerializeFrame(frame);
        sockets->Send(bytes, absolute_deadline_ns);
        if (expected_id >= options.warmup_messages) {
            ++statistics->measured_completed;
            statistics->encoded_bytes_total += bytes.size();
        }
    }
}

void RunSink(const CommonOptions& options, ZmqPipelineSockets* sockets,
             uint64_t absolute_deadline_ns, RunStatistics* statistics) {
    constexpr uint64_t kMaximumInitialLatencyReserve = 1'000'000;
    statistics->latencies_ns.reserve(static_cast<size_t>(std::min(
        options.messages, kMaximumInitialLatencyReserve)));
    const uint64_t total = TotalFrames(options);
    for (uint64_t expected_id = 0; expected_id < total; ++expected_id) {
        size_t encoded_size = 0;
        SemanticFrame frame = ReceiveAndDecode(
            sockets, absolute_deadline_ns, statistics, &encoded_size);
        ValidateSequenceAndPhase(options, frame, expected_id, statistics);
        std::string error;
        if (!ApplyStageForClockMode(Role::kCanbus, &frame,
                                    options.clock_mode, &error)) {
            ++statistics->corrupt;
            throw std::runtime_error("canbus stage rejected frame: " + error);
        }
        if (expected_id < options.warmup_messages) continue;

        // ApplyStage performs the complete deterministic payload validation.
        // Take the endpoint timestamp only after that work and sequence checking.
        const uint64_t completion_ns = NowNs();
        if (statistics->measured_completed == 0) {
            statistics->first_measured_origin_ns = frame.origin_timestamp_ns;
            statistics->first_measured_completion_ns = completion_ns;
        }
        statistics->last_measured_completion_ns = completion_ns;
        if (options.clock_mode == ClockMode::kSameHost) {
            if (completion_ns < frame.origin_timestamp_ns) {
                ++statistics->corrupt;
                throw std::runtime_error(
                    "sink completion timestamp precedes frame origin");
            }
            statistics->latencies_ns.push_back(completion_ns -
                                               frame.origin_timestamp_ns);
        }
        statistics->encoded_bytes_total += encoded_size;
        ++statistics->measured_completed;
    }
}

std::string ProtobufVersion() {
#if defined(GOOGLE_PROTOBUF_VERSION)
    constexpr int version = GOOGLE_PROTOBUF_VERSION;
    return std::to_string(version / 1'000'000) + "." +
           std::to_string((version / 1'000) % 1'000) + "." +
           std::to_string(version % 1'000);
#else
    return "unknown";
#endif
}

std::string AggregateTransport(const CommonOptions& options,
                               const BackendOptions& backend) {
    const bool has_input = InputEdge(options.role).has_value();
    const bool has_output = OutputEdge(options.role).has_value();
    if (!has_input) return std::string(TransportName(backend.output_transport));
    if (!has_output) return std::string(TransportName(backend.input_transport));
    if (backend.input_transport == backend.output_transport) {
        return std::string(TransportName(backend.input_transport));
    }
    return "mixed";
}

std::string BackendDetails(const CommonOptions& options,
                           const BackendOptions& backend) {
    int zmq_major = 0;
    int zmq_minor = 0;
    int zmq_patch = 0;
    zmq_version(&zmq_major, &zmq_minor, &zmq_patch);
    std::string details =
        "{\"libzmq_version\":\"" + std::to_string(zmq_major) + "." +
        std::to_string(zmq_minor) + "." + std::to_string(zmq_patch) +
        "\",\"protobuf_version\":\"" + ProtobufVersion() +
        "\",\"transport\":\"" + AggregateTransport(options, backend) +
        "\",\"input_transport\":\"" +
        std::string(TransportName(backend.input_transport)) +
        "\",\"output_transport\":\"" +
        std::string(TransportName(backend.output_transport)) +
        "\",\"socket_pattern\":\"upstream_push_downstream_pull\"," +
        "\"io_threads\":1,\"hwm\":" + std::to_string(backend.hwm) +
        ",\"tcp\":{\"listen_address\":\"" +
        JsonEscape(backend.listen_address) + "\",\"peer_address\":\"" +
        JsonEscape(backend.peer_address) + "\",\"upstream_address\":\"" +
        JsonEscape(backend.upstream_address) + "\",\"port_base\":" +
        std::to_string(backend.port_base) +
        ",\"ack_port_offset\":5}," +
        "\"linger\":\"remaining_process_deadline_at_close\",\"immediate\":1," +
        "\"timeouts\":{\"send\":\"deadline_bounded\"," +
        "\"receive\":\"deadline_bounded\"},\"endpoints\":[";
    bool needs_comma = false;
    if (const std::optional<size_t> edge = InputEdge(options.role)) {
        details += "{\"direction\":\"input\",\"edge\":" +
                   std::to_string(*edge) + ",\"transport\":\"" +
                   std::string(TransportName(backend.input_transport)) +
                   "\",\"endpoint\":\"" +
                   JsonEscape(InputEndpoint(options, backend, *edge)) + "\"}";
        needs_comma = true;
    }
    if (const std::optional<size_t> edge = OutputEdge(options.role)) {
        if (needs_comma) details += ',';
        details += "{\"direction\":\"output\",\"edge\":" +
                   std::to_string(*edge) + ",\"transport\":\"" +
                   std::string(TransportName(backend.output_transport)) +
                   "\",\"endpoint\":\"" +
                   JsonEscape(OutputEndpoint(options, backend, *edge)) + "\"}";
    }
    details +=
        "],\"completion_ack\":{\"pattern\":"
        "\"downstream_push_upstream_pull\",\"encoding\":\"binary_v1\","
        "\"validated_fields\":[\"run_id\",\"edge\",\"total_frames\"],"
        "\"hwm\":";
    details += std::to_string(kAckHwm) + ",\"endpoints\":[";
    needs_comma = false;
    if (const std::optional<size_t> edge = OutputEdge(options.role)) {
        details += "{\"direction\":\"input\",\"edge\":" +
                   std::to_string(*edge) + ",\"transport\":\"" +
                   std::string(TransportName(backend.output_transport)) +
                   "\",\"endpoint\":\"" +
                   JsonEscape(AckInputEndpoint(options, backend, *edge)) +
                   "\"}";
        needs_comma = true;
    }
    if (const std::optional<size_t> edge = InputEdge(options.role)) {
        if (needs_comma) details += ',';
        details += "{\"direction\":\"output\",\"edge\":" +
                   std::to_string(*edge) + ",\"transport\":\"" +
                   std::string(TransportName(backend.input_transport)) +
                   "\",\"endpoint\":\"" +
                   JsonEscape(AckOutputEndpoint(options, backend, *edge)) +
                   "\"}";
    }
    details += "]}}";
    return details;
}

uint64_t ParseUnsigned(std::string_view value, std::string_view option) {
    uint64_t parsed = 0;
    const auto conversion =
        std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (value.empty() || conversion.ec != std::errc{} ||
        conversion.ptr != value.data() + value.size()) {
        throw std::runtime_error(std::string(option) +
                                 " requires an unsigned integer");
    }
    return parsed;
}

std::optional<std::string_view> OptionValue(int* index, int argc, char** argv,
                                            std::string_view option) {
    const std::string_view argument(argv[*index]);
    if (argument == option) {
        if (*index + 1 >= argc || argv[*index + 1] == nullptr) {
            throw std::runtime_error(std::string(option) + " requires a value");
        }
        const std::string_view value(argv[++(*index)]);
        if (value.empty()) {
            throw std::runtime_error(std::string(option) +
                                     " requires a non-empty value");
        }
        return value;
    }
    if (argument.size() > option.size() && argument.starts_with(option) &&
        argument[option.size()] == '=') {
        const std::string_view value = argument.substr(option.size() + 1);
        if (value.empty()) {
            throw std::runtime_error(std::string(option) +
                                     " requires a non-empty value");
        }
        return value;
    }
    return std::nullopt;
}

EdgeTransport ParseTransport(std::string_view value, std::string_view option) {
    if (value == "ipc") return EdgeTransport::kIpc;
    if (value == "tcp") return EdgeTransport::kTcp;
    throw std::runtime_error(std::string(option) + " must be ipc or tcp");
}

void ValidateIpv4(std::string_view value, std::string_view option) {
    in_addr address {};
    const std::string text(value);
    if (inet_pton(AF_INET, text.c_str(), &address) != 1) {
        throw std::runtime_error(std::string(option) +
                                 " requires a numeric IPv4 address");
    }
}

BackendOptions ParseBackendOptions(int argc, char** argv) {
    if (argc < 0 || (argc > 0 && argv == nullptr)) {
        throw std::invalid_argument("invalid argc/argv");
    }
    constexpr std::array<std::string_view, 10> kCommonOptions = {
        "--role",          "--profile",          "--messages",
        "--warmup-messages", "--publish-interval-us", "--deadline-seconds",
        "--clock-mode",    "--run-id",           "--runtime-dir",
        "--output",
    };
    std::array<bool, kCommonOptions.size()> common_seen {};
    BackendOptions options;
    bool input_transport_seen = false;
    bool output_transport_seen = false;
    bool listen_seen = false;
    bool peer_seen = false;
    bool upstream_seen = false;
    bool port_seen = false;
    bool hwm_seen = false;
    for (int index = 1; index < argc; ++index) {
        if (argv[index] == nullptr) {
            throw std::invalid_argument("argv contains a null argument");
        }
        if (const auto value =
                OptionValue(&index, argc, argv, "--input-transport")) {
            if (input_transport_seen) {
                throw std::runtime_error(
                    "--input-transport may be specified only once");
            }
            input_transport_seen = true;
            options.input_transport =
                ParseTransport(*value, "--input-transport");
            continue;
        }
        if (const auto value =
                OptionValue(&index, argc, argv, "--output-transport")) {
            if (output_transport_seen) {
                throw std::runtime_error(
                    "--output-transport may be specified only once");
            }
            output_transport_seen = true;
            options.output_transport =
                ParseTransport(*value, "--output-transport");
            continue;
        }
        if (const auto value =
                OptionValue(&index, argc, argv, "--listen-address")) {
            if (listen_seen) {
                throw std::runtime_error(
                    "--listen-address may be specified only once");
            }
            listen_seen = true;
            ValidateIpv4(*value, "--listen-address");
            options.listen_address = std::string(*value);
            continue;
        }
        if (const auto value =
                OptionValue(&index, argc, argv, "--peer-address")) {
            if (peer_seen) {
                throw std::runtime_error(
                    "--peer-address may be specified only once");
            }
            peer_seen = true;
            ValidateIpv4(*value, "--peer-address");
            options.peer_address = std::string(*value);
            continue;
        }
        if (const auto value =
                OptionValue(&index, argc, argv, "--upstream-address")) {
            if (upstream_seen) {
                throw std::runtime_error(
                    "--upstream-address may be specified only once");
            }
            upstream_seen = true;
            ValidateIpv4(*value, "--upstream-address");
            options.upstream_address = std::string(*value);
            continue;
        }
        if (const auto value =
                OptionValue(&index, argc, argv, "--port-base")) {
            if (port_seen) {
                throw std::runtime_error(
                    "--port-base may be specified only once");
            }
            port_seen = true;
            const uint64_t parsed = ParseUnsigned(*value, "--port-base");
            if (parsed == 0 || parsed > kMaximumPortBase) {
                throw std::runtime_error("--port-base must be in [1, 65526]");
            }
            options.port_base = static_cast<uint16_t>(parsed);
            continue;
        }
        if (const auto value = OptionValue(&index, argc, argv, "--hwm")) {
            if (hwm_seen) {
                throw std::runtime_error("--hwm may be specified only once");
            }
            hwm_seen = true;
            const uint64_t parsed = ParseUnsigned(*value, "--hwm");
            if (parsed == 0 || parsed > static_cast<uint64_t>(INT_MAX)) {
                throw std::runtime_error("--hwm must be in [1, INT_MAX]");
            }
            options.hwm = static_cast<int>(parsed);
            continue;
        }

        bool common = false;
        for (size_t common_index = 0; common_index < kCommonOptions.size();
             ++common_index) {
            if (!OptionValue(&index, argc, argv,
                             kCommonOptions[common_index]).has_value()) {
                continue;
            }
            if (common_seen[common_index]) {
                throw std::runtime_error(
                    std::string(kCommonOptions[common_index]) +
                    " may be specified only once");
            }
            common_seen[common_index] = true;
            common = true;
            break;
        }
        if (common) continue;
        throw std::runtime_error("unknown argument: " +
                                 std::string(argv[index]));
    }
    return options;
}

void PopulateResult(const CommonOptions& options,
                    const RunStatistics& statistics, bool success,
                    SinkResult* result) {
    result->counts.offered = options.messages;
    result->counts.received = statistics.measured_completed;
    result->counts.duplicate = statistics.duplicate;
    result->counts.out_of_order = statistics.out_of_order;
    result->counts.corrupt = statistics.corrupt;
    result->counts.lost =
        statistics.measured_completed < options.messages
            ? options.messages - statistics.measured_completed
            : 0;
    if (success) {
        result->counts.received = options.messages;
        result->counts.lost = 0;
    }
    if (statistics.measured_completed != 0) {
        result->encoded_bytes =
            (statistics.encoded_bytes_total +
             statistics.measured_completed / 2) /
            statistics.measured_completed;
    }
    if (options.role != Role::kCanbus) return;

    result->latency_ns = Summarize(statistics.latencies_ns);
    if (options.clock_mode == ClockMode::kIndependentHosts) {
        if (statistics.measured_completed > 1 &&
            statistics.first_measured_completion_ns != 0 &&
            statistics.last_measured_completion_ns >
                statistics.first_measured_completion_ns) {
            result->elapsed_ns = statistics.last_measured_completion_ns -
                                 statistics.first_measured_completion_ns;
            result->throughput_messages_per_second =
                static_cast<double>(statistics.measured_completed - 1) *
                static_cast<double>(kNanosecondsPerSecond) /
                static_cast<double>(result->elapsed_ns);
        }
        return;
    }
    if (statistics.first_measured_origin_ns != 0 &&
        statistics.last_measured_completion_ns >=
            statistics.first_measured_origin_ns) {
        result->elapsed_ns = statistics.last_measured_completion_ns -
                             statistics.first_measured_origin_ns;
        if (result->elapsed_ns != 0) {
            result->throughput_messages_per_second =
                static_cast<double>(statistics.measured_completed) *
                static_cast<double>(kNanosecondsPerSecond) /
                static_cast<double>(result->elapsed_ns);
        }
    }
}

void WriteResultBestEffort(const SinkResult& result) noexcept {
    try {
        WriteSinkResult(result);
    } catch (const std::exception& exception) {
        std::cerr << "failed to write result artifact "
                  << result.options.output << ": " << exception.what() << '\n';
    } catch (...) {
        std::cerr << "failed to write result artifact "
                  << result.options.output << ": unknown exception\n";
    }
}

int PipelineMain(int argc, char** argv) {
    std::optional<CommonOptions> parsed_options;
    RunStatistics statistics;
    SinkResult result;
    BackendOptions backend;
    try {
        parsed_options = ParseCommonOptions(argc, argv);
        const CommonOptions& options = *parsed_options;
        result.backend = std::string(kBackend);
        result.options = options;
        result.payload_bytes = ProfilePayloadBytes(options.profile);

        backend = ParseBackendOptions(argc, argv);
        result.backend_details = BackendDetails(options, backend);
        ValidateEndpoints(options, backend);
        const uint64_t absolute_deadline_ns = AbsoluteDeadline(options);
        ZmqPipelineSockets sockets(options, backend, absolute_deadline_ns);

        WriteReadyFile(options.runtime_dir, kBackend, options.role,
                       options.run_id);
        if (!WaitForStartFile(options.runtime_dir, options.run_id,
                              absolute_deadline_ns)) {
            throw std::runtime_error("deadline expired waiting for start file");
        }

        switch (options.role) {
            case Role::kPerception:
                RunSource(options, &sockets, absolute_deadline_ns, &statistics);
                break;
            case Role::kPrediction:
            case Role::kPlanning:
            case Role::kControl:
            case Role::kGuardian:
                RunForwarder(options, &sockets, absolute_deadline_ns,
                             &statistics);
                break;
            case Role::kCanbus:
                RunSink(options, &sockets, absolute_deadline_ns, &statistics);
                break;
        }

        if (statistics.measured_completed != options.messages) {
            throw std::runtime_error("completed measured-frame count mismatch");
        }
        sockets.Complete(TotalFrames(options), absolute_deadline_ns);
        PopulateResult(options, statistics, true, &result);
        WriteSinkResult(result);
        return 0;
    } catch (const std::exception& exception) {
        std::cerr << kBackend << " pipeline failed: " << exception.what()
                  << '\n';
        if (parsed_options.has_value()) {
            if (result.backend.empty()) result.backend = std::string(kBackend);
            result.options = *parsed_options;
            result.payload_bytes = ProfilePayloadBytes(result.options.profile);
            if (result.backend_details.empty()) {
                result.backend_details =
                    BackendDetails(result.options, backend);
            }
            PopulateResult(result.options, statistics, false, &result);
            result.outcome = "failure";
            result.error = exception.what();
            WriteResultBestEffort(result);
        }
        return 1;
    } catch (...) {
        std::cerr << kBackend << " pipeline failed: unknown exception\n";
        if (parsed_options.has_value()) {
            if (result.backend.empty()) result.backend = std::string(kBackend);
            result.options = *parsed_options;
            result.payload_bytes = ProfilePayloadBytes(result.options.profile);
            if (result.backend_details.empty()) {
                result.backend_details =
                    BackendDetails(result.options, backend);
            }
            PopulateResult(result.options, statistics, false, &result);
            result.outcome = "failure";
            result.error = "unknown exception";
            WriteResultBestEffort(result);
        }
        return 1;
    }
}

}  // namespace
}  // namespace mino::benchmarks::pipeline

int main(int argc, char** argv) {
    return mino::benchmarks::pipeline::PipelineMain(argc, argv);
}
