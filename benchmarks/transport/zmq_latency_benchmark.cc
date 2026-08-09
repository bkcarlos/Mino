// Copyright 2026 The Mino Authors
//
// Direct libzmq comparison benchmark. This intentionally does not implement or
// use TransportDriver: it measures ZeroMQ's native TCP messaging path.

#include <zmq.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "mino/bridge/source_identity.h"

namespace {

using Clock = std::chrono::steady_clock;

constexpr std::array<std::byte, 8> kMagic = {
    std::byte{'Z'}, std::byte{'M'}, std::byte{'Q'}, std::byte{'B'},
    std::byte{'E'}, std::byte{'N'}, std::byte{'0'}, std::byte{'1'},
};
constexpr uint32_t kHello = 1;
constexpr uint32_t kData = 2;
constexpr uint32_t kStop = 3;
constexpr size_t kHeaderBytes = 40;
constexpr std::array<uint32_t, 6> kTopicCounts = {1, 2, 4, 8, 16, 32};

struct Options {
    std::string role;
    std::string address;
    uint16_t port = 0;
    uint16_t lane_count = 1;
    uint32_t messages_per_topic = 64;
    size_t payload_bytes = 256;
    uint32_t io_threads = 1;
    uint32_t deadline_seconds = 120;
    std::string output;
};

struct Message {
    uint32_t kind = 0;
    uint32_t phase = 0;
    uint32_t topic = 0;
    uint32_t sequence = 0;
    uint64_t origin_ns = 0;
};

struct LatencyRecord {
    std::string mode;
    uint32_t topic_count = 0;
    uint64_t sample_count = 0;
    uint64_t single_message_rtt_us = 0;
    uint64_t p50_rtt_us = 0;
    uint64_t p95_rtt_us = 0;
    uint64_t p99_rtt_us = 0;
    uint64_t max_rtt_us = 0;
    uint64_t elapsed_us = 0;
    double messages_per_second = 0;
};

uint64_t NowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            Clock::now().time_since_epoch())
            .count());
}

std::optional<std::string_view> Flag(int argc, char** argv,
                                     std::string_view name) {
    const std::string prefix = "--" + std::string(name) + "=";
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (argument.starts_with(prefix)) return argument.substr(prefix.size());
    }
    return std::nullopt;
}

template <typename Integer>
bool ParseInteger(std::string_view raw, Integer* output) {
    const auto parsed =
        std::from_chars(raw.data(), raw.data() + raw.size(), *output, 10);
    return parsed.ec == std::errc{} && parsed.ptr == raw.data() + raw.size();
}

bool ParseOptions(int argc, char** argv, Options* options, std::string* error) {
    const auto role = Flag(argc, argv, "role");
    const auto address = Flag(argc, argv, "address");
    const auto port = Flag(argc, argv, "port");
    const auto lanes = Flag(argc, argv, "tcp-lane-count");
    const auto messages = Flag(argc, argv, "messages-per-topic");
    const auto payload = Flag(argc, argv, "payload-bytes");
    const auto io_threads = Flag(argc, argv, "io-threads");
    const auto deadline = Flag(argc, argv, "deadline-seconds");
    const auto output = Flag(argc, argv, "output");
    if (!role || !address || !port || !lanes || !output) {
        *error = "required flags: role, address, port, tcp-lane-count, output";
        return false;
    }
    options->role = std::string(*role);
    options->address = std::string(*address);
    options->output = std::string(*output);
    if (!ParseInteger(*port, &options->port) ||
        !ParseInteger(*lanes, &options->lane_count) ||
        (messages && !ParseInteger(*messages, &options->messages_per_topic)) ||
        (payload && !ParseInteger(*payload, &options->payload_bytes)) ||
        (io_threads && !ParseInteger(*io_threads, &options->io_threads)) ||
        (deadline && !ParseInteger(*deadline, &options->deadline_seconds))) {
        *error = "numeric benchmark option is invalid";
        return false;
    }
    if ((options->role != "server" && options->role != "client") ||
        options->port < 1024 || options->lane_count == 0 ||
        options->lane_count > mino::bridge::kMaxBridgeLaneCount ||
        options->port >
            std::numeric_limits<uint16_t>::max() - options->lane_count + 1 ||
        options->messages_per_topic == 0 ||
        options->payload_bytes < kHeaderBytes ||
        options->payload_bytes > 64u * 1024u * 1024u ||
        options->io_threads == 0 || options->io_threads > 16 ||
        options->deadline_seconds == 0 || options->deadline_seconds > 3600) {
        *error = "benchmark option is outside the supported range";
        return false;
    }
    return true;
}

void PutU32(std::span<std::byte> bytes, size_t offset, uint32_t value) {
    for (size_t index = 0; index < 4; ++index) {
        bytes[offset + index] = static_cast<std::byte>(
            (value >> (8 * (3 - index))) & uint32_t{0xff});
    }
}

void PutU64(std::span<std::byte> bytes, size_t offset, uint64_t value) {
    for (size_t index = 0; index < 8; ++index) {
        bytes[offset + index] = static_cast<std::byte>(
            (value >> (8 * (7 - index))) & uint64_t{0xff});
    }
}

uint32_t GetU32(std::span<const std::byte> bytes, size_t offset) {
    uint32_t value = 0;
    for (size_t index = 0; index < 4; ++index) {
        value = (value << 8) | static_cast<uint32_t>(bytes[offset + index]);
    }
    return value;
}

uint64_t GetU64(std::span<const std::byte> bytes, size_t offset) {
    uint64_t value = 0;
    for (size_t index = 0; index < 8; ++index) {
        value = (value << 8) | static_cast<uint64_t>(bytes[offset + index]);
    }
    return value;
}

std::vector<std::byte> Encode(const Message& message, size_t payload_bytes) {
    std::vector<std::byte> bytes(payload_bytes);
    std::copy(kMagic.begin(), kMagic.end(), bytes.begin());
    PutU32(bytes, 8, message.kind);
    PutU32(bytes, 12, message.phase);
    PutU32(bytes, 16, message.topic);
    PutU32(bytes, 20, message.sequence);
    PutU64(bytes, 24, message.origin_ns);
    PutU64(bytes, 32, payload_bytes);
    const uint64_t seed = static_cast<uint64_t>(message.phase) * 31 +
                          static_cast<uint64_t>(message.topic) * 23 +
                          static_cast<uint64_t>(message.sequence) * 19;
    for (size_t index = kHeaderBytes; index < bytes.size(); ++index) {
        bytes[index] =
            static_cast<std::byte>((seed + index * 37) & uint64_t{0xff});
    }
    return bytes;
}

bool Decode(std::span<const std::byte> bytes, Message* message) {
    if (bytes.size() < kHeaderBytes ||
        !std::equal(kMagic.begin(), kMagic.end(), bytes.begin()) ||
        GetU64(bytes, 32) != bytes.size()) {
        return false;
    }
    message->kind = GetU32(bytes, 8);
    message->phase = GetU32(bytes, 12);
    message->topic = GetU32(bytes, 16);
    message->sequence = GetU32(bytes, 20);
    message->origin_ns = GetU64(bytes, 24);
    return true;
}

std::string Endpoint(const Options& options, uint16_t lane) {
    return "tcp://" + options.address + ":" +
           std::to_string(static_cast<uint32_t>(options.port) + lane);
}

bool SetSocketOptions(void* socket, std::string* error) {
    const int linger = 0;
    const int immediate = 1;
    const int hwm = 65'536;
    const int keepalive = 1;
    const int heartbeat_interval = 1000;
    const int heartbeat_timeout = 10'000;
    for (const auto [option, value] : {
             std::pair{ZMQ_LINGER, linger},
             std::pair{ZMQ_IMMEDIATE, immediate},
             std::pair{ZMQ_SNDHWM, hwm},
             std::pair{ZMQ_RCVHWM, hwm},
             std::pair{ZMQ_TCP_KEEPALIVE, keepalive},
             std::pair{ZMQ_HEARTBEAT_IVL, heartbeat_interval},
             std::pair{ZMQ_HEARTBEAT_TIMEOUT, heartbeat_timeout},
         }) {
        if (zmq_setsockopt(socket, option, &value, sizeof(value)) != 0) {
            *error = std::string("zmq_setsockopt failed: ") + zmq_strerror(errno);
            return false;
        }
    }
    return true;
}

void CloseSockets(std::vector<void*>* sockets, void* context) {
    for (void* socket : *sockets) {
        if (socket != nullptr) static_cast<void>(zmq_close(socket));
    }
    sockets->clear();
    if (context != nullptr) static_cast<void>(zmq_ctx_term(context));
}

bool Send(void* socket, const std::vector<std::byte>& bytes,
          std::string* error) {
    if (zmq_send(socket, bytes.data(), bytes.size(), 0) !=
        static_cast<int>(bytes.size())) {
        *error = std::string("zmq_send failed: ") + zmq_strerror(errno);
        return false;
    }
    return true;
}

bool Receive(void* socket, std::vector<std::byte>* bytes,
             size_t max_bytes, std::string* error) {
    bytes->resize(max_bytes);
    const int received = zmq_recv(socket, bytes->data(), bytes->size(), 0);
    if (received < 0) {
        *error = std::string("zmq_recv failed: ") + zmq_strerror(errno);
        return false;
    }
    if (static_cast<size_t>(received) > max_bytes) {
        *error = "ZeroMQ message exceeded configured receive bound";
        return false;
    }
    bytes->resize(static_cast<size_t>(received));
    return true;
}

uint16_t LaneFor(uint32_t topic, uint16_t lane_count) {
    return mino::bridge::BridgeLaneFor(
        mino::bridge::SourceIdentity{
            .node_id = 101,
            .publisher_id = 50'000 + topic,
            .publisher_epoch = 60'001,
        },
        lane_count);
}

uint64_t ToMicroseconds(uint64_t nanoseconds) {
    return std::max<uint64_t>(
        1, nanoseconds / 1000 + (nanoseconds % 1000 != 0 ? 1 : 0));
}

uint64_t Percentile(const std::vector<uint64_t>& sorted,
                    uint32_t percentile) {
    const size_t rank = (sorted.size() * percentile + 99) / 100;
    return sorted[rank - 1];
}

bool PollOne(std::vector<void*>& sockets, uint64_t deadline_ns,
             size_t* ready_lane, std::string* error) {
    std::vector<zmq_pollitem_t> items(sockets.size());
    for (size_t index = 0; index < sockets.size(); ++index) {
        items[index] = zmq_pollitem_t{
            .socket = sockets[index],
            .fd = 0,
            .events = ZMQ_POLLIN,
            .revents = 0,
        };
    }
    while (NowNs() < deadline_ns) {
        const int ready = zmq_poll(items.data(), items.size(), 100);
        if (ready < 0) {
            if (errno == EINTR) continue;
            *error = std::string("zmq_poll failed: ") + zmq_strerror(errno);
            return false;
        }
        if (ready == 0) continue;
        for (size_t index = 0; index < items.size(); ++index) {
            if ((items[index].revents & ZMQ_POLLIN) != 0) {
                *ready_lane = index;
                return true;
            }
        }
    }
    *error = "ZeroMQ benchmark deadline expired";
    return false;
}

bool RunPhase(const Options& options, std::vector<void*>& sockets,
              std::string_view mode, uint32_t phase, uint32_t topic_count,
              uint64_t deadline_ns, LatencyRecord* record,
              std::string* error) {
    const uint64_t expected_samples =
        static_cast<uint64_t>(topic_count) * options.messages_per_topic;
    std::vector<uint32_t> next_sequence(topic_count, 1);
    std::vector<uint32_t> completed(topic_count, 0);
    std::vector<uint64_t> samples;
    samples.reserve(expected_samples);
    size_t next_serial_topic = 0;
    const auto send_topic = [&](uint32_t topic) {
        const uint32_t sequence = next_sequence[topic]++;
        return Send(sockets[LaneFor(topic, options.lane_count)],
                    Encode(Message{
                               .kind = kData,
                               .phase = phase,
                               .topic = topic,
                               .sequence = sequence,
                               .origin_ns = NowNs(),
                           },
                           options.payload_bytes),
                    error);
    };

    const uint64_t started_ns = NowNs();
    if (mode == "serial") {
        if (!send_topic(0)) return false;
    } else {
        for (uint32_t topic = 0; topic < topic_count; ++topic) {
            if (!send_topic(topic)) return false;
        }
    }
    while (samples.size() < expected_samples) {
        size_t lane = 0;
        if (!PollOne(sockets, deadline_ns, &lane, error)) return false;
        std::vector<std::byte> bytes;
        if (!Receive(sockets[lane], &bytes, options.payload_bytes, error)) {
            return false;
        }
        Message message;
        const uint64_t completed_ns = NowNs();
        if (!Decode(bytes, &message) || message.kind != kData ||
            message.phase != phase || message.topic >= topic_count ||
            message.sequence != completed[message.topic] + 1 ||
            message.origin_ns == 0 || completed_ns <= message.origin_ns ||
            LaneFor(message.topic, options.lane_count) != lane) {
            *error = "ZeroMQ echo correlation is invalid";
            return false;
        }
        ++completed[message.topic];
        samples.push_back(completed_ns - message.origin_ns);
        if (mode == "serial") {
            if (samples.size() < expected_samples) {
                next_serial_topic = (next_serial_topic + 1) % topic_count;
                if (!send_topic(static_cast<uint32_t>(next_serial_topic))) {
                    return false;
                }
            }
        } else if (completed[message.topic] < options.messages_per_topic) {
            if (!send_topic(message.topic)) return false;
        }
    }
    const uint64_t elapsed_ns = NowNs() - started_ns;
    const uint64_t single_us = ToMicroseconds(samples.front());
    for (uint64_t& sample : samples) sample = ToMicroseconds(sample);
    std::sort(samples.begin(), samples.end());
    *record = LatencyRecord{
        .mode = std::string(mode),
        .topic_count = topic_count,
        .sample_count = expected_samples,
        .single_message_rtt_us = single_us,
        .p50_rtt_us = Percentile(samples, 50),
        .p95_rtt_us = Percentile(samples, 95),
        .p99_rtt_us = Percentile(samples, 99),
        .max_rtt_us = samples.back(),
        .elapsed_us = ToMicroseconds(elapsed_ns),
        .messages_per_second =
            elapsed_ns == 0
                ? 0
                : static_cast<double>(expected_samples) * 1'000'000'000.0 /
                      static_cast<double>(elapsed_ns),
    };
    return true;
}

bool WriteClientResult(const Options& options,
                       const std::vector<LatencyRecord>& records,
                       std::string_view outcome, std::string_view error) {
    std::ofstream output(options.output, std::ios::trunc);
    if (!output) return false;
    int major = 0;
    int minor = 0;
    int patch = 0;
    zmq_version(&major, &minor, &patch);
    output << "{\n"
           << "  \"schema_version\": 1,\n"
           << "  \"benchmark\": \"mino.zmq_direct_latency\",\n"
           << "  \"scope\": \"direct libzmq DEALER/ROUTER TCP echo; no Mino Bridge, schema, dedup, retransmit, or application ACK\",\n"
           << "  \"role\": \"client\",\n"
           << "  \"outcome\": \"" << outcome << "\",\n"
           << "  \"libzmq_version\": \"" << major << '.' << minor << '.'
           << patch << "\",\n"
           << "  \"tcp_lane_count\": " << options.lane_count << ",\n"
           << "  \"io_threads\": " << options.io_threads << ",\n"
           << "  \"payload_bytes\": " << options.payload_bytes << ",\n"
           << "  \"messages_per_topic\": " << options.messages_per_topic
           << ",\n"
           << "  \"latency_samples\": [\n";
    for (size_t index = 0; index < records.size(); ++index) {
        const LatencyRecord& record = records[index];
        output << "    {\"mode\": \"" << record.mode
               << "\", \"topic_count\": " << record.topic_count
               << ", \"sample_count\": " << record.sample_count
               << ", \"single_message_rtt_us\": "
               << record.single_message_rtt_us
               << ", \"p50_rtt_us\": " << record.p50_rtt_us
               << ", \"p95_rtt_us\": " << record.p95_rtt_us
               << ", \"p99_rtt_us\": " << record.p99_rtt_us
               << ", \"max_rtt_us\": " << record.max_rtt_us
               << ", \"elapsed_us\": " << record.elapsed_us
               << ", \"messages_per_second\": "
               << record.messages_per_second << "}"
               << (index + 1 == records.size() ? "\n" : ",\n");
    }
    output << "  ],\n"
           << "  \"error\": \"" << error << "\"\n"
           << "}\n";
    return static_cast<bool>(output);
}

bool WriteServerResult(const Options& options, uint64_t messages,
                       std::string_view outcome, std::string_view error) {
    std::ofstream output(options.output, std::ios::trunc);
    if (!output) return false;
    int major = 0;
    int minor = 0;
    int patch = 0;
    zmq_version(&major, &minor, &patch);
    output << "{\n"
           << "  \"schema_version\": 1,\n"
           << "  \"benchmark\": \"mino.zmq_direct_latency\",\n"
           << "  \"role\": \"server\",\n"
           << "  \"outcome\": \"" << outcome << "\",\n"
           << "  \"libzmq_version\": \"" << major << '.' << minor << '.'
           << patch << "\",\n"
           << "  \"tcp_lane_count\": " << options.lane_count << ",\n"
           << "  \"messages_echoed\": " << messages << ",\n"
           << "  \"error\": \"" << error << "\"\n"
           << "}\n";
    return static_cast<bool>(output);
}

int RunClient(const Options& options) {
    std::string error;
    void* context = zmq_ctx_new();
    if (context == nullptr ||
        zmq_ctx_set(context, ZMQ_IO_THREADS,
                    static_cast<int>(options.io_threads)) != 0) {
        std::cerr << "cannot create ZeroMQ context\n";
        return 1;
    }
    std::vector<void*> sockets;
    for (uint16_t lane = 0; lane < options.lane_count; ++lane) {
        void* socket = zmq_socket(context, ZMQ_DEALER);
        if (socket == nullptr || !SetSocketOptions(socket, &error)) {
            if (socket != nullptr) static_cast<void>(zmq_close(socket));
            CloseSockets(&sockets, context);
            WriteClientResult(options, {}, "failed", error);
            return 1;
        }
        const std::string identity = "mino-zmq-lane-" + std::to_string(lane);
        if (zmq_setsockopt(socket, ZMQ_ROUTING_ID, identity.data(),
                           identity.size()) != 0 ||
            zmq_connect(socket, Endpoint(options, lane).c_str()) != 0) {
            error = std::string("ZeroMQ connect failed: ") +
                    zmq_strerror(errno);
            static_cast<void>(zmq_close(socket));
            CloseSockets(&sockets, context);
            WriteClientResult(options, {}, "failed", error);
            return 1;
        }
        sockets.push_back(socket);
    }
    const uint64_t deadline_ns =
        NowNs() + static_cast<uint64_t>(options.deadline_seconds) *
                      1'000'000'000ull;
    for (uint16_t lane = 0; lane < options.lane_count; ++lane) {
        if (!Send(sockets[lane],
                  Encode(Message{.kind = kHello, .topic = lane},
                         options.payload_bytes),
                  &error)) {
            CloseSockets(&sockets, context);
            WriteClientResult(options, {}, "failed", error);
            return 1;
        }
    }
    size_t hellos = 0;
    while (hellos < options.lane_count) {
        size_t lane = 0;
        std::vector<std::byte> bytes;
        Message message;
        if (!PollOne(sockets, deadline_ns, &lane, &error) ||
            !Receive(sockets[lane], &bytes, options.payload_bytes, &error) ||
            !Decode(bytes, &message) || message.kind != kHello) {
            if (error.empty()) error = "ZeroMQ hello echo is invalid";
            CloseSockets(&sockets, context);
            WriteClientResult(options, {}, "failed", error);
            return 1;
        }
        ++hellos;
    }

    std::vector<LatencyRecord> records;
    records.reserve(kTopicCounts.size() * 2);
    uint32_t phase = 1;
    for (std::string_view mode : {std::string_view("serial"),
                                  std::string_view("per_topic_concurrent")}) {
        for (uint32_t topic_count : kTopicCounts) {
            LatencyRecord record;
            if (!RunPhase(options, sockets, mode, phase++, topic_count,
                          deadline_ns, &record, &error)) {
                CloseSockets(&sockets, context);
                WriteClientResult(options, records, "failed", error);
                return 1;
            }
            records.push_back(std::move(record));
        }
    }
    for (uint16_t lane = 0; lane < options.lane_count; ++lane) {
        if (!Send(sockets[lane],
                  Encode(Message{.kind = kStop, .topic = lane},
                         options.payload_bytes),
                  &error)) {
            CloseSockets(&sockets, context);
            WriteClientResult(options, records, "failed", error);
            return 1;
        }
    }
    size_t stops = 0;
    while (stops < options.lane_count) {
        size_t lane = 0;
        std::vector<std::byte> bytes;
        Message message;
        if (!PollOne(sockets, deadline_ns, &lane, &error) ||
            !Receive(sockets[lane], &bytes, options.payload_bytes, &error) ||
            !Decode(bytes, &message) || message.kind != kStop) {
            if (error.empty()) error = "ZeroMQ stop echo is invalid";
            CloseSockets(&sockets, context);
            WriteClientResult(options, records, "failed", error);
            return 1;
        }
        ++stops;
    }
    CloseSockets(&sockets, context);
    if (!WriteClientResult(options, records, "passed", "")) return 1;
    return 0;
}

int RunServer(const Options& options) {
    std::string error;
    void* context = zmq_ctx_new();
    if (context == nullptr ||
        zmq_ctx_set(context, ZMQ_IO_THREADS,
                    static_cast<int>(options.io_threads)) != 0) {
        std::cerr << "cannot create ZeroMQ context\n";
        return 1;
    }
    std::vector<void*> sockets;
    for (uint16_t lane = 0; lane < options.lane_count; ++lane) {
        void* socket = zmq_socket(context, ZMQ_ROUTER);
        if (socket == nullptr || !SetSocketOptions(socket, &error) ||
            zmq_bind(socket, Endpoint(options, lane).c_str()) != 0) {
            if (error.empty()) {
                error = std::string("ZeroMQ bind failed: ") +
                        zmq_strerror(errno);
            }
            if (socket != nullptr) static_cast<void>(zmq_close(socket));
            CloseSockets(&sockets, context);
            WriteServerResult(options, 0, "failed", error);
            return 1;
        }
        sockets.push_back(socket);
    }
    const uint64_t deadline_ns =
        NowNs() + static_cast<uint64_t>(options.deadline_seconds) *
                      1'000'000'000ull;
    uint64_t messages = 0;
    size_t stops = 0;
    while (stops < options.lane_count) {
        size_t lane = 0;
        if (!PollOne(sockets, deadline_ns, &lane, &error)) break;
        std::array<std::byte, 256> identity{};
        const int identity_bytes =
            zmq_recv(sockets[lane], identity.data(), identity.size(), 0);
        int more = 0;
        size_t more_size = sizeof(more);
        if (identity_bytes <= 0 ||
            zmq_getsockopt(sockets[lane], ZMQ_RCVMORE, &more, &more_size) != 0 ||
            more == 0) {
            error = "ZeroMQ ROUTER identity frame is invalid";
            break;
        }
        std::vector<std::byte> bytes;
        Message message;
        if (!Receive(sockets[lane], &bytes, options.payload_bytes, &error) ||
            !Decode(bytes, &message) ||
            zmq_send(sockets[lane], identity.data(), identity_bytes,
                     ZMQ_SNDMORE) != identity_bytes ||
            zmq_send(sockets[lane], bytes.data(), bytes.size(), 0) !=
                static_cast<int>(bytes.size())) {
            if (error.empty()) error = "ZeroMQ ROUTER echo failed";
            break;
        }
        if (message.kind == kData) {
            ++messages;
        } else if (message.kind == kStop) {
            ++stops;
        } else if (message.kind != kHello) {
            error = "ZeroMQ benchmark message kind is invalid";
            break;
        }
    }
    const bool passed = error.empty() && stops == options.lane_count;
    CloseSockets(&sockets, context);
    WriteServerResult(options, messages, passed ? "passed" : "failed", error);
    return passed ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
    Options options;
    std::string error;
    if (!ParseOptions(argc, argv, &options, &error)) {
        std::cerr << error << '\n';
        return 2;
    }
    return options.role == "server" ? RunServer(options) : RunClient(options);
}
