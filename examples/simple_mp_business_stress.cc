// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/pipeline_comparison/pipeline_common.h"
#include "benchmarks/pipeline_comparison/semantic_canonical_codec.h"
#include "mino/runtime/simple_node.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <optional>
#include <iostream>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

using mino::benchmarks::pipeline::InitializeSourceFrame;
using mino::benchmarks::pipeline::Profile;
using mino::benchmarks::pipeline::SemanticCanonicalCodec;
using mino::benchmarks::pipeline::SemanticFrame;
using mino::benchmarks::pipeline::ValidateSemanticFrame;
using mino::benchmarks::pipeline::kLargePayloadBytes;
using mino::benchmarks::pipeline::kMediumPayloadBytes;
using mino::benchmarks::pipeline::kSmallPayloadBytes;
using mino::benchmarks::pipeline::NowNs;

constexpr uint32_t kTopicSlots = 4;
constexpr uint32_t kDefaultMessages = 20000;
constexpr uint32_t kDefaultPayloadBytes = 256;
constexpr uint32_t kDefaultQueueDepth = 32;
constexpr uint32_t kHeaderAllowanceBytes = 2048;
constexpr uint64_t kMaxMessages = 1000000;
constexpr char kTopic[] = "camera";

struct Config {
    uint64_t messages = kDefaultMessages;
    uint32_t payload_bytes = kDefaultPayloadBytes;
    uint32_t queue_depth = kDefaultQueueDepth;
    std::string schema_descriptor;
};

void Usage() {
    std::cerr
        << "usage: simple_mp_business_stress pub|sub shm-name "
           "[--messages N] [--payload-bytes B] [--queue-depth D] "
           "--schema-descriptor PATH\n"
        << "  encodes SemanticFrame with CanonicalWireCodec\n";
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

std::optional<Profile> ProfileFromPayload(uint32_t payload_bytes) {
    if (payload_bytes == kSmallPayloadBytes) return Profile::kSmall;
    if (payload_bytes == kMediumPayloadBytes) return Profile::kMedium;
    if (payload_bytes == kLargePayloadBytes) return Profile::kLarge;
    return std::nullopt;
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
        if (flag == "--schema-descriptor") {
            config->schema_descriptor = argv[i + 1];
            ++i;
            continue;
        }
        uint64_t value = 0;
        if (!ParseU64(argv[i + 1], &value)) return false;
        ++i;
        if (flag == "--messages") {
            if (value == 0 || value > kMaxMessages) return false;
            config->messages = value;
        } else if (flag == "--payload-bytes") {
            if (ProfileFromPayload(static_cast<uint32_t>(value)) == std::nullopt) {
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
    return !config->schema_descriptor.empty();
}

uint32_t MaxEncodedBytes(uint32_t payload_bytes) {
    return payload_bytes + kHeaderAllowanceBytes;
}

uint64_t PercentileNs(std::vector<uint64_t>* latencies, int percent) {
    if (latencies == nullptr || latencies->empty()) return 0;
    std::sort(latencies->begin(), latencies->end());
    const size_t n = latencies->size();
    size_t index = (static_cast<size_t>(percent) * (n - 1)) / 100;
    if (index >= n) index = n - 1;
    return (*latencies)[index];
}

int RunPub(std::string_view name, const Config& config) {
    const auto profile = ProfileFromPayload(config.payload_bytes);
    if (!profile.has_value()) {
        std::cerr << "unsupported payload-bytes for business profile\n";
        return 1;
    }
    std::optional<SemanticCanonicalCodec> codec_holder;
    try {
        codec_holder = SemanticCanonicalCodec::FromDescriptorFile(
            config.schema_descriptor);
    } catch (const std::exception& ex) {
        std::cerr << "schema load failed: " << ex.what() << "\n";
        return 1;
    }
    SemanticCanonicalCodec& codec = *codec_holder;
    (void)mino::SimpleNode::Unlink(name);
    mino::SimpleNodeOptions options;
    options.topic_slots = kTopicSlots;
    options.queue_depth = config.queue_depth;
    options.max_payload_bytes = MaxEncodedBytes(config.payload_bytes);
    options.segment_bytes = 0;
    const auto required = mino::SimpleNode::RequiredBytes(options);
    if (!required.ok()) {
        std::cerr << "RequiredBytes failed: " << required.status().ToString()
                  << "\n";
        return 1;
    }
    auto created = mino::SimpleNode::Create(name, options);
    if (!created.ok()) {
        std::cerr << "Create failed: " << created.status().ToString() << "\n";
        return 1;
    }
    std::cerr << "segment " << created->size_bytes() << " bytes required "
              << *required << " max_payload " << options.max_payload_bytes
              << " at " << name << "\n";
    auto pub = created->Advertise(kTopic);
    if (!pub.ok()) {
        std::cerr << "Advertise failed: " << pub.status().ToString() << "\n";
        return 1;
    }
    std::vector<std::byte> encoded;
    for (uint64_t seq = 0; seq < config.messages; ++seq) {
        SemanticFrame frame = InitializeSourceFrame(seq, *profile, true);
        try {
            codec.Encode(frame, &encoded);
        } catch (const std::exception& ex) {
            std::cerr << "Encode failed at seq " << seq << ": " << ex.what()
                      << "\n";
            return 1;
        }
        if (encoded.size() > options.max_payload_bytes) {
            std::cerr << "encoded frame " << encoded.size()
                      << " exceeds max_payload_bytes "
                      << options.max_payload_bytes << "\n";
            return 1;
        }
        const mino::Status status = pub->Publish(
            std::span<const std::byte>{encoded.data(), encoded.size()},
            mino::Deadline::FromNow(std::chrono::seconds(30)));
        if (!status.ok()) {
            std::cerr << "Publish failed at seq " << seq << ": "
                      << status.ToString() << "\n";
            return 1;
        }
    }
    std::cerr << "published " << config.messages << " business frames\n";
    return 0;
}

int RunSub(std::string_view name, const Config& config) {
    const auto profile = ProfileFromPayload(config.payload_bytes);
    if (!profile.has_value()) {
        std::cerr << "unsupported payload-bytes for business profile\n";
        return 1;
    }
    std::optional<SemanticCanonicalCodec> codec_holder;
    try {
        codec_holder = SemanticCanonicalCodec::FromDescriptorFile(
            config.schema_descriptor);
    } catch (const std::exception& ex) {
        std::cerr << "schema load failed: " << ex.what() << "\n";
        return 1;
    }
    SemanticCanonicalCodec& codec = *codec_holder;
    mino::Result<mino::SimpleNode> opened = mino::Status::Error(
        mino::StatusCode::kNotFound, "not opened");
    const auto open_deadline =
        mino::Deadline::FromNow(std::chrono::seconds(20));
    while (!opened.ok()) {
        opened = mino::SimpleNode::Open(name);
        if (opened.ok()) break;
        if (open_deadline.expired()) {
            std::cerr << "Open failed: " << opened.status().ToString() << "\n";
            return 1;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    auto sub = opened->Subscribe(
        kTopic, mino::Deadline::FromNow(std::chrono::seconds(20)));
    if (!sub.ok()) {
        std::cerr << "Subscribe failed: " << sub.status().ToString() << "\n";
        return 1;
    }

    uint64_t received = 0;
    uint64_t lost = 0;
    uint64_t expected = 0;
    bool frames_ok = true;
    std::string frame_error;
    std::vector<uint64_t> latencies;
    latencies.reserve(static_cast<size_t>(
        std::min(config.messages, static_cast<uint64_t>(1 << 20))));

    const uint64_t loop_start_ns = NowNs();
    const auto overall = mino::Deadline::FromNow(std::chrono::seconds(90));
    while (received + lost < config.messages) {
        if (overall.expired()) {
            lost += config.messages - received - lost;
            break;
        }
        auto message =
            sub->Poll(mino::Deadline::FromNow(std::chrono::seconds(10)));
        if (!message.ok()) {
            if (message.status().code() == mino::StatusCode::kTimeout) {
                lost += config.messages - received - lost;
                break;
            }
            std::cerr << "Poll failed: " << message.status().ToString() << "\n";
            return 1;
        }
        SemanticFrame frame;
        try {
            codec.Decode(message->bytes(), &frame);
        } catch (const std::exception& ex) {
            std::cerr << "Decode failed: " << ex.what() << "\n";
            (void)std::move(*message).Release();
            return 1;
        }
        if (frame.sample_id < expected) {
            std::cerr << "out-of-order sample_id " << frame.sample_id
                      << " expected " << expected << "\n";
            (void)std::move(*message).Release();
            return 1;
        }
        if (frame.sample_id > expected) lost += frame.sample_id - expected;
        if (frame.payload.size() != config.payload_bytes) {
            frames_ok = false;
            frame_error = "payload size mismatch";
        } else if (!ValidateSemanticFrame(frame, &frame_error)) {
            frames_ok = false;
        }
        const uint64_t now_ns = NowNs();
        if (frame.origin_timestamp_ns != 0 &&
            now_ns >= frame.origin_timestamp_ns) {
            latencies.push_back(now_ns - frame.origin_timestamp_ns);
        }
        expected = frame.sample_id + 1;
        ++received;
        const mino::Status released = std::move(*message).Release();
        if (!released.ok()) {
            std::cerr << "Release failed: " << released.ToString() << "\n";
            return 1;
        }
    }
    const uint64_t loop_end_ns = NowNs();
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
    json << "{\"codec\":\"business\""
         << ",\"received\":" << received
         << ",\"lost\":" << lost
         << ",\"expected\":" << config.messages
         << ",\"payload_bytes\":" << config.payload_bytes
         << ",\"queue_depth\":" << config.queue_depth
         << ",\"size_bytes\":" << opened->size_bytes()
         << ",\"p50_ns\":" << p50
         << ",\"p95_ns\":" << p95
         << ",\"msgs_per_s\":" << msgs_per_s
         << "}\n";
    std::cout << json.str() << std::flush;
    if (!frames_ok) {
        std::cerr << "ValidateSemanticFrame failed: " << frame_error << "\n";
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
    try {
        if (mode == "pub") return RunPub(name, config);
        if (mode == "sub") return RunSub(name, config);
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << "\n";
        return 1;
    }
    Usage();
    return 2;
}
