// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/validation/validations/broadcast_ack.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>

#include <span>
#include <sstream>
#include <utility>
#include <vector>

#include "benchmarks/validation/common/aligned_memory.h"
#include "benchmarks/validation/common/runtime.h"
#include "benchmarks/validation/common/stats.h"
#include "mino/shm/channel/broadcast_channel.h"

namespace mino::benchmarks::validation {
namespace {

constexpr uint64_t kBroadcastCapacity = 256;



void FillBroadcastSlot(BroadcastChannel::Reservation& reservation,
                       uint64_t sequence) {
    reservation->msg_type = 0x563134u;
    reservation->schema_version = 0x00010000u;
    reservation->schema_short_id = 0x563134563134ULL;
    reservation->schema_layout_version = 1;
    reservation->timestamp_ns = sequence + 1;
    reservation->payload = ShmHandle{
        .offset = 4096 + sequence * 64,
        .generation = 1,
        .region_id = 1,
    };
    reservation->payload_len = 64;
    reservation->flags = 0;
}

void BroadcastRound(BroadcastChannel& channel,
                    std::span<const BroadcastChannel::SubscriberHandle> subscribers,
                    uint64_t tag, std::vector<uint64_t>* publish_samples,
                    std::vector<uint64_t>* ack_samples,
                    std::vector<uint64_t>* round_samples) {
    const auto round_begin = Clock::now();
    const auto publish_begin = Clock::now();
    auto reservation = Take(channel.Reserve(), "BroadcastChannel::Reserve");
    FillBroadcastSlot(reservation, tag);
    Require(std::move(reservation).Commit(), "BroadcastChannel::Commit");
    const auto publish_end = Clock::now();
    if (publish_samples != nullptr) {
        publish_samples->push_back(DurationNs(publish_begin, publish_end));
    }
    uint64_t sink = 0;
    for (const auto& subscriber : subscribers) {
        const auto ack_begin = Clock::now();
        auto borrow = Take(channel.Poll(subscriber), "BroadcastChannel::Poll");
        sink ^= borrow->sequence_num;
        Require(std::move(borrow).Ack(), "BroadcastChannel::Borrow::Ack");
        if (ack_samples != nullptr) {
            ack_samples->push_back(DurationNs(ack_begin, Clock::now()));
        }
    }
    const auto round_end = Clock::now();
    if (round_samples != nullptr) {
        round_samples->push_back(DurationNs(round_begin, round_end));
    }
    SinkXor(sink);
}

}  // namespace

std::string RunBroadcastAck(uint64_t iterations) {
    constexpr std::array<uint32_t, 5> kSubscriberCounts = {
        1, 2, 8, 16, BroadcastChannel::kMaxSubscribers};
    std::ostringstream output;
    output << "{\"status\":\"MEASURED\",\"subscriber_limit\":"
           << BroadcastChannel::kMaxSubscribers
           << ",\"channel_capacity\":" << kBroadcastCapacity
           << ",\"fixed_layout\":true,\"scenarios\":[";
    bool first = true;
    for (uint32_t subscriber_count : kSubscriberCounts) {
        const size_t bytes = static_cast<size_t>(
            BroadcastChannel::RequiredSize(kBroadcastCapacity));
        auto memory = AllocateAligned(bytes);
        auto channel = Take(BroadcastChannel::Init(memory.get(), kBroadcastCapacity),
                            "BroadcastChannel::Init");
        std::vector<BroadcastChannel::SubscriberHandle> subscribers;
        subscribers.reserve(subscriber_count);
        for (uint32_t id = 0; id < subscriber_count; ++id) {
            subscribers.push_back(Take(
                channel.RegisterSubscriber(SubscriberId{id}),
                "BroadcastChannel::RegisterSubscriber"));
        }
        const uint64_t warmup = std::max<uint64_t>(1, iterations / 10);
        for (uint64_t index = 0; index < warmup; ++index) {
            BroadcastRound(channel, subscribers, index, nullptr, nullptr, nullptr);
        }
        std::vector<uint64_t> publish_samples;
        std::vector<uint64_t> ack_samples;
        std::vector<uint64_t> round_samples;
        publish_samples.reserve(iterations);
        ack_samples.reserve(iterations * subscriber_count);
        round_samples.reserve(iterations);
        for (uint64_t index = 0; index < iterations; ++index) {
            BroadcastRound(channel, subscribers, warmup + index,
                           &publish_samples, &ack_samples, &round_samples);
        }
        if (!first) output << ',';
        first = false;
        output << "{\"subscribers\":" << subscriber_count
               << ",\"iterations\":" << iterations
               << ",\"warmup_iterations\":" << warmup
               << ",\"memory\":{\"required_bytes\":" << bytes
               << ",\"incremental_bytes_for_active_count\":0"
               << ",\"subscriber_slots_bytes\":"
               << BroadcastChannel::kMaxSubscribers *
                      sizeof(BroadcastChannel::SubscriberSlot)
               << ",\"era_sidecar_bytes_per_channel_slot\":"
               << sizeof(BroadcastChannel::BroadcastEraMeta)
               << ",\"legacy_ack_meta_bytes_per_channel_slot\":"
               << sizeof(BroadcastSlotMeta)
               << "},\"publish_commit_latency_ns\":";
        WriteDistribution(output, Summarize(std::move(publish_samples)));
        output << ",\"poll_ack_latency_ns\":";
        WriteDistribution(output, Summarize(std::move(ack_samples)));
        output << ",\"fanout_roundtrip_latency_ns\":";
        WriteDistribution(output, Summarize(std::move(round_samples)));
        output << '}';
    }
    output << "]}";
    return output.str();
}

}  // namespace mino::benchmarks::validation
