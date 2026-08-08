// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/validation/validations/buffer_capacity.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <sstream>
#include <stdexcept>

#include "benchmarks/validation/common/runtime.h"
#include "mino/storage/recorder_buffer_pool.h"

namespace mino::benchmarks::validation {

std::string RunBufferCapacity() {
    constexpr std::array<size_t, 7> kPayloads = {
        64, 4096, 4097, 65536, 65537, 1024 * 1024, 1024 * 1024 + 1};
    storage::RecorderBufferPoolOptions options;
    options.global_byte_limit = 32u * 1024u * 1024u;
    options.default_topic_byte_limit = options.global_byte_limit;
    options.queue_capacity = 32;
    auto pool = Take(storage::RecorderBufferPool::Create(options),
                     "RecorderBufferPool::Create");
    std::ostringstream output;
    output << "{\"status\":\"MEASURED_AND_MODELED\",\"capacity_charge_samples\":[";
    bool first = true;
    for (size_t payload : kPayloads) {
        storage::BufferReservationRequest request;
        request.topic_id = TopicId{1};
        request.payload_size = payload;
        request.full_policy = storage::BufferFullPolicy::kDropNewest;
        auto reserved = Take(pool->Reserve(request), "RecorderBufferPool::Reserve");
        if (!reserved.accepted()) {
            throw std::runtime_error(
                "capacity charge sample was unexpectedly dropped");
        }
        const size_t charged = reserved.reservation.capacity();
        static_cast<void>(reserved.reservation.Cancel());
        if (!first) output << ',';
        first = false;
        output << "{\"payload_bytes\":" << payload
               << ",\"charged_bytes\":" << charged << '}';
    }
    constexpr std::array<uint64_t, 3> kIngressMiB = {10, 100, 1000};
    constexpr std::array<uint64_t, 3> kPauseMs = {10, 100, 1000};
    output << "],\"disk_pause_capacity_model\":{\"kind\":\"deterministic_formula\",\"formula\":\"required_bytes=ingress_bytes_per_second*pause_seconds\",\"scenarios\":[";
    first = true;
    for (uint64_t ingress_mib : kIngressMiB) {
        for (uint64_t pause_ms : kPauseMs) {
            const uint64_t required =
                ingress_mib * 1024u * 1024u * pause_ms / 1000u;
            if (!first) output << ',';
            first = false;
            output << "{\"ingress_mib_per_second\":" << ingress_mib
                   << ",\"disk_pause_ms\":" << pause_ms
                   << ",\"minimum_payload_bytes\":" << required << '}';
        }
    }
    output << "],\"excludes\":[\"queue metadata\",\"fixed-class internal fragmentation beyond separately measured charged_bytes\",\"safety margin\"]}}";
    return output.str();
}

}  // namespace mino::benchmarks::validation
