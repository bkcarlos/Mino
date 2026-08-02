// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/bridge/control_payload.h"

#include <limits>
#include <new>
#include <string_view>

#include "mino/common/status.h"

namespace mino::bridge {
namespace {

constexpr uint8_t kAckHighestPresent = 1u << 0;
constexpr uint16_t kHelloDedupStateLost = 1u << 0;

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status Corruption(std::string_view message) {
    return Status::Error(StatusCode::kCorruption, message);
}

Status Resource(std::string_view message) {
    return Status::Error(StatusCode::kResourceExhausted, message);
}

void WriteBe16(std::span<std::byte> bytes, size_t offset,
               uint16_t value) noexcept {
    bytes[offset] = static_cast<std::byte>((value >> 8) & 0xffu);
    bytes[offset + 1] = static_cast<std::byte>(value & 0xffu);
}

void WriteBe32(std::span<std::byte> bytes, size_t offset,
               uint32_t value) noexcept {
    for (size_t i = 0; i < 4; ++i) {
        bytes[offset + i] =
            static_cast<std::byte>((value >> (24 - 8 * i)) & 0xffu);
    }
}

void WriteBe64(std::span<std::byte> bytes, size_t offset,
               uint64_t value) noexcept {
    for (size_t i = 0; i < 8; ++i) {
        bytes[offset + i] =
            static_cast<std::byte>((value >> (56 - 8 * i)) & 0xffu);
    }
}

uint16_t ReadBe16(std::span<const std::byte> bytes, size_t offset) noexcept {
    return static_cast<uint16_t>(static_cast<uint16_t>(bytes[offset]) << 8) |
           static_cast<uint16_t>(bytes[offset + 1]);
}

uint32_t ReadBe32(std::span<const std::byte> bytes, size_t offset) noexcept {
    uint32_t value = 0;
    for (size_t i = 0; i < 4; ++i) {
        value = (value << 8) | static_cast<uint8_t>(bytes[offset + i]);
    }
    return value;
}

uint64_t ReadBe64(std::span<const std::byte> bytes, size_t offset) noexcept {
    uint64_t value = 0;
    for (size_t i = 0; i < 8; ++i) {
        value = (value << 8) | static_cast<uint8_t>(bytes[offset + i]);
    }
    return value;
}

void EncodeSource(const SourceIdentity& source, std::span<std::byte> output,
                  size_t offset) noexcept {
    WriteBe64(output, offset, source.node_id);
    WriteBe64(output, offset + 8, source.publisher_id);
    WriteBe64(output, offset + 16, source.publisher_epoch);
}

SourceIdentity DecodeSource(std::span<const std::byte> input,
                            size_t offset) noexcept {
    return SourceIdentity{
        .node_id = ReadBe64(input, offset),
        .publisher_id = ReadBe64(input, offset + 8),
        .publisher_epoch = ReadBe64(input, offset + 16),
    };
}

bool ValidDisposition(AckDisposition disposition) noexcept {
    switch (disposition) {
        case AckDisposition::kAccepted:
        case AckDisposition::kNackWithHighest:
            return true;
    }
    return false;
}

bool ValidDiscoveryIdentity(const SessionDiscovery& discovery) noexcept {
    return discovery.session_epoch != 0 && discovery.node_id.value != 0 &&
           !discovery.process_identity.IsZero() &&
           discovery.process_identity.node_id == discovery.node_id.value &&
           discovery.lease_epoch != 0 && discovery.node_config_version != 0;
}

bool HasDuplicateSources(const std::vector<SessionHelloSource>& sources) {
    for (size_t i = 0; i < sources.size(); ++i) {
        for (size_t j = i + 1; j < sources.size(); ++j) {
            if (sources[i].source == sources[j].source) return true;
        }
    }
    return false;
}

Result<size_t> HelloWireSize(size_t source_count,
                             const ControlPayloadLimits& limits,
                             bool decoding) {
    Status (*const bad_input)(std::string_view) =
        decoding ? Corruption : Invalid;
    if (source_count > limits.max_hello_sources) {
        return Resource("session hello source count exceeds limit");
    }
    if (source_count > std::numeric_limits<uint32_t>::max()) {
        return bad_input("session hello source count is not representable");
    }
    if (limits.max_hello_payload_bytes < kSessionHelloHeaderWireSize) {
        return Resource("session hello payload limit is too small");
    }
    const size_t available =
        limits.max_hello_payload_bytes - kSessionHelloHeaderWireSize;
    if (source_count > available / kSessionHelloEntryWireSize) {
        return Resource("session hello payload exceeds byte limit");
    }
    return kSessionHelloHeaderWireSize +
           source_count * kSessionHelloEntryWireSize;
}

}  // namespace

Result<std::vector<std::byte>> ControlPayloadCodec::EncodeAck(
    const AckPayload& payload) noexcept {
    try {
        if (!ValidDisposition(payload.disposition)) {
            return Invalid("ACK disposition is unknown");
        }
        if (payload.disposition == AckDisposition::kNackWithHighest &&
            !payload.highest_contiguous_sequence.has_value()) {
            return Invalid("NACK_WITH_HIGHEST requires highest sequence");
        }

        std::vector<std::byte> output(kAckPayloadWireSize);
        WriteBe16(output, 0, kControlPayloadVersion);
        output[2] = static_cast<std::byte>(payload.disposition);
        output[3] = payload.highest_contiguous_sequence.has_value()
                        ? static_cast<std::byte>(kAckHighestPresent)
                        : std::byte{0};
        WriteBe32(output, 4, 0);
        WriteBe64(output, 8, payload.sender_session_epoch);
        WriteBe64(output, 16, payload.receiver_session_epoch);
        EncodeSource(payload.source, output, 24);
        WriteBe64(output, 48, payload.observed_sequence);
        WriteBe64(output, 56,
                  payload.highest_contiguous_sequence.value_or(0));
        return output;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (const std::length_error&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

Result<AckPayload> ControlPayloadCodec::DecodeAck(
    std::span<const std::byte> payload) noexcept {
    try {
        if (payload.size() != kAckPayloadWireSize) {
            return Corruption("ACK payload has noncanonical length");
        }
        if (ReadBe16(payload, 0) != kControlPayloadVersion) {
            return Corruption("ACK payload version is unsupported");
        }
        const auto disposition =
            static_cast<AckDisposition>(static_cast<uint8_t>(payload[2]));
        if (!ValidDisposition(disposition)) {
            return Corruption("ACK disposition is unknown");
        }
        const uint8_t flags = static_cast<uint8_t>(payload[3]);
        if ((flags & ~kAckHighestPresent) != 0 || ReadBe32(payload, 4) != 0) {
            return Corruption("ACK payload has nonzero reserved bits");
        }
        const bool has_highest = (flags & kAckHighestPresent) != 0;
        const uint64_t highest = ReadBe64(payload, 56);
        if (!has_highest && highest != 0) {
            return Corruption("ACK absent highest field is not zero");
        }
        if (disposition == AckDisposition::kNackWithHighest && !has_highest) {
            return Corruption("NACK_WITH_HIGHEST omits highest sequence");
        }

        AckPayload result;
        result.disposition = disposition;
        result.sender_session_epoch = ReadBe64(payload, 8);
        result.receiver_session_epoch = ReadBe64(payload, 16);
        result.source = DecodeSource(payload, 24);
        result.observed_sequence = ReadBe64(payload, 48);
        if (has_highest) result.highest_contiguous_sequence = highest;
        return result;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

Result<std::vector<std::byte>> ControlPayloadCodec::EncodeSessionHello(
    const SessionHello& hello, const ControlPayloadLimits& limits) noexcept {
    try {
        auto size = HelloWireSize(hello.sources.size(), limits, false);
        if (!size.ok()) return size.status();
        if (hello.sender_session_epoch == 0 ||
            hello.receiver_session_epoch == 0) {
            return Invalid("session hello epochs must be nonzero");
        }
        if (HasDuplicateSources(hello.sources)) {
            return Invalid("session hello contains a duplicate source");
        }

        std::vector<std::byte> output(*size);
        WriteBe16(output, 0, kControlPayloadVersion);
        WriteBe16(output, 2,
                  hello.dedup_state_lost ? kHelloDedupStateLost : 0);
        WriteBe32(output, 4, static_cast<uint32_t>(hello.sources.size()));
        WriteBe64(output, 8, hello.sender_session_epoch);
        WriteBe64(output, 16, hello.receiver_session_epoch);
        size_t offset = kSessionHelloHeaderWireSize;
        for (const SessionHelloSource& source : hello.sources) {
            EncodeSource(source.source, output, offset);
            WriteBe64(output, offset + 24, source.last_accepted_sequence);
            offset += kSessionHelloEntryWireSize;
        }
        return output;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (const std::length_error&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

Result<SessionHello> ControlPayloadCodec::DecodeSessionHello(
    std::span<const std::byte> payload,
    const ControlPayloadLimits& limits) noexcept {
    try {
        if (payload.size() < kSessionHelloHeaderWireSize) {
            return Corruption("session hello header is truncated");
        }
        if (payload.size() > limits.max_hello_payload_bytes) {
            return Resource("session hello payload exceeds byte limit");
        }
        if (ReadBe16(payload, 0) != kControlPayloadVersion) {
            return Corruption("session hello version is unsupported");
        }
        const uint16_t flags = ReadBe16(payload, 2);
        if ((flags & ~kHelloDedupStateLost) != 0) {
            return Corruption("session hello has nonzero reserved flags");
        }
        const size_t count = ReadBe32(payload, 4);
        auto expected_size = HelloWireSize(count, limits, true);
        if (!expected_size.ok()) return expected_size.status();
        if (payload.size() != *expected_size) {
            return Corruption("session hello has noncanonical length");
        }

        SessionHello hello;
        hello.sender_session_epoch = ReadBe64(payload, 8);
        hello.receiver_session_epoch = ReadBe64(payload, 16);
        if (hello.sender_session_epoch == 0 ||
            hello.receiver_session_epoch == 0) {
            return Corruption("session hello epochs must be nonzero");
        }
        hello.dedup_state_lost = (flags & kHelloDedupStateLost) != 0;
        hello.sources.reserve(count);
        size_t offset = kSessionHelloHeaderWireSize;
        for (size_t i = 0; i < count; ++i) {
            hello.sources.push_back(SessionHelloSource{
                .source = DecodeSource(payload, offset),
                .last_accepted_sequence = ReadBe64(payload, offset + 24),
            });
            offset += kSessionHelloEntryWireSize;
        }
        if (HasDuplicateSources(hello.sources)) {
            return Corruption("session hello contains a duplicate source");
        }
        return hello;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (const std::length_error&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

Result<std::vector<std::byte>> ControlPayloadCodec::EncodeSessionDiscovery(
    const SessionDiscovery& discovery) noexcept {
    try {
        if (!ValidDiscoveryIdentity(discovery)) {
            return Invalid("session discovery identity is incomplete");
        }
        std::vector<std::byte> output(kSessionDiscoveryPayloadWireSize);
        WriteBe16(output, 0, kSessionDiscoveryPayloadVersion);
        WriteBe16(output, 2, 0);
        WriteBe32(output, 4, 0);
        WriteBe64(output, 8, discovery.session_epoch);
        WriteBe64(output, 16, discovery.node_id.value);
        WriteBe64(output, 24, discovery.process_identity.node_id);
        WriteBe64(output, 32, discovery.process_identity.process_id);
        WriteBe64(output, 40, discovery.process_identity.process_epoch);
        WriteBe64(output, 48, discovery.process_identity.start_time_ns);
        WriteBe64(output, 56, discovery.lease_epoch);
        WriteBe64(output, 64, discovery.node_config_version);
        return output;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (const std::length_error&) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

Result<SessionDiscovery> ControlPayloadCodec::DecodeSessionDiscovery(
    std::span<const std::byte> payload) noexcept {
    if (payload.size() != kSessionDiscoveryPayloadWireSize) {
        return Corruption("session discovery payload has noncanonical length");
    }
    if (ReadBe16(payload, 0) != kSessionDiscoveryPayloadVersion) {
        return Corruption("session discovery payload version is unsupported");
    }
    if (ReadBe16(payload, 2) != 0 || ReadBe32(payload, 4) != 0) {
        return Corruption("session discovery payload has nonzero reserved bits");
    }
    SessionDiscovery discovery;
    discovery.session_epoch = ReadBe64(payload, 8);
    discovery.node_id = NodeId{ReadBe64(payload, 16)};
    discovery.process_identity = ProcessIdentity{
        .node_id = ReadBe64(payload, 24),
        .process_id = ReadBe64(payload, 32),
        .process_epoch = ReadBe64(payload, 40),
        .start_time_ns = ReadBe64(payload, 48),
    };
    discovery.lease_epoch = ReadBe64(payload, 56);
    discovery.node_config_version = ReadBe64(payload, 64);
    if (!ValidDiscoveryIdentity(discovery)) {
        return Corruption("session discovery identity is incomplete");
    }
    return discovery;
}

}  // namespace mino::bridge
