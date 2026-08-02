// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_BRIDGE_CONTROL_PAYLOAD_H_
#define MINO_BRIDGE_CONTROL_PAYLOAD_H_

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include "mino/bridge/source_identity.h"
#include "mino/common/result.h"

namespace mino::bridge {

inline constexpr uint16_t kControlPayloadVersion = 1;
inline constexpr size_t kAckPayloadWireSize = 64;
inline constexpr size_t kSessionHelloHeaderWireSize = 24;
inline constexpr size_t kSessionHelloEntryWireSize = 32;

enum class AckDisposition : uint8_t {
    kAccepted = 1,
    kNackWithHighest = 2,
};

// Session epochs identify both ends of the connection. sender_session_epoch is
// the ACK sender's epoch; receiver_session_epoch is the epoch of the endpoint
// receiving this ACK. The fixed-size wire form uses a presence flag and a zero
// canonical placeholder for an absent highest_contiguous_sequence.
struct AckPayload {
    uint64_t sender_session_epoch = 0;
    uint64_t receiver_session_epoch = 0;
    SourceIdentity source;
    uint64_t observed_sequence = 0;
    std::optional<uint64_t> highest_contiguous_sequence;
    AckDisposition disposition = AckDisposition::kAccepted;

    bool operator==(const AckPayload&) const = default;
};

struct SessionHelloSource {
    SourceIdentity source;
    uint64_t last_accepted_sequence = 0;

    bool operator==(const SessionHelloSource&) const = default;
};

// A reconnect handshake advertises the sender's epoch, the peer epoch it
// believes it is answering, and the bounded per-source cumulative ACK map.
// dedup_state_lost explicitly signals the degraded receiver-restart path.
struct SessionHello {
    uint64_t sender_session_epoch = 0;
    uint64_t receiver_session_epoch = 0;
    bool dedup_state_lost = false;
    std::vector<SessionHelloSource> sources;

    bool operator==(const SessionHello&) const = default;
};

struct ControlPayloadLimits {
    size_t max_hello_sources = 4096;
    size_t max_hello_payload_bytes = 256u * 1024u;
};

// Payloads exclude WireFrameCodec's four-byte control opcode prefix.
class ControlPayloadCodec {
public:
    static Result<std::vector<std::byte>> EncodeAck(
        const AckPayload& payload) noexcept;
    static Result<AckPayload> DecodeAck(
        std::span<const std::byte> payload) noexcept;

    static Result<std::vector<std::byte>> EncodeSessionHello(
        const SessionHello& hello,
        const ControlPayloadLimits& limits = {}) noexcept;
    static Result<SessionHello> DecodeSessionHello(
        std::span<const std::byte> payload,
        const ControlPayloadLimits& limits = {}) noexcept;
};

}  // namespace mino::bridge

#endif  // MINO_BRIDGE_CONTROL_PAYLOAD_H_
