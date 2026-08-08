// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_TRANSPORT_UDP_DRIVER_H_
#define MINO_TRANSPORT_UDP_DRIVER_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "mino/common/result.h"
#include "mino/transport/transport_driver.h"

namespace mino::transport {

inline constexpr uint32_t kUdpMaximumDatagramBytes = 65'507;
// Fragment envelope v1, all integers big-endian:
//   magic[0..4), version[4], flags[5], header_bytes[6..8),
//   message_id[8..16), fragment_id[16..20), fragment_count[20..24),
//   total_length[24..28), fragment_offset[28..32), crc32[32..36).
// Raw single datagrams are unmodified; the fragment magic is reserved.
inline constexpr uint32_t kUdpFragmentMagic = 0x4d554631;  // "MUF1"
inline constexpr uint8_t kUdpFragmentVersion = 1;
inline constexpr uint8_t kUdpFragmentFlag = 1;
inline constexpr uint16_t kUdpFragmentHeaderBytes = 36;
inline constexpr uint32_t kUdpMinimumFragmentDatagramBytes =
    kUdpFragmentHeaderBytes + 1;
inline constexpr uint32_t kUdpMaximumFragmentsPerMessage = 1u << 20;

struct UdpDriverOptions {
    // Messages at or below this bound remain compatible raw single datagrams.
    // Larger messages use the versioned fragmentation envelope.
    uint32_t max_datagram_bytes = 1200;
    uint32_t max_message_bytes = 16u * 1024u * 1024u;
    uint32_t max_fragments_per_message = 131'072;
    size_t max_reassembly_bytes_per_connection = 32u * 1024u * 1024u;
    uint32_t max_reassembly_messages_per_connection = 64;
    size_t max_reassembly_bytes_global = 64u * 1024u * 1024u;
    uint32_t max_reassembly_messages_global = 1024;
    uint32_t reassembly_timeout_ms = 5000;
    size_t socket_receive_buffer_bytes = 4u * 1024u * 1024u;
    uint32_t io_poll_max_ms = 50;
};

Status ValidateUdpDriverOptions(const UdpDriverOptions& options);

struct UdpDriverStats {
    size_t connected_sockets = 0;
    size_t listeners = 0;
    size_t pending_completions = 0;
    size_t reassembly_bytes = 0;
    size_t reassembly_messages = 0;
    uint64_t fragmented_messages_sent = 0;
    uint64_t fragments_sent = 0;
    uint64_t fragments_received = 0;
    uint64_t duplicate_fragments = 0;
    uint64_t reassembled_messages = 0;
    uint64_t reassembly_timeouts = 0;
    uint64_t rejected_fragments = 0;
};

// Non-blocking POSIX UDP transport. Small messages remain one raw datagram;
// larger messages use the bounded v1 fragmentation/reassembly envelope. Kernel
// send success cannot prove remote acceptance, so this driver retires a fully
// sent operation with kDegraded at kLocalPublished.
class UdpDriver final : public TransportDriver {
public:
    static Result<std::unique_ptr<UdpDriver>> Create(
        UdpDriverOptions options = {});
    ~UdpDriver() override;

    HealthState health() const noexcept override {
        return health_.load(std::memory_order_acquire);
    }
    TransportCapabilities capabilities() const noexcept override;
    UdpDriverStats stats() const noexcept;

protected:
    Status DoStart(const DriverConfig& config) override;
    void DoRequestStop() noexcept override;
    Status DoShutdown() override;
    Result<ConnectionInfo> DoConnect(const ConnectRequest& request) override;
    Result<ConnectionInfo> DoListen(const ListenRequest& request) override;
    Result<SendResult> DoSend(const SendRequest& request,
                              SendOperation operation) override;
    Result<size_t> DoSendUntracked(
        const UntrackedSendRequest& request) override;
    Result<ReceiveResult> DoPoll(const ReceiveRequest& request) override;
    Result<CompletionPollResult> DoPollCompletions(
        const CompletionPollRequest& request) override;
    Status DoClose(ConnectionId connection_id) override;

private:
    class Impl;
    explicit UdpDriver(UdpDriverOptions options);

    UdpDriverOptions options_;
    std::atomic<HealthState> health_{HealthState::kUnavailable};
    std::unique_ptr<Impl> impl_;
};

}  // namespace mino::transport

#endif  // MINO_TRANSPORT_UDP_DRIVER_H_
