// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_TRANSPORT_TCP_DRIVER_H_
#define MINO_TRANSPORT_TCP_DRIVER_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "mino/common/result.h"
#include "mino/transport/transport_driver.h"

namespace mino::transport {

struct TcpDriverOptions {
    uint32_t max_frame_body_bytes = 16u * 1024u * 1024u;
    size_t max_total_send_buffer_bytes = 64u * 1024u * 1024u;
    size_t max_connection_send_buffer_bytes = 16u * 1024u * 1024u;
    size_t max_ready_receive_bytes = 64u * 1024u * 1024u;
    uint32_t max_ready_receive_messages = 4096;
    uint32_t max_pending_accepts = 1024;
    uint32_t heartbeat_interval_ms = 1000;
    uint32_t idle_timeout_ms = 5000;
    uint32_t partial_frame_timeout_ms = 5000;
    uint32_t io_poll_max_ms = 50;
    // Untracked protocol-control frames have an independent bounded reserve so
    // ACK traffic can still make progress when the tracked data quota is full.
    size_t max_control_send_buffer_bytes = 16u * 1024u * 1024u + 4u;
    uint32_t max_control_send_messages = 1024;
};

Status ValidateTcpDriverOptions(const TcpDriverOptions& options);

struct TcpDriverStats {
    size_t active_connections = 0;
    size_t listeners = 0;
    size_t queued_send_bytes = 0;
    size_t ready_receive_bytes = 0;
    size_t ready_receive_messages = 0;
    size_t pending_accepts = 0;
    uint64_t successful_send_syscalls = 0;
    uint64_t gathered_send_syscalls = 0;
    uint64_t gathered_send_buffers = 0;
    uint64_t sent_bytes = 0;
};

// POSIX non-blocking TCP implementation. Send/Poll payloads are complete
// canonical Wire Frame bodies without the four-byte stream prefix; this driver
// adds/removes that prefix. TCP write completion is deliberately not reported
// as kRemoteAccepted. The bridge reliability layer supplies the message ACK protocol.
class TcpDriver final : public TransportDriver {
public:
    static Result<std::unique_ptr<TcpDriver>> Create(
        TcpDriverOptions options = {});

    ~TcpDriver() override;

    HealthState health() const noexcept override {
        return health_.load(std::memory_order_acquire);
    }
    TransportCapabilities capabilities() const noexcept override;
    TcpDriverStats stats() const noexcept;

protected:
    Status DoStart(const DriverConfig& config) override;
    void DoRequestStop() noexcept override;
    Status DoShutdown() override;
    Result<ConnectionInfo> DoConnect(const ConnectRequest& request) override;
    Result<ConnectionInfo> DoListen(const ListenRequest& request) override;
    Result<ConnectionInfo> DoAccept(const AcceptRequest& request) override;
    Result<SendResult> DoSend(const SendRequest& request,
                              SendOperation operation) override;
    Result<size_t> DoSendUntracked(
        const UntrackedSendRequest& request) override;
    Status DoConfirmRemoteAccepted(SendOperation operation) override;
    Result<ReceiveResult> DoPoll(const ReceiveRequest& request) override;
    Result<CompletionPollResult> DoPollCompletions(
        const CompletionPollRequest& request) override;
    Status DoClose(ConnectionId connection_id) override;

private:
    class Impl;
    explicit TcpDriver(TcpDriverOptions options);

    TcpDriverOptions options_;
    std::atomic<HealthState> health_{HealthState::kUnavailable};
    std::unique_ptr<Impl> impl_;
};

}  // namespace mino::transport

#endif  // MINO_TRANSPORT_TCP_DRIVER_H_
