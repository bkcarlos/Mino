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

struct UdpDriverOptions {
    // Defaults to an MTU-friendly payload. This first version never performs
    // application-level fragmentation or reassembly.
    uint32_t max_datagram_bytes = 1200;
    size_t socket_receive_buffer_bytes = 4u * 1024u * 1024u;
    uint32_t io_poll_max_ms = 50;
};

Status ValidateUdpDriverOptions(const UdpDriverOptions& options);

struct UdpDriverStats {
    size_t connected_sockets = 0;
    size_t listeners = 0;
    size_t pending_completions = 0;
};

// Non-blocking POSIX UDP transport. Each Send is exactly one datagram and each
// Poll message is exactly one datagram; no length prefix or fragmentation is
// added. Kernel send success cannot prove remote acceptance, so this driver
// retires the operation with kDegraded at kLocalPublished.
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
