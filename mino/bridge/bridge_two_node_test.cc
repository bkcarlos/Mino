// Copyright 2026 The Mino Authors

#include "mino/bridge/bridge_pipeline.h"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

#include "mino/transport/tcp_driver.h"

namespace mino::bridge {
namespace {

using namespace std::chrono_literals;

class ScopedFd final {
public:
    explicit ScopedFd(int fd = -1) noexcept : fd_(fd) {}
    ~ScopedFd() {
        if (fd_ >= 0) (void)::close(fd_);
    }
    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;
    int get() const noexcept { return fd_; }
    int release() noexcept {
        const int fd = fd_;
        fd_ = -1;
        return fd;
    }

private:
    int fd_;
};

uint16_t FreePort() {
    ScopedFd fd(::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP));
    if (fd.get() < 0) return 0;
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(fd.get(), reinterpret_cast<sockaddr*>(&address),
               sizeof(address)) != 0) {
        return 0;
    }
    socklen_t size = sizeof(address);
    if (::getsockname(fd.get(), reinterpret_cast<sockaddr*>(&address),
                      &size) != 0) {
        return 0;
    }
    return ntohs(address.sin_port);
}

Result<transport::EndpointDescriptor> Loopback(uint16_t port) {
    const std::array<std::byte, 4> address = {
        std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}};
    return transport::EndpointDescriptor::Ipv4Tcp(address, port);
}

transport::TcpDriverOptions TcpOptions() {
    transport::TcpDriverOptions options;
    options.max_frame_body_bytes = 4096;
    options.max_total_send_buffer_bytes = 32 * 1024;
    options.max_connection_send_buffer_bytes = 16 * 1024;
    options.max_ready_receive_bytes = 32 * 1024;
    options.max_ready_receive_messages = 64;
    options.max_pending_accepts = 4;
    options.heartbeat_interval_ms = 50;
    options.idle_timeout_ms = 3000;
    options.partial_frame_timeout_ms = 1000;
    options.io_poll_max_ms = 5;
    return options;
}

transport::DriverConfig DriverConfig() {
    return transport::DriverConfig{
        .max_connections = 4,
        .max_listeners = 1,
        .max_queued_sends = 16,
    };
}

BridgePipelineOptions PipelineOptions(uint64_t local, uint64_t remote) {
    BridgePipelineOptions options;
    options.local_session_epoch = local;
    options.remote_session_epoch = remote;
    options.wire_limits.max_payload_length = 4096;
    options.wire_limits.max_buffered_bytes = 8192;
    return options;
}

WireFrame DataFrame() {
    WireFrame frame;
    frame.header.frame_type = FrameType::kData;
    frame.header.flags = FlagValue(FrameFlag::kPayloadCrcPresent);
    frame.header.topic_id = 7;
    frame.header.msg_type = 8;
    frame.header.connection_schema_ref = 9;
    frame.header.schema_version = 1;
    frame.header.layout_version = 1;
    frame.header.source_node_id = 101;
    frame.header.source_publisher_id = 102;
    frame.header.source_publisher_epoch = 103;
    frame.header.sequence_num = 1;
    frame.payload.assign(64, std::byte{0x5a});
    return frame;
}

class OneShotEgress final : public BridgeEgressPort {
public:
    Result<EncodedOutboundFrame> TryPeekAndEncode() override {
        if (frames.empty()) return Status::Error(StatusCode::kWouldBlock);
        return frames.front();
    }

    void CommitPolled() noexcept override { frames.pop_front(); }

    std::deque<EncodedOutboundFrame> frames;
};

class CountingIngress final : public BridgeIngressPort {
public:
    Status DecodeValidatePublish(const WireFrame& frame) override {
        if (frame.payload != std::vector<std::byte>(64, std::byte{0x5a})) {
            return Status::Error(StatusCode::kCorruption);
        }
        ++published;
        return Status::Ok();
    }

    size_t published = 0;
};

bool WriteByte(int fd, uint8_t value) noexcept {
    return ::write(fd, &value, sizeof(value)) == sizeof(value);
}

bool ReadByte(int fd, uint8_t* value) noexcept {
    return ::read(fd, value, sizeof(*value)) == sizeof(*value);
}

int RunChild(uint16_t port, int ready_fd, int result_fd) {
    auto endpoint = Loopback(port);
    if (!endpoint.ok()) return 10;
    auto created = transport::TcpDriver::Create(TcpOptions());
    if (!created.ok()) return 11;
    auto driver = std::shared_ptr<transport::TcpDriver>(std::move(*created));
    if (!driver->Start(DriverConfig()).ok()) return 12;
    auto listener = driver->Listen({.local_endpoint = *endpoint, .backlog = 2});
    if (!listener.ok()) return 13;
    if (!WriteByte(ready_fd, 1)) return 14;
    auto accepted = driver->Accept(
        {.listener_id = listener->id, .timeout_ms = 3000});
    if (!accepted.ok()) return 15;

    CountingIngress ingress;
    auto pipeline = BridgePipeline::Create(
        PipelineOptions(202, 101), driver, accepted->id, nullptr, &ingress);
    if (!pipeline.ok()) return 16;
    size_t post_publish_pumps = 0;
    for (size_t i = 0; i < 3000; ++i) {
        BridgePumpBudget budget;
        budget.now_ns = i * 1'000'000;
        auto pumped = (*pipeline)->Pump(budget);
        if (!pumped.ok()) return 17;
        if (ingress.published == 1 && ++post_publish_pumps >= 50) break;
        std::this_thread::sleep_for(1ms);
    }
    const bool published_once = ingress.published == 1;
    if (!WriteByte(result_fd, published_once ? 1 : 0)) return 18;
    if (!driver->Shutdown().ok()) return 19;
    return published_once ? 0 : 20;
}

TEST(BridgeTwoNodeTest, CrossProcessTcpPublishAndRemoteAck) {
    const uint16_t port = FreePort();
    ASSERT_NE(port, 0);
    int ready_pipe[2] = {-1, -1};
    int result_pipe[2] = {-1, -1};
    ASSERT_EQ(::pipe(ready_pipe), 0);
    ASSERT_EQ(::pipe(result_pipe), 0);
    ScopedFd parent_ready_read(ready_pipe[0]);
    ScopedFd child_ready_write(ready_pipe[1]);
    ScopedFd parent_result_read(result_pipe[0]);
    ScopedFd child_result_write(result_pipe[1]);

    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        (void)::close(parent_ready_read.release());
        (void)::close(parent_result_read.release());
        const int code = RunChild(port, child_ready_write.get(),
                                  child_result_write.get());
        ::_exit(code);
    }
    (void)::close(child_ready_write.release());
    (void)::close(child_result_write.release());

    uint8_t ready = 0;
    ASSERT_TRUE(ReadByte(parent_ready_read.get(), &ready));
    ASSERT_EQ(ready, 1);
    auto endpoint = Loopback(port);
    ASSERT_TRUE(endpoint.ok()) << endpoint.status().ToString();
    auto created = transport::TcpDriver::Create(TcpOptions());
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    auto driver = std::shared_ptr<transport::TcpDriver>(std::move(*created));
    ASSERT_TRUE(driver->Start(DriverConfig()).ok());
    auto connected = driver->Connect({
        .remote_endpoint = *endpoint,
        .local_bind = std::nullopt,
        .timeout_ms = 3000,
    });
    ASSERT_TRUE(connected.ok()) << connected.status().ToString();

    OneShotEgress egress;
    CountingIngress ingress;
    egress.frames.push_back(EncodedOutboundFrame{
        .frame = DataFrame(),
        .reliability = registry::Reliability::kReliableOrdered,
        .allow_drop = false,
        .schema_identity = std::nullopt,
        .descriptor_artifact = {},
    });
    auto pipeline = BridgePipeline::Create(
        PipelineOptions(101, 202), driver, connected->id, &egress, &ingress);
    ASSERT_TRUE(pipeline.ok()) << pipeline.status().ToString();
    bool acknowledged = false;
    for (size_t i = 0; i < 3000; ++i) {
        BridgePumpBudget budget;
        budget.now_ns = i * 1'000'000;
        auto pumped = (*pipeline)->Pump(budget);
        ASSERT_TRUE(pumped.ok()) << pumped.status().ToString();
        if ((*pipeline)->session_ready() &&
            (*pipeline)->retransmit_entries() == 0 &&
            egress.frames.empty()) {
            acknowledged = true;
            break;
        }
        std::this_thread::sleep_for(1ms);
    }

    uint8_t child_published = 0;
    EXPECT_TRUE(ReadByte(parent_result_read.get(), &child_published));
    int child_status = 0;
    ASSERT_EQ(::waitpid(child, &child_status, 0), child);
    EXPECT_TRUE(WIFEXITED(child_status));
    EXPECT_EQ(WEXITSTATUS(child_status), 0);
    EXPECT_EQ(child_published, 1);
    EXPECT_TRUE(acknowledged);
    EXPECT_TRUE(driver->Shutdown().ok());
}

}  // namespace
}  // namespace mino::bridge
