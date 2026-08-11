// Copyright 2026 The Mino Authors

#include "mino/transport/fabric_driver.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "mino/bridge/wire_frame.h"

namespace mino::transport {
namespace {

class MockFabricProvider final : public platform::FabricDeviceProvider {
public:
    platform::FabricProviderCapabilities capabilities() const noexcept override {
        return {.provider_class = platform::FabricProviderClass::kMock,
                .kind = platform::FabricKind::kNtb,
                .device_present = true,
                .link_active = true,
                .cache_coherent = false,
                .cache_line_bytes = 64,
                .required_alignment = 8,
                .max_connections = 4,
                .max_listeners = 2,
                .max_windows_per_connection = 1,
                .max_window_bytes = storage_.size()};
    }
    std::string provenance() const override { return "testonly:mock-ntb-v1"; }
    std::string device_id() const override { return "mock-ntb0"; }
    Status Start(const platform::FabricProviderLimits&) override {
        started_ = true;
        return Status::Ok();
    }
    void RequestStop() noexcept override { stopping_ = true; }
    Status Shutdown() noexcept override {
        started_ = false;
        events_.clear();
        in_use_ = false;
        return Status::Ok();
    }
    Result<platform::FabricProviderConnection> Connect(
        const ConnectRequest& request) override {
        if (!started_) return Status::Error(StatusCode::kUnavailable);
        return Connection(request.remote_endpoint);
    }
    Result<platform::FabricProviderListener> Listen(
        const ListenRequest& request) override {
        return platform::FabricProviderListener{.id = 9,
                                                .local_endpoint =
                                                    request.local_endpoint};
    }
    Result<platform::FabricProviderConnection> Accept(
        platform::FabricProviderListenerId, uint32_t) override {
        auto endpoint = EndpointDescriptor::SharedFabric(7, 3);
        return endpoint.ok() ? Connection(*endpoint)
                             : Result<platform::FabricProviderConnection>(
                                   endpoint.status());
    }
    Result<platform::FabricTransmitWindow> AcquireTransmitWindow(
        platform::FabricProviderConnectionId connection_id,
        size_t minimum_bytes) override {
        if (connection_id != kProviderConnection || in_use_) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "mock fabric window busy");
        }
        if (minimum_bytes > storage_.size()) {
            return Status::Error(StatusCode::kResourceExhausted);
        }
        in_use_ = true;
        return platform::FabricTransmitWindow{
            .connection_id = connection_id,
            .window_id = kWindow,
            .window_generation = generation_,
            .session_epoch = session_epoch_,
            .consumer_sequence = consumer_sequence_,
            .bytes = storage_,
        };
    }
    Status AbortTransmitWindow(
        const platform::FabricTransmitWindow&) noexcept override {
        in_use_ = false;
        return Status::Ok();
    }
    Status MaintainCache(
        const platform::FabricCacheRequest& request) override {
        if (fail_next_cache_ ||
            (fail_cpu_cache_ && request.direction ==
                                    platform::FabricCacheDirection::kForCpuRead)) {
            fail_next_cache_ = false;
            fail_cpu_cache_ = false;
            return Status::Error(StatusCode::kUnavailable,
                                 "injected cache sync failure");
        }
        if (request.offset > storage_.size() ||
            request.bytes > storage_.size() - request.offset) {
            return Status::Error(StatusCode::kInvalidArgument);
        }
        return Status::Ok();
    }
    Status RingDoorbell(const platform::FabricDoorbell& doorbell) override {
        last_doorbell_ = doorbell;
        if (fail_next_doorbell_) {
            fail_next_doorbell_ = false;
            return Status::Error(StatusCode::kUnavailable,
                                 "injected doorbell failure");
        }
        if (doorbell.kind == platform::FabricDoorbellKind::kProducerCommit) {
            if (!drop_producer_doorbell_) QueueReceive(doorbell);
        } else {
            ++consumer_sequence_;
            in_use_ = false;
            events_.push_back(platform::FabricProviderEvent{
                .kind = platform::FabricProviderEventKind::kTransmitConsumed,
                .mailbox_protocol_version = doorbell.protocol_version,
                .mailbox_endian_marker = doorbell.endian_marker,
                .connection_id = doorbell.connection_id,
                .window_id = doorbell.window_id,
                .window_generation = doorbell.window_generation,
                .session_epoch = doorbell.session_epoch,
                .producer_sequence = doorbell.producer_sequence,
                .window = {},
            });
        }
        return Status::Ok();
    }
    Result<platform::FabricProviderPollResult> Poll(
        const platform::FabricProviderPollRequest& request) override {
        platform::FabricProviderPollResult result;
        uint32_t receives = 0;
        uint32_t controls = 0;
        for (auto iterator = events_.begin(); iterator != events_.end();) {
            const bool receive =
                iterator->kind ==
                platform::FabricProviderEventKind::kReceiveReady;
            if ((receive && receives >= request.max_receive_events) ||
                (!receive && controls >= request.max_control_events)) {
                ++iterator;
                continue;
            }
            if (receive) {
                ++receives;
            } else {
                ++controls;
            }
            result.events.push_back(*iterator);
            iterator = events_.erase(iterator);
        }
        return result;
    }
    Status ReleaseReceiveWindow(
        const platform::FabricProviderEvent&) noexcept override {
        return Status::Ok();
    }
    Status Close(platform::FabricProviderConnectionId) noexcept override {
        events_.clear();
        in_use_ = false;
        return Status::Ok();
    }

    void CorruptPayload() { storage_[kFabricWindowHeaderBytes] ^= std::byte{1}; }
    void ClearCommitMarker() {
        std::fill(storage_.begin() + kFabricWindowCommitMarkerOffset,
                  storage_.begin() + kFabricWindowCommitMarkerOffset + 8,
                  std::byte{0});
    }
    void FailNextCache() { fail_next_cache_ = true; }
    void FailCpuCache() { fail_cpu_cache_ = true; }
    void FailNextDoorbell() { fail_next_doorbell_ = true; }
    void DropProducerDoorbell() { drop_producer_doorbell_ = true; }
    void RecoverDoorbell() {
        drop_producer_doorbell_ = false;
        QueueReceive(last_doorbell_);
    }
    void InjectReset() {
        ++generation_;
        ++session_epoch_;
        events_.push_back(platform::FabricProviderEvent{
            .kind = platform::FabricProviderEventKind::kPeerReset,
            .connection_id = kProviderConnection,
            .window_generation = generation_,
            .session_epoch = session_epoch_,
            .window = {},
            .status = Status::Error(StatusCode::kUnavailable,
                                    "injected peer reset"),
        });
    }

private:
    static constexpr uint64_t kProviderConnection = 11;
    static constexpr uint64_t kWindow = 41;

    Result<platform::FabricProviderConnection> Connection(
        const EndpointDescriptor& peer) {
        auto local = EndpointDescriptor::SharedFabric(7, 4);
        if (!local.ok()) return local.status();
        return platform::FabricProviderConnection{
            .id = kProviderConnection,
            .local_endpoint = *local,
            .peer_endpoint = peer,
            .peer_node_id = NodeId{202},
            .peer_security_domain = SecurityDomainId{88},
            .peer_device_id = "peer-ntb0",
            .window_set_id = 19,
            .window_generation = generation_,
            .session_epoch = session_epoch_,
            .attestation_evidence = {std::byte{0xa5}, std::byte{0x5a}},
        };
    }

    void QueueReceive(const platform::FabricDoorbell& doorbell) {
        events_.push_back(platform::FabricProviderEvent{
            .kind = platform::FabricProviderEventKind::kReceiveReady,
            .mailbox_protocol_version = doorbell.protocol_version,
            .mailbox_endian_marker = doorbell.endian_marker,
            .connection_id = doorbell.connection_id,
            .window_id = doorbell.window_id,
            .window_generation = doorbell.window_generation,
            .session_epoch = doorbell.session_epoch,
            .producer_sequence = doorbell.producer_sequence,
            .window = storage_,
        });
    }

    alignas(64) std::array<std::byte, 4096> storage_{};
    std::deque<platform::FabricProviderEvent> events_;
    platform::FabricDoorbell last_doorbell_;
    uint64_t generation_ = 3;
    uint64_t session_epoch_ = 5;
    uint64_t consumer_sequence_ = 0;
    bool started_ = false;
    bool stopping_ = false;
    bool in_use_ = false;
    bool fail_next_cache_ = false;
    bool fail_cpu_cache_ = false;
    bool fail_next_doorbell_ = false;
    bool drop_producer_doorbell_ = false;
};

class BoundAttestor final : public FabricAttestationVerifier {
public:
    Result<security::AuthenticatedPeer> Verify(
        const FabricAttestation& attestation) const noexcept override {
        if (attestation.local_node_id != NodeId{101} ||
            attestation.local_security_domain != SecurityDomainId{77} ||
            attestation.peer_node_id != NodeId{202} ||
            attestation.peer_security_domain != SecurityDomainId{88} ||
            attestation.kind != platform::FabricKind::kNtb ||
            attestation.provider_provenance != "testonly:mock-ntb-v1" ||
            attestation.local_device_id != "mock-ntb0" ||
            attestation.peer_device_id != "peer-ntb0" ||
            attestation.window_set_id != 19 ||
            attestation.window_generation == 0 ||
            attestation.session_epoch == 0 || attestation.evidence.empty()) {
            return Status::Error(StatusCode::kPermissionDenied);
        }
        return security::AuthenticatedPeer{
            .node_id = attestation.peer_node_id,
            .security_domain = attestation.peer_security_domain,
            .credential_generation = attestation.window_generation,
        };
    }
};

std::vector<std::byte> Canonical(uint64_t sequence = 1) {
    bridge::WireFrame frame;
    frame.header.flags = bridge::FlagValue(bridge::FrameFlag::kPayloadCrcPresent);
    frame.header.topic_id = 1;
    frame.header.msg_type = 2;
    frame.header.connection_schema_ref = 1;
    frame.header.schema_version = 1;
    frame.header.layout_version = 1;
    frame.header.source_node_id = 101;
    frame.header.source_publisher_id = 9;
    frame.header.source_publisher_epoch = 7;
    frame.header.sequence_num = sequence;
    frame.payload = {std::byte{0x10}, std::byte{0x20}, std::byte{0x30}};
    auto encoded = bridge::WireFrameCodec::Encode(frame);
    EXPECT_TRUE(encoded.ok()) << encoded.status().ToString();
    return encoded.ok() ? *encoded : std::vector<std::byte>{};
}

struct Fixture {
    std::shared_ptr<MockFabricProvider> provider =
        std::make_shared<MockFabricProvider>();
    std::unique_ptr<FabricWindowDriver> driver;
    ConnectionInfo connection;

    void Open() {
        auto created = FabricWindowDriver::Create({
            .provider = provider,
            .attestation_verifier = std::make_shared<BoundAttestor>(),
            .local_node_id = NodeId{101},
            .local_security_domain = SecurityDomainId{77},
            .max_windows_per_connection = 1,
            .event_queue_depth = 8,
            .receive_queue_depth = 8,
            .completion_queue_depth = 8,
            .max_message_bytes = 1024,
            .max_queued_receive_bytes = 4096,
            .allow_mock_provider_for_testing = true,
        });
        ASSERT_TRUE(created.ok()) << created.status().ToString();
        driver = std::move(*created);
        ASSERT_TRUE(driver->Start({.max_connections = 2,
                                   .max_listeners = 1,
                                   .max_queued_sends = 8})
                        .ok());
        auto endpoint = EndpointDescriptor::SharedFabric(7, 3);
        ASSERT_TRUE(endpoint.ok());
        auto connected = driver->Connect({.remote_endpoint = *endpoint,
                                                  .local_bind = std::nullopt,
                                                  .timeout_ms = 0});
        ASSERT_TRUE(connected.ok()) << connected.status().ToString();
        connection = *connected;
    }
};

TEST(FabricDriverTest, LifecycleCanonicalRingAndBridgeAckAreSeparated) {
    Fixture fixture;
    fixture.Open();
    auto peer = fixture.driver->AuthenticatedPeer(fixture.connection.id);
    ASSERT_TRUE(peer.ok());
    EXPECT_EQ(peer->node_id, NodeId{202});

    const auto canonical = Canonical();
    auto sent = fixture.driver->Send({.connection_id = fixture.connection.id,
                                      .payload = canonical,
                                      .target_stage =
                                          DeliveryStage::kRemoteAccepted});
    ASSERT_TRUE(sent.ok()) << sent.status().ToString();
    EXPECT_EQ(fixture.driver->PollCompletions({.max_completions = 1})
                  .status()
                  .code(),
              StatusCode::kWouldBlock);
    auto received = fixture.driver->Poll({.max_messages = 1,
                                          .max_bytes = 2048,
                                          .connection_id = fixture.connection.id});
    ASSERT_TRUE(received.ok()) << received.status().ToString();
    EXPECT_EQ(received->messages.front().payload, canonical);
    ASSERT_TRUE(fixture.driver->ConfirmRemoteAccepted(sent->operation).ok());
    auto completions = fixture.driver->PollCompletions({.max_completions = 1});
    ASSERT_TRUE(completions.ok());
    EXPECT_EQ(completions->completions.front().reached_stage,
              DeliveryStage::kRemoteAccepted);
    EXPECT_TRUE(fixture.driver->Shutdown().ok());
}

TEST(FabricDriverTest, RejectsNonCanonicalNativeLookingBytes) {
    Fixture fixture;
    fixture.Open();
    const std::array<std::byte, 8> offset = {std::byte{1}};
    EXPECT_EQ(fixture.driver->SendUntracked({.connection_id = fixture.connection.id,
                                             .payload = offset})
                  .status()
                  .code(),
              StatusCode::kCorruption);
}

TEST(FabricDriverTest, DetectsPayloadCorruptionAndPartialCommit) {
    for (bool partial : {false, true}) {
        Fixture fixture;
        fixture.Open();
        const auto canonical = Canonical();
        ASSERT_TRUE(fixture.driver
                        ->SendUntracked({.connection_id = fixture.connection.id,
                                         .payload = canonical})
                        .ok());
        if (partial) {
            fixture.provider->ClearCommitMarker();
        } else {
            fixture.provider->CorruptPayload();
        }
        EXPECT_EQ(fixture.driver->Poll({.max_messages = 1, .max_bytes = 2048})
                      .status()
                      .code(),
                  StatusCode::kCorruption);
        EXPECT_EQ(fixture.driver->health(), HealthState::kDegraded);
    }
}

TEST(FabricDriverTest, DoorbellLossBackpressuresUntilRecovery) {
    Fixture fixture;
    fixture.Open();
    fixture.provider->DropProducerDoorbell();
    const auto canonical = Canonical();
    ASSERT_TRUE(fixture.driver
                    ->SendUntracked({.connection_id = fixture.connection.id,
                                     .payload = canonical})
                    .ok());
    EXPECT_EQ(fixture.driver
                  ->SendUntracked({.connection_id = fixture.connection.id,
                                   .payload = canonical})
                  .status()
                  .code(),
              StatusCode::kWouldBlock);
    fixture.provider->RecoverDoorbell();
    EXPECT_TRUE(fixture.driver->Poll({.max_messages = 1, .max_bytes = 2048}).ok());
}

TEST(FabricDriverTest, CacheAndDoorbellFailuresFailClosed) {
    const auto canonical = Canonical();
    {
        Fixture fixture;
        fixture.Open();
        fixture.provider->FailNextCache();
        EXPECT_EQ(fixture.driver
                      ->SendUntracked({.connection_id = fixture.connection.id,
                                       .payload = canonical})
                      .status()
                      .code(),
                  StatusCode::kUnavailable);
    }
    {
        Fixture fixture;
        fixture.Open();
        fixture.provider->FailNextDoorbell();
        EXPECT_EQ(fixture.driver
                      ->SendUntracked({.connection_id = fixture.connection.id,
                                       .payload = canonical})
                      .status()
                      .code(),
                  StatusCode::kUnavailable);
    }
    {
        Fixture fixture;
        fixture.Open();
        ASSERT_TRUE(fixture.driver
                        ->SendUntracked({.connection_id = fixture.connection.id,
                                         .payload = canonical})
                        .ok());
        fixture.provider->FailCpuCache();
        EXPECT_EQ(fixture.driver->Poll({.max_messages = 1, .max_bytes = 2048})
                      .status()
                      .code(),
                  StatusCode::kUnavailable);
        EXPECT_EQ(fixture.driver->health(), HealthState::kDegraded);
    }
}

TEST(FabricDriverTest, PeerResetFailsPendingAndInvalidatesOldConnection) {
    Fixture fixture;
    fixture.Open();
    fixture.provider->DropProducerDoorbell();
    const auto canonical = Canonical();
    auto sent = fixture.driver->Send({.connection_id = fixture.connection.id,
                                      .payload = canonical,
                                      .target_stage =
                                          DeliveryStage::kRemoteAccepted});
    ASSERT_TRUE(sent.ok());
    fixture.provider->InjectReset();
    auto completion = fixture.driver->PollCompletions({.max_completions = 1});
    ASSERT_TRUE(completion.ok()) << completion.status().ToString();
    EXPECT_EQ(completion->completions.front().status.code(),
              StatusCode::kUnavailable);
    EXPECT_EQ(fixture.driver
                  ->SendUntracked({.connection_id = fixture.connection.id,
                                   .payload = canonical})
                  .status()
                  .code(),
              StatusCode::kNotFound);
}

TEST(FabricDriverTest, ProductionRejectsMockProvider) {
    auto provider = std::make_shared<MockFabricProvider>();
    FabricDriverOptions options{
        .provider = provider,
        .attestation_verifier = std::make_shared<BoundAttestor>(),
        .local_node_id = NodeId{101},
        .local_security_domain = SecurityDomainId{77},
        .max_message_bytes = 1024,
        .max_queued_receive_bytes = 4096,
        .allow_mock_provider_for_testing = true,
    };
    EXPECT_EQ(ValidateFabricDriverOptions(options, true).code(),
              StatusCode::kPermissionDenied);
}

}  // namespace
}  // namespace mino::transport
