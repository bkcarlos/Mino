// Copyright 2026 The Mino Authors

#include "mino/transport/rdma_driver.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace mino::transport {
namespace {

EndpointDescriptor RdmaEndpoint(uint8_t host, uint16_t port) {
    const std::array<std::byte, 4> address = {
        std::byte{10}, std::byte{0}, std::byte{0}, static_cast<std::byte>(host)};
    auto endpoint = EndpointDescriptor::Ip(
        TransportKind::kRdma, EndpointAddressFamily::kIpv4,
        NetworkProtocol::kRdmaCompatible, address, port);
    EXPECT_TRUE(endpoint.ok()) << endpoint.status().ToString();
    return endpoint.ok() ? *endpoint : EndpointDescriptor{};
}

security::AuthenticatedPeer Peer(uint64_t node = 2) {
    security::AuthenticatedPeer peer;
    peer.node_id = NodeId{node};
    peer.security_domain = SecurityDomainId{7};
    peer.certificate_sha256[0] = std::byte{0x42};
    peer.credential_generation = 3;
    return peer;
}

class LoopbackRdmaProvider final : public platform::RdmaDeviceProvider {
public:
    enum class Fault { kNone, kCqError, kDeviceReset, kPeerDeath };

    MemoryRegistrationProviderClass provider_class() const noexcept override {
        return MemoryRegistrationProviderClass::kMock;
    }
    std::string name() const override { return "test-loopback-rdma"; }
    std::string provenance() const override { return "test-only/in-process"; }
    bool Supports(MemoryRegistrationKind kind) const noexcept override {
        return kind == MemoryRegistrationKind::kRdma;
    }

    Result<RegisteredMemory> Register(
        const MemoryRegistrationRequest& request) override {
        if (fail_registration_) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "injected MR failure");
        }
        if (!started_ || request.address == nullptr || request.bytes == 0 ||
            !request.owner.valid()) {
            return Status::Error(StatusCode::kInvalidArgument);
        }
        RegisteredMemory registration{
            .registration_id = next_registration_++,
            .bytes = request.bytes,
            .device_key = 0xabc000 + next_registration_,
            .kind = request.kind,
            .owner = request.owner,
            .physically_contiguous = false,
        };
        registrations_.emplace(registration.registration_id, registration);
        return registration;
    }

    Status Deregister(const RegisteredMemory& registration) override {
        if (fail_deregister_) {
            return Status::Error(StatusCode::kUnavailable,
                                 "injected deregistration failure");
        }
        const auto found = registrations_.find(registration.registration_id);
        if (found == registrations_.end()) return Status::Ok();
        registrations_.erase(found);
        ++deregistrations_;
        return Status::Ok();
    }

    Result<MemoryRegistrationRecoveryResult> RecoverStale(
        const MemoryRegistrationRecoveryRequest& request) override {
        MemoryRegistrationRecoveryResult result;
        for (auto iterator = registrations_.begin();
             iterator != registrations_.end();) {
            const auto& owner = iterator->second.owner;
            if (owner.process_id != request.current_process_id ||
                owner.process_epoch != request.current_process_epoch) {
                ++result.registrations_released;
                result.bytes_released += iterator->second.bytes;
                iterator = registrations_.erase(iterator);
            } else {
                ++iterator;
            }
        }
        return result;
    }

    Status Start(const platform::RdmaProviderLimits& limits) override {
        limits_ = limits;
        started_ = true;
        stop_requested_ = false;
        return Status::Ok();
    }
    void RequestStop() noexcept override { stop_requested_ = true; }
    Status Shutdown() noexcept override {
        started_ = false;
        completions_.clear();
        receives_.clear();
        registrations_.clear();
        return Status::Ok();
    }

    Result<platform::RdmaProviderConnection> Connect(
        const ConnectRequest& request) override {
        if (!started_) return Status::Error(StatusCode::kUnavailable);
        const auto local = request.local_bind.value_or(RdmaEndpoint(1, 18000));
        platform::RdmaProviderConnection connection{
            .id = next_connection_++,
            .local_endpoint = local,
            .peer_endpoint = request.remote_endpoint,
            .verified_peer = expose_identity_ ? std::optional(Peer())
                                              : std::nullopt,
        };
        pending_accepts_.push_back(platform::RdmaProviderConnection{
            .id = next_connection_++,
            .local_endpoint = request.remote_endpoint,
            .peer_endpoint = local,
            .verified_peer = expose_identity_ ? std::optional(Peer(1))
                                              : std::nullopt,
        });
        return connection;
    }

    Result<platform::RdmaProviderListener> Listen(
        const ListenRequest& request) override {
        if (!started_) return Status::Error(StatusCode::kUnavailable);
        return platform::RdmaProviderListener{
            .id = next_listener_++, .local_endpoint = request.local_endpoint};
    }

    Result<platform::RdmaProviderConnection> Accept(
        platform::RdmaProviderConnectionId, uint32_t timeout_ms) override {
        if (pending_accepts_.empty()) {
            return Status::Error(timeout_ms == 0 ? StatusCode::kWouldBlock
                                                : StatusCode::kTimeout);
        }
        auto connection = std::move(pending_accepts_.front());
        pending_accepts_.pop_front();
        return connection;
    }

    Status PostSend(const platform::RdmaProviderSendRequest& request) override {
        if (!registrations_.contains(request.registration.registration_id)) {
            return Status::Error(StatusCode::kInvalidArgument);
        }
        receives_.push_back(platform::RdmaProviderReceive{
            .connection_id = request.connection_id,
            .peer_endpoint = RdmaEndpoint(2, 19000),
            .canonical_wire = std::vector<std::byte>(
                request.canonical_wire.begin(), request.canonical_wire.end()),
        });
        const size_t first = request.canonical_wire.size() / 2;
        completions_.push_back(platform::RdmaProviderCompletion{
            .work_request_id = request.work_request_id,
            .connection_id = request.connection_id,
            .bytes_completed = first,
            .terminal = false,
        });
        platform::RdmaProviderCompletion terminal{
            .work_request_id = request.work_request_id,
            .connection_id = request.connection_id,
            .bytes_completed = request.canonical_wire.size() - first,
            .terminal = true,
        };
        switch (next_fault_) {
            case Fault::kNone:
                break;
            case Fault::kCqError:
                terminal.kind =
                    platform::RdmaProviderCompletionKind::kCqError;
                terminal.status =
                    Status::Error(StatusCode::kUnavailable, "injected CQ error");
                break;
            case Fault::kDeviceReset:
                terminal.kind =
                    platform::RdmaProviderCompletionKind::kDeviceReset;
                terminal.status = Status::Error(StatusCode::kUnavailable,
                                                "injected device reset");
                break;
            case Fault::kPeerDeath:
                terminal.kind =
                    platform::RdmaProviderCompletionKind::kPeerDeath;
                terminal.status = Status::Error(StatusCode::kUnavailable,
                                                "injected peer death");
                break;
        }
        next_fault_ = Fault::kNone;
        completions_.push_back(std::move(terminal));
        return Status::Ok();
    }

    Result<platform::RdmaProviderPollResult> Poll(
        const platform::RdmaProviderPollRequest& request) override {
        if (stop_requested_) {
            return Status::Error(StatusCode::kUnavailable,
                                 "test provider stopped");
        }
        platform::RdmaProviderPollResult result;
        while (!completions_.empty() &&
               result.completions.size() < request.max_completions) {
            result.completions.push_back(std::move(completions_.front()));
            completions_.pop_front();
        }
        size_t bytes = 0;
        while (!receives_.empty() &&
               result.receives.size() < request.max_receives &&
               receives_.front().canonical_wire.size() <=
                   request.max_receive_bytes - bytes) {
            bytes += receives_.front().canonical_wire.size();
            result.receives.push_back(std::move(receives_.front()));
            receives_.pop_front();
        }
        if (result.completions.empty() && result.receives.empty()) {
            return Status::Error(request.timeout_ms == 0
                                     ? StatusCode::kWouldBlock
                                     : StatusCode::kTimeout);
        }
        return result;
    }

    Status Close(platform::RdmaProviderConnectionId) noexcept override {
        return Status::Ok();
    }

    void set_fault(Fault fault) { next_fault_ = fault; }
    void set_fail_registration(bool value) { fail_registration_ = value; }
    void set_fail_deregister(bool value) { fail_deregister_ = value; }
    void set_expose_identity(bool value) { expose_identity_ = value; }
    size_t active_registrations() const { return registrations_.size(); }
    uint64_t deregistrations() const { return deregistrations_; }

private:
    platform::RdmaProviderLimits limits_;
    bool started_ = false;
    bool stop_requested_ = false;
    bool fail_registration_ = false;
    bool fail_deregister_ = false;
    bool expose_identity_ = true;
    Fault next_fault_ = Fault::kNone;
    uint64_t next_registration_ = 1;
    uint64_t next_connection_ = 1;
    uint64_t next_listener_ = 1000;
    uint64_t deregistrations_ = 0;
    std::map<uint64_t, RegisteredMemory> registrations_;
    std::deque<platform::RdmaProviderConnection> pending_accepts_;
    std::deque<platform::RdmaProviderCompletion> completions_;
    std::deque<platform::RdmaProviderReceive> receives_;
};

class ControlledFabricVerifier final : public RdmaControlledFabricVerifier {
public:
    Result<security::AuthenticatedPeer> Verify(
        const EndpointDescriptor&, const EndpointDescriptor&,
        std::string_view provenance) const noexcept override {
        if (provenance != "test-only/in-process") {
            return Status::Error(StatusCode::kPermissionDenied);
        }
        return Peer();
    }
};

RdmaDriverOptions Options(
    const std::shared_ptr<LoopbackRdmaProvider>& provider) {
    return RdmaDriverOptions{
        .provider = provider,
        .send_queue_depth = 4,
        .receive_queue_depth = 4,
        .completion_queue_depth = 8,
        .max_message_bytes = 4096,
        .max_queued_send_bytes = 16384,
        .max_queued_receive_bytes = 16384,
        .registration_quota_bytes = 16384,
        .registration_scope_id = 0xd606,
        .registration_owner = {.process_id = 10,
                               .process_epoch = 20,
                               .lease_id = 1},
        .authentication_mode = RdmaAuthenticationMode::kVerifiedPeer,
        .controlled_fabric_verifier = nullptr,
        .allow_mock_provider_for_testing = true,
    };
}

std::unique_ptr<RdmaDriver> Started(
    const std::shared_ptr<LoopbackRdmaProvider>& provider,
    DriverConfig config = {}) {
    auto driver = RdmaDriver::Create(Options(provider));
    EXPECT_TRUE(driver.ok()) << driver.status().ToString();
    if (!driver.ok()) return nullptr;
    EXPECT_TRUE((*driver)->Start(config).ok());
    return std::move(*driver);
}

ConnectionInfo Connected(RdmaDriver& driver) {
    auto connection = driver.Connect({.remote_endpoint = RdmaEndpoint(2, 19000),
                                      .local_bind = std::nullopt,
                                      .timeout_ms = 0});
    EXPECT_TRUE(connection.ok()) << connection.status().ToString();
    return connection.ok() ? *connection : ConnectionInfo{};
}

TEST(RdmaDriverTest, ImplementsConnectListenAcceptAndVerifiedIdentity) {
    auto provider = std::make_shared<LoopbackRdmaProvider>();
    auto driver = Started(provider);
    ASSERT_NE(driver, nullptr);
    auto listener = driver->Listen({.local_endpoint = RdmaEndpoint(2, 19000),
                                    .backlog = 4});
    ASSERT_TRUE(listener.ok()) << listener.status().ToString();
    const ConnectionInfo client = Connected(*driver);
    auto accepted = driver->Accept({.listener_id = listener->id, .timeout_ms = 0});
    ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();
    EXPECT_EQ(accepted->kind, TransportKind::kRdma);
    auto peer = driver->AuthenticatedPeer(client.id);
    ASSERT_TRUE(peer.ok());
    EXPECT_EQ(*peer, Peer());
    EXPECT_TRUE(driver->Close(accepted->id).ok());
    EXPECT_TRUE(driver->Close(listener->id).ok());
    EXPECT_TRUE(driver->Shutdown().ok());
}

TEST(RdmaDriverTest, CanonicalMessageHasPartialCqAndAckGatedCompletion) {
    auto provider = std::make_shared<LoopbackRdmaProvider>();
    auto driver = Started(provider);
    ASSERT_NE(driver, nullptr);
    const ConnectionInfo connection = Connected(*driver);
    const std::array<std::byte, 6> canonical = {
        std::byte{'M'}, std::byte{'W'}, std::byte{1},
        std::byte{2}, std::byte{3}, std::byte{4}};
    auto sent = driver->Send({.connection_id = connection.id,
                              .payload = canonical,
                              .target_stage = DeliveryStage::kRemoteAccepted});
    ASSERT_TRUE(sent.ok()) << sent.status().ToString();
    EXPECT_EQ(provider->active_registrations(), 1u);

    auto received = driver->Poll({.max_messages = 1,
                                  .max_bytes = 64,
                                  .connection_id = connection.id});
    ASSERT_TRUE(received.ok()) << received.status().ToString();
    EXPECT_EQ(received->messages[0].payload,
              std::vector<std::byte>(canonical.begin(), canonical.end()));
    EXPECT_EQ(provider->active_registrations(), 0u);
    EXPECT_GE(driver->stats().partial_completions, 1u);

    auto completion = driver->PollCompletions({.max_completions = 1});
    ASSERT_FALSE(completion.ok());
    EXPECT_EQ(completion.status().code(), StatusCode::kWouldBlock);
    ASSERT_TRUE(driver->ConfirmRemoteAccepted(sent->operation).ok());
    completion = driver->PollCompletions({.max_completions = 1});
    ASSERT_TRUE(completion.ok()) << completion.status().ToString();
    EXPECT_TRUE(completion->completions[0].status.ok());
    EXPECT_EQ(completion->completions[0].reached_stage,
              DeliveryStage::kRemoteAccepted);
    EXPECT_TRUE(driver->Shutdown().ok());
}

TEST(RdmaDriverTest, BoundedQueueBackpressuresUntilOldestAckRetires) {
    auto provider = std::make_shared<LoopbackRdmaProvider>();
    RdmaDriverOptions options = Options(provider);
    options.send_queue_depth = 1;
    auto created = RdmaDriver::Create(std::move(options));
    ASSERT_TRUE(created.ok());
    auto driver = std::move(*created);
    ASSERT_TRUE(driver->Start({}).ok());
    const ConnectionInfo connection = Connected(*driver);
    const std::array<std::byte, 4> payload{};
    auto first = driver->Send({.connection_id = connection.id,
                               .payload = payload});
    ASSERT_TRUE(first.ok());
    auto second = driver->Send({.connection_id = connection.id,
                                .payload = payload});
    ASSERT_FALSE(second.ok());
    EXPECT_EQ(second.status().code(), StatusCode::kWouldBlock);
    ASSERT_TRUE(driver->ConfirmRemoteAccepted(first->operation).ok());
    ASSERT_TRUE(driver->PollCompletions({.max_completions = 1}).ok());
    second = driver->Send({.connection_id = connection.id,
                           .payload = payload});
    EXPECT_TRUE(second.ok()) << second.status().ToString();
    EXPECT_TRUE(driver->Shutdown().ok());
}

TEST(RdmaDriverTest, RegistrationFailureDoesNotPretendAdmission) {
    auto provider = std::make_shared<LoopbackRdmaProvider>();
    auto driver = Started(provider);
    ASSERT_NE(driver, nullptr);
    const ConnectionInfo connection = Connected(*driver);
    provider->set_fail_registration(true);
    const std::array<std::byte, 8> payload{};
    auto sent = driver->Send({.connection_id = connection.id,
                              .payload = payload});
    ASSERT_FALSE(sent.ok());
    EXPECT_EQ(sent.status().code(), StatusCode::kResourceExhausted);
    EXPECT_EQ(driver->stats().registration_failures, 1u);
    EXPECT_EQ(provider->active_registrations(), 0u);
    EXPECT_TRUE(driver->Shutdown().ok());
}

TEST(RdmaDriverTest, FaultInjectionReportsCqErrorResetAndPeerDeath) {
    for (const auto fault : {LoopbackRdmaProvider::Fault::kCqError,
                             LoopbackRdmaProvider::Fault::kDeviceReset,
                             LoopbackRdmaProvider::Fault::kPeerDeath}) {
        auto provider = std::make_shared<LoopbackRdmaProvider>();
        auto driver = Started(provider);
        ASSERT_NE(driver, nullptr);
        const ConnectionInfo connection = Connected(*driver);
        provider->set_fault(fault);
        const std::array<std::byte, 8> payload{};
        auto sent = driver->Send({.connection_id = connection.id,
                                  .payload = payload});
        ASSERT_TRUE(sent.ok());
        auto completion = driver->PollCompletions({.max_completions = 1});
        ASSERT_TRUE(completion.ok()) << completion.status().ToString();
        EXPECT_FALSE(completion->completions[0].status.ok());
        EXPECT_EQ(completion->completions[0].reached_stage,
                  DeliveryStage::kLocalPublished);
        EXPECT_EQ(provider->active_registrations(), 0u);
        if (fault == LoopbackRdmaProvider::Fault::kDeviceReset) {
            EXPECT_EQ(driver->health(), HealthState::kUnavailable);
            EXPECT_EQ(driver->stats().device_resets, 1u);
        } else if (fault == LoopbackRdmaProvider::Fault::kPeerDeath) {
            EXPECT_EQ(driver->stats().peer_deaths, 1u);
        } else {
            EXPECT_EQ(driver->stats().cq_errors, 1u);
        }
        EXPECT_TRUE(driver->Shutdown().ok());
    }
}

TEST(RdmaDriverTest, ControlledFabricRequiresAndUsesExplicitAttestor) {
    auto provider = std::make_shared<LoopbackRdmaProvider>();
    provider->set_expose_identity(false);
    RdmaDriverOptions options = Options(provider);
    options.authentication_mode = RdmaAuthenticationMode::kControlledFabric;
    EXPECT_EQ(ValidateRdmaDriverOptions(options).code(),
              StatusCode::kPermissionDenied);
    options.controlled_fabric_verifier =
        std::make_shared<ControlledFabricVerifier>();
    auto created = RdmaDriver::Create(std::move(options));
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    auto driver = std::move(*created);
    ASSERT_TRUE(driver->Start({}).ok());
    const ConnectionInfo connection = Connected(*driver);
    auto peer = driver->AuthenticatedPeer(connection.id);
    ASSERT_TRUE(peer.ok());
    EXPECT_EQ(*peer, Peer());
    EXPECT_TRUE(driver->Shutdown().ok());
}

TEST(RdmaDriverTest, MissingPeerIdentityFailsClosedAndClosesProviderConnection) {
    auto provider = std::make_shared<LoopbackRdmaProvider>();
    provider->set_expose_identity(false);
    auto driver = Started(provider);
    ASSERT_NE(driver, nullptr);
    auto connection = driver->Connect({
        .remote_endpoint = RdmaEndpoint(2, 19000),
        .local_bind = std::nullopt,
        .timeout_ms = 0,
    });
    ASSERT_FALSE(connection.ok());
    EXPECT_EQ(connection.status().code(), StatusCode::kPermissionDenied);
    EXPECT_TRUE(driver->Shutdown().ok());
}

TEST(RdmaDriverTest, ProductionValidationRejectsMockProvider) {
    auto provider = std::make_shared<LoopbackRdmaProvider>();
    EXPECT_TRUE(ValidateRdmaDriverOptions(Options(provider)).ok());
    EXPECT_EQ(ValidateRdmaDriverOptions(Options(provider), true).code(),
              StatusCode::kPermissionDenied);
}

}  // namespace
}  // namespace mino::transport
