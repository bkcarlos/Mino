// Copyright 2026 The Mino Authors

#include "mino/transport/rdma_driver.h"

#include <algorithm>
#include <chrono>
#include <deque>
#include <limits>
#include <mutex>
#include <new>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

namespace mino::transport {
namespace {

Status Invalid(const char* message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}
Status Exhausted(const char* message) {
    return Status::Error(StatusCode::kResourceExhausted, message);
}
Status WouldBlock(const char* message) {
    return Status::Error(StatusCode::kWouldBlock, message);
}
Status Unavailable(const char* message) {
    return Status::Error(StatusCode::kUnavailable, message);
}

bool IsWaitStatus(const Status& status) noexcept {
    return status.code() == StatusCode::kWouldBlock ||
           status.code() == StatusCode::kTimeout;
}

uint32_t RemainingMilliseconds(
    std::chrono::steady_clock::time_point deadline) noexcept {
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) return 0;
    const auto remaining =
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
    return static_cast<uint32_t>(std::max<int64_t>(1, remaining.count()));
}

}  // namespace

Status ValidateRdmaDriverOptions(const RdmaDriverOptions& options,
                                 bool production) noexcept {
    if (options.provider == nullptr) {
        return Invalid("RDMA device provider is required");
    }
    const MemoryRegistrationProviderClass provider_class =
        options.provider->provider_class();
    if (provider_class != MemoryRegistrationProviderClass::kDevice &&
        !(provider_class == MemoryRegistrationProviderClass::kMock &&
          options.allow_mock_provider_for_testing && !production)) {
        return Status::Error(
            StatusCode::kPermissionDenied,
            "RDMA production requires a real device provider");
    }
    if (!options.provider->Supports(MemoryRegistrationKind::kRdma)) {
        return Status::Error(StatusCode::kUnsupported,
                             "provider does not support RDMA MR");
    }
    if (options.send_queue_depth == 0 || options.receive_queue_depth == 0 ||
        options.completion_queue_depth == 0 || options.max_message_bytes == 0 ||
        options.max_message_bytes > kMaxPayloadBytes ||
        options.max_queued_send_bytes < options.max_message_bytes ||
        options.max_queued_receive_bytes < options.max_message_bytes ||
        options.registration_quota_bytes < options.max_message_bytes ||
        options.registration_scope_id == 0 ||
        !options.registration_owner.valid()) {
        return Invalid("RDMA queue, payload, MR quota, scope, or owner is invalid");
    }
    if (options.authentication_mode ==
            RdmaAuthenticationMode::kControlledFabric &&
        options.controlled_fabric_verifier == nullptr) {
        return Status::Error(
            StatusCode::kPermissionDenied,
            "controlled RDMA fabric requires a peer identity verifier");
    }
    return Status::Ok();
}

class RdmaDriver::Impl {
public:
    explicit Impl(RdmaDriverOptions& options,
                  std::atomic<HealthState>& health) noexcept
        : options_(options), health_(health) {}

    Status Start(const DriverConfig& config) {
        std::lock_guard lock(mutex_);
        config_ = config;
        stopping_ = false;
        next_connection_id_ = 1;
        next_work_request_id_ = 1;
        const Status started = options_.provider->Start({
            .max_connections = config.max_connections,
            .max_listeners = config.max_listeners,
            .send_queue_depth = options_.send_queue_depth,
            .receive_queue_depth = options_.receive_queue_depth,
            .completion_queue_depth = options_.completion_queue_depth,
            .max_message_bytes = options_.max_message_bytes,
        });
        if (!started.ok()) return started;
        auto recovered = options_.provider->RecoverStale({
            .scope_id = options_.registration_scope_id,
            .current_process_id = options_.registration_owner.process_id,
            .current_process_epoch = options_.registration_owner.process_epoch,
        });
        if (!recovered.ok()) {
            static_cast<void>(options_.provider->Shutdown());
            return recovered.status();
        }
        stats_.stale_registrations_recovered =
            recovered->registrations_released;
        stats_.stale_registration_bytes_recovered = recovered->bytes_released;
        health_.store(HealthState::kHealthy, std::memory_order_release);
        return Status::Ok();
    }

    void RequestStop() noexcept {
        stopping_.store(true, std::memory_order_release);
        // Providers must wake Poll without invalidating QPs/CQs/MRs. Do not hold
        // mutex_: a blocked Poll may currently own it.
        options_.provider->RequestStop();
    }

    Status Shutdown() {
        std::lock_guard lock(mutex_);
        stopping_.store(true, std::memory_order_release);
        Status first = Status::Ok();
        // Close is the provider's synchronous DMA fence. Never deregister or
        // reclaim a retained send buffer before every owning QP is fenced.
        for (const auto& [id, connection] : connections_) {
            (void)id;
            const Status closed =
                options_.provider->Close(connection.provider_id);
            if (first.ok() && !closed.ok()) first = closed;
        }
        for (auto& [id, pending] : pending_) {
            (void)id;
            if (!pending.terminal) {
                pending.terminal = true;
                pending.status = Unavailable("RDMA driver shut down");
            }
            const Status released = ReleaseRegistration(pending);
            if (first.ok() && !released.ok()) first = released;
        }
        RetryOrphanCleanup(&first);
        const Status provider_stopped = options_.provider->Shutdown();
        if (first.ok() && !provider_stopped.ok()) first = provider_stopped;
        connections_.clear();
        provider_to_connection_.clear();
        listeners_.clear();
        receives_.clear();
        completions_.clear();
        order_.clear();
        pending_.clear();
        queued_receive_bytes_ = 0;
        queued_send_bytes_ = 0;
        health_.store(HealthState::kUnavailable, std::memory_order_release);
        return first;
    }

    Result<ConnectionInfo> Connect(const ConnectRequest& request) {
        std::lock_guard lock(mutex_);
        if (connections_.size() >= config_.max_connections) {
            return Exhausted("RDMA connection table is full");
        }
        MINO_ASSIGN_OR_RETURN(auto provider_connection,
                              options_.provider->Connect(request));
        auto identity = VerifyPeer(provider_connection);
        if (!identity.ok()) {
            static_cast<void>(
                options_.provider->Close(provider_connection.id));
            return identity.status();
        }
        return InsertConnection(std::move(provider_connection), *identity);
    }

    Result<ConnectionInfo> Listen(const ListenRequest& request) {
        std::lock_guard lock(mutex_);
        if (listeners_.size() >= config_.max_listeners) {
            return Exhausted("RDMA listener table is full");
        }
        for (const auto& [id, listener] : listeners_) {
            (void)id;
            if (listener.info.local_endpoint == request.local_endpoint) {
                return Status::Error(StatusCode::kAlreadyExists,
                                     "RDMA listener already exists");
            }
        }
        MINO_ASSIGN_OR_RETURN(auto provider_listener,
                              options_.provider->Listen(request));
        const ConnectionId id = AllocateConnectionId();
        ConnectionInfo info{.id = id,
                            .kind = TransportKind::kRdma,
                            .is_listener = true,
                            .local_endpoint = provider_listener.local_endpoint,
                            .peer_endpoint = std::nullopt};
        listeners_.emplace(id, Listener{.provider_id = provider_listener.id,
                                        .info = info});
        return info;
    }

    Result<ConnectionInfo> Accept(const AcceptRequest& request) {
        std::lock_guard lock(mutex_);
        const auto listener = listeners_.find(request.listener_id);
        if (listener == listeners_.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "RDMA listener is not open");
        }
        if (connections_.size() >= config_.max_connections) {
            return Exhausted("RDMA connection table is full");
        }
        MINO_ASSIGN_OR_RETURN(
            auto provider_connection,
            options_.provider->Accept(listener->second.provider_id,
                                      request.timeout_ms));
        auto identity = VerifyPeer(provider_connection);
        if (!identity.ok()) {
            static_cast<void>(
                options_.provider->Close(provider_connection.id));
            return identity.status();
        }
        return InsertConnection(std::move(provider_connection), *identity);
    }

    Result<SendResult> Send(const SendRequest& request,
                            SendOperation operation) {
        std::lock_guard lock(mutex_);
        MINO_RETURN_IF_ERROR(Progress(0));
        MINO_RETURN_IF_ERROR(ValidateConnected(request.connection_id));
        MINO_ASSIGN_OR_RETURN(
            platform::RdmaWorkRequestId wr,
            Post(request.connection_id, request.payload, operation));
        (void)wr;
        return SendResult{.operation = operation,
                          .admitted_bytes = request.payload.size()};
    }

    Result<size_t> SendUntracked(const UntrackedSendRequest& request) {
        std::lock_guard lock(mutex_);
        MINO_RETURN_IF_ERROR(Progress(0));
        MINO_RETURN_IF_ERROR(ValidateConnected(request.connection_id));
        MINO_ASSIGN_OR_RETURN(platform::RdmaWorkRequestId wr,
                              Post(request.connection_id, request.payload,
                                   std::nullopt));
        (void)wr;
        return request.payload.size();
    }

    Status ConfirmRemoteAccepted(SendOperation operation) {
        std::lock_guard lock(mutex_);
        MINO_RETURN_IF_ERROR(Progress(0));
        auto found = std::find_if(
            pending_.begin(), pending_.end(),
            [&operation](const auto& item) {
                return item.second.operation.has_value() &&
                       *item.second.operation == operation;
            });
        if (found == pending_.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "RDMA send operation is not outstanding");
        }
        found->second.remote_accepted = true;
        FlushOrdered(operation.connection_id);
        return Status::Ok();
    }

    Result<ReceiveResult> Poll(const ReceiveRequest& request) {
        std::unique_lock lock(mutex_);
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(request.timeout_ms);
        for (;;) {
            MINO_RETURN_IF_ERROR(Progress(0));
            ReceiveResult result;
            size_t bytes = 0;
            for (auto iterator = receives_.begin(); iterator != receives_.end() &&
                                                    result.messages.size() <
                                                        request.max_messages;) {
                if (request.connection_id != kInvalidConnectionId &&
                    iterator->connection_id != request.connection_id) {
                    ++iterator;
                    continue;
                }
                if (iterator->payload.size() > request.max_bytes - bytes) {
                    if (result.messages.empty()) {
                        return Exhausted(
                            "next RDMA message exceeds receive byte budget");
                    }
                    break;
                }
                bytes += iterator->payload.size();
                queued_receive_bytes_ -= iterator->payload.size();
                result.messages.push_back(std::move(*iterator));
                iterator = receives_.erase(iterator);
            }
            if (!result.messages.empty()) return result;
            if (request.timeout_ms == 0) {
                return WouldBlock("no RDMA message is ready");
            }
            const uint32_t remaining = RemainingMilliseconds(deadline);
            if (remaining == 0) {
                return Status::Error(StatusCode::kTimeout,
                                     "RDMA receive timed out");
            }
            MINO_RETURN_IF_ERROR(Progress(remaining));
        }
    }

    Result<CompletionPollResult> PollCompletions(
        const CompletionPollRequest& request) {
        std::unique_lock lock(mutex_);
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(request.timeout_ms);
        for (;;) {
            MINO_RETURN_IF_ERROR(Progress(0));
            CompletionPollResult result;
            for (auto iterator = completions_.begin();
                 iterator != completions_.end() &&
                 result.completions.size() < request.max_completions;) {
                if (request.connection_id != kInvalidConnectionId &&
                    iterator->operation.connection_id != request.connection_id) {
                    ++iterator;
                    continue;
                }
                result.completions.push_back(std::move(*iterator));
                iterator = completions_.erase(iterator);
            }
            if (!result.completions.empty()) return result;
            if (request.timeout_ms == 0) {
                return WouldBlock("no RDMA completion is ready");
            }
            const uint32_t remaining = RemainingMilliseconds(deadline);
            if (remaining == 0) {
                return Status::Error(StatusCode::kTimeout,
                                     "RDMA completion poll timed out");
            }
            MINO_RETURN_IF_ERROR(Progress(remaining));
        }
    }

    Result<security::AuthenticatedPeer> AuthenticatedPeer(
        ConnectionId connection_id) {
        std::lock_guard lock(mutex_);
        const auto connection = connections_.find(connection_id);
        if (connection == connections_.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "RDMA connection is not open");
        }
        return connection->second.peer;
    }

    Status Close(ConnectionId connection_id) {
        std::lock_guard lock(mutex_);
        const auto listener = listeners_.find(connection_id);
        if (listener != listeners_.end()) {
            const Status status =
                options_.provider->Close(listener->second.provider_id);
            listeners_.erase(listener);
            return status;
        }
        const auto connection = connections_.find(connection_id);
        if (connection == connections_.end()) return Status::Ok();
        const platform::RdmaProviderConnectionId provider_id =
            connection->second.provider_id;
        Status first = options_.provider->Close(provider_id);
        FailConnection(connection_id,
                       Unavailable("RDMA connection closed before completion"));
        provider_to_connection_.erase(provider_id);
        connections_.erase(connection);
        for (auto iterator = receives_.begin(); iterator != receives_.end();) {
            if (iterator->connection_id == connection_id) {
                queued_receive_bytes_ -= iterator->payload.size();
                iterator = receives_.erase(iterator);
            } else {
                ++iterator;
            }
        }
        return first;
    }

    RdmaDriverStats stats() const noexcept {
        std::lock_guard lock(mutex_);
        RdmaDriverStats result = stats_;
        result.active_connections = connections_.size();
        result.listeners = listeners_.size();
        result.outstanding_work_requests = pending_.size();
        result.queued_send_bytes = queued_send_bytes_;
        result.queued_receive_messages = receives_.size();
        result.queued_receive_bytes = queued_receive_bytes_;
        result.queued_completions = completions_.size();
        result.registered_bytes = registered_bytes_;
        return result;
    }

private:
    struct Connection {
        platform::RdmaProviderConnectionId provider_id = 0;
        ConnectionInfo info;
        security::AuthenticatedPeer peer;
    };
    struct Listener {
        platform::RdmaProviderConnectionId provider_id = 0;
        ConnectionInfo info;
    };
    struct Pending {
        ConnectionId connection_id = kInvalidConnectionId;
        platform::RdmaProviderConnectionId provider_connection_id = 0;
        std::optional<SendOperation> operation;
        std::vector<std::byte> payload;
        RegisteredMemory registration;
        size_t completed_bytes = 0;
        bool terminal = false;
        bool remote_accepted = false;
        bool registration_released = false;
        bool completion_emitted = false;
        Status status = Status::Ok();
    };
    struct Orphan {
        std::vector<std::byte> payload;
        RegisteredMemory registration;
    };

    Result<security::AuthenticatedPeer> VerifyPeer(
        const platform::RdmaProviderConnection& connection) const {
        if (options_.authentication_mode ==
            RdmaAuthenticationMode::kVerifiedPeer) {
            if (!connection.verified_peer.has_value() ||
                !connection.verified_peer->complete()) {
                return Status::Error(
                    StatusCode::kPermissionDenied,
                    "RDMA CM did not provide a verified peer identity");
            }
            return *connection.verified_peer;
        }
        auto verified = options_.controlled_fabric_verifier->Verify(
            connection.local_endpoint, connection.peer_endpoint,
            options_.provider->provenance());
        if (!verified.ok() || !verified->complete()) {
            return Status::Error(
                StatusCode::kPermissionDenied,
                "controlled RDMA fabric peer attestation failed");
        }
        return *verified;
    }

    Result<ConnectionInfo> InsertConnection(
        platform::RdmaProviderConnection provider_connection,
        security::AuthenticatedPeer peer) {
        if (provider_connection.id == 0 ||
            provider_to_connection_.contains(provider_connection.id)) {
            return Status::Error(StatusCode::kCorruption,
                                 "provider returned duplicate RDMA connection");
        }
        const ConnectionId id = AllocateConnectionId();
        ConnectionInfo info{.id = id,
                            .kind = TransportKind::kRdma,
                            .is_listener = false,
                            .local_endpoint = provider_connection.local_endpoint,
                            .peer_endpoint = provider_connection.peer_endpoint};
        provider_to_connection_.emplace(provider_connection.id, id);
        connections_.emplace(
            id, Connection{.provider_id = provider_connection.id,
                           .info = info,
                           .peer = std::move(peer)});
        return info;
    }

    ConnectionId AllocateConnectionId() noexcept {
        while (next_connection_id_ == kInvalidConnectionId ||
               connections_.contains(next_connection_id_) ||
               listeners_.contains(next_connection_id_)) {
            ++next_connection_id_;
        }
        return next_connection_id_++;
    }

    platform::RdmaWorkRequestId AllocateWorkRequestId() noexcept {
        while (next_work_request_id_ == 0 ||
               pending_.contains(next_work_request_id_)) {
            ++next_work_request_id_;
        }
        return next_work_request_id_++;
    }

    Status ValidateConnected(ConnectionId id) const {
        return connections_.contains(id)
                   ? Status::Ok()
                   : Status::Error(StatusCode::kNotFound,
                                   "RDMA connection is not open");
    }

    Result<platform::RdmaWorkRequestId> Post(
        ConnectionId connection_id, std::span<const std::byte> payload,
        std::optional<SendOperation> operation) {
        const auto connection = connections_.find(connection_id);
        if (pending_.size() >= options_.send_queue_depth ||
            queued_send_bytes_ > options_.max_queued_send_bytes - payload.size()) {
            return WouldBlock("RDMA send queue is full");
        }
        if (registered_bytes_ >
            options_.registration_quota_bytes - payload.size()) {
            return Exhausted("RDMA registration quota is exhausted");
        }
        const platform::RdmaWorkRequestId wr = AllocateWorkRequestId();
        Pending pending;
        pending.connection_id = connection_id;
        pending.provider_connection_id = connection->second.provider_id;
        pending.operation = operation;
        try {
            pending.payload.assign(payload.begin(), payload.end());
        } catch (const std::bad_alloc&) {
            return Exhausted("RDMA send buffer allocation failed");
        }
        MemoryRegistrationOwner owner = options_.registration_owner;
        owner.lease_id = wr;
        auto registration = options_.provider->Register({
            .address = pending.payload.data(),
            .bytes = pending.payload.size(),
            .alignment = alignof(std::max_align_t),
            .scope_id = options_.registration_scope_id,
            .kind = MemoryRegistrationKind::kRdma,
            .owner = owner,
            .require_physical_contiguous = false,
        });
        if (!registration.ok()) {
            ++stats_.registration_failures;
            return registration.status();
        }
        if (registration->registration_id == 0 ||
            registration->bytes != pending.payload.size() ||
            registration->kind != MemoryRegistrationKind::kRdma ||
            registration->owner != owner) {
            const Status cleanup = options_.provider->Deregister(*registration);
            if (!cleanup.ok()) {
                orphans_.push_back(Orphan{.payload = std::move(pending.payload),
                                          .registration = *registration});
                registered_bytes_ += registration->bytes;
                return cleanup;
            }
            return Status::Error(StatusCode::kCorruption,
                                 "RDMA provider returned invalid MR facts");
        }
        pending.registration = *registration;
        const auto [inserted, ok] = pending_.emplace(wr, std::move(pending));
        (void)ok;
        registered_bytes_ += inserted->second.registration.bytes;
        queued_send_bytes_ += inserted->second.payload.size();
        order_[connection_id].push_back(wr);
        const Status posted = options_.provider->PostSend({
            .work_request_id = wr,
            .connection_id = connection->second.provider_id,
            .canonical_wire = inserted->second.payload,
            .registration = inserted->second.registration,
        });
        if (!posted.ok()) {
            order_[connection_id].pop_back();
            const Status cleanup = ReleaseRegistration(inserted->second);
            if (!cleanup.ok()) {
                inserted->second.terminal = true;
                inserted->second.completion_emitted = true;
                inserted->second.status = cleanup;
            } else {
                queued_send_bytes_ -= inserted->second.payload.size();
                pending_.erase(inserted);
            }
            return posted;
        }
        return wr;
    }

    Status Progress(uint32_t timeout_ms) {
        const uint32_t completion_capacity =
            completions_.size() >= options_.completion_queue_depth
                ? 0
                : options_.completion_queue_depth - completions_.size();
        const uint32_t receive_capacity =
            receives_.size() >= options_.receive_queue_depth
                ? 0
                : options_.receive_queue_depth - receives_.size();
        const size_t receive_bytes =
            queued_receive_bytes_ >= options_.max_queued_receive_bytes
                ? 0
                : options_.max_queued_receive_bytes - queued_receive_bytes_;
        auto polled = options_.provider->Poll({
            .max_completions = completion_capacity,
            .max_receives = receive_capacity,
            .max_receive_bytes = receive_bytes,
            .timeout_ms = timeout_ms,
        });
        if (!polled.ok()) {
            if (IsWaitStatus(polled.status())) return Status::Ok();
            if (polled.status().code() == StatusCode::kUnavailable) {
                health_.store(HealthState::kUnavailable,
                              std::memory_order_release);
            }
            return polled.status();
        }
        if (polled->completions.size() > completion_capacity ||
            polled->receives.size() > receive_capacity) {
            return Status::Error(StatusCode::kCorruption,
                                 "RDMA provider exceeded poll bounds");
        }
        for (const auto& completion : polled->completions) {
            HandleCompletion(completion);
        }
        for (auto& receive : polled->receives) {
            const auto connection =
                provider_to_connection_.find(receive.connection_id);
            if (connection == provider_to_connection_.end() ||
                receive.canonical_wire.empty() ||
                receive.canonical_wire.size() > options_.max_message_bytes ||
                receive.canonical_wire.size() >
                    options_.max_queued_receive_bytes - queued_receive_bytes_) {
                return Status::Error(StatusCode::kCorruption,
                                     "RDMA provider returned invalid receive");
            }
            queued_receive_bytes_ += receive.canonical_wire.size();
            receives_.push_back(ReceivedMessage{
                .connection_id = connection->second,
                .from = receive.peer_endpoint,
                .payload = std::move(receive.canonical_wire),
            });
        }
        return Status::Ok();
    }

    void HandleCompletion(
        const platform::RdmaProviderCompletion& completion) {
        const auto found = pending_.find(completion.work_request_id);
        if (found == pending_.end() ||
            found->second.provider_connection_id != completion.connection_id ||
            found->second.terminal) {
            ++stats_.cq_errors;
            health_.store(HealthState::kDegraded, std::memory_order_release);
            return;
        }
        Pending& pending = found->second;
        if (completion.bytes_completed >
            pending.payload.size() - pending.completed_bytes) {
            pending.status = Status::Error(
                StatusCode::kCorruption,
                "RDMA CQ completion exceeds posted message");
            pending.terminal = true;
            ++stats_.cq_errors;
        } else {
            pending.completed_bytes += completion.bytes_completed;
            if (!completion.terminal) ++stats_.partial_completions;
            if (!completion.status.ok() ||
                completion.kind !=
                    platform::RdmaProviderCompletionKind::kSendProgress) {
                pending.status = completion.status.ok()
                                     ? Unavailable("RDMA CQ terminal failure")
                                     : completion.status;
                pending.terminal = true;
                ++stats_.cq_errors;
            } else if (completion.terminal) {
                pending.terminal = true;
                if (pending.completed_bytes != pending.payload.size()) {
                    pending.status = Status::Error(
                        StatusCode::kCorruption,
                        "RDMA terminal CQ completion is partial");
                    ++stats_.cq_errors;
                }
            }
        }
        if (completion.kind ==
            platform::RdmaProviderCompletionKind::kDeviceReset) {
            ++stats_.device_resets;
            health_.store(HealthState::kUnavailable,
                          std::memory_order_release);
            FailAll(Unavailable("RDMA device reset"));
        } else if (completion.kind ==
                   platform::RdmaProviderCompletionKind::kPeerDeath) {
            ++stats_.peer_deaths;
            FailConnection(pending.connection_id,
                           Unavailable("RDMA peer died"));
        } else {
            if (pending.terminal) {
                const Status released = ReleaseRegistration(pending);
                if (!released.ok() && pending.status.ok()) {
                    pending.status = released;
                }
            }
            FlushOrdered(pending.connection_id);
        }
    }

    Status ReleaseRegistration(Pending& pending) {
        if (pending.registration_released) return Status::Ok();
        const Status status =
            options_.provider->Deregister(pending.registration);
        if (!status.ok()) {
            ++stats_.registration_failures;
            return status;
        }
        pending.registration_released = true;
        registered_bytes_ -= pending.registration.bytes;
        return Status::Ok();
    }

    void FlushOrdered(ConnectionId connection_id) {
        auto ordered = order_.find(connection_id);
        if (ordered == order_.end()) return;
        while (!ordered->second.empty()) {
            const platform::RdmaWorkRequestId wr = ordered->second.front();
            auto found = pending_.find(wr);
            if (found == pending_.end()) {
                ordered->second.pop_front();
                continue;
            }
            Pending& pending = found->second;
            if (!pending.terminal ||
                (pending.status.ok() && pending.operation.has_value() &&
                 !pending.remote_accepted)) {
                break;
            }
            if (pending.operation.has_value()) {
                if (completions_.size() >= options_.completion_queue_depth) break;
                completions_.push_back(DeliveryCompletion{
                    .operation = *pending.operation,
                    .reached_stage = pending.status.ok()
                                         ? DeliveryStage::kRemoteAccepted
                                         : DeliveryStage::kLocalPublished,
                    .status = pending.status,
                });
            }
            pending.completion_emitted = true;
            ordered->second.pop_front();
            if (pending.registration_released) {
                queued_send_bytes_ -= pending.payload.size();
                pending_.erase(found);
            }
        }
        if (ordered->second.empty()) order_.erase(ordered);
    }

    void FailConnection(ConnectionId connection_id, const Status& status) {
        auto ordered = order_.find(connection_id);
        if (ordered == order_.end()) return;
        std::vector<platform::RdmaWorkRequestId> work(
            ordered->second.begin(), ordered->second.end());
        for (platform::RdmaWorkRequestId wr : work) {
            auto found = pending_.find(wr);
            if (found == pending_.end()) continue;
            found->second.terminal = true;
            found->second.status = status;
            const Status released = ReleaseRegistration(found->second);
            if (!released.ok()) found->second.status = released;
        }
        FlushOrdered(connection_id);
    }

    void FailAll(const Status& status) {
        std::vector<ConnectionId> ids;
        ids.reserve(order_.size());
        for (const auto& [id, queue] : order_) {
            (void)queue;
            ids.push_back(id);
        }
        for (ConnectionId id : ids) FailConnection(id, status);
    }

    void RetryOrphanCleanup(Status* first) {
        for (auto iterator = orphans_.begin(); iterator != orphans_.end();) {
            const Status released =
                options_.provider->Deregister(iterator->registration);
            if (!released.ok()) {
                if (first->ok()) *first = released;
                ++iterator;
                continue;
            }
            registered_bytes_ -= iterator->registration.bytes;
            iterator = orphans_.erase(iterator);
        }
    }

    RdmaDriverOptions& options_;
    std::atomic<HealthState>& health_;
    mutable std::mutex mutex_;
    DriverConfig config_;
    std::atomic<bool> stopping_{false};
    ConnectionId next_connection_id_ = 1;
    platform::RdmaWorkRequestId next_work_request_id_ = 1;
    std::unordered_map<ConnectionId, Connection> connections_;
    std::unordered_map<platform::RdmaProviderConnectionId, ConnectionId>
        provider_to_connection_;
    std::unordered_map<ConnectionId, Listener> listeners_;
    std::unordered_map<platform::RdmaWorkRequestId, Pending> pending_;
    std::unordered_map<ConnectionId,
                       std::deque<platform::RdmaWorkRequestId>>
        order_;
    std::deque<ReceivedMessage> receives_;
    std::deque<DeliveryCompletion> completions_;
    std::vector<Orphan> orphans_;
    size_t queued_send_bytes_ = 0;
    size_t queued_receive_bytes_ = 0;
    uint64_t registered_bytes_ = 0;
    RdmaDriverStats stats_;
};

Result<std::unique_ptr<RdmaDriver>> RdmaDriver::Create(
    RdmaDriverOptions options) noexcept {
    const Status valid = ValidateRdmaDriverOptions(options);
    if (!valid.ok()) return valid;
    try {
        return std::unique_ptr<RdmaDriver>(new RdmaDriver(std::move(options)));
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "RDMA driver allocation failed");
    }
}

RdmaDriver::RdmaDriver(RdmaDriverOptions options) noexcept
    : options_(std::move(options)),
      impl_(std::make_unique<Impl>(options_, health_)) {}

RdmaDriver::~RdmaDriver() {
    if (state() != DriverState::kStopped) static_cast<void>(Shutdown());
}

TransportCapabilities RdmaDriver::capabilities() const noexcept {
    return {.kind = TransportKind::kRdma,
            .reliability = TransportReliability::kReliable,
            .max_frame_size = static_cast<uint32_t>(options_.max_message_bytes),
            .max_reassembly_bytes = options_.max_queued_receive_bytes,
            // The generic driver path owns registered staging but does not expose
            // an AcquireWindow/one-sided-write API, so it must not advertise the
            // optional zero-copy-window or remote-write capability bits.
            .features = Capability::kConnect | Capability::kListen |
                        Capability::kRemoteAcceptedConfirmation};
}

RdmaDriverStats RdmaDriver::stats() const noexcept { return impl_->stats(); }

MemoryRegistrationProvider& RdmaDriver::registration_provider() noexcept {
    return *options_.provider;
}

Status RdmaDriver::DoStart(const DriverConfig& config) {
    return impl_->Start(config);
}
void RdmaDriver::DoRequestStop() noexcept { impl_->RequestStop(); }
Status RdmaDriver::DoShutdown() { return impl_->Shutdown(); }
Result<ConnectionInfo> RdmaDriver::DoConnect(const ConnectRequest& request) {
    return impl_->Connect(request);
}
Result<ConnectionInfo> RdmaDriver::DoListen(const ListenRequest& request) {
    return impl_->Listen(request);
}
Result<ConnectionInfo> RdmaDriver::DoAccept(const AcceptRequest& request) {
    return impl_->Accept(request);
}
Result<SendResult> RdmaDriver::DoSend(const SendRequest& request,
                                      SendOperation operation) {
    return impl_->Send(request, operation);
}
Result<size_t> RdmaDriver::DoSendUntracked(
    const UntrackedSendRequest& request) {
    return impl_->SendUntracked(request);
}
Status RdmaDriver::DoConfirmRemoteAccepted(SendOperation operation) {
    return impl_->ConfirmRemoteAccepted(operation);
}
Result<ReceiveResult> RdmaDriver::DoPoll(const ReceiveRequest& request) {
    return impl_->Poll(request);
}
Result<CompletionPollResult> RdmaDriver::DoPollCompletions(
    const CompletionPollRequest& request) {
    return impl_->PollCompletions(request);
}
Result<security::AuthenticatedPeer> RdmaDriver::DoAuthenticatedPeer(
    ConnectionId connection_id) {
    return impl_->AuthenticatedPeer(connection_id);
}
Status RdmaDriver::DoClose(ConnectionId connection_id) {
    return impl_->Close(connection_id);
}

}  // namespace mino::transport
