// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_RUNTIME_DEPLOYMENT_SHARED_HOST_DOMAIN_H_
#define MINO_RUNTIME_DEPLOYMENT_SHARED_HOST_DOMAIN_H_

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "mino/common/ids.h"
#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/platform/process_identity.h"
#include "mino/runtime/deadline.h"
#include "mino/schema/descriptor.h"

namespace mino::deployment {

// Same-host multi-process topology domain backed by one POSIX SHM segment.
// Independent processes Create or Open the domain by name, Join as peers, and
// Advertise/Subscribe topics without a pre-baked peer or channel manifest.
//
// LocalBusDeployment remains the in-process Bus assembly (heap BroadcastChannel
// + Coordinator). SharedHostDomain is the production same-host dynamic path that
// replaces benchmark-static SHM manifests for discoverable local topologies.
// Schema and queue shape are immutable after first advertisement; mismatches
// fail closed. Dead peers and endpoint leases are reclaimable via Recover().
//
// Limitations (intentional for this MVP):
// - One publisher per topic (BroadcastChannel contract).
// - Canonical payload is copied into a fixed per-topic payload ring (no
//   CentralSlab / owned-graph Publisher<T> wiring).
// - Coordinator / RemoteBridge / cross-host discovery are out of scope.
// - Optional static LocalBusConfig::topics manifests remain valid for the
//   in-process LocalBusDeployment path.

inline constexpr uint32_t kSharedHostMaxPeerSlots = 64;
inline constexpr uint32_t kSharedHostMaxTopicSlots = 64;
inline constexpr uint32_t kSharedHostMaxQueueDepth = 1024;
inline constexpr uint32_t kSharedHostMaxPayloadBytes = 1u << 20;
inline constexpr uint32_t kSharedHostMaxSubscribers = 64;

struct SharedHostDomainOptions {
    uint32_t peer_slots = 8;
    uint32_t topic_slots = 8;
    uint32_t queue_depth = 64;  // power of two
    uint32_t max_subscribers = 16;
    uint32_t max_payload_bytes = 4096;
    uint64_t peer_lease_ns = 30ull * 1000 * 1000 * 1000;
};

struct SharedPeerInfo {
    NodeId node_id{};
    ProcessIdentity identity{};
    uint64_t last_heartbeat_ns = 0;
};

struct SharedTopicInfo {
    std::string name;
    schema::SchemaIdentity schema{0, {}, 0, 0};
    uint32_t capacity = 0;
    uint32_t max_subscribers = 0;
    uint32_t max_payload_bytes = 0;
    bool publisher_active = false;
};

class SharedHostPublisher {
public:
    SharedHostPublisher(SharedHostPublisher&& other) noexcept;
    SharedHostPublisher& operator=(SharedHostPublisher&& other) noexcept;
    ~SharedHostPublisher();

    SharedHostPublisher(const SharedHostPublisher&) = delete;
    SharedHostPublisher& operator=(const SharedHostPublisher&) = delete;

    Status Publish(std::span<const std::byte> payload);
    bool active() const noexcept;

private:
    friend class SharedHostDomain;
    struct Impl;
    explicit SharedHostPublisher(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

class SharedHostSubscriber {
public:
    SharedHostSubscriber(SharedHostSubscriber&& other) noexcept;
    SharedHostSubscriber& operator=(SharedHostSubscriber&& other) noexcept;
    ~SharedHostSubscriber();

    SharedHostSubscriber(const SharedHostSubscriber&) = delete;
    SharedHostSubscriber& operator=(const SharedHostSubscriber&) = delete;

    Result<std::vector<std::byte>> TryPoll();
    Result<std::vector<std::byte>> Poll(
        Deadline deadline = Deadline::FromNow(std::chrono::seconds(5)));
    bool active() const noexcept;

private:
    friend class SharedHostDomain;
    struct Impl;
    explicit SharedHostSubscriber(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

class SharedHostDomain {
public:
    static Result<uint64_t> RequiredBytes(
        const SharedHostDomainOptions& options = {});

    static Result<SharedHostDomain> Create(
        std::string_view name, SharedHostDomainOptions options = {});
    static Result<SharedHostDomain> Open(std::string_view name);
    static Status Unlink(std::string_view name);

    SharedHostDomain(SharedHostDomain&& other) noexcept;
    SharedHostDomain& operator=(SharedHostDomain&& other) noexcept;
    ~SharedHostDomain();

    SharedHostDomain(const SharedHostDomain&) = delete;
    SharedHostDomain& operator=(const SharedHostDomain&) = delete;

    // Registers this process in the peer table. Required before Advertise /
    // Subscribe. Idempotent for the current ProcessIdentity.
    Status Join(NodeId node_id);
    Status Heartbeat();
    Status Leave();

    Result<std::vector<SharedPeerInfo>> ListPeers();
    Result<std::vector<SharedTopicInfo>> ListTopics();

    Result<SharedHostPublisher> Advertise(
        std::string_view topic, const schema::SchemaIdentity& schema);
    Result<SharedHostSubscriber> Subscribe(
        std::string_view topic, const schema::SchemaIdentity& schema,
        Deadline deadline = Deadline::FromNow(std::chrono::seconds(5)));

    // Reclaims peer slots and topic publisher leases whose ProcessIdentity is
    // proven dead. Survivors are never wedged by unknown/live owners.
    Status Recover();

    const std::string& name() const noexcept;
    uint64_t size_bytes() const noexcept;

private:
    struct Impl;
    explicit SharedHostDomain(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mino::deployment

#endif  // MINO_RUNTIME_DEPLOYMENT_SHARED_HOST_DOMAIN_H_
