// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_BRIDGE_SCHEMA_NEGOTIATOR_H_
#define MINO_BRIDGE_SCHEMA_NEGOTIATOR_H_

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <span>
#include <utility>
#include <vector>

#include "mino/bridge/wire_frame.h"
#include "mino/common/result.h"
#include "mino/schema/descriptor.h"
#include "mino/schema/registry.h"

namespace mino::bridge {

inline constexpr uint32_t kSchemaControlPayloadVersion = 1;
inline constexpr size_t kSchemaAnnouncementFixedPayloadBytes = 60;
inline constexpr size_t kDefaultSchemaControlFrameBytes = 256u * 1024u;

// The artifact is authenticated before it is allowed to affect a connection
// binding. Implementations may verify a signature, an authenticated transport
// principal, or both.
class DescriptorAuth {
public:
    virtual ~DescriptorAuth() = default;
    virtual Status Authenticate(
        const schema::SchemaIdentity& identity,
        std::span<const std::byte> descriptor_artifact) = 0;
};

// Persistence is a required publication barrier for descriptors learned from a
// peer. A successful return means the validated artifact is durably available.
class DescriptorPersistence {
public:
    virtual ~DescriptorPersistence() = default;
    virtual Status Persist(
        const schema::SchemaIdentity& identity,
        std::span<const std::byte> descriptor_artifact) = 0;
};

struct SchemaAnnouncement {
    SchemaAnnouncement(uint32_t schema_ref,
                       schema::SchemaIdentity schema_identity,
                       std::vector<std::byte> artifact = {})
        : connection_schema_ref(schema_ref),
          identity(std::move(schema_identity)),
          descriptor_artifact(std::move(artifact)) {}

    uint32_t connection_schema_ref;
    schema::SchemaIdentity identity;
    std::vector<std::byte> descriptor_artifact;
};

enum class SchemaRequestKind : uint32_t {
    kByRef = 1,
    kByDigest = 2,
};

struct SchemaRequest {
    static SchemaRequest ByRef(uint32_t connection_schema_ref) noexcept;
    static SchemaRequest ByDigest(
        const schema::CanonicalDigest& canonical_digest) noexcept;

    SchemaRequestKind kind = SchemaRequestKind::kByRef;
    uint32_t connection_schema_ref = 0;
    schema::CanonicalDigest canonical_digest{};
};

// Canonical control-payload codec. WireFrameCodec adds/removes the separate
// 4-byte control opcode; these methods encode only WireFrame::payload.
class SchemaControlCodec {
public:
    static Result<std::vector<std::byte>> EncodeAnnouncement(
        const SchemaAnnouncement& announcement) noexcept;
    static Result<SchemaAnnouncement> DecodeAnnouncement(
        std::span<const std::byte> payload) noexcept;
    static Result<std::vector<std::byte>> EncodeRequest(
        const SchemaRequest& request) noexcept;
    static Result<SchemaRequest> DecodeRequest(
        std::span<const std::byte> payload) noexcept;
};

struct SchemaNegotiatorLimits {
    size_t max_buffered_bytes = 4u * 1024u * 1024u;
    size_t max_buffered_frames = 1024;
    // Absolute parser/allocation ceiling. max_control_frame_bytes is the
    // effective pre-fragmentation ceiling and should match the pipeline's
    // per-frame control admission budget (normally max_control_bytes).
    size_t max_descriptor_bytes = 16u * 1024u * 1024u;
    size_t max_control_frame_bytes = kDefaultSchemaControlFrameBytes;
    size_t max_distinct_requests_per_window = 16;
    uint64_t request_window_ns = 1'000'000'000ull;
};

struct SchemaNegotiationResult {
    // Data frames whose full schema identity has been verified in the registry.
    std::vector<WireFrame> ready_frames;
    // SchemaAnnounce or SchemaRequest frames to send to the peer.
    std::vector<WireFrame> outbound_control_frames;
};

// Per-connection state machine. It is intentionally not internally synchronized;
// a connection's event loop must serialize calls.
class SchemaNegotiator {
public:
    SchemaNegotiator(schema::SchemaRegistry* registry,
                     DescriptorAuth* descriptor_auth,
                     DescriptorPersistence* descriptor_persistence,
                     SchemaNegotiatorLimits limits = {}) noexcept;

    // Allocates a receiver-owned, monotonically increasing ref and emits an
    // announcement. Rebinding the same full identity is idempotent and returns
    // its existing ref. descriptor_artifact is retained only to answer a later
    // request; it is not sent proactively.
    Result<WireFrame> BindLocalSchema(
        const schema::SchemaIdentity& identity,
        std::span<const std::byte> descriptor_artifact = {}) noexcept;

    Result<SchemaNegotiationResult> HandleFrame(WireFrame frame,
                                                 uint64_t now_ns) noexcept;
    Result<SchemaNegotiationResult> HandleDataFrame(WireFrame frame,
                                                     uint64_t now_ns) noexcept;
    Result<SchemaNegotiationResult> HandleControlFrame(
        WireFrame frame, uint64_t now_ns) noexcept;

    Result<schema::SchemaIdentity> IdentityForRemoteRef(
        uint32_t connection_schema_ref) const noexcept;

    // Schema requests remain provisional until the exact returned control frame
    // is admitted by the upper control queue. If admission fails, do not call
    // this method; another unresolved frame for the same ref/digest returns its
    // exact pending request without committing request or rate state.
    Status ConfirmControlQueued(const WireFrame& frame) noexcept;

    size_t buffered_bytes() const noexcept { return buffered_bytes_; }
    size_t buffered_frames() const noexcept { return buffered_frames_; }
    size_t pending_request_count() const noexcept {
        return requested_refs_.size() + requested_digests_.size() +
               pending_ref_requests_.size() +
               pending_digest_requests_.size();
    }
    uint32_t local_ref_high_watermark() const noexcept {
        return local_ref_high_watermark_;
    }
    uint32_t remote_ref_high_watermark() const noexcept {
        return remote_ref_high_watermark_;
    }
    bool failed() const noexcept { return failed_; }

    // Starts a new connection epoch. All mappings, high-water marks, pending
    // requests, rate state, and buffered frames are discarded.
    void Reset() noexcept;

private:
    struct LocalBinding {
        LocalBinding(schema::SchemaIdentity value,
                     std::vector<std::byte> artifact)
            : identity(std::move(value)), artifact(std::move(artifact)) {}
        schema::SchemaIdentity identity;
        std::vector<std::byte> artifact;
    };

    struct RemoteBinding {
        explicit RemoteBinding(schema::SchemaIdentity value)
            : identity(std::move(value)) {}
        schema::SchemaIdentity identity;
        bool ready = false;
    };

    Result<SchemaNegotiationResult> HandleAnnouncement(
        std::span<const std::byte> payload, uint64_t now_ns) noexcept;
    Result<SchemaNegotiationResult> HandleRequest(
        std::span<const std::byte> payload) noexcept;
    Result<std::optional<WireFrame>> MaybeRequestRef(uint32_t schema_ref,
                                                     uint64_t now_ns);
    Result<std::optional<WireFrame>> MaybeRequestDigest(
        const schema::CanonicalDigest& digest, uint64_t now_ns);
    Status ConsumeRequestBudget(uint64_t now_ns);
    Status ValidateDescriptorSize(size_t descriptor_bytes) const;
    Status BufferFrame(WireFrame frame);
    Status ValidateDataFrame(const WireFrame& frame,
                             const schema::SchemaIdentity& identity) const;
    Status ResolveIdentity(const schema::SchemaIdentity& identity,
                           SchemaNegotiationResult* result);
    Status AcceptDescriptor(const SchemaAnnouncement& announcement,
                            SchemaNegotiationResult* result);
    void FailConnection() noexcept;

    schema::SchemaRegistry* registry_ = nullptr;
    DescriptorAuth* descriptor_auth_ = nullptr;
    DescriptorPersistence* descriptor_persistence_ = nullptr;
    SchemaNegotiatorLimits limits_;

    uint32_t local_ref_high_watermark_ = 0;
    uint32_t remote_ref_high_watermark_ = 0;
    std::map<uint32_t, LocalBinding> local_bindings_;
    std::map<schema::CanonicalDigest, uint32_t> local_refs_by_digest_;
    std::map<uint32_t, RemoteBinding> remote_bindings_;
    std::map<uint32_t, std::vector<WireFrame>> buffered_by_ref_;
    std::set<uint32_t> requested_refs_;
    std::set<schema::CanonicalDigest> requested_digests_;
    std::map<uint32_t, WireFrame> pending_ref_requests_;
    std::map<schema::CanonicalDigest, WireFrame> pending_digest_requests_;

    size_t buffered_bytes_ = 0;
    size_t buffered_frames_ = 0;
    uint64_t request_window_start_ns_ = 0;
    size_t requests_in_window_ = 0;
    bool request_window_initialized_ = false;
    bool failed_ = false;
};

}  // namespace mino::bridge

#endif  // MINO_BRIDGE_SCHEMA_NEGOTIATOR_H_
