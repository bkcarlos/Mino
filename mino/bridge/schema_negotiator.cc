// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/bridge/schema_negotiator.h"

#include <algorithm>
#include <limits>
#include <new>
#include <utility>

#include "mino/schema/canonical.h"

namespace mino::bridge {
namespace {

constexpr size_t kAnnouncementFixedSize =
    kSchemaAnnouncementFixedPayloadBytes;
constexpr size_t kSchemaAnnouncementFrameOverhead =
    kWireBaseHeaderLength + kWireControlOpcodeLength +
    kSchemaAnnouncementFixedPayloadBytes;
constexpr size_t kRequestPrefixSize = 8;
constexpr size_t kRequestByRefSize = 12;
constexpr size_t kRequestByDigestSize = 40;

Status Error(StatusCode code, std::string_view message) {
    return Status::Error(code, message);
}

Status Corruption(std::string_view message) {
    return Error(StatusCode::kCorruption, message);
}

Status Invalid(std::string_view message) {
    return Error(StatusCode::kInvalidArgument, message);
}

Status Resource(std::string_view message) {
    return Error(StatusCode::kResourceExhausted, message);
}

void WriteBe32(std::span<std::byte> bytes, size_t offset,
               uint32_t value) noexcept {
    bytes[offset] = static_cast<std::byte>((value >> 24) & 0xffu);
    bytes[offset + 1] = static_cast<std::byte>((value >> 16) & 0xffu);
    bytes[offset + 2] = static_cast<std::byte>((value >> 8) & 0xffu);
    bytes[offset + 3] = static_cast<std::byte>(value & 0xffu);
}

void WriteBe64(std::span<std::byte> bytes, size_t offset,
               uint64_t value) noexcept {
    for (size_t i = 0; i < 8; ++i) {
        bytes[offset + i] =
            static_cast<std::byte>((value >> (56 - 8 * i)) & 0xffu);
    }
}

uint32_t ReadBe32(std::span<const std::byte> bytes, size_t offset) noexcept {
    return (static_cast<uint32_t>(bytes[offset]) << 24) |
           (static_cast<uint32_t>(bytes[offset + 1]) << 16) |
           (static_cast<uint32_t>(bytes[offset + 2]) << 8) |
           static_cast<uint32_t>(bytes[offset + 3]);
}

uint64_t ReadBe64(std::span<const std::byte> bytes, size_t offset) noexcept {
    uint64_t value = 0;
    for (size_t i = 0; i < 8; ++i) {
        value = (value << 8) | static_cast<uint64_t>(bytes[offset + i]);
    }
    return value;
}

bool SameIdentity(const schema::SchemaIdentity& lhs,
                  const schema::SchemaIdentity& rhs) noexcept {
    return lhs.short_id() == rhs.short_id() &&
           lhs.canonical_digest() == rhs.canonical_digest() &&
           lhs.schema_version() == rhs.schema_version() &&
           lhs.layout_version() == rhs.layout_version();
}

Status ValidateIdentity(const schema::SchemaIdentity& identity) {
    if (identity.short_id() !=
        schema::DigestShortId(identity.canonical_digest())) {
        return Error(StatusCode::kSchemaMismatch,
                     "schema short ID does not match canonical digest");
    }
    return Status::Ok();
}

WireFrame ControlFrame(FrameType type, std::vector<std::byte> payload) {
    WireFrame frame;
    frame.header.frame_type = type;
    frame.header.flags = FlagValue(FrameFlag::kControlFrame);
    frame.payload = std::move(payload);
    return frame;
}

size_t RetainedFrameBytes(const WireFrame& frame) noexcept {
    size_t bytes = kWireBaseHeaderLength + frame.payload.size();
    if (frame.header.perf_trace.has_value()) {
        bytes += kWirePerfTraceContextLength;
    }
    if (HasFrameFlag(frame.header.flags, FrameFlag::kPayloadCrcPresent)) {
        bytes += kWirePayloadCrcLength;
    }
    return bytes;
}

}  // namespace

SchemaRequest SchemaRequest::ByRef(uint32_t connection_schema_ref) noexcept {
    SchemaRequest request;
    request.kind = SchemaRequestKind::kByRef;
    request.connection_schema_ref = connection_schema_ref;
    return request;
}

SchemaRequest SchemaRequest::ByDigest(
    const schema::CanonicalDigest& canonical_digest) noexcept {
    SchemaRequest request;
    request.kind = SchemaRequestKind::kByDigest;
    request.canonical_digest = canonical_digest;
    return request;
}

Result<std::vector<std::byte>> SchemaControlCodec::EncodeAnnouncement(
    const SchemaAnnouncement& announcement) noexcept {
    try {
        if (announcement.connection_schema_ref == 0) {
            return Invalid("schema announcement ref must be nonzero");
        }
        const Status identity_status = ValidateIdentity(announcement.identity);
        if (!identity_status.ok()) return identity_status;
        if (announcement.descriptor_artifact.size() >
            std::numeric_limits<uint32_t>::max()) {
            return Resource("descriptor artifact cannot be represented on wire");
        }
        if (announcement.descriptor_artifact.size() >
            std::numeric_limits<size_t>::max() - kAnnouncementFixedSize) {
            return Resource("schema announcement size overflows size_t");
        }

        std::vector<std::byte> payload(
            kAnnouncementFixedSize + announcement.descriptor_artifact.size());
        WriteBe32(payload, 0, kSchemaControlPayloadVersion);
        WriteBe32(payload, 4, announcement.connection_schema_ref);
        WriteBe64(payload, 8, announcement.identity.short_id());
        std::copy(announcement.identity.canonical_digest().begin(),
                  announcement.identity.canonical_digest().end(),
                  payload.begin() + 16);
        WriteBe32(payload, 48, announcement.identity.schema_version());
        WriteBe32(payload, 52, announcement.identity.layout_version());
        WriteBe32(payload, 56,
                  static_cast<uint32_t>(
                      announcement.descriptor_artifact.size()));
        std::copy(announcement.descriptor_artifact.begin(),
                  announcement.descriptor_artifact.end(),
                  payload.begin() + kAnnouncementFixedSize);
        return payload;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<SchemaAnnouncement> SchemaControlCodec::DecodeAnnouncement(
    std::span<const std::byte> payload) noexcept {
    try {
        if (payload.size() < 4) {
            return Corruption("schema announcement is truncated before version");
        }
        if (ReadBe32(payload, 0) != kSchemaControlPayloadVersion) {
            return Error(StatusCode::kUnsupported,
                         "unknown schema announcement version");
        }
        if (payload.size() < kAnnouncementFixedSize) {
            return Corruption("schema announcement fixed fields are truncated");
        }

        const uint32_t schema_ref = ReadBe32(payload, 4);
        if (schema_ref == 0) {
            return Corruption("schema announcement ref must be nonzero");
        }
        schema::CanonicalDigest digest{};
        std::copy_n(payload.begin() + 16, digest.size(), digest.begin());
        schema::SchemaIdentity identity(ReadBe64(payload, 8), digest,
                                        ReadBe32(payload, 48),
                                        ReadBe32(payload, 52));
        const Status identity_status = ValidateIdentity(identity);
        if (!identity_status.ok()) return identity_status;

        const uint32_t artifact_size = ReadBe32(payload, 56);
        if (static_cast<size_t>(artifact_size) !=
            payload.size() - kAnnouncementFixedSize) {
            return Corruption(
                "schema announcement artifact is truncated or has trailing bytes");
        }
        std::vector<std::byte> artifact(
            payload.begin() + kAnnouncementFixedSize, payload.end());
        return SchemaAnnouncement(schema_ref, std::move(identity),
                                  std::move(artifact));
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<std::vector<std::byte>> SchemaControlCodec::EncodeRequest(
    const SchemaRequest& request) noexcept {
    try {
        size_t size = 0;
        switch (request.kind) {
            case SchemaRequestKind::kByRef:
                if (request.connection_schema_ref == 0) {
                    return Invalid("schema request ref must be nonzero");
                }
                size = kRequestByRefSize;
                break;
            case SchemaRequestKind::kByDigest:
                size = kRequestByDigestSize;
                break;
            default:
                return Invalid("unknown schema request kind");
        }
        std::vector<std::byte> payload(size);
        WriteBe32(payload, 0, kSchemaControlPayloadVersion);
        WriteBe32(payload, 4, static_cast<uint32_t>(request.kind));
        if (request.kind == SchemaRequestKind::kByRef) {
            WriteBe32(payload, kRequestPrefixSize,
                      request.connection_schema_ref);
        } else {
            std::copy(request.canonical_digest.begin(),
                      request.canonical_digest.end(),
                      payload.begin() + kRequestPrefixSize);
        }
        return payload;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<SchemaRequest> SchemaControlCodec::DecodeRequest(
    std::span<const std::byte> payload) noexcept {
    try {
        if (payload.size() < 4) {
            return Corruption("schema request is truncated before version");
        }
        if (ReadBe32(payload, 0) != kSchemaControlPayloadVersion) {
            return Error(StatusCode::kUnsupported,
                         "unknown schema request version");
        }
        if (payload.size() < kRequestPrefixSize) {
            return Corruption("schema request kind is truncated");
        }
        const uint32_t encoded_kind = ReadBe32(payload, 4);
        if (encoded_kind ==
            static_cast<uint32_t>(SchemaRequestKind::kByRef)) {
            if (payload.size() != kRequestByRefSize) {
                return Corruption(
                    "schema ref request is truncated or has trailing bytes");
            }
            const uint32_t ref = ReadBe32(payload, kRequestPrefixSize);
            if (ref == 0) {
                return Corruption("schema request ref must be nonzero");
            }
            return SchemaRequest::ByRef(ref);
        }
        if (encoded_kind ==
            static_cast<uint32_t>(SchemaRequestKind::kByDigest)) {
            if (payload.size() != kRequestByDigestSize) {
                return Corruption(
                    "schema digest request is truncated or has trailing bytes");
            }
            schema::CanonicalDigest digest{};
            std::copy_n(payload.begin() + kRequestPrefixSize, digest.size(),
                        digest.begin());
            return SchemaRequest::ByDigest(digest);
        }
        return Corruption("unknown schema request kind");
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

SchemaNegotiator::SchemaNegotiator(
    schema::SchemaRegistry* registry, DescriptorAuth* descriptor_auth,
    DescriptorPersistence* descriptor_persistence,
    SchemaNegotiatorLimits limits) noexcept
    : registry_(registry),
      descriptor_auth_(descriptor_auth),
      descriptor_persistence_(descriptor_persistence),
      limits_(limits) {}

Result<WireFrame> SchemaNegotiator::BindLocalSchema(
    const schema::SchemaIdentity& identity,
    std::span<const std::byte> descriptor_artifact) noexcept {
    try {
        if (failed_) {
            return Error(StatusCode::kUnavailable,
                         "schema negotiator is failed; call Reset");
        }
        const Status identity_status = ValidateIdentity(identity);
        if (!identity_status.ok()) return identity_status;
        if (registry_ == nullptr) {
            return Error(StatusCode::kUnavailable,
                         "schema registry is required for local binding");
        }
        auto registered = registry_->Find(identity);
        if (!registered.ok()) return registered.status();
        const Status descriptor_size =
            ValidateDescriptorSize(descriptor_artifact.size());
        if (!descriptor_size.ok()) return descriptor_size;

        const auto existing =
            local_refs_by_digest_.find(identity.canonical_digest());
        if (existing != local_refs_by_digest_.end()) {
            auto binding = local_bindings_.find(existing->second);
            if (binding == local_bindings_.end() ||
                !SameIdentity(binding->second.identity, identity)) {
                return Error(StatusCode::kSchemaMismatch,
                             "local digest is already bound to another identity");
            }
            if (!descriptor_artifact.empty()) {
                binding->second.artifact.assign(descriptor_artifact.begin(),
                                                descriptor_artifact.end());
            }
            auto encoded = SchemaControlCodec::EncodeAnnouncement(
                SchemaAnnouncement(existing->second, identity));
            if (!encoded.ok()) return encoded.status();
            return ControlFrame(FrameType::kSchemaAnnounce,
                                std::move(*encoded));
        }

        if (local_ref_high_watermark_ ==
            std::numeric_limits<uint32_t>::max()) {
            return Resource("connection schema ref space is exhausted");
        }
        std::vector<std::byte> artifact(descriptor_artifact.begin(),
                                       descriptor_artifact.end());
        const uint32_t schema_ref = local_ref_high_watermark_ + 1;
        local_bindings_.emplace(
            schema_ref, LocalBinding(identity, std::move(artifact)));
        try {
            local_refs_by_digest_.emplace(identity.canonical_digest(),
                                          schema_ref);
        } catch (...) {
            local_bindings_.erase(schema_ref);
            throw;
        }
        local_ref_high_watermark_ = schema_ref;

        auto encoded = SchemaControlCodec::EncodeAnnouncement(
            SchemaAnnouncement(schema_ref, identity));
        if (!encoded.ok()) return encoded.status();
        return ControlFrame(FrameType::kSchemaAnnounce, std::move(*encoded));
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<SchemaNegotiationResult> SchemaNegotiator::HandleFrame(
    WireFrame frame, uint64_t now_ns) noexcept {
    if (frame.header.frame_type == FrameType::kData) {
        return HandleDataFrame(std::move(frame), now_ns);
    }
    return HandleControlFrame(std::move(frame), now_ns);
}

Result<SchemaNegotiationResult> SchemaNegotiator::HandleDataFrame(
    WireFrame frame, uint64_t now_ns) noexcept {
    try {
        if (failed_) {
            return Error(StatusCode::kUnavailable,
                         "schema negotiator is failed; call Reset");
        }
        if (frame.header.frame_type != FrameType::kData ||
            HasFrameFlag(frame.header.flags, FrameFlag::kControlFrame)) {
            return Invalid("HandleDataFrame requires a data frame");
        }
        const uint32_t schema_ref = frame.header.connection_schema_ref;
        if (schema_ref == 0) {
            return Error(StatusCode::kSchemaMismatch,
                         "data frame schema ref must be nonzero");
        }

        SchemaNegotiationResult result;
        const auto binding = remote_bindings_.find(schema_ref);
        if (binding != remote_bindings_.end() && binding->second.ready) {
            const Status validation =
                ValidateDataFrame(frame, binding->second.identity);
            if (!validation.ok()) return validation;
            result.ready_frames.push_back(std::move(frame));
            return result;
        }

        Result<std::optional<WireFrame>> request =
            binding == remote_bindings_.end()
                ? MaybeRequestRef(schema_ref, now_ns)
                : MaybeRequestDigest(
                      binding->second.identity.canonical_digest(), now_ns);
        if (!request.ok()) return request.status();
        const Status buffered = BufferFrame(std::move(frame));
        if (!buffered.ok()) return buffered;
        if (request->has_value()) {
            result.outbound_control_frames.push_back(
                std::move(request->value()));
        }
        return result;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<SchemaNegotiationResult> SchemaNegotiator::HandleControlFrame(
    WireFrame frame, uint64_t now_ns) noexcept {
    try {
        if (failed_) {
            return Error(StatusCode::kUnavailable,
                         "schema negotiator is failed; call Reset");
        }
        if (!HasFrameFlag(frame.header.flags, FrameFlag::kControlFrame)) {
            return Invalid("HandleControlFrame requires CONTROL_FRAME");
        }
        switch (frame.header.frame_type) {
            case FrameType::kSchemaAnnounce:
                return HandleAnnouncement(frame.payload, now_ns);
            case FrameType::kSchemaRequest:
                return HandleRequest(frame.payload);
            default:
                return Error(StatusCode::kUnsupported,
                             "control frame is not a schema negotiation opcode");
        }
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Status SchemaNegotiator::ReadyToPublishData(
    const WireFrameHeader& header) const noexcept {
    try {
        if (failed_) {
            return Error(StatusCode::kUnavailable,
                         "schema negotiator is failed; call Reset");
        }
        if (header.frame_type != FrameType::kData ||
            HasFrameFlag(header.flags, FrameFlag::kControlFrame)) {
            return Invalid("HandleDataFrame requires a data frame");
        }
        if (header.connection_schema_ref == 0) {
            return Error(StatusCode::kSchemaMismatch,
                         "data frame schema ref must be nonzero");
        }
        const auto binding = remote_bindings_.find(header.connection_schema_ref);
        if (binding == remote_bindings_.end() || !binding->second.ready) {
            return Status::Error(StatusCode::kNotFound);
        }
        if (header.schema_version != binding->second.identity.schema_version() ||
            header.layout_version != binding->second.identity.layout_version() ||
            header.msg_type !=
                static_cast<uint32_t>(binding->second.identity.short_id())) {
            return Error(
                StatusCode::kSchemaMismatch,
                "data frame header does not match negotiated full identity");
        }
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<schema::SchemaIdentity> SchemaNegotiator::IdentityForRemoteRef(
    uint32_t connection_schema_ref) const noexcept {
    try {
        const auto binding = remote_bindings_.find(connection_schema_ref);
        if (binding == remote_bindings_.end()) {
            return Error(StatusCode::kNotFound,
                         "connection schema ref is not announced");
        }
        return binding->second.identity;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Status SchemaNegotiator::ConfirmControlQueued(
    const WireFrame& frame) noexcept {
    try {
        if (failed_) {
            return Error(StatusCode::kUnavailable,
                         "schema negotiator is failed; call Reset");
        }
        if (frame.header.frame_type != FrameType::kSchemaRequest ||
            !HasFrameFlag(frame.header.flags, FrameFlag::kControlFrame)) {
            return Invalid("control frame is not a pending schema request");
        }
        auto request = SchemaControlCodec::DecodeRequest(frame.payload);
        if (!request.ok()) return request.status();

        if (request->kind == SchemaRequestKind::kByRef) {
            const auto pending = pending_ref_requests_.find(
                request->connection_schema_ref);
            if (pending == pending_ref_requests_.end() ||
                pending->second != frame) {
                return Invalid("control frame is not the pending schema request");
            }
            requested_refs_.insert(request->connection_schema_ref);
            ++requests_in_window_;
            pending_ref_requests_.erase(pending);
            return Status::Ok();
        }

        const auto pending = pending_digest_requests_.find(
            request->canonical_digest);
        if (pending == pending_digest_requests_.end() ||
            pending->second != frame) {
            return Invalid("control frame is not the pending schema request");
        }
        requested_digests_.insert(request->canonical_digest);
        ++requests_in_window_;
        pending_digest_requests_.erase(pending);
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<SchemaNegotiationResult> SchemaNegotiator::HandleAnnouncement(
    std::span<const std::byte> payload, uint64_t now_ns) noexcept {
    try {
        auto decoded = SchemaControlCodec::DecodeAnnouncement(payload);
        if (!decoded.ok()) return decoded.status();
        const Status descriptor_size =
            ValidateDescriptorSize(decoded->descriptor_artifact.size());
        if (!descriptor_size.ok()) return descriptor_size;

        const uint32_t schema_ref = decoded->connection_schema_ref;
        auto binding = remote_bindings_.find(schema_ref);
        if (binding != remote_bindings_.end()) {
            if (!SameIdentity(binding->second.identity, decoded->identity)) {
                return Error(StatusCode::kSchemaMismatch,
                             "connection schema ref cannot be rebound");
            }
        } else {
            if (schema_ref <= remote_ref_high_watermark_) {
                return Error(StatusCode::kSchemaMismatch,
                             "connection schema ref is not monotonic");
            }
            const auto inserted = remote_bindings_.emplace(
                schema_ref, RemoteBinding(decoded->identity));
            binding = inserted.first;
            remote_ref_high_watermark_ = schema_ref;
        }
        requested_refs_.erase(schema_ref);
        pending_ref_requests_.erase(schema_ref);

        SchemaNegotiationResult result;
        if (binding->second.ready) return result;
        if (!decoded->descriptor_artifact.empty()) {
            const Status accepted = AcceptDescriptor(*decoded, &result);
            if (!accepted.ok()) return accepted;
            return result;
        }

        if (registry_ == nullptr) {
            return Error(StatusCode::kUnavailable,
                         "schema registry is required for announcements");
        }
        auto known = registry_->Find(decoded->identity);
        if (known.ok()) {
            const Status resolved = ResolveIdentity(decoded->identity, &result);
            if (!resolved.ok()) return resolved;
            return result;
        }
        if (known.status().code() != StatusCode::kNotFound) {
            return known.status();
        }

        auto request = MaybeRequestDigest(
            decoded->identity.canonical_digest(), now_ns);
        if (!request.ok()) return request.status();
        if (request->has_value()) {
            result.outbound_control_frames.push_back(
                std::move(request->value()));
        }
        return result;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<SchemaNegotiationResult> SchemaNegotiator::HandleRequest(
    std::span<const std::byte> payload) noexcept {
    try {
        auto decoded = SchemaControlCodec::DecodeRequest(payload);
        if (!decoded.ok()) return decoded.status();

        auto binding = local_bindings_.end();
        if (decoded->kind == SchemaRequestKind::kByRef) {
            binding = local_bindings_.find(decoded->connection_schema_ref);
        } else {
            const auto ref =
                local_refs_by_digest_.find(decoded->canonical_digest);
            if (ref != local_refs_by_digest_.end()) {
                binding = local_bindings_.find(ref->second);
            }
        }
        if (binding == local_bindings_.end()) {
            return Error(StatusCode::kNotFound,
                         "requested local schema binding is unknown");
        }
        if (binding->second.artifact.empty()) {
            return Error(StatusCode::kUnavailable,
                         "requested descriptor artifact is unavailable");
        }

        auto encoded = SchemaControlCodec::EncodeAnnouncement(
            SchemaAnnouncement(binding->first, binding->second.identity,
                               binding->second.artifact));
        if (!encoded.ok()) return encoded.status();
        SchemaNegotiationResult result;
        result.outbound_control_frames.push_back(ControlFrame(
            FrameType::kSchemaAnnounce, std::move(*encoded)));
        return result;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<std::optional<WireFrame>> SchemaNegotiator::MaybeRequestRef(
    uint32_t schema_ref, uint64_t now_ns) {
    if (requested_refs_.contains(schema_ref)) {
        return std::optional<WireFrame>{};
    }
    const auto pending = pending_ref_requests_.find(schema_ref);
    if (pending != pending_ref_requests_.end()) {
        return std::optional<WireFrame>(pending->second);
    }
    const Status budget = ConsumeRequestBudget(now_ns);
    if (!budget.ok()) return budget;
    const SchemaRequest request = SchemaRequest::ByRef(schema_ref);
    auto encoded = SchemaControlCodec::EncodeRequest(request);
    if (!encoded.ok()) return encoded.status();
    WireFrame frame =
        ControlFrame(FrameType::kSchemaRequest, std::move(*encoded));
    pending_ref_requests_.emplace(schema_ref, frame);
    return std::optional<WireFrame>(std::move(frame));
}

Result<std::optional<WireFrame>> SchemaNegotiator::MaybeRequestDigest(
    const schema::CanonicalDigest& digest, uint64_t now_ns) {
    if (requested_digests_.contains(digest)) {
        return std::optional<WireFrame>{};
    }
    const auto pending = pending_digest_requests_.find(digest);
    if (pending != pending_digest_requests_.end()) {
        return std::optional<WireFrame>(pending->second);
    }
    const Status budget = ConsumeRequestBudget(now_ns);
    if (!budget.ok()) return budget;
    const SchemaRequest request = SchemaRequest::ByDigest(digest);
    auto encoded = SchemaControlCodec::EncodeRequest(request);
    if (!encoded.ok()) return encoded.status();
    WireFrame frame =
        ControlFrame(FrameType::kSchemaRequest, std::move(*encoded));
    pending_digest_requests_.emplace(digest, frame);
    return std::optional<WireFrame>(std::move(frame));
}

Status SchemaNegotiator::ConsumeRequestBudget(uint64_t now_ns) {
    if (!request_window_initialized_ || now_ns < request_window_start_ns_ ||
        now_ns - request_window_start_ns_ >= limits_.request_window_ns) {
        request_window_initialized_ = true;
        request_window_start_ns_ = now_ns;
        requests_in_window_ = 0;
    }
    if (requests_in_window_ >= limits_.max_distinct_requests_per_window) {
        return Resource("schema request rate limit exceeded");
    }
    const size_t provisional_slots =
        limits_.max_distinct_requests_per_window - requests_in_window_;
    if (pending_ref_requests_.size() >= provisional_slots ||
        pending_digest_requests_.size() >=
            provisional_slots - pending_ref_requests_.size()) {
        return Resource("schema provisional request limit exceeded");
    }
    return Status::Ok();
}

Status SchemaNegotiator::ValidateDescriptorSize(
    size_t descriptor_bytes) const {
    if (descriptor_bytes > limits_.max_descriptor_bytes) {
        return Resource("descriptor artifact exceeds configured limit");
    }
    if (limits_.max_control_frame_bytes <
            kSchemaAnnouncementFrameOverhead ||
        descriptor_bytes > limits_.max_control_frame_bytes -
                               kSchemaAnnouncementFrameOverhead) {
        return Resource(
            "descriptor artifact cannot fit configured schema control frame; "
            "raise the shared control-frame limit or add fragmentation");
    }
    return Status::Ok();
}

Status SchemaNegotiator::BufferFrame(WireFrame frame) {
    const size_t retained = RetainedFrameBytes(frame);
    if (buffered_frames_ >= limits_.max_buffered_frames ||
        retained > limits_.max_buffered_bytes -
                       std::min(buffered_bytes_, limits_.max_buffered_bytes)) {
        FailConnection();
        return Resource("unknown-schema frame buffer limit exceeded");
    }
    const uint32_t schema_ref = frame.header.connection_schema_ref;
    buffered_by_ref_[schema_ref].push_back(std::move(frame));
    buffered_bytes_ += retained;
    ++buffered_frames_;
    return Status::Ok();
}

Status SchemaNegotiator::ValidateDataFrame(
    const WireFrame& frame,
    const schema::SchemaIdentity& identity) const {
    if (frame.header.connection_schema_ref == 0 ||
        frame.header.schema_version != identity.schema_version() ||
        frame.header.layout_version != identity.layout_version() ||
        frame.header.msg_type != static_cast<uint32_t>(identity.short_id())) {
        return Error(StatusCode::kSchemaMismatch,
                     "data frame header does not match negotiated full identity");
    }
    return Status::Ok();
}

Status SchemaNegotiator::ResolveIdentity(
    const schema::SchemaIdentity& identity,
    SchemaNegotiationResult* result) {
    if (registry_ == nullptr) {
        return Error(StatusCode::kUnavailable,
                     "schema registry is required to resolve identity");
    }
    auto verified = registry_->Find(identity);
    if (!verified.ok()) return verified.status();
    if (!SameIdentity((*verified)->identity(), identity)) {
        return Error(StatusCode::kSchemaMismatch,
                     "registry returned a different full schema identity");
    }

    size_t releasing = 0;
    for (const auto& [schema_ref, binding] : remote_bindings_) {
        if (!binding.ready && SameIdentity(binding.identity, identity)) {
            const auto buffered = buffered_by_ref_.find(schema_ref);
            if (buffered != buffered_by_ref_.end()) {
                for (const WireFrame& frame : buffered->second) {
                    const Status validation =
                        ValidateDataFrame(frame, binding.identity);
                    if (!validation.ok()) return validation;
                }
                if (releasing >
                    std::numeric_limits<size_t>::max() -
                        buffered->second.size()) {
                    return Resource("released frame count overflows size_t");
                }
                releasing += buffered->second.size();
            }
        }
    }
    if (releasing >
        std::numeric_limits<size_t>::max() - result->ready_frames.size()) {
        return Resource("released frame count overflows size_t");
    }
    result->ready_frames.reserve(result->ready_frames.size() + releasing);

    for (auto& [schema_ref, binding] : remote_bindings_) {
        if (binding.ready || !SameIdentity(binding.identity, identity)) continue;
        binding.ready = true;
        requested_refs_.erase(schema_ref);
        auto buffered = buffered_by_ref_.find(schema_ref);
        if (buffered == buffered_by_ref_.end()) continue;
        for (WireFrame& frame : buffered->second) {
            buffered_bytes_ -= RetainedFrameBytes(frame);
            --buffered_frames_;
            result->ready_frames.push_back(std::move(frame));
        }
        buffered_by_ref_.erase(buffered);
    }
    requested_digests_.erase(identity.canonical_digest());
    pending_digest_requests_.erase(identity.canonical_digest());
    return Status::Ok();
}

Status SchemaNegotiator::AcceptDescriptor(
    const SchemaAnnouncement& announcement,
    SchemaNegotiationResult* result) {
    if (descriptor_auth_ == nullptr) {
        return Error(StatusCode::kPermissionDenied,
                     "descriptor authentication seam is missing");
    }
    if (descriptor_persistence_ == nullptr) {
        return Error(StatusCode::kUnavailable,
                     "descriptor persistence seam is missing");
    }
    if (registry_ == nullptr) {
        return Error(StatusCode::kUnavailable,
                     "schema registry is required for descriptor registration");
    }

    Status authenticated = descriptor_auth_->Authenticate(
        announcement.identity, announcement.descriptor_artifact);
    if (!authenticated.ok()) return authenticated;

    auto validated = registry_->ValidateDescriptorArtifact(
        announcement.descriptor_artifact);
    if (!validated.ok()) return validated.status();

    const auto announced = std::find_if(
        validated->descriptors().begin(), validated->descriptors().end(),
        [&](const schema::SchemaHandle& descriptor) {
            return descriptor != nullptr &&
                   SameIdentity(descriptor->identity(), announcement.identity);
        });
    if (announced == validated->descriptors().end()) {
        return Error(StatusCode::kSchemaMismatch,
                     "validated artifact omits announced full identity");
    }

    Status persisted = descriptor_persistence_->Persist(
        announcement.identity, announcement.descriptor_artifact);
    if (!persisted.ok()) return persisted;

    auto published =
        registry_->PublishDescriptorArtifact(std::move(*validated));
    if (!published.ok()) return published.status();
    return ResolveIdentity(announcement.identity, result);
}

void SchemaNegotiator::FailConnection() noexcept {
    failed_ = true;
    buffered_by_ref_.clear();
    requested_refs_.clear();
    requested_digests_.clear();
    pending_ref_requests_.clear();
    pending_digest_requests_.clear();
    buffered_bytes_ = 0;
    buffered_frames_ = 0;
}

void SchemaNegotiator::Reset() noexcept {
    local_bindings_.clear();
    local_refs_by_digest_.clear();
    remote_bindings_.clear();
    buffered_by_ref_.clear();
    requested_refs_.clear();
    requested_digests_.clear();
    pending_ref_requests_.clear();
    pending_digest_requests_.clear();
    local_ref_high_watermark_ = 0;
    remote_ref_high_watermark_ = 0;
    buffered_bytes_ = 0;
    buffered_frames_ = 0;
    request_window_start_ns_ = 0;
    requests_in_window_ = 0;
    request_window_initialized_ = false;
    failed_ = false;
}

}  // namespace mino::bridge
