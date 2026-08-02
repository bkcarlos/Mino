// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/bridge/remote_object_reconstructor.h"

#include <algorithm>
#include <limits>
#include <new>
#include <utility>

#include "mino/shm/channel/index_slot.h"

namespace mino::bridge {
namespace {

bool SameIdentity(const schema::SchemaIdentity& lhs,
                  const schema::SchemaIdentity& rhs) noexcept {
    return lhs.short_id() == rhs.short_id() &&
           lhs.canonical_digest() == rhs.canonical_digest() &&
           lhs.schema_version() == rhs.schema_version() &&
           lhs.layout_version() == rhs.layout_version();
}

Status ValidateDataFrame(const WireFrame& frame) noexcept {
    if (frame.header.frame_type != FrameType::kData ||
        HasFrameFlag(frame.header.flags, FrameFlag::kControlFrame)) {
        return Status::Error(StatusCode::kInvalidArgument);
    }
    return Status::Ok();
}

Status ValidateActiveTopic(const WireFrame& frame,
                           const RemoteObjectBinding& binding) noexcept {
    if (binding.topic == nullptr ||
        binding.topic->topic_id != TopicId{frame.header.topic_id}) {
        return Status::Error(StatusCode::kSchemaMismatch);
    }
    if (binding.topic->state != registry::TopicState::kActive) {
        return Status::Error(StatusCode::kUnavailable);
    }
    return Status::Ok();
}

bool TopicAcceptsSchema(const registry::TopicMetadata& topic,
                        const schema::SchemaIdentity& identity) noexcept {
    if (SameIdentity(topic.schema, identity)) return true;
    return std::any_of(topic.accepted_schemas.begin(),
                       topic.accepted_schemas.end(),
                       [&identity](const schema::SchemaIdentity& accepted) {
                           return SameIdentity(accepted, identity);
                       });
}

Status ValidateIdentityAndBinding(
    const WireFrame& frame, const schema::SchemaIdentity& negotiated,
    const RemoteObjectBinding& binding) noexcept {
    if (frame.header.connection_schema_ref == 0 ||
        frame.header.msg_type != static_cast<uint32_t>(negotiated.short_id()) ||
        frame.header.schema_version != negotiated.schema_version() ||
        frame.header.layout_version != negotiated.layout_version()) {
        return Status::Error(StatusCode::kSchemaMismatch);
    }
    if (binding.schema_handle == nullptr || binding.layout_plan == nullptr ||
        binding.allocator == nullptr || binding.allocation_journal == nullptr ||
        binding.publication_target == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument);
    }
    if (!TopicAcceptsSchema(*binding.topic, negotiated) ||
        !SameIdentity(binding.schema_handle->identity(), negotiated) ||
        binding.layout_plan->layout_version() != negotiated.layout_version() ||
        binding.type_id.value != frame.header.msg_type ||
        binding.layout_plan->object_size() == 0 ||
        binding.layout_plan->object_size() >
            std::numeric_limits<uint32_t>::max()) {
        return Status::Error(StatusCode::kSchemaMismatch);
    }
    return Status::Ok();
}

Status RollbackWith(schema::PreparedDynamicObject& object,
                    StatusCode primary) noexcept {
    const Status rollback = object.Rollback();
    return rollback.ok() ? Status::Error(primary) : rollback;
}

}  // namespace

Status RemoteObjectReconstructor::DecodeValidatePublish(
    const WireFrame& frame) noexcept {
    try {
        if (schema_negotiator_ == nullptr || binding_resolver_ == nullptr) {
            return Status::Error(StatusCode::kInvalidArgument);
        }

        // Keep this order aligned with detailed design section 16.3: data frame,
        // active Topic, full negotiated identity/version, bounded wire decode,
        // Slab construction/validation, then local publication.
        const Status data_status = ValidateDataFrame(frame);
        if (!data_status.ok()) return data_status;

        auto identity = schema_negotiator_->IdentityForRemoteRef(
            frame.header.connection_schema_ref);
        if (!identity.ok()) return identity.status();
        auto binding_result = binding_resolver_->Resolve(
            TopicId{frame.header.topic_id}, *identity);
        if (!binding_result.ok()) return binding_result.status();
        RemoteObjectBinding binding = std::move(*binding_result);

        const Status topic_status = ValidateActiveTopic(frame, binding);
        if (!topic_status.ok()) return topic_status;

        const Status identity_status =
            ValidateIdentityAndBinding(frame, *identity, binding);
        if (!identity_status.ok()) return identity_status;

        if (frame.payload.size() > options_.wire_limits.max_frame_bytes ||
            binding.descriptor_closure.size() >
                options_.max_descriptor_closure) {
            return Status::Error(StatusCode::kResourceExhausted);
        }
        auto message = schema::CanonicalWireCodec::Decode(
            *binding.schema_handle, frame.payload,
            binding.descriptor_closure, options_.wire_limits);
        if (!message.ok()) return message.status();

        auto builder = schema::DynamicBuilder::FromDynamicMessage(
            binding.schema_handle, *binding.layout_plan, *message,
            *binding.allocator, *binding.allocation_journal, binding.type_id,
            binding.descriptor_closure, options_.allocation_owner,
            options_.object_options);
        if (!builder.ok()) return builder.status();

        auto prepared = builder->Prepare();
        if (!prepared.ok()) return prepared.status();

        const DynamicPublicationMetadata metadata{
            .message_type = frame.header.msg_type,
            .schema_version = identity->schema_version(),
            .schema_short_id = identity->short_id(),
            .layout_version = identity->layout_version(),
            .timestamp_ns = frame.header.timestamp_ns,
            .payload_size = static_cast<uint32_t>(
                binding.layout_plan->object_size()),
            .index_flags = binding.layout_plan->max_dynamic_children() == 0
                               ? 0u
                               : kIndexSlotFlagHasChildSlabs,
        };
        return binding.publication_target->Publish(std::move(*prepared),
                                                   metadata);
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Status SpscDynamicPublicationTarget::Publish(
    schema::PreparedDynamicObject&& object,
    const DynamicPublicationMetadata& metadata) noexcept {
    if (channel_ == nullptr || channel_id_ == 0 || !object.active() ||
        metadata.message_type == 0 || metadata.schema_short_id == 0 ||
        metadata.layout_version == 0 || metadata.payload_size == 0 ||
        (metadata.index_flags & kIndexSlotFlagReservedMask) != 0) {
        return RollbackWith(object, StatusCode::kInvalidArgument);
    }

    auto reservation = channel_->Reserve(QueueFullPolicy::kFail);
    if (!reservation.ok()) {
        return RollbackWith(object, reservation.status().code());
    }

    IndexSlot* slot = reservation->slot();
    slot->msg_type = metadata.message_type;
    slot->schema_version = metadata.schema_version;
    slot->schema_short_id = metadata.schema_short_id;
    slot->schema_layout_version = metadata.layout_version;
    slot->reserved0 = 0;
    slot->timestamp_ns = metadata.timestamp_ns;
    slot->payload = object.root_handle();
    slot->payload_len = metadata.payload_size;
    slot->flags = metadata.index_flags;

    const uint64_t sequence =
        slot->sequence_num.load(std::memory_order_relaxed);
    const Status journal_commit = object.CommitPublication(PublicationBinding{
        .channel_kind = PublicationChannelKind::kSpsc,
        .channel_id = channel_id_,
        .sequence = sequence,
        .payload = object.root_handle(),
    });
    if (!journal_commit.ok()) {
        const Status channel_abort = std::move(*reservation).Abort();
        if (!channel_abort.ok()) {
            return RollbackWith(object, channel_abort.code());
        }
        return RollbackWith(object, journal_commit.code());
    }

    const Status channel_commit = std::move(*reservation).Commit();
    if (!channel_commit.ok()) {
        return RollbackWith(object, channel_commit.code());
    }

    // Channel Commit is the visibility/acceptance linearization point. A
    // finalization error is cleanup debt recoverable from the persisted binding;
    // returning failure here could make the sender retry an already-visible
    // object.
    if (!object.FinalizeVisible().ok()) {
        journal_cleanup_debt_count_.fetch_add(1, std::memory_order_relaxed);
    }
    return Status::Ok();
}

}  // namespace mino::bridge
