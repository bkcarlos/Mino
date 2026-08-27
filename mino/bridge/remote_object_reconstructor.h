// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_BRIDGE_REMOTE_OBJECT_RECONSTRUCTOR_H_
#define MINO_BRIDGE_REMOTE_OBJECT_RECONSTRUCTOR_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <span>

#include "mino/bridge/bridge_pipeline.h"
#include "mino/common/ids.h"
#include "mino/common/result.h"
#include "mino/platform/process_identity.h"
#include "mino/registry/metadata.h"
#include "mino/runtime/allocation_journal.h"
#include "mino/schema/dynamic_object.h"
#include "mino/schema/layout.h"
#include "mino/schema/registry.h"
#include "mino/schema/wire.h"
#include "mino/shm/allocator/central_slab.h"
#include "mino/shm/channel/spsc_channel.h"

namespace mino::bridge {

// Immutable channel metadata accompanying one prepared dynamic graph. The
// sequence written to an IndexSlot is deliberately assigned by the local
// channel reservation, not copied from the remote source sequence.
struct DynamicPublicationMetadata {
    uint32_t message_type = 0;
    uint32_t schema_version = 0;
    uint64_t schema_short_id = 0;
    uint32_t layout_version = 0;
    uint64_t timestamp_ns = 0;
    uint32_t payload_size = 0;
    uint32_t index_flags = 0;
};

class DynamicPublicationTarget {
public:
    virtual ~DynamicPublicationTarget() = default;

    // On entry object owns a prepared BUILDING journal transaction. A target
    // must either make it visible and finalize it, or roll it back. Once its
    // Channel commit succeeds it must report success even if finalization leaves
    // crash-recoverable journal cleanup debt.
    virtual Status Publish(
        schema::PreparedDynamicObject&& object,
        const DynamicPublicationMetadata& metadata) = 0;
};

// All pointed-to resources and descriptor_closure elements must remain valid
// until Resolve()'s caller returns. schema_handle itself keeps the root
// descriptor alive for the reconstruction operation.
struct RemoteObjectBinding {
    const registry::TopicMetadata* topic = nullptr;
    schema::SchemaHandle schema_handle;
    const schema::LayoutPlan* layout_plan = nullptr;
    std::span<const schema::SchemaHandle> descriptor_closure;
    CentralSlabAllocator* allocator = nullptr;
    AllocationJournal* allocation_journal = nullptr;
    TypeId type_id;
    DynamicPublicationTarget* publication_target = nullptr;
};

class RemoteObjectBindingResolver {
public:
    virtual ~RemoteObjectBindingResolver() = default;
    // N/N-1 ingress requires a binding whose descriptor and layout match the
    // connection-negotiated identity, not merely the Topic's primary schema.
    virtual Result<RemoteObjectBinding> Resolve(
        TopicId topic_id, const schema::SchemaIdentity& identity) = 0;
};

struct RemoteObjectReconstructorOptions {
    schema::WireLimits wire_limits;
    schema::DynamicObjectOptions object_options;
    size_t max_descriptor_closure = 4096;
    ProcessIdentity allocation_owner = ProcessIdentity::Current();
};

// Bridge ingress implementation for canonical wire objects. The BridgePipeline
// only acknowledges a frame after this port has completed local publication.
class RemoteObjectReconstructor final : public BridgeIngressPort {
public:
    RemoteObjectReconstructor(
        SchemaNegotiator& schema_negotiator,
        RemoteObjectBindingResolver& binding_resolver,
        RemoteObjectReconstructorOptions options = {}) noexcept
        : schema_negotiator_(&schema_negotiator),
          binding_resolver_(&binding_resolver),
          options_(options) {}

    Status DecodeValidatePublish(const WireFrame& frame) noexcept override;
    Status DecodeValidatePublish(const WireFrameHeader& header,
                                 std::span<const std::byte> payload) noexcept override;

private:
    SchemaNegotiator* schema_negotiator_ = nullptr;
    RemoteObjectBindingResolver* binding_resolver_ = nullptr;
    RemoteObjectReconstructorOptions options_;
};

// A real, non-blocking SPSC publication target suitable for a serialized Bridge
// ingress loop. Reserve(kFail) keeps queue-full handling bounded; callers may
// retry according to the Topic's backpressure policy outside the publication
// transaction.
class SpscDynamicPublicationTarget final : public DynamicPublicationTarget {
public:
    SpscDynamicPublicationTarget(SpscChannel& channel,
                                 uint64_t channel_id) noexcept
        : channel_(&channel), channel_id_(channel_id) {}

    Status Publish(
        schema::PreparedDynamicObject&& object,
        const DynamicPublicationMetadata& metadata) noexcept override;

    uint64_t journal_cleanup_debt_count() const noexcept {
        return journal_cleanup_debt_count_.load(std::memory_order_relaxed);
    }

private:
    SpscChannel* channel_ = nullptr;
    uint64_t channel_id_ = 0;
    std::atomic<uint64_t> journal_cleanup_debt_count_{0};
};

}  // namespace mino::bridge

#endif  // MINO_BRIDGE_REMOTE_OBJECT_RECONSTRUCTOR_H_
