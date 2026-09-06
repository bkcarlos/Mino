// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_BRIDGE_GRAPH_OWNERSHIP_FORWARD_H_
#define MINO_BRIDGE_GRAPH_OWNERSHIP_FORWARD_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "mino/common/ids.h"
#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/platform/memory_registration.h"
#include "mino/platform/process_identity.h"
#include "mino/runtime/allocation_journal.h"
#include "mino/schema/dynamic_object.h"
#include "mino/schema/layout.h"
#include "mino/schema/registry.h"
#include "mino/schema/wire.h"
#include "mino/shm/allocator/central_slab.h"

namespace mino::bridge {

// Hybrid cross-host ownership forwarding.
//
// True shared-memory zero-copy across hosts is impossible without RDMA/Fabric
// memory registration. This path eliminates the graph→SemanticFrame→wire and
// wire→SemanticFrame→graph *intermediate* payload copies:
//
//   encode: SHM child BytesView → EncodeInto → canonical wire buffer (1 copy)
//   reconstruct: wire BytesView → DynamicBuilder AllocateChild (1 SHM memcpy)
//
// The WireFrame body materialization / TCP or RDMA transfer remains an
// unavoidable wire copy (documented below). When a MemoryRegistrationProvider
// is injected, TryRegisterPayload offers a hook for P9 verbs plugins to post
// registered memory instead of bouncing through staging.
struct GraphOwnershipForwardOptions {
    schema::WireLimits wire_limits{};
    schema::DynamicObjectOptions object_options{};
    MemoryRegistrationProvider* registration_provider = nullptr;
    MemoryRegistrationOwner registration_owner{};
    MemoryRegistrationKind registration_kind = MemoryRegistrationKind::kRdma;
    uint64_t registration_scope_id = 0;

    GraphOwnershipForwardOptions() noexcept {
        // Reconstruct borrows bytes from the wire span so DynamicBuilder is the
        // sole payload memcpy into local SHM.
        wire_limits.borrow_bytes_fields = true;
    }
};

// Structural honesty counters for tests and docs. Not a profiler.
struct GraphOwnershipCopyCensus {
    uint64_t encode_calls = 0;
    uint64_t reconstruct_calls = 0;
    // Payload bytes copied SHM→wire during EncodeFromGraph / EncodeMessage.
    uint64_t encode_payload_bytes_copied = 0;
    // Payload bytes copied wire→SHM during Reconstruct*.
    uint64_t reconstruct_payload_bytes_copied = 0;
    // SemanticFrame.payload.assign paths avoided by this forwarder (always 0
    // here; hybrid bridge records when it skips GeneratedToSemantic assign).
    uint64_t semantic_payload_assigns_avoided = 0;
    uint64_t registration_attempts = 0;
    uint64_t registration_successes = 0;
};

class GraphOwnershipForwarder {
public:
    static Result<GraphOwnershipForwarder> Create(
        schema::DynamicSchemaHandle root,
        std::span<const schema::DynamicSchemaHandle> descriptor_closure = {},
        GraphOwnershipForwardOptions options = {}) noexcept;

    // Encode a pinned SHM graph to canonical wire without SemanticFrame.
    // Bytes leaves are borrowed from SHM; EncodeInto performs the single
    // payload materialization into `output`.
    Status EncodeFromGraph(
        ShmHandle root, const CentralSlabAllocator& allocator,
        ShmPinToken root_pin, std::vector<std::byte>& output) noexcept;

    // Encode an already-built DynamicMessage (callers may supply BytesView
    // fields pointing at SHM or wire). Counts bytes fields as payload copies
    // into `output`.
    Status EncodeMessage(const schema::DynamicMessage& message,
                         std::vector<std::byte>& output) noexcept;

    // Decode borrowed wire bytes and rebuild a local SHM graph. Payload bytes
    // are copied once from the wire span into child slabs.
    Result<schema::PreparedDynamicObject> ReconstructToPrepared(
        std::span<const std::byte> wire_payload, CentralSlabAllocator& allocator,
        AllocationJournal& journal, TypeId type_id,
        const ProcessIdentity& owner = ProcessIdentity::Current()) noexcept;

    // Full Decode→Commit helper when a pin table is available (matches
    // generated WireAdapter::Decode ownership).
    Result<schema::DynamicObject> ReconstructToObject(
        std::span<const std::byte> wire_payload, CentralSlabAllocator& allocator,
        AllocationJournal& journal, ShmPinTable& pins, TypeId type_id,
        const ProcessIdentity& owner = ProcessIdentity::Current()) noexcept;

    // Optional RDMA/DMA registration hook. No-ops with Unavailable providers;
    // production verbs plugins (P9) inject a real provider.
    Result<std::optional<RegisteredMemory>> TryRegisterPayload(
        void* address, uint64_t bytes) noexcept;

    const GraphOwnershipCopyCensus& census() const noexcept { return census_; }
    GraphOwnershipCopyCensus* mutable_census() noexcept { return &census_; }

    const schema::SchemaDescriptor& root_descriptor() const noexcept {
        return *root_;
    }
    const schema::LayoutPlan& layout_plan() const noexcept { return layout_; }
    const GraphOwnershipForwardOptions& options() const noexcept {
        return options_;
    }

private:
    GraphOwnershipForwarder(
        schema::DynamicSchemaHandle root,
        std::vector<schema::DynamicSchemaHandle> closure, schema::LayoutPlan layout,
        schema::PreparedCanonicalWireCodec codec,
        GraphOwnershipForwardOptions options) noexcept;

    static uint64_t CountBytesPayload(const schema::DynamicMessage& message,
                                      bool borrowed_only) noexcept;

    schema::DynamicSchemaHandle root_;
    std::vector<schema::DynamicSchemaHandle> closure_;
    schema::LayoutPlan layout_;
    schema::PreparedCanonicalWireCodec codec_;
    GraphOwnershipForwardOptions options_;
    schema::CanonicalWireScratch encode_scratch_;
    schema::CanonicalWireScratch decode_scratch_;
    schema::DynamicMessage decode_message_;
    GraphOwnershipCopyCensus census_{};
};

}  // namespace mino::bridge

#endif  // MINO_BRIDGE_GRAPH_OWNERSHIP_FORWARD_H_
