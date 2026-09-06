// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/bridge/graph_ownership_forward.h"

#include <utility>

namespace mino::bridge {
namespace {

uint64_t CountBytesInValue(const schema::DynamicValue& value,
                           bool borrowed_only) noexcept {
    if (const auto* view = value.bytes_view()) {
        return static_cast<uint64_t>(view->value.size());
    }
    if (!borrowed_only) {
        if (const auto* bytes = value.bytes()) {
            return static_cast<uint64_t>(bytes->value.size());
        }
    }
    if (const auto* message = value.message()) {
        uint64_t total = 0;
        for (const auto& field : message->value->fields()) {
            total += CountBytesInValue(field.value(), borrowed_only);
        }
        return total;
    }
    if (const auto* vector = value.vector()) {
        uint64_t total = 0;
        for (const auto& element : vector->value->values()) {
            total += CountBytesInValue(element, borrowed_only);
        }
        return total;
    }
    return 0;
}

}  // namespace

Result<GraphOwnershipForwarder> GraphOwnershipForwarder::Create(
    schema::DynamicSchemaHandle root,
    std::span<const schema::DynamicSchemaHandle> descriptor_closure,
    GraphOwnershipForwardOptions options) noexcept {
    try {
        if (root == nullptr) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "graph ownership forwarder requires root");
        }
        std::vector<schema::DynamicSchemaHandle> closure(
            descriptor_closure.begin(), descriptor_closure.end());
        if (closure.empty()) {
            closure.push_back(root);
        }
        auto layout = schema::LayoutPlanner::Plan(**closure.begin(), closure);
        if (!layout.ok()) return layout.status();

        options.wire_limits.borrow_bytes_fields = true;
        auto codec = schema::PreparedCanonicalWireCodec::Create(
            root, closure, options.wire_limits);
        if (!codec.ok()) return codec.status();

        return GraphOwnershipForwarder(std::move(root), std::move(closure),
                                       std::move(*layout), std::move(*codec),
                                       std::move(options));
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

GraphOwnershipForwarder::GraphOwnershipForwarder(
    schema::DynamicSchemaHandle root,
    std::vector<schema::DynamicSchemaHandle> closure, schema::LayoutPlan layout,
    schema::PreparedCanonicalWireCodec codec,
    GraphOwnershipForwardOptions options) noexcept
    : root_(std::move(root)),
      closure_(std::move(closure)),
      layout_(std::move(layout)),
      codec_(std::move(codec)),
      options_(std::move(options)),
      decode_message_(options_.wire_limits.unknown_fields) {}

uint64_t GraphOwnershipForwarder::CountBytesPayload(
    const schema::DynamicMessage& message, bool borrowed_only) noexcept {
    uint64_t total = 0;
    for (const auto& field : message.fields()) {
        total += CountBytesInValue(field.value(), borrowed_only);
    }
    return total;
}

Status GraphOwnershipForwarder::EncodeFromGraph(
    ShmHandle root, const CentralSlabAllocator& allocator,
    ShmPinToken root_pin, std::vector<std::byte>& output) noexcept {
    try {
        schema::LayoutPlan layout = layout_;
        auto view = schema::DynamicView::Create(
            root_, std::move(layout), root, allocator, std::move(root_pin),
            closure_, options_.object_options);
        if (!view.ok()) return view.status();
        // Borrow SHM bytes; EncodeInto materializes once into `output`.
        auto message = view->ToDynamicMessage(/*borrow_bytes=*/true);
        if (!message.ok()) return message.status();
        return EncodeMessage(*message, output);
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Status GraphOwnershipForwarder::EncodeMessage(
    const schema::DynamicMessage& message,
    std::vector<std::byte>& output) noexcept {
    try {
        const uint64_t payload_bytes =
            CountBytesPayload(message, /*borrowed_only=*/false);
        const Status encoded =
            codec_.EncodeInto(message, encode_scratch_, output);
        if (!encoded.ok()) return encoded;
        ++census_.encode_calls;
        census_.encode_payload_bytes_copied += payload_bytes;
        ++census_.semantic_payload_assigns_avoided;
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<schema::PreparedDynamicObject>
GraphOwnershipForwarder::ReconstructToPrepared(
    std::span<const std::byte> wire_payload, CentralSlabAllocator& allocator,
    AllocationJournal& journal, TypeId type_id,
    const ProcessIdentity& owner) noexcept {
    try {
        const Status decoded = codec_.DecodeInto(
            wire_payload, decode_scratch_, decode_message_);
        if (!decoded.ok()) return decoded;

        const uint64_t payload_bytes =
            CountBytesPayload(decode_message_, /*borrowed_only=*/true);

        schema::LayoutPlan layout = layout_;
        auto builder = schema::DynamicBuilder::FromDynamicMessage(
            root_, std::move(layout), decode_message_, allocator, journal,
            type_id, closure_, owner, options_.object_options);
        if (!builder.ok()) return builder.status();
        auto prepared = builder->Prepare();
        if (!prepared.ok()) return prepared.status();

        ++census_.reconstruct_calls;
        census_.reconstruct_payload_bytes_copied += payload_bytes;
        ++census_.semantic_payload_assigns_avoided;
        return prepared;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<schema::DynamicObject> GraphOwnershipForwarder::ReconstructToObject(
    std::span<const std::byte> wire_payload, CentralSlabAllocator& allocator,
    AllocationJournal& journal, ShmPinTable& pins, TypeId type_id,
    const ProcessIdentity& owner) noexcept {
    try {
        const Status decoded = codec_.DecodeInto(
            wire_payload, decode_scratch_, decode_message_);
        if (!decoded.ok()) return decoded;

        const uint64_t payload_bytes =
            CountBytesPayload(decode_message_, /*borrowed_only=*/true);

        schema::LayoutPlan layout = layout_;
        auto builder = schema::DynamicBuilder::FromDynamicMessage(
            root_, std::move(layout), decode_message_, allocator, journal,
            type_id, closure_, owner, options_.object_options);
        if (!builder.ok()) return builder.status();
        auto object = builder->Commit(pins);
        if (!object.ok()) return object.status();

        ++census_.reconstruct_calls;
        census_.reconstruct_payload_bytes_copied += payload_bytes;
        ++census_.semantic_payload_assigns_avoided;
        return object;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<std::optional<RegisteredMemory>>
GraphOwnershipForwarder::TryRegisterPayload(void* address,
                                            uint64_t bytes) noexcept {
    ++census_.registration_attempts;
    MemoryRegistrationProvider* provider = options_.registration_provider;
    if (provider == nullptr) {
        provider = &UnavailableMemoryRegistrationProvider();
    }
    if (provider->provider_class() ==
            MemoryRegistrationProviderClass::kUnavailable ||
        !provider->Supports(options_.registration_kind) || address == nullptr ||
        bytes == 0) {
        return std::optional<RegisteredMemory>{};
    }
    MemoryRegistrationRequest request{
        .address = address,
        .bytes = bytes,
        .alignment = 1,
        .scope_id = options_.registration_scope_id,
        .kind = options_.registration_kind,
        .owner = options_.registration_owner,
        .require_physical_contiguous = false,
    };
    auto registered = provider->Register(request);
    if (!registered.ok()) return registered.status();
    ++census_.registration_successes;
    return std::optional<RegisteredMemory>{std::move(*registered)};
}

}  // namespace mino::bridge
