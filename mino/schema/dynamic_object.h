// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_SCHEMA_DYNAMIC_OBJECT_H_
#define MINO_SCHEMA_DYNAMIC_OBJECT_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>
#include <utility>

#include "mino/abi/shm_handle.h"
#include "mino/common/ids.h"
#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/platform/process_identity.h"
#include "mino/runtime/allocation_journal.h"
#include "mino/runtime/shm_shared_ptr.h"
#include "mino/schema/descriptor.h"
#include "mino/schema/dynamic_value.h"
#include "mino/schema/layout.h"
#include "mino/schema/object_graph_walker.h"
#include "mino/shm/allocator/central_slab.h"

namespace mino::schema {

using DynamicSchemaHandle = std::shared_ptr<const SchemaDescriptor>;

class FieldHandle {
public:
    static Result<FieldHandle> ByName(const SchemaDescriptor& descriptor,
                                      const LayoutPlan& layout,
                                      std::string_view name) noexcept;
    static Result<FieldHandle> ById(const SchemaDescriptor& descriptor,
                                    const LayoutPlan& layout,
                                    uint32_t field_id) noexcept;
    static Result<FieldHandle> ByIndex(const SchemaDescriptor& descriptor,
                                       const LayoutPlan& layout,
                                       size_t field_index) noexcept;

    const CanonicalDigest& schema_digest() const noexcept { return digest_; }
    uint64_t schema_short_id() const noexcept { return schema_short_id_; }
    uint32_t layout_version() const noexcept { return layout_version_; }
    uint32_t field_id() const noexcept { return field_id_; }
    size_t field_index() const noexcept { return field_index_; }

private:
    FieldHandle(CanonicalDigest digest, uint64_t schema_short_id,
                uint32_t layout_version, uint32_t field_id,
                size_t field_index) noexcept
        : digest_(digest),
          schema_short_id_(schema_short_id),
          layout_version_(layout_version),
          field_id_(field_id),
          field_index_(field_index) {}

    friend class DynamicBuilder;
    CanonicalDigest digest_{};
    uint64_t schema_short_id_ = 0;
    uint32_t layout_version_ = 0;
    uint32_t field_id_ = 0;
    size_t field_index_ = 0;
};

struct DynamicObjectOptions {
    ObjectGraphLimits graph_limits;
    UnknownFieldLimits unknown_fields;
};

class DynamicObject {
public:
    DynamicObject() noexcept = default;
    DynamicObject(DynamicObject&& other) noexcept;
    DynamicObject& operator=(DynamicObject&& other) noexcept;
    DynamicObject(const DynamicObject&) = delete;
    DynamicObject& operator=(const DynamicObject&) = delete;
    ~DynamicObject() noexcept;

    bool active() const noexcept { return active_; }
    ShmHandle root_handle() const noexcept { return root_; }

    // Establishes a real shared-memory Pin. The returned token is the only
    // lifetime capability accepted by DynamicView.
    Result<ShmPinToken> Pin(
        const ProcessIdentity& owner = ProcessIdentity::Current()) noexcept;

    // Retires the root through the Pin table, then reclaims the retained
    // COMMITTED graph only after all root Pins have released. Returns
    // kUnavailable while reclamation is safely deferred.
    Status Reclaim() noexcept;

private:
    friend class DynamicBuilder;
    DynamicObject(AllocationJournal* journal, ShmPinTable* pins,
                  AllocationTransaction transaction, ShmHandle root,
                  ShmPinContract contract) noexcept
        : journal_(journal),
          pins_(pins),
          transaction_(transaction),
          root_(root),
          contract_(contract),
          active_(true) {}
    void MoveFrom(DynamicObject& other) noexcept;

    AllocationJournal* journal_ = nullptr;
    ShmPinTable* pins_ = nullptr;
    AllocationTransaction transaction_;
    ShmHandle root_{};
    ShmPinContract contract_;
    bool active_ = false;
    bool retired_ = false;
};

// Transaction capability for Channel publication. Prepare() has validated and
// published the graph but intentionally retains BUILDING journal ownership.
class PreparedDynamicObject {
public:
    PreparedDynamicObject() noexcept = default;
    PreparedDynamicObject(PreparedDynamicObject&& other) noexcept;
    PreparedDynamicObject& operator=(PreparedDynamicObject&& other) noexcept;
    PreparedDynamicObject(const PreparedDynamicObject&) = delete;
    PreparedDynamicObject& operator=(const PreparedDynamicObject&) = delete;
    ~PreparedDynamicObject() noexcept;

    bool active() const noexcept { return active_; }
    bool journal_committed() const noexcept { return journal_committed_; }
    ShmHandle root_handle() const noexcept { return root_; }
    const AllocationTransaction& transaction() const noexcept {
        return transaction_;
    }

    // Call after the Channel reservation and sequence are known, but before the
    // Channel publication linearization point.
    Status CommitPublication(const PublicationBinding& binding) noexcept;
    // Call only after Channel visibility succeeds.
    Status FinalizeVisible() noexcept;
    // Call when reservation/Channel publication fails.
    Status Rollback() noexcept;

private:
    friend class DynamicBuilder;
    PreparedDynamicObject(AllocationJournal* journal,
                          AllocationTransaction transaction,
                          ShmHandle root) noexcept
        : journal_(journal), transaction_(transaction), root_(root), active_(true) {}
    void MoveFrom(PreparedDynamicObject& other) noexcept;
    void Disarm() noexcept;

    AllocationJournal* journal_ = nullptr;
    AllocationTransaction transaction_;
    ShmHandle root_{};
    bool active_ = false;
    bool journal_committed_ = false;
};

class DynamicBuilder {
public:
    static Result<DynamicBuilder> Create(
        DynamicSchemaHandle descriptor, LayoutPlan layout,
        CentralSlabAllocator& allocator, AllocationJournal& journal,
        TypeId type_id = {},
        std::span<const DynamicSchemaHandle> descriptors = {},
        const ProcessIdentity& owner = ProcessIdentity::Current(),
        const DynamicObjectOptions& options = {}) noexcept;

    static Result<DynamicBuilder> FromDynamicMessage(
        DynamicSchemaHandle descriptor, LayoutPlan layout,
        const DynamicMessage& message, CentralSlabAllocator& allocator,
        AllocationJournal& journal, TypeId type_id = {},
        std::span<const DynamicSchemaHandle> descriptors = {},
        const ProcessIdentity& owner = ProcessIdentity::Current(),
        const DynamicObjectOptions& options = {}) noexcept;

    DynamicBuilder(DynamicBuilder&& other) noexcept;
    DynamicBuilder& operator=(DynamicBuilder&& other) noexcept;
    DynamicBuilder(const DynamicBuilder&) = delete;
    DynamicBuilder& operator=(const DynamicBuilder&) = delete;
    ~DynamicBuilder() noexcept;

    ShmHandle root_handle() const noexcept;
    bool active() const noexcept;

    Status Set(const FieldHandle& field, const DynamicValue& value) noexcept;
    Status SetById(uint32_t field_id, const DynamicValue& value) noexcept;
    Status SetByIndex(size_t field_index,
                      const DynamicValue& value) noexcept;
    Status SetFromDynamicMessage(const DynamicMessage& message) noexcept;

    Status SetSigned(const FieldHandle& field, int64_t value) noexcept;
    Status SetUnsigned(const FieldHandle& field, uint64_t value) noexcept;
    Status SetBool(const FieldHandle& field, bool value) noexcept;
    Status SetFixed32(const FieldHandle& field, uint32_t value) noexcept;
    Status SetFixed64(const FieldHandle& field, uint64_t value) noexcept;
    Status SetFloat32Bits(const FieldHandle& field, uint32_t bits) noexcept;
    Status SetFloat64Bits(const FieldHandle& field, uint64_t bits) noexcept;
    Status SetString(const FieldHandle& field, std::string_view value) noexcept;
    Status SetBytes(const FieldHandle& field,
                    std::span<const std::byte> value) noexcept;
    Status SetVector(const FieldHandle& field,
                     const DynamicVector& value) noexcept;
    Status SetNested(const FieldHandle& field,
                     const DynamicMessage& value) noexcept;

    Result<PreparedDynamicObject> Prepare() noexcept;
    Result<DynamicObject> Commit(ShmPinTable& pins) noexcept;
    Status Abort() noexcept;

private:
    struct Impl;
    explicit DynamicBuilder(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

class DynamicVectorView;

class DynamicView {
public:
    struct Context;

    static Result<DynamicView> Create(
        DynamicSchemaHandle descriptor, LayoutPlan layout, ShmHandle root,
        const CentralSlabAllocator& allocator, ShmPinToken root_pin,
        std::span<const DynamicSchemaHandle> descriptors = {},
        const DynamicObjectOptions& options = {}) noexcept;

    ShmHandle root_handle() const noexcept { return root_; }
    const SchemaDescriptor& descriptor() const noexcept;
    const LayoutPlan& layout() const noexcept;

    Result<bool> Has(const FieldHandle& field) const noexcept;
    Result<bool> HasById(uint32_t field_id) const noexcept;
    Result<bool> HasByIndex(size_t field_index) const noexcept;
    Result<int64_t> GetSigned(const FieldHandle& field) const noexcept;
    Result<uint64_t> GetUnsigned(const FieldHandle& field) const noexcept;
    Result<bool> GetBool(const FieldHandle& field) const noexcept;
    Result<uint32_t> GetFloat32Bits(const FieldHandle& field) const noexcept;
    Result<uint64_t> GetFloat64Bits(const FieldHandle& field) const noexcept;
    Result<std::string_view> GetString(const FieldHandle& field) const noexcept;
    Result<std::span<const std::byte>> GetBytes(
        const FieldHandle& field) const noexcept;
    Result<DynamicView> GetNested(const FieldHandle& field) const noexcept;
    Result<DynamicVectorView> GetVector(
        const FieldHandle& field) const noexcept;
    Result<DynamicMessage> ToDynamicMessage() const noexcept;

    const std::shared_ptr<const Context>& context_for_internal() const noexcept {
        return context_;
    }
    ShmHandle storage_handle_for_internal() const noexcept {
        return storage_handle_;
    }
    size_t storage_offset_for_internal() const noexcept {
        return storage_offset_;
    }

private:
    DynamicView(std::shared_ptr<const Context> context,
                const SchemaDescriptor* descriptor, const LayoutPlan* layout,
                ShmHandle root, ShmHandle storage_handle = {},
                size_t storage_offset = 0) noexcept
        : context_(std::move(context)),
          descriptor_(descriptor),
          layout_(layout),
          root_(root),
          storage_handle_(storage_handle.IsNull() ? root : storage_handle),
          storage_offset_(storage_offset) {}

    friend class DynamicVectorView;
    std::shared_ptr<const Context> context_;
    const SchemaDescriptor* descriptor_ = nullptr;
    const LayoutPlan* layout_ = nullptr;
    ShmHandle root_{};
    ShmHandle storage_handle_{};
    size_t storage_offset_ = 0;
};

class DynamicVectorView {
public:
    size_t size() const noexcept { return size_; }
    Result<int64_t> GetSigned(size_t index) const noexcept;
    Result<uint64_t> GetUnsigned(size_t index) const noexcept;
    Result<bool> GetBool(size_t index) const noexcept;
    Result<uint32_t> GetFloat32Bits(size_t index) const noexcept;
    Result<uint64_t> GetFloat64Bits(size_t index) const noexcept;
    Result<std::string_view> GetString(size_t index) const noexcept;
    Result<std::span<const std::byte>> GetBytes(size_t index) const noexcept;
    Result<DynamicView> GetNested(size_t index) const noexcept;
    Result<DynamicVectorView> GetVector(size_t index) const noexcept;
    Result<DynamicVector> ToDynamicVector() const noexcept;

private:
    struct ElementAccess {
        const std::byte* data = nullptr;
        ShmHandle storage_handle{};
        size_t storage_offset = 0;
    };
    Result<ElementAccess> ElementData(size_t index) const noexcept;
    friend class DynamicView;
    DynamicVectorView(std::shared_ptr<const DynamicView::Context> context,
                      const TypeDescriptor* element_type,
                      const ConstraintSet* constraints,
                      ShmHandle metadata_handle, size_t metadata_offset,
                      size_t size, size_t element_size) noexcept
        : context_(std::move(context)),
          element_type_(element_type),
          constraints_(constraints),
          metadata_handle_(metadata_handle),
          metadata_offset_(metadata_offset),
          size_(size),
          element_size_(element_size) {}

    std::shared_ptr<const DynamicView::Context> context_;
    const TypeDescriptor* element_type_ = nullptr;
    const ConstraintSet* constraints_ = nullptr;
    ShmHandle metadata_handle_{};
    size_t metadata_offset_ = 0;
    size_t size_ = 0;
    size_t element_size_ = 0;
};

}  // namespace mino::schema

#endif  // MINO_SCHEMA_DYNAMIC_OBJECT_H_
