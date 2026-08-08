// Copyright 2026 The Mino Authors
//
// Node-wide resource admission control.

#ifndef MINO_CAPACITY_CAPACITY_H_
#define MINO_CAPACITY_CAPACITY_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "mino/common/result.h"
#include "mino/common/status.h"

namespace mino::capacity {

// Mirrors the allocator's fixed ABI ceiling without making Capacity depend on
// a concrete SHM allocator implementation. Class IDs index this array directly.
inline constexpr size_t kMaxSlabClasses = 256;

// Fixed, component-wise node resource accounting. Values are capacities, not
// current process observations; every field is admitted atomically as one unit.
struct ResourceVector {
    uint64_t shm_bytes = 0;
    std::array<uint64_t, kMaxSlabClasses> slab_bytes{};
    uint64_t topics = 0;
    uint64_t publishers = 0;
    uint64_t subscribers = 0;
    uint64_t bridge_connections = 0;
    uint64_t bridge_egress_bytes = 0;
    uint64_t schema_buffer_bytes = 0;
    uint64_t recorder_buffer_bytes = 0;
    uint64_t file_descriptors = 0;
    uint64_t threads = 0;

    bool empty() const noexcept;
    friend bool operator==(const ResourceVector&, const ResourceVector&) = default;
};

struct NodeBudget {
    ResourceVector limit;
    // Data-plane requests cannot consume this reserve. Control-plane requests
    // may consume it but are still bounded by limit.
    ResourceVector emergency_reserve;
};

enum class AdmissionClass : uint8_t {
    kDataPlane = 0,
    kControlPlane = 1,
};

enum class ResourceScope : uint8_t {
    kTopic = 0,
    kPublisher = 1,
    kSubscriber = 2,
    kBridge = 3,
    kSchema = 4,
    kRecorder = 5,
    kOther = 6,
};

enum class ResourceDimension : uint8_t {
    kShmBytes = 0,
    kSlabBytes = 1,
    kTopics = 2,
    kPublishers = 3,
    kSubscribers = 4,
    kBridgeConnections = 5,
    kBridgeEgressBytes = 6,
    kSchemaBufferBytes = 7,
    kRecorderBufferBytes = 8,
    kFileDescriptors = 9,
    kThreads = 10,
};

struct ResourceRequest {
    ResourceVector resources;
    ResourceScope scope = ResourceScope::kOther;
    AdmissionClass admission_class = AdmissionClass::kDataPlane;
    // Used only in diagnostics and is never retained by the controller.
    std::string_view name;
};

// Typed refusal detail. slab_class is meaningful only for kSlabBytes.
struct AdmissionRejection {
    ResourceDimension dimension = ResourceDimension::kShmBytes;
    uint16_t slab_class = 0;
    ResourceScope scope = ResourceScope::kOther;
    AdmissionClass admission_class = AdmissionClass::kDataPlane;
    uint64_t requested = 0;
    uint64_t available = 0;
    uint64_t limit = 0;
    uint64_t emergency_reserve = 0;
    uint64_t committed = 0;
    uint64_t pending = 0;
};

struct CapacitySnapshot {
    NodeBudget budget;
    ResourceVector committed;
    ResourceVector pending;
    ResourceVector data_plane_headroom;
    ResourceVector control_plane_headroom;
    uint64_t sequence = 0;
    uint64_t accepted_reservations = 0;
    uint64_t rejected_reservations = 0;
};

namespace detail {
struct CapacityState;
}  // namespace detail

// A committed charge. Destruction releases all dimensions atomically.
class CapacityLease final {
public:
    CapacityLease() noexcept = default;
    ~CapacityLease();
    CapacityLease(const CapacityLease&) = delete;
    CapacityLease& operator=(const CapacityLease&) = delete;
    CapacityLease(CapacityLease&& other) noexcept;
    CapacityLease& operator=(CapacityLease&& other) noexcept;

    bool valid() const noexcept { return active_; }
    const ResourceVector& resources() const noexcept { return resources_; }
    void Reset() noexcept;

private:
    friend class CapacityReservation;
    CapacityLease(std::shared_ptr<detail::CapacityState> state,
                  ResourceVector resources) noexcept;

    std::shared_ptr<detail::CapacityState> state_;
    ResourceVector resources_{};
    bool active_ = false;
};

// A pending all-or-nothing reservation. Destruction rolls back unless Commit()
// succeeds. A default reservation is a valid no-op for optional integrations.
class CapacityReservation final {
public:
    CapacityReservation() noexcept = default;
    ~CapacityReservation();
    CapacityReservation(const CapacityReservation&) = delete;
    CapacityReservation& operator=(const CapacityReservation&) = delete;
    CapacityReservation(CapacityReservation&& other) noexcept;
    CapacityReservation& operator=(CapacityReservation&& other) noexcept;

    bool pending() const noexcept { return pending_; }
    const ResourceVector& resources() const noexcept { return resources_; }
    Result<CapacityLease> Commit() noexcept;
    Status Rollback() noexcept;

private:
    friend class CapacityController;
    CapacityReservation(std::shared_ptr<detail::CapacityState> state,
                        ResourceVector resources) noexcept;

    std::shared_ptr<detail::CapacityState> state_;
    ResourceVector resources_{};
    bool pending_ = false;
};

class CapacityController final {
public:
    static Result<std::shared_ptr<CapacityController>> Create(
        NodeBudget budget) noexcept;

    CapacityController(const CapacityController&) = delete;
    CapacityController& operator=(const CapacityController&) = delete;

    // On resource exhaustion, rejection receives stable programmatic detail and
    // the returned Status contains a human-readable dimension/reason.
    Result<CapacityReservation> Reserve(
        const ResourceRequest& request,
        AdmissionRejection* rejection = nullptr) noexcept;
    CapacitySnapshot Snapshot() const noexcept;

private:
    explicit CapacityController(
        std::shared_ptr<detail::CapacityState> state) noexcept;
    std::shared_ptr<detail::CapacityState> state_;
};

Status ValidateNodeBudget(const NodeBudget& budget) noexcept;
Result<ResourceVector> CheckedAdd(const ResourceVector& lhs,
                                  const ResourceVector& rhs) noexcept;
Result<ResourceVector> CheckedScale(const ResourceVector& value,
                                    uint64_t factor) noexcept;
std::string_view ResourceDimensionName(ResourceDimension dimension) noexcept;
std::string_view ResourceScopeName(ResourceScope scope) noexcept;
Result<std::string> CapacitySnapshotToJson(
    const CapacitySnapshot& snapshot) noexcept;
Result<std::string> AdmissionRejectionToJson(
    const AdmissionRejection& rejection) noexcept;

}  // namespace mino::capacity

#endif  // MINO_CAPACITY_CAPACITY_H_
