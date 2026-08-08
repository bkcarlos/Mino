// Copyright 2026 The Mino Authors

#include "mino/capacity/capacity.h"

#include <limits>
#include <mutex>
#include <new>
#include <sstream>
#include <string>
#include <utility>

#include "mino/common/checked_arithmetic.h"

namespace mino::capacity {
namespace detail {

struct CapacityState {
    explicit CapacityState(NodeBudget configured) noexcept
        : budget(std::move(configured)) {}

    mutable std::mutex mutex;
    NodeBudget budget;
    ResourceVector committed;
    ResourceVector pending;
    uint64_t sequence = 0;
    uint64_t accepted_reservations = 0;
    uint64_t rejected_reservations = 0;
};

}  // namespace detail
namespace {

void IncrementSaturating(uint64_t* value) noexcept {
    if (*value != std::numeric_limits<uint64_t>::max()) ++*value;
}

template <typename Visitor>
bool VisitResources(const ResourceVector& value, Visitor&& visitor) {
    if (!visitor(ResourceDimension::kShmBytes, 0, value.shm_bytes)) return false;
    for (size_t index = 0; index < value.slab_bytes.size(); ++index) {
        if (!visitor(ResourceDimension::kSlabBytes,
                     static_cast<uint16_t>(index), value.slab_bytes[index])) {
            return false;
        }
    }
    if (!visitor(ResourceDimension::kTopics, 0, value.topics)) return false;
    if (!visitor(ResourceDimension::kPublishers, 0, value.publishers)) return false;
    if (!visitor(ResourceDimension::kSubscribers, 0, value.subscribers)) return false;
    if (!visitor(ResourceDimension::kBridgeConnections, 0,
                 value.bridge_connections)) {
        return false;
    }
    if (!visitor(ResourceDimension::kBridgeEgressBytes, 0,
                 value.bridge_egress_bytes)) {
        return false;
    }
    if (!visitor(ResourceDimension::kSchemaBufferBytes, 0,
                 value.schema_buffer_bytes)) {
        return false;
    }
    if (!visitor(ResourceDimension::kRecorderBufferBytes, 0,
                 value.recorder_buffer_bytes)) {
        return false;
    }
    if (!visitor(ResourceDimension::kFileDescriptors, 0,
                 value.file_descriptors)) {
        return false;
    }
    return visitor(ResourceDimension::kThreads, 0, value.threads);
}

uint64_t ValueAt(const ResourceVector& value, ResourceDimension dimension,
                 uint16_t slab_class) noexcept {
    switch (dimension) {
        case ResourceDimension::kShmBytes:
            return value.shm_bytes;
        case ResourceDimension::kSlabBytes:
            return value.slab_bytes[slab_class];
        case ResourceDimension::kTopics:
            return value.topics;
        case ResourceDimension::kPublishers:
            return value.publishers;
        case ResourceDimension::kSubscribers:
            return value.subscribers;
        case ResourceDimension::kBridgeConnections:
            return value.bridge_connections;
        case ResourceDimension::kBridgeEgressBytes:
            return value.bridge_egress_bytes;
        case ResourceDimension::kSchemaBufferBytes:
            return value.schema_buffer_bytes;
        case ResourceDimension::kRecorderBufferBytes:
            return value.recorder_buffer_bytes;
        case ResourceDimension::kFileDescriptors:
            return value.file_descriptors;
        case ResourceDimension::kThreads:
            return value.threads;
    }
    return 0;
}

uint64_t* MutableValueAt(ResourceVector* value, ResourceDimension dimension,
                         uint16_t slab_class) noexcept {
    switch (dimension) {
        case ResourceDimension::kShmBytes:
            return &value->shm_bytes;
        case ResourceDimension::kSlabBytes:
            return &value->slab_bytes[slab_class];
        case ResourceDimension::kTopics:
            return &value->topics;
        case ResourceDimension::kPublishers:
            return &value->publishers;
        case ResourceDimension::kSubscribers:
            return &value->subscribers;
        case ResourceDimension::kBridgeConnections:
            return &value->bridge_connections;
        case ResourceDimension::kBridgeEgressBytes:
            return &value->bridge_egress_bytes;
        case ResourceDimension::kSchemaBufferBytes:
            return &value->schema_buffer_bytes;
        case ResourceDimension::kRecorderBufferBytes:
            return &value->recorder_buffer_bytes;
        case ResourceDimension::kFileDescriptors:
            return &value->file_descriptors;
        case ResourceDimension::kThreads:
            return &value->threads;
    }
    return nullptr;
}

bool ValidAdmissionClass(AdmissionClass value) noexcept {
    return value == AdmissionClass::kDataPlane ||
           value == AdmissionClass::kControlPlane;
}

bool ValidScope(ResourceScope value) noexcept {
    switch (value) {
        case ResourceScope::kTopic:
        case ResourceScope::kPublisher:
        case ResourceScope::kSubscriber:
        case ResourceScope::kBridge:
        case ResourceScope::kSchema:
        case ResourceScope::kRecorder:
        case ResourceScope::kOther:
            return true;
    }
    return false;
}

bool CanSubtract(const ResourceVector& from,
                 const ResourceVector& amount) noexcept {
    return VisitResources(amount, [&](ResourceDimension dimension,
                                      uint16_t slab_class, uint64_t requested) {
        return requested <= ValueAt(from, dimension, slab_class);
    });
}

void AddUnchecked(ResourceVector* target,
                  const ResourceVector& amount) noexcept {
    VisitResources(amount, [&](ResourceDimension dimension,
                               uint16_t slab_class, uint64_t increment) {
        *MutableValueAt(target, dimension, slab_class) += increment;
        return true;
    });
}

void SubtractUnchecked(ResourceVector* target,
                       const ResourceVector& amount) noexcept {
    VisitResources(amount, [&](ResourceDimension dimension,
                               uint16_t slab_class, uint64_t decrement) {
        *MutableValueAt(target, dimension, slab_class) -= decrement;
        return true;
    });
}

ResourceVector ComputeHeadroom(const NodeBudget& budget,
                               const ResourceVector& committed,
                               const ResourceVector& pending,
                               AdmissionClass admission_class) noexcept {
    ResourceVector result;
    VisitResources(budget.limit,
                   [&](ResourceDimension dimension, uint16_t slab_class,
                       uint64_t limit) {
        const uint64_t reserve = admission_class == AdmissionClass::kDataPlane
                                     ? ValueAt(budget.emergency_reserve,
                                               dimension, slab_class)
                                     : 0;
        const uint64_t hard_headroom = limit - reserve;
        const uint64_t used = ValueAt(committed, dimension, slab_class) +
                              ValueAt(pending, dimension, slab_class);
        *MutableValueAt(&result, dimension, slab_class) =
            used >= hard_headroom ? 0 : hard_headroom - used;
        return true;
    });
    return result;
}

Status Invalid(std::string_view message) noexcept {
    try {
        return Status::Error(StatusCode::kInvalidArgument, message);
    } catch (...) {
        return Status::Error(StatusCode::kInvalidArgument);
    }
}

Status Exhausted(const AdmissionRejection& rejection,
                 std::string_view request_name) noexcept {
    try {
        std::string message = "capacity admission rejected";
        if (!request_name.empty()) {
            message += " for ";
            message.append(request_name);
        }
        message += ": ";
        message.append(ResourceDimensionName(rejection.dimension));
        if (rejection.dimension == ResourceDimension::kSlabBytes) {
            message += "[" + std::to_string(rejection.slab_class) + "]";
        }
        message += " requested=" + std::to_string(rejection.requested) +
                   " available=" + std::to_string(rejection.available);
        return Status::Error(StatusCode::kResourceExhausted, message);
    } catch (...) {
        return Status::Error(StatusCode::kResourceExhausted);
    }
}

void AppendResourceJson(std::ostringstream& output,
                        const ResourceVector& value) {
    output << "{\"shm_bytes\":" << value.shm_bytes << ",\"slab_bytes\":[";
    for (size_t index = 0; index < value.slab_bytes.size(); ++index) {
        if (index != 0) output << ',';
        output << value.slab_bytes[index];
    }
    output << "],\"topics\":" << value.topics
           << ",\"publishers\":" << value.publishers
           << ",\"subscribers\":" << value.subscribers
           << ",\"bridge_connections\":" << value.bridge_connections
           << ",\"bridge_egress_bytes\":" << value.bridge_egress_bytes
           << ",\"schema_buffer_bytes\":" << value.schema_buffer_bytes
           << ",\"recorder_buffer_bytes\":" << value.recorder_buffer_bytes
           << ",\"file_descriptors\":" << value.file_descriptors
           << ",\"threads\":" << value.threads << '}';
}

}  // namespace

bool ResourceVector::empty() const noexcept {
    return VisitResources(*this, [](ResourceDimension, uint16_t, uint64_t value) {
        return value == 0;
    });
}

Status ValidateNodeBudget(const NodeBudget& budget) noexcept {
    const bool valid = VisitResources(
        budget.emergency_reserve,
        [&](ResourceDimension dimension, uint16_t slab_class, uint64_t reserve) {
            return reserve <= ValueAt(budget.limit, dimension, slab_class);
        });
    return valid ? Status::Ok()
                 : Invalid("capacity emergency reserve exceeds node limit");
}

Result<ResourceVector> CheckedAdd(const ResourceVector& lhs,
                                  const ResourceVector& rhs) noexcept {
    ResourceVector result = lhs;
    bool valid = VisitResources(
        rhs, [&](ResourceDimension dimension, uint16_t slab_class,
                 uint64_t increment) {
            uint64_t sum = 0;
            uint64_t* target = MutableValueAt(&result, dimension, slab_class);
            if (!CheckedAddU64(*target, increment, &sum)) return false;
            *target = sum;
            return true;
        });
    if (!valid) return Invalid("capacity resource addition overflows");
    return result;
}

Result<ResourceVector> CheckedScale(const ResourceVector& value,
                                    uint64_t factor) noexcept {
    ResourceVector result;
    bool valid = VisitResources(
        value, [&](ResourceDimension dimension, uint16_t slab_class,
                   uint64_t current) {
            uint64_t product = 0;
            if (!CheckedMulU64(current, factor, &product)) return false;
            *MutableValueAt(&result, dimension, slab_class) = product;
            return true;
        });
    if (!valid) return Invalid("capacity resource multiplication overflows");
    return result;
}

CapacityLease::CapacityLease(std::shared_ptr<detail::CapacityState> state,
                             ResourceVector resources) noexcept
    : state_(std::move(state)), resources_(resources), active_(state_ != nullptr) {}

CapacityLease::~CapacityLease() { Reset(); }

CapacityLease::CapacityLease(CapacityLease&& other) noexcept
    : state_(std::move(other.state_)),
      resources_(other.resources_),
      active_(other.active_) {
    other.resources_ = {};
    other.active_ = false;
}

CapacityLease& CapacityLease::operator=(CapacityLease&& other) noexcept {
    if (this == &other) return *this;
    Reset();
    state_ = std::move(other.state_);
    resources_ = other.resources_;
    active_ = other.active_;
    other.resources_ = {};
    other.active_ = false;
    return *this;
}

void CapacityLease::Reset() noexcept {
    if (!active_ || state_ == nullptr) return;
    std::lock_guard lock(state_->mutex);
    if (CanSubtract(state_->committed, resources_)) {
        SubtractUnchecked(&state_->committed, resources_);
        IncrementSaturating(&state_->sequence);
    }
    active_ = false;
    resources_ = {};
    state_.reset();
}

CapacityReservation::CapacityReservation(
    std::shared_ptr<detail::CapacityState> state,
    ResourceVector resources) noexcept
    : state_(std::move(state)), resources_(resources), pending_(state_ != nullptr) {}

CapacityReservation::~CapacityReservation() { static_cast<void>(Rollback()); }

CapacityReservation::CapacityReservation(CapacityReservation&& other) noexcept
    : state_(std::move(other.state_)),
      resources_(other.resources_),
      pending_(other.pending_) {
    other.resources_ = {};
    other.pending_ = false;
}

CapacityReservation& CapacityReservation::operator=(
    CapacityReservation&& other) noexcept {
    if (this == &other) return *this;
    static_cast<void>(Rollback());
    state_ = std::move(other.state_);
    resources_ = other.resources_;
    pending_ = other.pending_;
    other.resources_ = {};
    other.pending_ = false;
    return *this;
}

Result<CapacityLease> CapacityReservation::Commit() noexcept {
    if (state_ == nullptr && !pending_) return CapacityLease{};
    if (state_ == nullptr || !pending_) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "capacity reservation is not pending");
    }
    std::lock_guard lock(state_->mutex);
    if (!CanSubtract(state_->pending, resources_)) {
        return Status::Error(StatusCode::kInternal,
                             "capacity pending accounting is inconsistent");
    }
    SubtractUnchecked(&state_->pending, resources_);
    AddUnchecked(&state_->committed, resources_);
    IncrementSaturating(&state_->sequence);
    pending_ = false;
    auto state = std::move(state_);
    ResourceVector resources = resources_;
    resources_ = {};
    return CapacityLease(std::move(state), resources);
}

Status CapacityReservation::Rollback() noexcept {
    if (!pending_) return Status::Ok();
    if (state_ == nullptr) {
        pending_ = false;
        resources_ = {};
        return Status::Error(StatusCode::kInternal,
                             "capacity reservation lost its controller state");
    }
    std::lock_guard lock(state_->mutex);
    if (!CanSubtract(state_->pending, resources_)) {
        return Status::Error(StatusCode::kInternal,
                             "capacity pending accounting is inconsistent");
    }
    SubtractUnchecked(&state_->pending, resources_);
    IncrementSaturating(&state_->sequence);
    pending_ = false;
    resources_ = {};
    state_.reset();
    return Status::Ok();
}

Result<std::shared_ptr<CapacityController>> CapacityController::Create(
    NodeBudget budget) noexcept {
    MINO_RETURN_IF_ERROR(ValidateNodeBudget(budget));
    try {
        auto state = std::make_shared<detail::CapacityState>(std::move(budget));
        return std::shared_ptr<CapacityController>(
            new CapacityController(std::move(state)));
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "cannot allocate capacity controller");
    } catch (...) {
        return Status::Error(StatusCode::kInternal,
                             "cannot create capacity controller");
    }
}

CapacityController::CapacityController(
    std::shared_ptr<detail::CapacityState> state) noexcept
    : state_(std::move(state)) {}

Result<CapacityReservation> CapacityController::Reserve(
    const ResourceRequest& request, AdmissionRejection* rejection) noexcept {
    if (rejection != nullptr) *rejection = {};
    if (!ValidAdmissionClass(request.admission_class) ||
        !ValidScope(request.scope)) {
        return Invalid("capacity request has an invalid class or scope");
    }
    if (request.resources.empty()) {
        return Invalid("capacity request must contain at least one resource");
    }

    std::lock_guard lock(state_->mutex);
    AdmissionRejection denied;
    bool admitted = VisitResources(
        request.resources,
        [&](ResourceDimension dimension, uint16_t slab_class,
            uint64_t requested) {
            if (requested == 0) return true;
            const uint64_t limit =
                ValueAt(state_->budget.limit, dimension, slab_class);
            const uint64_t reserve =
                ValueAt(state_->budget.emergency_reserve, dimension, slab_class);
            const uint64_t ceiling =
                request.admission_class == AdmissionClass::kDataPlane
                    ? limit - reserve
                    : limit;
            const uint64_t committed =
                ValueAt(state_->committed, dimension, slab_class);
            const uint64_t pending =
                ValueAt(state_->pending, dimension, slab_class);
            const uint64_t available =
                committed >= ceiling || pending >= ceiling - committed
                    ? 0
                    : ceiling - committed - pending;
            if (requested <= available) return true;
            denied = AdmissionRejection{
                .dimension = dimension,
                .slab_class = slab_class,
                .scope = request.scope,
                .admission_class = request.admission_class,
                .requested = requested,
                .available = available,
                .limit = limit,
                .emergency_reserve = reserve,
                .committed = committed,
                .pending = pending,
            };
            return false;
        });
    if (!admitted) {
        IncrementSaturating(&state_->rejected_reservations);
        IncrementSaturating(&state_->sequence);
        if (rejection != nullptr) *rejection = denied;
        return Exhausted(denied, request.name);
    }
    AddUnchecked(&state_->pending, request.resources);
    IncrementSaturating(&state_->accepted_reservations);
    IncrementSaturating(&state_->sequence);
    return CapacityReservation(state_, request.resources);
}

CapacitySnapshot CapacityController::Snapshot() const noexcept {
    std::lock_guard lock(state_->mutex);
    return CapacitySnapshot{
        .budget = state_->budget,
        .committed = state_->committed,
        .pending = state_->pending,
        .data_plane_headroom =
            ComputeHeadroom(state_->budget, state_->committed, state_->pending,
                            AdmissionClass::kDataPlane),
        .control_plane_headroom =
            ComputeHeadroom(state_->budget, state_->committed, state_->pending,
                            AdmissionClass::kControlPlane),
        .sequence = state_->sequence,
        .accepted_reservations = state_->accepted_reservations,
        .rejected_reservations = state_->rejected_reservations,
    };
}

std::string_view ResourceDimensionName(ResourceDimension dimension) noexcept {
    switch (dimension) {
        case ResourceDimension::kShmBytes:
            return "shm_bytes";
        case ResourceDimension::kSlabBytes:
            return "slab_bytes";
        case ResourceDimension::kTopics:
            return "topics";
        case ResourceDimension::kPublishers:
            return "publishers";
        case ResourceDimension::kSubscribers:
            return "subscribers";
        case ResourceDimension::kBridgeConnections:
            return "bridge_connections";
        case ResourceDimension::kBridgeEgressBytes:
            return "bridge_egress_bytes";
        case ResourceDimension::kSchemaBufferBytes:
            return "schema_buffer_bytes";
        case ResourceDimension::kRecorderBufferBytes:
            return "recorder_buffer_bytes";
        case ResourceDimension::kFileDescriptors:
            return "file_descriptors";
        case ResourceDimension::kThreads:
            return "threads";
    }
    return "unknown";
}

std::string_view ResourceScopeName(ResourceScope scope) noexcept {
    switch (scope) {
        case ResourceScope::kTopic:
            return "topic";
        case ResourceScope::kPublisher:
            return "publisher";
        case ResourceScope::kSubscriber:
            return "subscriber";
        case ResourceScope::kBridge:
            return "bridge";
        case ResourceScope::kSchema:
            return "schema";
        case ResourceScope::kRecorder:
            return "recorder";
        case ResourceScope::kOther:
            return "other";
    }
    return "unknown";
}

Result<std::string> CapacitySnapshotToJson(
    const CapacitySnapshot& snapshot) noexcept {
    try {
        std::ostringstream output;
        output << "{\"sequence\":" << snapshot.sequence
               << ",\"accepted_reservations\":"
               << snapshot.accepted_reservations
               << ",\"rejected_reservations\":"
               << snapshot.rejected_reservations << ",\"budget\":{\"limit\":";
        AppendResourceJson(output, snapshot.budget.limit);
        output << ",\"emergency_reserve\":";
        AppendResourceJson(output, snapshot.budget.emergency_reserve);
        output << "},\"committed\":";
        AppendResourceJson(output, snapshot.committed);
        output << ",\"pending\":";
        AppendResourceJson(output, snapshot.pending);
        output << ",\"data_plane_headroom\":";
        AppendResourceJson(output, snapshot.data_plane_headroom);
        output << ",\"control_plane_headroom\":";
        AppendResourceJson(output, snapshot.control_plane_headroom);
        output << '}';
        return output.str();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "cannot allocate capacity JSON report");
    } catch (...) {
        return Status::Error(StatusCode::kInternal,
                             "cannot encode capacity JSON report");
    }
}

Result<std::string> AdmissionRejectionToJson(
    const AdmissionRejection& rejection) noexcept {
    try {
        std::ostringstream output;
        output << "{\"dimension\":\""
               << ResourceDimensionName(rejection.dimension)
               << "\",\"slab_class\":" << rejection.slab_class
               << ",\"scope\":\"" << ResourceScopeName(rejection.scope)
               << "\",\"admission_class\":\""
               << (rejection.admission_class == AdmissionClass::kControlPlane
                       ? "control_plane"
                       : "data_plane")
               << "\",\"requested\":" << rejection.requested
               << ",\"available\":" << rejection.available
               << ",\"limit\":" << rejection.limit
               << ",\"emergency_reserve\":" << rejection.emergency_reserve
               << ",\"committed\":" << rejection.committed
               << ",\"pending\":" << rejection.pending << '}';
        return output.str();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "cannot allocate rejection JSON report");
    } catch (...) {
        return Status::Error(StatusCode::kInternal,
                             "cannot encode rejection JSON report");
    }
}

}  // namespace mino::capacity
