// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.gnu.org/licenses/lgpl-3.0.txt
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

#include "mino/shm/region/region.h"

#include <atomic>
#include <cerrno>
#include <new>
#include <random>
#include <utility>

#if defined(__unix__) || defined(__APPLE__)
#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#include "mino/common/checked_arithmetic.h"
#include "mino/shm/region/recovery.h"

namespace mino {
namespace {

// Name of the durable high-water mark segment used to allocate persistent,
// never-reused region_ids (design doc section 13.10). A full Authoritative
// Registry service replaces this in a later milestone; for D1 the HWM is a
// single atomic counter in a well-known POSIX shm object.
constexpr const char* kRegionIdHwmName = "/mino_region_id_hwm";

struct RegionIdHwm {
    // Plain integer accessed via std::atomic_ref (consistent with SuperBlock).
    uint32_t next_id;
};

// Allocates the next persistent region_id via the durable high-water mark.
// The new high-water mark is stored (fetch_add) before the id is returned, so
// ids are never reused even across crashes (section 13.10).
Result<uint32_t> AllocateRegionId() {
    // Open-or-create the HWM segment. We intentionally do not use O_EXCL here.
    int fd = -1;
    bool created = false;
#if defined(__unix__) || defined(__APPLE__)
    fd = ::shm_open(kRegionIdHwmName, O_RDWR | O_CREAT, 0600);
    if (fd < 0) {
        return Status::Error(StatusCode::kInternal,
                             "failed to open region id high-water mark");
    }
    struct stat st_buf;
    if (::fstat(fd, &st_buf) != 0) {
        ::close(fd);
        return Status::Error(StatusCode::kInternal, "fstat(hwm) failed");
    }
    if (st_buf.st_size < static_cast<off_t>(sizeof(RegionIdHwm))) {
        if (::ftruncate(fd, sizeof(RegionIdHwm)) != 0) {
            ::close(fd);
            return Status::Error(StatusCode::kInternal, "ftruncate(hwm) failed");
        }
        created = true;
    }
    void* p = ::mmap(nullptr, sizeof(RegionIdHwm), PROT_READ | PROT_WRITE,
                     MAP_SHARED, fd, 0);
    ::close(fd);
    if (p == MAP_FAILED) {
        return Status::Error(StatusCode::kInternal, "mmap(hwm) failed");
    }
    auto* hwm = static_cast<RegionIdHwm*>(p);
    if (created) {
        // Construct the atomic in place and start ids at 1 (0 is reserved as
        // "unset" in RegionAttachOptions).
        std::atomic_ref(hwm->next_id).store(1, std::memory_order_relaxed);
    }
    const uint32_t id =
        std::atomic_ref(hwm->next_id).fetch_add(1, std::memory_order_acq_rel);
    ::munmap(p, sizeof(RegionIdHwm));
    return id;
#else
    (void)fd;
    (void)created;
    return Status::Error(StatusCode::kUnsupported,
                         "region id allocation unsupported on this platform");
#endif
}

// Generates a 128-bit region UUID from a secure random source (design doc 6.4).
void GenerateRegionUuid(uint64_t* lo, uint64_t* hi) {
    std::random_device rd;
    const uint64_t a = (static_cast<uint64_t>(rd()) << 32) | rd();
    const uint64_t b = (static_cast<uint64_t>(rd()) << 32) | rd();
    *lo = a;
    *hi = b;
}

uint32_t HostPageSize() {
#if defined(__unix__) || defined(__APPLE__)
    const long p = ::sysconf(_SC_PAGESIZE);
    return p > 0 ? static_cast<uint32_t>(p) : kDefaultPageSize;
#else
    return kDefaultPageSize;
#endif
}

Result<int> TryAcquireSupervisorLock(const std::string& name) {
#if defined(__unix__) || defined(__APPLE__)
    const int fd = ::shm_open(name.c_str(), O_RDWR, 0);
    if (fd < 0) {
        return Status::Error(StatusCode::kInternal,
                             "failed to open supervisor lock handle");
    }
    (void)::fcntl(fd, F_SETFD, FD_CLOEXEC);
    if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
        const int lock_errno = errno;
        ::close(fd);
        if (lock_errno == EWOULDBLOCK || lock_errno == EAGAIN) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "live writable supervisor already attached");
        }
        return Status::Error(StatusCode::kInternal,
                             "failed to acquire supervisor lock");
    }
    return fd;
#else
    (void)name;
    return Status::Error(StatusCode::kUnsupported,
                         "writable supervisor lock unsupported on this platform");
#endif
}

void ReleaseSupervisorLock(int fd) noexcept {
#if defined(__unix__) || defined(__APPLE__)
    if (fd >= 0) {
        (void)::flock(fd, LOCK_UN);
        (void)::close(fd);
    }
#else
    (void)fd;
#endif
}

}  // namespace

SharedMemoryRegion::SharedMemoryRegion(SharedMemoryRegion&& other) noexcept {
    MoveFrom(std::move(other));
}

SharedMemoryRegion& SharedMemoryRegion::operator=(
    SharedMemoryRegion&& other) noexcept {
    if (this != &other) {
        if (!detached_) {
            (void)Detach();
        }
        MoveFrom(std::move(other));
    }
    return *this;
}

void SharedMemoryRegion::MoveFrom(SharedMemoryRegion&& other) noexcept {
    segment_ = std::move(other.segment_);
    region_id_ = other.region_id_;
    owner_identity_ = other.owner_identity_;
    service_fence_at_attach_ = other.service_fence_at_attach_;
    supervisor_lock_fd_ = other.supervisor_lock_fd_;
    is_supervisor_ = other.is_supervisor_;
    detached_ = other.detached_;

    other.segment_.reset();
    other.region_id_ = 0;
    other.owner_identity_ = ProcessIdentity{};
    other.service_fence_at_attach_ = 0;
    other.supervisor_lock_fd_ = -1;
    other.is_supervisor_ = false;
    other.detached_ = true;
}

SharedMemoryRegion::~SharedMemoryRegion() {
    if (!detached_) {
        // Best-effort detach on destruction. A crash skips this; the kernel
        // releases the supervisor lock and the next writable Attach proves the
        // old ProcessIdentity dead before destructive recovery.
        (void)Detach();
    }
}

void SharedMemoryRegion::CloseWithoutLifecycleUpdate() noexcept {
    // If this object already published a new service generation, relinquish
    // only that generation. Lifecycle state/clean_shutdown are deliberately
    // untouched so a failed recovery cannot masquerade as a clean detach.
    if (is_supervisor_ && service_fence_at_attach_ != 0 &&
        segment_.has_value() && !segment_->read_only()) {
        SuperBlock* sb = superblock();
        uint64_t expected = service_fence_at_attach_;
        const uint64_t closing = EncodeServiceFence(
            ServiceFenceEpoch(expected), ServiceFencePhase::kClosing);
        if (CompareExchangeServiceFence(*sb, &expected, closing)) {
            StoreServiceOwner(*sb, ProcessIdentity{});
            StoreServiceFence(
                *sb, EncodeServiceFence(ServiceFenceEpoch(closing),
                                        ServiceFencePhase::kUnowned));
        }
    }
    detached_ = true;
    if (segment_.has_value()) {
        (void)segment_->Close();
    }
    ReleaseSupervisorLock(supervisor_lock_fd_);
    supervisor_lock_fd_ = -1;
    is_supervisor_ = false;
}

Result<SharedMemoryRegion> SharedMemoryRegion::Create(
    const RegionCreateOptions& options) {
    if (options.size_bytes == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "region size must be > 0");
    }

    // Compute the sub-region layout with checked arithmetic (6.3 note).
    // Layout: [0, kSuperBlockSize) SuperBlock, then Directory, then Allocator,
    // then page-aligned Data area.
    uint64_t dir_off = 0;
    if (!CheckedAlignUpU64(kSuperBlockSize, 64, &dir_off)) {
        return Status::Error(StatusCode::kInvalidArgument, "layout overflow");
    }
    uint64_t alloc_off = 0;
    if (!CheckedAlignUpU64(dir_off + options.directory_size_bytes, 64,
                           &alloc_off)) {
        return Status::Error(StatusCode::kInvalidArgument, "layout overflow");
    }
    uint64_t data_off = 0;
    if (!CheckedAlignUpU64(alloc_off + options.allocator_size_bytes,
                           HostPageSize(), &data_off)) {
        return Status::Error(StatusCode::kInvalidArgument, "layout overflow");
    }
    if (options.size_bytes <= data_off) {
        return Status::Error(
            StatusCode::kInvalidArgument,
            "region size too small for SuperBlock + Directory + Allocator");
    }
    uint64_t data_size = 0;
    if (!CheckedSubU64(options.size_bytes, data_off, &data_size)) {
        return Status::Error(StatusCode::kInvalidArgument, "layout overflow");
    }

    // Allocate a persistent region id from the authoritative high-water mark.
    MINO_ASSIGN_OR_RETURN(const uint32_t region_id, AllocateRegionId());

    // Create and map the segment.
    SharedMemoryCreateOptions shm_opts;
    shm_opts.name = options.name;
    shm_opts.size = options.size_bytes;
    shm_opts.use_huge_pages = options.use_huge_pages;
    MINO_ASSIGN_OR_RETURN(SharedMemorySegment segment,
                          SharedMemorySegment::Create(shm_opts));
    MINO_ASSIGN_OR_RETURN(const int supervisor_lock_fd,
                          TryAcquireSupervisorLock(options.name));

    // Initialize the SuperBlock (6.1: INITIALIZING). Zero the header region
    // first so all padding/reserved fields are deterministic. Placement
    // value-initialization is used instead of memset: SuperBlock is not
    // trivially default-constructible (ProcessIdentity has default member
    // initializers), and GCC rejects memset on it with -Wclass-memaccess.
    auto* sb = new (segment.base()) SuperBlock();

    sb->magic = kSuperBlockMagic;
    sb->layout_version = kRegionLayoutVersion;
    sb->header_size = kSuperBlockSize;
    sb->region_size = segment.size();
    sb->byte_order = kByteOrderNative;
    sb->page_size = HostPageSize();
    GenerateRegionUuid(&sb->region_uuid_lo, &sb->region_uuid_hi);
    sb->directory_offset = dir_off;
    sb->allocator_offset = alloc_off;
    sb->data_offset = data_off;
    sb->data_size = data_size;
    sb->region_id = region_id;
    sb->feature_flags = options.feature_flags;
    sb->minimum_reader_version = options.minimum_reader_version;

    // Lifecycle: begin INITIALIZING, in-use (clean_shutdown=false), epoch 1.
    StoreRegionEpoch(*sb, 1);
    StoreCleanShutdown(*sb, false);
    StoreState(*sb, RegionState::kInitializing);
    std::atomic_ref(sb->recovery_lease_ns)
        .store(0, std::memory_order_relaxed);
    // Recovery fencing starts at the same generation as Region Epoch. Every
    // acquisition advances recovery_epoch; a successful commit publishes that
    // generation as the new Region Epoch, making crash replay idempotent.
    std::atomic_ref(sb->recovery_epoch).store(1, std::memory_order_relaxed);
    sb->recovery_owner = ProcessIdentity{};
    StoreRecoveryFence(
        *sb, EncodeRecoveryFence(/*epoch=*/1, RecoveryFencePhase::kActive));
    StoreServiceOwner(*sb, ProcessIdentity::Current());
    StoreServiceFence(
        *sb, EncodeServiceFence(/*epoch=*/1, ServiceFencePhase::kOwned));

    // Seal the immutable header with its CRC (covers fields [0, 80)).
    sb->immutable_crc32 = SuperBlockImmutableCrc(*sb);

    // Initialization complete -> ACTIVE (6.1). clean_shutdown stays false
    // while the Region is in use; it becomes true only on clean Detach.
    StoreState(*sb, RegionState::kActive);

    SharedMemoryRegion region;
    region.segment_ = std::move(segment);
    region.region_id_ = region_id;
    region.owner_identity_ = ProcessIdentity::Current();
    region.service_fence_at_attach_ = LoadServiceFence(*sb);
    region.supervisor_lock_fd_ = supervisor_lock_fd;
    region.is_supervisor_ = true;
    region.detached_ = false;
    return region;
}

Status SharedMemoryRegion::ValidateImmutableHeader(
    const SuperBlock& sb, uint64_t actual_object_size,
    uint32_t expected_feature_flags) {
    // Step 3: Magic and header length.
    if (sb.magic != kSuperBlockMagic) {
        return Status::Error(StatusCode::kCorruption, "bad superblock magic");
    }
    if (sb.header_size != kSuperBlockSize) {
        return Status::Error(StatusCode::kCorruption,
                             "unexpected superblock header size");
    }
    // Step 4: Layout version, byte order, feature flags.
    if (sb.layout_version < kOldestReadableRegionLayoutVersion ||
        sb.layout_version > kRegionLayoutVersion) {
        return Status::Error(StatusCode::kUnsupported,
                             "unsupported region layout version");
    }
    if (sb.byte_order != kByteOrderNative) {
        return Status::Error(StatusCode::kUnsupported,
                             "region byte order mismatch");
    }
    // The reader must support every feature the Region requires.
    if ((sb.feature_flags & ~expected_feature_flags) != 0) {
        return Status::Error(StatusCode::kUnsupported,
                             "region requires unsupported feature flags");
    }
    if (sb.minimum_reader_version > kRegionLayoutVersion) {
        return Status::Error(StatusCode::kUnsupported,
                             "region requires a newer reader version");
    }
    // Step 5: Region size vs the actual mapped object.
    if (sb.region_size == 0 || sb.region_size != actual_object_size) {
        return Status::Error(StatusCode::kCorruption,
                             "region size does not match shm object");
    }
    // Step 6: Page size and necessary alignment.
    if (sb.page_size != HostPageSize()) {
        return Status::Error(StatusCode::kUnsupported,
                             "region page size mismatch");
    }
    if (sb.data_offset % HostPageSize() != 0) {
        return Status::Error(StatusCode::kCorruption,
                             "data offset not page aligned");
    }
    // Step 7: Header CRC over the immutable header.
    if (SuperBlockImmutableCrc(sb) != sb.immutable_crc32) {
        return Status::Error(StatusCode::kCorruption,
                             "superblock immutable header CRC mismatch");
    }
    return Status::Ok();
}

Status SharedMemoryRegion::ValidateSubRegionBounds(const SuperBlock& sb) {
    // Step 9: Directory Offset, Allocator Offset, and data-area bounds, all
    // with checked arithmetic (6.3).
    const uint64_t region_size = sb.region_size;

    // Offsets must be ordered and within the region.
    if (sb.directory_offset < kSuperBlockSize ||
        sb.directory_offset > region_size) {
        return Status::Error(StatusCode::kCorruption,
                             "directory offset out of bounds");
    }
    if (sb.allocator_offset < sb.directory_offset ||
        sb.allocator_offset > region_size) {
        return Status::Error(StatusCode::kCorruption,
                             "allocator offset out of bounds");
    }
    if (sb.data_offset < sb.allocator_offset || sb.data_offset > region_size) {
        return Status::Error(StatusCode::kCorruption,
                             "data offset out of bounds");
    }
    // data_offset + data_size must not overflow nor exceed the region.
    uint64_t data_end = 0;
    if (!CheckedAddU64(sb.data_offset, sb.data_size, &data_end)) {
        return Status::Error(StatusCode::kCorruption,
                             "data area extent overflow");
    }
    if (data_end > region_size) {
        return Status::Error(StatusCode::kCorruption,
                             "data area extends past region end");
    }
    return Status::Ok();
}

Result<SharedMemoryRegion> SharedMemoryRegion::Attach(
    const RegionAttachOptions& options) {
    // Step 1 (permissions) is enforced by opening the object: a read-write
    // open fails with EACCES if the caller lacks write permission.
    //
    // We open read-write even for read_only=false so that recovery (which
    // writes lifecycle fields) is possible. A read_only Attach maps read-only.
    MINO_ASSIGN_OR_RETURN(
        SharedMemorySegment segment,
        SharedMemorySegment::Open(options.name, options.read_only));

    const uint64_t object_size = segment.size();
    if (object_size < kSuperBlockSize) {
        return Status::Error(StatusCode::kCorruption,
                             "shm object smaller than superblock");
    }

    // Step 2: the minimal header is already mapped (the segment maps the whole
    // object; the SuperBlock sits at offset 0). Validate it before trusting
    // any other field.
    auto* sb = static_cast<SuperBlock*>(segment.base());

    // Steps 3-7: immutable header validation. The set of feature flags this
    // reader supports is, for D1, "none required"; callers requiring specific
    // features pass them via expected_feature_flags (currently 0).
    MINO_RETURN_IF_ERROR(ValidateImmutableHeader(*sb, object_size,
                                                 /*expected_feature_flags=*/0));

    // Step 8: full Region is mapped (segment maps the whole object).

    // Step 9: sub-region bounds.
    MINO_RETURN_IF_ERROR(ValidateSubRegionBounds(*sb));

    // If the caller specified an explicit region_id, it must match the one
    // recorded at Create (Region ID mismatch is a hard rejection, 7.1).
    if (options.region_id != 0 && options.region_id != sb->region_id) {
        return Status::Error(StatusCode::kNotFound,
                             "region_id mismatch with shm object");
    }

    // A quarantined Region must not be attached by normal clients (6.5 step 7).
    // The explicit diagnostic exception is deliberately gated by read_only so
    // it can never admit a writable supervisor or enter recovery.
    if (LoadRegionState(*sb) == RegionState::kQuarantined &&
        !(options.read_only && options.allow_quarantined_read_only)) {
        return Status::Error(StatusCode::kUnavailable,
                             "region is quarantined");
    }

    // v2 remains readable, but it has no service-owner identity/fence. A v2
    // writable Attach therefore cannot prove that destructive recovery is safe.
    if (!options.read_only && sb->layout_version < kRegionLayoutVersion) {
        return Status::Error(
            StatusCode::kUnsupported,
            "v2 Region supports read-only compatibility only; recreate as v3 for writable supervisor Attach");
    }

    SharedMemoryRegion region;
    region.segment_ = std::move(segment);
    region.region_id_ = sb->region_id;
    region.owner_identity_ = ProcessIdentity::Current();
    region.detached_ = false;

    // Steps 10-11: a writable Attach first acquires the unique host-local
    // supervisor lock. Lock acquisition is the non-time-based proof that no
    // live writable supervisor still owns this Region. ProcessIdentity then
    // distinguishes a dead incarnation from PID reuse and catches malformed or
    // unverifiable metadata before ACTIVE can become DIRTY.
    if (!options.read_only) {
        auto lock = TryAcquireSupervisorLock(options.name);
        if (!lock.ok()) {
            region.CloseWithoutLifecycleUpdate();
            return lock.status();
        }
        region.supervisor_lock_fd_ = *lock;
        region.is_supervisor_ = true;

        const uint64_t previous_service_fence = LoadServiceFence(*sb);
        const ServiceFencePhase previous_phase =
            ServiceFencePhaseOf(previous_service_fence);
        if (previous_phase != ServiceFencePhase::kOwned &&
            previous_phase != ServiceFencePhase::kUnowned &&
            previous_phase != ServiceFencePhase::kClosing) {
            region.CloseWithoutLifecycleUpdate();
            return Status::Error(StatusCode::kCorruption,
                                 "invalid service fence phase");
        }

        const ProcessIdentity previous_owner = LoadServiceOwner(*sb);
        if (previous_phase == ServiceFencePhase::kOwned) {
            const ProcessIdentityLiveness liveness =
                ProbeProcessIdentity(previous_owner);
            if (liveness == ProcessIdentityLiveness::kAlive) {
                region.CloseWithoutLifecycleUpdate();
                return Status::Error(
                    StatusCode::kWouldBlock,
                    "service owner identity is still live; recovery refused");
            }
            if (liveness == ProcessIdentityLiveness::kUnknown) {
                region.CloseWithoutLifecycleUpdate();
                return Status::Error(
                    StatusCode::kUnavailable,
                    "service owner liveness is unknown; destructive recovery refused");
            }
        }

        const uint64_t previous_service_epoch =
            ServiceFenceEpoch(previous_service_fence);
        if (previous_service_epoch >= kMaxServiceFenceEpoch) {
            region.CloseWithoutLifecycleUpdate();
            return Status::Error(StatusCode::kResourceExhausted,
                                 "service fencing epoch exhausted");
        }
        const uint64_t service_fence = EncodeServiceFence(
            previous_service_epoch + 1, ServiceFencePhase::kOwned);
        StoreServiceOwner(*sb, region.owner_identity_);
        StoreServiceFence(*sb, service_fence);
        region.service_fence_at_attach_ = service_fence;

        // ACTIVE + !clean is recoverable only here: the old supervisor lock is
        // gone and its exact ProcessIdentity was proven dead. Publish DIRTY
        // before the allocator scanner can run.
        if (LoadRegionState(*sb) == RegionState::kActive) {
            if (LoadCleanShutdown(*sb)) {
                region.CloseWithoutLifecycleUpdate();
                return Status::Error(StatusCode::kCorruption,
                                     "ACTIVE Region cannot be cleanly shut down");
            }
            StoreState(*sb, RegionState::kDirty);
        }

        Status recovery = RecoverRegionForAttach(
            region, region.owner_identity_, options.recovery_wait_timeout_ms);
        if (!recovery.ok()) {
            // A failed attach must only unmap and release the supervisor lock;
            // it must not publish clean shutdown or overwrite recovery state.
            region.CloseWithoutLifecycleUpdate();
            return recovery;
        }
    }

    return region;
}

Status SharedMemoryRegion::ValidateSupervisorFence() const {
    if (detached_ || !is_supervisor_ || supervisor_lock_fd_ < 0 ||
        !segment_.has_value() || segment_->read_only()) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "Region is not a writable supervisor attachment");
    }
    const SuperBlock* sb = superblock();
    if (LoadServiceFence(*sb) != service_fence_at_attach_ ||
        ServiceFencePhaseOf(service_fence_at_attach_) !=
            ServiceFencePhase::kOwned ||
        LoadServiceOwner(*sb) != owner_identity_ ||
        owner_identity_ != ProcessIdentity::Current()) {
        return Status::Error(StatusCode::kUnavailable,
                             "writable supervisor service fence is stale");
    }
    return Status::Ok();
}

Status SharedMemoryRegion::Detach() {
    if (detached_) {
        return Status::Ok();
    }

    Status lifecycle_status = Status::Ok();
    SuperBlock* sb = superblock();
    if (sb != nullptr && !read_only() && is_supervisor_) {
        lifecycle_status = ValidateSupervisorFence();
        if (lifecycle_status.ok()) {
            uint64_t expected = service_fence_at_attach_;
            const uint64_t closing = EncodeServiceFence(
                ServiceFenceEpoch(expected), ServiceFencePhase::kClosing);
            if (!CompareExchangeServiceFence(*sb, &expected, closing)) {
                lifecycle_status = Status::Error(
                    StatusCode::kUnavailable,
                    "service fence changed before clean detach");
            } else {
                // The closing fence prevents a stale attachment from racing a
                // lifecycle update. Only ACTIVE may be cleanly closed; never
                // overwrite DIRTY/RECOVERING/QUARANTINED during teardown.
                if (LoadRegionState(*sb) == RegionState::kActive) {
                    StoreCleanShutdown(*sb, true);
                    uint32_t active =
                        static_cast<uint32_t>(RegionState::kActive);
                    if (!std::atomic_ref(sb->state).compare_exchange_strong(
                            active,
                            static_cast<uint32_t>(RegionState::kClosed),
                            std::memory_order_acq_rel,
                            std::memory_order_acquire)) {
                        StoreCleanShutdown(*sb, false);
                    }
                }
                StoreServiceOwner(*sb, ProcessIdentity{});
                StoreServiceFence(
                    *sb, EncodeServiceFence(ServiceFenceEpoch(closing),
                                            ServiceFencePhase::kUnowned));
            }
        }
    }

    detached_ = true;
    Status close_status = segment_->Close();
    ReleaseSupervisorLock(supervisor_lock_fd_);
    supervisor_lock_fd_ = -1;
    is_supervisor_ = false;
    if (!lifecycle_status.ok()) {
        return lifecycle_status;
    }
    return close_status;
}

}  // namespace mino
