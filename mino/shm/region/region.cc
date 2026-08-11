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

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <limits>
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
#include "mino/shm/channel/mpmc_ring.h"
#include "mino/shm/region/recovery.h"
#include "mino/shm/region/region_id_allocator.h"

namespace mino {
namespace {

constexpr uint32_t kPrivateRegionPermissions = 0600;

uint64_t CurrentUserId() noexcept {
#if defined(__unix__) || defined(__APPLE__)
    return static_cast<uint64_t>(::geteuid());
#else
    return 0;
#endif
}

uint64_t CurrentGroupId() noexcept {
#if defined(__unix__) || defined(__APPLE__)
    return static_cast<uint64_t>(::getegid());
#else
    return 0;
#endif
}

SecurityDomainId ResolveSecurityDomain(SecurityDomainId configured) noexcept {
    return configured.value == 0 ? CurrentSecurityDomainId() : configured;
}

Status ValidateSegmentSecurity(const SharedMemorySegment& segment) {
    const uint64_t user = CurrentUserId();
    const uint64_t group = CurrentGroupId();
    if (segment.marker_owner_user_id() != user ||
        segment.backing_owner_user_id() != user ||
        segment.marker_owner_group_id() != group ||
        segment.backing_owner_group_id() != group) {
        return Status::Error(
            StatusCode::kPermissionDenied,
            "Region owner UID/GID does not match the attaching process");
    }
    if (segment.marker_permissions() != kPrivateRegionPermissions ||
        segment.backing_permissions() != kPrivateRegionPermissions) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "Region marker and backing must have mode 0600");
    }
    return Status::Ok();
}

// Generates a nonzero 128-bit region UUID from a secure random source.
void GenerateRegionUuid(uint64_t* lo, uint64_t* hi) {
    std::random_device rd;
    do {
        *lo = (static_cast<uint64_t>(rd()) << 32) | rd();
        *hi = (static_cast<uint64_t>(rd()) << 32) | rd();
    } while (*lo == 0 && *hi == 0);
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
#if defined(__APPLE__)
    (void)name;
    return Status::Error(
        StatusCode::kUnsupported,
        "writable Region supervisor locking is supported only on Linux");
#elif defined(__unix__)
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

class ScopedSupervisorLock {
public:
    explicit ScopedSupervisorLock(int fd) : fd_(fd) {}
    ScopedSupervisorLock(const ScopedSupervisorLock&) = delete;
    ScopedSupervisorLock& operator=(const ScopedSupervisorLock&) = delete;
    ~ScopedSupervisorLock() { ReleaseSupervisorLock(fd_); }

private:
    int fd_;
};

Status ValidateOfflineV4Source(const SuperBlock& sb) {
    if (sb.layout_version != kRecoveryDirectoryRegionLayoutVersion) {
        return Status::Error(StatusCode::kUnsupported,
                             "offline upgrade requires Region layout v4");
    }
    if (!LoadCleanShutdown(sb) ||
        LoadRegionState(sb) != RegionState::kClosed) {
        return Status::Error(StatusCode::kUnavailable,
                             "offline upgrade requires a clean CLOSED Region");
    }
    return Status::Ok();
}

}  // namespace

SecurityDomainId CurrentSecurityDomainId() noexcept {
    // UID 0 remains distinct and nonzero. Explicit deployment IDs are required
    // when one OS account hosts more than one security level or tenant.
    return SecurityDomainId{CurrentUserId() + 1};
}

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
    if (options.read_only) {
        return Status::Error(
            StatusCode::kInvalidArgument,
            "Create does not support read_only; create writable and Attach read-only");
    }
    if (options.size_bytes == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "region size must be > 0");
    }
    const SecurityDomainId security_domain =
        ResolveSecurityDomain(options.security_domain);
    if (security_domain.value == 0 ||
        CurrentUserId() > std::numeric_limits<uint32_t>::max() ||
        CurrentGroupId() > std::numeric_limits<uint32_t>::max()) {
        return Status::Error(
            StatusCode::kInvalidArgument,
            "Region Security Domain or owner identity is invalid");
    }

    // Compute the sub-region layout with checked arithmetic (6.3 note).
    // Layout: [0, kSuperBlockSize) SuperBlock, then Directory, then Allocator,
    // then page-aligned Data area.
    uint64_t dir_off = 0;
    if (!CheckedAlignUpU64(kSuperBlockSize, 64, &dir_off)) {
        return Status::Error(StatusCode::kInvalidArgument, "layout overflow");
    }
    if (options.directory_size_bytes < kRegionDirectoryMinimumSize) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "directory sub-region is too small for Region v5 directories");
    }
    uint64_t directory_end = 0;
    uint64_t alloc_off = 0;
    if (!CheckedAddU64(dir_off, options.directory_size_bytes, &directory_end) ||
        !CheckedAlignUpU64(directory_end, 64, &alloc_off)) {
        return Status::Error(StatusCode::kInvalidArgument, "layout overflow");
    }
    uint64_t allocator_end = 0;
    uint64_t data_off = 0;
    if (!CheckedAddU64(alloc_off, options.allocator_size_bytes, &allocator_end) ||
        !CheckedAlignUpU64(allocator_end,
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

    // Commit the following durable HWM before exposing this ID. ID 0 is
    // reserved and exhaustion is represented above UINT32_MAX so the counter
    // cannot wrap.
    MINO_ASSIGN_OR_RETURN(const uint32_t region_id,
                          region_internal::AllocateRegionId());

    // Create and map the segment.
    SharedMemoryCreateOptions shm_opts;
    shm_opts.name = options.name;
    shm_opts.size = options.size_bytes;
    shm_opts.use_huge_pages = options.use_huge_pages;
    MINO_ASSIGN_OR_RETURN(SharedMemorySegment segment,
                          SharedMemorySegment::Create(shm_opts));
    const Status segment_security = ValidateSegmentSecurity(segment);
    if (!segment_security.ok()) {
        (void)segment.Close();
        (void)SharedMemorySegment::Unlink(options.name);
        return segment_security;
    }
    auto supervisor_lock = TryAcquireSupervisorLock(options.name);
    if (!supervisor_lock.ok()) {
        (void)segment.Close();
        (void)SharedMemorySegment::Unlink(options.name);
        return supervisor_lock.status();
    }
    const int supervisor_lock_fd = *supervisor_lock;

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
    sb->security_domain_id = security_domain.value;
    sb->owner_user_id = static_cast<uint32_t>(segment.backing_owner_user_id());
    sb->owner_group_id = static_cast<uint32_t>(segment.backing_owner_group_id());
    sb->access_mode = segment.backing_permissions();
    sb->feature_flags = options.feature_flags;
    sb->minimum_reader_version = std::max<uint32_t>(
        options.minimum_reader_version, kSecurityDomainRegionLayoutVersion);

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

    // Seal the immutable identity and Security Domain metadata with its CRC.
    sb->immutable_crc32 = SuperBlockImmutableCrc(*sb);

    // Initialize both fixed-capacity directory images before publishing ACTIVE.
    // The immutable SuperBlock directory_offset remains the start of the
    // recovery directory; v5 locates the Channel Directory at its fixed relative
    // offset inside the same reserved sub-region.
    auto* directory_base =
        static_cast<std::byte*>(segment.base()) + dir_off;
    const uint64_t directory_size = alloc_off - dir_off;
    MINO_RETURN_IF_ERROR(
        InitializeRecoveryDirectory(directory_base, directory_size));
    MINO_RETURN_IF_ERROR(InitializeChannelDirectory(
        directory_base + kChannelDirectoryRelativeOffset,
        directory_size - kChannelDirectoryRelativeOffset));

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
    if (sb.region_uuid_lo == 0 && sb.region_uuid_hi == 0) {
        return Status::Error(StatusCode::kCorruption, "Region UUID is zero");
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
    if (options.name.empty()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "Attach requires a Region name; ID-only Attach is unsupported");
    }

    // Step 1 (permissions) is enforced by opening the object: a read-write
    // open fails with EACCES if the caller lacks write permission.
    //
    // We open read-write even for read_only=false so that recovery (which
    // writes lifecycle fields) is possible. A read_only Attach maps read-only.
    MINO_ASSIGN_OR_RETURN(
        SharedMemorySegment segment,
        SharedMemorySegment::Open(options.name, options.read_only));
    MINO_RETURN_IF_ERROR(ValidateSegmentSecurity(segment));

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
    // reader supports is currently "none required"; callers requiring specific
    // features pass them via expected_feature_flags (currently 0).
    MINO_RETURN_IF_ERROR(ValidateImmutableHeader(*sb, object_size,
                                                 /*expected_feature_flags=*/0));

    if (sb->layout_version < kSecurityDomainRegionLayoutVersion) {
        if (!(options.read_only && options.allow_unscoped_legacy_read_only)) {
            return Status::Error(
                StatusCode::kPermissionDenied,
                "legacy Region has no authenticated Security Domain metadata");
        }
    } else {
        const SecurityDomainId expected =
            ResolveSecurityDomain(options.security_domain);
        if (sb->security_domain_id == 0 ||
            sb->security_domain_id != expected.value) {
            return Status::Error(StatusCode::kPermissionDenied,
                                 "Region Security Domain mismatch");
        }
        if (sb->owner_user_id != segment.backing_owner_user_id() ||
            sb->owner_group_id != segment.backing_owner_group_id() ||
            sb->access_mode != segment.backing_permissions() ||
            segment.marker_owner_user_id() != segment.backing_owner_user_id() ||
            segment.marker_owner_group_id() !=
                segment.backing_owner_group_id() ||
            segment.marker_permissions() != segment.backing_permissions()) {
            return Status::Error(StatusCode::kPermissionDenied,
                                 "Region owner or permission metadata mismatch");
        }
    }

    // Step 8: full Region is mapped (segment maps the whole object).

    // Step 9: sub-region bounds.
    MINO_RETURN_IF_ERROR(ValidateSubRegionBounds(*sb));
    const uint64_t directory_size =
        sb->allocator_offset - sb->directory_offset;
    const auto* directory_base =
        static_cast<const std::byte*>(segment.base()) + sb->directory_offset;
    if (sb->layout_version >= kRecoveryDirectoryRegionLayoutVersion) {
        auto directory =
            ::mino::ReadRecoveryDirectory(directory_base, directory_size);
        if (!directory.ok()) {
            return directory.status();
        }
    }
    if (sb->layout_version >= kChannelDirectoryRegionLayoutVersion) {
        if (directory_size < kRegionDirectoryMinimumSize) {
            return Status::Error(StatusCode::kCorruption,
                                 "Region v5 directory sub-region is too small");
        }
        auto channels = ::mino::ReadChannelDirectory(
            directory_base + kChannelDirectoryRelativeOffset,
            directory_size - kChannelDirectoryRelativeOffset, object_size,
            sb->data_offset);
        if (!channels.ok()) {
            return channels.status();
        }
    }

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

    // Older layouts remain readable. Writable compatibility is deliberately
    // conservative because v4 lacks the Channel Directory required by v5.
    if (!options.read_only && sb->layout_version < kRegionLayoutVersion) {
        return Status::Error(
            StatusCode::kUnsupported,
            "older Region layout supports read-only compatibility only; recreate as v4 for writable supervisor Attach");
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

Status SharedMemoryRegion::UpgradeV4ToV5Offline(
    const RegionV4UpgradeOptions& options) {
    if (options.name.empty()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "upgrade Region name must not be empty");
    }
    if (options.rings.empty() && !options.confirm_no_channels) {
        return Status::Error(
            StatusCode::kInvalidArgument,
            "v4 has no channel inventory; explicitly confirm no channels or provide the complete MPMC descriptor set");
    }

    MINO_ASSIGN_OR_RETURN(const int lock_fd,
                          TryAcquireSupervisorLock(options.name));
    ScopedSupervisorLock lock(lock_fd);
    MINO_ASSIGN_OR_RETURN(SharedMemorySegment segment,
                          SharedMemorySegment::Open(options.name,
                                                    /*read_only=*/false));
    MINO_RETURN_IF_ERROR(ValidateSegmentSecurity(segment));
    if (segment.size() < kSuperBlockSize) {
        return Status::Error(StatusCode::kCorruption,
                             "upgrade source is smaller than SuperBlock");
    }
    auto* sb = static_cast<SuperBlock*>(segment.base());
    MINO_RETURN_IF_ERROR(ValidateImmutableHeader(
        *sb, segment.size(), /*expected_feature_flags=*/0));
    MINO_RETURN_IF_ERROR(ValidateSubRegionBounds(*sb));
    MINO_RETURN_IF_ERROR(ValidateOfflineV4Source(*sb));

    const uint64_t directory_size =
        sb->allocator_offset - sb->directory_offset;
    if (directory_size < kRegionDirectoryMinimumSize) {
        return Status::Error(
            StatusCode::kResourceExhausted,
            "v4 reserved directory is too small for in-place v5 upgrade; use copy/recreate migration");
    }
    auto* region_base = static_cast<std::byte*>(segment.base());
    auto recovery = ReadRecoveryDirectory(
        region_base + sb->directory_offset, directory_size);
    if (!recovery.ok()) {
        return recovery.status();
    }

    auto* channel_base = region_base + sb->directory_offset +
                         kChannelDirectoryRelativeOffset;
    const uint64_t channel_size =
        directory_size - kChannelDirectoryRelativeOffset;
    MINO_RETURN_IF_ERROR(
        InitializeChannelDirectory(channel_base, channel_size));

    for (const ChannelRingDescriptor& descriptor : options.rings) {
        if (descriptor.channel_type !=
            static_cast<uint32_t>(ChannelRingType::kMpmcRing)) {
            return Status::Error(
                StatusCode::kUnsupported,
                "non-MPMC channels, including Broadcast v4, require copy/recreate migration");
        }
        MINO_RETURN_IF_ERROR(ValidateChannelRingDescriptor(
            descriptor, segment.size(), sb->data_offset));
        auto* control = reinterpret_cast<MpmcRingControlBlock*>(
            region_base + descriptor.control_offset);
        if (control->magic.load(std::memory_order_acquire) != kMpmcRingMagic ||
            control->capacity != descriptor.capacity ||
            control->elem_size != descriptor.element_size ||
            control->elem_align != descriptor.element_alignment) {
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "v4 MPMC descriptor does not match backing control");
        }

        const uint32_t ring_version =
            control->layout_version.load(std::memory_order_acquire);
        if (ring_version == kOldestUpgradeableMpmcRingLayoutVersion) {
            // v1 used elem_align as the complete slot alignment. The old and v2
            // strides are identical only when elem_align already aligns the
            // outer uint64_t sequence.
            if (descriptor.element_alignment < alignof(uint64_t)) {
                return Status::Error(
                    StatusCode::kUnsupported,
                    "MPMC v1 small-alignment stride requires copy/recreate migration");
            }
            const uint64_t old_stride =
                (sizeof(uint64_t) + descriptor.element_size +
                 descriptor.element_alignment - 1) /
                descriptor.element_alignment * descriptor.element_alignment;
            uint64_t slots_size = 0;
            uint64_t old_extent = 0;
            if (!CheckedMulU64(descriptor.capacity, old_stride, &slots_size) ||
                !CheckedAddU64(sizeof(MpmcRingControlBlock), slots_size,
                               &old_extent) ||
                old_extent != descriptor.extent_size) {
                return Status::Error(
                    StatusCode::kSchemaMismatch,
                    "MPMC v1 extent differs from the v2 in-place layout");
            }
            control->active_state.store(
                static_cast<uint32_t>(MpmcRingState::kInactive),
                std::memory_order_release);
            control->generation.store(descriptor.generation,
                                      std::memory_order_relaxed);
            control->reserved1 = 0;
            control->layout_version.store(kMpmcRingLayoutVersion,
                                          std::memory_order_release);
            control->active_state.store(
                static_cast<uint32_t>(MpmcRingState::kActive),
                std::memory_order_release);
        } else if (ring_version == kMpmcRingLayoutVersion) {
            MINO_RETURN_IF_ERROR(
                ValidateMpmcRingFence(*control, descriptor.generation));
        } else {
            return Status::Error(StatusCode::kUnsupported,
                                 "unsupported MPMC layout in v4 source");
        }

        MINO_RETURN_IF_ERROR(::mino::RegisterChannelRing(
            channel_base, channel_size, segment.size(), sb->data_offset,
            descriptor));
    }

    // Offline readers are excluded by contract and the supervisor lock excludes
    // a writer. Publish the Security Domain and current immutable header only
    // after both directories and every ring fence are complete.
    const SecurityDomainId security_domain = CurrentSecurityDomainId();
    if (security_domain.value == 0 ||
        segment.backing_owner_user_id() >
            std::numeric_limits<uint32_t>::max() ||
        segment.backing_owner_group_id() >
            std::numeric_limits<uint32_t>::max()) {
        return Status::Error(
            StatusCode::kInvalidArgument,
            "upgrade Security Domain or owner identity is invalid");
    }
    sb->layout_version = kRegionLayoutVersion;
    sb->security_domain_id = security_domain.value;
    sb->owner_user_id = static_cast<uint32_t>(segment.backing_owner_user_id());
    sb->owner_group_id = static_cast<uint32_t>(segment.backing_owner_group_id());
    sb->access_mode = segment.backing_permissions();
    sb->minimum_reader_version = kSecurityDomainRegionLayoutVersion;
    sb->immutable_crc32 = SuperBlockImmutableCrc(*sb);
    std::atomic_thread_fence(std::memory_order_seq_cst);
    return segment.Close();
}

Result<SharedMemoryRegion> SharedMemoryRegion::CopyUpgradeV4ToV5Offline(
    const RegionV4CopyUpgradeOptions& options) {
    if (options.source_name.empty() || options.destination.name.empty() ||
        options.source_name == options.destination.name ||
        options.migrate == nullptr) {
        return Status::Error(
            StatusCode::kInvalidArgument,
            "copy/recreate migration requires distinct names and a semantic migration callback");
    }

    MINO_ASSIGN_OR_RETURN(const int lock_fd,
                          TryAcquireSupervisorLock(options.source_name));
    ScopedSupervisorLock lock(lock_fd);
    RegionAttachOptions source_options;
    source_options.name = options.source_name;
    source_options.read_only = true;
    source_options.allow_unscoped_legacy_read_only = true;
    MINO_ASSIGN_OR_RETURN(SharedMemoryRegion source,
                          SharedMemoryRegion::Attach(source_options));
    MINO_RETURN_IF_ERROR(ValidateOfflineV4Source(*source.superblock()));

    auto destination = SharedMemoryRegion::Create(options.destination);
    if (!destination.ok()) {
        return destination.status();
    }
    Status migrated = options.migrate(source, *destination, options.context);
    if (!migrated.ok()) {
        (void)destination->Detach();
        (void)SharedMemorySegment::Unlink(options.destination.name);
        return migrated;
    }
    return std::move(*destination);
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

Result<RecoveryDirectorySnapshot>
SharedMemoryRegion::recovery_directory() const {
    if (!segment_.has_value() || detached_) {
        return Status::Error(StatusCode::kUnavailable,
                             "Region is detached");
    }
    const SuperBlock* sb = superblock();
    if (sb->layout_version < kRecoveryDirectoryRegionLayoutVersion) {
        return Status::Error(StatusCode::kUnsupported,
                             "Region layout has no recovery directory");
    }
    return ::mino::ReadRecoveryDirectory(
        base() + sb->directory_offset,
        sb->allocator_offset - sb->directory_offset);
}

Result<ChannelDirectorySnapshot>
SharedMemoryRegion::channel_directory() const {
    if (!segment_.has_value() || detached_) {
        return Status::Error(StatusCode::kUnavailable, "Region is detached");
    }
    const SuperBlock* sb = superblock();
    if (sb->layout_version < kChannelDirectoryRegionLayoutVersion) {
        return Status::Error(StatusCode::kUnsupported,
                             "Region layout has no channel directory");
    }
    const uint64_t directory_size =
        sb->allocator_offset - sb->directory_offset;
    if (directory_size < kRegionDirectoryMinimumSize) {
        return Status::Error(StatusCode::kCorruption,
                             "Region channel directory storage is truncated");
    }
    return ::mino::ReadChannelDirectory(
        base() + sb->directory_offset + kChannelDirectoryRelativeOffset,
        directory_size - kChannelDirectoryRelativeOffset, size(),
        sb->data_offset);
}

Status SharedMemoryRegion::RegisterChannelRing(
    const ChannelRingDescriptor& descriptor) {
    MINO_RETURN_IF_ERROR(ValidateSupervisorFence());
    const SuperBlock* sb = superblock();
    if (sb->layout_version < kChannelDirectoryRegionLayoutVersion) {
        return Status::Error(StatusCode::kUnsupported,
                             "Region layout has no channel directory");
    }
    MINO_RETURN_IF_ERROR(ValidateChannelRingDescriptor(
        descriptor, size(), sb->data_offset));

    const auto* control = reinterpret_cast<const MpmcRingControlBlock*>(
        base() + descriptor.control_offset);
    if (control->magic.load(std::memory_order_acquire) != kMpmcRingMagic) {
        return Status::Error(StatusCode::kCorruption,
                             "registered ring control magic mismatch");
    }
    if (control->layout_version.load(std::memory_order_relaxed) !=
            descriptor.ring_layout_version ||
        control->capacity != descriptor.capacity ||
        control->elem_size != descriptor.element_size ||
        control->elem_align != descriptor.element_alignment ||
        control->generation.load(std::memory_order_acquire) !=
            descriptor.generation ||
        control->active_state.load(std::memory_order_acquire) !=
            static_cast<uint32_t>(MpmcRingState::kActive)) {
        return Status::Error(StatusCode::kSchemaMismatch,
                             "registered ring descriptor does not match control ABI");
    }

    const uint64_t directory_size =
        sb->allocator_offset - sb->directory_offset;
    std::lock_guard lock(channel_directory_mutex_);
    return ::mino::RegisterChannelRing(
        base() + sb->directory_offset + kChannelDirectoryRelativeOffset,
        directory_size - kChannelDirectoryRelativeOffset, size(),
        sb->data_offset, descriptor);
}

Status SharedMemoryRegion::UnregisterChannelRing(uint32_t channel_id,
                                                  uint64_t generation) {
    MINO_RETURN_IF_ERROR(ValidateSupervisorFence());
    const SuperBlock* sb = superblock();
    if (sb->layout_version < kChannelDirectoryRegionLayoutVersion) {
        return Status::Error(StatusCode::kUnsupported,
                             "Region layout has no channel directory");
    }
    const uint64_t directory_size =
        sb->allocator_offset - sb->directory_offset;
    std::lock_guard lock(channel_directory_mutex_);
    auto snapshot = channel_directory();
    if (!snapshot.ok()) {
        return snapshot.status();
    }
    const ChannelRingDescriptor* descriptor = nullptr;
    for (uint32_t i = 0; i < snapshot->entry_count; ++i) {
        if (snapshot->entries[i].channel_id == channel_id) {
            descriptor = &snapshot->entries[i];
            break;
        }
    }
    if (descriptor == nullptr) {
        return Status::Error(StatusCode::kNotFound,
                             "channel id is not registered");
    }
    if (descriptor->state !=
            static_cast<uint32_t>(ChannelRingState::kActive) ||
        descriptor->generation != generation) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "channel unregistration generation is stale");
    }

    auto* control = reinterpret_cast<MpmcRingControlBlock*>(
        base() + descriptor->control_offset);
    MINO_RETURN_IF_ERROR(RetireMpmcRing(*control, generation));
    Status unpublished = ::mino::UnregisterChannelRing(
        base() + sb->directory_offset + kChannelDirectoryRelativeOffset,
        directory_size - kChannelDirectoryRelativeOffset, size(),
        sb->data_offset, channel_id, generation);
    if (!unpublished.ok()) {
        if (!ReactivateRetiredMpmcRing(*control, generation)) {
            return Status::Error(
                StatusCode::kCorruption,
                "channel directory publication failed and ring fence could not be restored");
        }
        return unpublished;
    }
    return Status::Ok();
}

Status SharedMemoryRegion::RegisterRecoveryResource(
    const RecoveryResourceDescriptor& descriptor) {
    MINO_RETURN_IF_ERROR(ValidateSupervisorFence());
    const SuperBlock* sb = superblock();
    if (sb->layout_version < kRecoveryDirectoryRegionLayoutVersion) {
        return Status::Error(StatusCode::kUnsupported,
                             "Region layout has no recovery directory");
    }
    const auto kind = static_cast<RecoveryResourceKind>(descriptor.kind);
    const uint64_t minimum_offset =
        kind == RecoveryResourceKind::kCentralAllocator
            ? sb->allocator_offset
            : sb->data_offset;
    if (descriptor.offset < minimum_offset ||
        ((kind == RecoveryResourceKind::kChannelAckSource ||
          kind == RecoveryResourceKind::kPinCleanupParticipant) &&
         descriptor.control_offset < sb->data_offset)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "recovery resource overlaps Region control metadata");
    }
    std::lock_guard lock(recovery_directory_mutex_);
    return ::mino::PublishRecoveryResource(
        base() + sb->directory_offset,
        sb->allocator_offset - sb->directory_offset, size(), descriptor);
}

Status SharedMemoryRegion::PublishRecoveryReferences(
    std::span<const RecoveryObjectReference> references, bool complete) {
    MINO_RETURN_IF_ERROR(ValidateSupervisorFence());
    std::lock_guard lock(recovery_directory_mutex_);
    auto directory = recovery_directory();
    if (!directory.ok()) {
        return directory.status();
    }
    for (const RecoveryObjectReference& reference : references) {
        bool found = false;
        for (uint32_t i = 0; i < directory->resource_count; ++i) {
            found |= directory->resources[i].resource_id == reference.resource_id;
        }
        if (!found) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "reference names an unregistered recovery resource");
        }
    }
    const SuperBlock* sb = superblock();
    return ::mino::PublishRecoveryReferences(
        base() + sb->directory_offset,
        sb->allocator_offset - sb->directory_offset, references, complete);
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
