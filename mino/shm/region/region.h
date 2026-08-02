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

#ifndef MINO_SHM_REGION_REGION_H_
#define MINO_SHM_REGION_REGION_H_

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <span>
#include <string>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/platform/process_identity.h"
#include "mino/platform/shared_memory.h"
#include "mino/shm/region/channel_directory.h"
#include "mino/shm/region/recovery_directory.h"
#include "mino/shm/region/superblock.h"

namespace mino {

class SharedMemoryRegion;

// Options for SharedMemoryRegion::Create (design doc section 6.2).
struct RegionCreateOptions {
    // POSIX shm object name (e.g. "/my_region"). The region_id is assigned by
    // the authoritative registry at Create and must NOT be specified by the
    // caller (design doc section 6.2).
    std::string name;
    uint64_t size_bytes = 0;
    bool read_only = false;

    // Back the mapping with huge pages when available (degrades to normal
    // pages otherwise).
    bool use_huge_pages = false;

    // Reserved space between the SuperBlock and the data area for the fixed
    // recovery and channel directories plus allocator metadata. Region layout
    // v5 requires enough space for both directory images.
    uint64_t directory_size_bytes = 64 * 1024;
    uint64_t allocator_size_bytes = 64 * 1024;

    // Feature flags to publish in the SuperBlock. Readers that do not support
    // a required flag reject the Region at Attach.
    uint64_t feature_flags = 0;
    uint32_t minimum_reader_version = 1;
};

// Options for SharedMemoryRegion::Attach (design doc section 6.2).
//
// v5 attachment contract:
//   * read_only=true supports any number of concurrent processes and never
//     participates in lifecycle recovery. Region layouts v2-v4 remain readable;
//     v4 has a Recovery Directory but no Channel Directory. This is offline
//     read compatibility, not mixed-version rolling compatibility.
//   * read_only=false requests the unique supervisor role and requires the
//     current v5 layout. It fails with kWouldBlock while another supervisor
//     process is live. Writable non-supervisor Attach remains unsupported.
struct RegionV4UpgradeOptions {
    std::string name;
    std::span<const ChannelRingDescriptor> rings;
    // An empty list is ambiguous for v4 because no channel inventory existed.
    // Callers must explicitly attest that the Region contains no channels.
    bool confirm_no_channels = false;
};

using RegionV4CopyCallback = Status (*)(const SharedMemoryRegion& source,
                                        SharedMemoryRegion& destination,
                                        void* context);

struct RegionV4CopyUpgradeOptions {
    std::string source_name;
    RegionCreateOptions destination;
    RegionV4CopyCallback migrate = nullptr;
    void* context = nullptr;
};

struct RegionAttachOptions {
    std::string name;        // resolved to a region_id via the Registry
    uint32_t region_id = 0;  // or specified explicitly (mutually exclusive)
    bool read_only = false;

    // Diagnostic escape hatch for quarantined Regions. It is honored only when
    // read_only=true; writable Attach always rejects QUARANTINED before taking
    // the supervisor lock or changing any lifecycle/fence metadata.
    bool allow_quarantined_read_only = false;

    // How long to wait for an in-progress recovery by another process before
    // giving up (design doc section 6.5 step 4). Zero means do not wait.
    uint64_t recovery_wait_timeout_ms = 5000;
};

// SharedMemoryRegion owns a mapping of a single shared-memory Region and its
// SuperBlock (design doc section 6.2). It is the unit of attachment and the
// boundary for all Offset-based addressing.
class SharedMemoryRegion {
public:
    SharedMemoryRegion(const SharedMemoryRegion&) = delete;
    SharedMemoryRegion& operator=(const SharedMemoryRegion&) = delete;
    SharedMemoryRegion(SharedMemoryRegion&& other) noexcept;
    SharedMemoryRegion& operator=(SharedMemoryRegion&& other) noexcept;
    ~SharedMemoryRegion();

    // Creates a new Region: allocates a persistent region_id, lays out and
    // initializes the SuperBlock, and maps the Region (design doc 6.1:
    // ABSENT -> INITIALIZING -> ACTIVE).
    static Result<SharedMemoryRegion> Create(const RegionCreateOptions&);

    // Attaches to an existing Region, performing the full 11-step validation
    // of design doc section 6.3, then mapping the full Region. If the Region
    // is dirty (unclean shutdown), enters the recovery flow (section 6.5).
    static Result<SharedMemoryRegion> Attach(const RegionAttachOptions&);

    // Offline-only in-place v4 -> v5 upgrade. Requires a clean CLOSED Region,
    // exclusive supervisor lock, sufficient reserved directory bytes, and an
    // explicit complete MPMC inventory. This is not a rolling upgrade.
    static Status UpgradeV4ToV5Offline(const RegionV4UpgradeOptions&);

    // Creates a fresh v5 Region and invokes a semantic migration callback while
    // holding an offline lock on the clean CLOSED v4 source. Raw Region bytes
    // are never copied because allocator handles embed Region identity. Use this
    // path to recreate layouts such as Broadcast v4 that cannot be upgraded by
    // the generic Region layer in place.
    static Result<SharedMemoryRegion> CopyUpgradeV4ToV5Offline(
        const RegionV4CopyUpgradeOptions&);

    std::byte* base() noexcept {
        return static_cast<std::byte*>(segment_->base());
    }
    const std::byte* base() const noexcept {
        return static_cast<const std::byte*>(segment_->base());
    }
    uint64_t size() const noexcept { return segment_->size(); }
    uint32_t region_id() const noexcept { return region_id_; }
    bool read_only() const noexcept { return segment_->read_only(); }

    // The SuperBlock at offset 0. Exposed for the region package (Recovery,
    // Resolver). Callers must not mutate immutable fields.
    SuperBlock* superblock() noexcept {
        return reinterpret_cast<SuperBlock*>(segment_->base());
    }
    const SuperBlock* superblock() const noexcept {
        return reinterpret_cast<const SuperBlock*>(segment_->base());
    }

    // Cleanly detaches. A writable supervisor first validates its exact v3
    // service fence, then marks clean_shutdown=true and ACTIVE->CLOSED. A stale
    // Region object can only unmap; it cannot close a replacement supervisor's
    // Region. Read-only attachments only unmap.
    Status Detach();

    // True for the unique writable supervisor attachment. Mutable Region data
    // access is valid only while ValidateSupervisorFence() succeeds.
    bool is_supervisor() const noexcept { return is_supervisor_; }
    uint64_t service_epoch() const noexcept {
        return ServiceFenceEpoch(service_fence_at_attach_);
    }
    Status ValidateSupervisorFence() const;

    // The identity used for service and recovery ownership in this process.
    const ProcessIdentity& owner_identity() const { return owner_identity_; }

    // Publishes one persistent, Region-relative recovery resource descriptor.
    // Registration is restricted to the current writable supervisor. Reusing a
    // resource_id atomically replaces that descriptor in the next directory
    // snapshot; no process pointer is ever persisted.
    Status RegisterRecoveryResource(
        const RecoveryResourceDescriptor& descriptor);

    // Atomically publishes the allocator-unit reference set. `complete=false`
    // is conservative: recovery validates resources and protocol intermediates
    // but never classifies a normal PUBLISHED object as an orphan.
    Status PublishRecoveryReferences(
        std::span<const RecoveryObjectReference> references, bool complete);

    // Returns the current CRC-validated recovery directory snapshot.
    Result<RecoveryDirectorySnapshot> recovery_directory() const;

    // Publishes one initialized MPMC Ring using only Region-relative metadata.
    // The descriptor must be ACTIVE and match the actual control block. A
    // retired channel id may be reused only with a strictly newer generation.
    Status RegisterChannelRing(const ChannelRingDescriptor& descriptor);

    // Retires exactly the active generation. A stale generation cannot remove
    // a replacement registration.
    Status UnregisterChannelRing(uint32_t channel_id, uint64_t generation);

    // Returns an immutable, CRC-validated point-in-time directory snapshot.
    Result<ChannelDirectorySnapshot> channel_directory() const;

private:
    SharedMemoryRegion() = default;

    // Validation helpers for Attach (design doc 6.3). Each returns OK or an
    // error; they are run in the mandated order.
    static Status ValidateImmutableHeader(const SuperBlock& sb,
                                          uint64_t actual_object_size,
                                          uint32_t expected_feature_flags);
    static Status ValidateSubRegionBounds(const SuperBlock& sb);
    void MoveFrom(SharedMemoryRegion&& other) noexcept;
    void CloseWithoutLifecycleUpdate() noexcept;

    std::optional<SharedMemorySegment> segment_;
    uint32_t region_id_ = 0;
    ProcessIdentity owner_identity_;
    uint64_t service_fence_at_attach_ = 0;
    int supervisor_lock_fd_ = -1;
    bool is_supervisor_ = false;
    bool detached_ = false;
    mutable std::mutex recovery_directory_mutex_;
    mutable std::mutex channel_directory_mutex_;
};

}  // namespace mino

#endif  // MINO_SHM_REGION_REGION_H_
