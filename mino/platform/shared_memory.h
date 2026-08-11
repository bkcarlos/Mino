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

#ifndef MINO_PLATFORM_SHARED_MEMORY_H_
#define MINO_PLATFORM_SHARED_MEMORY_H_

#include <cstdint>
#include <string>

#include "mino/common/result.h"
#include "mino/common/status.h"

namespace mino {

enum class HugePageFallbackReason : uint8_t {
    kNone = 0,
    kUnsupportedPlatform,
    kHugetlbfsUnavailable,
    kInsufficientHugePages,
    kPermissionDenied,
    kUnsupportedBacking,
    kSystemError,
};

const char* HugePageFallbackReasonName(HugePageFallbackReason reason) noexcept;

struct SharedMemoryCreateOptions {
    // Authoritative POSIX marker name. The marker contains only versioned,
    // CRC-protected backing metadata; user data always lives in a separate
    // hugetlbfs file or POSIX data object.
    std::string name;
    uint64_t size = 0;
    bool use_huge_pages = false;

    // Used only while Create chooses a huge backing. Open and Unlink never
    // consult configuration or guess a path; they use the validated marker.
    std::string hugetlbfs_path;
};

struct SharedMemoryOpenOptions {
    std::string name;
    bool read_only = false;

    // Open polls a CREATING marker only for this bounded interval, then returns
    // kWouldBlock. UNLINKING returns kWouldBlock immediately.
    uint64_t creating_wait_timeout_ms = 100;
};

class SharedMemorySegment {
public:
    SharedMemorySegment(const SharedMemorySegment&) = delete;
    SharedMemorySegment& operator=(const SharedMemorySegment&) = delete;
    SharedMemorySegment(SharedMemorySegment&& other) noexcept;
    SharedMemorySegment& operator=(SharedMemorySegment&& other) noexcept;
    ~SharedMemorySegment();

    static Result<SharedMemorySegment> Create(
        const SharedMemoryCreateOptions& options);
    static Result<SharedMemorySegment> Create(const std::string& name,
                                              uint64_t size);

    static Result<SharedMemorySegment> Open(
        const SharedMemoryOpenOptions& options);
    static Result<SharedMemorySegment> Open(const std::string& name,
                                            bool read_only);

    void* base() const noexcept { return base_; }
    uint64_t size() const noexcept { return size_; }
    bool huge_pages_requested() const noexcept {
        return huge_pages_requested_;
    }
    bool huge_pages_actual() const noexcept { return huge_pages_actual_; }
    bool huge_page_enabled() const noexcept { return huge_pages_actual(); }
    uint64_t actual_page_size() const noexcept { return actual_page_size_; }
    HugePageFallbackReason huge_page_fallback_reason() const noexcept {
        return huge_page_fallback_reason_;
    }
    int huge_page_fallback_errno() const noexcept {
        return huge_page_fallback_errno_;
    }
    bool read_only() const noexcept { return read_only_; }
    const std::string& name() const noexcept { return name_; }

    // Credential and mode snapshots captured with fstat before the backing is
    // mapped. Region Attach uses both marker and data identities so a permissive
    // or replaced object is rejected before lifecycle metadata can be changed.
    uint64_t marker_owner_user_id() const noexcept {
        return marker_owner_user_id_;
    }
    uint64_t marker_owner_group_id() const noexcept {
        return marker_owner_group_id_;
    }
    uint32_t marker_permissions() const noexcept { return marker_permissions_; }
    uint64_t backing_owner_user_id() const noexcept {
        return backing_owner_user_id_;
    }
    uint64_t backing_owner_group_id() const noexcept {
        return backing_owner_group_id_;
    }
    uint32_t backing_permissions() const noexcept { return backing_permissions_; }

    Status Close();

    // Atomically publishes UNLINKING before deleting the exact recorded
    // backing. The marker is removed last; failures leave retryable metadata.
    static Status Unlink(const std::string& name);

private:
    SharedMemorySegment() = default;

    void* base_ = nullptr;
    uint64_t size_ = 0;
    bool read_only_ = false;
    bool huge_pages_requested_ = false;
    bool huge_pages_actual_ = false;
    uint64_t actual_page_size_ = 0;
    HugePageFallbackReason huge_page_fallback_reason_ =
        HugePageFallbackReason::kNone;
    int huge_page_fallback_errno_ = 0;
    uint64_t marker_owner_user_id_ = 0;
    uint64_t marker_owner_group_id_ = 0;
    uint32_t marker_permissions_ = 0;
    uint64_t backing_owner_user_id_ = 0;
    uint64_t backing_owner_group_id_ = 0;
    uint32_t backing_permissions_ = 0;
    std::string name_;
};

}  // namespace mino

#endif  // MINO_PLATFORM_SHARED_MEMORY_H_
