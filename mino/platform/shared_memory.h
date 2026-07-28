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

// Options controlling SharedMemorySegment creation.
struct SharedMemoryCreateOptions {
    // POSIX shared memory object name (e.g. "/my_region"). Must start with
    // '/'. Stored without platform-specific prefixes.
    std::string name;

    // Size of the segment in bytes. Must be > 0 and will be rounded up to a
    // multiple of the system page size for mapping.
    uint64_t size = 0;

    // If true, attempt to back the mapping with huge pages (MAP_HUGETLB on
    // Linux). If huge pages are unavailable, creation transparently degrades
    // to normal pages. Use huge_page_enabled() on the returned segment to
    // detect which backing was actually used.
    bool use_huge_pages = false;
};

// SharedMemorySegment is a thin, owning RAII wrapper over POSIX
// shm_open/ftruncate/mmap/munmap/shm_unlink (D1-02).
//
// The segment maps a contiguous virtual address range. Ownership of the
// mapping is tied to the object's lifetime; Close() unmaps. Unlink() is a
// separate static operation that removes the named object (it does not
// require an open mapping).
//
// All system call failures are converted to Status; no exceptions cross the
// API boundary.
class SharedMemorySegment {
public:
    SharedMemorySegment(const SharedMemorySegment&) = delete;
    SharedMemorySegment& operator=(const SharedMemorySegment&) = delete;

    // Move-only. The moved-from object is left unmapped.
    SharedMemorySegment(SharedMemorySegment&& other) noexcept;
    SharedMemorySegment& operator=(SharedMemorySegment&& other) noexcept;

    // Unmaps the segment if still mapped. Does not unlink the named object.
    ~SharedMemorySegment();

    // Creates a new shared memory object of `options.size` bytes and maps it
    // read-write. Fails with kAlreadyExists if the object already exists.
    //
    // If `options.use_huge_pages` is set, attempts a MAP_HUGETLB-backed
    // mapping first and degrades to normal pages if huge pages are
    // unavailable (e.g. not reserved on the host).
    static Result<SharedMemorySegment> Create(
        const SharedMemoryCreateOptions& options);

    // Convenience overload: Create with default (non-huge-page) options.
    static Result<SharedMemorySegment> Create(const std::string& name,
                                              uint64_t size);

    // Opens and maps an existing shared memory object. Fails with kNotFound
    // if the object does not exist. The mapped size is discovered from the
    // object's fstat size.
    static Result<SharedMemorySegment> Open(const std::string& name,
                                            bool read_only);

    // Base address of the mapping, or nullptr if not mapped.
    void* base() const noexcept { return base_; }

    // Size of the mapping in bytes, or 0 if not mapped.
    uint64_t size() const noexcept { return size_; }

    // True if the mapping was backed by huge pages.
    bool huge_page_enabled() const noexcept { return huge_pages_; }

    // True if the mapping is read-only.
    bool read_only() const noexcept { return read_only_; }

    // The name this segment was opened/created with.
    const std::string& name() const noexcept { return name_; }

    // Unmaps the segment. Safe to call multiple times. After Close(), base()
    // returns nullptr and size() returns 0. Does not unlink the object.
    Status Close();

    // Removes the named shared memory object. Returns OK if it existed and
    // was removed; kNotFound if it did not exist.
    static Status Unlink(const std::string& name);

private:
    SharedMemorySegment() = default;

    // Maps `fd` of `size` bytes with the given protection/flags, trying huge
    // pages first if requested. On success fills base_/size_/huge_pages_.
    static Status MapFd(int fd, uint64_t size, bool read_only,
                        bool try_huge_pages, void** out_base,
                        bool* out_used_huge);

    void* base_ = nullptr;
    uint64_t size_ = 0;
    bool read_only_ = false;
    bool huge_pages_ = false;
    std::string name_;
};

}  // namespace mino

#endif  // MINO_PLATFORM_SHARED_MEMORY_H_
