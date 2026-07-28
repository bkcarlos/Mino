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

#include "mino/platform/shared_memory.h"

#include <cerrno>
#include <cstring>
#include <utility>

#if defined(__unix__) || defined(__APPLE__)
#define MINO_HAS_POSIX 1
#include <fcntl.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#else
#define MINO_HAS_POSIX 0
#endif

// MAP_HUGETLB is Linux-specific.
#if defined(__linux__)
#define MINO_HAS_HUGETLB 1
#else
#define MINO_HAS_HUGETLB 0
#endif

namespace mino {
namespace {

Status ErrnoStatus(std::string_view what) {
    return Status::Error(StatusCode::kInternal,
                         std::string(what) + ": " + std::strerror(errno));
}

// Validates a POSIX shm object name: must start with '/', be non-empty after
// it, and contain no further '/'.
Status ValidateName(const std::string& name) {
    if (name.empty() || name[0] != '/') {
        return Status::Error(StatusCode::kInvalidArgument,
                             "shm name must start with '/'");
    }
    if (name.size() == 1) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "shm name is empty after '/'");
    }
    if (name.find('/', 1) != std::string::npos) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "shm name must not contain additional '/'");
    }
    return Status::Ok();
}

uint64_t RoundUpToPageSize(uint64_t size) {
    const long page = ::sysconf(_SC_PAGESIZE);
    const uint64_t p = page > 0 ? static_cast<uint64_t>(page) : 4096;
    // Checked rounding; size here is already validated to be reasonable.
    const uint64_t rem = size % p;
    if (rem == 0) {
        return size;
    }
    return size + (p - rem);
}

}  // namespace

SharedMemorySegment::SharedMemorySegment(SharedMemorySegment&& other) noexcept {
    *this = std::move(other);
}

SharedMemorySegment& SharedMemorySegment::operator=(
    SharedMemorySegment&& other) noexcept {
    if (this != &other) {
        Close();
        base_ = other.base_;
        size_ = other.size_;
        read_only_ = other.read_only_;
        huge_pages_ = other.huge_pages_;
        name_ = std::move(other.name_);
        other.base_ = nullptr;
        other.size_ = 0;
        other.huge_pages_ = false;
    }
    return *this;
}

SharedMemorySegment::~SharedMemorySegment() { Close(); }

Status SharedMemorySegment::MapFd(int fd, uint64_t size, bool read_only,
                                  bool try_huge_pages, void** out_base,
                                  bool* out_used_huge) {
    *out_base = nullptr;
    *out_used_huge = false;

    const int prot = read_only ? PROT_READ : (PROT_READ | PROT_WRITE);

#if MINO_HAS_HUGETLB
    if (try_huge_pages) {
        // Attempt a huge-page-backed mapping. MAP_HUGETLB requires the size to
        // be aligned to the huge page size; we let mmap fail and degrade.
        void* p = ::mmap(nullptr, size, prot, MAP_SHARED | MAP_HUGETLB, fd, 0);
        if (p != MAP_FAILED) {
            *out_base = p;
            *out_used_huge = true;
            return Status::Ok();
        }
        // Fall through to normal pages (degrade gracefully).
    }
#else
    (void)try_huge_pages;
#endif

    void* p = ::mmap(nullptr, size, prot, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED) {
        return ErrnoStatus("mmap failed");
    }
    *out_base = p;
    *out_used_huge = false;
    return Status::Ok();
}

Result<SharedMemorySegment> SharedMemorySegment::Create(
    const SharedMemoryCreateOptions& options) {
    MINO_RETURN_IF_ERROR(ValidateName(options.name));
    if (options.size == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "shm size must be > 0");
    }

#if !MINO_HAS_POSIX
    return Status::Error(StatusCode::kUnsupported,
                         "POSIX shared memory not supported on this platform");
#else
    // O_EXCL: fail if it already exists so Create never clobbers an existing
    // region.
    int fd = ::shm_open(options.name.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
    if (fd < 0) {
        if (errno == EEXIST) {
            return Status::Error(StatusCode::kAlreadyExists,
                                 "shm object already exists: " + options.name);
        }
        return ErrnoStatus("shm_open(create) failed");
    }

    const uint64_t map_size = RoundUpToPageSize(options.size);
    if (::ftruncate(fd, static_cast<off_t>(map_size)) != 0) {
        Status s = ErrnoStatus("ftruncate failed");
        ::close(fd);
        ::shm_unlink(options.name.c_str());
        return s;
    }

    SharedMemorySegment seg;
    void* base = nullptr;
    bool used_huge = false;
    Status st = MapFd(fd, map_size, /*read_only=*/false,
                      options.use_huge_pages, &base, &used_huge);
    ::close(fd);  // fd no longer needed once mapped.
    if (!st.ok()) {
        ::shm_unlink(options.name.c_str());
        return st;
    }

    seg.base_ = base;
    seg.size_ = map_size;
    seg.read_only_ = false;
    seg.huge_pages_ = used_huge;
    seg.name_ = options.name;
    return seg;
#endif
}

Result<SharedMemorySegment> SharedMemorySegment::Create(
    const std::string& name, uint64_t size) {
    SharedMemoryCreateOptions options;
    options.name = name;
    options.size = size;
    options.use_huge_pages = false;
    return Create(options);
}

Result<SharedMemorySegment> SharedMemorySegment::Open(const std::string& name,
                                                      bool read_only) {
    MINO_RETURN_IF_ERROR(ValidateName(name));

#if !MINO_HAS_POSIX
    (void)read_only;
    return Status::Error(StatusCode::kUnsupported,
                         "POSIX shared memory not supported on this platform");
#else
    const int flags = read_only ? O_RDONLY : O_RDWR;
    int fd = ::shm_open(name.c_str(), flags, 0600);
    if (fd < 0) {
        if (errno == ENOENT) {
            return Status::Error(StatusCode::kNotFound,
                                 "shm object not found: " + name);
        }
        if (errno == EACCES) {
            return Status::Error(StatusCode::kPermissionDenied,
                                 "permission denied opening shm: " + name);
        }
        return ErrnoStatus("shm_open(open) failed");
    }

    struct stat st_buf;
    if (::fstat(fd, &st_buf) != 0) {
        Status s = ErrnoStatus("fstat failed");
        ::close(fd);
        return s;
    }
    const uint64_t size = static_cast<uint64_t>(st_buf.st_size);
    if (size == 0) {
        ::close(fd);
        return Status::Error(StatusCode::kCorruption,
                             "shm object has zero size: " + name);
    }

    SharedMemorySegment seg;
    void* base = nullptr;
    bool used_huge = false;
    // Never request huge pages on Open; the creator chose the backing.
    Status st = MapFd(fd, size, read_only, /*try_huge_pages=*/false, &base,
                      &used_huge);
    ::close(fd);
    if (!st.ok()) {
        return st;
    }

    seg.base_ = base;
    seg.size_ = size;
    seg.read_only_ = read_only;
    seg.huge_pages_ = used_huge;
    seg.name_ = name;
    return seg;
#endif
}

Status SharedMemorySegment::Close() {
    if (base_ == nullptr) {
        return Status::Ok();
    }
#if MINO_HAS_POSIX
    if (::munmap(base_, size_) != 0) {
        // Still clear state so we don't double-munmap; report the error.
        Status s = ErrnoStatus("munmap failed");
        base_ = nullptr;
        size_ = 0;
        huge_pages_ = false;
        return s;
    }
#endif
    base_ = nullptr;
    size_ = 0;
    huge_pages_ = false;
    return Status::Ok();
}

Status SharedMemorySegment::Unlink(const std::string& name) {
    MINO_RETURN_IF_ERROR(ValidateName(name));
#if !MINO_HAS_POSIX
    return Status::Error(StatusCode::kUnsupported,
                         "POSIX shared memory not supported on this platform");
#else
    if (::shm_unlink(name.c_str()) != 0) {
        if (errno == ENOENT) {
            return Status::Error(StatusCode::kNotFound,
                                 "shm object not found: " + name);
        }
        return ErrnoStatus("shm_unlink failed");
    }
    return Status::Ok();
#endif
}

}  // namespace mino
