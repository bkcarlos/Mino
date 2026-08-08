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

// Sharded allocation bitmap for the Central Slab Allocator.
// See docs/Mino_详细设计文档.md sections 8.1 and 8.3.

#ifndef MINO_SHM_ALLOCATOR_BITMAP_H_
#define MINO_SHM_ALLOCATOR_BITMAP_H_

#include <atomic>
#include <cstdint>

#include "mino/common/result.h"
#include "mino/common/status.h"

namespace mino {

// Number of slots tracked by one bitmap shard. Shards are whole cache lines
// so that concurrent allocations in different shards do not false-share.
inline constexpr uint32_t kBitmapShardBits = 64;

// ShardedBitmap tracks slot occupancy as an array of 64-bit atomic words
// ("shards"), one bit per slot, stored in shared memory (design doc 8.1).
//
// Concurrency model:
//   - FindAndSetFreeBit claims a bit with a single CAS; on success the caller
//     owns the slot exclusively until ClearBit. No ABA is possible because a
//     claimed bit is not reused until it is explicitly cleared after the
//     full lifecycle protocol (design doc 8.4).
//   - ClearBit releases the bit. Only the owner (or recovery, observing the
//     crash-recovery invariant of design doc 8.3) may clear.
//   - IsSet is a point-in-time observation (acquire) used by recovery scans
//     and Inspect; it must be combined with object_state to draw conclusions.
struct BitmapClaim {
    uint32_t bit_index = 0;
    uint32_t shard_index = 0;
    uint32_t shards_probed = 0;
};

class ShardedBitmap {
public:
    // Initializes the bitmap over `shard_count` shards located at `storage`
    // (which must point to shared memory holding exactly shard_count
    // std::atomic<uint64_t> values, already zero-initialized).
    // Fails with kInvalidArgument if shard_count is zero or storage is null.
    static Result<ShardedBitmap> Create(std::atomic<uint64_t>* storage,
                                        uint32_t shard_count);

    // Scans shards starting at `shard_hint % shard_count()` for a clear bit
    // and claims it with a CAS (acq_rel on success, acquire on failure).
    // Returns the claimed global bit index, or kResourceExhausted when every
    // bit is set (design doc 8.3 steps 3-5: shard selection, free-bit
    // search, CAS claim).
    Result<uint32_t> FindAndSetFreeBit(uint32_t shard_hint);

    // Claims one clear bit in [begin, end). This bounded variant prevents a
    // size-class allocation from spilling into another class's bit range.
    Result<uint32_t> FindAndSetFreeBitInRange(uint32_t begin, uint32_t end);

    // Word-oriented bounded claim used by high-contention allocators. The scan
    // begins in the shard containing bit_hint, wraps exactly once inside the
    // range, and never examines a neighbouring size class. BitmapClaim reports
    // whether the first (hinted) shard succeeded without adding shared state.
    Result<BitmapClaim> FindAndSetFreeBitInRangeHinted(uint32_t begin,
                                                      uint32_t end,
                                                      uint32_t bit_hint);

    // Claims bits in the half-open global index range [begin, end) that are
    // currently clear. `end` must be <= bit_count() and `begin` <= `end`.
    // Used by recovery to rebuild an occupancy snapshot; bits already set
    // are left untouched.
    void SetRange(std::uint32_t begin, std::uint32_t end);

    // Clears bit `index` (release). Fails with kInvalidArgument if index is
    // out of range, and with kNotFound if the bit was not set (double-clear
    // indicates a lifecycle bug).
    Status ClearBit(uint32_t index);

    // Returns true iff bit `index` is currently set (acquire). Returns false
    // for out-of-range indexes.
    bool IsSet(uint32_t index) const;

    uint32_t shard_count() const { return shard_count_; }
    uint32_t bit_count() const { return shard_count_ * kBitmapShardBits; }

    // Default-constructible so facades can hold ShardedBitmap members;
    // Create() is the only supported way to build a usable bitmap.
    ShardedBitmap() = default;

private:
    std::atomic<uint64_t>* storage_ = nullptr;
    uint32_t shard_count_ = 0;
};

}  // namespace mino

#endif  // MINO_SHM_ALLOCATOR_BITMAP_H_
