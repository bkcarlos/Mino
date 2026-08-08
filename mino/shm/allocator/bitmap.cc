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

#include "mino/shm/allocator/bitmap.h"

namespace mino {
namespace {

constexpr uint64_t LowBits(uint32_t count) {
    return count == 64 ? ~uint64_t{0} : (uint64_t{1} << count) - 1;
}

}  // namespace

Result<ShardedBitmap> ShardedBitmap::Create(std::atomic<uint64_t>* storage,
                                            uint32_t shard_count) {
    if (storage == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "bitmap storage must not be null");
    }
    if (shard_count == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "bitmap requires at least one shard");
    }
    ShardedBitmap bitmap;
    bitmap.storage_ = storage;
    bitmap.shard_count_ = shard_count;
    return bitmap;
}

Result<uint32_t> ShardedBitmap::FindAndSetFreeBit(uint32_t shard_hint) {
    const uint32_t start = shard_hint % shard_count_;
    for (uint32_t probe = 0; probe < shard_count_; ++probe) {
        const uint32_t shard_index = (start + probe) % shard_count_;
        std::atomic<uint64_t>& shard = storage_[shard_index];

        uint64_t word = shard.load(std::memory_order_acquire);
        while (word != ~uint64_t{0}) {
            // Least significant clear bit.
            const uint32_t bit = static_cast<uint32_t>(__builtin_ctzll(~word));
            const uint64_t desired = word | (uint64_t{1} << bit);
            // Success: we exclusively own the bit. Failure: another thread
            // changed the word; retry with the refreshed value.
            if (shard.compare_exchange_weak(word, desired,
                                            std::memory_order_acq_rel,
                                            std::memory_order_acquire)) {
                return shard_index * kBitmapShardBits + bit;
            }
        }
    }
    return Status::Error(StatusCode::kResourceExhausted,
                         "allocation bitmap is full");
}

Result<uint32_t> ShardedBitmap::FindAndSetFreeBitInRange(uint32_t begin,
                                                         uint32_t end) {
    MINO_ASSIGN_OR_RETURN(
        BitmapClaim claim,
        FindAndSetFreeBitInRangeHinted(begin, end, begin));
    return claim.bit_index;
}

Result<BitmapClaim> ShardedBitmap::FindAndSetFreeBitInRangeHinted(
    uint32_t begin, uint32_t end, uint32_t bit_hint) {
    if (begin >= end || end > bit_count()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "invalid bitmap allocation range");
    }
    if (bit_hint < begin || bit_hint >= end) {
        bit_hint = begin;
    }

    const uint32_t first_shard = begin / kBitmapShardBits;
    const uint32_t last_shard = (end - 1) / kBitmapShardBits;
    const uint32_t range_shards = last_shard - first_shard + 1;
    const uint32_t start_shard = bit_hint / kBitmapShardBits;

    for (uint32_t probe = 0; probe < range_shards; ++probe) {
        const uint32_t shard_index =
            first_shard + (start_shard - first_shard + probe) % range_shards;
        const uint32_t lower = shard_index == first_shard
                                   ? begin % kBitmapShardBits
                                   : 0;
        const uint32_t upper = shard_index == last_shard
                                   ? (end - 1) % kBitmapShardBits + 1
                                   : kBitmapShardBits;
        const uint64_t valid_mask = LowBits(upper) & ~LowBits(lower);
        std::atomic<uint64_t>& shard = storage_[shard_index];
        uint64_t word = shard.load(std::memory_order_acquire);

        for (;;) {
            const uint64_t available = ~word & valid_mask;
            if (available == 0) break;

            uint64_t candidates = available;
            if (probe == 0) {
                const uint32_t hint_in_shard = bit_hint % kBitmapShardBits;
                const uint64_t at_or_after_hint =
                    available & ~LowBits(hint_in_shard);
                if (at_or_after_hint != 0) candidates = at_or_after_hint;
            }
            const uint32_t bit =
                static_cast<uint32_t>(__builtin_ctzll(candidates));
            const uint64_t desired = word | (uint64_t{1} << bit);
            if (shard.compare_exchange_weak(word, desired,
                                            std::memory_order_acq_rel,
                                            std::memory_order_acquire)) {
                return BitmapClaim{
                    .bit_index = shard_index * kBitmapShardBits + bit,
                    .shard_index = shard_index,
                    .shards_probed = probe + 1,
                };
            }
        }
    }
    return Status::Error(StatusCode::kResourceExhausted,
                         "allocation bitmap range is full");
}

void ShardedBitmap::SetRange(std::uint32_t begin, std::uint32_t end) {
    if (begin >= end || end > bit_count()) {
        return;
    }
    for (uint32_t index = begin; index < end; ++index) {
        storage_[index / kBitmapShardBits].fetch_or(
            uint64_t{1} << (index % kBitmapShardBits), std::memory_order_acq_rel);
    }
}

Status ShardedBitmap::ClearBit(uint32_t index) {
    if (index >= bit_count()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "bitmap index out of range");
    }
    const uint64_t mask = uint64_t{1} << (index % kBitmapShardBits);
    const uint64_t prev =
        storage_[index / kBitmapShardBits].fetch_and(~mask, std::memory_order_acq_rel);
    if ((prev & mask) == 0) {
        return Status::Error(StatusCode::kNotFound,
                             "bitmap bit was not set (double clear)");
    }
    return Status::Ok();
}

bool ShardedBitmap::IsSet(uint32_t index) const {
    if (index >= bit_count()) {
        return false;
    }
    const uint64_t word =
        storage_[index / kBitmapShardBits].load(std::memory_order_acquire);
    return (word & (uint64_t{1} << (index % kBitmapShardBits))) != 0;
}

}  // namespace mino
