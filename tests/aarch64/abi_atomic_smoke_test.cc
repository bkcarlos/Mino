// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "mino/abi/shm_handle.h"
#include "mino/shm/channel/index_slot.h"
#include "mino/shm/region/superblock.h"

static_assert(sizeof(mino::ShmHandle) == 16);
static_assert(alignof(mino::ShmHandle) == 8);
static_assert(sizeof(mino::SuperBlock) == 256);
static_assert(alignof(mino::SuperBlock) == 8);
static_assert(sizeof(mino::IndexSlot) == 128);
static_assert(alignof(mino::IndexSlot) == 64);
static_assert(std::atomic<bool>::is_always_lock_free);
static_assert(std::atomic<uint16_t>::is_always_lock_free);
static_assert(std::atomic<uint32_t>::is_always_lock_free);
static_assert(std::atomic<uint64_t>::is_always_lock_free);

int main() {
    std::atomic<uint64_t> value{1};
    uint64_t expected = 1;
    if (!value.compare_exchange_strong(expected, 2,
                                       std::memory_order_seq_cst)) {
        return 1;
    }
    return value.fetch_add(1, std::memory_order_seq_cst) == 2 ? 0 : 1;
}
