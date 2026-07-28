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

#include "mino/shm/allocator/generation_array.h"

namespace mino {

Result<GenerationArray> GenerationArray::Create(std::atomic<uint32_t>* storage,
                                                uint32_t slot_count) {
    if (storage == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "generation array storage must not be null");
    }
    if (slot_count == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "generation array requires at least one slot");
    }
    GenerationArray array;
    array.storage_ = storage;
    array.slot_count_ = slot_count;
    return array;
}

uint32_t GenerationArray::Increment(uint32_t slot_index) {
    if (slot_index >= slot_count_) {
        return kGenerationDraining;
    }
    std::atomic<uint32_t>& cell = storage_[slot_index];
    const uint32_t current = cell.load(std::memory_order_relaxed);
    if (current == kGenerationDraining) {
        // Design doc 8.3 step 6 / ADR-0002: never wrap; the Class/Region
        // must be drained and migrated instead.
        return kGenerationDraining;
    }
    return cell.fetch_add(1, std::memory_order_relaxed) + 1;
}

uint32_t GenerationArray::Get(uint32_t slot_index) const {
    if (slot_index >= slot_count_) {
        return 0;
    }
    return storage_[slot_index].load(std::memory_order_acquire);
}

}  // namespace mino
