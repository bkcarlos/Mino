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

#include "mino/shm/allocator/class_table.h"

#include <algorithm>
#include <cassert>
#include <limits>

namespace mino {

ClassTableConfig DefaultClassTableConfig() {
    ClassTableConfig config;
    // Default size classes from design doc 8.1. Slot counts are conservative
    // placeholders; real deployments must configure counts from the measured
    // message size distribution.
    config.classes = {
        {.slot_size = 64, .slot_count = 4096},
        {.slot_size = 256, .slot_count = 2048},
        {.slot_size = 2u * 1024u, .slot_count = 1024},
        {.slot_size = 64u * 1024u, .slot_count = 256},
    };
    return config;
}

Result<ClassTable> ClassTable::Create(const ClassTableConfig& config) {
    if (config.classes.empty()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "ClassTable requires at least one class");
    }
    if (config.classes.size() > kMaxClassCount) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "ClassTable has too many classes");
    }

    ClassTable table;
    table.classes_.reserve(config.classes.size());

    uint32_t prev_slot_size = 0;
    uint64_t next_bit = 0;  // next class starts on a 64-bit shard boundary
    for (uint32_t i = 0; i < config.classes.size(); ++i) {
        const ClassTableConfig::Entry& entry = config.classes[i];
        if (entry.slot_size == 0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "class slot_size must be positive");
        }
        if (entry.slot_size > std::numeric_limits<uint32_t>::max() / 2) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "class slot_size too large");
        }
        if (entry.slot_count == 0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "class slot_count must be positive");
        }
        if (i > 0 && entry.slot_size <= prev_slot_size) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "class slot sizes must be strictly increasing");
        }

        ClassDescriptor desc;
        desc.class_id = static_cast<uint16_t>(i);
        desc.slot_size = entry.slot_size;
        desc.slot_count = entry.slot_count;
        desc.bitmap_shard_offset = static_cast<uint32_t>(next_bit);
        table.classes_.push_back(desc);
        prev_slot_size = entry.slot_size;

        next_bit += entry.slot_count;
        // Round up to the next 64-bit shard boundary so that the shard hint
        // of design doc 8.3 step 3 never scans a neighbouring class's bits.
        next_bit = (next_bit + 63) / 64 * 64;
        if (next_bit > std::numeric_limits<uint32_t>::max()) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "total slot count overflows uint32_t");
        }
    }

    table.total_slots_ = static_cast<uint32_t>(next_bit);
    return table;
}

Result<uint16_t> ClassTable::FindClass(uint32_t object_size) const {
    if (object_size == 0) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "object_size must be positive");
    }
    // Classes are sorted by slot_size ascending (enforced at Create); the
    // first class that fits is the smallest one (design doc 8.3 step 2).
    for (const ClassDescriptor& desc : classes_) {
        if (object_size <= desc.slot_size) {
            return desc.class_id;
        }
    }
    return Status::Error(StatusCode::kNotFound,
                         "no size class can hold the requested object size");
}

const ClassDescriptor& ClassTable::GetClass(uint16_t class_id) const {
    assert(class_id < classes_.size() && "class_id out of range");
    return classes_[class_id];
}

}  // namespace mino
