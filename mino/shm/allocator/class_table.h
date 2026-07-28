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

// Size-class table for the Central Slab Allocator.
// See docs/Mino_详细设计文档.md sections 8.1 and 8.2.

#ifndef MINO_SHM_ALLOCATOR_CLASS_TABLE_H_
#define MINO_SHM_ALLOCATOR_CLASS_TABLE_H_

#include <cstdint>
#include <vector>

#include "mino/common/result.h"
#include "mino/common/status.h"

namespace mino {

// Upper bound on the number of size classes in one Region. Class ids are
// uint16_t in SlabHeader, and realistic configurations use a handful of
// classes; 256 keeps the metadata bound tight.
inline constexpr uint32_t kMaxClassCount = 256;

// ClassDescriptor describes one size class of the allocator (design doc 8.1).
// All slots of a class are contiguous inside the data region. To keep the
// shard-hinted free-bit search of design doc 8.3 step 3 inside one class,
// every class begins on a 64-bit shard boundary; bitmap_shard_offset is the
// global index of the class's first slot bit.
struct ClassDescriptor {
    uint16_t class_id;          // Index into the class table.
    uint32_t slot_size;         // Payload bytes per slot (excludes SlabHeader).
    uint32_t slot_count;        // Number of slots of this class.
    uint32_t bitmap_shard_offset;  // First bit of this class in the shared
                                   // ShardedBitmap (slot index base).
};

// ClassTableConfig configures the size classes of one Region (design doc
// 8.1: "Class 大小必须通过真实消息尺寸分布确定，不作为永久 ABI 写死在代码
// 中"). Class sizes come from configuration; only the defaults are provided
// as a convenience by DefaultClassTableConfig().
struct ClassTableConfig {
    struct Entry {
        uint32_t slot_size;   // Payload bytes per slot.
        uint32_t slot_count;  // Number of slots of this class.
    };
    std::vector<Entry> classes;
};

// Returns the default configuration (64 B, 256 B, 2 KiB, 64 KiB payload)
// from design doc 8.1. Slot counts default to a small, test-friendly value;
// production deployments must derive both sizes and counts from the real
// message size distribution.
ClassTableConfig DefaultClassTableConfig();

// ClassTable selects the smallest size class able to hold an object.
// The table of a Region is immutable after Region creation (design doc 8.1).
//
// ClassTable is default-constructible (empty) so that facades such as
// CentralSlabAllocator can hold it as a member; a default-constructed table
// has no classes and FindClass always fails with kNotFound.
class ClassTable {
public:
    // Builds a ClassTable from config. Fails with kInvalidArgument if:
    //   - config.classes is empty or has more than kMaxClassCount entries;
    //   - a slot_size is zero or exceeds UINT32_MAX / 2;
    //   - slot sizes are not strictly increasing;
    //   - a slot_count is zero or the total slot count overflows uint32_t.
    static Result<ClassTable> Create(const ClassTableConfig& config);

    // Returns the id of the smallest class whose slot_size >= object_size,
    // or kNotFound if no class can hold the object (design doc 8.3 step 2).
    // Zero-size requests are invalid (kInvalidArgument).
    Result<uint16_t> FindClass(uint32_t object_size) const;

    // Returns the descriptor for class_id. Precondition: class_id <
    // class_count() (checked by assertion in debug builds).
    const ClassDescriptor& GetClass(uint16_t class_id) const;

    uint16_t class_count() const { return static_cast<uint16_t>(classes_.size()); }

    // Total number of slots across all classes; also the number of bits the
    // shared ShardedBitmap and the GenerationArray must provide.
    uint32_t total_slot_count() const { return total_slots_; }

    // Largest payload any class can hold.
    uint32_t max_object_size() const { return classes_.empty() ? 0 : classes_.back().slot_size; }

    // True for tables built by Create(); a default-constructed table is
    // empty and cannot serve allocations.
    bool valid() const { return !classes_.empty(); }

    // Public default constructor so facades can hold ClassTable members;
    // Create() is the only supported way to build a usable table.
    ClassTable() = default;

private:
    std::vector<ClassDescriptor> classes_;
    uint32_t total_slots_ = 0;
};

}  // namespace mino

#endif  // MINO_SHM_ALLOCATOR_CLASS_TABLE_H_
