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

#ifndef MINO_COMMON_IDS_H_
#define MINO_COMMON_IDS_H_

#include <cstdint>
#include <functional>
#include <string>
#include <type_traits>

namespace mino {

// Strongly-typed identifiers (design doc section 5.2).
//
// Raw integer types must never be mixed across different ID domains. Each ID
// type wraps a fixed-width integer in a distinct struct, disables implicit
// conversion, and exposes only comparison and formatting.
//
// The underlying field is named `value` per design doc section 5.2 and is
// public to keep these types trivial aggregates (usable in shared-memory
// layouts and constexpr contexts). Construction from a raw integer is always
// explicit.
//
// Serialization note: these C++ wrappers are in-process types only. When
// writing to disk or network, serialize the underlying fixed-width integer
// explicitly (design doc section 5.2).

#define MINO_DEFINE_STRONG_ID(Name, UnderlyingType)                            \
    struct Name {                                                              \
        UnderlyingType value = 0;                                              \
                                                                               \
        friend constexpr bool operator==(Name lhs, Name rhs) {                 \
            return lhs.value == rhs.value;                                     \
        }                                                                      \
        friend constexpr bool operator!=(Name lhs, Name rhs) {                 \
            return !(lhs == rhs);                                              \
        }                                                                      \
        friend constexpr bool operator<(Name lhs, Name rhs) {                  \
            return lhs.value < rhs.value;                                      \
        }                                                                      \
                                                                               \
        std::string ToString() const {                                         \
            return #Name "(" + std::to_string(value) + ")";                    \
        }                                                                      \
    };                                                                         \
    static_assert(std::is_trivially_copyable_v<Name>);                         \
    static_assert(std::is_standard_layout_v<Name>)

MINO_DEFINE_STRONG_ID(TopicId, uint32_t);
MINO_DEFINE_STRONG_ID(TypeId, uint32_t);
MINO_DEFINE_STRONG_ID(NodeId, uint64_t);
MINO_DEFINE_STRONG_ID(PublisherId, uint64_t);
MINO_DEFINE_STRONG_ID(SubscriberId, uint32_t);
MINO_DEFINE_STRONG_ID(SchemaId, uint64_t);

#undef MINO_DEFINE_STRONG_ID

}  // namespace mino

// std::hash specializations so the IDs can be used as keys in unordered
// containers. Hashing is value-based and domain-tag-free because each ID type
// is a distinct C++ type.
namespace std {

#define MINO_ID_HASH(Name)                                                     \
    template <>                                                                \
    struct hash<mino::Name> {                                                  \
        size_t operator()(mino::Name id) const noexcept {                      \
            return std::hash<decltype(id.value)>{}(id.value);                  \
        }                                                                      \
    }

MINO_ID_HASH(TopicId);
MINO_ID_HASH(TypeId);
MINO_ID_HASH(NodeId);
MINO_ID_HASH(PublisherId);
MINO_ID_HASH(SubscriberId);
MINO_ID_HASH(SchemaId);

#undef MINO_ID_HASH

}  // namespace std

#endif  // MINO_COMMON_IDS_H_
