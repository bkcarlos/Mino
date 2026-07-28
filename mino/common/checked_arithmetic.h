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

#ifndef MINO_COMMON_CHECKED_ARITHMETIC_H_
#define MINO_COMMON_CHECKED_ARITHMETIC_H_

#include <cstdint>
#include <limits>

namespace mino {

// Checked unsigned arithmetic helpers (design doc section 6.3: "任何长度加法
// 必须使用 Checked Arithmetic").
//
// Each function returns true and writes the result to *out on success, or
// returns false on overflow/underflow (leaving *out unmodified). These are
// used pervasively when computing shared-memory offsets so that a corrupt or
// malicious header cannot trigger an out-of-bounds access via wraparound.

inline bool CheckedAddU64(uint64_t a, uint64_t b, uint64_t* out) {
    if (a > std::numeric_limits<uint64_t>::max() - b) {
        return false;
    }
    *out = a + b;
    return true;
}

inline bool CheckedAddU32(uint32_t a, uint32_t b, uint32_t* out) {
    const uint64_t r = static_cast<uint64_t>(a) + static_cast<uint64_t>(b);
    if (r > std::numeric_limits<uint32_t>::max()) {
        return false;
    }
    *out = static_cast<uint32_t>(r);
    return true;
}

inline bool CheckedSubU64(uint64_t a, uint64_t b, uint64_t* out) {
    if (a < b) {
        return false;
    }
    *out = a - b;
    return true;
}

inline bool CheckedMulU64(uint64_t a, uint64_t b, uint64_t* out) {
    if (a != 0 && b > std::numeric_limits<uint64_t>::max() / a) {
        return false;
    }
    *out = a * b;
    return true;
}

// Aligns `value` up to `alignment` (which must be a power of two). Returns
// false on overflow.
inline bool CheckedAlignUpU64(uint64_t value, uint64_t alignment,
                              uint64_t* out) {
    if (alignment == 0 || (alignment & (alignment - 1)) != 0) {
        return false;  // not a power of two
    }
    uint64_t with_mask;
    if (!CheckedAddU64(value, alignment - 1, &with_mask)) {
        return false;
    }
    *out = with_mask & ~(alignment - 1);
    return true;
}

}  // namespace mino

#endif  // MINO_COMMON_CHECKED_ARITHMETIC_H_
