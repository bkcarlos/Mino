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

#ifndef MINO_PLATFORM_PROCESS_IDENTITY_H_
#define MINO_PLATFORM_PROCESS_IDENTITY_H_

#include <array>
#include <cstdint>
#include <string>

namespace mino {

// ProcessIdentity uniquely identifies a process incarnation (design doc
// section 4.3).
//
// `process_epoch` must distinguish PID reuse. On Linux the first version
// combines PID, process start time, and a random boot epoch (design doc
// section 4.3).
//
// The struct is a fixed-size, trivially-copyable POD so it can be stored in
// shared memory and serialized deterministically.
struct ProcessIdentity {
    uint64_t node_id = 0;
    uint64_t process_id = 0;
    uint64_t process_epoch = 0;
    uint64_t start_time_ns = 0;

    // Returns the identity of the current process. The node_id is derived
    // from the host, process_id from getpid(), start_time_ns from the process
    // start time, and process_epoch from a combination that distinguishes PID
    // reuse (PID + start time + a random component).
    //
    // The returned value is computed once per process and cached; repeated
    // calls return the same identity.
    static const ProcessIdentity& Current();

    // Returns true if all fields are zero (default-constructed / unset).
    bool IsZero() const noexcept {
        return node_id == 0 && process_id == 0 && process_epoch == 0 &&
               start_time_ns == 0;
    }

    friend bool operator==(const ProcessIdentity& lhs,
                           const ProcessIdentity& rhs) noexcept {
        return lhs.node_id == rhs.node_id && lhs.process_id == rhs.process_id &&
               lhs.process_epoch == rhs.process_epoch &&
               lhs.start_time_ns == rhs.start_time_ns;
    }
    friend bool operator!=(const ProcessIdentity& lhs,
                           const ProcessIdentity& rhs) noexcept {
        return !(lhs == rhs);
    }

    std::string ToString() const;

    // ------------------------------------------------------------------
    // Serialization
    // ------------------------------------------------------------------
    // Fixed-size wire/storage encoding, little-endian field order as declared.
    // Total size: 4 * sizeof(uint64_t) == 32 bytes.
    static constexpr size_t kSerializedSize = 4 * sizeof(uint64_t);

    // Serializes into `dest`, which must point to at least kSerializedSize
    // bytes. The encoding is deterministic and byte-order preserving (fields
    // are written in declaration order, each as 8 raw bytes).
    void SerializeTo(std::array<std::byte, kSerializedSize>& dest) const
        noexcept;

    // Deserializes from `src`, which must contain at least kSerializedSize
    // bytes previously produced by SerializeTo.
    static ProcessIdentity DeserializeFrom(
        const std::array<std::byte, kSerializedSize>& src) noexcept;
};

static_assert(sizeof(ProcessIdentity) == 32,
              "ProcessIdentity must remain 4 x uint64_t");
static_assert(std::is_trivially_copyable_v<ProcessIdentity>);
static_assert(std::is_standard_layout_v<ProcessIdentity>);

enum class ProcessIdentityLiveness {
    kAlive,
    kDead,
    // The platform or current permissions cannot safely distinguish this
    // incarnation from a recycled PID. Destructive recovery must treat this as
    // live/blocked, never as dead.
    kUnknown,
};

// Probes the exact recorded process incarnation. Linux validates PID, process
// start time, and zombie state through /proc; macOS uses KERN_PROC_PID. A PID
// that exists with a different start time is kDead (PID reuse), while an
// uncheckable incarnation is kUnknown.
ProcessIdentityLiveness ProbeProcessIdentity(
    const ProcessIdentity& identity) noexcept;

// Compatibility convenience. Unknown is deliberately not reported as alive;
// recovery code must use ProbeProcessIdentity() and handle kUnknown explicitly.
bool IsProcessIdentityAlive(const ProcessIdentity& identity) noexcept;

}  // namespace mino

#endif  // MINO_PLATFORM_PROCESS_IDENTITY_H_
