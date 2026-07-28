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

#ifndef MINO_SHM_CHANNEL_QUEUE_FULL_POLICY_H_
#define MINO_SHM_CHANNEL_QUEUE_FULL_POLICY_H_

#include <cstdint>

namespace mino {

// Policy applied when a producer tries to publish into a full channel
// (design doc 9.8). Applicability per channel kind:
//
//   | policy        | SPSC/MPSC | Broadcast | Work Queue |
//   |---------------|-----------|-----------|------------|
//   | kBlock        | yes       | yes       | yes        |
//   | kFail         | yes       | yes       | yes        |
//   | kDropNewest   | yes       | yes (gap) | yes        |
//   | kDropOldest   | yes       | yes (gap) | yes        |
//   | kSample       | yes       | yes       | NO (breaks exactly-once) |
//
// kDropOldest must never reuse payload memory that a reader still borrows:
// the index reference is dropped/forced forward and the payload is reclaimed
// only after no borrows remain (design doc 9.8).
enum class QueueFullPolicy : uint8_t {
    // Wait until space is available (blocking reservation).
    kBlock = 0,
    // Fail the publish immediately with kResourceExhausted.
    kFail = 1,
    // Drop the incoming message and report success-with-drop (kDegraded).
    kDropNewest = 2,
    // Force the oldest slot out (advance the slowest cursor) to make room.
    kDropOldest = 3,
    // Probabilistically admit messages at a configured sample rate; all
    // subscribers of a broadcast see the same admitted subset.
    kSample = 4,
};

}  // namespace mino

#endif  // MINO_SHM_CHANNEL_QUEUE_FULL_POLICY_H_
