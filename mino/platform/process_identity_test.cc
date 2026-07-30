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

#include "mino/platform/process_identity.h"

#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <set>
#include <thread>
#include <vector>

#if defined(__unix__) || defined(__APPLE__)
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace mino {
namespace {

TEST(ProcessIdentityTest, CurrentReturnsSameValueOnRepeatCalls) {
    const ProcessIdentity& a = ProcessIdentity::Current();
    const ProcessIdentity& b = ProcessIdentity::Current();
    EXPECT_EQ(&a, &b);          // cached: same object
    EXPECT_EQ(a, b);
}

TEST(ProcessIdentityTest, CurrentHasPid) {
    const ProcessIdentity& id = ProcessIdentity::Current();
    EXPECT_EQ(id.process_id, static_cast<uint64_t>(::getpid()));
    EXPECT_NE(id.process_id, 0u);
}

TEST(ProcessIdentityTest, CurrentHasNonZeroEpoch) {
    const ProcessIdentity& id = ProcessIdentity::Current();
    // process_epoch must distinguish PID reuse, so it must be populated.
    EXPECT_NE(id.process_epoch, 0u);
}

TEST(ProcessIdentityTest, CurrentHasStartTime) {
    const ProcessIdentity& id = ProcessIdentity::Current();
    EXPECT_NE(id.start_time_ns, 0u);
}

TEST(ProcessIdentityTest, EpochUniqueAcrossProcesses) {
    // Fork a child and verify its identity differs from the parent's, and in
    // particular that the epoch differs even though the child may share the
    // same start_time_ns derivation. This validates the random component.
#if defined(__unix__) || defined(__APPLE__)
    const ProcessIdentity& parent = ProcessIdentity::Current();

    int pipefd[2];
    ASSERT_EQ(::pipe(pipefd), 0);

    pid_t pid = ::fork();
    ASSERT_GE(pid, 0);
    if (pid == 0) {
        // Child: compute its own Current() and send the epoch back.
        ::close(pipefd[0]);
        const ProcessIdentity& child = ProcessIdentity::Current();
        uint64_t epoch = child.process_epoch;
        ssize_t w = ::write(pipefd[1], &epoch, sizeof(epoch));
        (void)w;
        ::close(pipefd[1]);
        _exit(0);
    }
    ::close(pipefd[1]);
    uint64_t child_epoch = 0;
    ssize_t r = ::read(pipefd[0], &child_epoch, sizeof(child_epoch));
    ::close(pipefd[0]);
    ASSERT_EQ(r, static_cast<ssize_t>(sizeof(child_epoch)));
    int status = 0;
    ::waitpid(pid, &status, 0);

    // Child has a different PID and must have a different epoch.
    EXPECT_NE(child_epoch, 0u);
    EXPECT_NE(child_epoch, parent.process_epoch);
#else
    GTEST_SKIP() << "requires POSIX fork";
#endif
}

TEST(ProcessIdentityTest, LivenessValidatesProcessIncarnation) {
    const ProcessIdentity current = ProcessIdentity::Current();
    EXPECT_TRUE(IsProcessIdentityAlive(current));

    ProcessIdentity recycled = current;
    recycled.process_epoch ^= 1u;
    EXPECT_FALSE(IsProcessIdentityAlive(recycled));

    EXPECT_FALSE(IsProcessIdentityAlive(ProcessIdentity{}));
}

TEST(ProcessIdentityTest, EqualityOperators) {
    ProcessIdentity a;
    a.node_id = 1;
    a.process_id = 2;
    a.process_epoch = 3;
    a.start_time_ns = 4;

    ProcessIdentity b = a;
    EXPECT_EQ(a, b);
    EXPECT_FALSE(a != b);

    b.process_epoch = 99;
    EXPECT_NE(a, b);
}

TEST(ProcessIdentityTest, IsZero) {
    ProcessIdentity zero;
    EXPECT_TRUE(zero.IsZero());
    zero.process_epoch = 1;
    EXPECT_FALSE(zero.IsZero());
}

TEST(ProcessIdentityTest, ToStringContainsFields) {
    ProcessIdentity id;
    id.node_id = 1;
    id.process_id = 2;
    id.process_epoch = 3;
    id.start_time_ns = 4;
    const std::string s = id.ToString();
    EXPECT_NE(s.find("ProcessIdentity"), std::string::npos);
}

TEST(ProcessIdentityTest, SerializeRoundTrip) {
    const ProcessIdentity& original = ProcessIdentity::Current();

    std::array<std::byte, ProcessIdentity::kSerializedSize> buf{};
    original.SerializeTo(buf);

    ProcessIdentity decoded = ProcessIdentity::DeserializeFrom(buf);
    EXPECT_EQ(decoded, original);
}

TEST(ProcessIdentityTest, SerializeIsDeterministicAndFixedSize) {
    static_assert(ProcessIdentity::kSerializedSize == 32);
    ProcessIdentity id;
    id.node_id = 0x1122334455667788ull;
    id.process_id = 0x99;
    id.process_epoch = 0xaabbccddeeff0011ull;
    id.start_time_ns = 0x1234;

    std::array<std::byte, ProcessIdentity::kSerializedSize> b1{};
    std::array<std::byte, ProcessIdentity::kSerializedSize> b2{};
    id.SerializeTo(b1);
    id.SerializeTo(b2);
    EXPECT_EQ(b1, b2);

    // Round trip.
    EXPECT_EQ(ProcessIdentity::DeserializeFrom(b1), id);
}

TEST(ProcessIdentityTest, LayoutIsFixed) {
    EXPECT_EQ(sizeof(ProcessIdentity), 32u);
    EXPECT_EQ(offsetof(ProcessIdentity, node_id), 0u);
    EXPECT_EQ(offsetof(ProcessIdentity, process_id), 8u);
    EXPECT_EQ(offsetof(ProcessIdentity, process_epoch), 16u);
    EXPECT_EQ(offsetof(ProcessIdentity, start_time_ns), 24u);
}

}  // namespace
}  // namespace mino
