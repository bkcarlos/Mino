// Copyright 2026 The Mino Authors

#include "mino/bridge/retransmit_window.h"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "mino/common/status.h"

namespace mino::bridge {
namespace {

constexpr SourceIdentity kSource{1, 2, 3};
constexpr SourceIdentity kOtherSource{4, 5, 6};

std::unique_ptr<RetransmitWindow> MakeWindow(
    RetransmitWindowOptions options = {}) {
    auto result = RetransmitWindow::Create(options);
    EXPECT_TRUE(result.ok()) << result.status().ToString();
    return result.ok() ? std::move(*result) : nullptr;
}

TEST(RetransmitWindowTest, OwnsFrameAndEnforcesEntryAndByteLimits) {
    auto window = MakeWindow(RetransmitWindowOptions{
        .max_entries = 2,
        .max_bytes = 5,
        .max_age_ns = 100,
    });
    ASSERT_NE(window, nullptr);
    window->BeginSession(10, 20, 0);
    std::array<std::byte, 3> first{std::byte{1}, std::byte{2}, std::byte{3}};
    ASSERT_TRUE(window->Add(kSource, 1, first, 0).ok());
    first[0] = std::byte{9};
    ASSERT_NE(window->Find(kSource, 1), nullptr);
    EXPECT_EQ(window->Find(kSource, 1)->frame[0], std::byte{1});

    const std::array<std::byte, 2> second{std::byte{4}, std::byte{5}};
    ASSERT_TRUE(window->Add(kSource, 2, second, 1).ok());
    const std::array<std::byte, 1> third{std::byte{6}};
    Status full = window->Add(kSource, 3, third, 2);
    EXPECT_EQ(full.code(), StatusCode::kResourceExhausted);
    EXPECT_EQ(window->size(), 2u);
    EXPECT_EQ(window->bytes(), 5u);
}

TEST(RetransmitWindowTest, AcceptedAckRemovesObservedAndCumulativeEntries) {
    auto window = MakeWindow();
    ASSERT_NE(window, nullptr);
    window->BeginSession(10, 20, 0);
    const std::array<std::byte, 1> frame{std::byte{1}};
    for (uint64_t sequence = 1; sequence <= 4; ++sequence) {
        ASSERT_TRUE(window->Add(kSource, sequence, frame, sequence).ok());
    }
    ASSERT_TRUE(window->Add(kOtherSource, 1, frame, 1).ok());

    AckPayload ack{
        .sender_session_epoch = 20,
        .receiver_session_epoch = 10,
        .source = kSource,
        .observed_sequence = 4,
        .highest_contiguous_sequence = 2,
        .disposition = AckDisposition::kAccepted,
    };
    auto applied = window->ApplyAck(ack);
    ASSERT_TRUE(applied.ok()) << applied.status().ToString();
    EXPECT_EQ(applied->removed_entries, 3u);
    EXPECT_EQ(window->Find(kSource, 1), nullptr);
    EXPECT_EQ(window->Find(kSource, 2), nullptr);
    ASSERT_NE(window->Find(kSource, 3), nullptr);
    EXPECT_EQ(window->Find(kSource, 4), nullptr);
    ASSERT_NE(window->Find(kOtherSource, 1), nullptr);
}

TEST(RetransmitWindowTest, NackRemovesCumulativePrefixAndRetainsRejectedFrame) {
    auto window = MakeWindow();
    ASSERT_NE(window, nullptr);
    window->BeginSession(10, 20, 0);
    const std::array<std::byte, 1> frame{std::byte{1}};
    for (uint64_t sequence = 5; sequence <= 8; ++sequence) {
        ASSERT_TRUE(window->Add(kSource, sequence, frame, sequence).ok());
    }
    AckPayload nack{
        .sender_session_epoch = 20,
        .receiver_session_epoch = 10,
        .source = kSource,
        .observed_sequence = 8,
        .highest_contiguous_sequence = 6,
        .disposition = AckDisposition::kNackWithHighest,
    };
    auto applied = window->ApplyAck(nack);
    ASSERT_TRUE(applied.ok());
    EXPECT_EQ(applied->removed_entries, 2u);
    EXPECT_EQ(window->Find(kSource, 5), nullptr);
    EXPECT_EQ(window->Find(kSource, 6), nullptr);
    ASSERT_NE(window->Find(kSource, 7), nullptr);
    ASSERT_NE(window->Find(kSource, 8), nullptr);
    EXPECT_EQ(window->stats().nack_acks, 1u);
}

TEST(RetransmitWindowTest, SessionFencingAndAgeAreBounded) {
    auto window = MakeWindow(RetransmitWindowOptions{
        .max_entries = 4,
        .max_bytes = 16,
        .max_age_ns = 10,
    });
    ASSERT_NE(window, nullptr);
    window->BeginSession(1, 2, 0);
    const std::array<std::byte, 1> frame{std::byte{1}};
    ASSERT_TRUE(window->Add(kSource, 1, frame, 0).ok());

    AckPayload stale{
        .sender_session_epoch = 2,
        .receiver_session_epoch = 9,
        .source = kSource,
        .observed_sequence = 1,
        .highest_contiguous_sequence = 1,
        .disposition = AckDisposition::kAccepted,
    };
    auto rejected = window->ApplyAck(stale);
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), StatusCode::kUnavailable);
    ASSERT_NE(window->Find(kSource, 1), nullptr);

    EXPECT_EQ(window->PurgeExpired(11), 1u);
    ASSERT_TRUE(window->Add(kSource, 2, frame, 12).ok());
    EXPECT_EQ(window->BeginSession(3, 4, 13), 0u);
    EXPECT_EQ(window->size(), 1u);
    EXPECT_EQ(window->stats().session_switches, 1u);
}

}  // namespace
}  // namespace mino::bridge
