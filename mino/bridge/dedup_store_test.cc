// Copyright 2026 The Mino Authors

#include "mino/bridge/dedup_store.h"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

#include <gtest/gtest.h>
#include <unistd.h>

namespace mino::bridge {
namespace {

constexpr SourceIdentity kSource{1, 2, 3};
constexpr SourceIdentity kOther{4, 5, 6};

class DedupStoreTest : public ::testing::Test {
protected:
    void SetUp() override {
        const char* tmp = std::getenv("TEST_TMPDIR");
        ASSERT_NE(tmp, nullptr);
        dir_ = std::filesystem::path(tmp) /
               ("dedup_store_" + std::to_string(::getpid()) + "_" +
                std::to_string(++sequence_));
        std::error_code error;
        std::filesystem::create_directories(dir_, error);
        ASSERT_FALSE(error) << error.message();
        path_ = (dir_ / "dedup.snap").string();
    }

    void TearDown() override {
        std::error_code error;
        std::filesystem::remove_all(dir_, error);
    }

    DedupStoreOptions Options(size_t max_sources = 8) const {
        return DedupStoreOptions{.path = path_, .max_sources = max_sources};
    }

    static uint32_t sequence_;
    std::filesystem::path dir_;
    std::string path_;
};

uint32_t DedupStoreTest::sequence_ = 0;

TEST_F(DedupStoreTest, OpenMissingIsEmptyAndPersistsAcrossReopen) {
    {
        auto opened = DedupStore::Open(Options());
        ASSERT_TRUE(opened.ok()) << opened.status().ToString();
        EXPECT_EQ((*opened)->size(), 0u);
        ASSERT_TRUE((*opened)->RecordAccepted(kSource, 7).ok());
        ASSERT_TRUE((*opened)->RecordAccepted(kOther, 3).ok());
        ASSERT_TRUE((*opened)->RecordAccepted(kSource, 5).ok());  // no-op
        EXPECT_EQ((*opened)->size(), 2u);
    }

    auto reopened = DedupStore::Open(Options());
    ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
    auto snapshot = (*reopened)->Load();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    ASSERT_EQ(snapshot->size(), 2u);
    EXPECT_EQ((*snapshot)[0].source, kSource);
    EXPECT_EQ((*snapshot)[0].highest_contiguous_sequence, 7u);
    EXPECT_EQ((*snapshot)[1].source, kOther);
    EXPECT_EQ((*snapshot)[1].highest_contiguous_sequence, 3u);
}

TEST_F(DedupStoreTest, RestartedWindowRejectsPreviouslySeenSequences) {
    {
        auto store = DedupStore::Open(Options());
        ASSERT_TRUE(store.ok()) << store.status().ToString();
        ASSERT_TRUE((*store)->RecordAccepted(kSource, 11).ok());
    }

    auto store = DedupStore::Open(Options());
    ASSERT_TRUE(store.ok()) << store.status().ToString();
    auto window = DedupWindow::Create(DedupWindowOptions{
        .max_sources = 4,
        .max_bytes = 4096,
        .max_sequence_distance = 64,
        .max_source_age_ns = 1'000'000'000ull,
    });
    ASSERT_TRUE(window.ok()) << window.status().ToString();
    (*window)->BeginSession(/*peer_session_epoch=*/42, /*now_ns=*/0);
    ASSERT_TRUE(
        SeedDedupWindowFromStore(window->get(), store->get(), 42, 0).ok());

    auto duplicate = (*window)->Check(42, kSource, 11, 1);
    ASSERT_TRUE(duplicate.ok()) << duplicate.status().ToString();
    EXPECT_EQ(duplicate->decision, DedupDecision::kDuplicateAccepted);
    EXPECT_EQ(duplicate->highest_contiguous_sequence, 11u);

    auto next = (*window)->Check(42, kSource, 12, 2);
    ASSERT_TRUE(next.ok()) << next.status().ToString();
    EXPECT_EQ(next->decision, DedupDecision::kAccept);
}

TEST_F(DedupStoreTest, CorruptSnapshotFailsClosed) {
    {
        auto store = DedupStore::Open(Options());
        ASSERT_TRUE(store.ok()) << store.status().ToString();
        ASSERT_TRUE((*store)->RecordAccepted(kSource, 9).ok());
    }

    {
        std::fstream file(path_, std::ios::in | std::ios::out | std::ios::binary);
        ASSERT_TRUE(file.good());
        file.seekp(0);
        const char bogus[] = {'X', 'X', 'X', 'X', 'X', 'X', 'X', 'X'};
        file.write(bogus, sizeof(bogus));
        ASSERT_TRUE(file.good());
    }

    auto opened = DedupStore::Open(Options());
    ASSERT_FALSE(opened.ok());
    EXPECT_EQ(opened.status().code(), StatusCode::kCorruption);

    // Flip the trailing CRC byte instead of the magic.
    {
        const std::string other = (dir_ / "other.snap").string();
        auto store = DedupStore::Open(DedupStoreOptions{
            .path = other,
            .max_sources = 8,
        });
        ASSERT_TRUE(store.ok()) << store.status().ToString();
        ASSERT_TRUE((*store)->RecordAccepted(kSource, 2).ok());
        store->reset();

        std::fstream file(other, std::ios::in | std::ios::out | std::ios::binary);
        ASSERT_TRUE(file.good());
        file.seekg(0, std::ios::end);
        const auto size = file.tellg();
        ASSERT_GT(size, 4);
        file.seekg(static_cast<std::streamoff>(size) - 4);
        char crc[4] = {};
        file.read(crc, 4);
        ASSERT_TRUE(file.good());
        crc[0] = static_cast<char>(static_cast<unsigned char>(crc[0]) ^ 0xffu);
        file.clear();
        file.seekp(static_cast<std::streamoff>(size) - 4);
        file.write(crc, 4);
        file.flush();
        ASSERT_TRUE(file.good());
        file.close();

        auto corrupt = DedupStore::Open(DedupStoreOptions{
            .path = other,
            .max_sources = 8,
        });
        ASSERT_FALSE(corrupt.ok());
        EXPECT_EQ(corrupt.status().code(), StatusCode::kCorruption);
    }
}

TEST_F(DedupStoreTest, EmptyFileAndTruncationFailClosed) {
    {
        std::ofstream empty(path_, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(empty.good());
    }
    auto empty_open = DedupStore::Open(Options());
    ASSERT_FALSE(empty_open.ok());
    EXPECT_EQ(empty_open.status().code(), StatusCode::kCorruption);

    {
        auto store = DedupStore::Open(DedupStoreOptions{
            .path = (dir_ / "trunc.snap").string(),
            .max_sources = 8,
        });
        ASSERT_TRUE(store.ok()) << store.status().ToString();
        ASSERT_TRUE((*store)->RecordAccepted(kSource, 4).ok());
        const std::string trunc_path = (*store)->path();
        store->reset();
        std::filesystem::resize_file(trunc_path, 8);
    }
    auto trunc_open = DedupStore::Open(DedupStoreOptions{
        .path = (dir_ / "trunc.snap").string(),
        .max_sources = 8,
    });
    ASSERT_FALSE(trunc_open.ok());
    EXPECT_EQ(trunc_open.status().code(), StatusCode::kCorruption);
}

TEST_F(DedupStoreTest, RejectsInvalidInputsAndEnforcesMaxSources) {
    auto store = DedupStore::Open(Options(/*max_sources=*/1));
    ASSERT_TRUE(store.ok()) << store.status().ToString();
    EXPECT_EQ((*store)->RecordAccepted(SourceIdentity{}, 1).code(),
              StatusCode::kInvalidArgument);
    EXPECT_EQ((*store)->RecordAccepted(kSource, 0).code(),
              StatusCode::kInvalidArgument);
    ASSERT_TRUE((*store)->RecordAccepted(kSource, 1).ok());
    EXPECT_EQ((*store)->RecordAccepted(kOther, 1).code(),
              StatusCode::kResourceExhausted);
}

}  // namespace
}  // namespace mino::bridge
