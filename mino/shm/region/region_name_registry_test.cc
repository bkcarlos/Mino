// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include "mino/shm/region/region_name_registry.h"

#include <cstdlib>
#include <filesystem>
#include <string>

#include <gtest/gtest.h>
#include <unistd.h>

namespace mino::region_internal {
namespace {

class RegionNameRegistryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    const char* tmp = std::getenv("TEST_TMPDIR");
    ASSERT_NE(tmp, nullptr);
    dir_ = std::filesystem::path(tmp) / ("region_names_" +
                                         std::to_string(::getpid()) + "_" +
                                         std::to_string(++sequence_));
    std::error_code error;
    std::filesystem::create_directories(dir_, error);
    ASSERT_FALSE(error) << error.message();
    options_.registry_dir = dir_.string();
  }

  void TearDown() override {
    std::error_code error;
    std::filesystem::remove_all(dir_, error);
  }

  static uint32_t sequence_;
  std::filesystem::path dir_;
  RegionNameRegistryOptions options_;
};

uint32_t RegionNameRegistryTest::sequence_ = 0;

TEST_F(RegionNameRegistryTest, RegisterLookupAndRejectDuplicate) {
  ASSERT_TRUE(RegisterRegionName(42, "/mino_reg_a", options_).ok());
  auto found = LookupRegionName(42, options_);
  ASSERT_TRUE(found.ok()) << found.status().ToString();
  EXPECT_EQ(*found, "/mino_reg_a");

  auto dup = RegisterRegionName(42, "/mino_reg_b", options_);
  ASSERT_FALSE(dup.ok());
  EXPECT_EQ(dup.code(), StatusCode::kAlreadyExists);

  // Original mapping remains authoritative.
  found = LookupRegionName(42, options_);
  ASSERT_TRUE(found.ok()) << found.status().ToString();
  EXPECT_EQ(*found, "/mino_reg_a");
}

TEST_F(RegionNameRegistryTest, MissingIdIsNotFound) {
  auto missing = LookupRegionName(7, options_);
  ASSERT_FALSE(missing.ok());
  EXPECT_EQ(missing.status().code(), StatusCode::kNotFound);
}

TEST_F(RegionNameRegistryTest, UnregisterIsIdempotent) {
  ASSERT_TRUE(RegisterRegionName(9, "/mino_reg_u", options_).ok());
  ASSERT_TRUE(UnregisterRegionName(9, options_).ok());
  ASSERT_TRUE(UnregisterRegionName(9, options_).ok());
  auto missing = LookupRegionName(9, options_);
  ASSERT_FALSE(missing.ok());
  EXPECT_EQ(missing.status().code(), StatusCode::kNotFound);
}

TEST_F(RegionNameRegistryTest, RejectsInvalidNamesAndZeroId) {
  EXPECT_EQ(RegisterRegionName(0, "/ok", options_).code(),
            StatusCode::kInvalidArgument);
  EXPECT_EQ(RegisterRegionName(1, "no_slash", options_).code(),
            StatusCode::kInvalidArgument);
  EXPECT_EQ(RegisterRegionName(1, "/a/b", options_).code(),
            StatusCode::kInvalidArgument);
  EXPECT_EQ(LookupRegionName(0, options_).status().code(),
            StatusCode::kInvalidArgument);
}

}  // namespace
}  // namespace mino::region_internal
