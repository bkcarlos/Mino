// Copyright 2026 The Mino Authors

#include "tools/minoc/output_commit.h"

#include <array>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace mino::tools::minoc {
namespace {

std::filesystem::path TestDirectory(std::string_view name) {
    const char* temporary = std::getenv("TEST_TMPDIR");
    std::filesystem::path path =
        std::filesystem::path(temporary == nullptr ? "." : temporary) / name;
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
    std::filesystem::create_directories(path);
    return path;
}

void Write(const std::filesystem::path& path, std::string_view value) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(output);
    output << value;
    ASSERT_TRUE(output);
}

std::string Read(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    EXPECT_TRUE(input);
    return std::string(std::istreambuf_iterator<char>(input),
                       std::istreambuf_iterator<char>());
}

void ExpectNoTransactionFiles(const std::filesystem::path& directory) {
    for (const auto& entry : std::filesystem::directory_iterator(directory)) {
        EXPECT_EQ(entry.path().filename().string().find(".minoc."),
                  std::string::npos)
            << entry.path();
    }
}

TEST(OutputCommitTest, SecondAndThirdRenameFailuresRestoreAllOldFiles) {
    const auto directory = TestDirectory("rollback");
    const std::array paths = {directory / "a", directory / "b", directory / "c"};
    for (size_t failure : {2u, 3u}) {
        Write(paths[0], "old-a");
        Write(paths[1], "old-b");
        Write(paths[2], "old-c");
        const std::array<OutputFile, 3> files = {{{paths[0], "new-a"},
                                                  {paths[1], "new-b"},
                                                  {paths[2], "new-c"}}};
        CommitOptions options;
        options.fail_before_rename = failure;
        const Status status = CommitOutputFiles(files, options);
        EXPECT_FALSE(status.ok());
        EXPECT_EQ(Read(paths[0]), "old-a");
        EXPECT_EQ(Read(paths[1]), "old-b");
        EXPECT_EQ(Read(paths[2]), "old-c");
        ExpectNoTransactionFiles(directory);
    }
}

TEST(OutputCommitTest, ConcurrentWritersNeverPublishMixedArtifactSets) {
    const auto directory = TestDirectory("concurrent");
    const std::array paths = {directory / "a", directory / "b", directory / "c"};
    std::vector<std::thread> threads;
    std::vector<Status> statuses(8);
    for (size_t i = 0; i < statuses.size(); ++i) {
        threads.emplace_back([&, i] {
            const std::string prefix = "writer-" + std::to_string(i);
            const std::array<std::string, 3> contents = {
                prefix + ":a", prefix + ":b", prefix + ":c"};
            const std::array<OutputFile, 3> files = {{{paths[0], contents[0]},
                                                      {paths[1], contents[1]},
                                                      {paths[2], contents[2]}}};
            statuses[i] = CommitOutputFiles(files);
        });
    }
    for (auto& thread : threads) thread.join();
    for (const Status& status : statuses) EXPECT_TRUE(status.ok()) << status.ToString();
    const std::string a = Read(paths[0]);
    const std::string b = Read(paths[1]);
    const std::string c = Read(paths[2]);
    ASSERT_GE(a.size(), 2u);
    const std::string prefix = a.substr(0, a.size() - 2);
    EXPECT_EQ(b, prefix + ":b");
    EXPECT_EQ(c, prefix + ":c");
    ExpectNoTransactionFiles(directory);
}

TEST(OutputCommitTest, RejectsPhysicalAliasesAndSymlinkOutputLeaf) {
    const auto directory = TestDirectory("aliases");
    const auto input = directory / "input.mino";
    const auto alias = directory / "alias.mino";
    const auto output = directory / "output.h";
    Write(input, "schema");
    std::filesystem::create_symlink(input, alias);
    const std::array outputs = {output};
    const std::array protected_paths = {input, alias};
    EXPECT_FALSE(ValidateOutputPaths(outputs, protected_paths).ok());
    std::filesystem::remove(alias);
    std::filesystem::create_hard_link(input, alias);
    EXPECT_FALSE(ValidateOutputPaths(outputs, protected_paths).ok());

    std::filesystem::create_symlink(input, output);
    EXPECT_FALSE(ValidateOutputPaths(outputs).ok());
}

}  // namespace
}  // namespace mino::tools::minoc
