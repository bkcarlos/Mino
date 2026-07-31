// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/schema/fuzz/fuzz_harness.h"

#include <gtest/gtest.h>

#include <array>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "mino/common/status.h"
#include "mino/schema/canonical.h"
#include "mino/schema/compiler.h"
#include "mino/schema/descriptor.h"
#include "mino/schema/dynamic_value.h"
#include "mino/schema/fuzz/mutation.h"
#include "mino/schema/registry.h"
#include "mino/schema/wire.h"

namespace mino::schema::fuzz {
namespace {

static_assert(noexcept(FuzzIdl(std::span<const std::byte>{})));
static_assert(noexcept(FuzzDescriptor(std::span<const std::byte>{})));
static_assert(noexcept(FuzzCanonicalPayload(std::span<const std::byte>{})));
static_assert(noexcept(FuzzOneInput(std::span<const std::byte>{})));

constexpr size_t kCasesPerHarness = 1200;
constexpr std::array<uint64_t, 4> kMutationSeeds = {
    0x0000000000000001ull,
    0x243f6a8885a308d3ull,
    0x9e3779b97f4a7c15ull,
    0xffffffffffffffffull,
};

std::string ReadFile(std::string_view path) {
    std::ifstream input(std::string(path), std::ios::binary);
    if (!input) {
        ADD_FAILURE() << "cannot read testdata: " << path;
        return {};
    }
    return std::string(std::istreambuf_iterator<char>(input),
                       std::istreambuf_iterator<char>());
}

int HexNibble(char ch) {
    if (ch >= '0' && ch <= '9') return ch - '0';
    if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
    if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
    return -1;
}

std::vector<std::byte> ParseHex(std::string_view text) {
    std::string compact;
    for (unsigned char ch : text) {
        if (std::isspace(ch) == 0) compact.push_back(static_cast<char>(ch));
    }
    if ((compact.size() & 1u) != 0) {
        ADD_FAILURE() << "odd-length hex testdata";
        return {};
    }
    std::vector<std::byte> result;
    result.reserve(compact.size() / 2);
    for (size_t i = 0; i < compact.size(); i += 2) {
        const int high = HexNibble(compact[i]);
        const int low = HexNibble(compact[i + 1]);
        if (high < 0 || low < 0) {
            ADD_FAILURE() << "non-hex testdata at byte " << i / 2;
            return {};
        }
        result.push_back(static_cast<std::byte>((high << 4) | low));
    }
    return result;
}

std::vector<std::byte> Bytes(std::string_view text) {
    const char* data = text.empty() ? "" : text.data();
    const auto bytes = std::as_bytes(std::span(data, text.size()));
    return std::vector<std::byte>(bytes.begin(), bytes.end());
}

std::vector<std::byte> Bytes(std::initializer_list<uint8_t> values) {
    std::vector<std::byte> result;
    result.reserve(values.size());
    for (uint8_t value : values) {
        result.push_back(static_cast<std::byte>(value));
    }
    return result;
}

uint64_t Mix(uint64_t value) {
    value += 0x9e3779b97f4a7c15ull;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ull;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebull;
    return value ^ (value >> 31);
}

enum class HarnessKind { kIdl, kDescriptor, kCanonicalPayload };

bool IsAllowed(HarnessKind harness, StatusCode code) {
    if (code == StatusCode::kOk || code == StatusCode::kResourceExhausted) {
        return true;
    }
    if (harness == HarnessKind::kIdl || harness == HarnessKind::kDescriptor) {
        return code == StatusCode::kInvalidArgument;
    }
    return code == StatusCode::kCorruption ||
           code == StatusCode::kSchemaMismatch;
}

Status Run(HarnessKind harness, std::span<const std::byte> input) {
    if (harness == HarnessKind::kIdl) return FuzzIdl(input);
    if (harness == HarnessKind::kDescriptor) return FuzzDescriptor(input);
    return FuzzCanonicalPayload(input);
}

void ExpectAllowed(HarnessKind harness, std::span<const std::byte> input,
                   size_t case_index) {
    const Status status = Run(harness, input);
    EXPECT_TRUE(IsAllowed(harness, status.code()))
        << "case=" << case_index << " status=" << status.ToString();
}

std::shared_ptr<const SchemaDescriptor> CompileGoldenSchema(
    const std::string& idl) {
    auto compiled = SchemaCompiler::Compile(idl);
    EXPECT_TRUE(compiled.ok()) << compiled.status().ToString();
    if (!compiled.ok()) return nullptr;
    const SchemaDescriptor* descriptor = compiled->FindType("fuzz.Payload");
    EXPECT_NE(descriptor, nullptr);
    if (descriptor == nullptr) return nullptr;
    for (const auto& candidate : compiled->types()) {
        if (candidate.get() == descriptor) return candidate;
    }
    return nullptr;
}

std::vector<std::byte> EncodeGoldenPayload(const SchemaDescriptor& descriptor) {
    DynamicMessage message;
    EXPECT_TRUE(message.SetField(4, DynamicValue::Float32Bits(0x7fc01234u)).ok());
    EXPECT_TRUE(message.SetField(2, DynamicValue::Unsigned(300)).ok());
    EXPECT_TRUE(message.SetField(1, DynamicValue::Signed(-1)).ok());
    EXPECT_TRUE(message.SetField(3, DynamicValue::Unsigned(0x78563412u)).ok());
    auto label = DynamicValue::String("é");
    EXPECT_TRUE(label.ok());
    if (!label.ok()) return {};
    EXPECT_TRUE(message.SetField(5, std::move(*label)).ok());
    auto vector = std::make_shared<DynamicVector>();
    EXPECT_TRUE(vector->Add(DynamicValue::Unsigned(1)).ok());
    EXPECT_TRUE(vector->Add(DynamicValue::Unsigned(127)).ok());
    EXPECT_TRUE(vector->Add(DynamicValue::Unsigned(128)).ok());
    auto values = DynamicValue::Vector(vector);
    EXPECT_TRUE(values.ok());
    if (!values.ok()) return {};
    EXPECT_TRUE(message.SetField(6, std::move(*values)).ok());
    auto encoded = CanonicalWireCodec::Encode(descriptor, message);
    EXPECT_TRUE(encoded.ok()) << encoded.status().ToString();
    return encoded.ok() ? std::move(*encoded) : std::vector<std::byte>{};
}

TEST(GoldenVectorTest, CanonicalSchemaAndWireAreStable) {
    const std::string idl =
        ReadFile("mino/schema/fuzz/testdata/canonical_payload.mino");
    auto descriptor = CompileGoldenSchema(idl);
    ASSERT_NE(descriptor, nullptr);

    const auto canonical = ParseHex(ReadFile(
        "mino/schema/fuzz/testdata/canonical_payload.canonical.hex"));
    const std::string canonical_text(
        reinterpret_cast<const char*>(canonical.data()), canonical.size());
    EXPECT_EQ(descriptor->canonical_schema(), canonical_text);
    EXPECT_EQ(DigestHex(descriptor->identity().canonical_digest()),
              "ecfd2e19309c038bcb0914b4a6636340471b3cc794c0601e0afdddf7ff0e2088");
    EXPECT_EQ(descriptor->identity().short_id(), 10017021726596988396ull);

    const auto expected_wire = ParseHex(
        ReadFile("mino/schema/fuzz/testdata/canonical_payload.wire.hex"));
    const auto encoded = EncodeGoldenPayload(*descriptor);
    EXPECT_EQ(encoded, expected_wire);
    EXPECT_TRUE(FuzzIdl(Bytes(idl)).ok());
    const Status fuzzed = FuzzCanonicalPayload(expected_wire);
    EXPECT_TRUE(fuzzed.ok()) << fuzzed.ToString();
}

TEST(DescriptorHarnessTest,
     DocumentsRegistryByteCodecGapAndRejectsCppObjectBytes) {
    const auto artifact = Bytes(ReadFile(
        "mino/schema/fuzz/testdata/codegen_golden.descriptor"));
    EXPECT_TRUE(FuzzDescriptor(artifact).ok());

    SchemaRegistry registry;
    auto artifact_registration = registry.RegisterDescriptor(artifact);
    ASSERT_FALSE(artifact_registration.ok());
    EXPECT_EQ(artifact_registration.status().code(), StatusCode::kUnsupported);

    const std::string idl =
        ReadFile("mino/schema/fuzz/testdata/canonical_payload.mino");
    auto descriptor = CompileGoldenSchema(idl);
    ASSERT_NE(descriptor, nullptr);
    const auto abi_bytes = std::as_bytes(std::span(descriptor.get(), size_t{1}));
    EXPECT_FALSE(FuzzDescriptor(abi_bytes).ok());
    auto abi_registration = registry.RegisterDescriptor(abi_bytes);
    ASSERT_FALSE(abi_registration.ok());
    EXPECT_EQ(abi_registration.status().code(), StatusCode::kUnsupported);
}

TEST(FuzzHarnessTest, ExplicitLimitsRejectOversizedInputs) {
    std::vector<std::byte> oversized_idl(kMaxIdlInputBytes + 1);
    std::vector<std::byte> oversized_descriptor(kMaxDescriptorInputBytes + 1);
    std::vector<std::byte> oversized_payload(kMaxCanonicalPayloadBytes + 1);
    EXPECT_EQ(FuzzIdl(oversized_idl).code(), StatusCode::kResourceExhausted);
    EXPECT_EQ(FuzzDescriptor(oversized_descriptor).code(),
              StatusCode::kResourceExhausted);
    EXPECT_EQ(FuzzCanonicalPayload(oversized_payload).code(),
              StatusCode::kResourceExhausted);
}

TEST(FuzzCorpusTest, DeterministicMutationsOnlyReturnDefinedStatuses) {
    const std::string golden_idl =
        ReadFile("mino/schema/fuzz/testdata/canonical_payload.mino");
    const std::string golden_descriptor = ReadFile(
        "mino/schema/fuzz/testdata/codegen_golden.descriptor");
    const auto golden_wire = ParseHex(
        ReadFile("mino/schema/fuzz/testdata/canonical_payload.wire.hex"));

    const std::array<std::vector<std::byte>, 5> idl_seeds = {
        Bytes(golden_idl), Bytes(""), Bytes("message M {}"),
        Bytes("package p; message M { vector<vector<uint32>> v = 1; }"),
        Bytes({0x00, 0xff, 0x7f, 0x22, 0x5c}),
    };
    const std::array<std::vector<std::byte>, 4> descriptor_seeds = {
        Bytes(golden_descriptor), Bytes(""), Bytes("mino-descriptor-v1\n"),
        Bytes({0x00, 0xff, 0x0a, 0x39}),
    };
    const std::array<std::vector<std::byte>, 5> payload_seeds = {
        golden_wire, Bytes(""), Bytes({0x08, 0x81, 0x00}),
        Bytes({0x2a, 0xff, 0xff, 0xff, 0xff, 0x7f}),
        Bytes({0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02}),
    };

    const auto exercise = [&](HarnessKind harness, const auto& seeds,
                              size_t max_bytes) {
        std::array<size_t, kMutationKindCount> kinds{};
        for (size_t i = 0; i < kCasesPerHarness; ++i) {
            const auto kind = static_cast<MutationKind>(i % kMutationKindCount);
            ++kinds[i % kMutationKindCount];
            const uint64_t entropy =
                Mix(kMutationSeeds[i % kMutationSeeds.size()] + i);
            auto mutated = Mutate(seeds[i % seeds.size()], kind, entropy,
                                  max_bytes);
            ASSERT_TRUE(mutated.ok()) << mutated.status().ToString();
            ASSERT_LE(mutated->size(), max_bytes);
            ExpectAllowed(harness, *mutated, i);
        }
        for (size_t count : kinds) EXPECT_GT(count, 0u);
    };

    exercise(HarnessKind::kIdl, idl_seeds, kMaxIdlInputBytes);
    exercise(HarnessKind::kDescriptor, descriptor_seeds,
             kMaxDescriptorInputBytes);
    exercise(HarnessKind::kCanonicalPayload, payload_seeds,
             kMaxCanonicalPayloadBytes);
}

}  // namespace
}  // namespace mino::schema::fuzz
