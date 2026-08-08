// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/validation/validations/dynamic_view.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>

#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "benchmarks/validation/common/aligned_memory.h"
#include "benchmarks/validation/common/runtime.h"
#include "benchmarks/validation/common/stats.h"
#include "mino/runtime/allocation_journal.h"
#include "mino/schema/codegen/testdata/golden.generated.h"
#include "mino/schema/compiler.h"
#include "mino/schema/dynamic_object.h"
#include "mino/schema/layout.h"
#include "mino/shm/allocator/central_slab.h"

namespace mino::benchmarks::validation {
namespace {



ClassTableConfig DynamicAllocatorConfig() {
    ClassTableConfig config;
    config.classes = {
        {.slot_size = 64, .slot_count = 32},
        {.slot_size = 128, .slot_count = 32},
        {.slot_size = 256, .slot_count = 32},
        {.slot_size = 512, .slot_count = 32},
        {.slot_size = 1024, .slot_count = 16},
        {.slot_size = 2048, .slot_count = 16},
    };
    return config;
}

}  // namespace

std::string RunDynamicView(uint64_t iterations) {
    auto compiled = Take(schema::SchemaCompiler::Compile(R"idl(
syntax = "v1";
package golden;
option schema_version = "2.1";
message Telemetry {
  required uint32 sequence = 1;
  optional string label = 2
      [max_bytes = 16, snapshot_key, default = "re\x00ady"];
  bytes payload = 3 [max_bytes = 32, default = "\xff\x00"];
  vector<uint64> samples = 4 [max_capacity = 8];
  optional bool active = 5;
  reserved 6 to 7;
}
)idl"), "SchemaCompiler::Compile");
    schema::DynamicSchemaHandle descriptor;
    for (const auto& candidate : compiled.types()) {
        if (candidate->aggregate().full_name() == "golden.Telemetry") {
            descriptor = candidate;
        }
    }
    if (descriptor == nullptr) {
        throw std::runtime_error("Telemetry descriptor absent");
    }
    auto layout = Take(schema::LayoutPlanner::Plan(*descriptor, compiled.types()),
                       "LayoutPlanner::Plan");
    auto sequence_field = Take(
        schema::FieldHandle::ById(*descriptor, layout, 1),
        "FieldHandle::ById(sequence)");
    auto payload_field = Take(
        schema::FieldHandle::ById(*descriptor, layout, 3),
        "FieldHandle::ById(payload)");
    auto samples_field = Take(
        schema::FieldHandle::ById(*descriptor, layout, 4),
        "FieldHandle::ById(samples)");

    constexpr size_t kAllocatorBytes = 4u << 20;
    auto allocator_memory = AllocateAligned(kAllocatorBytes);
    auto allocator = Take(CentralSlabAllocator::Create(
                              allocator_memory.get(), kAllocatorBytes,
                              DynamicAllocatorConfig()),
                          "CentralSlabAllocator::Create");
    const size_t journal_bytes = AllocationJournal::RequiredSize(8, 128);
    auto journal_memory = AllocateAligned(journal_bytes);
    auto journal = Take(AllocationJournal::Init(
                            journal_memory.get(), journal_bytes, 8, 128, allocator),
                        "AllocationJournal::Init");
    auto pin_memory = AllocateAligned(ShmPinTable::RequiredSize());
    auto pins = Take(ShmPinTable::Init(pin_memory.get(),
                                      ShmPinTable::RequiredSize(), allocator),
                     "ShmPinTable::Init");

    auto builder = Take(schema::DynamicBuilder::Create(
                            descriptor, layout, allocator, journal, TypeId{42},
                            compiled.types()),
                        "DynamicBuilder::Create");
    Require(builder.SetUnsigned(sequence_field, 0x12345678u),
            "DynamicBuilder::SetUnsigned");
    const std::array<std::byte, 2> payload = {std::byte{0xff}, std::byte{0}};
    Require(builder.SetBytes(payload_field, payload),
            "DynamicBuilder::SetBytes");
    const schema::DynamicVector samples;
    Require(builder.SetVector(samples_field, samples),
            "DynamicBuilder::SetVector");
    auto object = Take(builder.Commit(pins), "DynamicBuilder::Commit");

    golden::Telemetry static_object;
    golden::TelemetryBuilder static_builder(static_object);
    static_builder.set_sequence(0x12345678u);
    if (!static_builder.set_payload({.element_size = 1u}) ||
        !static_builder.set_samples({.element_size = 8u})) {
        throw std::runtime_error("generated static builder initialization failed");
    }
    const golden::TelemetryAccessor static_view(static_object);

    std::vector<uint64_t> static_samples;
    std::vector<uint64_t> dynamic_samples;
    static_samples.reserve(iterations);
    dynamic_samples.reserve(iterations);
    uint64_t dynamic_errors = 0;
    std::string json;
    {
        auto pin = Take(object.Pin(), "DynamicObject::Pin");
        auto dynamic_view = Take(schema::DynamicView::Create(
                                     descriptor, layout, object.root_handle(),
                                     allocator, std::move(pin), compiled.types()),
                                 "DynamicView::Create");
        const uint64_t warmup = std::max<uint64_t>(1, iterations / 10);
        for (uint64_t index = 0; index < warmup; ++index) {
            SinkXor(static_view.sequence());
            auto value = dynamic_view.GetUnsigned(sequence_field);
            if (!value.ok()) throw std::runtime_error(value.status().ToString());
            SinkXor(*value);
        }
        for (uint64_t index = 0; index < iterations; ++index) {
            auto begin = Clock::now();
            const uint32_t static_value = static_view.sequence();
            std::atomic_signal_fence(std::memory_order_seq_cst);
            static_samples.push_back(DurationNs(begin, Clock::now()));
            SinkXor(static_value);

            begin = Clock::now();
            auto dynamic_value = dynamic_view.GetUnsigned(sequence_field);
            std::atomic_signal_fence(std::memory_order_seq_cst);
            dynamic_samples.push_back(DurationNs(begin, Clock::now()));
            if (!dynamic_value.ok()) {
                ++dynamic_errors;
            } else {
                SinkXor(*dynamic_value);
            }
        }
        if (dynamic_errors != 0) MarkFailed();
        std::ostringstream output;
        output << "{\"status\":\""
               << (dynamic_errors == 0 ? "MEASURED" : "FAILED")
               << "\",\"iterations\":" << iterations
               << ",\"warmup_iterations\":" << warmup
               << ",\"logical_field\":\"golden.Telemetry.sequence(uint32)\""
               << ",\"static_view\":{\"implementation\":\"generated TelemetryAccessor\",\"errors\":0,\"latency_ns\":";
        WriteDistribution(output, Summarize(std::move(static_samples)));
        output << "},\"dynamic_view\":{\"implementation\":\"schema::DynamicView::GetUnsigned(FieldHandle)\",\"errors\":"
               << dynamic_errors << ",\"latency_ns\":";
        WriteDistribution(output, Summarize(std::move(dynamic_samples)));
        output << "}}";
        json = output.str();
    }
    Require(object.Reclaim(), "DynamicObject::Reclaim");
    return json;
}

}  // namespace mino::benchmarks::validation
