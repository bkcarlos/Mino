// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>

#include "benchmarks/validation/common/config.h"
#include "benchmarks/validation/common/json.h"
#include "benchmarks/validation/common/runtime.h"
#include "benchmarks/validation/common/temporary_directory.h"
#include "benchmarks/validation/report/report.h"
#include "benchmarks/validation/validations/broadcast_ack.h"
#include "benchmarks/validation/validations/buffer_capacity.h"
#include "benchmarks/validation/validations/dynamic_view.h"
#include "benchmarks/validation/validations/pin_lease.h"
#include "benchmarks/validation/validations/sync_policy.h"
#include "benchmarks/validation/validations/topic_writer_scaling.h"

namespace benchmark = mino::benchmarks::validation;

int main(int argc, char** argv) {
    try {
        const benchmark::Config config = benchmark::ParseArguments(argc, argv);
        const bool run_memory =
            config.suite == "all" || config.suite == "memory";
        const bool run_storage =
            config.suite == "all" || config.suite == "storage";
        std::optional<benchmark::OwnedTemporaryDirectory> temporary;
        if (run_storage) temporary.emplace(config.directory);

        benchmark::ValidationResults results;
        results.broadcast_ack = run_memory
            ? benchmark::RunBroadcastAck(config.iterations)
            : benchmark::PendingResult("suite excludes memory benchmarks");
        results.dynamic_view = run_memory
            ? benchmark::RunDynamicView(config.iterations)
            : benchmark::PendingResult("suite excludes memory benchmarks");
        results.buffer_capacity = run_memory
            ? benchmark::RunBufferCapacity()
            : benchmark::PendingResult("suite excludes memory benchmarks");
        results.pin_lease = run_memory
            ? benchmark::RunPinLease(config.pin_count)
            : benchmark::PendingResult("suite excludes memory benchmarks");
        results.topic_writer_scaling =
            run_storage
                ? benchmark::RunTopicWriterScaling(
                      temporary->path(), config.records_per_writer,
                      config.payload_bytes)
                : benchmark::PendingResult(
                      "suite excludes storage benchmarks");
        results.sync_policy =
            run_storage
                ? benchmark::RunSyncPolicy(temporary->path(),
                                           config.storage_records,
                                           config.payload_bytes)
                : benchmark::PendingResult(
                      "suite excludes storage benchmarks");

        const std::string json =
            benchmark::BuildJson(config, results, temporary);
        std::cout << json;
        if (config.output_json.has_value()) {
            const std::filesystem::path parent =
                config.output_json->parent_path();
            if (!parent.empty()) {
                std::error_code error;
                std::filesystem::create_directories(parent, error);
                if (error) {
                    throw std::runtime_error(
                        "cannot create --output-json directory: " +
                        error.message());
                }
            }
            std::ofstream output(*config.output_json,
                                 std::ios::binary | std::ios::trunc);
            output << json;
            output.close();
            if (!output) {
                throw std::runtime_error("cannot write --output-json");
            }
        }
        return benchmark::HasFailed() ? 1 : 0;
    } catch (const std::exception& error) {
        benchmark::MarkFailed();
        std::cout << benchmark::BuildFailureJson(argc, argv, error.what());
        std::cerr << "validation_benchmark: " << error.what() << '\n';
        return 1;
    }
}
