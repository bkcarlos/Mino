// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/validation/report/report.h"

#include <iomanip>
#include <sstream>
#include <string>
#include <string_view>

#include "benchmarks/validation/common/config.h"
#include "benchmarks/validation/common/json.h"
#include "benchmarks/validation/common/provenance.h"
#include "benchmarks/validation/common/runtime.h"

namespace mino::benchmarks::validation {

std::string BuildFailureJson(int argc, char** argv, std::string_view reason) {
    std::ostringstream command;
    for (int index = 0; index < argc; ++index) {
        if (index != 0) command << ' ';
        command << ShellQuote(argv[index]);
    }
    const std::string pending =
        PendingResult("benchmark aborted: " + std::string(reason));
    std::ostringstream output;
    output << "{\n  \"schema\": \"mino.validation_benchmark.v1\",\n"
           << "  \"artifact_status\": \"FAILED\",\n"
           << "  \"clock\": \"std::chrono::steady_clock\",\n"
           << "  \"provenance\": {\n"
           << "    \"run_timestamp_utc\": \""
           << JsonEscape(RunTimestampUtc()) << "\",\n"
           << "    \"commit\": \"PENDING\",\n"
           << "    \"command\": \"" << JsonEscape(command.str()) << "\",\n"
           << "    \"build_config\": \"PENDING\",\n"
           << "    \"compiler\": \"" << JsonEscape(__VERSION__) << "\",\n"
           << "    \"cplusplus\": " << __cplusplus << ",\n"
           << "    \"os\": \"" << JsonEscape(OperatingSystem()) << "\",\n"
           << "    \"hardware\": {\"logical_cpu_count\":"
           << LogicalCpuCount()
           << ",\"physical_memory_bytes_detected\":"
           << PhysicalMemoryBytes()
           << ",\"cpu_model\":\"PENDING\",\"memory\":\"PENDING\","
              "\"storage_device\":\"PENDING\",\"filesystem\":\"PENDING\"}\n"
           << "  },\n"
           << "  \"configuration\": {\"suite\":\"PENDING\","
              "\"iterations\":null,\"storage_records\":null,"
              "\"records_per_writer\":null,\"payload_bytes\":null,"
              "\"pin_count\":null,\"temporary_directory_base\":null},\n"
           << "  \"methodology\": {\"percentile_method\":\"nearest-rank\","
              "\"latency_unit\":\"ns\"},\n"
           << "  \"results\": {\"V-14\":" << pending
           << ",\"V-15\":" << pending << ",\"V-16\":" << pending
           << ",\"V-17\":" << pending << ",\"V-18\":" << pending
           << ",\"V-27\":" << pending << "},\n"
           << "  \"sink\": " << SinkValue() << "\n}\n";
    return output.str();
}

std::string BuildJson(
    const Config& config, const ValidationResults& results,
    const std::optional<OwnedTemporaryDirectory>& temporary) {
    std::ostringstream output;
    output << std::setprecision(17)
           << "{\n  \"schema\": \"mino.validation_benchmark.v1\",\n"
           << "  \"artifact_status\": \""
           << (HasFailed() ? "FAILED" : "MEASURED") << "\",\n"
           << "  \"clock\": \"std::chrono::steady_clock\",\n"
           << "  \"provenance\": {\n"
           << "    \"run_timestamp_utc\": \""
           << JsonEscape(RunTimestampUtc()) << "\",\n"
           << "    \"commit\": \"" << JsonEscape(config.commit) << "\",\n"
           << "    \"command\": \"" << JsonEscape(config.command) << "\",\n"
           << "    \"build_config\": \"" << JsonEscape(config.build_config)
           << "\",\n"
           << "    \"compiler\": \"" << JsonEscape(__VERSION__) << "\",\n"
           << "    \"cplusplus\": " << __cplusplus << ",\n"
           << "    \"os\": \"" << JsonEscape(OperatingSystem()) << "\",\n"
           << "    \"hardware\": {\n"
           << "      \"logical_cpu_count\": " << LogicalCpuCount() << ",\n"
           << "      \"physical_memory_bytes_detected\": "
           << PhysicalMemoryBytes() << ",\n"
           << "      \"cpu_model\": \"" << JsonEscape(config.cpu_model)
           << "\",\n"
           << "      \"memory\": \""
           << JsonEscape(config.memory_description) << "\",\n"
           << "      \"storage_device\": \""
           << JsonEscape(config.storage_device) << "\",\n"
           << "      \"filesystem\": \"" << JsonEscape(config.filesystem)
           << "\"\n    }\n  },\n"
           << "  \"configuration\": {\n"
           << "    \"suite\": \"" << config.suite << "\",\n"
           << "    \"iterations\": " << config.iterations << ",\n"
           << "    \"storage_records\": " << config.storage_records << ",\n"
           << "    \"records_per_writer\": " << config.records_per_writer
           << ",\n"
           << "    \"payload_bytes\": " << config.payload_bytes << ",\n"
           << "    \"pin_count\": " << config.pin_count << ",\n"
           << "    \"temporary_directory_base\": \""
           << JsonEscape(temporary.has_value() ? temporary->base().string()
                                               : "not-used")
           << "\"\n  },\n"
           << "  \"methodology\": {\n"
           << "    \"percentile_method\": \"nearest-rank\",\n"
           << "    \"latency_unit\": \"ns\",\n"
           << "    \"measured_results_are_generated_only_by_execution\": true,\n"
           << "    \"capacity_model_is_labeled_separately\": true\n  },\n"
           << "  \"results\": {\n"
           << "    \"V-14\": " << results.broadcast_ack << ",\n"
           << "    \"V-15\": " << results.dynamic_view << ",\n"
           << "    \"V-16\": " << results.topic_writer_scaling << ",\n"
           << "    \"V-17\": " << results.sync_policy << ",\n"
           << "    \"V-18\": " << results.buffer_capacity << ",\n"
           << "    \"V-27\": " << results.pin_lease << "\n  },\n"
           << "  \"sink\": " << SinkValue() << "\n}\n";
    return output.str();
}

}  // namespace mino::benchmarks::validation
