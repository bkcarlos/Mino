// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef BENCHMARKS_PIPELINE_COMPARISON_SEMANTIC_PROTOBUF_CODEC_H_
#define BENCHMARKS_PIPELINE_COMPARISON_SEMANTIC_PROTOBUF_CODEC_H_

#include "benchmarks/pipeline_comparison/autonomy_pipeline.pb.h"
#include "benchmarks/pipeline_comparison/pipeline_common.h"

#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace mino::benchmarks::pipeline {

void SemanticToProtobuf(const SemanticFrame& source,
                        AutonomyPipelineFrame* destination);
bool ProtobufToSemantic(const AutonomyPipelineFrame& source,
                        SemanticFrame* destination, std::string* error);
std::string SerializeFrame(const SemanticFrame& frame);
bool ParseFrame(std::span<const uint8_t> bytes, SemanticFrame* frame,
                std::string* error);
bool ParseFrame(std::span<const std::byte> bytes, SemanticFrame* frame,
                std::string* error);
inline bool ParseFrame(const std::vector<uint8_t>& bytes, SemanticFrame* frame,
                       std::string* error) {
    return ParseFrame(std::span<const uint8_t>(bytes), frame, error);
}

}  // namespace mino::benchmarks::pipeline

#endif  // BENCHMARKS_PIPELINE_COMPARISON_SEMANTIC_PROTOBUF_CODEC_H_
