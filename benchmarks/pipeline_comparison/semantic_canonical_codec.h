// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef BENCHMARKS_PIPELINE_COMPARISON_SEMANTIC_CANONICAL_CODEC_H_
#define BENCHMARKS_PIPELINE_COMPARISON_SEMANTIC_CANONICAL_CODEC_H_

#include "benchmarks/pipeline_comparison/pipeline_common.h"

#include <cstddef>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "mino/schema/descriptor.h"
#include "mino/schema/dynamic_value.h"
#include "mino/schema/wire.h"

namespace mino::benchmarks::pipeline {

// CanonicalWireCodec EncodeInto/DecodeInto of SemanticFrame. Field mapping
// matches CanonicalSchema in mino_tcp_pipeline.cc.
class SemanticCanonicalCodec {
public:
    static SemanticCanonicalCodec FromDescriptorFile(
        const std::filesystem::path& artifact_path);

    void Encode(const SemanticFrame& frame, std::vector<std::byte>* output);
    void Decode(std::span<const std::byte> bytes, SemanticFrame* frame);

private:
    SemanticCanonicalCodec() = default;

    std::shared_ptr<const schema::SchemaDescriptor> descriptor_;
    std::optional<schema::PreparedCanonicalWireCodec> prepared_codec_;
    schema::DynamicMessage encode_message_;
    schema::DynamicMessage decode_message_;
    schema::CanonicalWireScratch encode_scratch_;
    schema::CanonicalWireScratch decode_scratch_;
};

}  // namespace mino::benchmarks::pipeline

#endif  // BENCHMARKS_PIPELINE_COMPARISON_SEMANTIC_CANONICAL_CODEC_H_
