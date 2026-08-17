// Copyright 2016 Proyectos y Sistemas de Mantenimiento SL (eProsima).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*!
 * @file AutonomyPipelineFrameCdrAux.hpp
 * This source file contains some definitions of CDR related functions.
 *
 * The Gradle distribution download timed out, so fastddsgen was not run for this
 * version. This generator-compatible checked-in support was mechanically produced
 * and reviewed against Fast DDS-Gen 4.2.0 templates and upstream Fast DDS 3.4.2
 * generated references, with output equivalent to -no-typeobjectsupport. Before a
 * formal release, regenerate with the pinned JDK 17 and diff the outputs.
 */

#ifndef FAST_DDS_GENERATED__AUTONOMYPIPELINEFRAMECDRAUX_HPP
#define FAST_DDS_GENERATED__AUTONOMYPIPELINEFRAMECDRAUX_HPP

#include "AutonomyPipelineFrame.hpp"

#include <limits>

constexpr uint32_t mino_benchmarks_pipeline_AutonomyPipelineFrame_max_cdr_typesize {1048700UL};
constexpr uint32_t mino_benchmarks_pipeline_AutonomyPipelineFrame_max_key_cdr_typesize {0UL};

static_assert(
    static_cast<uint64_t>(mino_benchmarks_pipeline_AutonomyPipelineFrame_max_cdr_typesize) + 3UL + 4UL <=
    static_cast<uint64_t>((std::numeric_limits<uint32_t>::max)()),
    "AutonomyPipelineFrame maximum serialized size must fit Fast DDS uint32_t size fields");

namespace eprosima {
namespace fastcdr {

class Cdr;
class CdrSizeCalculator;

eProsima_user_DllExport void serialize_key(
        eprosima::fastcdr::Cdr& scdr,
        const mino::benchmarks::pipeline::AutonomyPipelineFrame& data);

} // namespace fastcdr
} // namespace eprosima

#endif // FAST_DDS_GENERATED__AUTONOMYPIPELINEFRAMECDRAUX_HPP
