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
 * @file AutonomyPipelineFrameCdrAux.ipp
 * This source file contains some declarations of CDR related functions.
 *
 * The Gradle distribution download timed out, so fastddsgen was not run for this
 * version. This generator-compatible checked-in support was mechanically produced
 * and reviewed against Fast DDS-Gen 4.2.0 templates and upstream Fast DDS 3.4.2
 * generated references, with output equivalent to -no-typeobjectsupport. Before a
 * formal release, regenerate with the pinned JDK 17 and diff the outputs.
 */

#ifndef FAST_DDS_GENERATED__AUTONOMYPIPELINEFRAMECDRAUX_IPP
#define FAST_DDS_GENERATED__AUTONOMYPIPELINEFRAMECDRAUX_IPP

#include "AutonomyPipelineFrameCdrAux.hpp"

#include <fastcdr/Cdr.h>
#include <fastcdr/CdrSizeCalculator.hpp>
#include <fastcdr/exceptions/BadParamException.h>

using namespace eprosima::fastcdr::exception;

namespace eprosima {
namespace fastcdr {

template<>
eProsima_user_DllExport size_t calculate_serialized_size(
        eprosima::fastcdr::CdrSizeCalculator& calculator,
        const mino::benchmarks::pipeline::AutonomyPipelineFrame& data,
        size_t& current_alignment)
{
    using namespace mino::benchmarks::pipeline;

    static_cast<void>(data);

    eprosima::fastcdr::EncodingAlgorithmFlag previous_encoding = calculator.get_encoding();
    size_t calculated_size {calculator.begin_calculate_type_serialized_size(
                                eprosima::fastcdr::CdrVersion::XCDRv2 == calculator.get_cdr_version() ?
                                eprosima::fastcdr::EncodingAlgorithmFlag::PLAIN_CDR2 :
                                eprosima::fastcdr::EncodingAlgorithmFlag::PLAIN_CDR,
                                current_alignment)};

    calculated_size += calculator.calculate_member_serialized_size(eprosima::fastcdr::MemberId(0),
            data.sample_id(), current_alignment);
    calculated_size += calculator.calculate_member_serialized_size(eprosima::fastcdr::MemberId(1),
            data.origin_timestamp_ns(), current_alignment);
    calculated_size += calculator.calculate_member_serialized_size(eprosima::fastcdr::MemberId(2),
            data.perception_timestamp_ns(), current_alignment);
    calculated_size += calculator.calculate_member_serialized_size(eprosima::fastcdr::MemberId(3),
            data.prediction_timestamp_ns(), current_alignment);
    calculated_size += calculator.calculate_member_serialized_size(eprosima::fastcdr::MemberId(4),
            data.planning_timestamp_ns(), current_alignment);
    calculated_size += calculator.calculate_member_serialized_size(eprosima::fastcdr::MemberId(5),
            data.control_timestamp_ns(), current_alignment);
    calculated_size += calculator.calculate_member_serialized_size(eprosima::fastcdr::MemberId(6),
            data.guardian_timestamp_ns(), current_alignment);
    calculated_size += calculator.calculate_member_serialized_size(eprosima::fastcdr::MemberId(7),
            data.completed_stage_mask(), current_alignment);
    calculated_size += calculator.calculate_member_serialized_size(eprosima::fastcdr::MemberId(8),
            data.profile(), current_alignment);
    calculated_size += calculator.calculate_member_serialized_size(eprosima::fastcdr::MemberId(9),
            data.object_count(), current_alignment);
    calculated_size += calculator.calculate_member_serialized_size(eprosima::fastcdr::MemberId(10),
            data.trajectory_point_count(), current_alignment);
    calculated_size += calculator.calculate_member_serialized_size(eprosima::fastcdr::MemberId(11),
            data.ego_speed_mps(), current_alignment);
    calculated_size += calculator.calculate_member_serialized_size(eprosima::fastcdr::MemberId(12),
            data.steering_angle_rad(), current_alignment);
    calculated_size += calculator.calculate_member_serialized_size(eprosima::fastcdr::MemberId(13),
            data.acceleration_mps2(), current_alignment);
    calculated_size += calculator.calculate_member_serialized_size(eprosima::fastcdr::MemberId(14),
            data.brake_percentage(), current_alignment);
    calculated_size += calculator.calculate_member_serialized_size(eprosima::fastcdr::MemberId(15),
            data.emergency_stop(), current_alignment);
    calculated_size += calculator.calculate_member_serialized_size(eprosima::fastcdr::MemberId(16),
            data.payload_checksum(), current_alignment);
    calculated_size += calculator.calculate_member_serialized_size(eprosima::fastcdr::MemberId(17),
            data.payload(), current_alignment);

    calculated_size += calculator.end_calculate_type_serialized_size(previous_encoding, current_alignment);

    return calculated_size;
}

template<>
eProsima_user_DllExport void serialize(
        eprosima::fastcdr::Cdr& scdr,
        const mino::benchmarks::pipeline::AutonomyPipelineFrame& data)
{
    using namespace mino::benchmarks::pipeline;

    eprosima::fastcdr::Cdr::state current_state(scdr);
    scdr.begin_serialize_type(current_state,
            eprosima::fastcdr::CdrVersion::XCDRv2 == scdr.get_cdr_version() ?
            eprosima::fastcdr::EncodingAlgorithmFlag::PLAIN_CDR2 :
            eprosima::fastcdr::EncodingAlgorithmFlag::PLAIN_CDR);

    scdr
        << eprosima::fastcdr::MemberId(0) << data.sample_id()
        << eprosima::fastcdr::MemberId(1) << data.origin_timestamp_ns()
        << eprosima::fastcdr::MemberId(2) << data.perception_timestamp_ns()
        << eprosima::fastcdr::MemberId(3) << data.prediction_timestamp_ns()
        << eprosima::fastcdr::MemberId(4) << data.planning_timestamp_ns()
        << eprosima::fastcdr::MemberId(5) << data.control_timestamp_ns()
        << eprosima::fastcdr::MemberId(6) << data.guardian_timestamp_ns()
        << eprosima::fastcdr::MemberId(7) << data.completed_stage_mask()
        << eprosima::fastcdr::MemberId(8) << data.profile()
        << eprosima::fastcdr::MemberId(9) << data.object_count()
        << eprosima::fastcdr::MemberId(10) << data.trajectory_point_count()
        << eprosima::fastcdr::MemberId(11) << data.ego_speed_mps()
        << eprosima::fastcdr::MemberId(12) << data.steering_angle_rad()
        << eprosima::fastcdr::MemberId(13) << data.acceleration_mps2()
        << eprosima::fastcdr::MemberId(14) << data.brake_percentage()
        << eprosima::fastcdr::MemberId(15) << data.emergency_stop()
        << eprosima::fastcdr::MemberId(16) << data.payload_checksum()
        << eprosima::fastcdr::MemberId(17) << data.payload();
    scdr.end_serialize_type(current_state);
}

template<>
eProsima_user_DllExport void deserialize(
        eprosima::fastcdr::Cdr& cdr,
        mino::benchmarks::pipeline::AutonomyPipelineFrame& data)
{
    using namespace mino::benchmarks::pipeline;

    cdr.deserialize_type(eprosima::fastcdr::CdrVersion::XCDRv2 == cdr.get_cdr_version() ?
            eprosima::fastcdr::EncodingAlgorithmFlag::PLAIN_CDR2 :
            eprosima::fastcdr::EncodingAlgorithmFlag::PLAIN_CDR,
            [&data](eprosima::fastcdr::Cdr& dcdr, const eprosima::fastcdr::MemberId& mid) -> bool
            {
                bool ret_value = true;
                switch (mid.id)
                {
                    case 0:
                        dcdr >> data.sample_id();
                        break;
                    case 1:
                        dcdr >> data.origin_timestamp_ns();
                        break;
                    case 2:
                        dcdr >> data.perception_timestamp_ns();
                        break;
                    case 3:
                        dcdr >> data.prediction_timestamp_ns();
                        break;
                    case 4:
                        dcdr >> data.planning_timestamp_ns();
                        break;
                    case 5:
                        dcdr >> data.control_timestamp_ns();
                        break;
                    case 6:
                        dcdr >> data.guardian_timestamp_ns();
                        break;
                    case 7:
                        dcdr >> data.completed_stage_mask();
                        break;
                    case 8:
                        dcdr >> data.profile();
                        break;
                    case 9:
                        dcdr >> data.object_count();
                        break;
                    case 10:
                        dcdr >> data.trajectory_point_count();
                        break;
                    case 11:
                        dcdr >> data.ego_speed_mps();
                        break;
                    case 12:
                        dcdr >> data.steering_angle_rad();
                        break;
                    case 13:
                        dcdr >> data.acceleration_mps2();
                        break;
                    case 14:
                        dcdr >> data.brake_percentage();
                        break;
                    case 15:
                        dcdr >> data.emergency_stop();
                        break;
                    case 16:
                        dcdr >> data.payload_checksum();
                        break;
                    case 17:
                        dcdr >> data.payload();
                        break;
                    default:
                        ret_value = false;
                        break;
                }
                return ret_value;
            });
}

void serialize_key(
        eprosima::fastcdr::Cdr& scdr,
        const mino::benchmarks::pipeline::AutonomyPipelineFrame& data)
{
    using namespace mino::benchmarks::pipeline;

    static_cast<void>(scdr);
    static_cast<void>(data);
    scdr << data.sample_id();
    scdr << data.origin_timestamp_ns();
    scdr << data.perception_timestamp_ns();
    scdr << data.prediction_timestamp_ns();
    scdr << data.planning_timestamp_ns();
    scdr << data.control_timestamp_ns();
    scdr << data.guardian_timestamp_ns();
    scdr << data.completed_stage_mask();
    scdr << data.profile();
    scdr << data.object_count();
    scdr << data.trajectory_point_count();
    scdr << data.ego_speed_mps();
    scdr << data.steering_angle_rad();
    scdr << data.acceleration_mps2();
    scdr << data.brake_percentage();
    scdr << data.emergency_stop();
    scdr << data.payload_checksum();
    scdr << data.payload();
}

} // namespace fastcdr
} // namespace eprosima

#endif // FAST_DDS_GENERATED__AUTONOMYPIPELINEFRAMECDRAUX_IPP
