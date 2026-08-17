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
 * @file AutonomyPipelineFrame.hpp
 * This header file contains the declaration of the described types in the IDL file.
 *
 * The Gradle distribution download timed out, so fastddsgen was not run for this
 * version. This generator-compatible checked-in support was mechanically produced
 * and reviewed against Fast DDS-Gen 4.2.0 templates and upstream Fast DDS 3.4.2
 * generated references, with output equivalent to -no-typeobjectsupport. Before a
 * formal release, regenerate with the pinned JDK 17 and diff the outputs.
 */

#ifndef FAST_DDS_GENERATED__AUTONOMYPIPELINEFRAME_HPP
#define FAST_DDS_GENERATED__AUTONOMYPIPELINEFRAME_HPP

#include <cstdint>
#include <utility>
#include <vector>

#if defined(_WIN32)
#if defined(EPROSIMA_USER_DLL_EXPORT)
#define eProsima_user_DllExport __declspec( dllexport )
#else
#define eProsima_user_DllExport
#endif  // EPROSIMA_USER_DLL_EXPORT
#else
#define eProsima_user_DllExport
#endif  // _WIN32

#if defined(_WIN32)
#if defined(EPROSIMA_USER_DLL_EXPORT)
#if defined(AUTONOMYPIPELINEFRAME_SOURCE)
#define AUTONOMYPIPELINEFRAME_DllAPI __declspec( dllexport )
#else
#define AUTONOMYPIPELINEFRAME_DllAPI __declspec( dllimport )
#endif  // AUTONOMYPIPELINEFRAME_SOURCE
#else
#define AUTONOMYPIPELINEFRAME_DllAPI
#endif  // EPROSIMA_USER_DLL_EXPORT
#else
#define AUTONOMYPIPELINEFRAME_DllAPI
#endif  // _WIN32

namespace mino {
namespace benchmarks {
namespace pipeline {

/*!
 * @brief This class represents the structure AutonomyPipelineFrame defined by the user in the IDL file.
 * @ingroup AutonomyPipelineFrame
 */
class AutonomyPipelineFrame
{
public:

    eProsima_user_DllExport AutonomyPipelineFrame() = default;

    eProsima_user_DllExport ~AutonomyPipelineFrame() = default;

    eProsima_user_DllExport AutonomyPipelineFrame(
            const AutonomyPipelineFrame& x) = default;

    eProsima_user_DllExport AutonomyPipelineFrame(
            AutonomyPipelineFrame&& x) noexcept = default;

    eProsima_user_DllExport AutonomyPipelineFrame& operator =(
            const AutonomyPipelineFrame& x) = default;

    eProsima_user_DllExport AutonomyPipelineFrame& operator =(
            AutonomyPipelineFrame&& x) noexcept = default;

    eProsima_user_DllExport bool operator ==(
            const AutonomyPipelineFrame& x) const
    {
        return m_sample_id == x.m_sample_id &&
               m_origin_timestamp_ns == x.m_origin_timestamp_ns &&
               m_perception_timestamp_ns == x.m_perception_timestamp_ns &&
               m_prediction_timestamp_ns == x.m_prediction_timestamp_ns &&
               m_planning_timestamp_ns == x.m_planning_timestamp_ns &&
               m_control_timestamp_ns == x.m_control_timestamp_ns &&
               m_guardian_timestamp_ns == x.m_guardian_timestamp_ns &&
               m_completed_stage_mask == x.m_completed_stage_mask &&
               m_profile == x.m_profile &&
               m_object_count == x.m_object_count &&
               m_trajectory_point_count == x.m_trajectory_point_count &&
               m_ego_speed_mps == x.m_ego_speed_mps &&
               m_steering_angle_rad == x.m_steering_angle_rad &&
               m_acceleration_mps2 == x.m_acceleration_mps2 &&
               m_brake_percentage == x.m_brake_percentage &&
               m_emergency_stop == x.m_emergency_stop &&
               m_payload_checksum == x.m_payload_checksum &&
               m_payload == x.m_payload;
    }

    eProsima_user_DllExport bool operator !=(
            const AutonomyPipelineFrame& x) const
    {
        return !(*this == x);
    }

    eProsima_user_DllExport void sample_id(
            uint64_t _sample_id)
    {
        m_sample_id = _sample_id;
    }

    eProsima_user_DllExport uint64_t sample_id() const
    {
        return m_sample_id;
    }

    eProsima_user_DllExport uint64_t& sample_id()
    {
        return m_sample_id;
    }

    eProsima_user_DllExport void origin_timestamp_ns(
            uint64_t _origin_timestamp_ns)
    {
        m_origin_timestamp_ns = _origin_timestamp_ns;
    }

    eProsima_user_DllExport uint64_t origin_timestamp_ns() const
    {
        return m_origin_timestamp_ns;
    }

    eProsima_user_DllExport uint64_t& origin_timestamp_ns()
    {
        return m_origin_timestamp_ns;
    }

    eProsima_user_DllExport void perception_timestamp_ns(
            uint64_t _perception_timestamp_ns)
    {
        m_perception_timestamp_ns = _perception_timestamp_ns;
    }

    eProsima_user_DllExport uint64_t perception_timestamp_ns() const
    {
        return m_perception_timestamp_ns;
    }

    eProsima_user_DllExport uint64_t& perception_timestamp_ns()
    {
        return m_perception_timestamp_ns;
    }

    eProsima_user_DllExport void prediction_timestamp_ns(
            uint64_t _prediction_timestamp_ns)
    {
        m_prediction_timestamp_ns = _prediction_timestamp_ns;
    }

    eProsima_user_DllExport uint64_t prediction_timestamp_ns() const
    {
        return m_prediction_timestamp_ns;
    }

    eProsima_user_DllExport uint64_t& prediction_timestamp_ns()
    {
        return m_prediction_timestamp_ns;
    }

    eProsima_user_DllExport void planning_timestamp_ns(
            uint64_t _planning_timestamp_ns)
    {
        m_planning_timestamp_ns = _planning_timestamp_ns;
    }

    eProsima_user_DllExport uint64_t planning_timestamp_ns() const
    {
        return m_planning_timestamp_ns;
    }

    eProsima_user_DllExport uint64_t& planning_timestamp_ns()
    {
        return m_planning_timestamp_ns;
    }

    eProsima_user_DllExport void control_timestamp_ns(
            uint64_t _control_timestamp_ns)
    {
        m_control_timestamp_ns = _control_timestamp_ns;
    }

    eProsima_user_DllExport uint64_t control_timestamp_ns() const
    {
        return m_control_timestamp_ns;
    }

    eProsima_user_DllExport uint64_t& control_timestamp_ns()
    {
        return m_control_timestamp_ns;
    }

    eProsima_user_DllExport void guardian_timestamp_ns(
            uint64_t _guardian_timestamp_ns)
    {
        m_guardian_timestamp_ns = _guardian_timestamp_ns;
    }

    eProsima_user_DllExport uint64_t guardian_timestamp_ns() const
    {
        return m_guardian_timestamp_ns;
    }

    eProsima_user_DllExport uint64_t& guardian_timestamp_ns()
    {
        return m_guardian_timestamp_ns;
    }

    eProsima_user_DllExport void completed_stage_mask(
            uint32_t _completed_stage_mask)
    {
        m_completed_stage_mask = _completed_stage_mask;
    }

    eProsima_user_DllExport uint32_t completed_stage_mask() const
    {
        return m_completed_stage_mask;
    }

    eProsima_user_DllExport uint32_t& completed_stage_mask()
    {
        return m_completed_stage_mask;
    }

    eProsima_user_DllExport void profile(
            uint32_t _profile)
    {
        m_profile = _profile;
    }

    eProsima_user_DllExport uint32_t profile() const
    {
        return m_profile;
    }

    eProsima_user_DllExport uint32_t& profile()
    {
        return m_profile;
    }

    eProsima_user_DllExport void object_count(
            uint32_t _object_count)
    {
        m_object_count = _object_count;
    }

    eProsima_user_DllExport uint32_t object_count() const
    {
        return m_object_count;
    }

    eProsima_user_DllExport uint32_t& object_count()
    {
        return m_object_count;
    }

    eProsima_user_DllExport void trajectory_point_count(
            uint32_t _trajectory_point_count)
    {
        m_trajectory_point_count = _trajectory_point_count;
    }

    eProsima_user_DllExport uint32_t trajectory_point_count() const
    {
        return m_trajectory_point_count;
    }

    eProsima_user_DllExport uint32_t& trajectory_point_count()
    {
        return m_trajectory_point_count;
    }

    eProsima_user_DllExport void ego_speed_mps(
            double _ego_speed_mps)
    {
        m_ego_speed_mps = _ego_speed_mps;
    }

    eProsima_user_DllExport double ego_speed_mps() const
    {
        return m_ego_speed_mps;
    }

    eProsima_user_DllExport double& ego_speed_mps()
    {
        return m_ego_speed_mps;
    }

    eProsima_user_DllExport void steering_angle_rad(
            double _steering_angle_rad)
    {
        m_steering_angle_rad = _steering_angle_rad;
    }

    eProsima_user_DllExport double steering_angle_rad() const
    {
        return m_steering_angle_rad;
    }

    eProsima_user_DllExport double& steering_angle_rad()
    {
        return m_steering_angle_rad;
    }

    eProsima_user_DllExport void acceleration_mps2(
            double _acceleration_mps2)
    {
        m_acceleration_mps2 = _acceleration_mps2;
    }

    eProsima_user_DllExport double acceleration_mps2() const
    {
        return m_acceleration_mps2;
    }

    eProsima_user_DllExport double& acceleration_mps2()
    {
        return m_acceleration_mps2;
    }

    eProsima_user_DllExport void brake_percentage(
            double _brake_percentage)
    {
        m_brake_percentage = _brake_percentage;
    }

    eProsima_user_DllExport double brake_percentage() const
    {
        return m_brake_percentage;
    }

    eProsima_user_DllExport double& brake_percentage()
    {
        return m_brake_percentage;
    }

    eProsima_user_DllExport void emergency_stop(
            bool _emergency_stop)
    {
        m_emergency_stop = _emergency_stop;
    }

    eProsima_user_DllExport bool emergency_stop() const
    {
        return m_emergency_stop;
    }

    eProsima_user_DllExport bool& emergency_stop()
    {
        return m_emergency_stop;
    }

    eProsima_user_DllExport void payload_checksum(
            uint64_t _payload_checksum)
    {
        m_payload_checksum = _payload_checksum;
    }

    eProsima_user_DllExport uint64_t payload_checksum() const
    {
        return m_payload_checksum;
    }

    eProsima_user_DllExport uint64_t& payload_checksum()
    {
        return m_payload_checksum;
    }

    eProsima_user_DllExport void payload(
            const std::vector<uint8_t>& _payload)
    {
        m_payload = _payload;
    }

    eProsima_user_DllExport void payload(
            std::vector<uint8_t>&& _payload)
    {
        m_payload = std::move(_payload);
    }

    eProsima_user_DllExport const std::vector<uint8_t>& payload() const
    {
        return m_payload;
    }

    eProsima_user_DllExport std::vector<uint8_t>& payload()
    {
        return m_payload;
    }

private:

    uint64_t m_sample_id {0};
    uint64_t m_origin_timestamp_ns {0};
    uint64_t m_perception_timestamp_ns {0};
    uint64_t m_prediction_timestamp_ns {0};
    uint64_t m_planning_timestamp_ns {0};
    uint64_t m_control_timestamp_ns {0};
    uint64_t m_guardian_timestamp_ns {0};
    uint32_t m_completed_stage_mask {0};
    uint32_t m_profile {0};
    uint32_t m_object_count {0};
    uint32_t m_trajectory_point_count {0};
    double m_ego_speed_mps {0.0};
    double m_steering_angle_rad {0.0};
    double m_acceleration_mps2 {0.0};
    double m_brake_percentage {0.0};
    bool m_emergency_stop {false};
    uint64_t m_payload_checksum {0};
    std::vector<uint8_t> m_payload;
};

} // namespace pipeline
} // namespace benchmarks
} // namespace mino

#endif // FAST_DDS_GENERATED__AUTONOMYPIPELINEFRAME_HPP
