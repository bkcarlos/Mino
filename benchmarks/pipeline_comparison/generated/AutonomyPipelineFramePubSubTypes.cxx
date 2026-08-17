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
 * @file AutonomyPipelineFramePubSubTypes.cxx
 * This source file contains the implementation of the serialization functions.
 *
 * The Gradle distribution download timed out, so fastddsgen was not run for this
 * version. This generator-compatible checked-in support was mechanically produced
 * and reviewed against Fast DDS-Gen 4.2.0 templates and upstream Fast DDS 3.4.2
 * generated references, with output equivalent to -no-typeobjectsupport. Before a
 * formal release, regenerate with the pinned JDK 17 and diff the outputs.
 */

#include "AutonomyPipelineFramePubSubTypes.hpp"

#include <limits>

#include <fastdds/dds/log/Log.hpp>
#include <fastdds/rtps/common/CdrSerialization.hpp>

#include "AutonomyPipelineFrameCdrAux.hpp"

using SerializedPayload_t = eprosima::fastdds::rtps::SerializedPayload_t;
using InstanceHandle_t = eprosima::fastdds::rtps::InstanceHandle_t;
using DataRepresentationId_t = eprosima::fastdds::dds::DataRepresentationId_t;

namespace mino {
namespace benchmarks {
namespace pipeline {

namespace {

constexpr size_t kAutonomyPipelineFramePayloadBound = 1048576UL;

} // namespace

AutonomyPipelineFramePubSubType::AutonomyPipelineFramePubSubType()
{
    set_name("mino::benchmarks::pipeline::AutonomyPipelineFrame");
    uint32_t type_size = mino_benchmarks_pipeline_AutonomyPipelineFrame_max_cdr_typesize;
    type_size += static_cast<uint32_t>(eprosima::fastcdr::Cdr::alignment(type_size, 4)); /* possible submessage alignment */
    max_serialized_type_size = type_size + 4; /* encapsulation */
    is_compute_key_provided = false;
}

AutonomyPipelineFramePubSubType::~AutonomyPipelineFramePubSubType()
{
}

bool AutonomyPipelineFramePubSubType::serialize(
        const void* const data,
        SerializedPayload_t& payload,
        DataRepresentationId_t data_representation)
{
    const ::mino::benchmarks::pipeline::AutonomyPipelineFrame* p_type =
            static_cast<const ::mino::benchmarks::pipeline::AutonomyPipelineFrame*>(data);

    if (nullptr == p_type || p_type->payload().size() > kAutonomyPipelineFramePayloadBound)
    {
        return false;
    }

    // Object that manages the raw buffer.
    eprosima::fastcdr::FastBuffer fastbuffer(reinterpret_cast<char*>(payload.data), payload.max_size);
    // Object that serializes the data.
    eprosima::fastcdr::Cdr ser(fastbuffer, eprosima::fastcdr::Cdr::DEFAULT_ENDIAN,
            data_representation == DataRepresentationId_t::XCDR_DATA_REPRESENTATION ?
            eprosima::fastcdr::CdrVersion::XCDRv1 : eprosima::fastcdr::CdrVersion::XCDRv2);
    payload.encapsulation = ser.endianness() == eprosima::fastcdr::Cdr::BIG_ENDIANNESS ? CDR_BE : CDR_LE;
    ser.set_encoding_flag(
        data_representation == DataRepresentationId_t::XCDR_DATA_REPRESENTATION ?
        eprosima::fastcdr::EncodingAlgorithmFlag::PLAIN_CDR :
        eprosima::fastcdr::EncodingAlgorithmFlag::PLAIN_CDR2);

    try
    {
        // Serialize encapsulation.
        ser.serialize_encapsulation();
        // Serialize the object.
        ser << *p_type;
        ser.set_dds_cdr_options({0, 0});
    }
    catch (eprosima::fastcdr::exception::Exception& /* exception */)
    {
        return false;
    }

    const size_t serialized_length = ser.get_serialized_data_length();
    if (serialized_length > max_serialized_type_size ||
            serialized_length > static_cast<size_t>((std::numeric_limits<uint32_t>::max)()))
    {
        return false;
    }

    payload.length = static_cast<uint32_t>(serialized_length);
    return true;
}

bool AutonomyPipelineFramePubSubType::deserialize(
        SerializedPayload_t& payload,
        void* data)
{
    if (nullptr == data || payload.length > max_serialized_type_size)
    {
        return false;
    }

    try
    {
        // Convert DATA to pointer of your type.
        ::mino::benchmarks::pipeline::AutonomyPipelineFrame* p_type =
                static_cast<::mino::benchmarks::pipeline::AutonomyPipelineFrame*>(data);

        // Object that manages the raw buffer.
        eprosima::fastcdr::FastBuffer fastbuffer(reinterpret_cast<char*>(payload.data), payload.length);

        // Object that deserializes the data.
        eprosima::fastcdr::Cdr deser(fastbuffer, eprosima::fastcdr::Cdr::DEFAULT_ENDIAN);

        // Deserialize encapsulation.
        deser.read_encapsulation();
        payload.encapsulation = deser.endianness() == eprosima::fastcdr::Cdr::BIG_ENDIANNESS ? CDR_BE : CDR_LE;

        // Deserialize the object.
        deser >> *p_type;
        if (p_type->payload().size() > kAutonomyPipelineFramePayloadBound)
        {
            return false;
        }
    }
    catch (eprosima::fastcdr::exception::Exception& /* exception */)
    {
        return false;
    }

    return true;
}

uint32_t AutonomyPipelineFramePubSubType::calculate_serialized_size(
        const void* const data,
        DataRepresentationId_t data_representation)
{
    const ::mino::benchmarks::pipeline::AutonomyPipelineFrame* p_type =
            static_cast<const ::mino::benchmarks::pipeline::AutonomyPipelineFrame*>(data);
    if (nullptr == p_type || p_type->payload().size() > kAutonomyPipelineFramePayloadBound)
    {
        return 0;
    }

    try
    {
        eprosima::fastcdr::CdrSizeCalculator calculator(
            data_representation == DataRepresentationId_t::XCDR_DATA_REPRESENTATION ?
            eprosima::fastcdr::CdrVersion::XCDRv1 : eprosima::fastcdr::CdrVersion::XCDRv2);
        size_t current_alignment {0};
        const size_t calculated_size = calculator.calculate_serialized_size(*p_type, current_alignment);
        if (calculated_size > mino_benchmarks_pipeline_AutonomyPipelineFrame_max_cdr_typesize ||
                calculated_size > static_cast<size_t>((std::numeric_limits<uint32_t>::max)()) - 4U)
        {
            return 0;
        }
        return static_cast<uint32_t>(calculated_size) + 4U /* encapsulation */;
    }
    catch (eprosima::fastcdr::exception::Exception& /* exception */)
    {
        return 0;
    }
}

void* AutonomyPipelineFramePubSubType::create_data()
{
    return reinterpret_cast<void*>(new ::mino::benchmarks::pipeline::AutonomyPipelineFrame());
}

void AutonomyPipelineFramePubSubType::delete_data(
        void* data)
{
    delete reinterpret_cast<::mino::benchmarks::pipeline::AutonomyPipelineFrame*>(data);
}

bool AutonomyPipelineFramePubSubType::compute_key(
        SerializedPayload_t& payload,
        InstanceHandle_t& handle,
        bool force_md5)
{
    static_cast<void>(payload);
    static_cast<void>(handle);
    static_cast<void>(force_md5);

    return false;
}

bool AutonomyPipelineFramePubSubType::compute_key(
        const void* const data,
        InstanceHandle_t& handle,
        bool force_md5)
{
    static_cast<void>(data);
    static_cast<void>(handle);
    static_cast<void>(force_md5);

    return false;
}

void AutonomyPipelineFramePubSubType::register_type_object_representation()
{
    EPROSIMA_LOG_WARNING(XTYPES_TYPE_REPRESENTATION,
            "TypeObject type representation support disabled in generated code");
}

} // namespace pipeline
} // namespace benchmarks
} // namespace mino

// Include auxiliary functions like for serializing/deserializing.
#include "AutonomyPipelineFrameCdrAux.ipp"
