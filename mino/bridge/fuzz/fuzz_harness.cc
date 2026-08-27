// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include "mino/bridge/fuzz/fuzz_harness.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <new>
#include <span>
#include <string_view>

#include "mino/bridge/control_payload.h"
#include "mino/bridge/wire_frame.h"

namespace mino::bridge::fuzz {
namespace {

WireFrameLimits BoundedFrameLimits() {
    WireFrameLimits limits;
    limits.max_payload_length = kMaxFrameFuzzInputBytes;
    limits.max_buffered_bytes = kLengthPrefixSize + kWireMaximumHeaderLength +
                                kMaxFrameFuzzInputBytes;
    limits.max_frames_per_push = 64;
    limits.max_decoded_payload_bytes_per_push = kMaxFrameFuzzInputBytes;
    limits.max_work_bytes_per_push = 4u * kMaxFrameFuzzInputBytes;
    return limits;
}

Status Internal(std::string_view message) {
    return Status::Error(StatusCode::kInternal, message);
}

Status CheckFrameRoundTrip(const WireFrame& frame,
                           const WireFrameLimits& limits) {
    auto encoded = WireFrameCodec::Encode(frame, limits);
    if (!encoded.ok()) return Internal("decoded frame could not be re-encoded");
    auto decoded = WireFrameCodec::Decode(*encoded, limits);
    if (!decoded.ok() || *decoded != frame) {
        return Internal("frame decode/encode round trip is unstable");
    }
    return Status::Ok();
}

template <typename T, typename Encode, typename Decode>
Status CheckControlRoundTrip(const T& value, Encode encode, Decode decode) {
    auto encoded = encode(value);
    if (!encoded.ok()) return Internal("decoded control payload did not encode");
    auto decoded = decode(*encoded);
    if (!decoded.ok() || *decoded != value) {
        return Internal("control payload round trip is unstable");
    }
    return Status::Ok();
}

}  // namespace

FrameFuzzSelector SelectFrameHarness(
    std::span<const std::byte> input) noexcept {
    if (input.empty()) return FrameFuzzSelector::kFrameBody;
    return static_cast<FrameFuzzSelector>(
        static_cast<uint8_t>(input.front()) % 3u);
}

Status FuzzFrameBody(std::span<const std::byte> input) noexcept {
    try {
        if (input.size() > kMaxFrameFuzzInputBytes) {
            return Status::Error(StatusCode::kResourceExhausted);
        }
        const WireFrameLimits limits = BoundedFrameLimits();
        auto decoded = WireFrameCodec::Decode(input, limits);
        if (!decoded.ok()) return decoded.status();
        return CheckFrameRoundTrip(*decoded, limits);
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Internal("frame decoder threw an exception");
    }
}

Status FuzzFrameStream(std::span<const std::byte> input) noexcept {
    try {
        if (input.size() > kMaxFrameFuzzInputBytes) {
            return Status::Error(StatusCode::kResourceExhausted);
        }
        const WireFrameLimits limits = BoundedFrameLimits();
        LengthPrefixedFrameDecoder decoder(limits);
        size_t offset = 0;
        while (offset < input.size()) {
            const size_t remaining = input.size() - offset;
            const size_t requested =
                1u + (static_cast<uint8_t>(input[offset]) % 31u);
            const size_t chunk_size = std::min(remaining, requested);
            auto frames = decoder.Push(input.subspan(offset, chunk_size));
            offset += chunk_size;
            if (!frames.ok()) {
                if (!decoder.failed()) {
                    return Internal("stream decoder error did not latch failure");
                }
                decoder.Reset();
                if (decoder.failed() || decoder.buffered_bytes() != 0) {
                    return Internal("stream decoder reset retained failed state");
                }
                return Status::Ok();
            }
            for (const ValidatedWireFrameView& view : *frames) {
                WireFrame frame;
                frame.header = view.header;
                frame.payload.assign(view.payload.begin(), view.payload.end());
                const Status checked = CheckFrameRoundTrip(frame, limits);
                if (!checked.ok()) return checked;
            }
            if (decoder.buffered_bytes() > limits.max_buffered_bytes) {
                return Internal("stream decoder exceeded its byte budget");
            }
        }
        const Status finish = decoder.Finish();
        if (!finish.ok() && !decoder.failed()) {
            return Internal("stream finish error did not latch failure");
        }
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Internal("stream decoder threw an exception");
    }
}

Status FuzzControlPayload(std::span<const std::byte> input) noexcept {
    try {
        if (input.size() > kMaxFrameFuzzInputBytes) {
            return Status::Error(StatusCode::kResourceExhausted);
        }
        const uint8_t selector =
            input.empty() ? 0 : static_cast<uint8_t>(input.front()) % 3u;
        const auto payload = input.empty() ? input : input.subspan(1);
        if (selector == 0) {
            auto decoded = ControlPayloadCodec::DecodeAck(payload);
            if (!decoded.ok()) return decoded.status();
            return CheckControlRoundTrip(
                *decoded,
                [](const AckPayload& value) {
                    return ControlPayloadCodec::EncodeAck(value);
                },
                [](std::span<const std::byte> bytes) {
                    return ControlPayloadCodec::DecodeAck(bytes);
                });
        }
        if (selector == 1) {
            const ControlPayloadLimits limits{
                .max_hello_sources = 128,
                .max_hello_payload_bytes = kMaxFrameFuzzInputBytes,
            };
            auto decoded = ControlPayloadCodec::DecodeSessionHello(payload, limits);
            if (!decoded.ok()) return decoded.status();
            return CheckControlRoundTrip(
                *decoded,
                [&limits](const SessionHello& value) {
                    return ControlPayloadCodec::EncodeSessionHello(value, limits);
                },
                [&limits](std::span<const std::byte> bytes) {
                    return ControlPayloadCodec::DecodeSessionHello(bytes, limits);
                });
        }
        auto decoded = ControlPayloadCodec::DecodeSessionDiscovery(payload);
        if (!decoded.ok()) return decoded.status();
        return CheckControlRoundTrip(
            *decoded,
            [](const SessionDiscovery& value) {
                return ControlPayloadCodec::EncodeSessionDiscovery(value);
            },
            [](std::span<const std::byte> bytes) {
                return ControlPayloadCodec::DecodeSessionDiscovery(bytes);
            });
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Internal("control payload decoder threw an exception");
    }
}

Status FuzzOneInput(std::span<const std::byte> input) noexcept {
    const FrameFuzzSelector selector = SelectFrameHarness(input);
    const auto payload = input.empty() ? input : input.subspan(1);
    if (selector == FrameFuzzSelector::kFrameBody) return FuzzFrameBody(payload);
    if (selector == FrameFuzzSelector::kStream) return FuzzFrameStream(payload);
    return FuzzControlPayload(payload);
}

}  // namespace mino::bridge::fuzz
