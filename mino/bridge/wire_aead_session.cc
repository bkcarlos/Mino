// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/bridge/wire_aead_session.h"

#include "mino/common/status.h"

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <new>
#include <string_view>
#include <utility>

namespace mino::bridge {
namespace {

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status Corruption(std::string_view message) {
    return Status::Error(StatusCode::kCorruption, message);
}

Status Internal(std::string_view message) {
    return Status::Error(StatusCode::kInternal, message);
}

Status Resource(std::string_view message) {
    return Status::Error(StatusCode::kResourceExhausted, message);
}

void WriteBe64(std::span<std::byte> bytes, size_t offset,
               uint64_t value) noexcept {
    for (size_t i = 0; i < 8; ++i) {
        bytes[offset + i] =
            static_cast<std::byte>((value >> (56 - 8 * i)) & 0xffu);
    }
}

void WriteBe16(std::span<std::byte> bytes, size_t offset,
               uint16_t value) noexcept {
    bytes[offset] = static_cast<std::byte>((value >> 8) & 0xffu);
    bytes[offset + 1] = static_cast<std::byte>(value & 0xffu);
}

uint16_t ReadBe16(std::span<const std::byte> bytes, size_t offset) noexcept {
    return static_cast<uint16_t>(
               (static_cast<uint16_t>(bytes[offset]) << 8) |
               static_cast<uint16_t>(bytes[offset + 1]));
}

void Scrub(std::span<std::byte> bytes) noexcept {
    volatile std::byte* data = bytes.data();
    for (size_t i = 0; i < bytes.size(); ++i) data[i] = std::byte{0};
}

Status HmacSha256(std::span<const std::byte> key,
                  std::span<const std::byte> message,
                  std::span<std::byte, 32> out) noexcept {
    unsigned int out_len = 0;
    if (HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
             reinterpret_cast<const unsigned char*>(message.data()),
             message.size(), reinterpret_cast<unsigned char*>(out.data()),
             &out_len) == nullptr ||
        out_len != 32) {
        return Internal("HMAC-SHA256 failed");
    }
    return Status::Ok();
}

// HKDF-Extract + Expand (RFC 5869) with SHA-256.
Result<std::array<std::byte, kWireAeadExporterLength>> HkdfSha256(
    std::span<const std::byte> ikm, std::span<const std::byte> salt,
    std::span<const std::byte> info) noexcept {
    try {
        std::array<std::byte, 32> prk{};
        // Extract: PRK = HMAC(salt, IKM). Empty salt → zeros of HashLen.
        std::array<std::byte, 32> zero_salt{};
        std::span<const std::byte> extract_salt =
            salt.empty() ? std::span<const std::byte>(zero_salt) : salt;
        MINO_RETURN_IF_ERROR(HmacSha256(extract_salt, ikm, prk));

        std::array<std::byte, kWireAeadExporterLength> okm{};
        std::array<std::byte, 32> previous{};
        size_t generated = 0;
        uint8_t counter = 1;
        while (generated < okm.size()) {
            std::vector<std::byte> block_input;
            block_input.reserve(previous.size() + info.size() + 1);
            if (counter > 1) {
                block_input.insert(block_input.end(), previous.begin(),
                                   previous.end());
            }
            block_input.insert(block_input.end(), info.begin(), info.end());
            block_input.push_back(static_cast<std::byte>(counter));
            MINO_RETURN_IF_ERROR(HmacSha256(prk, block_input, previous));
            const size_t copy =
                std::min(previous.size(), okm.size() - generated);
            std::memcpy(okm.data() + generated, previous.data(), copy);
            generated += copy;
            ++counter;
            if (counter == 0) {
                Scrub(prk);
                Scrub(previous);
                Scrub(block_input);
                return Internal("HKDF counter overflow");
            }
            Scrub(block_input);
        }
        Scrub(prk);
        Scrub(previous);
        return okm;
    } catch (const std::bad_alloc&) {
        return Resource("HKDF allocation failed");
    }
}

std::vector<std::byte> KeyShareMacInput(
    uint64_t sender_session_epoch, uint64_t receiver_session_epoch,
    std::span<const std::byte, kSessionKeyShareNonceLength> nonce) {
    std::vector<std::byte> input;
    constexpr std::string_view kPrefix = "mino-aead-keyshare-v1";
    input.resize(kPrefix.size() + 16 + kSessionKeyShareNonceLength);
    std::memcpy(input.data(), kPrefix.data(), kPrefix.size());
    WriteBe64(input, kPrefix.size(), sender_session_epoch);
    WriteBe64(input, kPrefix.size() + 8, receiver_session_epoch);
    std::memcpy(input.data() + kPrefix.size() + 16, nonce.data(),
                kSessionKeyShareNonceLength);
    return input;
}

bool ConstantTimeEqual(std::span<const std::byte> left,
                       std::span<const std::byte> right) noexcept {
    if (left.size() != right.size()) return false;
    unsigned diff = 0;
    for (size_t i = 0; i < left.size(); ++i) {
        diff |= static_cast<unsigned>(left[i]) ^
                static_cast<unsigned>(right[i]);
    }
    return diff == 0;
}

}  // namespace

std::array<std::byte, 16> WireAeadEpochContext(uint64_t epoch_a,
                                               uint64_t epoch_b) noexcept {
    std::array<std::byte, 16> context{};
    const uint64_t lo = std::min(epoch_a, epoch_b);
    const uint64_t hi = std::max(epoch_a, epoch_b);
    WriteBe64(context, 0, lo);
    WriteBe64(context, 8, hi);
    return context;
}

Result<WireAeadKeyring> MakeWireAeadKeyringFromExporterMaterial(
    std::span<const std::byte> material, bool local_is_client) noexcept {
    try {
        if (material.size() != kWireAeadExporterLength) {
            return Invalid("AEAD exporter material must be 64 bytes");
        }
        auto c2s = WireAeadKeyMaterial::FromBytes(
            material.first(kWireAeadKeyLength));
        if (!c2s.ok()) return c2s.status();
        auto s2c = WireAeadKeyMaterial::FromBytes(
            material.subspan(kWireAeadKeyLength));
        if (!s2c.ok()) return s2c.status();

        WireAeadKeyring keyring;
        WireAeadKey encode;
        WireAeadKey decode;
        if (local_is_client) {
            encode.key_id = kWireAeadClientToServerKeyId;
            encode.material = std::move(*c2s);
            decode.key_id = kWireAeadServerToClientKeyId;
            decode.material = std::move(*s2c);
        } else {
            encode.key_id = kWireAeadServerToClientKeyId;
            encode.material = std::move(*s2c);
            decode.key_id = kWireAeadClientToServerKeyId;
            decode.material = std::move(*c2s);
        }
        MINO_RETURN_IF_ERROR(keyring.SetEncodeKey(std::move(encode)));
        MINO_RETURN_IF_ERROR(keyring.AddDecodeKey(std::move(decode)));
        return keyring;
    } catch (const std::bad_alloc&) {
        return Resource("AEAD keyring allocation failed");
    }
}

Result<WireAeadKeyring> DeriveWireAeadKeyringFromSharedSecret(
    std::span<const std::byte> shared_secret,
    std::span<const std::byte> context, bool local_is_client) noexcept {
    if (shared_secret.empty()) {
        return Invalid("AEAD shared secret is empty");
    }
    constexpr std::string_view kInfo = "mino wire aead hkdf v1";
    auto material =
        HkdfSha256(shared_secret, context,
                   std::as_bytes(std::span(kInfo.data(), kInfo.size())));
    if (!material.ok()) return material.status();
    auto keyring =
        MakeWireAeadKeyringFromExporterMaterial(*material, local_is_client);
    Scrub(*material);
    return keyring;
}

Result<std::vector<std::byte>> EncodeSessionKeyShare(
    const SessionKeyShare& share) noexcept {
    try {
        std::vector<std::byte> output(kSessionKeyShareWireSize);
        WriteBe16(output, 0, kSessionKeySharePayloadVersion);
        WriteBe16(output, 2, 0);
        std::memcpy(output.data() + 4, share.nonce.data(),
                    kSessionKeyShareNonceLength);
        std::memcpy(output.data() + 4 + kSessionKeyShareNonceLength,
                    share.mac.data(), kSessionKeyShareMacLength);
        return output;
    } catch (const std::bad_alloc&) {
        return Resource("SessionKeyShare encode allocation failed");
    }
}

Result<SessionKeyShare> DecodeSessionKeyShare(
    std::span<const std::byte> payload) noexcept {
    if (payload.size() != kSessionKeyShareWireSize) {
        return Corruption("SessionKeyShare payload length is invalid");
    }
    if (ReadBe16(payload, 0) != kSessionKeySharePayloadVersion) {
        return Corruption("SessionKeyShare version is unsupported");
    }
    if (ReadBe16(payload, 2) != 0) {
        return Corruption("SessionKeyShare flags must be zero");
    }
    SessionKeyShare share;
    std::memcpy(share.nonce.data(), payload.data() + 4,
                kSessionKeyShareNonceLength);
    std::memcpy(share.mac.data(),
                payload.data() + 4 + kSessionKeyShareNonceLength,
                kSessionKeyShareMacLength);
    return share;
}

Result<SessionKeyShare> MakeSessionKeyShare(
    std::span<const std::byte> psk, uint64_t sender_session_epoch,
    uint64_t receiver_session_epoch) noexcept {
    try {
        if (psk.size() < 16) {
            return Invalid("AEAD KeyShare PSK must be at least 16 bytes");
        }
        if (sender_session_epoch == 0 || receiver_session_epoch == 0) {
            return Invalid("AEAD KeyShare epochs must be nonzero");
        }
        SessionKeyShare share;
        if (RAND_bytes(reinterpret_cast<unsigned char*>(share.nonce.data()),
                       static_cast<int>(share.nonce.size())) != 1) {
            return Internal("AEAD KeyShare nonce generation failed");
        }
        auto mac_input = KeyShareMacInput(sender_session_epoch,
                                          receiver_session_epoch, share.nonce);
        MINO_RETURN_IF_ERROR(HmacSha256(psk, mac_input, share.mac));
        Scrub(mac_input);
        return share;
    } catch (const std::bad_alloc&) {
        return Resource("SessionKeyShare allocation failed");
    }
}

Status VerifySessionKeyShare(const SessionKeyShare& share,
                             std::span<const std::byte> psk,
                             uint64_t sender_session_epoch,
                             uint64_t receiver_session_epoch) noexcept {
    try {
        if (psk.size() < 16) {
            return Invalid("AEAD KeyShare PSK must be at least 16 bytes");
        }
        auto mac_input = KeyShareMacInput(sender_session_epoch,
                                          receiver_session_epoch, share.nonce);
        std::array<std::byte, 32> expected{};
        const Status status = HmacSha256(psk, mac_input, expected);
        Scrub(mac_input);
        if (!status.ok()) return status;
        if (!ConstantTimeEqual(share.mac, expected)) {
            Scrub(expected);
            return Corruption("SessionKeyShare MAC verification failed");
        }
        Scrub(expected);
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Resource("SessionKeyShare verify allocation failed");
    }
}

Result<WireAeadKeyring> DeriveWireAeadKeyringFromKeyShare(
    std::span<const std::byte> psk,
    std::span<const std::byte, kSessionKeyShareNonceLength> local_nonce,
    std::span<const std::byte, kSessionKeyShareNonceLength> peer_nonce,
    uint64_t local_session_epoch, uint64_t remote_session_epoch,
    bool local_is_client) noexcept {
    try {
        if (psk.size() < 16) {
            return Invalid("AEAD KeyShare PSK must be at least 16 bytes");
        }
        if (std::equal(local_nonce.begin(), local_nonce.end(),
                       peer_nonce.begin())) {
            return Corruption("SessionKeyShare nonces must differ");
        }
        std::array<std::byte, 16 + 2 * kSessionKeyShareNonceLength> context{};
        auto epochs = WireAeadEpochContext(local_session_epoch,
                                           remote_session_epoch);
        std::memcpy(context.data(), epochs.data(), epochs.size());
        // Canonical nonce order: lexicographic min || max.
        const bool local_first = std::lexicographical_compare(
            local_nonce.begin(), local_nonce.end(), peer_nonce.begin(),
            peer_nonce.end());
        if (local_first) {
            std::memcpy(context.data() + 16, local_nonce.data(),
                        kSessionKeyShareNonceLength);
            std::memcpy(context.data() + 16 + kSessionKeyShareNonceLength,
                        peer_nonce.data(), kSessionKeyShareNonceLength);
        } else {
            std::memcpy(context.data() + 16, peer_nonce.data(),
                        kSessionKeyShareNonceLength);
            std::memcpy(context.data() + 16 + kSessionKeyShareNonceLength,
                        local_nonce.data(), kSessionKeyShareNonceLength);
        }
        return DeriveWireAeadKeyringFromSharedSecret(psk, context,
                                                     local_is_client);
    } catch (const std::bad_alloc&) {
        return Resource("AEAD KeyShare derive allocation failed");
    }
}

void ApplySessionAeadFlags(WireFrameHeader* header) noexcept {
    if (header == nullptr) return;
    header->flags =
        static_cast<uint16_t>(header->flags & ~FlagValue(FrameFlag::kPayloadCrcPresent));
    header->flags =
        static_cast<uint16_t>(header->flags | FlagValue(FrameFlag::kAeadPresent));
}

}  // namespace mino::bridge
