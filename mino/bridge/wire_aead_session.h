// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_BRIDGE_WIRE_AEAD_SESSION_H_
#define MINO_BRIDGE_WIRE_AEAD_SESSION_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

#include "mino/bridge/wire_aead.h"
#include "mino/bridge/wire_frame.h"
#include "mino/common/result.h"
#include "mino/common/status.h"

namespace mino::bridge {

// TLS 1.3 exporter label for frame-AEAD session keys (RFC 8446 §7.5).
inline constexpr std::string_view kWireAeadTlsExporterLabel =
    "EXPORTER-mino-wire-aead-v1";
inline constexpr size_t kWireAeadExporterLength = 64;
inline constexpr uint32_t kWireAeadClientToServerKeyId = 1;
inline constexpr uint32_t kWireAeadServerToClientKeyId = 2;

inline constexpr uint16_t kSessionKeySharePayloadVersion = 1;
inline constexpr size_t kSessionKeyShareNonceLength = 32;
inline constexpr size_t kSessionKeyShareMacLength = 32;
inline constexpr size_t kSessionKeyShareWireSize =
    4 + kSessionKeyShareNonceLength + kSessionKeyShareMacLength;

struct SessionKeyShare {
    std::array<std::byte, kSessionKeyShareNonceLength> nonce{};
    std::array<std::byte, kSessionKeyShareMacLength> mac{};

    bool operator==(const SessionKeyShare&) const = default;
};

// Builds a WireAeadKeyring from 64 bytes of TLS exporter (or HKDF) material:
// bytes[0..32) = client→server key, bytes[32..64) = server→client key.
// local_is_client selects the encode key; the peer direction is registered for
// decode. key_ids are kWireAeadClientToServerKeyId /
// kWireAeadServerToClientKeyId.
Result<WireAeadKeyring> MakeWireAeadKeyringFromExporterMaterial(
    std::span<const std::byte> material, bool local_is_client) noexcept;

// HKDF-SHA256(secret, salt=context, info="mino wire aead hkdf v1") → 64 bytes,
// then MakeWireAeadKeyringFromExporterMaterial.
Result<WireAeadKeyring> DeriveWireAeadKeyringFromSharedSecret(
    std::span<const std::byte> shared_secret,
    std::span<const std::byte> context, bool local_is_client) noexcept;

// Canonical exporter/HKDF context: min(epoch)*||max(epoch)* as big-endian.
std::array<std::byte, 16> WireAeadEpochContext(uint64_t epoch_a,
                                               uint64_t epoch_b) noexcept;

Result<std::vector<std::byte>> EncodeSessionKeyShare(
    const SessionKeyShare& share) noexcept;
Result<SessionKeyShare> DecodeSessionKeyShare(
    std::span<const std::byte> payload) noexcept;

// Builds an authenticated KeyShare: random nonce + HMAC-SHA256(psk, ...).
Result<SessionKeyShare> MakeSessionKeyShare(
    std::span<const std::byte> psk, uint64_t sender_session_epoch,
    uint64_t receiver_session_epoch) noexcept;

Status VerifySessionKeyShare(const SessionKeyShare& share,
                             std::span<const std::byte> psk,
                             uint64_t sender_session_epoch,
                             uint64_t receiver_session_epoch) noexcept;

// After both nonces are known, derive the session keyring from PSK.
Result<WireAeadKeyring> DeriveWireAeadKeyringFromKeyShare(
    std::span<const std::byte> psk,
    std::span<const std::byte, kSessionKeyShareNonceLength> local_nonce,
    std::span<const std::byte, kSessionKeyShareNonceLength> peer_nonce,
    uint64_t local_session_epoch, uint64_t remote_session_epoch,
    bool local_is_client) noexcept;

// Marks a data-frame header for session AEAD (clears payload-CRC; AEAD and CRC
// share the optional 4-byte header slot).
void ApplySessionAeadFlags(WireFrameHeader* header) noexcept;

// Convenience for LengthPrefixedFrameDecoder / external encode helpers: bind
// the session keyring so AEAD_PRESENT frames decode without a separate manual
// SetAeadKeyring-only workflow when the caller already holds the pipeline
// keyring pointer.
inline void BindAeadKeyring(LengthPrefixedFrameDecoder* decoder,
                            const WireAeadKeyring* keyring) noexcept {
    if (decoder != nullptr) decoder->SetAeadKeyring(keyring);
}

}  // namespace mino::bridge

#endif  // MINO_BRIDGE_WIRE_AEAD_SESSION_H_
