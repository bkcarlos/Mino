// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_BRIDGE_WIRE_AEAD_H_
#define MINO_BRIDGE_WIRE_AEAD_H_

#include <array>
#include <cstddef>
#include <optional>
#include <cstdint>
#include <span>
#include <unordered_map>
#include <vector>

#include "mino/common/result.h"
#include "mino/common/status.h"

namespace mino::bridge {

// In-frame AEAD for Canonical Wire frames (AES-256-GCM via OpenSSL EVP).
// This is independent of socket-layer TLS (mino/security/tls.h). Session key
// establishment (TLS exporter / authenticated KeyShare) and BridgePipeline
// auto-install live in wire_aead_session.*; this type only holds key material.
inline constexpr size_t kWireAeadKeyLength = 32;
inline constexpr size_t kWireAeadNonceLength = 12;
inline constexpr size_t kWireAeadTagLength = 16;
inline constexpr size_t kWireAeadOverhead =
    kWireAeadNonceLength + kWireAeadTagLength;

// Optional 4-byte header field shared with PAYLOAD_CRC when AEAD_PRESENT is
// set. Stores the AEAD key_id (big-endian). The 16-byte GCM tag travels with
// the ciphertext in the wire payload (see WireFrameCodec).
inline constexpr uint32_t kWireAeadKeyIdLength = 4;

// Move-only 256-bit key. Scrubs owned bytes on destruction. Never place in
// SHM, logs, telemetry, or ordinary diagnostics.
class WireAeadKeyMaterial final {
public:
    WireAeadKeyMaterial() = default;
    static Result<WireAeadKeyMaterial> FromBytes(
        std::span<const std::byte> bytes);

    WireAeadKeyMaterial(const WireAeadKeyMaterial&) = delete;
    WireAeadKeyMaterial& operator=(const WireAeadKeyMaterial&) = delete;
    WireAeadKeyMaterial(WireAeadKeyMaterial&& other) noexcept;
    WireAeadKeyMaterial& operator=(WireAeadKeyMaterial&& other) noexcept;
    ~WireAeadKeyMaterial();

    std::span<const std::byte, kWireAeadKeyLength> bytes() const noexcept {
        return key_;
    }
    bool empty() const noexcept { return empty_; }

private:
    explicit WireAeadKeyMaterial(
        std::array<std::byte, kWireAeadKeyLength> key) noexcept;
    void Clear() noexcept;

    std::array<std::byte, kWireAeadKeyLength> key_{};
    bool empty_ = true;
};

struct WireAeadKey {
    uint32_t key_id = 0;
    WireAeadKeyMaterial material;
};

// Minimal keyring for the wire codec. One encode key plus zero or more decode
// keys keyed by key_id. Session helpers install keys after TLS exporter or
// KeyShare; this type does not perform PKI or handshake itself.
class WireAeadKeyring final {
public:
    WireAeadKeyring() = default;
    WireAeadKeyring(const WireAeadKeyring&) = delete;
    WireAeadKeyring& operator=(const WireAeadKeyring&) = delete;
    WireAeadKeyring(WireAeadKeyring&&) noexcept = default;
    WireAeadKeyring& operator=(WireAeadKeyring&&) noexcept = default;

    // Installs the key used for encoding. Also registered for decoding.
    Status SetEncodeKey(WireAeadKey key) noexcept;
    // Registers an additional peer/decode key without changing the encode key.
    Status AddDecodeKey(WireAeadKey key) noexcept;
    void Clear() noexcept;

    bool has_encode_key() const noexcept { return encode_key_id_.has_value(); }
    std::optional<uint32_t> encode_key_id() const noexcept {
        return encode_key_id_;
    }

    const WireAeadKeyMaterial* FindKey(uint32_t key_id) const noexcept;
    const WireAeadKeyMaterial* EncodeKey(uint32_t* key_id) const noexcept;

private:
    std::optional<uint32_t> encode_key_id_;
    std::unordered_map<uint32_t, WireAeadKeyMaterial> keys_;
};

// Encrypts plaintext into nonce||ciphertext||tag. AAD authenticates the clear
// header bytes (and clear control opcode when present).
Result<std::vector<std::byte>> SealWireAead(
    const WireAeadKeyMaterial& key, std::span<const std::byte> aad,
    std::span<const std::byte> plaintext) noexcept;

// Decrypts nonce||ciphertext||tag in place into plaintext_out (size must match
// ciphertext length). Returns Corruption on authentication failure.
Status OpenWireAead(const WireAeadKeyMaterial& key,
                    std::span<const std::byte> aad,
                    std::span<const std::byte> sealed,
                    std::span<std::byte> plaintext_out) noexcept;

}  // namespace mino::bridge

#endif  // MINO_BRIDGE_WIRE_AEAD_H_
