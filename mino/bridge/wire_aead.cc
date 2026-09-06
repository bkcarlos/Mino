// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/bridge/wire_aead.h"

#include <openssl/evp.h>
#include <openssl/rand.h>

#include <cstring>
#include <memory>
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

struct EvpCipherCtxDeleter {
    void operator()(EVP_CIPHER_CTX* value) const noexcept {
        EVP_CIPHER_CTX_free(value);
    }
};

using UniqueEvpCipherCtx =
    std::unique_ptr<EVP_CIPHER_CTX, EvpCipherCtxDeleter>;

}  // namespace

WireAeadKeyMaterial::WireAeadKeyMaterial(
    std::array<std::byte, kWireAeadKeyLength> key) noexcept
    : key_(key), empty_(false) {}

Result<WireAeadKeyMaterial> WireAeadKeyMaterial::FromBytes(
    std::span<const std::byte> bytes) {
    if (bytes.size() != kWireAeadKeyLength) {
        return Invalid("AEAD key must be 32 bytes");
    }
    std::array<std::byte, kWireAeadKeyLength> key{};
    std::memcpy(key.data(), bytes.data(), kWireAeadKeyLength);
    return WireAeadKeyMaterial(key);
}

WireAeadKeyMaterial::WireAeadKeyMaterial(WireAeadKeyMaterial&& other) noexcept
    : key_(other.key_), empty_(other.empty_) {
    other.Clear();
}

WireAeadKeyMaterial& WireAeadKeyMaterial::operator=(
    WireAeadKeyMaterial&& other) noexcept {
    if (this != &other) {
        Clear();
        key_ = other.key_;
        empty_ = other.empty_;
        other.Clear();
    }
    return *this;
}

WireAeadKeyMaterial::~WireAeadKeyMaterial() { Clear(); }

void WireAeadKeyMaterial::Clear() noexcept {
    volatile std::byte* data = key_.data();
    for (size_t index = 0; index < key_.size(); ++index) {
        data[index] = std::byte{0};
    }
    empty_ = true;
}

Status WireAeadKeyring::SetEncodeKey(WireAeadKey key) noexcept {
    try {
        if (key.material.empty()) {
            return Invalid("AEAD encode key material is empty");
        }
        if (key.key_id == 0) {
            return Invalid("AEAD key_id must be non-zero");
        }
        keys_.insert_or_assign(key.key_id, std::move(key.material));
        encode_key_id_ = key.key_id;
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Resource("AEAD keyring allocation failed");
    }
}

Status WireAeadKeyring::AddDecodeKey(WireAeadKey key) noexcept {
    try {
        if (key.material.empty()) {
            return Invalid("AEAD decode key material is empty");
        }
        if (key.key_id == 0) {
            return Invalid("AEAD key_id must be non-zero");
        }
        keys_.insert_or_assign(key.key_id, std::move(key.material));
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Resource("AEAD keyring allocation failed");
    }
}

void WireAeadKeyring::Clear() noexcept {
    keys_.clear();
    encode_key_id_.reset();
}

const WireAeadKeyMaterial* WireAeadKeyring::FindKey(
    uint32_t key_id) const noexcept {
    const auto it = keys_.find(key_id);
    if (it == keys_.end() || it->second.empty()) return nullptr;
    return &it->second;
}

const WireAeadKeyMaterial* WireAeadKeyring::EncodeKey(
    uint32_t* key_id) const noexcept {
    if (!encode_key_id_.has_value()) return nullptr;
    const WireAeadKeyMaterial* material = FindKey(*encode_key_id_);
    if (material == nullptr) return nullptr;
    if (key_id != nullptr) *key_id = *encode_key_id_;
    return material;
}

Result<std::vector<std::byte>> SealWireAead(
    const WireAeadKeyMaterial& key, std::span<const std::byte> aad,
    std::span<const std::byte> plaintext) noexcept {
    try {
        if (key.empty()) return Invalid("AEAD key material is empty");
        if (plaintext.size() >
            std::numeric_limits<int>::max() - kWireAeadOverhead) {
            return Resource("AEAD plaintext exceeds OpenSSL length limits");
        }
        if (aad.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
            return Resource("AEAD AAD exceeds OpenSSL length limits");
        }

        std::vector<std::byte> sealed(kWireAeadOverhead + plaintext.size());
        std::span<std::byte> nonce = std::span(sealed).first(kWireAeadNonceLength);
        if (RAND_bytes(reinterpret_cast<unsigned char*>(nonce.data()),
                       static_cast<int>(nonce.size())) != 1) {
            return Internal("AEAD nonce generation failed");
        }

        UniqueEvpCipherCtx ctx(EVP_CIPHER_CTX_new());
        if (!ctx) return Resource("AEAD cipher context allocation failed");

        if (EVP_EncryptInit_ex(ctx.get(), EVP_aes_256_gcm(), nullptr, nullptr,
                               nullptr) != 1 ||
            EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_SET_IVLEN,
                               static_cast<int>(kWireAeadNonceLength),
                               nullptr) != 1 ||
            EVP_EncryptInit_ex(
                ctx.get(), nullptr, nullptr,
                reinterpret_cast<const unsigned char*>(key.bytes().data()),
                reinterpret_cast<const unsigned char*>(nonce.data())) != 1) {
            return Internal("AEAD encrypt init failed");
        }

        int out_len = 0;
        if (!aad.empty()) {
            if (EVP_EncryptUpdate(
                    ctx.get(), nullptr, &out_len,
                    reinterpret_cast<const unsigned char*>(aad.data()),
                    static_cast<int>(aad.size())) != 1) {
                return Internal("AEAD AAD update failed");
            }
        }

        unsigned char* ciphertext = reinterpret_cast<unsigned char*>(
            sealed.data() + kWireAeadNonceLength);
        if (!plaintext.empty()) {
            if (EVP_EncryptUpdate(
                    ctx.get(), ciphertext, &out_len,
                    reinterpret_cast<const unsigned char*>(plaintext.data()),
                    static_cast<int>(plaintext.size())) != 1) {
                return Internal("AEAD encrypt update failed");
            }
        } else {
            out_len = 0;
        }
        int final_len = 0;
        if (EVP_EncryptFinal_ex(ctx.get(), ciphertext + out_len, &final_len) !=
            1) {
            return Internal("AEAD encrypt final failed");
        }
        if (static_cast<size_t>(out_len + final_len) != plaintext.size()) {
            return Internal("AEAD ciphertext length mismatch");
        }

        unsigned char* tag = reinterpret_cast<unsigned char*>(
            sealed.data() + kWireAeadNonceLength + plaintext.size());
        if (EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_GET_TAG,
                               static_cast<int>(kWireAeadTagLength),
                               tag) != 1) {
            return Internal("AEAD tag extraction failed");
        }
        return sealed;
    } catch (const std::bad_alloc&) {
        return Resource("AEAD seal allocation failed");
    } catch (const std::length_error&) {
        return Resource("AEAD seal allocation failed");
    }
}

Status OpenWireAead(const WireAeadKeyMaterial& key,
                    std::span<const std::byte> aad,
                    std::span<const std::byte> sealed,
                    std::span<std::byte> plaintext_out) noexcept {
    try {
        if (key.empty()) return Invalid("AEAD key material is empty");
        if (sealed.size() < kWireAeadOverhead) {
            return Corruption("AEAD sealed payload is truncated");
        }
        const size_t plaintext_size = sealed.size() - kWireAeadOverhead;
        if (plaintext_out.size() != plaintext_size) {
            return Internal("AEAD plaintext buffer size mismatch");
        }
        if (plaintext_size >
                static_cast<size_t>(std::numeric_limits<int>::max()) ||
            aad.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
            return Resource("AEAD OpenSSL length limits exceeded");
        }

        const unsigned char* nonce =
            reinterpret_cast<const unsigned char*>(sealed.data());
        const unsigned char* ciphertext = nonce + kWireAeadNonceLength;
        const unsigned char* tag = ciphertext + plaintext_size;

        UniqueEvpCipherCtx ctx(EVP_CIPHER_CTX_new());
        if (!ctx) return Resource("AEAD cipher context allocation failed");

        if (EVP_DecryptInit_ex(ctx.get(), EVP_aes_256_gcm(), nullptr, nullptr,
                               nullptr) != 1 ||
            EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_SET_IVLEN,
                               static_cast<int>(kWireAeadNonceLength),
                               nullptr) != 1 ||
            EVP_DecryptInit_ex(
                ctx.get(), nullptr, nullptr,
                reinterpret_cast<const unsigned char*>(key.bytes().data()),
                nonce) != 1) {
            return Internal("AEAD decrypt init failed");
        }

        int out_len = 0;
        if (!aad.empty()) {
            if (EVP_DecryptUpdate(
                    ctx.get(), nullptr, &out_len,
                    reinterpret_cast<const unsigned char*>(aad.data()),
                    static_cast<int>(aad.size())) != 1) {
                return Internal("AEAD AAD update failed");
            }
        }

        unsigned char* plaintext =
            reinterpret_cast<unsigned char*>(plaintext_out.data());
        if (plaintext_size != 0) {
            if (EVP_DecryptUpdate(ctx.get(), plaintext, &out_len, ciphertext,
                                  static_cast<int>(plaintext_size)) != 1) {
                return Corruption("AEAD authentication failed");
            }
        } else {
            out_len = 0;
        }

        if (EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_SET_TAG,
                               static_cast<int>(kWireAeadTagLength),
                               const_cast<unsigned char*>(tag)) != 1) {
            return Internal("AEAD tag install failed");
        }

        int final_len = 0;
        if (EVP_DecryptFinal_ex(ctx.get(), plaintext + out_len, &final_len) !=
            1) {
            return Corruption("AEAD authentication failed");
        }
        if (static_cast<size_t>(out_len + final_len) != plaintext_size) {
            return Internal("AEAD plaintext length mismatch");
        }
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Resource("AEAD open allocation failed");
    } catch (const std::length_error&) {
        return Resource("AEAD open allocation failed");
    }
}

}  // namespace mino::bridge
