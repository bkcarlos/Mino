// Copyright 2026 The Mino Authors

#ifndef MINO_SECURITY_TLS_H_
#define MINO_SECURITY_TLS_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "mino/common/ids.h"
#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/security/security_domain.h"

namespace mino::security {



inline constexpr size_t kCertificateFingerprintBytes = 32;

// Public, bounded identity extracted only after certificate-chain verification.
// It is safe to pass through ordinary process memory. It contains no certificate
// or key material and is intentionally not a shared-memory wire structure.
struct AuthenticatedPeer {
    NodeId node_id;
    SecurityDomainId security_domain;
    std::array<std::byte, kCertificateFingerprintBytes> certificate_sha256{};
    uint64_t credential_generation = 0;

    bool complete() const noexcept {
        return node_id.value != 0 && security_domain.value != 0 &&
               credential_generation != 0;
    }
    friend bool operator==(const AuthenticatedPeer&,
                           const AuthenticatedPeer&) = default;
};

Result<std::string> PrincipalUri(NodeId node_id,
                                 SecurityDomainId security_domain);
Result<AuthenticatedPeer> ParsePrincipalUri(
    std::string_view uri, uint64_t credential_generation,
    std::span<const std::byte, kCertificateFingerprintBytes> fingerprint);

// Move-only secret storage. It has no formatting or string conversion API and
// scrubs owned storage before release. Callers must never put its bytes in SHM,
// diagnostics, telemetry, or ordinary logs.
class SecretPem final {
public:
    SecretPem() = default;
    static Result<SecretPem> FromBytes(std::span<const std::byte> bytes);
    static Result<SecretPem> FromString(std::string_view pem);

    SecretPem(const SecretPem&) = delete;
    SecretPem& operator=(const SecretPem&) = delete;
    SecretPem(SecretPem&& other) noexcept;
    SecretPem& operator=(SecretPem&& other) noexcept;
    ~SecretPem();

    std::span<const std::byte> bytes() const noexcept { return bytes_; }
    bool empty() const noexcept { return bytes_.empty(); }

private:
    explicit SecretPem(std::vector<std::byte> bytes) noexcept
        : bytes_(std::move(bytes)) {}
    void Clear() noexcept;

    std::vector<std::byte> bytes_;
};

struct TlsCredentials {
    AuthenticatedPeer local_identity;
    // PEM certificate followed by optional intermediates. Certificates and
    // trust anchors are public material; only private_key_pem is secret storage.
    std::string certificate_chain_pem;
    SecretPem private_key_pem;
    std::string trust_anchors_pem;
};

struct TlsCredentialSnapshot {
    uint64_t generation = 0;
    std::shared_ptr<const TlsCredentials> credentials;
};

class TlsCredentialProvider {
public:
    virtual ~TlsCredentialProvider() = default;
    // Must return an immutable, self-consistent snapshot. A connection retains
    // this snapshot for its lifetime; a later generation affects new connections.
    virtual Result<TlsCredentialSnapshot> Snapshot() = 0;
};

class StaticTlsCredentialProvider final : public TlsCredentialProvider {
public:
    static Result<std::shared_ptr<StaticTlsCredentialProvider>> Create(
        TlsCredentials credentials);

    Result<TlsCredentialSnapshot> Snapshot() override;
    Status Rotate(TlsCredentials credentials);

private:
    explicit StaticTlsCredentialProvider(
        std::shared_ptr<const TlsCredentials> credentials) noexcept;

    std::mutex mutex_;
    uint64_t generation_ = 1;
    std::shared_ptr<const TlsCredentials> credentials_;
};

enum class TlsRole : uint8_t { kClient = 0, kServer = 1 };
enum class TlsIoNeed : uint8_t { kNone = 0, kRead = 1, kWrite = 2 };

struct TlsIoResult {
    size_t bytes = 0;
    TlsIoNeed need = TlsIoNeed::kNone;
    bool peer_closed = false;
};

struct TlsOperationalStats {
    uint64_t handshake_failures = 0;
    uint64_t certificate_expiry_unix_seconds = 0;
};

// One non-blocking TLS connection. Implementations never own the socket fd.
class TlsChannel {
public:
    virtual ~TlsChannel() = default;
    virtual Result<TlsIoResult> Handshake() noexcept = 0;
    virtual Result<TlsIoResult> Read(std::span<std::byte> output) noexcept = 0;
    virtual Result<TlsIoResult> Write(
        std::span<const std::byte> input) noexcept = 0;
    virtual bool handshake_complete() const noexcept = 0;
    // True when OpenSSL already holds decrypted application bytes and another
    // Read can progress without new socket readability.
    virtual bool has_buffered_read() const noexcept = 0;
    virtual Result<AuthenticatedPeer> peer() const noexcept = 0;
};

class TlsChannelFactory {
public:
    virtual ~TlsChannelFactory() = default;
    // Refreshes the immutable generation cache without binding a socket. Drivers
    // may call this outside their global connection lock before accepting peers.
    virtual Status Prepare() = 0;
    virtual Result<std::unique_ptr<TlsChannel>> Create(int socket_fd,
                                                       TlsRole role) = 0;
    // Cold-path aggregate. Implementations must not expose peer/certificate
    // identity or create per-certificate label sets.
    virtual TlsOperationalStats OperationalStats() const noexcept { return {}; }
};

// BCR OpenSSL-backed implementation. It requires TLS 1.3, verifies both peers,
// loads all credentials from immutable in-memory snapshots, and never falls back
// to host trust paths or host-installed TLS libraries.
Result<std::shared_ptr<TlsChannelFactory>> CreateOpenSslTlsChannelFactory(
    std::shared_ptr<TlsCredentialProvider> provider);

}  // namespace mino::security

#endif  // MINO_SECURITY_TLS_H_
