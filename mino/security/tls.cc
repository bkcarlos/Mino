// Copyright 2026 The Mino Authors

#include "mino/security/tls.h"

#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <atomic>
#include <charconv>
#include <limits>
#include <new>
#include <utility>

namespace mino::security {
namespace {

constexpr std::string_view kPrincipalPrefix = "spiffe://mino/domain/";
constexpr std::string_view kNodeSeparator = "/node/";

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}
Status Denied(std::string_view message) {
    return Status::Error(StatusCode::kPermissionDenied, message);
}
Status Unavailable(std::string_view message) {
    return Status::Error(StatusCode::kUnavailable, message);
}
Status Internal(std::string_view message) {
    return Status::Error(StatusCode::kInternal, message);
}
Status AllocationFailure() {
    return Status::Error(StatusCode::kResourceExhausted,
                         "TLS allocation failed");
}

template <typename Integer>
bool ParseDecimal(std::string_view text, Integer* value) noexcept {
    if (text.empty() || value == nullptr ||
        (text.size() > 1 && text.front() == '0')) {
        return false;
    }
    Integer parsed = 0;
    const auto result =
        std::from_chars(text.data(), text.data() + text.size(), parsed, 10);
    if (result.ec != std::errc{} || result.ptr != text.data() + text.size() ||
        parsed == 0) {
        return false;
    }
    *value = parsed;
    return true;
}

struct BioDeleter {
    void operator()(BIO* value) const noexcept { BIO_free(value); }
};
struct X509Deleter {
    void operator()(X509* value) const noexcept { X509_free(value); }
};
struct PkeyDeleter {
    void operator()(EVP_PKEY* value) const noexcept { EVP_PKEY_free(value); }
};
struct ContextDeleter {
    void operator()(SSL_CTX* value) const noexcept { SSL_CTX_free(value); }
};
struct SslDeleter {
    void operator()(SSL* value) const noexcept { SSL_free(value); }
};
struct GeneralNamesDeleter {
    void operator()(GENERAL_NAMES* value) const noexcept {
        GENERAL_NAMES_free(value);
    }
};

using UniqueBio = std::unique_ptr<BIO, BioDeleter>;
using UniqueX509 = std::unique_ptr<X509, X509Deleter>;
using UniquePkey = std::unique_ptr<EVP_PKEY, PkeyDeleter>;
using UniqueContext = std::unique_ptr<SSL_CTX, ContextDeleter>;
using SharedContext = std::shared_ptr<SSL_CTX>;
using UniqueSsl = std::unique_ptr<SSL, SslDeleter>;
using UniqueGeneralNames = std::unique_ptr<GENERAL_NAMES, GeneralNamesDeleter>;

Result<AuthenticatedPeer> ExtractPeer(X509* certificate,
                                      uint64_t generation) {
    if (certificate == nullptr || generation == 0) {
        return Denied("TLS peer certificate or generation is missing");
    }
    std::array<std::byte, kCertificateFingerprintBytes> fingerprint{};
    unsigned int fingerprint_size = 0;
    if (X509_digest(certificate, EVP_sha256(),
                    reinterpret_cast<unsigned char*>(fingerprint.data()),
                    &fingerprint_size) != 1 ||
        fingerprint_size != fingerprint.size()) {
        return Internal("TLS peer certificate fingerprint failed");
    }

    UniqueGeneralNames names(static_cast<GENERAL_NAMES*>(
        X509_get_ext_d2i(certificate, NID_subject_alt_name, nullptr, nullptr)));
    if (!names) return Denied("TLS peer certificate has no SAN");

    std::string_view principal_uri;
    size_t uri_count = 0;
    const int count = sk_GENERAL_NAME_num(names.get());
    for (int index = 0; index < count; ++index) {
        const GENERAL_NAME* name = sk_GENERAL_NAME_value(names.get(), index);
        if (name == nullptr || name->type != GEN_URI) continue;
        ++uri_count;
        const ASN1_IA5STRING* uri = name->d.uniformResourceIdentifier;
        const unsigned char* bytes = ASN1_STRING_get0_data(uri);
        const int size = ASN1_STRING_length(uri);
        if (bytes == nullptr || size <= 0) {
            return Denied("TLS peer certificate has an invalid URI SAN");
        }
        const std::string_view candidate(
            reinterpret_cast<const char*>(bytes), static_cast<size_t>(size));
        if (candidate.find('\0') != std::string_view::npos) {
            return Denied("TLS peer certificate URI SAN contains NUL");
        }
        principal_uri = candidate;
    }
    if (uri_count != 1) {
        return Denied("TLS peer certificate must have exactly one URI SAN");
    }
    return ParsePrincipalUri(principal_uri, generation, fingerprint);
}

Status ValidateCredentials(const TlsCredentials& credentials) {
    if (credentials.local_identity.node_id.value == 0 ||
        credentials.local_identity.security_domain.value == 0 ||
        credentials.certificate_chain_pem.empty() ||
        credentials.private_key_pem.empty() ||
        credentials.trust_anchors_pem.empty()) {
        return Invalid("TLS credentials are incomplete");
    }
    return Status::Ok();
}

Result<UniqueContext> BuildContext(const TlsCredentials& credentials,
                                   uint64_t generation) {
    MINO_RETURN_IF_ERROR(ValidateCredentials(credentials));
    UniqueContext context(SSL_CTX_new(TLS_method()));
    if (!context) return AllocationFailure();
    if (SSL_CTX_set_min_proto_version(context.get(), TLS1_3_VERSION) != 1 ||
        SSL_CTX_set_max_proto_version(context.get(), TLS1_3_VERSION) != 1) {
        return Internal("TLS 1.3 context configuration failed");
    }
    SSL_CTX_set_verify(context.get(),
                       SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
    SSL_CTX_set_verify_depth(context.get(), 8);
    SSL_CTX_set_options(context.get(), SSL_OP_NO_RENEGOTIATION);
    // Session resumption is intentionally disabled. Besides keeping credential
    // rotation semantics simple, this prevents unsolicited TLS 1.3 NewSessionTicket
    // records from initiating a read operation before application traffic exists.
    if (SSL_CTX_set_num_tickets(context.get(), 0) != 1) {
        return Internal("TLS session-ticket configuration failed");
    }
    SSL_CTX_set_mode(context.get(), SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER |
                                        SSL_MODE_ENABLE_PARTIAL_WRITE);

    UniqueBio certificates(BIO_new_mem_buf(
        credentials.certificate_chain_pem.data(),
        static_cast<int>(credentials.certificate_chain_pem.size())));
    if (!certificates) return AllocationFailure();
    UniqueX509 leaf(PEM_read_bio_X509(certificates.get(), nullptr, nullptr,
                                     nullptr));
    if (!leaf) return Invalid("TLS certificate chain PEM is invalid");
    MINO_ASSIGN_OR_RETURN(const AuthenticatedPeer local,
                          ExtractPeer(leaf.get(), generation));
    if (local.node_id != credentials.local_identity.node_id ||
        local.security_domain !=
            credentials.local_identity.security_domain) {
        return Denied("TLS certificate principal does not match local identity");
    }
    if (SSL_CTX_use_certificate(context.get(), leaf.get()) != 1) {
        return Invalid("TLS leaf certificate is unusable");
    }
    for (;;) {
        ERR_clear_error();
        UniqueX509 intermediate(PEM_read_bio_X509(
            certificates.get(), nullptr, nullptr, nullptr));
        if (!intermediate) {
            ERR_clear_error();
            break;
        }
        if (SSL_CTX_add_extra_chain_cert(context.get(),
                                         intermediate.release()) != 1) {
            return Invalid("TLS intermediate certificate is unusable");
        }
    }

    const auto private_key = credentials.private_key_pem.bytes();
    UniqueBio key_bio(BIO_new_mem_buf(private_key.data(),
                                      static_cast<int>(private_key.size())));
    if (!key_bio) return AllocationFailure();
    UniquePkey key(
        PEM_read_bio_PrivateKey(key_bio.get(), nullptr, nullptr, nullptr));
    if (!key || SSL_CTX_use_PrivateKey(context.get(), key.get()) != 1 ||
        SSL_CTX_check_private_key(context.get()) != 1) {
        return Invalid("TLS private key is invalid or does not match certificate");
    }

    UniqueBio anchors(BIO_new_mem_buf(credentials.trust_anchors_pem.data(),
                                      static_cast<int>(
                                          credentials.trust_anchors_pem.size())));
    if (!anchors) return AllocationFailure();
    X509_STORE* store = SSL_CTX_get_cert_store(context.get());
    size_t anchor_count = 0;
    for (;;) {
        ERR_clear_error();
        UniqueX509 anchor(
            PEM_read_bio_X509(anchors.get(), nullptr, nullptr, nullptr));
        if (!anchor) {
            ERR_clear_error();
            break;
        }
        if (X509_STORE_add_cert(store, anchor.get()) != 1) {
            return Invalid("TLS trust anchor PEM contains an unusable certificate");
        }
        ++anchor_count;
    }
    if (anchor_count == 0) return Invalid("TLS trust anchor PEM is invalid");
    return context;
}

struct TlsOperationalCounters {
    std::atomic<uint64_t> handshake_failures{0};
    std::atomic<uint64_t> certificate_expiry_unix_seconds{0};
};

Result<TlsIoResult> TranslateIoResult(SSL* ssl, int result,
                                      size_t bytes) noexcept {
    if (result == 1) return TlsIoResult{.bytes = bytes};
    switch (SSL_get_error(ssl, result)) {
        case SSL_ERROR_WANT_READ:
            return TlsIoResult{.need = TlsIoNeed::kRead};
        case SSL_ERROR_WANT_WRITE:
            return TlsIoResult{.need = TlsIoNeed::kWrite};
        case SSL_ERROR_ZERO_RETURN:
            return TlsIoResult{.peer_closed = true};
        default:
            ERR_clear_error();
            return Unavailable("TLS connection failed");
    }
}

class OpenSslTlsChannel final : public TlsChannel {
public:
    OpenSslTlsChannel(SharedContext context, UniqueSsl ssl,
                      uint64_t generation,
                      std::shared_ptr<TlsOperationalCounters> counters) noexcept
        : context_(std::move(context)),
          ssl_(std::move(ssl)),
          generation_(generation),
          counters_(std::move(counters)) {}

    Result<TlsIoResult> Handshake() noexcept override {
        if (handshake_complete_) return TlsIoResult{};
        ERR_clear_error();
        const int result = SSL_do_handshake(ssl_.get());
        if (result != 1) {
            if (SSL_get_error(ssl_.get(), result) == SSL_ERROR_SSL &&
                SSL_get_verify_result(ssl_.get()) != X509_V_OK) {
                ERR_clear_error();
                RecordHandshakeFailure();
                return Denied("TLS peer certificate verification failed");
            }
            auto translated = TranslateIoResult(ssl_.get(), result, 0);
            if (!translated.ok()) RecordHandshakeFailure();
            return translated;
        }
        if (SSL_get_verify_result(ssl_.get()) != X509_V_OK) {
            RecordHandshakeFailure();
            return Denied("TLS peer certificate verification failed");
        }
        UniqueX509 peer_certificate(SSL_get1_peer_certificate(ssl_.get()));
        auto extracted = ExtractPeer(peer_certificate.get(), generation_);
        if (!extracted.ok()) {
            RecordHandshakeFailure();
            return extracted.status();
        }
        peer_ = *extracted;
        handshake_complete_ = true;
        return TlsIoResult{};
    }

    Result<TlsIoResult> Read(std::span<std::byte> output) noexcept override {
        if (!handshake_complete_) return Unavailable("TLS handshake is incomplete");
        if (output.empty()) return Invalid("TLS read buffer is empty");
        if (pending_operation_ == PendingOperation::kWrite) {
            return Unavailable("TLS write must complete before TLS read");
        }
        if (pending_operation_ == PendingOperation::kRead &&
            (output.data() != pending_read_data_ ||
             output.size() != pending_read_size_)) {
            return Invalid("TLS read retry arguments changed");
        }
        size_t bytes = 0;
        ERR_clear_error();
        const int result = SSL_read_ex(ssl_.get(), output.data(), output.size(),
                                       &bytes);
        auto translated = TranslateIoResult(ssl_.get(), result, bytes);
        if (!translated.ok()) return translated.status();
        if (translated->need != TlsIoNeed::kNone) {
            pending_operation_ = PendingOperation::kRead;
            pending_read_data_ = output.data();
            pending_read_size_ = output.size();
        } else {
            ClearPendingOperation();
        }
        return translated;
    }

    Result<TlsIoResult> Write(
        std::span<const std::byte> input) noexcept override {
        if (!handshake_complete_) return Unavailable("TLS handshake is incomplete");
        if (input.empty()) return Invalid("TLS write buffer is empty");
        if (pending_operation_ == PendingOperation::kRead) {
            return Unavailable("TLS read must complete before TLS write");
        }
        // SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER permits a different address on a
        // retry, but OpenSSL still requires the exact original byte count.
        if (pending_operation_ == PendingOperation::kWrite &&
            input.size() != pending_write_size_) {
            return Invalid("TLS write retry length changed");
        }
        size_t bytes = 0;
        ERR_clear_error();
        const int result = SSL_write_ex(ssl_.get(), input.data(), input.size(),
                                        &bytes);
        auto translated = TranslateIoResult(ssl_.get(), result, bytes);
        if (!translated.ok()) return translated.status();
        if (translated->need != TlsIoNeed::kNone) {
            pending_operation_ = PendingOperation::kWrite;
            pending_write_size_ = input.size();
        } else {
            ClearPendingOperation();
        }
        return translated;
    }

    bool handshake_complete() const noexcept override {
        return handshake_complete_;
    }

    bool has_buffered_read() const noexcept override {
        return handshake_complete_ && SSL_pending(ssl_.get()) > 0;
    }

    Result<AuthenticatedPeer> peer() const noexcept override {
        if (!handshake_complete_ || !peer_.complete()) {
            return Status::Error(StatusCode::kWouldBlock,
                                 "TLS peer is not authenticated yet");
        }
        return peer_;
    }

private:
    enum class PendingOperation : uint8_t { kNone, kRead, kWrite };

    void RecordHandshakeFailure() noexcept {
        if (handshake_failure_recorded_ || counters_ == nullptr) return;
        handshake_failure_recorded_ = true;
        counters_->handshake_failures.fetch_add(1, std::memory_order_relaxed);
    }

    void ClearPendingOperation() noexcept {
        pending_operation_ = PendingOperation::kNone;
        pending_read_data_ = nullptr;
        pending_read_size_ = 0;
        pending_write_size_ = 0;
    }

    SharedContext context_;
    UniqueSsl ssl_;
    uint64_t generation_ = 0;
    std::shared_ptr<TlsOperationalCounters> counters_;
    AuthenticatedPeer peer_;
    std::byte* pending_read_data_ = nullptr;
    size_t pending_read_size_ = 0;
    size_t pending_write_size_ = 0;
    PendingOperation pending_operation_ = PendingOperation::kNone;
    bool handshake_complete_ = false;
    bool handshake_failure_recorded_ = false;
};

class OpenSslTlsChannelFactory final : public TlsChannelFactory {
public:
    OpenSslTlsChannelFactory(
        std::shared_ptr<TlsCredentialProvider> provider, uint64_t generation,
        SharedContext context,
        std::shared_ptr<TlsOperationalCounters> counters) noexcept
        : provider_(std::move(provider)),
          cached_generation_(generation),
          cached_context_(std::move(context)),
          counters_(std::move(counters)) {}

    Status Prepare() override {
        try {
            MINO_ASSIGN_OR_RETURN(auto snapshot, provider_->Snapshot());
            if (snapshot.generation == 0 || !snapshot.credentials) {
                return Unavailable(
                    "TLS credential provider returned no credentials");
            }
            MINO_ASSIGN_OR_RETURN(auto context, ContextFor(snapshot));
            (void)context;
            return Status::Ok();
        } catch (const std::bad_alloc&) {
            return AllocationFailure();
        }
    }

    Result<std::unique_ptr<TlsChannel>> Create(int socket_fd,
                                               TlsRole role) override {
        try {
            if (socket_fd < 0) return Invalid("TLS socket fd is invalid");
            MINO_ASSIGN_OR_RETURN(auto snapshot, provider_->Snapshot());
            if (snapshot.generation == 0 || !snapshot.credentials) {
                return Unavailable("TLS credential provider returned no credentials");
            }
            MINO_ASSIGN_OR_RETURN(auto context, ContextFor(snapshot));
            UniqueSsl ssl(SSL_new(context.get()));
            if (!ssl) return AllocationFailure();
            if (SSL_set_fd(ssl.get(), socket_fd) != 1) {
                return Internal("TLS socket binding failed");
            }
            if (role == TlsRole::kClient) {
                SSL_set_connect_state(ssl.get());
            } else {
                SSL_set_accept_state(ssl.get());
            }
            return std::unique_ptr<TlsChannel>(new OpenSslTlsChannel(
                std::move(context), std::move(ssl), snapshot.generation,
                counters_));
        } catch (const std::bad_alloc&) {
            return AllocationFailure();
        }
    }

    TlsOperationalStats OperationalStats() const noexcept override {
        return TlsOperationalStats{
            .handshake_failures =
                counters_->handshake_failures.load(std::memory_order_relaxed),
            .certificate_expiry_unix_seconds =
                counters_->certificate_expiry_unix_seconds.load(
                    std::memory_order_relaxed),
        };
    }

private:
    Result<SharedContext> ContextFor(
        const TlsCredentialSnapshot& snapshot) {
        {
            std::lock_guard lock(cache_mutex_);
            if (cached_generation_ == snapshot.generation && cached_context_) {
                return cached_context_;
            }
        }

        // PEM parsing and key validation happen without the cache lock. The
        // resulting SSL_CTX is immutable and safely shared by every connection
        // pinned to this credential generation.
        MINO_ASSIGN_OR_RETURN(
            auto built,
            BuildContext(*snapshot.credentials, snapshot.generation));
        SharedContext context(built.release(), ContextDeleter{});
        {
            std::lock_guard lock(cache_mutex_);
            if (!cached_context_ || snapshot.generation > cached_generation_) {
                cached_generation_ = snapshot.generation;
                cached_context_ = context;
            } else if (snapshot.generation == cached_generation_) {
                context = cached_context_;
            }
        }
        return context;
    }

    std::shared_ptr<TlsCredentialProvider> provider_;
    std::mutex cache_mutex_;
    uint64_t cached_generation_ = 0;
    SharedContext cached_context_;
    std::shared_ptr<TlsOperationalCounters> counters_;
};

}  // namespace

Result<std::string> PrincipalUri(NodeId node_id,
                                 SecurityDomainId security_domain) {
    try {
        if (node_id.value == 0 || security_domain.value == 0) {
            return Invalid("TLS principal node or security domain is zero");
        }
        return std::string(kPrincipalPrefix) +
               std::to_string(security_domain.value) +
               std::string(kNodeSeparator) + std::to_string(node_id.value);
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Result<AuthenticatedPeer> ParsePrincipalUri(
    std::string_view uri, uint64_t credential_generation,
    std::span<const std::byte, kCertificateFingerprintBytes> fingerprint) {
    if (credential_generation == 0 || !uri.starts_with(kPrincipalPrefix)) {
        return Denied("TLS principal URI is invalid");
    }
    uri.remove_prefix(kPrincipalPrefix.size());
    const size_t separator = uri.find(kNodeSeparator);
    if (separator == std::string_view::npos ||
        uri.find(kNodeSeparator, separator + 1) != std::string_view::npos) {
        return Denied("TLS principal URI is non-canonical");
    }
    uint64_t domain = 0;
    uint64_t node = 0;
    if (!ParseDecimal(uri.substr(0, separator), &domain) ||
        !ParseDecimal(uri.substr(separator + kNodeSeparator.size()), &node)) {
        return Denied("TLS principal URI identifiers are invalid");
    }
    AuthenticatedPeer result{
        .node_id = NodeId{node},
        .security_domain = SecurityDomainId{domain},
        .credential_generation = credential_generation,
    };
    std::copy(fingerprint.begin(), fingerprint.end(),
              result.certificate_sha256.begin());
    return result;
}

Result<SecretPem> SecretPem::FromBytes(std::span<const std::byte> bytes) {
    try {
        if (bytes.empty() || bytes.size() >
                                 static_cast<size_t>(std::numeric_limits<int>::max())) {
            return Invalid("secret PEM size is invalid");
        }
        return SecretPem(std::vector<std::byte>(bytes.begin(), bytes.end()));
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Result<SecretPem> SecretPem::FromString(std::string_view pem) {
    return FromBytes(std::as_bytes(std::span(pem.data(), pem.size())));
}

SecretPem::SecretPem(SecretPem&& other) noexcept
    : bytes_(std::move(other.bytes_)) {
    other.Clear();
}

SecretPem& SecretPem::operator=(SecretPem&& other) noexcept {
    if (this != &other) {
        Clear();
        bytes_ = std::move(other.bytes_);
        other.Clear();
    }
    return *this;
}

SecretPem::~SecretPem() { Clear(); }

void SecretPem::Clear() noexcept {
    volatile std::byte* data = bytes_.data();
    for (size_t index = 0; index < bytes_.size(); ++index) {
        data[index] = std::byte{0};
    }
    bytes_.clear();
}

StaticTlsCredentialProvider::StaticTlsCredentialProvider(
    std::shared_ptr<const TlsCredentials> credentials) noexcept
    : credentials_(std::move(credentials)) {}

Result<std::shared_ptr<StaticTlsCredentialProvider>>
StaticTlsCredentialProvider::Create(TlsCredentials credentials) {
    try {
        MINO_RETURN_IF_ERROR(ValidateCredentials(credentials));
        auto immutable =
            std::make_shared<const TlsCredentials>(std::move(credentials));
        return std::shared_ptr<StaticTlsCredentialProvider>(
            new StaticTlsCredentialProvider(std::move(immutable)));
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Result<TlsCredentialSnapshot> StaticTlsCredentialProvider::Snapshot() {
    std::lock_guard lock(mutex_);
    if (!credentials_ || generation_ == 0) {
        return Unavailable("TLS credentials are unavailable");
    }
    return TlsCredentialSnapshot{
        .generation = generation_,
        .credentials = credentials_,
    };
}

Status StaticTlsCredentialProvider::Rotate(TlsCredentials credentials) {
    try {
        MINO_RETURN_IF_ERROR(ValidateCredentials(credentials));
        auto immutable =
            std::make_shared<const TlsCredentials>(std::move(credentials));
        std::lock_guard lock(mutex_);
        if (generation_ == std::numeric_limits<uint64_t>::max()) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "TLS credential generation exhausted");
        }
        credentials_ = std::move(immutable);
        ++generation_;
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Result<std::shared_ptr<TlsChannelFactory>> CreateOpenSslTlsChannelFactory(
    std::shared_ptr<TlsCredentialProvider> provider) {
    try {
        if (!provider) return Invalid("TLS credential provider is null");
        MINO_ASSIGN_OR_RETURN(auto snapshot, provider->Snapshot());
        if (snapshot.generation == 0 || !snapshot.credentials) {
            return Unavailable("TLS credential provider returned no credentials");
        }
        MINO_ASSIGN_OR_RETURN(
            auto built,
            BuildContext(*snapshot.credentials, snapshot.generation));
        SharedContext context(built.release(), ContextDeleter{});
        auto counters = std::make_shared<TlsOperationalCounters>();
        return std::shared_ptr<TlsChannelFactory>(new OpenSslTlsChannelFactory(
            std::move(provider), snapshot.generation, std::move(context),
            std::move(counters)));
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

}  // namespace mino::security
