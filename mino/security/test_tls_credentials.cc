// Copyright 2026 The Mino Authors

#include "mino/security/test_tls_credentials.h"

#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/x509v3.h>

#include <memory>
#include <new>
#include <string>

namespace mino::security::testing {
namespace {

Status Internal(const char* message) {
    return Status::Error(StatusCode::kInternal, message);
}
Status Invalid(const char* message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}
Status AllocationFailure() {
    return Status::Error(StatusCode::kResourceExhausted,
                         "test TLS certificate allocation failed");
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
struct PkeyContextDeleter {
    void operator()(EVP_PKEY_CTX* value) const noexcept {
        EVP_PKEY_CTX_free(value);
    }
};
struct ExtensionDeleter {
    void operator()(X509_EXTENSION* value) const noexcept {
        X509_EXTENSION_free(value);
    }
};

using UniqueBio = std::unique_ptr<BIO, BioDeleter>;
using UniqueX509 = std::unique_ptr<X509, X509Deleter>;
using UniquePkey = std::unique_ptr<EVP_PKEY, PkeyDeleter>;
using UniquePkeyContext = std::unique_ptr<EVP_PKEY_CTX, PkeyContextDeleter>;
using UniqueExtension = std::unique_ptr<X509_EXTENSION, ExtensionDeleter>;

Result<UniquePkey> GenerateKey() {
    UniquePkeyContext context(EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr));
    if (!context || EVP_PKEY_keygen_init(context.get()) <= 0 ||
        EVP_PKEY_CTX_set_rsa_keygen_bits(context.get(), 2048) <= 0) {
        return Internal("test TLS RSA setup failed");
    }
    EVP_PKEY* key = nullptr;
    if (EVP_PKEY_keygen(context.get(), &key) <= 0 || key == nullptr) {
        return Internal("test TLS RSA generation failed");
    }
    return UniquePkey(key);
}

Status AddName(X509_NAME* name, const char* common_name) {
    if (name == nullptr ||
        X509_NAME_add_entry_by_txt(
            name, "CN", MBSTRING_ASC,
            reinterpret_cast<const unsigned char*>(common_name), -1, -1, 0) !=
            1) {
        return Internal("test TLS certificate name failed");
    }
    return Status::Ok();
}

Status AddExtension(X509* certificate, X509* issuer, int nid,
                    const std::string& value) {
    X509V3_CTX context{};
    X509V3_set_ctx(&context, issuer, certificate, nullptr, nullptr, 0);
    UniqueExtension extension(
        X509V3_EXT_conf_nid(nullptr, &context, nid, value.c_str()));
    if (!extension || X509_add_ext(certificate, extension.get(), -1) != 1) {
        return Internal("test TLS certificate extension failed");
    }
    return Status::Ok();
}

Result<UniqueX509> GenerateCertificate(EVP_PKEY* subject_key,
                                       X509* issuer_certificate,
                                       EVP_PKEY* issuer_key,
                                       long serial,
                                       const std::string& principal_uri,
                                       bool include_san = true) {
    UniqueX509 certificate(X509_new());
    if (!certificate || X509_set_version(certificate.get(), 2) != 1 ||
        ASN1_INTEGER_set(X509_get_serialNumber(certificate.get()), serial) != 1 ||
        X509_gmtime_adj(X509_getm_notBefore(certificate.get()), -60) == nullptr ||
        X509_gmtime_adj(X509_getm_notAfter(certificate.get()), 3600) == nullptr ||
        X509_set_pubkey(certificate.get(), subject_key) != 1) {
        return Internal("test TLS certificate fields failed");
    }
    X509_NAME* subject = X509_get_subject_name(certificate.get());
    MINO_RETURN_IF_ERROR(AddName(subject, principal_uri.empty() ? "Mino Test CA"
                                                               : "Mino Test Node"));
    if (issuer_certificate == nullptr) {
        if (X509_set_issuer_name(certificate.get(), subject) != 1) {
            return Internal("test TLS CA issuer failed");
        }
        MINO_RETURN_IF_ERROR(AddExtension(certificate.get(), certificate.get(),
                                          NID_basic_constraints,
                                          "critical,CA:TRUE"));
        MINO_RETURN_IF_ERROR(AddExtension(certificate.get(), certificate.get(),
                                          NID_key_usage,
                                          "critical,keyCertSign,cRLSign"));
    } else {
        if (X509_set_issuer_name(certificate.get(),
                                 X509_get_subject_name(issuer_certificate)) != 1) {
            return Internal("test TLS leaf issuer failed");
        }
        MINO_RETURN_IF_ERROR(AddExtension(certificate.get(), issuer_certificate,
                                          NID_basic_constraints,
                                          "critical,CA:FALSE"));
        MINO_RETURN_IF_ERROR(AddExtension(certificate.get(), issuer_certificate,
                                          NID_key_usage,
                                          "critical,digitalSignature,keyEncipherment"));
        MINO_RETURN_IF_ERROR(AddExtension(certificate.get(), issuer_certificate,
                                          NID_ext_key_usage,
                                          "serverAuth,clientAuth"));
        if (include_san) {
            MINO_RETURN_IF_ERROR(AddExtension(
                certificate.get(), issuer_certificate, NID_subject_alt_name,
                "URI:" + principal_uri));
        }
    }
    if (X509_sign(certificate.get(), issuer_key, EVP_sha256()) <= 0) {
        return Internal("test TLS certificate signing failed");
    }
    return certificate;
}

Result<std::string> CertificatePem(X509* certificate) {
    UniqueBio bio(BIO_new(BIO_s_mem()));
    if (!bio || PEM_write_bio_X509(bio.get(), certificate) != 1) {
        return Internal("test TLS certificate PEM failed");
    }
    const char* bytes = nullptr;
    const long size = BIO_get_mem_data(bio.get(), &bytes);
    if (bytes == nullptr || size <= 0) {
        return Internal("test TLS certificate PEM is empty");
    }
    return std::string(bytes, static_cast<size_t>(size));
}

Result<std::string> PrivateKeyPem(EVP_PKEY* key) {
    UniqueBio bio(BIO_new(BIO_s_mem()));
    if (!bio || PEM_write_bio_PrivateKey(bio.get(), key, nullptr, nullptr, 0,
                                         nullptr, nullptr) != 1) {
        return Internal("test TLS private key PEM failed");
    }
    const char* bytes = nullptr;
    const long size = BIO_get_mem_data(bio.get(), &bytes);
    if (bytes == nullptr || size <= 0) {
        return Internal("test TLS private key PEM is empty");
    }
    return std::string(bytes, static_cast<size_t>(size));
}

}  // namespace

Result<std::vector<TlsCredentials>> GenerateTlsCredentialsImpl(
    std::span<const TestPrincipal> principals, bool include_san) {
    try {
        if (principals.empty()) return Invalid("test TLS principals are empty");
        MINO_ASSIGN_OR_RETURN(auto ca_key, GenerateKey());
        MINO_ASSIGN_OR_RETURN(
            auto ca_certificate,
            GenerateCertificate(ca_key.get(), nullptr, ca_key.get(), 1, ""));
        MINO_ASSIGN_OR_RETURN(const std::string ca_pem,
                              CertificatePem(ca_certificate.get()));

        std::vector<TlsCredentials> credentials;
        credentials.reserve(principals.size());
        long serial = 2;
        for (const TestPrincipal& principal : principals) {
            MINO_ASSIGN_OR_RETURN(
                const std::string uri,
                PrincipalUri(principal.node_id, principal.security_domain));
            MINO_ASSIGN_OR_RETURN(auto key, GenerateKey());
            MINO_ASSIGN_OR_RETURN(
                auto certificate,
                GenerateCertificate(key.get(), ca_certificate.get(), ca_key.get(),
                                    serial++, uri, include_san));
            MINO_ASSIGN_OR_RETURN(const std::string certificate_pem,
                                  CertificatePem(certificate.get()));
            MINO_ASSIGN_OR_RETURN(const std::string private_key_pem,
                                  PrivateKeyPem(key.get()));
            MINO_ASSIGN_OR_RETURN(auto secret,
                                  SecretPem::FromString(private_key_pem));
            credentials.push_back(TlsCredentials{
                .local_identity = AuthenticatedPeer{
                    .node_id = principal.node_id,
                    .security_domain = principal.security_domain,
                },
                .certificate_chain_pem = certificate_pem,
                .private_key_pem = std::move(secret),
                .trust_anchors_pem = ca_pem,
            });
        }
        return credentials;
    } catch (const std::bad_alloc&) {
        return AllocationFailure();
    }
}

Result<std::vector<TlsCredentials>> GenerateTlsCredentials(
    std::span<const TestPrincipal> principals) {
    return GenerateTlsCredentialsImpl(principals, true);
}

Result<std::vector<TlsCredentials>> GenerateTlsCredentialsWithoutSan(
    std::span<const TestPrincipal> principals) {
    return GenerateTlsCredentialsImpl(principals, false);
}

}  // namespace mino::security::testing
