// Copyright 2026 The Mino Authors

#ifndef MINO_SECURITY_TEST_TLS_CREDENTIALS_H_
#define MINO_SECURITY_TEST_TLS_CREDENTIALS_H_

#include <span>
#include <vector>

#include "mino/common/result.h"
#include "mino/security/tls.h"

namespace mino::security::testing {

struct TestPrincipal {
    NodeId node_id;
    SecurityDomainId security_domain;
};

// Generates one ephemeral CA and one TLS 1.3 client/server-capable leaf per
// principal. Private keys remain only in SecretPem returned to the test.
Result<std::vector<TlsCredentials>> GenerateTlsCredentials(
    std::span<const TestPrincipal> principals);
Result<std::vector<TlsCredentials>> GenerateTlsCredentialsWithoutSan(
    std::span<const TestPrincipal> principals);

}  // namespace mino::security::testing

#endif  // MINO_SECURITY_TEST_TLS_CREDENTIALS_H_
