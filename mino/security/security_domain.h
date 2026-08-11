// Copyright 2026 The Mino Authors

#ifndef MINO_SECURITY_SECURITY_DOMAIN_H_
#define MINO_SECURITY_SECURITY_DOMAIN_H_

#include <compare>
#include <cstdint>

namespace mino {

// Stable numeric boundary shared by TLS principals, Registry identities, Topic
// ACL subjects, and shared-memory Regions. Zero is always unspecified/invalid.
struct SecurityDomainId {
    uint64_t value = 0;
    friend constexpr auto operator<=>(SecurityDomainId, SecurityDomainId) =
        default;
};

namespace security {
using ::mino::SecurityDomainId;
}  // namespace security

}  // namespace mino

#endif  // MINO_SECURITY_SECURITY_DOMAIN_H_
