// Copyright 2026 The Mino Authors
// Internal Linux XFRM policy parser/evaluator, also used by unprivileged tests.
#ifndef MINO_SECURITY_IPSEC_POLICY_H_
#define MINO_SECURITY_IPSEC_POLICY_H_

#include <linux/xfrm.h>

#include "mino/security/ipsec.h"

namespace mino::security::internal {

struct XfrmPolicy {
    xfrm_userpolicy_info info{};
    std::vector<xfrm_user_tmpl> templates;
    bool unsupported_scope = false;
};

Result<XfrmPolicy> ParseXfrmPolicy(std::span<const std::byte> payload);
Status RequireXfrmPolicies(const IpsecSaSelector& selector,
                          std::span<const XfrmPolicy> policies,
                          std::span<const IpsecSaInfo> sas);

}  // namespace mino::security::internal
#endif
