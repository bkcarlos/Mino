// Copyright 2026 The Mino Authors
#include "mino/security/ipsec_policy.h"

#include <arpa/inet.h>
#include <linux/netlink.h>

#include <algorithm>
#include <cstring>
#include <limits>

namespace mino::security::internal {
namespace {

bool PrefixMatches(const IpsecEndpoint& endpoint, const xfrm_address_t& address,
                   uint8_t prefix) {
    if (prefix > endpoint.address_bytes * 8) return false;
    const auto* bytes = reinterpret_cast<const unsigned char*>(&address);
    for (uint8_t bit = 0; bit < prefix; ++bit) {
        const unsigned mask = 1u << (7 - bit % 8);
        if ((std::to_integer<unsigned>(endpoint.address[bit / 8]) & mask) !=
            (bytes[bit / 8] & mask)) return false;
    }
    return true;
}

bool PortMatches(uint16_t port, uint16_t value, uint16_t mask) {
    return (htons(port) & mask) == (value & mask);
}

bool Matches(const xfrm_selector& rule, const IpsecSaSelector& flow,
             bool inbound) {
    const auto family = flow.peer.family == IpsecAddressFamily::kIpv4
                            ? AF_INET : AF_INET6;
    if (rule.family != family) return false;
    if (rule.proto != 0 && rule.proto != static_cast<uint8_t>(flow.peer.protocol))
        return false;
    const auto& peer_address = inbound ? rule.saddr : rule.daddr;
    const auto peer_prefix = inbound ? rule.prefixlen_s : rule.prefixlen_d;
    const auto peer_port = inbound ? rule.sport : rule.dport;
    const auto peer_mask = inbound ? rule.sport_mask : rule.dport_mask;
    if (!PrefixMatches(flow.peer, peer_address, peer_prefix) ||
        !PortMatches(flow.peer.port, peer_port, peer_mask)) return false;
    // Without a known source, consider every potentially matching policy.
    // RequireDirection below rejects constrained local selectors; this avoids
    // choosing a wildcard policy while a more specific bypass might apply.
    if (!flow.local.has_value()) return true;
    const auto& local_address = inbound ? rule.daddr : rule.saddr;
    const auto local_prefix = inbound ? rule.prefixlen_d : rule.prefixlen_s;
    const auto local_port = inbound ? rule.dport : rule.sport;
    const auto local_mask = inbound ? rule.dport_mask : rule.sport_mask;
    return flow.local->family == flow.peer.family &&
        PrefixMatches(*flow.local, local_address, local_prefix) &&
        (flow.local->port == 0 ||
         PortMatches(flow.local->port, local_port, local_mask));
}

bool BoundToEncryptingSa(const xfrm_user_tmpl& tmpl,
                        std::span<const IpsecSaInfo> sas) {
    // This verifier supports transport mode only. Tunnel/route-based policy
    // requires route/mark/interface context that is not in IpsecSaSelector.
    if (tmpl.optional || tmpl.id.proto != IPPROTO_ESP ||
        tmpl.mode != XFRM_MODE_TRANSPORT) return false;
    for (const auto& sa : sas) {
        const auto family = sa.family == IpsecAddressFamily::kIpv4
                                ? AF_INET : AF_INET6;
        if (tmpl.family != family || sa.proto != IPPROTO_ESP || !sa.encrypts ||
            sa.mode != tmpl.mode || sa.reqid != tmpl.reqid ||
            (tmpl.id.spi != 0 && ntohl(tmpl.id.spi) != sa.spi)) continue;
        if (std::memcmp(sa.daddr.data(), &tmpl.id.daddr, sa.address_bytes) == 0 &&
            std::memcmp(sa.saddr.data(), &tmpl.saddr, sa.address_bytes) == 0) {
            return true;
        }
    }
    return false;
}

Status RequireDirection(const IpsecSaSelector& flow,
                        std::span<const XfrmPolicy> policies,
                        std::span<const IpsecSaInfo> sas, bool inbound) {
    const uint8_t direction = inbound ? XFRM_POLICY_IN : XFRM_POLICY_OUT;
    uint32_t priority = std::numeric_limits<uint32_t>::max();
    bool found = false;
    for (const auto& policy : policies) {
        if (policy.info.dir != direction || !Matches(policy.info.sel, flow, inbound))
            continue;
        const auto& sel = policy.info.sel;
        const uint8_t local_prefix = inbound ? sel.prefixlen_d : sel.prefixlen_s;
        const uint16_t local_mask = inbound ? sel.dport_mask : sel.sport_mask;
        if ((!flow.local.has_value() && (local_prefix != 0 || local_mask != 0)) ||
            (flow.local.has_value() && flow.local->port == 0 && local_mask != 0)) {
            return Status::Error(StatusCode::kUnavailable,
                                 "IPsec policy requires a concrete local address/port");
        }
        priority = std::min(priority, policy.info.priority);
        found = true;
    }
    if (!found) return Status::Error(StatusCode::kUnavailable,
                                    "no covering IPsec policy");
    // Fail closed on priority ties unless every eligible policy protects.
    for (const auto& policy : policies) {
        if (policy.info.dir != direction || policy.info.priority != priority ||
            !Matches(policy.info.sel, flow, inbound)) continue;
        if (policy.unsupported_scope || policy.info.sel.ifindex != 0 ||
            policy.info.sel.user != 0) {
            return Status::Error(StatusCode::kUnavailable,
                                 "IPsec policy scope cannot be verified");
        }
        if (policy.info.action != XFRM_POLICY_ALLOW || policy.templates.size() != 1) {
            return Status::Error(StatusCode::kUnavailable,
                                 "effective IPsec policy is not verified mandatory ESP");
        }
        if (!BoundToEncryptingSa(policy.templates.front(), sas)) {
            return Status::Error(StatusCode::kUnavailable,
                                 "mandatory IPsec template has no matching encrypting SA");
        }
        const auto& tmpl = policy.templates.front();
        if (flow.socket_policy) {
            auto socket_tmpl = tmpl;
            socket_tmpl.reqid = inbound ? flow.socket_policy->inbound_reqid :
                                         flow.socket_policy->outbound_reqid;
            socket_tmpl.id.spi = htonl(inbound ? flow.socket_policy->inbound_spi :
                                               flow.socket_policy->outbound_spi);
            if (!BoundToEncryptingSa(socket_tmpl, sas)) {
                return Status::Error(StatusCode::kUnavailable,
                    "socket IPsec template has no matching encrypting SA");
            }
        }
        if (!PrefixMatches(flow.peer, inbound ? tmpl.saddr : tmpl.id.daddr,
                           flow.peer.address_bytes * 8) ||
            (flow.local.has_value() &&
             !PrefixMatches(*flow.local, inbound ? tmpl.id.daddr : tmpl.saddr,
                            flow.local->address_bytes * 8))) {
            return Status::Error(StatusCode::kUnavailable,
                                 "IPsec template endpoints do not match flow");
        }
    }
    return Status::Ok();
}

}  // namespace

Result<XfrmPolicy> ParseXfrmPolicy(std::span<const std::byte> payload) {
    if (payload.size() < sizeof(xfrm_userpolicy_info)) {
        return Status::Error(StatusCode::kCorruption, "short XFRM policy");
    }
    XfrmPolicy result;
    std::memcpy(&result.info, payload.data(), sizeof(result.info));
    size_t offset = NLA_ALIGN(sizeof(result.info));
    bool have_templates = false;
    while (offset < payload.size()) {
        if (payload.size() - offset < sizeof(nlattr)) {
            return Status::Error(StatusCode::kCorruption, "short XFRM attribute");
        }
        nlattr attr{};
        std::memcpy(&attr, payload.data() + offset, sizeof(attr));
        if (attr.nla_len < sizeof(attr) || attr.nla_len > payload.size() - offset) {
            return Status::Error(StatusCode::kCorruption, "invalid XFRM attribute");
        }
        const size_t length = attr.nla_len - sizeof(attr);
        const auto* data = payload.data() + offset + sizeof(attr);
        if (attr.nla_type == XFRMA_TMPL) {
            if (have_templates || length == 0 || length % sizeof(xfrm_user_tmpl)) {
                return Status::Error(StatusCode::kCorruption, "invalid XFRM templates");
            }
            have_templates = true;
            result.templates.resize(length / sizeof(xfrm_user_tmpl));
            std::memcpy(result.templates.data(), data, length);
        } else if (attr.nla_type == XFRMA_MARK) {
            if (length != sizeof(xfrm_mark)) {
                return Status::Error(StatusCode::kCorruption, "invalid XFRM mark");
            }
            xfrm_mark mark{};
            std::memcpy(&mark, data, sizeof(mark));
            result.unsupported_scope |= mark.m != 0;
        } else if (attr.nla_type == XFRMA_POLICY_TYPE) {
            if (length != sizeof(xfrm_userpolicy_type)) {
                return Status::Error(StatusCode::kCorruption, "invalid XFRM policy type");
            }
            xfrm_userpolicy_type type{};
            std::memcpy(&type, data, sizeof(type));
            result.unsupported_scope |= type.type != XFRM_POLICY_TYPE_MAIN;
        } else if (attr.nla_type == XFRMA_IF_ID || attr.nla_type == XFRMA_SEC_CTX) {
            result.unsupported_scope = true;
        }
        offset += NLA_ALIGN(attr.nla_len);
        if (offset > payload.size()) {
            return Status::Error(StatusCode::kCorruption, "truncated XFRM padding");
        }
    }
    return result;
}

Status RequireXfrmPolicies(const IpsecSaSelector& selector,
                          std::span<const XfrmPolicy> policies,
                          std::span<const IpsecSaInfo> sas) {
    const auto valid = [](const IpsecEndpoint& endpoint) {
        return (endpoint.family == IpsecAddressFamily::kIpv4 && endpoint.address_bytes == 4) ||
               (endpoint.family == IpsecAddressFamily::kIpv6 && endpoint.address_bytes == 16);
    };
    if (!valid(selector.peer) || (selector.local && !valid(*selector.local))) {
        return Status::Error(StatusCode::kInvalidArgument, "invalid IPsec flow address");
    }
    MINO_RETURN_IF_ERROR(RequireDirection(selector, policies, sas, false));
    if (selector.require_inbound) {
        if (!selector.local.has_value()) {
            return Status::Error(StatusCode::kUnavailable, "inbound IPsec needs local endpoint");
        }
        MINO_RETURN_IF_ERROR(RequireDirection(selector, policies, sas, true));
    }
    return Status::Ok();
}

}  // namespace mino::security::internal
