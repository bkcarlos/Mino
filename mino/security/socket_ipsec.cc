// Copyright 2026 The Mino Authors
#include "mino/security/socket_ipsec.h"
#include <initializer_list>

#if defined(__linux__)
#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <linux/xfrm.h>
#include <netinet/in.h>
#include <sys/socket.h>
#endif

namespace mino::security {
Status InstallSocketIpsecPolicy(int fd, int family,
                                const SocketIpsecPolicy& policy) {
#if defined(__linux__)
    if (fd < 0 || (family != AF_INET && family != AF_INET6)) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "IPsec socket requires IPv4 or IPv6");
    }
    if (family == AF_INET6) {
        const int enabled = 1;
        if (::setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &enabled, sizeof(enabled)) != 0) {
            return Status::Error(StatusCode::kUnavailable, "cannot enforce IPv6-only IPsec socket");
        }
    }
    for (const bool inbound : {true, false}) {
        struct {
            xfrm_userpolicy_info info;
            xfrm_user_tmpl tmpl;
        } request{};
        static_assert(sizeof(request) == sizeof(request.info) + sizeof(request.tmpl));
        request.info.sel.family = family;
        request.info.dir = inbound ? XFRM_POLICY_IN : XFRM_POLICY_OUT;
        request.info.action = XFRM_POLICY_ALLOW;
        request.info.share = XFRM_SHARE_ANY;
        request.info.lft.soft_byte_limit = XFRM_INF;
        request.info.lft.hard_byte_limit = XFRM_INF;
        request.info.lft.soft_packet_limit = XFRM_INF;
        request.info.lft.hard_packet_limit = XFRM_INF;
        request.tmpl.family = family;
        request.tmpl.id.proto = IPPROTO_ESP;
        request.tmpl.id.spi = htonl(inbound ? policy.inbound_spi : policy.outbound_spi);
        request.tmpl.reqid = inbound ? policy.inbound_reqid : policy.outbound_reqid;
        request.tmpl.mode = XFRM_MODE_TRANSPORT;
        request.tmpl.share = XFRM_SHARE_ANY;
        request.tmpl.optional = 0;
        request.tmpl.aalgos = ~uint32_t{0};
        request.tmpl.ealgos = ~uint32_t{0};
        request.tmpl.calgos = ~uint32_t{0};
        const int level = family == AF_INET ? IPPROTO_IP : IPPROTO_IPV6;
        const int option = family == AF_INET ? IP_XFRM_POLICY : IPV6_XFRM_POLICY;
        if (::setsockopt(fd, level, option, &request, sizeof(request)) != 0) {
            return Status::Error(errno == EPERM || errno == EACCES ?
                StatusCode::kPermissionDenied : StatusCode::kUnavailable,
                "cannot install mandatory socket IPsec policy");
        }
    }
    return Status::Ok();
#else
    (void)fd;
    (void)family;
    (void)policy;
    return Status::Error(StatusCode::kUnsupported, "socket IPsec requires Linux XFRM");
#endif
}
}  // namespace mino::security
