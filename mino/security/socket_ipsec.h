// Copyright 2026 The Mino Authors
#ifndef MINO_SECURITY_SOCKET_IPSEC_H_
#define MINO_SECURITY_SOCKET_IPSEC_H_

#include <cstdint>
#include "mino/common/status.h"

namespace mino::security {

// Linux transport-mode ESP requirements bound to a socket's lifetime. reqid
// must match the key manager's SA configuration (zero is an exact outbound
// reqid, not a wildcard). Optional SPIs are host order; zero permits rekeying.
// The key manager must only install encrypting ESP SAs for these reqids. Linux
// socket policy algorithm masks do not enforce a non-null encryption cipher.
struct SocketIpsecPolicy {
    uint32_t outbound_reqid = 0;
    uint32_t inbound_reqid = 0;
    uint32_t outbound_spi = 0;
    uint32_t inbound_spi = 0;
};

// Apply IN and OUT mandatory policies before bind/connect/listen. Close fd on
// ANY error, since one direction may already be installed. IPv6 sockets are
// made IPV6_V6ONLY so mapped IPv4 cannot miss the IPv6 policy selector.
// Requires Linux CAP_NET_ADMIN; never silently falls back on unsupported OSes.
Status InstallSocketIpsecPolicy(int fd, int family,
                                const SocketIpsecPolicy& policy);

}  // namespace mino::security
#endif
