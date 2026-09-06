// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/security/ipsec.h"

#include <cerrno>
#include <cstring>
#include <string>
#include <string_view>
#include <utility>

#if defined(__linux__)
#include <arpa/inet.h>
#include <linux/netlink.h>
#include <linux/xfrm.h>
#include <netinet/in.h>
#include <sys/random.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstdlib>
#include <ctime>
#endif

namespace mino::security {
namespace {

bool SameAddress(const IpsecEndpoint& a, const std::byte* addr,
                 uint8_t addr_bytes) {
    if (a.address_bytes != addr_bytes) return false;
    return std::memcmp(a.address.data(), addr, addr_bytes) == 0;
}

bool EndpointMatchesPeer(const IpsecEndpoint& peer, const IpsecSaInfo& sa) {
    if (peer.family != sa.family) return false;
    return SameAddress(peer, sa.daddr.data(), sa.address_bytes);
}

#if defined(__linux__)

class ScopedFd final {
public:
    explicit ScopedFd(int fd = -1) noexcept : fd_(fd) {}
    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;
    ScopedFd(ScopedFd&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
    ScopedFd& operator=(ScopedFd&& other) noexcept {
        if (this != &other) {
            Reset();
            fd_ = other.fd_;
            other.fd_ = -1;
        }
        return *this;
    }
    ~ScopedFd() { Reset(); }
    int get() const noexcept { return fd_; }
    int Release() noexcept {
        const int fd = fd_;
        fd_ = -1;
        return fd;
    }
    void Reset() noexcept {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }

private:
    int fd_;
};

Status NetlinkErrno(std::string_view what) {
    const int err = errno;
    if (err == EPERM || err == EACCES) {
        return Status::Error(StatusCode::kPermissionDenied,
                             std::string(what) + ": permission denied");
    }
    return Status::Error(StatusCode::kUnavailable,
                         std::string(what) + ": " + std::strerror(err));
}

Result<ScopedFd> OpenXfrmSocket() {
    ScopedFd fd(::socket(AF_NETLINK, SOCK_RAW | SOCK_CLOEXEC, NETLINK_XFRM));
    if (fd.get() < 0) {
        return NetlinkErrno("NETLINK_XFRM socket");
    }
    sockaddr_nl local{};
    local.nl_family = AF_NETLINK;
    if (::bind(fd.get(), reinterpret_cast<sockaddr*>(&local), sizeof(local)) !=
        0) {
        return NetlinkErrno("bind NETLINK_XFRM");
    }
    return fd;
}

uint32_t NextNlSeq() {
    static uint32_t seq = 1;
    return seq++;
}

Status SendNetlinkDump(int fd, uint16_t nlmsg_type) {
    struct {
        nlmsghdr nlh;
        char pad[4]{};
    } req{};
    req.nlh.nlmsg_len = NLMSG_LENGTH(0);
    req.nlh.nlmsg_type = nlmsg_type;
    req.nlh.nlmsg_flags = NLM_F_REQUEST | NLM_F_DUMP;
    req.nlh.nlmsg_seq = NextNlSeq();
    req.nlh.nlmsg_pid = 0;

    sockaddr_nl kernel{};
    kernel.nl_family = AF_NETLINK;
    const ssize_t sent =
        ::sendto(fd, &req, req.nlh.nlmsg_len, 0,
                 reinterpret_cast<sockaddr*>(&kernel), sizeof(kernel));
    if (sent < 0) {
        return NetlinkErrno("netlink dump request");
    }
    return Status::Ok();
}

Result<std::vector<IpsecSaInfo>> DumpSecurityAssociations(int fd) {
    MINO_RETURN_IF_ERROR(SendNetlinkDump(fd, XFRM_MSG_GETSA));

    std::vector<IpsecSaInfo> out;
    std::array<std::byte, 8192> buffer{};
    for (;;) {
        const ssize_t n = ::recv(fd, buffer.data(), buffer.size(), 0);
        if (n < 0) {
            return NetlinkErrno("netlink dump recv");
        }
        if (n == 0) break;

        const std::byte* p = buffer.data();
        const std::byte* end = buffer.data() + n;
        bool done = false;
        while (p + sizeof(nlmsghdr) <= end) {
            const auto* nlh = reinterpret_cast<const nlmsghdr*>(p);
            if (nlh->nlmsg_len < sizeof(nlmsghdr) ||
                p + nlh->nlmsg_len > end) {
                return Status::Error(StatusCode::kCorruption,
                                     "truncated XFRM netlink message");
            }
            if (nlh->nlmsg_type == NLMSG_DONE) {
                done = true;
                break;
            }
            if (nlh->nlmsg_type == NLMSG_ERROR) {
                const auto* err =
                    reinterpret_cast<const nlmsgerr*>(NLMSG_DATA(nlh));
                if (err != nullptr && err->error != 0) {
                    errno = -err->error;
                    return NetlinkErrno("XFRM_MSG_GETSA");
                }
                break;
            }
            if (nlh->nlmsg_type == XFRM_MSG_NEWSA) {
                if (nlh->nlmsg_len < NLMSG_LENGTH(sizeof(xfrm_usersa_info))) {
                    return Status::Error(StatusCode::kCorruption,
                                         "short XFRM_MSG_NEWSA");
                }
                const auto* sa =
                    reinterpret_cast<const xfrm_usersa_info*>(NLMSG_DATA(nlh));
                IpsecSaInfo info;
                if (sa->family == AF_INET) {
                    info.family = IpsecAddressFamily::kIpv4;
                    info.address_bytes = 4;
                    std::memcpy(info.daddr.data(), &sa->id.daddr.a4, 4);
                    std::memcpy(info.saddr.data(), &sa->saddr.a4, 4);
                } else if (sa->family == AF_INET6) {
                    info.family = IpsecAddressFamily::kIpv6;
                    info.address_bytes = 16;
                    std::memcpy(info.daddr.data(), &sa->id.daddr.a6, 16);
                    std::memcpy(info.saddr.data(), &sa->saddr.a6, 16);
                } else {
                    p += NLMSG_ALIGN(nlh->nlmsg_len);
                    continue;
                }
                info.spi = ntohl(sa->id.spi);
                info.mode = sa->mode;
                info.proto = sa->id.proto;
                out.push_back(info);
            }
            p += NLMSG_ALIGN(nlh->nlmsg_len);
        }
        if (done) break;
    }
    return out;
}

Result<std::vector<xfrm_userpolicy_info>> DumpPolicies(int fd) {
    MINO_RETURN_IF_ERROR(SendNetlinkDump(fd, XFRM_MSG_GETPOLICY));
    std::vector<xfrm_userpolicy_info> out;
    std::array<std::byte, 8192> buffer{};
    for (;;) {
        const ssize_t n = ::recv(fd, buffer.data(), buffer.size(), 0);
        if (n < 0) {
            return NetlinkErrno("netlink policy dump recv");
        }
        if (n == 0) break;
        const std::byte* p = buffer.data();
        const std::byte* end = buffer.data() + n;
        bool done = false;
        while (p + sizeof(nlmsghdr) <= end) {
            const auto* nlh = reinterpret_cast<const nlmsghdr*>(p);
            if (nlh->nlmsg_len < sizeof(nlmsghdr) ||
                p + nlh->nlmsg_len > end) {
                return Status::Error(StatusCode::kCorruption,
                                     "truncated XFRM policy netlink message");
            }
            if (nlh->nlmsg_type == NLMSG_DONE) {
                done = true;
                break;
            }
            if (nlh->nlmsg_type == NLMSG_ERROR) {
                const auto* err =
                    reinterpret_cast<const nlmsgerr*>(NLMSG_DATA(nlh));
                if (err != nullptr && err->error != 0) {
                    errno = -err->error;
                    return NetlinkErrno("XFRM_MSG_GETPOLICY");
                }
                break;
            }
            if (nlh->nlmsg_type == XFRM_MSG_NEWPOLICY) {
                if (nlh->nlmsg_len <
                    NLMSG_LENGTH(sizeof(xfrm_userpolicy_info))) {
                    return Status::Error(StatusCode::kCorruption,
                                         "short XFRM_MSG_NEWPOLICY");
                }
                const auto* pol =
                    reinterpret_cast<const xfrm_userpolicy_info*>(
                        NLMSG_DATA(nlh));
                out.push_back(*pol);
            }
            p += NLMSG_ALIGN(nlh->nlmsg_len);
        }
        if (done) break;
    }
    return out;
}

bool PolicyCoversIpv4(const xfrm_userpolicy_info& pol,
                      const IpsecSaSelector& selector) {
    if (pol.sel.family != AF_INET) return false;
    if (pol.dir != XFRM_POLICY_OUT) return false;
    const auto* daddr =
        reinterpret_cast<const std::byte*>(&pol.sel.daddr.a4);
    if (!SameAddress(selector.peer, daddr, 4)) return false;
    if (selector.peer.protocol != IpsecProtocol::kAny &&
        pol.sel.proto != 0 &&
        pol.sel.proto != static_cast<uint8_t>(selector.peer.protocol)) {
        return false;
    }
    return true;
}

void FillRandomKey(std::span<std::byte> key) {
    if (::getrandom(key.data(), key.size(), 0) ==
        static_cast<ssize_t>(key.size())) {
        return;
    }
    std::srand(static_cast<unsigned>(::time(nullptr) ^ ::getpid()));
    for (auto& b : key) {
        b = static_cast<std::byte>(::rand() & 0xff);
    }
}

Status NetlinkAddSa(int fd, uint32_t spi, bool outbound, uint8_t proto,
                    std::span<const std::byte, 16> key) {
    // xfrm_algo ends with a flexible array; pack attr + header + key manually.
    struct XfrmAlgoCryptAttr {
        nlattr hdr;
        char alg_name[64];
        unsigned int alg_key_len;
        char key[16];
    };
    struct {
        nlmsghdr nlh;
        xfrm_usersa_info sa;
        XfrmAlgoCryptAttr crypt;
    } req{};

    std::memset(&req, 0, sizeof(req));
    req.nlh.nlmsg_len = NLMSG_LENGTH(sizeof(req.sa) + sizeof(req.crypt));
    req.nlh.nlmsg_type = XFRM_MSG_NEWSA;
    req.nlh.nlmsg_flags = NLM_F_REQUEST | NLM_F_ACK | NLM_F_CREATE | NLM_F_EXCL;
    req.nlh.nlmsg_seq = NextNlSeq();

    req.sa.family = AF_INET;
    req.sa.saddr.a4 = htonl(INADDR_LOOPBACK);
    req.sa.id.daddr.a4 = htonl(INADDR_LOOPBACK);
    req.sa.id.spi = htonl(spi);
    req.sa.id.proto = IPPROTO_ESP;
    req.sa.sel.family = AF_INET;
    req.sa.sel.saddr.a4 = htonl(INADDR_LOOPBACK);
    req.sa.sel.daddr.a4 = htonl(INADDR_LOOPBACK);
    req.sa.sel.prefixlen_s = 32;
    req.sa.sel.prefixlen_d = 32;
    req.sa.sel.proto = proto;
    req.sa.mode = XFRM_MODE_TRANSPORT;
    req.sa.replay_window = 32;
    req.sa.lft.soft_byte_limit = XFRM_INF;
    req.sa.lft.hard_byte_limit = XFRM_INF;
    req.sa.lft.soft_packet_limit = XFRM_INF;
    req.sa.lft.hard_packet_limit = XFRM_INF;
    (void)outbound;

    req.crypt.hdr.nla_len = sizeof(req.crypt);
    req.crypt.hdr.nla_type = XFRMA_ALG_CRYPT;
    std::strncpy(req.crypt.alg_name, "aes", sizeof(req.crypt.alg_name));
    req.crypt.alg_key_len = 128;
    std::memcpy(req.crypt.key, key.data(), 16);

    sockaddr_nl kernel{};
    kernel.nl_family = AF_NETLINK;
    if (::sendto(fd, &req, req.nlh.nlmsg_len, 0,
                 reinterpret_cast<sockaddr*>(&kernel), sizeof(kernel)) < 0) {
        return NetlinkErrno("XFRM_MSG_NEWSA");
    }

    std::array<std::byte, 1024> buffer{};
    const ssize_t n = ::recv(fd, buffer.data(), buffer.size(), 0);
    if (n < 0) return NetlinkErrno("XFRM_MSG_NEWSA ack");
    const auto* nlh = reinterpret_cast<const nlmsghdr*>(buffer.data());
    if (n >= static_cast<ssize_t>(sizeof(nlmsghdr)) &&
        nlh->nlmsg_type == NLMSG_ERROR) {
        const auto* err = reinterpret_cast<const nlmsgerr*>(NLMSG_DATA(nlh));
        if (err != nullptr && err->error != 0) {
            errno = -err->error;
            return NetlinkErrno("XFRM_MSG_NEWSA");
        }
    }
    return Status::Ok();
}

Status NetlinkDelSa(int fd, uint32_t spi) {
    struct {
        nlmsghdr nlh;
        xfrm_usersa_id id;
    } req{};
    req.nlh.nlmsg_len = NLMSG_LENGTH(sizeof(req.id));
    req.nlh.nlmsg_type = XFRM_MSG_DELSA;
    req.nlh.nlmsg_flags = NLM_F_REQUEST | NLM_F_ACK;
    req.nlh.nlmsg_seq = NextNlSeq();
    req.id.daddr.a4 = htonl(INADDR_LOOPBACK);
    req.id.spi = htonl(spi);
    req.id.family = AF_INET;
    req.id.proto = IPPROTO_ESP;

    sockaddr_nl kernel{};
    kernel.nl_family = AF_NETLINK;
    if (::sendto(fd, &req, req.nlh.nlmsg_len, 0,
                 reinterpret_cast<sockaddr*>(&kernel), sizeof(kernel)) < 0) {
        return NetlinkErrno("XFRM_MSG_DELSA");
    }
    std::array<std::byte, 512> buffer{};
    (void)::recv(fd, buffer.data(), buffer.size(), 0);
    return Status::Ok();
}

Status NetlinkAddPolicy(int fd, uint8_t dir, uint8_t proto, uint32_t spi) {
    struct {
        nlmsghdr nlh;
        xfrm_userpolicy_info pol;
        struct {
            nlattr hdr;
            xfrm_user_tmpl tmpl;
        } tmpl_attr;
    } req{};
    std::memset(&req, 0, sizeof(req));
    req.nlh.nlmsg_len = NLMSG_LENGTH(sizeof(req.pol) + sizeof(req.tmpl_attr));
    req.nlh.nlmsg_type = XFRM_MSG_NEWPOLICY;
    req.nlh.nlmsg_flags = NLM_F_REQUEST | NLM_F_ACK | NLM_F_CREATE | NLM_F_EXCL;
    req.nlh.nlmsg_seq = NextNlSeq();

    req.pol.sel.family = AF_INET;
    req.pol.sel.saddr.a4 = htonl(INADDR_LOOPBACK);
    req.pol.sel.daddr.a4 = htonl(INADDR_LOOPBACK);
    req.pol.sel.prefixlen_s = 32;
    req.pol.sel.prefixlen_d = 32;
    req.pol.sel.proto = proto;
    req.pol.lft.soft_byte_limit = XFRM_INF;
    req.pol.lft.hard_byte_limit = XFRM_INF;
    req.pol.lft.soft_packet_limit = XFRM_INF;
    req.pol.lft.hard_packet_limit = XFRM_INF;
    req.pol.priority = 1000;
    req.pol.action = XFRM_POLICY_ALLOW;
    req.pol.dir = dir;

    req.tmpl_attr.hdr.nla_type = XFRMA_TMPL;
    req.tmpl_attr.hdr.nla_len = sizeof(req.tmpl_attr);
    req.tmpl_attr.tmpl.id.daddr.a4 = htonl(INADDR_LOOPBACK);
    req.tmpl_attr.tmpl.id.spi = htonl(spi);
    req.tmpl_attr.tmpl.id.proto = IPPROTO_ESP;
    req.tmpl_attr.tmpl.family = AF_INET;
    req.tmpl_attr.tmpl.saddr.a4 = htonl(INADDR_LOOPBACK);
    req.tmpl_attr.tmpl.mode = XFRM_MODE_TRANSPORT;
    req.tmpl_attr.tmpl.aalgos = ~0u;
    req.tmpl_attr.tmpl.ealgos = ~0u;
    req.tmpl_attr.tmpl.calgos = ~0u;

    sockaddr_nl kernel{};
    kernel.nl_family = AF_NETLINK;
    if (::sendto(fd, &req, req.nlh.nlmsg_len, 0,
                 reinterpret_cast<sockaddr*>(&kernel), sizeof(kernel)) < 0) {
        return NetlinkErrno("XFRM_MSG_NEWPOLICY");
    }
    std::array<std::byte, 1024> buffer{};
    const ssize_t n = ::recv(fd, buffer.data(), buffer.size(), 0);
    if (n < 0) return NetlinkErrno("XFRM_MSG_NEWPOLICY ack");
    const auto* nlh = reinterpret_cast<const nlmsghdr*>(buffer.data());
    if (n >= static_cast<ssize_t>(sizeof(nlmsghdr)) &&
        nlh->nlmsg_type == NLMSG_ERROR) {
        const auto* err = reinterpret_cast<const nlmsgerr*>(NLMSG_DATA(nlh));
        if (err != nullptr && err->error != 0) {
            errno = -err->error;
            return NetlinkErrno("XFRM_MSG_NEWPOLICY");
        }
    }
    return Status::Ok();
}

Status NetlinkDelPolicy(int fd, uint8_t dir, uint8_t proto) {
    struct {
        nlmsghdr nlh;
        xfrm_userpolicy_id id;
    } req{};
    req.nlh.nlmsg_len = NLMSG_LENGTH(sizeof(req.id));
    req.nlh.nlmsg_type = XFRM_MSG_DELPOLICY;
    req.nlh.nlmsg_flags = NLM_F_REQUEST | NLM_F_ACK;
    req.nlh.nlmsg_seq = NextNlSeq();
    req.id.sel.family = AF_INET;
    req.id.sel.saddr.a4 = htonl(INADDR_LOOPBACK);
    req.id.sel.daddr.a4 = htonl(INADDR_LOOPBACK);
    req.id.sel.prefixlen_s = 32;
    req.id.sel.prefixlen_d = 32;
    req.id.sel.proto = proto;
    req.id.dir = dir;

    sockaddr_nl kernel{};
    kernel.nl_family = AF_NETLINK;
    if (::sendto(fd, &req, req.nlh.nlmsg_len, 0,
                 reinterpret_cast<sockaddr*>(&kernel), sizeof(kernel)) < 0) {
        return NetlinkErrno("XFRM_MSG_DELPOLICY");
    }
    std::array<std::byte, 512> buffer{};
    (void)::recv(fd, buffer.data(), buffer.size(), 0);
    return Status::Ok();
}

#endif  // __linux__

}  // namespace

Result<IpsecEndpoint> MakeIpv4Endpoint(std::span<const std::byte, 4> address,
                                       uint16_t port, IpsecProtocol protocol) {
    IpsecEndpoint endpoint;
    endpoint.family = IpsecAddressFamily::kIpv4;
    endpoint.protocol = protocol;
    endpoint.address_bytes = 4;
    endpoint.port = port;
    std::memcpy(endpoint.address.data(), address.data(), 4);
    return endpoint;
}

Status ScriptedIpsecSaProbe::RequireProtection(const IpsecSaSelector& selector) {
    if (selector.peer.address_bytes != 4 && selector.peer.address_bytes != 16) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "IPsec selector peer address length invalid");
    }
    if (allow_all_) return Status::Ok();
    for (const auto& allowed : allowed_peers_) {
        if (allowed.family == selector.peer.family &&
            allowed.address_bytes == selector.peer.address_bytes &&
            std::memcmp(allowed.address.data(), selector.peer.address.data(),
                        allowed.address_bytes) == 0) {
            if (allowed.protocol == IpsecProtocol::kAny ||
                selector.peer.protocol == IpsecProtocol::kAny ||
                allowed.protocol == selector.peer.protocol) {
                return Status::Ok();
            }
        }
    }
    return Status::Error(StatusCode::kUnavailable,
                         deny_message_.empty() ? "IPsec SA missing"
                                               : deny_message_);
}

Result<std::shared_ptr<NetlinkXfrmSaProbe>> NetlinkXfrmSaProbe::Create() {
#if defined(__linux__)
    MINO_ASSIGN_OR_RETURN(ScopedFd fd, OpenXfrmSocket());
    fd.Reset();  // prove the family is available; reopen per call
    return std::shared_ptr<NetlinkXfrmSaProbe>(new NetlinkXfrmSaProbe());
#else
    return Status::Error(StatusCode::kUnsupported,
                         "NetlinkXfrmSaProbe requires Linux XFRM");
#endif
}

Result<std::vector<IpsecSaInfo>>
NetlinkXfrmSaProbe::ListSecurityAssociations() {
#if defined(__linux__)
    MINO_ASSIGN_OR_RETURN(ScopedFd fd, OpenXfrmSocket());
    return DumpSecurityAssociations(fd.get());
#else
    return Status::Error(StatusCode::kUnsupported,
                         "NetlinkXfrmSaProbe requires Linux XFRM");
#endif
}

Status NetlinkXfrmSaProbe::RequireProtection(const IpsecSaSelector& selector) {
#if defined(__linux__)
    if (selector.peer.address_bytes != 4 && selector.peer.address_bytes != 16) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "IPsec selector peer address length invalid");
    }
    MINO_ASSIGN_OR_RETURN(ScopedFd fd, OpenXfrmSocket());
    auto sas = DumpSecurityAssociations(fd.get());
    if (!sas.ok()) {
        if (sas.status().code() == StatusCode::kPermissionDenied) {
            return Status::Error(
                StatusCode::kUnavailable,
                "cannot verify IPsec SA (netlink permission denied); fail-closed");
        }
        return sas.status();
    }

    bool found_out = false;
    bool found_in = !selector.require_inbound;
    for (const auto& sa : *sas) {
        if (EndpointMatchesPeer(selector.peer, sa)) {
            found_out = true;
        }
        if (selector.require_inbound && selector.local.has_value()) {
            // Inbound SA toward local: SA daddr matches local, saddr matches peer.
            if (SameAddress(*selector.local, sa.daddr.data(),
                            sa.address_bytes) &&
                SameAddress(selector.peer, sa.saddr.data(), sa.address_bytes)) {
                found_in = true;
            }
        }
    }
    if (!found_out) {
        return Status::Error(
            StatusCode::kUnavailable,
            "no covering outbound IPsec SA for peer (fail-closed)");
    }
    if (!found_in) {
        return Status::Error(
            StatusCode::kUnavailable,
            "no covering inbound IPsec SA for local endpoint (fail-closed)");
    }
    if (selector.require_out_policy) {
        MINO_ASSIGN_OR_RETURN(auto policies, DumpPolicies(fd.get()));
        bool found_policy = false;
        for (const auto& pol : policies) {
            if (selector.peer.family == IpsecAddressFamily::kIpv4 &&
                PolicyCoversIpv4(pol, selector)) {
                found_policy = true;
                break;
            }
        }
        if (!found_policy) {
            return Status::Error(
                StatusCode::kUnavailable,
                "no covering outbound IPsec policy for peer (fail-closed)");
        }
    }
    return Status::Ok();
#else
    (void)selector;
    return Status::Error(StatusCode::kUnsupported,
                         "NetlinkXfrmSaProbe requires Linux XFRM");
#endif
}

TestLoopbackXfrmSession::TestLoopbackXfrmSession(
    TestLoopbackXfrmSession&& other) noexcept {
    *this = std::move(other);
}

TestLoopbackXfrmSession& TestLoopbackXfrmSession::operator=(
    TestLoopbackXfrmSession&& other) noexcept {
    if (this == &other) return *this;
    TearDown();
    active_ = other.active_;
    spi_out_ = other.spi_out_;
    spi_in_ = other.spi_in_;
    protocol_ = other.protocol_;
    key_a_ = other.key_a_;
    key_b_ = other.key_b_;
    other.active_ = false;
    other.spi_out_ = 0;
    other.spi_in_ = 0;
    std::memset(other.key_a_.data(), 0, other.key_a_.size());
    std::memset(other.key_b_.data(), 0, other.key_b_.size());
    return *this;
}

TestLoopbackXfrmSession::~TestLoopbackXfrmSession() { TearDown(); }

void TestLoopbackXfrmSession::TearDown() noexcept {
#if defined(__linux__)
    if (!active_) return;
    auto fd = OpenXfrmSocket();
    if (fd.ok()) {
        const uint8_t proto = static_cast<uint8_t>(protocol_);
        (void)NetlinkDelPolicy(fd->get(), XFRM_POLICY_OUT, proto);
        (void)NetlinkDelPolicy(fd->get(), XFRM_POLICY_IN, proto);
        if (spi_out_ != 0) (void)NetlinkDelSa(fd->get(), spi_out_);
        if (spi_in_ != 0) (void)NetlinkDelSa(fd->get(), spi_in_);
    }
#endif
    active_ = false;
    spi_out_ = 0;
    spi_in_ = 0;
    std::memset(key_a_.data(), 0, key_a_.size());
    std::memset(key_b_.data(), 0, key_b_.size());
}

Result<TestLoopbackXfrmSession> TestLoopbackXfrmSession::InstallIpv4Loopback(
    IpsecProtocol protocol) {
#if defined(__linux__)
    if (protocol != IpsecProtocol::kTcp && protocol != IpsecProtocol::kUdp &&
        protocol != IpsecProtocol::kAny) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "unsupported IPsec test protocol");
    }
    MINO_ASSIGN_OR_RETURN(ScopedFd fd, OpenXfrmSocket());
    TestLoopbackXfrmSession session;
    session.protocol_ = protocol;
    FillRandomKey(session.key_a_);
    FillRandomKey(session.key_b_);
    // Fixed high SPIs in test range to avoid clashing with system SAs.
    session.spi_out_ = 0xA11CE001u;
    session.spi_in_ = 0xA11CE002u;
    const uint8_t proto = protocol == IpsecProtocol::kAny
                                ? 0
                                : static_cast<uint8_t>(protocol);

    MINO_RETURN_IF_ERROR(
        NetlinkAddSa(fd.get(), session.spi_out_, /*outbound=*/true, proto,
                     session.key_a_));
    auto inbound = NetlinkAddSa(fd.get(), session.spi_in_, /*outbound=*/false,
                                proto, session.key_b_);
    if (!inbound.ok()) {
        (void)NetlinkDelSa(fd.get(), session.spi_out_);
        return inbound;
    }
    auto out_pol =
        NetlinkAddPolicy(fd.get(), XFRM_POLICY_OUT, proto, session.spi_out_);
    if (!out_pol.ok()) {
        (void)NetlinkDelSa(fd.get(), session.spi_out_);
        (void)NetlinkDelSa(fd.get(), session.spi_in_);
        return out_pol;
    }
    auto in_pol =
        NetlinkAddPolicy(fd.get(), XFRM_POLICY_IN, proto, session.spi_in_);
    if (!in_pol.ok()) {
        (void)NetlinkDelPolicy(fd.get(), XFRM_POLICY_OUT, proto);
        (void)NetlinkDelSa(fd.get(), session.spi_out_);
        (void)NetlinkDelSa(fd.get(), session.spi_in_);
        return in_pol;
    }
    session.active_ = true;
    return session;
#else
    (void)protocol;
    return Status::Error(StatusCode::kUnsupported,
                         "TestLoopbackXfrmSession requires Linux XFRM");
#endif
}

}  // namespace mino::security
