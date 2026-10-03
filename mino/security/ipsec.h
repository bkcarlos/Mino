// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_SECURITY_IPSEC_H_
#define MINO_SECURITY_IPSEC_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/security/socket_ipsec.h"

namespace mino::security {

// IPsec is an architecture option alongside TLS (arch §14.2). Mino does not
// terminate ESP in userspace: the kernel XFRM stack protects the socket path.
// These helpers verify that a covering Security Association (and optionally a
// policy) exists before Bridge/TCP/UDP traffic is admitted (fail-closed).

enum class IpsecAddressFamily : uint8_t {
    kIpv4 = 4,
    kIpv6 = 6,
};

enum class IpsecProtocol : uint8_t {
    kAny = 0,
    kTcp = 6,
    kUdp = 17,
};

struct IpsecEndpoint {
    IpsecAddressFamily family = IpsecAddressFamily::kIpv4;
    IpsecProtocol protocol = IpsecProtocol::kAny;
    // Network-order address bytes (4 or 16).
    std::array<std::byte, 16> address{};
    uint8_t address_bytes = 4;
    // Host-order port; 0 means "any".
    uint16_t port = 0;
};

struct IpsecSaSelector {
    // Peer (destination for outbound; source for inbound checks).
    IpsecEndpoint peer;
    // Optional local address/port to match against SA/policy selectors.
    std::optional<IpsecEndpoint> local;
    // When true, require at least one inbound SA covering `local`←`peer`.
    bool require_inbound = false;
    // Require an effective mandatory ESP policy bound to an encrypting SA.
    // Inbound policy is also required when require_inbound is set. False is
    // reserved for SA diagnostics; the transport wrapper always requests true.
    bool require_out_policy = true;
    // When socket policies override the global SPD, verify their reqid/SPI
    // bindings too. Never grant based on an unrelated global encrypting SA.
    std::optional<SocketIpsecPolicy> socket_policy = std::nullopt;
};

// Observed kernel SA used for diagnostics (no key material).
struct IpsecSaInfo {
    IpsecAddressFamily family = IpsecAddressFamily::kIpv4;
    std::array<std::byte, 16> daddr{};
    std::array<std::byte, 16> saddr{};
    uint8_t address_bytes = 4;
    uint32_t spi = 0;
    uint8_t mode = 0;  // XFRM_MODE_*
    uint8_t proto = 0; // IPPROTO_ESP / AH
    uint32_t reqid = 0;
    bool encrypts = false;
};

class IpsecSaProbe {
public:
    virtual ~IpsecSaProbe() = default;
    // Fail-closed: missing covering SA (or policy when required) returns
    // kFailedPrecondition-equivalent StatusCode::kUnavailable with a clear
    // message. Malformed selectors return kInvalidArgument.
    // Calls may run concurrently on admission and background verifier threads.
    // Implementations must be thread safe and finish within a bounded time;
    // transport shutdown joins any in-flight background check.
    virtual Status RequireProtection(const IpsecSaSelector& selector) = 0;
};

// Dumps kernel XFRM state (and optionally policy) via NETLINK_XFRM.
// Linux-only; other platforms return kUnsupported from the factory.
class NetlinkXfrmSaProbe final : public IpsecSaProbe {
public:
    static Result<std::shared_ptr<NetlinkXfrmSaProbe>> Create();

    Status RequireProtection(const IpsecSaSelector& selector) override;

    // Test/ops helper: list currently visible SAs (no keys).
    Result<std::vector<IpsecSaInfo>> ListSecurityAssociations();

private:
    NetlinkXfrmSaProbe() = default;
};

// In-memory probe for unit tests. Default deny (fail-closed).
class ScriptedIpsecSaProbe final : public IpsecSaProbe {
public:
    void AllowAll() {
        std::lock_guard lock(mutex_);
        allow_all_ = true;
        deny_message_.clear();
    }
    void DenyWith(std::string message) {
        std::lock_guard lock(mutex_);
        allow_all_ = false;
        allowed_peers_.clear();
        deny_message_ = std::move(message);
    }
    void AllowPeer(IpsecEndpoint peer) {
        std::lock_guard lock(mutex_);
        allow_all_ = false;
        allowed_peers_.push_back(std::move(peer));
    }

    Status RequireProtection(const IpsecSaSelector& selector) override;

private:
    std::mutex mutex_;
    bool allow_all_ = false;
    std::string deny_message_ = "IPsec SA missing (scripted deny)";
    std::vector<IpsecEndpoint> allowed_peers_;
};

// Minimal transport-mode ESP SA+policy installer for loopback tests.
// Requires CAP_NET_ADMIN. On permission errors returns kPermissionDenied so
// tests can GTEST_SKIP. Keys are ephemeral and scrubbed on Destroy.
class TestLoopbackXfrmSession final {
public:
    TestLoopbackXfrmSession(const TestLoopbackXfrmSession&) = delete;
    TestLoopbackXfrmSession& operator=(const TestLoopbackXfrmSession&) = delete;
    TestLoopbackXfrmSession(TestLoopbackXfrmSession&& other) noexcept;
    TestLoopbackXfrmSession& operator=(TestLoopbackXfrmSession&& other) noexcept;
    ~TestLoopbackXfrmSession();

    // Installs a pair of transport-mode ESP SAs and matching policies for
    // 127.0.0.1 <-> 127.0.0.1 covering the given IP protocol (TCP or UDP).
    // Ports are unrestricted (selector port 0) so any loopback port works.
    static Result<TestLoopbackXfrmSession> InstallIpv4Loopback(
        IpsecProtocol protocol);

    bool active() const noexcept { return active_; }
    // Fault injection: retain SAs while removing their selecting policies.
    Status RemovePoliciesForTesting();

private:
    TestLoopbackXfrmSession() = default;
    void TearDown() noexcept;

    bool active_ = false;
    uint32_t spi_out_ = 0;
    uint32_t spi_in_ = 0;
    IpsecProtocol protocol_ = IpsecProtocol::kTcp;
    std::array<std::byte, 16> key_a_{};
    std::array<std::byte, 16> key_b_{};
};

// Builds an IpsecEndpoint from a dotted IPv4 host-order port.
Result<IpsecEndpoint> MakeIpv4Endpoint(std::span<const std::byte, 4> address,
                                       uint16_t port, IpsecProtocol protocol);

}  // namespace mino::security

#endif  // MINO_SECURITY_IPSEC_H_
