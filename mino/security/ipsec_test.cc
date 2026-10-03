// Copyright 2026 The Mino Authors

#include "mino/security/ipsec.h"

#include <cstdlib>

#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <thread>

namespace mino::security {
namespace {

IpsecEndpoint LoopbackTcp() {
    const std::array<std::byte, 4> addr = {
        std::byte{127}, std::byte{0}, std::byte{0}, std::byte{1}};
    auto ep = MakeIpv4Endpoint(addr, 0, IpsecProtocol::kTcp);
    EXPECT_TRUE(ep.ok()) << ep.status().ToString();
    return ep.ok() ? *ep : IpsecEndpoint{};
}

TEST(IpsecSaProbeTest, ScriptedFailClosedByDefault) {
    ScriptedIpsecSaProbe probe;
    IpsecSaSelector selector;
    selector.peer = LoopbackTcp();
    const Status status = probe.RequireProtection(selector);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), StatusCode::kUnavailable);
}

TEST(IpsecSaProbeTest, ScriptedAllowPeer) {
    ScriptedIpsecSaProbe probe;
    probe.AllowPeer(LoopbackTcp());
    IpsecSaSelector selector;
    selector.peer = LoopbackTcp();
    EXPECT_TRUE(probe.RequireProtection(selector).ok());

    const std::array<std::byte, 4> other = {
        std::byte{10}, std::byte{0}, std::byte{0}, std::byte{1}};
    auto other_ep = MakeIpv4Endpoint(other, 0, IpsecProtocol::kTcp);
    ASSERT_TRUE(other_ep.ok());
    selector.peer = *other_ep;
    EXPECT_EQ(probe.RequireProtection(selector).code(),
              StatusCode::kUnavailable);
}

TEST(IpsecSaProbeTest, ScriptedRevocationClearsPeersAndSupportsConcurrentChecks) {
    ScriptedIpsecSaProbe probe;
    IpsecSaSelector selector;
    selector.peer = LoopbackTcp();
    probe.AllowPeer(selector.peer);
    ASSERT_TRUE(probe.RequireProtection(selector).ok());
    probe.DenyWith("revoked");
    EXPECT_EQ(probe.RequireProtection(selector).code(), StatusCode::kUnavailable);
    std::thread reader([&] {
        for (int i = 0; i < 1000; ++i) {
            const auto status = probe.RequireProtection(selector);
            EXPECT_TRUE(status.ok() || status.code() == StatusCode::kUnavailable);
        }
    });
    for (int i = 0; i < 1000; ++i) {
        probe.AllowPeer(selector.peer);
        probe.DenyWith("revoked concurrently");
    }
    reader.join();
    EXPECT_EQ(probe.RequireProtection(selector).code(), StatusCode::kUnavailable);
}

TEST(IpsecSaProbeTest, NetlinkProbeCreateAndList) {
    auto probe = NetlinkXfrmSaProbe::Create();
    ASSERT_TRUE(probe.ok()) << probe.status().ToString();
    auto listed = (*probe)->ListSecurityAssociations();
    if (!listed.ok() &&
        listed.status().code() == StatusCode::kPermissionDenied) {
        ASSERT_EQ(std::getenv("MINO_REQUIRE_KERNEL_IPSEC"), nullptr)
            << "required kernel IPsec test cannot skip: " << listed.status().ToString();
        GTEST_SKIP() << "NETLINK_XFRM dump requires privileges: "
                     << listed.status().ToString();
    }
    ASSERT_TRUE(listed.ok()) << listed.status().ToString();
    // Empty is fine; presence of the dump path is what we need without CAP.
}

TEST(IpsecSaProbeTest, NetlinkFailClosedWithoutCoveringSa) {
    auto probe = NetlinkXfrmSaProbe::Create();
    ASSERT_TRUE(probe.ok()) << probe.status().ToString();
    IpsecSaSelector selector;
    // Use a unique TEST-NET address unlikely to have a system SA.
    const std::array<std::byte, 4> addr = {
        std::byte{192}, std::byte{0}, std::byte{2}, std::byte{99}};
    auto ep = MakeIpv4Endpoint(addr, 443, IpsecProtocol::kTcp);
    ASSERT_TRUE(ep.ok());
    selector.peer = *ep;
    const Status status = (*probe)->RequireProtection(selector);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), StatusCode::kUnavailable);
}

TEST(IpsecSaProbeTest, LoopbackInstallOrSkip) {
    auto session = TestLoopbackXfrmSession::InstallIpv4Loopback(IpsecProtocol::kTcp);
    if (!session.ok()) {
        if (session.status().code() == StatusCode::kPermissionDenied ||
            session.status().code() == StatusCode::kUnavailable) {
            ASSERT_EQ(std::getenv("MINO_REQUIRE_KERNEL_IPSEC"), nullptr)
                << "required kernel IPsec test cannot skip: " << session.status().ToString();
            GTEST_SKIP() << "CAP_NET_ADMIN required for XFRM install: "
                         << session.status().ToString();
        }
        FAIL() << session.status().ToString();
    }
    ASSERT_TRUE(session->active());

    auto probe = NetlinkXfrmSaProbe::Create();
    ASSERT_TRUE(probe.ok()) << probe.status().ToString();
    IpsecSaSelector selector;
    selector.peer = LoopbackTcp();
    selector.local = LoopbackTcp();
    selector.require_inbound = true;
    const auto protection = (*probe)->RequireProtection(selector);
    EXPECT_TRUE(protection.ok()) << protection.ToString();

    // The original implementation admitted this state: SAs remain installed,
    // but there is no policy selecting ESP for the actual traffic.
    ASSERT_TRUE(session->RemovePoliciesForTesting().ok());
    auto sas = (*probe)->ListSecurityAssociations();
    ASSERT_TRUE(sas.ok()) << sas.status().ToString();
    EXPECT_GE(sas->size(), 2u);
    EXPECT_EQ((*probe)->RequireProtection(selector).code(), StatusCode::kUnavailable);
}

}  // namespace
}  // namespace mino::security
