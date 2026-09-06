// Copyright 2026 The Mino Authors

#include "mino/security/ipsec.h"

#include <gtest/gtest.h>

#include <array>
#include <cstring>

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

TEST(IpsecSaProbeTest, NetlinkProbeCreateAndList) {
    auto probe = NetlinkXfrmSaProbe::Create();
    ASSERT_TRUE(probe.ok()) << probe.status().ToString();
    auto listed = (*probe)->ListSecurityAssociations();
    if (!listed.ok() &&
        listed.status().code() == StatusCode::kPermissionDenied) {
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
    EXPECT_TRUE((*probe)->RequireProtection(selector).ok())
        << "installed loopback SA should cover 127.0.0.1";
}

}  // namespace
}  // namespace mino::security
