// Copyright 2026 The Mino Authors
#include "mino/security/ipsec_policy.h"

#include <arpa/inet.h>
#include <linux/netlink.h>
#include <gtest/gtest.h>

#include <cstring>

namespace mino::security::internal {
namespace {

class XfrmPolicyTest : public ::testing::Test {
protected:
    void SetUp() override {
        flow.peer.address = {std::byte{192}, std::byte{0}, std::byte{2}, std::byte{2}};
        flow.peer.protocol = IpsecProtocol::kTcp;
        flow.peer.port = 443;
        flow.local = flow.peer;
        flow.local->address[3] = std::byte{1};
        flow.local->port = 12000;
        XfrmPolicy policy;
        policy.info.sel.family = AF_INET;
        policy.info.sel.proto = IPPROTO_TCP;
        policy.info.sel.prefixlen_s = 32;
        policy.info.sel.prefixlen_d = 32;
        policy.info.sel.dport = htons(443);
        policy.info.sel.dport_mask = 0xffff;
        std::memcpy(&policy.info.sel.saddr, flow.local->address.data(), 4);
        std::memcpy(&policy.info.sel.daddr, flow.peer.address.data(), 4);
        policy.info.dir = XFRM_POLICY_OUT;
        policy.info.action = XFRM_POLICY_ALLOW;
        policy.info.priority = 100;
        xfrm_user_tmpl tmpl{};
        tmpl.family = AF_INET;
        tmpl.id.proto = IPPROTO_ESP;
        tmpl.id.spi = htonl(42);
        tmpl.id.daddr = policy.info.sel.daddr;
        tmpl.saddr = policy.info.sel.saddr;
        tmpl.reqid = 7;
        tmpl.mode = XFRM_MODE_TRANSPORT;
        policy.templates.push_back(tmpl);
        policies.push_back(policy);
        IpsecSaInfo sa;
        sa.daddr = flow.peer.address;
        sa.saddr = flow.local->address;
        sa.spi = 42;
        sa.reqid = 7;
        sa.proto = IPPROTO_ESP;
        sa.mode = XFRM_MODE_TRANSPORT;
        sa.encrypts = true;
        sas.push_back(sa);
    }
    bool Allowed() { return RequireXfrmPolicies(flow, policies, sas).ok(); }
    IpsecSaSelector flow;
    std::vector<XfrmPolicy> policies;
    std::vector<IpsecSaInfo> sas;
};

TEST_F(XfrmPolicyTest, RequiresPolicyInAdditionToSa) {
    EXPECT_TRUE(Allowed());
    policies.clear();
    EXPECT_FALSE(Allowed());
}

TEST_F(XfrmPolicyTest, RejectsBypassOptionalAndUnencryptedState) {
    const auto tmpl = policies[0].templates[0];
    policies[0].templates.clear();
    EXPECT_FALSE(Allowed());
    policies[0].templates.push_back(tmpl);
    policies[0].templates[0].optional = 1;
    EXPECT_FALSE(Allowed());
    policies[0].templates[0].optional = 0;
    sas[0].encrypts = false;
    EXPECT_FALSE(Allowed());
}

TEST_F(XfrmPolicyTest, SocketBindingCannotUseUnrelatedEncryptingSa) {
    flow.socket_policy = SocketIpsecPolicy{.outbound_reqid = 7, .outbound_spi = 42};
    EXPECT_TRUE(Allowed());
    flow.socket_policy->outbound_spi = 43;
    EXPECT_FALSE(Allowed());
    auto null_cipher = sas.front();
    null_cipher.spi = 43;
    null_cipher.encrypts = false;
    sas.push_back(null_cipher);
    EXPECT_FALSE(Allowed());
    flow.socket_policy->outbound_spi = 42;
    flow.socket_policy->outbound_reqid = 8;
    EXPECT_FALSE(Allowed());
}

TEST_F(XfrmPolicyTest, BindsTemplateToSaIdentity) {
    sas[0].reqid++;
    EXPECT_FALSE(Allowed());
    sas[0].reqid--;
    sas[0].spi++;
    EXPECT_FALSE(Allowed());
    sas[0].spi--;
    sas[0].saddr[3] = std::byte{9};
    EXPECT_FALSE(Allowed());
}

TEST_F(XfrmPolicyTest, ChecksPortsProtocolsAddressesAndPrefix) {
    flow.peer.port = 80;
    EXPECT_FALSE(Allowed());
    flow.peer.port = 443;
    flow.peer.protocol = IpsecProtocol::kUdp;
    EXPECT_FALSE(Allowed());
    flow.peer.protocol = IpsecProtocol::kTcp;
    flow.local->address[3] = std::byte{9};
    EXPECT_FALSE(Allowed());
    flow.local->address[3] = std::byte{1};
    policies[0].info.sel.prefixlen_d = 24;
    policies[0].info.sel.daddr.a4 = htonl(0xc0000200);
    EXPECT_TRUE(Allowed());
}

TEST_F(XfrmPolicyTest, HigherPriorityBypassOrBlockWins) {
    auto bypass = policies[0];
    bypass.info.priority = 10;
    bypass.templates.clear();
    policies.push_back(bypass);
    EXPECT_FALSE(Allowed());
    policies[1] = policies[0];
    policies[1].info.priority = 10;
    policies[1].info.action = XFRM_POLICY_BLOCK;
    EXPECT_FALSE(Allowed());
    policies[1].info.priority = 101;
    EXPECT_TRUE(Allowed());
}

TEST_F(XfrmPolicyTest, RejectsAmbiguousPriorityAndUnsupportedScope) {
    policies.push_back(policies[0]);
    policies[1].templates.clear();
    EXPECT_FALSE(Allowed());
    policies.pop_back();
    policies[0].unsupported_scope = true;
    EXPECT_FALSE(Allowed());
}

TEST_F(XfrmPolicyTest, RequiresKnownLocalEndpointForConstrainedPolicy) {
    flow.local.reset();
    EXPECT_FALSE(Allowed());
}

TEST_F(XfrmPolicyTest, RequiresInboundPolicyAndSa) {
    flow.require_inbound = true;
    EXPECT_FALSE(Allowed());
    auto inbound = policies[0];
    inbound.info.dir = XFRM_POLICY_IN;
    std::swap(inbound.info.sel.saddr, inbound.info.sel.daddr);
    std::swap(inbound.info.sel.sport, inbound.info.sel.dport);
    std::swap(inbound.info.sel.sport_mask, inbound.info.sel.dport_mask);
    std::swap(inbound.templates[0].saddr, inbound.templates[0].id.daddr);
    policies.push_back(inbound);
    EXPECT_FALSE(Allowed());
    auto sa = sas[0];
    std::swap(sa.saddr, sa.daddr);
    sas.push_back(sa);
    EXPECT_TRUE(Allowed());
}

TEST_F(XfrmPolicyTest, Ipv6SelectorsAreValidated) {
    flow.peer.family = IpsecAddressFamily::kIpv6;
    flow.peer.address_bytes = 16;
    flow.peer.address[15] = std::byte{2};
    flow.local = flow.peer;
    flow.local->address[15] = std::byte{1};
    auto& policy = policies[0];
    policy.info.sel.family = AF_INET6;
    policy.info.sel.prefixlen_s = policy.info.sel.prefixlen_d = 128;
    std::memcpy(&policy.info.sel.saddr, flow.local->address.data(), 16);
    std::memcpy(&policy.info.sel.daddr, flow.peer.address.data(), 16);
    policy.templates[0].family = AF_INET6;
    policy.templates[0].saddr = policy.info.sel.saddr;
    policy.templates[0].id.daddr = policy.info.sel.daddr;
    sas[0].family = IpsecAddressFamily::kIpv6;
    sas[0].address_bytes = 16;
    sas[0].saddr = flow.local->address;
    sas[0].daddr = flow.peer.address;
    EXPECT_TRUE(Allowed());
    flow.peer.address[15] = std::byte{3};
    EXPECT_FALSE(Allowed());
}

TEST_F(XfrmPolicyTest, ParsesTemplatesAndRejectsTruncatedAttributes) {
    const size_t offset = NLA_ALIGN(sizeof(xfrm_userpolicy_info));
    std::vector<std::byte> bytes(offset + NLA_ALIGN(sizeof(nlattr) + sizeof(xfrm_user_tmpl)));
    std::memcpy(bytes.data(), &policies[0].info, sizeof(xfrm_userpolicy_info));
    nlattr attr{};
    attr.nla_type = XFRMA_TMPL;
    attr.nla_len = sizeof(nlattr) + sizeof(xfrm_user_tmpl);
    std::memcpy(bytes.data() + offset, &attr, sizeof(attr));
    std::memcpy(bytes.data() + offset + sizeof(attr), &policies[0].templates[0],
                sizeof(xfrm_user_tmpl));
    auto parsed = ParseXfrmPolicy(bytes);
    ASSERT_TRUE(parsed.ok());
    ASSERT_EQ(parsed->templates.size(), 1u);
    EXPECT_EQ(parsed->templates[0].reqid, 7u);
    bytes.pop_back();
    EXPECT_EQ(ParseXfrmPolicy(bytes).status().code(), StatusCode::kCorruption);
}

TEST_F(XfrmPolicyTest, KernelMainPolicyAndZeroMarkAreUnscoped) {
    const size_t offset = NLA_ALIGN(sizeof(xfrm_userpolicy_info));
    std::vector<std::byte> bytes(offset);
    std::memcpy(bytes.data(), &policies[0].info, sizeof(xfrm_userpolicy_info));
    const auto append = [&](uint16_t type, const auto& value) {
        const size_t start = bytes.size();
        nlattr attr{};
        attr.nla_type = type;
        attr.nla_len = sizeof(attr) + sizeof(value);
        bytes.resize(start + NLA_ALIGN(attr.nla_len));
        std::memcpy(bytes.data() + start, &attr, sizeof(attr));
        std::memcpy(bytes.data() + start + sizeof(attr), &value, sizeof(value));
    };
    xfrm_userpolicy_type type{};
    type.type = XFRM_POLICY_TYPE_MAIN;
    append(XFRMA_POLICY_TYPE, type);
    xfrm_mark mark{};
    append(XFRMA_MARK, mark);
    auto parsed = ParseXfrmPolicy(bytes);
    ASSERT_TRUE(parsed.ok());
    EXPECT_FALSE(parsed->unsupported_scope);
    mark.m = 0xff;
    std::memcpy(bytes.data() + bytes.size() - sizeof(mark), &mark, sizeof(mark));
    parsed = ParseXfrmPolicy(bytes);
    ASSERT_TRUE(parsed.ok());
    EXPECT_TRUE(parsed->unsupported_scope);
}

}  // namespace
}  // namespace mino::security::internal
