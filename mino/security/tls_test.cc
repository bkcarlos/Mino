// Copyright 2026 The Mino Authors

#include "mino/security/tls.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cstddef>
#include <memory>
#include <span>
#include <string_view>
#include <utility>

#include "mino/security/test_tls_credentials.h"

namespace mino::security {
namespace {

class SocketPair final {
public:
    SocketPair() {
        if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds_) != 0) return;
        valid_ = true;
        for (int fd : fds_) {
            const int flags = ::fcntl(fd, F_GETFL, 0);
            if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
                valid_ = false;
            }
        }
    }
    ~SocketPair() {
        if (fds_[0] >= 0) (void)::close(fds_[0]);
        if (fds_[1] >= 0) (void)::close(fds_[1]);
    }
    bool valid() const noexcept { return valid_; }
    int first() const noexcept { return fds_[0]; }
    int second() const noexcept { return fds_[1]; }
    void CloseSecond() noexcept {
        if (fds_[1] >= 0) {
            (void)::close(fds_[1]);
            fds_[1] = -1;
        }
    }

private:
    int fds_[2] = {-1, -1};
    bool valid_ = false;
};

TEST(TlsTest, PrincipalUriIsCanonicalAndBounded) {
    auto uri = PrincipalUri(NodeId{42}, SecurityDomainId{7});
    ASSERT_TRUE(uri.ok()) << uri.status().ToString();
    EXPECT_EQ(*uri, "spiffe://mino/domain/7/node/42");
    std::array<std::byte, kCertificateFingerprintBytes> fingerprint{};
    fingerprint[0] = std::byte{0xa5};
    auto parsed = ParsePrincipalUri(*uri, 9, fingerprint);
    ASSERT_TRUE(parsed.ok()) << parsed.status().ToString();
    EXPECT_EQ(parsed->node_id, NodeId{42});
    EXPECT_EQ(parsed->security_domain, SecurityDomainId{7});
    EXPECT_EQ(parsed->credential_generation, 9u);
    EXPECT_EQ(parsed->certificate_sha256, fingerprint);
    EXPECT_EQ(ParsePrincipalUri("spiffe://mino/domain/0/node/42", 9,
                                fingerprint)
                  .status()
                  .code(),
              StatusCode::kPermissionDenied);
    EXPECT_EQ(ParsePrincipalUri("spiffe://mino/domain/07/node/42", 9,
                                fingerprint)
                  .status()
                  .code(),
              StatusCode::kPermissionDenied);
    EXPECT_EQ(ParsePrincipalUri("spiffe://mino/domain/7/node/042", 9,
                                fingerprint)
                  .status()
                  .code(),
              StatusCode::kPermissionDenied);
}

TEST(TlsTest, ProviderRotationIsMonotonicAndSnapshotsRemainImmutable) {
    const std::array principals = {
        testing::TestPrincipal{NodeId{1}, SecurityDomainId{10}},
        testing::TestPrincipal{NodeId{2}, SecurityDomainId{10}},
    };
    auto generated = testing::GenerateTlsCredentials(principals);
    ASSERT_TRUE(generated.ok()) << generated.status().ToString();
    auto provider = StaticTlsCredentialProvider::Create(
        std::move((*generated)[0]));
    ASSERT_TRUE(provider.ok()) << provider.status().ToString();
    auto first = (*provider)->Snapshot();
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    ASSERT_TRUE((*provider)->Rotate(std::move((*generated)[1])).ok());
    auto second = (*provider)->Snapshot();
    ASSERT_TRUE(second.ok()) << second.status().ToString();
    EXPECT_EQ(first->generation, 1u);
    EXPECT_EQ(second->generation, 2u);
    EXPECT_EQ(first->credentials->local_identity.node_id, NodeId{1});
    EXPECT_EQ(second->credentials->local_identity.node_id, NodeId{2});
}

TEST(TlsTest, RejectsCertificateWithoutPrincipalSan) {
    const std::array principals = {
        testing::TestPrincipal{NodeId{1}, SecurityDomainId{10}},
    };
    auto generated = testing::GenerateTlsCredentialsWithoutSan(principals);
    ASSERT_TRUE(generated.ok()) << generated.status().ToString();
    auto provider = StaticTlsCredentialProvider::Create(
        std::move((*generated)[0]));
    ASSERT_TRUE(provider.ok()) << provider.status().ToString();
    auto factory = CreateOpenSslTlsChannelFactory(*provider);
    ASSERT_FALSE(factory.ok());
    EXPECT_EQ(factory.status().code(), StatusCode::kPermissionDenied);
}

TEST(TlsTest, RejectsPeerSignedByUntrustedCa) {
    const std::array client_principals = {
        testing::TestPrincipal{NodeId{101}, SecurityDomainId{77}},
    };
    const std::array server_principals = {
        testing::TestPrincipal{NodeId{202}, SecurityDomainId{77}},
    };
    auto client_credentials =
        testing::GenerateTlsCredentials(client_principals);
    auto server_credentials =
        testing::GenerateTlsCredentials(server_principals);
    ASSERT_TRUE(client_credentials.ok());
    ASSERT_TRUE(server_credentials.ok());
    auto client_provider = StaticTlsCredentialProvider::Create(
        std::move((*client_credentials)[0]));
    auto server_provider = StaticTlsCredentialProvider::Create(
        std::move((*server_credentials)[0]));
    ASSERT_TRUE(client_provider.ok());
    ASSERT_TRUE(server_provider.ok());
    auto client_factory = CreateOpenSslTlsChannelFactory(*client_provider);
    auto server_factory = CreateOpenSslTlsChannelFactory(*server_provider);
    ASSERT_TRUE(client_factory.ok());
    ASSERT_TRUE(server_factory.ok());

    SocketPair sockets;
    ASSERT_TRUE(sockets.valid());
    auto client = (*client_factory)->Create(sockets.first(), TlsRole::kClient);
    auto server = (*server_factory)->Create(sockets.second(), TlsRole::kServer);
    ASSERT_TRUE(client.ok());
    ASSERT_TRUE(server.ok());
    bool rejected = false;
    for (size_t attempt = 0; attempt < 10'000 && !rejected; ++attempt) {
        auto client_step = (*client)->Handshake();
        auto server_step = (*server)->Handshake();
        rejected = !client_step.ok() || !server_step.ok();
    }
    EXPECT_TRUE(rejected);
    EXPECT_FALSE((*client)->handshake_complete() &&
                 (*server)->handshake_complete());
}

TEST(TlsTest, HandshakeFailsWhenPeerCloses) {
    const std::array principals = {
        testing::TestPrincipal{NodeId{101}, SecurityDomainId{77}},
    };
    auto generated = testing::GenerateTlsCredentials(principals);
    ASSERT_TRUE(generated.ok());
    auto provider = StaticTlsCredentialProvider::Create(
        std::move((*generated)[0]));
    ASSERT_TRUE(provider.ok());
    auto factory = CreateOpenSslTlsChannelFactory(*provider);
    ASSERT_TRUE(factory.ok());
    SocketPair sockets;
    ASSERT_TRUE(sockets.valid());
    auto client = (*factory)->Create(sockets.first(), TlsRole::kClient);
    ASSERT_TRUE(client.ok());
    struct sigaction ignored{};
    struct sigaction previous{};
    ignored.sa_handler = SIG_IGN;
    ASSERT_EQ(sigemptyset(&ignored.sa_mask), 0);
    ASSERT_EQ(::sigaction(SIGPIPE, &ignored, &previous), 0);
    sockets.CloseSecond();
    auto step = (*client)->Handshake();
    EXPECT_FALSE(step.ok());
    EXPECT_EQ((*factory)->OperationalStats().handshake_failures, 1u);
    ASSERT_EQ(::sigaction(SIGPIPE, &previous, nullptr), 0);
}

TEST(TlsTest, OpenSslChannelsPerformMutualTlsAndExposePrincipals) {
    const std::array principals = {
        testing::TestPrincipal{NodeId{101}, SecurityDomainId{77}},
        testing::TestPrincipal{NodeId{202}, SecurityDomainId{77}},
    };
    auto generated = testing::GenerateTlsCredentials(principals);
    ASSERT_TRUE(generated.ok()) << generated.status().ToString();
    auto client_provider = StaticTlsCredentialProvider::Create(
        std::move((*generated)[0]));
    auto server_provider = StaticTlsCredentialProvider::Create(
        std::move((*generated)[1]));
    ASSERT_TRUE(client_provider.ok()) << client_provider.status().ToString();
    ASSERT_TRUE(server_provider.ok()) << server_provider.status().ToString();
    auto client_factory = CreateOpenSslTlsChannelFactory(*client_provider);
    auto server_factory = CreateOpenSslTlsChannelFactory(*server_provider);
    ASSERT_TRUE(client_factory.ok()) << client_factory.status().ToString();
    ASSERT_TRUE(server_factory.ok()) << server_factory.status().ToString();

    SocketPair sockets;
    ASSERT_TRUE(sockets.valid());
    auto client = (*client_factory)->Create(sockets.first(), TlsRole::kClient);
    auto server = (*server_factory)->Create(sockets.second(), TlsRole::kServer);
    ASSERT_TRUE(client.ok()) << client.status().ToString();
    ASSERT_TRUE(server.ok()) << server.status().ToString();
    for (size_t attempt = 0;
         attempt < 10'000 &&
         (!(*client)->handshake_complete() ||
          !(*server)->handshake_complete());
         ++attempt) {
        auto client_step = (*client)->Handshake();
        auto server_step = (*server)->Handshake();
        ASSERT_TRUE(client_step.ok()) << client_step.status().ToString();
        ASSERT_TRUE(server_step.ok()) << server_step.status().ToString();
    }
    ASSERT_TRUE((*client)->handshake_complete());
    ASSERT_TRUE((*server)->handshake_complete());
    auto client_peer = (*client)->peer();
    auto server_peer = (*server)->peer();
    ASSERT_TRUE(client_peer.ok()) << client_peer.status().ToString();
    ASSERT_TRUE(server_peer.ok()) << server_peer.status().ToString();
    EXPECT_EQ(client_peer->node_id, NodeId{202});
    EXPECT_EQ(server_peer->node_id, NodeId{101});
    EXPECT_EQ(client_peer->security_domain, SecurityDomainId{77});
    EXPECT_EQ(server_peer->security_domain, SecurityDomainId{77});
    EXPECT_NE(client_peer->certificate_sha256,
              server_peer->certificate_sha256);

    constexpr std::string_view message = "authenticated payload";
    const auto input = std::as_bytes(std::span(message.data(), message.size()));
    size_t sent = 0;
    std::array<std::byte, 64> output{};
    size_t received = 0;
    for (size_t attempt = 0;
         attempt < 10'000 && (sent != input.size() || received == 0);
         ++attempt) {
        if (sent != input.size()) {
            auto write = (*client)->Write(input.subspan(sent));
            ASSERT_TRUE(write.ok()) << write.status().ToString();
            sent += write->bytes;
        }
        auto read = (*server)->Read(output);
        ASSERT_TRUE(read.ok()) << read.status().ToString();
        received += read->bytes;
    }
    ASSERT_EQ(sent, input.size());
    ASSERT_EQ(received, input.size());
    EXPECT_TRUE(std::equal(input.begin(), input.end(), output.begin()));
}

}  // namespace
}  // namespace mino::security
