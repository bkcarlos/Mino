// Copyright 2026 The Mino Authors

#include "mino/bridge/wire_aead_session.h"

#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <string_view>
#include <utility>
#include <vector>

#include "mino/security/test_tls_credentials.h"
#include "mino/security/tls.h"

namespace mino::bridge {
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

private:
    int fds_[2] = {-1, -1};
    bool valid_ = false;
};

TEST(WireAeadSessionTest, SharedSecretRoundTripEncryptsBothDirections) {
    std::array<std::byte, 32> secret{};
    for (size_t i = 0; i < secret.size(); ++i) {
        secret[i] = static_cast<std::byte>(i + 1);
    }
    auto context = WireAeadEpochContext(101, 202);
    auto client = DeriveWireAeadKeyringFromSharedSecret(secret, context, true);
    auto server = DeriveWireAeadKeyringFromSharedSecret(secret, context, false);
    ASSERT_TRUE(client.ok()) << client.status().ToString();
    ASSERT_TRUE(server.ok()) << server.status().ToString();
    EXPECT_EQ(client->encode_key_id(), kWireAeadClientToServerKeyId);
    EXPECT_EQ(server->encode_key_id(), kWireAeadServerToClientKeyId);

    WireFrame frame;
    frame.header.frame_type = FrameType::kData;
    frame.header.flags = FlagValue(FrameFlag::kAeadPresent);
    frame.header.topic_id = 1;
    frame.header.msg_type = 2;
    frame.header.connection_schema_ref = 3;
    frame.header.schema_version = 1;
    frame.header.layout_version = 1;
    frame.header.source_node_id = 11;
    frame.header.source_publisher_id = 22;
    frame.header.source_publisher_epoch = 33;
    frame.header.sequence_num = 7;
    frame.payload.assign(48, std::byte{0x5a});

    auto encoded = WireFrameCodec::Encode(frame, {}, &*client);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
    auto decoded = WireFrameCodec::Decode(*encoded, {}, &*server);
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    EXPECT_EQ(decoded->payload, frame.payload);

    // Tamper still fails.
    (*encoded)[encoded->size() / 2] ^= std::byte{0x01};
    auto tampered = WireFrameCodec::Decode(*encoded, {}, &*server);
    EXPECT_FALSE(tampered.ok());
    EXPECT_EQ(tampered.status().code(), StatusCode::kCorruption);
}

TEST(WireAeadSessionTest, KeyShareDeriveIsSymmetric) {
    std::vector<std::byte> psk(32, std::byte{0x11});
    auto local = MakeSessionKeyShare(psk, 101, 202);
    auto peer = MakeSessionKeyShare(psk, 202, 101);
    ASSERT_TRUE(local.ok());
    ASSERT_TRUE(peer.ok());
    ASSERT_TRUE(VerifySessionKeyShare(*peer, psk, 202, 101).ok());
    auto client = DeriveWireAeadKeyringFromKeyShare(
        psk, local->nonce, peer->nonce, 101, 202, true);
    auto server = DeriveWireAeadKeyringFromKeyShare(
        psk, peer->nonce, local->nonce, 202, 101, false);
    ASSERT_TRUE(client.ok()) << client.status().ToString();
    ASSERT_TRUE(server.ok()) << server.status().ToString();

    WireFrame frame;
    frame.header.frame_type = FrameType::kData;
    ApplySessionAeadFlags(&frame.header);
    frame.header.topic_id = 9;
    frame.header.msg_type = 1;
    frame.header.connection_schema_ref = 1;
    frame.header.schema_version = 1;
    frame.header.layout_version = 1;
    frame.header.source_node_id = 1;
    frame.header.source_publisher_id = 2;
    frame.header.source_publisher_epoch = 3;
    frame.header.sequence_num = 1;
    frame.payload = {std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};
    auto encoded = WireFrameCodec::Encode(frame, {}, &*client);
    ASSERT_TRUE(encoded.ok());
    LengthPrefixedFrameDecoder decoder;
    BindAeadKeyring(&decoder, &*server);
    auto prefixed = WireFrameCodec::EncodeLengthPrefixed(frame, {}, &*client);
    ASSERT_TRUE(prefixed.ok());
    auto pushed = decoder.Push(*prefixed);
    ASSERT_TRUE(pushed.ok()) << pushed.status().ToString();
    ASSERT_EQ(pushed->size(), 1u);
    EXPECT_EQ(pushed->front().payload.size(), frame.payload.size());
}

TEST(WireAeadSessionTest, TlsExporterInstallsMatchingKeyrings) {
    using mino::security::CreateOpenSslTlsChannelFactory;
    using mino::security::StaticTlsCredentialProvider;
    using mino::security::TlsRole;

    const std::array principals = {
        mino::security::testing::TestPrincipal{::mino::NodeId{101},
                                               ::mino::SecurityDomainId{77}},
        mino::security::testing::TestPrincipal{::mino::NodeId{202},
                                               ::mino::SecurityDomainId{77}},
    };
    auto generated =
        mino::security::testing::GenerateTlsCredentials(principals);
    ASSERT_TRUE(generated.ok());
    auto client_provider = StaticTlsCredentialProvider::Create(
        std::move((*generated)[0]));
    auto server_provider = StaticTlsCredentialProvider::Create(
        std::move((*generated)[1]));
    ASSERT_TRUE(client_provider.ok());
    ASSERT_TRUE(server_provider.ok());
    auto client_factory = CreateOpenSslTlsChannelFactory(*client_provider);
    auto server_factory = CreateOpenSslTlsChannelFactory(*server_provider);
    ASSERT_TRUE(client_factory.ok());
    ASSERT_TRUE(server_factory.ok());

    SocketPair sockets;
    ASSERT_TRUE(sockets.valid());
    auto client = (*client_factory)->Create(sockets.first(), TlsRole::kClient);
    auto server =
        (*server_factory)->Create(sockets.second(), TlsRole::kServer);
    ASSERT_TRUE(client.ok());
    ASSERT_TRUE(server.ok());
    for (size_t attempt = 0;
         attempt < 10'000 &&
         (!(*client)->handshake_complete() ||
          !(*server)->handshake_complete());
         ++attempt) {
        ASSERT_TRUE((*client)->Handshake().ok());
        ASSERT_TRUE((*server)->Handshake().ok());
    }
    ASSERT_TRUE((*client)->handshake_complete());
    ASSERT_TRUE((*server)->handshake_complete());

    auto context = WireAeadEpochContext(101, 202);
    auto client_material = (*client)->ExportKeyingMaterial(
        kWireAeadTlsExporterLabel, context, kWireAeadExporterLength);
    auto server_material = (*server)->ExportKeyingMaterial(
        kWireAeadTlsExporterLabel, context, kWireAeadExporterLength);
    ASSERT_TRUE(client_material.ok()) << client_material.status().ToString();
    ASSERT_TRUE(server_material.ok()) << server_material.status().ToString();
    ASSERT_EQ(*client_material, *server_material);

    auto client_keys =
        MakeWireAeadKeyringFromExporterMaterial(*client_material, true);
    auto server_keys =
        MakeWireAeadKeyringFromExporterMaterial(*server_material, false);
    ASSERT_TRUE(client_keys.ok());
    ASSERT_TRUE(server_keys.ok());

    WireFrame frame;
    frame.header.frame_type = FrameType::kData;
    ApplySessionAeadFlags(&frame.header);
    frame.header.topic_id = 1;
    frame.header.msg_type = 1;
    frame.header.connection_schema_ref = 1;
    frame.header.schema_version = 1;
    frame.header.layout_version = 1;
    frame.header.source_node_id = 101;
    frame.header.source_publisher_id = 1;
    frame.header.source_publisher_epoch = 1;
    frame.header.sequence_num = 42;
    frame.payload.assign(16, std::byte{0xaa});
    auto encoded = WireFrameCodec::Encode(frame, {}, &*client_keys);
    ASSERT_TRUE(encoded.ok());
    auto decoded = WireFrameCodec::Decode(*encoded, {}, &*server_keys);
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    EXPECT_EQ(decoded->payload, frame.payload);
}

TEST(WireAeadSessionTest, FailClosedWithoutKeys) {
    WireFrame frame;
    frame.header.frame_type = FrameType::kData;
    ApplySessionAeadFlags(&frame.header);
    frame.header.topic_id = 1;
    frame.header.msg_type = 1;
    frame.header.connection_schema_ref = 1;
    frame.header.schema_version = 1;
    frame.header.layout_version = 1;
    frame.header.source_node_id = 1;
    frame.header.source_publisher_id = 1;
    frame.header.source_publisher_epoch = 1;
    frame.header.sequence_num = 1;
    frame.payload = {std::byte{9}};
    auto encoded = WireFrameCodec::Encode(frame, {}, nullptr);
    EXPECT_FALSE(encoded.ok());
    EXPECT_EQ(encoded.status().code(), StatusCode::kInvalidArgument);
}

}  // namespace
}  // namespace mino::bridge
