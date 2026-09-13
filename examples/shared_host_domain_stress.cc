// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/deployment/shared_host_domain.h"

#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

mino::schema::SchemaIdentity MakeSchema() {
    mino::schema::CanonicalDigest digest{};
    digest[0] = std::byte{0x44};
    digest[1] = std::byte{0x59};
    digest[2] = std::byte{0x4e};
    return mino::schema::SchemaIdentity(0x44594e01ull, digest, 1, 1);
}

bool WaitForPeerCount(mino::deployment::SharedHostDomain* domain,
                      size_t minimum, std::chrono::seconds timeout) {
    const auto deadline = mino::Deadline::FromNow(timeout);
    for (;;) {
        auto peers = domain->ListPeers();
        if (peers.ok() && peers->size() >= minimum) {
            return true;
        }
        if (deadline.expired()) {
            return false;
        }
        (void)domain->Heartbeat();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "usage: shared_host_domain_stress pub|sub|join-wait "
                     "shm-name node-id [messages]\n";
        return 2;
    }
    const std::string role = argv[1];
    const std::string name = argv[2];
    const uint64_t node_id = std::stoull(argv[3]);
    const uint64_t messages = argc >= 5 ? std::stoull(argv[4]) : 8;

    using mino::deployment::SharedHostDomain;
    using mino::deployment::SharedHostDomainOptions;

    SharedHostDomainOptions options;
    options.peer_slots = 8;
    options.topic_slots = 4;
    options.queue_depth = 32;
    options.max_payload_bytes = 128;

    mino::Result<SharedHostDomain> domain = mino::Status::Error(
        mino::StatusCode::kNotFound, "domain not opened");
    if (role == "pub") {
        domain = SharedHostDomain::Create(name, options);
    } else {
        const auto open_deadline =
            mino::Deadline::FromNow(std::chrono::seconds(20));
        while (!domain.ok()) {
            domain = SharedHostDomain::Open(name);
            if (domain.ok()) break;
            if (open_deadline.expired()) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }
    if (!domain.ok()) {
        std::cerr << "open/create failed: " << domain.status().ToString() << "\n";
        return 1;
    }
    if (!domain->Join(mino::NodeId{node_id}).ok()) {
        std::cerr << "join failed\n";
        return 1;
    }

    const auto schema = MakeSchema();
    if (role == "join-wait") {
        if (!WaitForPeerCount(&*domain, 2, std::chrono::seconds(20))) {
            std::cerr << "join-wait timed out\n";
            return 1;
        }
        auto peers = domain->ListPeers();
        const size_t count = peers.ok() ? peers->size() : 0;
        // Stay joined briefly so the existing peer's ListPeers can observe
        // this slot before destructor Leave() clears it.
        for (int i = 0; i < 50; ++i) {
            (void)domain->Heartbeat();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        std::cout << "{\"role\":\"join-wait\",\"peers\":" << count << "}\n";
        return 0;
    }

    if (role == "pub") {
        auto pub = domain->Advertise("stress", schema);
        if (!pub.ok()) {
            std::cerr << "advertise failed: " << pub.status().ToString() << "\n";
            return 1;
        }
        // Stay joined until a late joiner (or the paired subscriber) is
        // visible. A fixed 200ms settle races destructor Leave() under
        // debug/O0, so LateJoiner only saw itself.
        (void)WaitForPeerCount(&*domain, 2, std::chrono::seconds(20));
        for (uint64_t i = 0; i < messages; ++i) {
            const std::string payload = "msg-" + std::to_string(i);
            const mino::Status st = pub->Publish(
                std::as_bytes(std::span(payload.data(), payload.size())));
            if (!st.ok()) {
                std::cerr << "publish failed: " << st.ToString() << "\n";
                return 1;
            }
        }
        std::cout << "{\"role\":\"pub\",\"sent\":" << messages << "}\n";
        return 0;
    }

    if (role == "sub") {
        auto sub = domain->Subscribe(
            "stress", schema, mino::Deadline::FromNow(std::chrono::seconds(5)));
        if (!sub.ok()) {
            std::cerr << "subscribe failed: " << sub.status().ToString() << "\n";
            return 1;
        }
        // Settle after Subscribe so the publisher's attach/Recover path observes
        // a live registration+heartbeat before the first Publish.
        for (int i = 0; i < 10; ++i) {
            (void)domain->Heartbeat();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        uint64_t got = 0;
        while (got < messages) {
            (void)domain->Heartbeat();
            auto message =
                sub->Poll(mino::Deadline::FromNow(std::chrono::seconds(5)));
            if (!message.ok()) {
                std::cerr << "poll failed: " << message.status().ToString() << "\n";
                return 1;
            }
            ++got;
        }
        std::cout << "{\"role\":\"sub\",\"received\":" << got << "}\n";
        return 0;
    }

    std::cerr << "unknown role\n";
    return 2;
}
