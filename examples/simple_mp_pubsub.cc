// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/runtime/simple_node.h"

#include <chrono>
#include <cstring>
#include <iostream>
#include <string>
#include <span>
#include <string_view>
#include <thread>

namespace {

void Usage() {
    std::cerr << "usage: simple_mp_pubsub pub|sub [shm-name]\n"
              << "  pub  creates /mino_demo (or shm-name) and publishes 8 frames\n"
              << "  sub  opens the same object and prints borrowed payloads\n";
}

int RunPub(std::string_view name) {
    (void)mino::SimpleNode::Unlink(name);
    mino::SimpleNodeOptions options;
    options.topic_slots = 4;
    options.queue_depth = 32;
    options.max_payload_bytes = 256;
    auto created = mino::SimpleNode::Create(name, options);
    if (!created.ok()) {
        std::cerr << "Create failed: " << created.status().ToString() << "\n";
        return 1;
    }
    std::cerr << "segment " << created->size_bytes() << " bytes at " << name
              << "\n";
    auto pub = created->Advertise("camera");
    if (!pub.ok()) {
        std::cerr << "Advertise failed: " << pub.status().ToString() << "\n";
        return 1;
    }
    for (int i = 0; i < 8; ++i) {
        const std::string payload = "frame-" + std::to_string(i);
        const mino::Status status = pub->Publish(std::as_bytes(std::span{
            reinterpret_cast<const std::byte*>(payload.data()),
            payload.size()}));
        if (!status.ok()) {
            std::cerr << "Publish failed: " << status.ToString() << "\n";
            return 1;
        }
        std::cout << "published " << payload << "\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return 0;
}

int RunSub(std::string_view name) {
    mino::Result<mino::SimpleNode> opened = mino::Status::Error(
        mino::StatusCode::kNotFound, "not opened");
    const auto deadline =
        mino::Deadline::FromNow(std::chrono::seconds(8));
    while (!opened.ok()) {
        opened = mino::SimpleNode::Open(name);
        if (opened.ok()) break;
        if (deadline.expired()) {
            std::cerr << "Open failed: " << opened.status().ToString() << "\n";
            return 1;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    auto sub = opened->Subscribe("camera");
    if (!sub.ok()) {
        std::cerr << "Subscribe failed: " << sub.status().ToString() << "\n";
        return 1;
    }
    for (int i = 0; i < 8; ++i) {
        auto message = sub->Poll(mino::Deadline::FromNow(std::chrono::seconds(5)));
        if (!message.ok()) {
            std::cerr << "Poll failed: " << message.status().ToString() << "\n";
            return 1;
        }
        const auto bytes = message->bytes();
        std::cout.write(reinterpret_cast<const char*>(bytes.data()),
                        static_cast<std::streamsize>(bytes.size()));
        std::cout << "\n";
        const mino::Status released = std::move(*message).Release();
        if (!released.ok()) {
            std::cerr << "Release failed: " << released.ToString() << "\n";
            return 1;
        }
    }
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2 || argv[1] == nullptr) {
        Usage();
        return 2;
    }
    const std::string_view mode(argv[1]);
    const std::string_view name = argc >= 3 && argv[2] != nullptr
                                      ? std::string_view(argv[2])
                                      : std::string_view("/mino_demo");
    if (mode == "pub") return RunPub(name);
    if (mode == "sub") return RunSub(name);
    Usage();
    return 2;
}
