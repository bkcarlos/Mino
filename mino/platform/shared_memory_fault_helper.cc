// Copyright 2026 The Mino Authors
// SPDX-License-Identifier: LGPL-3.0-only

#include "mino/platform/shared_memory.h"
#include "mino/platform/shared_memory_marker.h"

#include <csignal>
#include <cstdlib>
#include <string>

#include <unistd.h>

namespace {

using mino::shared_memory_internal::SharedMemoryTestPoint;

std::string g_action;
int g_publication_count = 0;

void FaultHook(SharedMemoryTestPoint point) {
    if (g_action == "stop-after-marker" &&
        point == SharedMemoryTestPoint::kAfterCreatingMarker) {
        ::raise(SIGSTOP);
        return;
    }
    if (g_action == "crash-after-marker" &&
        point == SharedMemoryTestPoint::kAfterCreatingMarker) {
        ::_exit(70);
    }
    if (g_action == "crash-before-identity-publication" &&
        point == SharedMemoryTestPoint::kBeforeMarkerPublication &&
        ++g_publication_count == 2) {
        ::_exit(70);
    }
    if (g_action == "crash-after-backing" &&
        point == SharedMemoryTestPoint::kAfterBackingIdentityRecorded) {
        ::_exit(70);
    }
    if (g_action == "crash-after-unlinking" &&
        point == SharedMemoryTestPoint::kAfterUnlinkingPublished) {
        ::_exit(70);
    }
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 3) return 64;
    g_action = argv[1];
    const std::string name = argv[2];
    mino::shared_memory_internal::SetSharedMemoryTestHook(&FaultHook);
    if (g_action == "crash-after-unlinking") {
        mino::Status status = mino::SharedMemorySegment::Unlink(name);
        return status.ok() ? 0 : 72;
    }
    mino::SharedMemoryCreateOptions options;
    options.name = name;
    options.size = 8192;
    auto created = mino::SharedMemorySegment::Create(options);
    return created.ok() ? 0 : 71;
}
