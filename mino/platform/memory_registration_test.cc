// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/platform/memory_registration.h"

#include <gtest/gtest.h>

namespace mino {
namespace {

TEST(MemoryRegistrationTest, BuiltinProviderFailsClosedWithoutHardware) {
    MemoryRegistrationProvider& provider =
        UnavailableMemoryRegistrationProvider();
    EXPECT_EQ(provider.provider_class(),
              MemoryRegistrationProviderClass::kUnavailable);
    EXPECT_FALSE(provider.Supports(MemoryRegistrationKind::kDma));
    EXPECT_FALSE(provider.Supports(MemoryRegistrationKind::kRdma));
    EXPECT_EQ(provider.Register({.address = reinterpret_cast<void*>(0x1000),
                             .bytes = 4096,
                             .alignment = 4096,
                             .scope_id = 1,
                             .kind = MemoryRegistrationKind::kRdma,
                             .owner = {.process_id = 1,
                                       .process_epoch = 1,
                                       .lease_id = 1},
                             .require_physical_contiguous = false})
                  .status()
                  .code(),
              StatusCode::kUnsupported);
    EXPECT_EQ(provider.Deregister({.registration_id = 1,
                               .bytes = 4096,
                               .device_key = 0,
                               .kind = MemoryRegistrationKind::kRdma,
                               .owner = {.process_id = 1,
                                         .process_epoch = 1,
                                         .lease_id = 1},
                               .physically_contiguous = false})
                  .code(),
              StatusCode::kUnsupported);
}

}  // namespace
}  // namespace mino
