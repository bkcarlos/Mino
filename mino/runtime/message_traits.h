// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_RUNTIME_MESSAGE_TRAITS_H_
#define MINO_RUNTIME_MESSAGE_TRAITS_H_

#include <cstdint>

#include "mino/common/ids.h"
#include "mino/common/status.h"

namespace mino {

// D2 bridge to the D3 static CodeGen contract. Applications may specialize
// this trait for fixed-layout SHM messages today; D3-generated types will emit
// equivalent specializations. Runtime intentionally does not invent a partial
// Schema Registry here.
template <typename T>
struct StaticMessageTraits {
    static constexpr bool kIsSpecialized = false;
};

// Documents the required shape without forcing a C++ concept into every public
// signature. Publisher<T> and Subscriber<T> validate this flag and the fixed
// layout constraints with static_asserts.
template <typename T>
inline constexpr bool kHasStaticMessageTraits =
    StaticMessageTraits<T>::kIsSpecialized;

}  // namespace mino

#endif  // MINO_RUNTIME_MESSAGE_TRAITS_H_
