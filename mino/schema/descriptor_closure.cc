// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/schema/descriptor_closure.h"

#include <map>
#include <new>
#include <set>
#include <string>

#include "mino/schema/canonical.h"

namespace mino::schema {

Status ValidateDescriptorClosure(
    const SchemaDescriptor& root,
    std::span<const std::shared_ptr<const SchemaDescriptor>> descriptors)
    noexcept {
    try {
        using DigestMap = std::map<std::string, CanonicalDigest, std::less<>>;
        DigestMap expected;
        expected.emplace(std::string(root.aggregate().full_name()),
                         root.identity().canonical_digest());
        for (const DependencyDescriptor& dependency : root.dependencies()) {
            const auto [it, inserted] = expected.emplace(
                std::string(dependency.full_name()), dependency.digest());
            if (!inserted && it->second != dependency.digest()) {
                return Status::Error(StatusCode::kSchemaMismatch,
                                     "root dependency closure is inconsistent");
            }
        }

        std::map<std::string, const SchemaDescriptor*, std::less<>> actual;
        std::set<std::string, std::less<>> supplied_names;
        const auto add = [&](const SchemaDescriptor& descriptor) -> Status {
            auto canonical = Canonicalizer::Canonicalize(
                descriptor.aggregate(), descriptor.dependencies());
            if (!canonical.ok()) return canonical.status();
            if (canonical->text() != descriptor.canonical_schema() ||
                canonical->digest() != descriptor.identity().canonical_digest() ||
                canonical->short_id() != descriptor.identity().short_id() ||
                DigestShortId(descriptor.identity().canonical_digest()) !=
                    descriptor.identity().short_id()) {
                return Status::Error(
                    StatusCode::kSchemaMismatch,
                    "descriptor is not an authenticated canonical capability");
            }
            const std::string name(descriptor.aggregate().full_name());
            const auto wanted = expected.find(name);
            if (wanted == expected.end() ||
                wanted->second != descriptor.identity().canonical_digest()) {
                return Status::Error(StatusCode::kSchemaMismatch,
                                     "descriptor closure contains an extra or mismatched type");
            }
            const auto [it, inserted] = actual.emplace(name, &descriptor);
            if (!inserted &&
                it->second->identity().canonical_digest() !=
                    descriptor.identity().canonical_digest()) {
                return Status::Error(StatusCode::kSchemaMismatch,
                                     "descriptor closure contains a name collision");
            }
            for (const auto& [unused, existing] : actual) {
                (void)unused;
                if (existing != &descriptor &&
                    existing->identity().short_id() ==
                        descriptor.identity().short_id() &&
                    existing->identity().canonical_digest() !=
                        descriptor.identity().canonical_digest()) {
                    return Status::Error(StatusCode::kSchemaMismatch,
                                         "descriptor closure contains a short ID collision");
                }
            }
            return Status::Ok();
        };

        MINO_RETURN_IF_ERROR(add(root));
        for (const auto& descriptor : descriptors) {
            if (descriptor == nullptr) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "descriptor closure contains null");
            }
            const std::string name(descriptor->aggregate().full_name());
            if (!supplied_names.emplace(name).second) {
                return Status::Error(StatusCode::kSchemaMismatch,
                                     "descriptor closure contains a duplicate name");
            }
            MINO_RETURN_IF_ERROR(add(*descriptor));
        }
        if (actual.size() != expected.size()) {
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "descriptor closure is missing a dependency");
        }
        for (const auto& [name, descriptor] : actual) {
            (void)name;
            for (const DependencyDescriptor& dependency :
                 descriptor->dependencies()) {
                const auto found = actual.find(dependency.full_name());
                if (found == actual.end() ||
                    found->second->identity().canonical_digest() !=
                        dependency.digest()) {
                    return Status::Error(
                        StatusCode::kSchemaMismatch,
                        "descriptor dependency name/digest is not in closure");
                }
            }
        }
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

}  // namespace mino::schema
