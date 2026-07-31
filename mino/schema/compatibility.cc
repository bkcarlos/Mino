// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.

#include "mino/schema/compatibility.h"

#include <map>
#include <new>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>

#include "mino/common/status.h"

namespace mino::schema {
namespace {

enum class Direction { kNeutral, kRead, kWrite, kIncompatible };

Direction Combine(Direction lhs, Direction rhs) {
    if (lhs == Direction::kIncompatible || rhs == Direction::kIncompatible) {
        return Direction::kIncompatible;
    }
    if (lhs == Direction::kNeutral) return rhs;
    if (rhs == Direction::kNeutral || lhs == rhs) return lhs;
    return Direction::kIncompatible;
}

Direction CompatibilityDirection(Compatibility compatibility) {
    switch (compatibility) {
        case Compatibility::kIdentical:
        case Compatibility::kWireCompatible:
            return Direction::kNeutral;
        case Compatibility::kReadCompatible:
            return Direction::kRead;
        case Compatibility::kWriteCompatible:
            return Direction::kWrite;
        case Compatibility::kRequiresTranslation:
        case Compatibility::kIncompatible:
            return Direction::kIncompatible;
    }
    return Direction::kIncompatible;
}

Compatibility DirectionCompatibility(Direction direction) {
    switch (direction) {
        case Direction::kNeutral:
            return Compatibility::kWireCompatible;
        case Direction::kRead:
            return Compatibility::kReadCompatible;
        case Direction::kWrite:
            return Compatibility::kWriteCompatible;
        case Direction::kIncompatible:
            return Compatibility::kIncompatible;
    }
    return Compatibility::kIncompatible;
}

bool SameType(const TypeDescriptor& lhs, const TypeDescriptor& rhs) {
    if (lhs.kind() != rhs.kind() || lhs.scalar() != rhs.scalar() ||
        lhs.name() != rhs.name()) {
        return false;
    }
    if (lhs.kind() != TypeDescriptor::Kind::kVector) return true;
    return lhs.element_type() != nullptr && rhs.element_type() != nullptr &&
           SameType(*lhs.element_type(), *rhs.element_type());
}

bool SameDefault(const std::optional<DefaultValue>& lhs,
                 const std::optional<DefaultValue>& rhs) {
    if (lhs.has_value() != rhs.has_value()) return false;
    return !lhs.has_value() ||
           (lhs->kind() == rhs->kind() &&
            lhs->canonical_value() == rhs->canonical_value());
}

Direction CompareLimit(std::optional<uint64_t> from,
                       std::optional<uint64_t> to) {
    if (from == to) return Direction::kNeutral;
    if (!from.has_value()) return Direction::kWrite;
    if (!to.has_value()) return Direction::kRead;
    return *to < *from ? Direction::kWrite : Direction::kRead;
}

void CollectUserTypes(const TypeDescriptor& type,
                      std::set<std::string, std::less<>>& names) {
    if (type.kind() == TypeDescriptor::Kind::kUserDefined) {
        names.emplace(type.name());
    } else if (type.kind() == TypeDescriptor::Kind::kVector &&
               type.element_type() != nullptr) {
        CollectUserTypes(*type.element_type(), names);
    }
}

using DependencyMap =
    std::map<std::string, CanonicalDigest, std::less<>>;
using DigestPair = std::pair<CanonicalDigest, CanonicalDigest>;

DependencyMap MakeDependencyMap(const SchemaDescriptor& descriptor) {
    DependencyMap result;
    for (const DependencyDescriptor& dependency : descriptor.dependencies()) {
        result.emplace(std::string(dependency.full_name()), dependency.digest());
    }
    return result;
}

class Checker {
public:
    Checker(const SchemaDescriptor& from, const SchemaDescriptor& to,
            std::span<const std::shared_ptr<const SchemaDescriptor>> closure)
        : from_(from), to_(to) {
        descriptors_.emplace(from.identity().canonical_digest(), &from);
        descriptors_.emplace(to.identity().canonical_digest(), &to);
        for (const auto& descriptor : closure) {
            if (descriptor != nullptr) {
                descriptors_.emplace(descriptor->identity().canonical_digest(),
                                     descriptor.get());
            }
        }
    }

    Result<Compatibility> Run() { return CheckPair(from_, to_); }

private:
    Result<Direction> CompareMatchedField(
        const FieldDescriptor& from, const FieldDescriptor& to,
        const DependencyMap& from_dependencies,
        const DependencyMap& to_dependencies) {
        if (!SameType(from.type(), to.type()) ||
            from.cardinality() != to.cardinality() ||
            !SameDefault(from.default_value(), to.default_value()) ||
            from.constraints().snapshot_key() !=
                to.constraints().snapshot_key()) {
            return Direction::kIncompatible;
        }

        Direction result = CompareLimit(from.constraints().max_bytes(),
                                        to.constraints().max_bytes());
        result = Combine(result,
                         CompareLimit(from.constraints().max_capacity(),
                                      to.constraints().max_capacity()));

        std::set<std::string, std::less<>> user_types;
        CollectUserTypes(from.type(), user_types);
        for (const std::string& name : user_types) {
            const auto old_dependency = from_dependencies.find(name);
            const auto new_dependency = to_dependencies.find(name);
            if (old_dependency == from_dependencies.end() ||
                new_dependency == to_dependencies.end()) {
                return Direction::kIncompatible;
            }
            if (old_dependency->second == new_dependency->second) continue;

            const auto old_descriptor =
                descriptors_.find(old_dependency->second);
            const auto new_descriptor =
                descriptors_.find(new_dependency->second);
            if (old_descriptor == descriptors_.end() ||
                new_descriptor == descriptors_.end() ||
                old_descriptor->second->aggregate().full_name() != name ||
                new_descriptor->second->aggregate().full_name() != name) {
                return Direction::kIncompatible;
            }
            auto nested =
                CheckPair(*old_descriptor->second, *new_descriptor->second);
            if (!nested.ok()) return nested.status();
            result = Combine(result, CompatibilityDirection(*nested));
            if (result == Direction::kIncompatible) return result;
        }
        return result;
    }

    Result<Compatibility> CheckPair(const SchemaDescriptor& from,
                                    const SchemaDescriptor& to) {
        if (from.identity().canonical_digest() ==
            to.identity().canonical_digest()) {
            return Compatibility::kIdentical;
        }
        const DigestPair key{from.identity().canonical_digest(),
                             to.identity().canonical_digest()};
        const auto memoized = memo_.find(key);
        if (memoized != memo_.end()) return memoized->second;
        if (!active_.insert(key).second) {
            return Compatibility::kIncompatible;
        }
        if (from.aggregate().full_name() != to.aggregate().full_name() ||
            from.aggregate().kind() != to.aggregate().kind()) {
            return Finish(key, Compatibility::kIncompatible);
        }

        const DependencyMap from_dependencies = MakeDependencyMap(from);
        const DependencyMap to_dependencies = MakeDependencyMap(to);
        Direction direction = Direction::kNeutral;
        size_t from_index = 0;
        size_t to_index = 0;
        const auto from_fields = from.aggregate().fields();
        const auto to_fields = to.aggregate().fields();
        while (from_index < from_fields.size() ||
               to_index < to_fields.size()) {
            if (from_index == from_fields.size()) {
                if (to_fields[to_index].cardinality() !=
                    FieldCardinality::kOptional) {
                    return Finish(key, Compatibility::kIncompatible);
                }
                ++to_index;
                continue;
            }
            if (to_index == to_fields.size()) {
                const FieldDescriptor& removed = from_fields[from_index];
                if (removed.cardinality() != FieldCardinality::kOptional ||
                    !to.aggregate().IsReserved(removed.id())) {
                    return Finish(key, Compatibility::kIncompatible);
                }
                ++from_index;
                continue;
            }
            const FieldDescriptor& old_field = from_fields[from_index];
            const FieldDescriptor& new_field = to_fields[to_index];
            if (old_field.id() < new_field.id()) {
                if (old_field.cardinality() != FieldCardinality::kOptional ||
                    !to.aggregate().IsReserved(old_field.id())) {
                    return Finish(key, Compatibility::kIncompatible);
                }
                ++from_index;
            } else if (new_field.id() < old_field.id()) {
                if (new_field.cardinality() != FieldCardinality::kOptional ||
                    from.aggregate().IsReserved(new_field.id())) {
                    return Finish(key, Compatibility::kIncompatible);
                }
                ++to_index;
            } else {
                auto field_direction = CompareMatchedField(
                    old_field, new_field, from_dependencies, to_dependencies);
                if (!field_direction.ok()) {
                    active_.erase(key);
                    return field_direction.status();
                }
                direction = Combine(direction, *field_direction);
                if (direction == Direction::kIncompatible) {
                    return Finish(key, Compatibility::kIncompatible);
                }
                ++from_index;
                ++to_index;
            }
        }
        return Finish(key, DirectionCompatibility(direction));
    }

    Compatibility Finish(const DigestPair& key, Compatibility compatibility) {
        active_.erase(key);
        memo_.emplace(key, compatibility);
        return compatibility;
    }

    const SchemaDescriptor& from_;
    const SchemaDescriptor& to_;
    std::map<CanonicalDigest, const SchemaDescriptor*> descriptors_;
    std::map<DigestPair, Compatibility> memo_;
    std::set<DigestPair> active_;
};

Result<Compatibility> CheckImpl(
    const SchemaDescriptor& from, const SchemaDescriptor& to,
    std::span<const std::shared_ptr<const SchemaDescriptor>> closure) noexcept {
    try {
        return Checker(from, to, closure).Run();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

}  // namespace

Result<Compatibility> CompatibilityChecker::Check(
    const SchemaDescriptor& from, const SchemaDescriptor& to) noexcept {
    return CheckImpl(from, to, {});
}

Result<Compatibility> CompatibilityChecker::Check(
    const SchemaDescriptor& from, const SchemaDescriptor& to,
    std::span<const std::shared_ptr<const SchemaDescriptor>>
        descriptor_closure) noexcept {
    return CheckImpl(from, to, descriptor_closure);
}

}  // namespace mino::schema
