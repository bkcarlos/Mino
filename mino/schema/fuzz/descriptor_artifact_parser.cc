// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/schema/fuzz/descriptor_artifact_parser.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <span>
#include <string_view>

namespace mino::schema::fuzz::internal {
namespace {

Status Malformed() {
    return Status::Error(StatusCode::kInvalidArgument,
                         "malformed codegen descriptor artifact");
}

Status Resource() {
    return Status::Error(StatusCode::kResourceExhausted,
                         "codegen descriptor artifact exceeds fuzz limits");
}

class LineReader {
public:
    LineReader(std::string_view input, size_t max_line_bytes) noexcept
        : input_(input), max_line_bytes_(max_line_bytes) {}

    Status Next(std::string_view& line) noexcept {
        if (offset_ >= input_.size()) return Malformed();
        const size_t end = input_.find('\n', offset_);
        const size_t length = end == std::string_view::npos
                                  ? input_.size() - offset_
                                  : end - offset_;
        if (length > max_line_bytes_) return Resource();
        line = input_.substr(offset_, length);
        if (line.find('\r') != std::string_view::npos ||
            line.find('\0') != std::string_view::npos) {
            return Malformed();
        }
        offset_ = end == std::string_view::npos ? input_.size() : end + 1;
        return Status::Ok();
    }

    bool done() const noexcept { return offset_ == input_.size(); }

private:
    std::string_view input_;
    size_t max_line_bytes_ = 0;
    size_t offset_ = 0;
};

Status ParseDecimal(std::string_view text, uint64_t& value) noexcept {
    if (text.empty()) return Malformed();
    if (text.size() > 1 && text.front() == '0') return Malformed();
    uint64_t parsed = 0;
    for (char ch : text) {
        if (ch < '0' || ch > '9') return Malformed();
        const uint64_t digit = static_cast<uint64_t>(ch - '0');
        if (parsed > (std::numeric_limits<uint64_t>::max() - digit) / 10) {
            return Resource();
        }
        parsed = parsed * 10 + digit;
    }
    value = parsed;
    return Status::Ok();
}

Status ParsePrefixedDecimal(std::string_view line, std::string_view prefix,
                            uint64_t& value) noexcept {
    if (!line.starts_with(prefix)) return Malformed();
    return ParseDecimal(line.substr(prefix.size()), value);
}

Status ParseLengthDelimited(std::string_view text, size_t max_bytes,
                            std::string_view& value) noexcept {
    const size_t colon = text.find(':');
    if (colon == std::string_view::npos) return Malformed();
    uint64_t length = 0;
    Status status = ParseDecimal(text.substr(0, colon), length);
    if (!status.ok()) return status;
    if (length > max_bytes || length > std::numeric_limits<size_t>::max()) {
        return Resource();
    }
    const size_t size = static_cast<size_t>(length);
    if (text.size() - colon - 1 != size) return Malformed();
    value = text.substr(colon + 1, size);
    if (value.empty()) return Malformed();
    return Status::Ok();
}

int HexNibble(char ch) noexcept {
    if (ch >= '0' && ch <= '9') return ch - '0';
    if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
    return -1;
}

Status ParseDigest(std::string_view text,
                   std::array<uint8_t, 8>* prefix = nullptr) noexcept {
    if (text.size() != 64) return Malformed();
    for (size_t i = 0; i < 32; ++i) {
        const int high = HexNibble(text[i * 2]);
        const int low = HexNibble(text[i * 2 + 1]);
        if (high < 0 || low < 0) return Malformed();
        if (prefix != nullptr && i < prefix->size()) {
            (*prefix)[i] = static_cast<uint8_t>((high << 4) | low);
        }
    }
    return Status::Ok();
}

Status TakeToken(std::string_view& input, std::string_view prefix,
                 std::string_view& value) noexcept {
    if (!input.starts_with(prefix)) return Malformed();
    input.remove_prefix(prefix.size());
    const size_t space = input.find(' ');
    value = input.substr(0, space);
    input.remove_prefix(space == std::string_view::npos ? input.size()
                                                        : space);
    if (value.empty()) return Malformed();
    return Status::Ok();
}

Status ParseOptionalDecimal(std::string_view text, uint64_t limit) noexcept {
    if (text == "-") return Status::Ok();
    uint64_t value = 0;
    Status status = ParseDecimal(text, value);
    if (!status.ok()) return status;
    return value <= limit ? Status::Ok() : Resource();
}

bool IsScalar(std::string_view type) noexcept {
    return type == "int32" || type == "int64" || type == "uint32" ||
           type == "uint64" || type == "fixed32" || type == "fixed64" ||
           type == "float" || type == "double" || type == "bool" ||
           type == "string" || type == "bytes";
}

Status ValidateType(std::string_view type, size_t depth,
                    const DescriptorArtifactLimits& limits) noexcept {
    if (depth > limits.max_type_depth) return Resource();
    if (IsScalar(type)) return Status::Ok();
    if (type.starts_with("vector<") && type.ends_with('>')) {
        return ValidateType(type.substr(7, type.size() - 8), depth + 1, limits);
    }
    if (type.starts_with("type(")) {
        const size_t close = type.find("):", 5);
        if (close == std::string_view::npos) return Malformed();
        uint64_t length = 0;
        Status status = ParseDecimal(type.substr(5, close - 5), length);
        if (!status.ok()) return status;
        const std::string_view name = type.substr(close + 2);
        if (length != name.size() || name.empty()) return Malformed();
        return length <= limits.max_name_bytes ? Status::Ok() : Resource();
    }
    return Malformed();
}

Status ParseLayout(std::string_view line,
                   const DescriptorArtifactLimits& limits) noexcept {
    std::string_view rest = line;
    constexpr std::array<std::string_view, 9> kKeys = {
        "layout header_size=",       " presence_offset=",
        " presence_words=",          " fixed_offset=",
        " fixed_size=",              " object_size=",
        " alignment=",               " max_child_bytes=",
        " max_dynamic_children=",
    };
    for (size_t i = 0; i < kKeys.size(); ++i) {
        std::string_view token;
        Status status = TakeToken(rest, kKeys[i], token);
        if (!status.ok()) return status;
        uint64_t value = 0;
        status = ParseDecimal(token, value);
        if (!status.ok()) return status;
        if ((i <= 6 && value > limits.max_object_bytes) ||
            (i == 7 && value > limits.max_child_bytes) ||
            (i == 8 && value > limits.max_dynamic_children)) {
            return Resource();
        }
    }
    return rest.empty() ? Status::Ok() : Malformed();
}

Status ParseDependency(std::string_view line,
                       const DescriptorArtifactLimits& limits) noexcept {
    constexpr std::string_view kPrefix = "dependency ";
    if (!line.starts_with(kPrefix)) return Malformed();
    std::string_view rest = line.substr(kPrefix.size());
    const size_t colon = rest.find(':');
    if (colon == std::string_view::npos) return Malformed();
    uint64_t length = 0;
    Status status = ParseDecimal(rest.substr(0, colon), length);
    if (!status.ok()) return status;
    if (length == 0 || length > limits.max_name_bytes ||
        length > std::numeric_limits<size_t>::max()) {
        return length == 0 ? Malformed() : Resource();
    }
    const size_t name_size = static_cast<size_t>(length);
    if (rest.size() < colon + 1 + name_size + 1) return Malformed();
    const size_t separator = colon + 1 + name_size;
    if (rest[separator] != ' ') return Malformed();
    return ParseDigest(rest.substr(separator + 1));
}

Status ParseField(std::string_view line, uint32_t& previous_id,
                  const DescriptorArtifactLimits& limits) noexcept {
    std::string_view rest = line;
    std::string_view token;
    Status status = TakeToken(rest, "field id=", token);
    if (!status.ok()) return status;
    uint64_t id = 0;
    status = ParseDecimal(token, id);
    if (!status.ok()) return status;
    if (id == 0 || id > 536870911u || id <= previous_id) return Malformed();
    previous_id = static_cast<uint32_t>(id);

    if (!rest.starts_with(" name=")) return Malformed();
    rest.remove_prefix(6);
    const size_t colon = rest.find(':');
    if (colon == std::string_view::npos) return Malformed();
    uint64_t name_length = 0;
    status = ParseDecimal(rest.substr(0, colon), name_length);
    if (!status.ok()) return status;
    if (name_length == 0 || name_length > limits.max_name_bytes ||
        name_length > std::numeric_limits<size_t>::max()) {
        return name_length == 0 ? Malformed() : Resource();
    }
    const size_t name_size = static_cast<size_t>(name_length);
    if (rest.size() < colon + 1 + name_size) return Malformed();
    rest.remove_prefix(colon + 1 + name_size);

    status = TakeToken(rest, " cardinality=", token);
    if (!status.ok()) return status;
    if (token != "unspecified" && token != "optional" && token != "required") {
        return Malformed();
    }
    status = TakeToken(rest, " type=", token);
    if (!status.ok()) return status;
    status = ValidateType(token, 0, limits);
    if (!status.ok()) return status;

    constexpr std::array<std::string_view, 3> kNumericKeys = {
        " offset=", " size=", " alignment="};
    for (std::string_view key : kNumericKeys) {
        status = TakeToken(rest, key, token);
        if (!status.ok()) return status;
        uint64_t value = 0;
        status = ParseDecimal(token, value);
        if (!status.ok()) return status;
        if (value > limits.max_object_bytes) return Resource();
    }

    status = TakeToken(rest, " storage=", token);
    if (!status.ok()) return status;
    if (token != "scalar" && token != "inline_struct" && token != "variable") {
        return Malformed();
    }
    status = TakeToken(rest, " presence=", token);
    if (!status.ok()) return status;
    status = ParseOptionalDecimal(token, limits.max_fields_per_type);
    if (!status.ok()) return status;
    status = TakeToken(rest, " max_bytes=", token);
    if (!status.ok()) return status;
    status = ParseOptionalDecimal(token, limits.max_child_bytes);
    if (!status.ok()) return status;
    status = TakeToken(rest, " max_capacity=", token);
    if (!status.ok()) return status;
    status = ParseOptionalDecimal(token, limits.max_dynamic_children);
    if (!status.ok()) return status;
    return rest.empty() ? Status::Ok() : Malformed();
}

Status ExpectLine(LineReader& reader, std::string_view expected) noexcept {
    std::string_view line;
    Status status = reader.Next(line);
    if (!status.ok()) return status;
    return line == expected ? Status::Ok() : Malformed();
}

}  // namespace

Status ValidateCodegenDescriptorArtifact(
    std::span<const std::byte> input,
    const DescriptorArtifactLimits& limits) noexcept {
    try {
        if (input.size() > limits.max_input_bytes) return Resource();
        if (limits.max_line_bytes == 0 || limits.max_types == 0 ||
            limits.max_fields_per_type == 0 || limits.max_name_bytes == 0 ||
            limits.max_type_depth == 0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "descriptor artifact limits must be non-zero");
        }
        const char* chars = input.empty()
                                ? ""
                                : reinterpret_cast<const char*>(input.data());
        LineReader reader(std::string_view(chars, input.size()),
                          limits.max_line_bytes);
        Status status = ExpectLine(reader, "mino-descriptor-v1");
        if (!status.ok()) return status;
        status = ExpectLine(reader, "encoding length-delimited-utf8-decimal");
        if (!status.ok()) return status;

        std::string_view line;
        status = reader.Next(line);
        if (!status.ok()) return status;
        uint64_t type_count = 0;
        status = ParsePrefixedDecimal(line, "type_count ", type_count);
        if (!status.ok()) return status;
        if (type_count == 0) return Malformed();
        if (type_count > limits.max_types) return Resource();

        for (uint64_t type_index = 0; type_index < type_count; ++type_index) {
            status = reader.Next(line);
            if (!status.ok()) return status;
            if (!line.starts_with("type ")) return Malformed();
            std::string_view type_name;
            status = ParseLengthDelimited(line.substr(5), limits.max_name_bytes,
                                          type_name);
            if (!status.ok()) return status;
            static_cast<void>(type_name);

            status = reader.Next(line);
            if (!status.ok()) return status;
            if (line != "kind message" && line != "kind struct") {
                return Malformed();
            }

            status = reader.Next(line);
            if (!status.ok() || !line.starts_with("digest ")) {
                return status.ok() ? Malformed() : status;
            }
            std::array<uint8_t, 8> digest_prefix{};
            status = ParseDigest(line.substr(7), &digest_prefix);
            if (!status.ok()) return status;

            status = reader.Next(line);
            if (!status.ok()) return status;
            uint64_t short_id = 0;
            status = ParsePrefixedDecimal(line, "short_id ", short_id);
            if (!status.ok()) return status;
            uint64_t expected_short_id = 0;
            for (size_t i = 0; i < digest_prefix.size(); ++i) {
                expected_short_id |= static_cast<uint64_t>(digest_prefix[i])
                                     << (i * 8);
            }
            if (short_id != expected_short_id) return Malformed();

            status = reader.Next(line);
            if (!status.ok()) return status;
            uint64_t schema_version = 0;
            status = ParsePrefixedDecimal(line, "schema_version ", schema_version);
            if (!status.ok()) return status;
            if (schema_version > std::numeric_limits<uint32_t>::max()) {
                return Resource();
            }

            status = reader.Next(line);
            if (!status.ok()) return status;
            uint64_t layout_version = 0;
            status = ParsePrefixedDecimal(line, "layout_version ", layout_version);
            if (!status.ok()) return status;
            if (layout_version > std::numeric_limits<uint32_t>::max()) {
                return Resource();
            }

            status = reader.Next(line);
            if (!status.ok()) return status;
            status = ParseLayout(line, limits);
            if (!status.ok()) return status;

            status = reader.Next(line);
            if (!status.ok()) return status;
            uint64_t dependency_count = 0;
            status = ParsePrefixedDecimal(line, "dependency_count ",
                                          dependency_count);
            if (!status.ok()) return status;
            if (dependency_count > limits.max_dependencies_per_type) {
                return Resource();
            }
            for (uint64_t i = 0; i < dependency_count; ++i) {
                status = reader.Next(line);
                if (!status.ok()) return status;
                status = ParseDependency(line, limits);
                if (!status.ok()) return status;
            }

            status = reader.Next(line);
            if (!status.ok()) return status;
            uint64_t field_count = 0;
            status = ParsePrefixedDecimal(line, "field_count ", field_count);
            if (!status.ok()) return status;
            if (field_count > limits.max_fields_per_type) return Resource();
            uint32_t previous_id = 0;
            for (uint64_t i = 0; i < field_count; ++i) {
                status = reader.Next(line);
                if (!status.ok()) return status;
                status = ParseField(line, previous_id, limits);
                if (!status.ok()) return status;
            }
            status = ExpectLine(reader, "end_type");
            if (!status.ok()) return status;
        }
        return reader.done() ? Status::Ok() : Malformed();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

}  // namespace mino::schema::fuzz::internal
