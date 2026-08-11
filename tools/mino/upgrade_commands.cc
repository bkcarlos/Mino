// Copyright 2026 The Mino Authors

#include "tools/mino/upgrade_commands.h"

#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "mino/common/status.h"
#include "mino/upgrade/manifest.h"
#include "mino/upgrade/orchestrator.h"

namespace mino::tools {
namespace {

using upgrade::RegionIdentity;
using upgrade::TopicBinding;
using upgrade::UpgradePlan;

uint64_t NowNs() noexcept {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

Status Invalid(std::string message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

Status CheckSecureInput(const std::filesystem::path& path) {
    struct stat state {};
    if (::lstat(path.c_str(), &state) != 0 || !S_ISREG(state.st_mode) ||
        state.st_nlink != 1 || state.st_uid != ::geteuid() ||
        (state.st_mode & (S_IWGRP | S_IWOTH)) != 0 || state.st_size <= 0 ||
        state.st_size > static_cast<off_t>(upgrade::kMaximumUpgradeFileBytes)) {
        return Status::Error(
            StatusCode::kPermissionDenied,
            "upgrade input must be a bounded, single-link regular file owned by this uid and not group/world writable");
    }
    return Status::Ok();
}

std::string Trim(std::string value) {
    const size_t begin = value.find_first_not_of(" \t\r\n");
    if (begin == std::string::npos) return {};
    const size_t end = value.find_last_not_of(" \t\r\n");
    return value.substr(begin, end - begin + 1);
}

std::vector<std::string> Split(std::string_view value, char separator) {
    std::vector<std::string> fields;
    size_t begin = 0;
    while (begin <= value.size()) {
        const size_t end = value.find(separator, begin);
        fields.push_back(Trim(std::string(value.substr(
            begin, end == std::string_view::npos ? value.size() - begin
                                                 : end - begin))));
        if (end == std::string_view::npos) break;
        begin = end + 1;
    }
    return fields;
}

template <typename T>
Result<T> Unsigned(std::string_view value, std::string_view field) {
    T parsed = 0;
    const auto [end, error] =
        std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (error != std::errc() || end != value.data() + value.size()) {
        return Invalid("invalid unsigned field '" + std::string(field) + "'");
    }
    return parsed;
}

Result<bool> Boolean(std::string_view value, std::string_view field) {
    if (value == "true" || value == "1") return true;
    if (value == "false" || value == "0") return false;
    return Invalid("invalid boolean field '" + std::string(field) + "'");
}

using Fields = std::map<std::string, std::vector<std::string>, std::less<>>;

Result<Fields> ParseFields(const std::filesystem::path& path) {
    MINO_RETURN_IF_ERROR(CheckSecureInput(path));
    std::ifstream input(path);
    if (!input) return Invalid("cannot open upgrade input");
    Fields fields;
    std::string line;
    uint64_t line_number = 0;
    while (std::getline(input, line)) {
        ++line_number;
        line = Trim(line);
        if (line.empty() || line.front() == '#') continue;
        const size_t equals = line.find('=');
        if (equals == std::string::npos) {
            return Invalid(path.string() + ":" + std::to_string(line_number) +
                           ": expected key=value");
        }
        std::string key = Trim(line.substr(0, equals));
        std::string value = Trim(line.substr(equals + 1));
        if (key.empty() || value.empty()) {
            return Invalid(path.string() + ":" + std::to_string(line_number) +
                           ": empty key or value");
        }
        fields[std::move(key)].push_back(std::move(value));
    }
    return fields;
}

Result<std::string> One(const Fields& fields, std::string_view key) {
    const auto found = fields.find(key);
    if (found == fields.end() || found->second.size() != 1) {
        return Invalid("upgrade input requires exactly one '" + std::string(key) + "'");
    }
    return found->second.front();
}

Result<schema::CanonicalDigest> Digest(std::string_view text,
                                       std::string_view field) {
    schema::CanonicalDigest digest{};
    if (text.size() != digest.size() * 2) {
        return Invalid("schema digest field '" + std::string(field) +
                       "' must contain 64 hexadecimal characters");
    }
    auto nibble = [](char value) -> int {
        if (value >= '0' && value <= '9') return value - '0';
        if (value >= 'a' && value <= 'f') return value - 'a' + 10;
        if (value >= 'A' && value <= 'F') return value - 'A' + 10;
        return -1;
    };
    for (size_t index = 0; index < digest.size(); ++index) {
        const int high = nibble(text[index * 2]);
        const int low = nibble(text[index * 2 + 1]);
        if (high < 0 || low < 0) {
            return Invalid("schema digest field '" + std::string(field) +
                           "' is not hexadecimal");
        }
        digest[index] = static_cast<std::byte>((high << 4) | low);
    }
    return digest;
}

Result<RegionIdentity> ParseRegion(std::string_view text,
                                   std::string_view field) {
    const std::vector<std::string> values = Split(text, ',');
    if (values.size() != 6) {
        return Invalid(std::string(field) +
                       " requires name,id,uuid_lo,uuid_hi,layout,security_domain");
    }
    RegionIdentity region;
    region.name = values[0];
    MINO_ASSIGN_OR_RETURN(region.region_id, Unsigned<uint32_t>(values[1], field));
    MINO_ASSIGN_OR_RETURN(region.uuid_lo, Unsigned<uint64_t>(values[2], field));
    MINO_ASSIGN_OR_RETURN(region.uuid_hi, Unsigned<uint64_t>(values[3], field));
    MINO_ASSIGN_OR_RETURN(region.layout_version,
                          Unsigned<uint16_t>(values[4], field));
    MINO_ASSIGN_OR_RETURN(region.security_domain.value,
                          Unsigned<uint64_t>(values[5], field));
    return region;
}

Result<UpgradePlan> ParsePlan(const std::filesystem::path& path) {
    MINO_ASSIGN_OR_RETURN(const Fields fields, ParseFields(path));
    UpgradePlan plan;
    MINO_ASSIGN_OR_RETURN(plan.operation_id, One(fields, "operation_id"));
    MINO_ASSIGN_OR_RETURN(plan.commit_token, One(fields, "commit_token"));
    MINO_ASSIGN_OR_RETURN(const std::string source_region,
                          One(fields, "source_region"));
    MINO_ASSIGN_OR_RETURN(plan.source_region,
                          ParseRegion(source_region, "source_region"));
    MINO_ASSIGN_OR_RETURN(const std::string target_region,
                          One(fields, "target_region"));
    MINO_ASSIGN_OR_RETURN(plan.target_region,
                          ParseRegion(target_region, "target_region"));
    MINO_ASSIGN_OR_RETURN(const std::string required_shm,
                          One(fields, "required_shm_bytes"));
    MINO_ASSIGN_OR_RETURN(plan.required_shm_bytes,
                          Unsigned<uint64_t>(required_shm, "required_shm_bytes"));
    MINO_ASSIGN_OR_RETURN(const std::string publisher_slots,
                          One(fields, "required_publisher_slots"));
    MINO_ASSIGN_OR_RETURN(
        plan.required_publisher_slots,
        Unsigned<uint32_t>(publisher_slots, "required_publisher_slots"));
    MINO_ASSIGN_OR_RETURN(const std::string subscriber_slots,
                          One(fields, "required_subscriber_slots"));
    MINO_ASSIGN_OR_RETURN(
        plan.required_subscriber_slots,
        Unsigned<uint32_t>(subscriber_slots, "required_subscriber_slots"));
    MINO_ASSIGN_OR_RETURN(const std::string samples,
                          One(fields, "minimum_observation_samples"));
    MINO_ASSIGN_OR_RETURN(
        plan.minimum_observation_samples,
        Unsigned<uint64_t>(samples, "minimum_observation_samples"));

    const auto topics = fields.find("topic");
    if (topics == fields.end()) return Invalid("upgrade plan requires topic entries");
    for (const std::string& encoded : topics->second) {
        const std::vector<std::string> values = Split(encoded, ',');
        if (values.size() != 20) {
            return Invalid(
                "topic requires 20 comma-separated identity/config/schema fields; see rolling_upgrade.md");
        }
        upgrade::TopicUpgrade topic;
        MINO_ASSIGN_OR_RETURN(topic.source.topic_id.value,
                              Unsigned<uint32_t>(values[0], "source_topic_id"));
        MINO_ASSIGN_OR_RETURN(topic.target.topic_id.value,
                              Unsigned<uint32_t>(values[1], "target_topic_id"));
        topic.source.name = values[2];
        topic.target.name = values[3];
        MINO_ASSIGN_OR_RETURN(topic.source.config_version,
                              Unsigned<uint64_t>(values[4], "source_config"));
        MINO_ASSIGN_OR_RETURN(topic.target.config_version,
                              Unsigned<uint64_t>(values[5], "target_config"));
        MINO_ASSIGN_OR_RETURN(topic.source.region_version,
                              Unsigned<uint64_t>(values[6], "source_region_version"));
        MINO_ASSIGN_OR_RETURN(topic.target.region_version,
                              Unsigned<uint64_t>(values[7], "target_region_version"));
        MINO_ASSIGN_OR_RETURN(topic.source.channel_version,
                              Unsigned<uint64_t>(values[8], "source_channel_version"));
        MINO_ASSIGN_OR_RETURN(topic.target.channel_version,
                              Unsigned<uint64_t>(values[9], "target_channel_version"));
        MINO_ASSIGN_OR_RETURN(topic.source.acl_version,
                              Unsigned<uint64_t>(values[10], "source_acl_version"));
        MINO_ASSIGN_OR_RETURN(topic.target.acl_version,
                              Unsigned<uint64_t>(values[11], "target_acl_version"));
        uint64_t source_short = 0;
        uint32_t source_schema = 0;
        uint32_t source_layout = 0;
        uint64_t target_short = 0;
        uint32_t target_schema = 0;
        uint32_t target_layout = 0;
        MINO_ASSIGN_OR_RETURN(source_short,
                              Unsigned<uint64_t>(values[12], "source_schema_short"));
        MINO_ASSIGN_OR_RETURN(source_schema,
                              Unsigned<uint32_t>(values[13], "source_schema_version"));
        MINO_ASSIGN_OR_RETURN(source_layout,
                              Unsigned<uint32_t>(values[14], "source_layout_version"));
        MINO_ASSIGN_OR_RETURN(target_short,
                              Unsigned<uint64_t>(values[15], "target_schema_short"));
        MINO_ASSIGN_OR_RETURN(target_schema,
                              Unsigned<uint32_t>(values[16], "target_schema_version"));
        MINO_ASSIGN_OR_RETURN(target_layout,
                              Unsigned<uint32_t>(values[17], "target_layout_version"));
        MINO_ASSIGN_OR_RETURN(const schema::CanonicalDigest source_digest,
                              Digest(values[18], "source_digest"));
        MINO_ASSIGN_OR_RETURN(const schema::CanonicalDigest target_digest,
                              Digest(values[19], "target_digest"));
        topic.source.schema = schema::SchemaIdentity(
            source_short, source_digest, source_schema, source_layout);
        topic.target.schema = schema::SchemaIdentity(
            target_short, target_digest, target_schema, target_layout);
        plan.topics.push_back(std::move(topic));
    }
    const auto grants = fields.find("topic_acl");
    if (grants == fields.end()) return Invalid("upgrade plan requires topic_acl entries");
    for (const std::string& encoded : grants->second) {
        const std::vector<std::string> values = Split(encoded, ',');
        if (values.size() != 4) {
            return Invalid("topic_acl requires topic_index,node_id,security_domain,permissions");
        }
        uint32_t topic_index = 0;
        registry::TopicAclEntry entry;
        MINO_ASSIGN_OR_RETURN(topic_index,
                              Unsigned<uint32_t>(values[0], "topic_index"));
        if (topic_index >= plan.topics.size()) {
            return Invalid("topic_acl index is outside the topic list");
        }
        MINO_ASSIGN_OR_RETURN(entry.node_id.value,
                              Unsigned<uint64_t>(values[1], "acl_node"));
        MINO_ASSIGN_OR_RETURN(entry.security_domain_id.value,
                              Unsigned<uint64_t>(values[2], "acl_domain"));
        MINO_ASSIGN_OR_RETURN(entry.permissions,
                              Unsigned<uint32_t>(values[3], "acl_permissions"));
        plan.topics[topic_index].source.acl.entries.push_back(entry);
        plan.topics[topic_index].target.acl.entries.push_back(entry);
    }
    MINO_RETURN_IF_ERROR(upgrade::ValidateUpgradePlan(plan));
    return plan;
}

Result<bool> BoolField(const Fields& fields, std::string_view key) {
    MINO_ASSIGN_OR_RETURN(const std::string encoded, One(fields, key));
    return Boolean(encoded, key);
}

Result<uint64_t> U64Field(const Fields& fields, std::string_view key) {
    MINO_ASSIGN_OR_RETURN(const std::string encoded, One(fields, key));
    return Unsigned<uint64_t>(encoded, key);
}

Result<uint32_t> U32Field(const Fields& fields, std::string_view key) {
    MINO_ASSIGN_OR_RETURN(const std::string encoded, One(fields, key));
    return Unsigned<uint32_t>(encoded, key);
}

struct Evidence {
    std::string commit_token;
    RegionIdentity target_region;
    bool prepared = false;
    bool target_topics_ready = false;
    std::vector<TopicBinding> target_topics;
    upgrade::TargetReadinessProof readiness;
    bool drain_ack = false;
    upgrade::DrainProof drain;
    bool cutover_ack = false;
    upgrade::CutoverObservation observation;
    bool commit_ack = false;
    bool rollback_ack = false;
    upgrade::SafeRollbackProof rollback;
};

Result<Evidence> ParseEvidence(const std::filesystem::path& path) {
    MINO_ASSIGN_OR_RETURN(const Fields fields, ParseFields(path));
    Evidence evidence;
    MINO_ASSIGN_OR_RETURN(evidence.commit_token, One(fields, "commit_token"));
    MINO_ASSIGN_OR_RETURN(const std::string target_region,
                          One(fields, "target_region"));
    MINO_ASSIGN_OR_RETURN(evidence.target_region,
                          ParseRegion(target_region, "target_region"));
    const auto target_topics = fields.find("target_topic");
    if (target_topics == fields.end() || target_topics->second.empty()) {
        return Invalid("upgrade evidence requires target_topic entries");
    }
    for (const std::string& encoded : target_topics->second) {
        const std::vector<std::string> values = Split(encoded, ',');
        if (values.size() != 10) {
            return Invalid(
                "target_topic requires id,name,config,region_version,channel_version,acl_version,schema_short,schema_version,layout_version,digest");
        }
        TopicBinding topic;
        MINO_ASSIGN_OR_RETURN(topic.topic_id.value,
                              Unsigned<uint32_t>(values[0], "target_topic_id"));
        topic.name = values[1];
        MINO_ASSIGN_OR_RETURN(topic.config_version,
                              Unsigned<uint64_t>(values[2], "target_config"));
        MINO_ASSIGN_OR_RETURN(topic.region_version,
                              Unsigned<uint64_t>(values[3], "target_region_version"));
        MINO_ASSIGN_OR_RETURN(topic.channel_version,
                              Unsigned<uint64_t>(values[4], "target_channel_version"));
        MINO_ASSIGN_OR_RETURN(topic.acl_version,
                              Unsigned<uint64_t>(values[5], "target_acl_version"));
        uint64_t short_id = 0;
        uint32_t schema_version = 0;
        uint32_t layout_version = 0;
        MINO_ASSIGN_OR_RETURN(short_id,
                              Unsigned<uint64_t>(values[6], "target_schema_short"));
        MINO_ASSIGN_OR_RETURN(schema_version,
                              Unsigned<uint32_t>(values[7], "target_schema_version"));
        MINO_ASSIGN_OR_RETURN(layout_version,
                              Unsigned<uint32_t>(values[8], "target_layout_version"));
        MINO_ASSIGN_OR_RETURN(const schema::CanonicalDigest digest,
                              Digest(values[9], "target_digest"));
        topic.schema = schema::SchemaIdentity(short_id, digest, schema_version,
                                              layout_version);
        evidence.target_topics.push_back(std::move(topic));
    }
#define MINO_EVIDENCE_BOOL(member, key) \
    MINO_ASSIGN_OR_RETURN(member, BoolField(fields, key))
#define MINO_EVIDENCE_U64(member, key) \
    MINO_ASSIGN_OR_RETURN(member, U64Field(fields, key))
#define MINO_EVIDENCE_U32(member, key) \
    MINO_ASSIGN_OR_RETURN(member, U32Field(fields, key))
    MINO_EVIDENCE_BOOL(evidence.prepared, "prepared_ack");
    MINO_EVIDENCE_BOOL(evidence.target_topics_ready, "target_topics_ready");
    MINO_EVIDENCE_BOOL(evidence.readiness.region_active, "region_active");
    MINO_EVIDENCE_BOOL(evidence.readiness.processes_ready, "processes_ready");
    MINO_EVIDENCE_BOOL(evidence.readiness.channels_ready, "channels_ready");
    MINO_EVIDENCE_BOOL(evidence.readiness.routes_ready, "routes_ready");
    MINO_EVIDENCE_BOOL(evidence.readiness.schema_bidirectionally_compatible,
                       "schema_bidirectionally_compatible");
    MINO_EVIDENCE_BOOL(evidence.readiness.acl_exactly_preserved,
                       "acl_exactly_preserved");
    MINO_EVIDENCE_BOOL(evidence.readiness.capacity_admitted, "capacity_admitted");
    MINO_EVIDENCE_U64(evidence.readiness.available_shm_bytes,
                      "available_shm_bytes");
    MINO_EVIDENCE_U32(evidence.readiness.available_publisher_slots,
                      "available_publisher_slots");
    MINO_EVIDENCE_U32(evidence.readiness.available_subscriber_slots,
                      "available_subscriber_slots");
    MINO_EVIDENCE_BOOL(evidence.drain_ack, "drain_ack");
    MINO_EVIDENCE_BOOL(evidence.drain.old_publishers_fenced,
                       "old_publishers_fenced");
    MINO_EVIDENCE_U64(evidence.drain.publishers, "publishers");
    MINO_EVIDENCE_U64(evidence.drain.subscribers, "subscribers");
    MINO_EVIDENCE_U64(evidence.drain.pins, "pins");
    MINO_EVIDENCE_U64(evidence.drain.outstanding_receipts,
                      "outstanding_receipts");
    MINO_EVIDENCE_U64(evidence.drain.outstanding_borrows, "outstanding_borrows");
    MINO_EVIDENCE_U64(evidence.drain.queue_depth, "queue_depth");
    MINO_EVIDENCE_U64(evidence.drain.last_published_sequence,
                      "last_published_sequence");
    MINO_EVIDENCE_U64(evidence.drain.last_consumed_sequence,
                      "last_consumed_sequence");
    MINO_EVIDENCE_BOOL(evidence.cutover_ack, "cutover_ack");
    MINO_EVIDENCE_U64(evidence.observation.old_publisher_count,
                      "old_publisher_count");
    MINO_EVIDENCE_U64(evidence.observation.new_publisher_count,
                      "new_publisher_count");
    MINO_EVIDENCE_U64(evidence.observation.observed_samples, "observed_samples");
    MINO_EVIDENCE_U64(evidence.observation.duplicate_count, "duplicate_count");
    MINO_EVIDENCE_U64(evidence.observation.unexplained_loss_count,
                      "unexplained_loss_count");
    MINO_EVIDENCE_BOOL(evidence.commit_ack, "commit_ack");
    MINO_EVIDENCE_BOOL(evidence.rollback_ack, "rollback_ack");
    MINO_EVIDENCE_BOOL(evidence.rollback.target_publishers_fenced,
                       "target_publishers_fenced");
    MINO_EVIDENCE_BOOL(evidence.rollback.source_ready, "source_ready");
    MINO_EVIDENCE_U64(evidence.rollback.target_publications_after_cutover,
                      "target_publications_after_cutover");
    MINO_EVIDENCE_BOOL(
        evidence.rollback.sequence_receipt_reconciliation_complete,
        "sequence_receipt_reconciliation_complete");
#undef MINO_EVIDENCE_U32
#undef MINO_EVIDENCE_U64
#undef MINO_EVIDENCE_BOOL
    evidence.readiness.region = evidence.target_region;
    evidence.observation.active_region = evidence.target_region;
    evidence.observation.acknowledged_commit_token = evidence.commit_token;
    return evidence;
}

struct CommandOptions {
    std::filesystem::path manifest;
    std::filesystem::path plan;
    std::filesystem::path evidence;
    std::filesystem::path supervisor_socket;
    bool apply = false;
};

Result<CommandOptions> ParseCommandOptions(const std::vector<std::string>& args,
                                           size_t begin) {
    CommandOptions options;
    for (size_t index = begin; index < args.size(); ++index) {
        if (args[index] == "--apply") {
            options.apply = true;
            continue;
        }
        if (index + 1 >= args.size()) {
            return Invalid("upgrade option requires a value: " + args[index]);
        }
        const std::string value = args[++index];
        if (args[index - 1] == "--manifest") {
            options.manifest = value;
        } else if (args[index - 1] == "--plan") {
            options.plan = value;
        } else if (args[index - 1] == "--evidence") {
            options.evidence = value;
        } else if (args[index - 1] == "--supervisor-socket") {
            options.supervisor_socket = value;
        } else {
            return Invalid("unknown upgrade option: " + args[index - 1]);
        }
    }
    return options;
}

Result<std::string> SendSupervisorRequest(
    const std::filesystem::path& socket_path, std::string_view command,
    const std::filesystem::path& manifest) {
    struct stat state {};
    if (::lstat(socket_path.c_str(), &state) != 0 || !S_ISSOCK(state.st_mode) ||
        state.st_uid != ::geteuid() ||
        (state.st_mode & (S_IWGRP | S_IWOTH)) != 0) {
        return Status::Error(
            StatusCode::kPermissionDenied,
            "production supervisor socket must be owner-only and owned by this uid");
    }
    const std::string manifest_text = manifest.string();
    if (manifest_text.empty() || manifest_text.find('\0') != std::string::npos ||
        manifest_text.find('\n') != std::string::npos ||
        socket_path.string().size() >= sizeof(sockaddr_un::sun_path)) {
        return Invalid("supervisor socket or manifest path is invalid");
    }
    const int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        return Status::Error(StatusCode::kUnavailable,
                             "cannot create supervisor control socket");
    }
    struct CloseFd final {
        int fd;
        ~CloseFd() { static_cast<void>(::close(fd)); }
    } close_fd{fd};
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::memcpy(address.sun_path, socket_path.c_str(),
                socket_path.string().size() + 1);
    if (::connect(fd, reinterpret_cast<const sockaddr*>(&address),
                  sizeof(address)) != 0) {
        return Status::Error(StatusCode::kUnavailable,
                             "cannot connect to production upgrade supervisor");
    }
    const std::string request = "MINO-UPGRADE/1\n" + std::string(command) +
                                "\nmanifest=" + manifest_text + "\n\n";
    size_t written = 0;
    while (written < request.size()) {
        const ssize_t count =
            ::write(fd, request.data() + written, request.size() - written);
        if (count < 0) {
            if (errno == EINTR) continue;
            return Status::Error(StatusCode::kUnavailable,
                                 "cannot write supervisor upgrade request");
        }
        if (count == 0) {
            return Status::Error(StatusCode::kUnavailable,
                                 "short supervisor upgrade request write");
        }
        written += static_cast<size_t>(count);
    }
    if (::shutdown(fd, SHUT_WR) != 0) {
        return Status::Error(StatusCode::kUnavailable,
                             "cannot finish supervisor upgrade request");
    }
    std::string response;
    char buffer[1024];
    for (;;) {
        const ssize_t count = ::read(fd, buffer, sizeof(buffer));
        if (count < 0) {
            if (errno == EINTR) continue;
            return Status::Error(StatusCode::kUnavailable,
                                 "cannot read supervisor upgrade response");
        }
        if (count == 0) break;
        if (response.size() + static_cast<size_t>(count) >
            upgrade::kMaximumUpgradeFileBytes) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "supervisor upgrade response exceeds its bound");
        }
        response.append(buffer, static_cast<size_t>(count));
    }
    if (!response.starts_with("OK\n")) {
        return Status::Error(StatusCode::kUnavailable,
                             "production supervisor refused upgrade: " + response);
    }
    return response.substr(3);
}

void PrintStatus(const upgrade::UpgradeSnapshot& snapshot, std::ostream& output) {
    output << "operation_id=" << snapshot.plan.operation_id << '\n'
           << "phase=" << upgrade::UpgradePhaseName(snapshot.phase) << '\n'
           << "generation=" << snapshot.generation << '\n'
           << "source_region=" << snapshot.plan.source_region.name << ','
           << snapshot.plan.source_region.region_id << '\n'
           << "target_region=" << snapshot.plan.target_region.name << ','
           << snapshot.plan.target_region.region_id << '\n'
           << "topics=" << snapshot.plan.topics.size() << '\n'
           << "terminal_reason=" << snapshot.terminal_reason << '\n';
}

}  // namespace

void PrintUpgradeUsage(std::ostream& output) {
    output << R"(mino upgrade: durable New Region + Drain/Cutover orchestration

USAGE:
  mino upgrade plan --manifest <file> --plan <key-value-file> [--apply]
  mino upgrade status --manifest <file>
  mino upgrade inspect --evidence <file>
  mino upgrade execute --manifest <file> [--apply --supervisor-socket <path>]
  mino upgrade resume --manifest <file> [--apply --supervisor-socket <path>]
  mino upgrade rollback --manifest <file> [--apply --supervisor-socket <path>]

Mutating commands are dry-run unless --apply is explicit. execute/resume/rollback
can apply only through an owner-authenticated production supervisor socket.
Offline evidence is accepted only by inspect/dry-run and can never advance the
manifest. See docs/operations/rolling_upgrade.md.
)";
}

int RunUpgradeCommand(const std::vector<std::string>& args, std::ostream& output,
                      std::ostream& error) {
    if (args.empty() || args[0] == "--help" || args[0] == "-h") {
        PrintUpgradeUsage(output);
        return args.empty() ? 2 : 0;
    }
    const std::string& command = args[0];
    auto options = ParseCommandOptions(args, 1);
    if (!options.ok()) {
        error << "mino upgrade: " << options.status().ToString() << '\n';
        return 2;
    }
    if (command == "inspect") {
        if (options->apply || options->evidence.empty()) {
            error << "mino upgrade inspect: --evidence is required and --apply is forbidden\n";
            return 2;
        }
        auto evidence = ParseEvidence(options->evidence);
        if (!evidence.ok()) {
            error << "mino upgrade inspect: " << evidence.status().ToString()
                  << '\n';
            return 1;
        }
        output << "offline-evidence=inspect-only\ncommit_token="
               << evidence->commit_token << "\ntarget_region="
               << evidence->target_region.name << ','
               << evidence->target_region.region_id << "\ncomplete_drain="
               << (evidence->drain.complete() ? "true" : "false") << '\n';
        return 0;
    }
    if (options->manifest.empty()) {
        error << "mino upgrade " << command << ": --manifest is required\n";
        return 2;
    }
    if (command == "plan") {
        if (options->plan.empty()) {
            error << "mino upgrade plan: --plan is required\n";
            return 2;
        }
        auto plan = ParsePlan(options->plan);
        if (!plan.ok()) {
            error << "mino upgrade plan: " << plan.status().ToString() << '\n';
            return 1;
        }
        if (!options->apply) {
            output << "dry-run: valid plan; would create "
                   << options->manifest.string() << " in prepare\n";
            return 0;
        }
        auto store = upgrade::UpgradeManifestStore::Create(
            options->manifest, std::move(*plan), NowNs());
        if (!store.ok()) {
            error << "mino upgrade plan: " << store.status().ToString() << '\n';
            return 1;
        }
        PrintStatus((*store)->snapshot(), output);
        return 0;
    }
    if (command != "status" && command != "execute" && command != "resume" &&
        command != "rollback") {
        error << "mino upgrade: unknown subcommand '" << command << "'\n";
        return 2;
    }
    if (command != "status" && options->apply) {
        if (!options->evidence.empty()) {
            error << "mino upgrade " << command
                  << ": offline --evidence is inspect-only and cannot apply\n";
            return 2;
        }
        if (options->supervisor_socket.empty()) {
            error << "mino upgrade " << command
                  << ": --supervisor-socket is required with --apply\n";
            return 2;
        }
        auto response = SendSupervisorRequest(options->supervisor_socket, command,
                                              options->manifest);
        if (!response.ok()) {
            error << "mino upgrade " << command << ": "
                  << response.status().ToString() << '\n';
            return 1;
        }
        output << *response;
        return 0;
    }
    auto store = upgrade::UpgradeManifestStore::Open(options->manifest);
    if (!store.ok()) {
        error << "mino upgrade " << command << ": " << store.status().ToString()
              << '\n';
        return 1;
    }
    if (command == "status") {
        PrintStatus((*store)->snapshot(), output);
        return 0;
    }
    if (!options->evidence.empty()) {
        auto evidence = ParseEvidence(options->evidence);
        if (!evidence.ok()) {
            error << "mino upgrade " << command << " dry-run evidence: "
                  << evidence.status().ToString() << '\n';
            return 1;
        }
        output << "offline-evidence=inspect-only ";
    }
    const upgrade::UpgradeStepPreview preview =
        upgrade::UpgradeOrchestrator(store->get(), nullptr).Preview();
    output << "dry-run: phase=" << upgrade::UpgradePhaseName(preview.current)
           << " next=" << upgrade::UpgradePhaseName(preview.next)
           << " action=" << preview.action << '\n';
    return 0;
}

}  // namespace mino::tools
