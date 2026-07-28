// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/config/toml_config.h"

#include <fstream>
#include <initializer_list>
#include <limits>
#include <sstream>
#include <string>
#include <string_view>

#include "toml.hpp"

#include "mino/common/status.h"

namespace mino {
namespace {

Status Invalid(std::string message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

bool IsAllowed(std::string_view key,
               std::initializer_list<std::string_view> allowed) {
    for (std::string_view candidate : allowed) {
        if (key == candidate) {
            return true;
        }
    }
    return false;
}

Status ValidateKeys(const toml::table& table, std::string_view table_name,
                    std::initializer_list<std::string_view> allowed) {
    for (const auto& [key, value] : table) {
        (void)value;
        if (!IsAllowed(key.str(), allowed)) {
            return Invalid("unknown key '" + std::string(table_name) + "." +
                           std::string(key.str()) + "'");
        }
    }
    return Status::Ok();
}

Result<const toml::table*> ReadTable(const toml::table& parent,
                                     std::string_view key,
                                     std::string_view full_name) {
    const toml::node* node = parent.get(key);
    if (node == nullptr) {
        return static_cast<const toml::table*>(nullptr);
    }
    const toml::table* table = node->as_table();
    if (table == nullptr) {
        return Invalid("'" + std::string(full_name) + "' must be a table");
    }
    return table;
}

Status ReadBool(const toml::table& table, std::string_view key,
                std::string_view full_name, bool* output) {
    const toml::node* node = table.get(key);
    if (node == nullptr) {
        return Status::Ok();
    }
    auto value = node->value<bool>();
    if (!value) {
        return Invalid("'" + std::string(full_name) + "' must be a boolean");
    }
    *output = *value;
    return Status::Ok();
}

Status ReadString(const toml::table& table, std::string_view key,
                  std::string_view full_name, std::string* output) {
    const toml::node* node = table.get(key);
    if (node == nullptr) {
        return Status::Ok();
    }
    auto value = node->value<std::string>();
    if (!value) {
        return Invalid("'" + std::string(full_name) + "' must be a string");
    }
    *output = std::move(*value);
    return Status::Ok();
}

template <typename T>
Status ReadPositiveInteger(const toml::table& table, std::string_view key,
                           std::string_view full_name, T* output) {
    const toml::node* node = table.get(key);
    if (node == nullptr) {
        return Status::Ok();
    }
    auto value = node->value<int64_t>();
    if (!value || *value <= 0 ||
        static_cast<uint64_t>(*value) >
            static_cast<uint64_t>(std::numeric_limits<T>::max())) {
        return Invalid("'" + std::string(full_name) +
                       "' must be a positive integer in range");
    }
    *output = static_cast<T>(*value);
    return Status::Ok();
}

Result<LogLevel> ParseLevel(const toml::table& table, std::string_view key,
                            LogLevel default_level) {
    const toml::node* node = table.get(key);
    if (node == nullptr) {
        return default_level;
    }
    auto value = node->value<std::string_view>();
    if (!value) {
        return Invalid("'logging." + std::string(key) + "' must be a string");
    }
    if (*value == "trace") return LogLevel::kTrace;
    if (*value == "debug") return LogLevel::kDebug;
    if (*value == "info") return LogLevel::kInfo;
    if (*value == "warn") return LogLevel::kWarn;
    if (*value == "error") return LogLevel::kError;
    if (*value == "critical") return LogLevel::kCritical;
    if (*value == "off") return LogLevel::kOff;
    return Invalid("invalid logging level '" + std::string(*value) + "'");
}

Status PopulateLoggingConfig(const toml::table& logging,
                             LoggingConfig* config) {
    Status status = ValidateKeys(
        logging, "logging",
        {"level", "pattern", "console", "file", "rotation", "async",
         "flush_on", "flush_interval_ms"});
    if (!status.ok()) return status;

    auto level = ParseLevel(logging, "level", config->level);
    if (!level.ok()) return level.status();
    config->level = *level;
    auto flush_on = ParseLevel(logging, "flush_on", config->flush_on);
    if (!flush_on.ok()) return flush_on.status();
    config->flush_on = *flush_on;

    status = ReadString(logging, "pattern", "logging.pattern", &config->pattern);
    if (!status.ok()) return status;
    status = ReadPositiveInteger(logging, "flush_interval_ms",
                                 "logging.flush_interval_ms",
                                 &config->flush_interval_ms);
    if (!status.ok()) return status;

    auto console = ReadTable(logging, "console", "logging.console");
    if (!console.ok()) return console.status();
    if (*console != nullptr) {
        status = ValidateKeys(**console, "logging.console", {"enabled"});
        if (!status.ok()) return status;
        status = ReadBool(**console, "enabled", "logging.console.enabled",
                          &config->console.enabled);
        if (!status.ok()) return status;
    }

    auto file = ReadTable(logging, "file", "logging.file");
    if (!file.ok()) return file.status();
    if (*file != nullptr) {
        status = ValidateKeys(**file, "logging.file", {"enabled", "path"});
        if (!status.ok()) return status;
        status = ReadBool(**file, "enabled", "logging.file.enabled",
                          &config->file.enabled);
        if (!status.ok()) return status;
        status = ReadString(**file, "path", "logging.file.path",
                            &config->file.path);
        if (!status.ok()) return status;
    }

    auto rotation = ReadTable(logging, "rotation", "logging.rotation");
    if (!rotation.ok()) return rotation.status();
    if (*rotation != nullptr) {
        status = ValidateKeys(**rotation, "logging.rotation",
                              {"max_size_bytes", "max_files"});
        if (!status.ok()) return status;
        status = ReadPositiveInteger(**rotation, "max_size_bytes",
                                     "logging.rotation.max_size_bytes",
                                     &config->rotation.max_size_bytes);
        if (!status.ok()) return status;
        status = ReadPositiveInteger(**rotation, "max_files",
                                     "logging.rotation.max_files",
                                     &config->rotation.max_files);
        if (!status.ok()) return status;
    }

    auto async = ReadTable(logging, "async", "logging.async");
    if (!async.ok()) return async.status();
    if (*async != nullptr) {
        status = ValidateKeys(**async, "logging.async",
                              {"enabled", "queue_size", "thread_count"});
        if (!status.ok()) return status;
        status = ReadBool(**async, "enabled", "logging.async.enabled",
                          &config->async.enabled);
        if (!status.ok()) return status;
        status = ReadPositiveInteger(**async, "queue_size",
                                     "logging.async.queue_size",
                                     &config->async.queue_size);
        if (!status.ok()) return status;
        status = ReadPositiveInteger(**async, "thread_count",
                                     "logging.async.thread_count",
                                     &config->async.thread_count);
        if (!status.ok()) return status;
    }

    if (!config->console.enabled && !config->file.enabled) {
        return Invalid("at least one logging sink must be enabled");
    }
    if (config->file.enabled && config->file.path.empty()) {
        return Invalid("'logging.file.path' must be non-empty when file logging is enabled");
    }
    return Status::Ok();
}

}  // namespace

Result<LoggingConfig> ParseLoggingConfigToml(std::string_view text) {
    try {
        toml::table root = toml::parse(text);
        Status status = ValidateKeys(root, "root", {"logging"});
        if (!status.ok()) return status;

        LoggingConfig config;
        auto logging = ReadTable(root, "logging", "logging");
        if (!logging.ok()) return logging.status();
        if (*logging != nullptr) {
            status = PopulateLoggingConfig(**logging, &config);
            if (!status.ok()) return status;
        }
        return config;
    } catch (const toml::parse_error& error) {
        std::ostringstream message;
        message << "invalid TOML: " << error.description();
        return Invalid(message.str());
    }
}

Result<LoggingConfig> LoadLoggingConfigFromTomlFile(std::string_view path) {
    std::ifstream input{std::string(path)};
    if (!input) {
        return Status::Error(StatusCode::kNotFound,
                             "cannot open configuration file '" +
                                 std::string(path) + "'");
    }
    std::ostringstream contents;
    contents << input.rdbuf();
    if (input.bad()) {
        return Status::Error(StatusCode::kInternal,
                             "failed to read configuration file '" +
                                 std::string(path) + "'");
    }
    return ParseLoggingConfigToml(contents.str());
}

}  // namespace mino
