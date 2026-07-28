// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_CONFIG_TOML_CONFIG_H_
#define MINO_CONFIG_TOML_CONFIG_H_

#include <string_view>

#include "mino/common/result.h"
#include "mino/config/logging_config.h"

namespace mino {

Result<LoggingConfig> LoadLoggingConfigFromTomlFile(std::string_view path);
Result<LoggingConfig> ParseLoggingConfigToml(std::string_view text);

}  // namespace mino

#endif  // MINO_CONFIG_TOML_CONFIG_H_
