// Copyright 2026 The Mino Authors

#ifndef TOOLS_MINO_UPGRADE_COMMANDS_H_
#define TOOLS_MINO_UPGRADE_COMMANDS_H_

#include <iosfwd>
#include <string>
#include <vector>

namespace mino::tools {

// args begins with plan/status/execute/resume/rollback.
int RunUpgradeCommand(const std::vector<std::string>& args, std::ostream& output,
                      std::ostream& error);
void PrintUpgradeUsage(std::ostream& output);

}  // namespace mino::tools

#endif  // TOOLS_MINO_UPGRADE_COMMANDS_H_
