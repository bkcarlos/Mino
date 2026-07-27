// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.gnu.org/licenses/lgpl-3.0.txt
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

#include "mino/common/status.h"

#include <string>

namespace mino {

std::string Status::ToString() const {
    std::string result;
    result.reserve(32 + message_.size());

    // Append the numeric code value.
    result.append(std::to_string(static_cast<uint16_t>(code_)));
    result.append(": ");

    if (message_.empty()) {
        // Provide a short default description for well-known codes when no
        // message was supplied. This keeps ToString() useful even for
        // hot-path errors constructed without a message.
        switch (code_) {
            case StatusCode::kOk:
                result.append("OK");
                break;
            case StatusCode::kInvalidArgument:
                result.append("Invalid argument");
                break;
            case StatusCode::kNotFound:
                result.append("Not found");
                break;
            case StatusCode::kAlreadyExists:
                result.append("Already exists");
                break;
            case StatusCode::kResourceExhausted:
                result.append("Resource exhausted");
                break;
            case StatusCode::kWouldBlock:
                result.append("Would block");
                break;
            case StatusCode::kTimeout:
                result.append("Timeout");
                break;
            case StatusCode::kSchemaMismatch:
                result.append("Schema mismatch");
                break;
            case StatusCode::kCorruption:
                result.append("Corruption");
                break;
            case StatusCode::kUnavailable:
                result.append("Unavailable");
                break;
            case StatusCode::kPermissionDenied:
                result.append("Permission denied");
                break;
            case StatusCode::kUnsupported:
                result.append("Unsupported");
                break;
            case StatusCode::kDegraded:
                result.append("Degraded");
                break;
            case StatusCode::kInternal:
                result.append("Internal error");
                break;
        }
    } else {
        result.append(message_);
    }

    return result;
}

}  // namespace mino
