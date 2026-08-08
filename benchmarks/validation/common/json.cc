// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "benchmarks/validation/common/json.h"

#include <iomanip>
#include <sstream>

namespace mino::benchmarks::validation {

std::string JsonEscape(std::string_view input) {
    std::ostringstream output;
    for (const unsigned char character : input) {
        switch (character) {
            case '"': output << "\\\""; break;
            case '\\': output << "\\\\"; break;
            case '\b': output << "\\b"; break;
            case '\f': output << "\\f"; break;
            case '\n': output << "\\n"; break;
            case '\r': output << "\\r"; break;
            case '\t': output << "\\t"; break;
            default:
                if (character < 0x20u) {
                    output << "\\u" << std::hex << std::setw(4)
                           << std::setfill('0')
                           << static_cast<unsigned int>(character) << std::dec
                           << std::setfill(' ');
                } else {
                    output << static_cast<char>(character);
                }
        }
    }
    return output.str();
}

std::string PendingResult(std::string_view reason) {
    return "{\"status\":\"PENDING\",\"reason\":\"" +
           JsonEscape(reason) + "\",\"metrics\":null}";
}

}  // namespace mino::benchmarks::validation
