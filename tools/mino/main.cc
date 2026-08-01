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

// mino: Mino CLI tool (D1-11).
//
// Subcommands:
//   mino inspect <region_name> [--output <file>]
//       Run the Inspector diagnostic scan and print a report.
//   mino dump-ring <region_name> <channel_id> [--output <file>]
//       Dump one RingBuffer (control block + per-slot summaries).
//   mino recover <region_name> [--dry-run]
//       Run the recovery scanner: reclaim orphan slabs, fix bitmap
//       inconsistencies, report corruption. Requires recovery ownership.
//
// `inspect` attaches to a live Region by name and derives slab layout from
// persisted allocator metadata. `--layout` + `--image` remain available for
// offline images and are currently required for ring dumps because the Region
// Directory does not yet persist channel ring locations. `recover` continues
// to use its explicit recovery sidecar/image path.

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "mino/common/status.h"
#include "mino/shm/recovery/scanner.h"
#include "tools/mino/inspector.h"

namespace {

using mino::Status;
using mino::StatusCode;

void PrintUsage(std::ostream& out) {
    out <<
        R"(mino: Mino diagnostic and recovery CLI

USAGE:
    mino inspect <region_name> [--layout <file>] [--image <file>] [--output <file>]
    mino dump-ring <region_name> <channel_id> [--layout <file>] [--image <file>] [--output <file>]
    mino recover <region_name> [--layout <file>] [--image <file>] [--dry-run]

OPTIONS:
    --layout <file>   Layout sidecar describing class table and ring buffers.
                      Must be paired with --image; required for ring locations.
    --image <file>    Raw offline region image. Must be paired with --layout.
    --output <file>   Write the report to <file> instead of stdout.
    --dry-run         (recover only) Scan without repairing.
)";
}

// ---------------------------------------------------------------------------
// Layout sidecar parsing
//
// The sidecar is a tiny line-based format (no third-party deps per task):
//
//   class <class_id> <slot_count> <bitmap_off> <slots_off> <slot_stride>
//   ring  <channel_id> <control_off>
//   recovery <state_off>            (required for `recover`)
//   # comment
//
// All offsets are decimal or 0x-prefixed hex region-relative byte offsets.
// ---------------------------------------------------------------------------
struct Sidecar {
    mino::tools::Inspector::Layout inspector_layout;
    uint64_t recovery_state_offset = 0;
    uint64_t class_table_offset = 0;  // In-image class descriptor table.
    bool has_recovery = false;
};

Status ParseSidecar(const std::string& path, Sidecar* out) {
    std::ifstream in(path);
    if (!in) {
        return Status::Error(StatusCode::kNotFound,
                             "cannot open layout sidecar: " + path);
    }
    std::string line;
    uint32_t line_no = 0;
    while (std::getline(in, line)) {
        ++line_no;
        std::istringstream ls(line);
        std::string keyword;
        if (!(ls >> keyword) || keyword.empty() || keyword[0] == '#') {
            continue;
        }
        auto parse_u64 = [&](uint64_t* v) -> bool {
            std::string tok;
            if (!(ls >> tok)) {
                return false;
            }
            char* end = nullptr;
            *v = std::strtoull(tok.c_str(), &end, 0);
            return end != tok.c_str() && *end == '\0';
        };
        if (keyword == "class") {
            mino::tools::Inspector::ClassView cls{};
            uint64_t tmp = 0;
            if (!parse_u64(&tmp)) goto malformed;
            cls.class_id = static_cast<uint32_t>(tmp);
            if (!parse_u64(&tmp)) goto malformed;
            cls.slot_count = static_cast<uint32_t>(tmp);
            if (!parse_u64(&cls.bitmap_offset)) goto malformed;
            if (!parse_u64(&cls.slots_offset)) goto malformed;
            if (!parse_u64(&tmp)) goto malformed;
            cls.slot_stride = static_cast<uint32_t>(tmp);
            out->inspector_layout.classes.push_back(cls);
            continue;
        }
        if (keyword == "ring") {
            mino::tools::Inspector::Layout::RingRef ref{};
            uint64_t tmp = 0;
            if (!parse_u64(&tmp)) goto malformed;
            ref.channel_id = static_cast<uint32_t>(tmp);
            if (!parse_u64(&ref.control_offset)) goto malformed;
            out->inspector_layout.rings.push_back(ref);
            continue;
        }
        if (keyword == "recovery") {
            if (!parse_u64(&out->recovery_state_offset)) goto malformed;
            out->has_recovery = true;
            continue;
        }
        if (keyword == "class_table") {
            if (!parse_u64(&out->class_table_offset)) {
                goto malformed;
            }
            continue;
        }
        goto malformed;
    }
    return Status::Ok();

malformed:
    return Status::Error(StatusCode::kInvalidArgument,
                         path + ":" + std::to_string(line_no) +
                             ": malformed layout line: " + line);
}

// Reads an entire file into memory. Region images can be large; this is a
// diagnostic tool, not a hot path.
Status ReadFile(const std::string& path, std::vector<std::byte>* out) {
    std::ifstream in(path, std::ios::binary | std::ios::ate);
    if (!in) {
        return Status::Error(StatusCode::kNotFound,
                             "cannot open region image: " + path);
    }
    const std::streamsize size = in.tellg();
    if (size < 0) {
        return Status::Error(StatusCode::kInternal,
                             "cannot determine size of: " + path);
    }
    out->resize(static_cast<size_t>(size));
    in.seekg(0);
    if (size > 0 && !in.read(reinterpret_cast<char*>(out->data()), size)) {
        return Status::Error(StatusCode::kInternal,
                             "failed to read region image: " + path);
    }
    return Status::Ok();
}

struct CommonArgs {
    std::string region_name;
    std::string layout_path;
    std::string image_path;
    std::string output_path;
    bool dry_run = false;
    uint32_t channel_id = 0;
    bool has_channel = false;
};

// Minimal flag parser (no third-party deps per task constraints).
Status ParseArgs(int argc, char** argv, CommonArgs* args, int* positional) {
    int pos = 0;
    for (int i = 0; i < argc; ++i) {
        const std::string arg = argv[i];
        auto next = [&](const char* flag, std::string* dst) -> Status {
            if (i + 1 >= argc) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     std::string(flag) + " requires a value");
            }
            *dst = argv[++i];
            return Status::Ok();
        };
        if (arg == "--layout") {
            MINO_RETURN_IF_ERROR(next("--layout", &args->layout_path));
        } else if (arg == "--image") {
            MINO_RETURN_IF_ERROR(next("--image", &args->image_path));
        } else if (arg == "--output" || arg == "-o") {
            MINO_RETURN_IF_ERROR(next("--output", &args->output_path));
        } else if (arg == "--dry-run") {
            args->dry_run = true;
        } else if (arg == "--help" || arg == "-h") {
            return Status::Error(StatusCode::kInvalidArgument, "help");
        } else if (!arg.empty() && arg[0] == '-') {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "unknown flag: " + arg);
        } else {
            positional[pos++] = i;
        }
    }
    return Status::Ok();
}

// Writes `content` to stdout or to args.output_path.
Status Emit(const CommonArgs& args, const std::string& content) {
    if (args.output_path.empty()) {
        std::cout << content;
        return Status::Ok();
    }
    std::ofstream out(args.output_path);
    if (!out) {
        return Status::Error(StatusCode::kPermissionDenied,
                             "cannot open output file: " + args.output_path);
    }
    out << content;
    return Status::Ok();
}

// Attaches by Region name, or loads an explicit sidecar + offline image.
mino::Result<mino::tools::Inspector> LoadInspector(
    const CommonArgs& args, std::vector<std::byte>* image_storage) {
    if (args.layout_path.empty() && args.image_path.empty()) {
        return mino::tools::Inspector::Attach(args.region_name);
    }
    if (args.layout_path.empty() || args.image_path.empty()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "--layout and --image must be provided together");
    }
    Sidecar sidecar;
    MINO_RETURN_IF_ERROR(ParseSidecar(args.layout_path, &sidecar));
    MINO_RETURN_IF_ERROR(ReadFile(args.image_path, image_storage));
    return mino::tools::Inspector::AttachMemory(
        image_storage->data(), image_storage->size(),
        sidecar.inspector_layout, args.region_name);
}

int CmdInspect(const CommonArgs& args) {
    std::vector<std::byte> image;
    auto inspector_or = LoadInspector(args, &image);
    if (!inspector_or.ok()) {
        std::cerr << "inspect: " << inspector_or.status().ToString() << "\n";
        return 1;
    }
    mino::tools::Inspector inspector = std::move(inspector_or.value());
    std::ostringstream report;
    Status st = inspector.PrintReport(report);
    if (!st.ok()) {
        std::cerr << "inspect: " << st.ToString() << "\n";
        return 1;
    }
    st = Emit(args, report.str());
    if (!st.ok()) {
        std::cerr << "inspect: " << st.ToString() << "\n";
        return 1;
    }
    return 0;
}

int CmdDumpRing(const CommonArgs& args) {
    std::vector<std::byte> image;
    auto inspector_or = LoadInspector(args, &image);
    if (!inspector_or.ok()) {
        std::cerr << "dump-ring: " << inspector_or.status().ToString() << "\n";
        return 1;
    }
    mino::tools::Inspector inspector = std::move(inspector_or.value());
    auto dump = inspector.DumpRingBuffer(args.channel_id);
    if (!dump.ok()) {
        std::cerr << "dump-ring: " << dump.status().ToString() << "\n";
        return 1;
    }
    std::ostringstream out;
    out << "channel:            " << dump->channel_id << "\n";
    out << "capacity:           " << dump->capacity << "\n";
    out << "enqueue_pos:        " << dump->enqueue_pos << "\n";
    out << "dequeue_pos:        " << dump->dequeue_pos << "\n";
    out << "pending:            " << dump->pending << "\n";
    out << "elem_size/align:    " << dump->elem_size << " / "
        << dump->elem_align << "\n";
    out << "layout_version:     " << dump->layout_version << "\n";
    for (uint64_t i = 0; i < dump->slots.size(); ++i) {
        const auto& s = dump->slots[i];
        out << "[" << i << "] ring_seq=" << s.ring_sequence
            << " seq=" << s.sequence
            << " state=" << mino::tools::SlotStateName(s.state)
            << " msg_type=" << s.msg_type << " ts=" << s.timestamp_ns
            << " payload_off=0x" << std::hex << s.payload_offset << std::dec
            << " len=" << s.payload_len << " gen=" << s.payload_generation
            << "\n";
    }
    Status st = Emit(args, out.str());
    if (!st.ok()) {
        std::cerr << "dump-ring: " << st.ToString() << "\n";
        return 1;
    }
    return 0;
}

int CmdRecover(const CommonArgs& args) {
    // Load sidecar + image. The recovery scanner needs the recovery owner
    // offset which the inspector layout does not carry.
    if (args.layout_path.empty() || args.image_path.empty()) {
        std::cerr << "recover: live recovery is not exposed by this CLI path; "
                     "provide --layout and --image\n";
        return 1;
    }
    Sidecar sidecar;
    Status st = ParseSidecar(args.layout_path, &sidecar);
    if (!st.ok()) {
        std::cerr << "recover: " << st.ToString() << "\n";
        return 1;
    }
    if (!sidecar.has_recovery) {
        std::cerr << "recover: layout sidecar lacks a `recovery` line\n";
        return 1;
    }
    std::vector<std::byte> image;
    st = ReadFile(args.image_path, &image);
    if (!st.ok()) {
        std::cerr << "recover: " << st.ToString() << "\n";
        return 1;
    }

    // Translate the inspector class list into the scanner layout.
    mino::shm::recovery::RecoveryScanner::Layout layout;
    layout.recovery_state_offset = sidecar.recovery_state_offset;
    layout.class_table_offset = sidecar.class_table_offset;
    layout.class_count =
        static_cast<uint32_t>(sidecar.inspector_layout.classes.size());
    // The scanner reads class descriptors from a table inside the region
    // (allocator metadata contract). For image-based recovery the sidecar
    // is authoritative: materialize it into the image at class_table_offset.
    // Bounds were validated by ParseSidecar + the RecoveryScanner::Create
    // contract check below.
    const uint64_t table_bytes =
        layout.class_count *
        sizeof(mino::shm::recovery::RecoveryScanner::ClassDescriptor);
    if (layout.class_table_offset + table_bytes > image.size()) {
        std::cerr << "recover: class_table_offset out of image bounds\n";
        return 1;
    }
    auto* table = reinterpret_cast<
        mino::shm::recovery::RecoveryScanner::ClassDescriptor*>(
        image.data() + layout.class_table_offset);
    for (uint32_t i = 0; i < layout.class_count; ++i) {
        const auto& src = sidecar.inspector_layout.classes[i];
        auto& dst = table[i];
        dst.class_id = src.class_id;
        dst.slot_count = src.slot_count;
        dst.bitmap_offset = src.bitmap_offset;
        dst.slots_offset = src.slots_offset;
        dst.slot_stride = src.slot_stride;
        dst.reserved = 0;
    }

    mino::shm::recovery::RecoveryScannerOptions options;
    options.repair = !args.dry_run;
    auto scanner = mino::shm::recovery::RecoveryScanner::Create(
        image.data(), image.size(), layout, options);
    if (!scanner.ok()) {
        std::cerr << "recover: " << scanner.status().ToString() << "\n";
        return 1;
    }

    if (!args.dry_run) {
        st = scanner->Owner().TryAcquire();
        if (!st.ok()) {
            std::cerr << "recover: cannot acquire recovery ownership: "
                      << st.ToString() << "\n";
            return 1;
        }
    }

    auto report = scanner->Scan();
    if (!report.ok()) {
        std::cerr << "recover: " << report.status().ToString() << "\n";
        return 1;
    }

    std::ostringstream out;
    out << "=== Mino Recovery Report ===\n";
    out << "region:                  " << args.region_name << "\n";
    out << "mode:                    "
        << (args.dry_run ? "dry-run (read-only)" : "repair") << "\n";
    out << "slots_scanned:           " << report->slots_scanned << "\n";
    out << "orphan_slab_count:       " << report->orphan_slab_count << "\n";
    out << "reclaimed_slab_count:    " << report->reclaimed_slab_count << "\n";
    out << "stale_ack_count:         " << report->stale_ack_count << "\n";
    out << "bitmap_inconsistencies:  " << report->bitmap_inconsistency_count
        << "\n";
    out << "corrupted_slab_count:    " << report->corrupted_slab_count
        << "\n";
    if (!report->details.empty()) {
        out << "\ndetails:\n" << report->details;
    }
    st = Emit(args, out.str());
    if (!st.ok()) {
        std::cerr << "recover: " << st.ToString() << "\n";
        return 1;
    }

    if (!args.dry_run) {
        scanner->Owner().Release();
    }
    return report->corrupted_slab_count > 0 ? 2 : 0;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        PrintUsage(std::cerr);
        return 2;
    }
    const std::string command = argv[1];

    CommonArgs args;
    int positional[3] = {-1, -1, -1};
    Status st = ParseArgs(argc - 2, argv + 2, &args, positional);
    if (!st.ok()) {
        if (st.message() == "help") {
            PrintUsage(std::cout);
            return 0;
        }
        std::cerr << "mino: " << st.ToString() << "\n";
        PrintUsage(std::cerr);
        return 2;
    }

    if (command == "inspect" || command == "recover") {
        if (positional[0] < 0) {
            std::cerr << "mino: " << command << " requires <region_name>\n";
            return 2;
        }
        args.region_name = argv[positional[0] + 2];
        return command == "inspect" ? CmdInspect(args) : CmdRecover(args);
    }
    if (command == "dump-ring") {
        if (positional[0] < 0 || positional[1] < 0) {
            std::cerr << "mino: dump-ring requires <region_name> <channel_id>\n";
            return 2;
        }
        args.region_name = argv[positional[0] + 2];
        char* end = nullptr;
        args.channel_id =
            static_cast<uint32_t>(std::strtoul(argv[positional[1] + 2], &end, 0));
        if (end == argv[positional[1] + 2] || *end != '\0') {
            std::cerr << "mino: invalid channel_id\n";
            return 2;
        }
        args.has_channel = true;
        return CmdDumpRing(args);
    }

    std::cerr << "mino: unknown command: " << command << "\n";
    PrintUsage(std::cerr);
    return 2;
}
