// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <new>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/schema/codegen/code_generator.h"
#include "mino/schema/compiler.h"
#include "mino/schema/layout.h"
#include "mino/schema/parser.h"
#include "tools/minoc/output_commit.h"

namespace {

using ::mino::Result;
using ::mino::Status;
using ::mino::StatusCode;
using ::mino::schema::CanonicalDigest;
using ::mino::schema::CompiledSchema;
using ::mino::schema::CompileOptions;
using ::mino::schema::LayoutPlan;
using ::mino::schema::LayoutPlanner;
using ::mino::schema::Parser;
using ::mino::schema::SchemaCompiler;
using ::mino::schema::SchemaDescriptor;
using ::mino::schema::codegen::CodeGenerator;
using ::mino::schema::codegen::CodeGeneratorOptions;

using ::mino::tools::minoc::CommitOutputFiles;
using ::mino::tools::minoc::OutputFile;
using ::mino::tools::minoc::ValidateOutputPaths;

constexpr size_t kMaxFileBytes = 1u << 20;
constexpr size_t kMaxTotalImportBytes = 16u << 20;
constexpr size_t kMaxImports = 256;

// Stable process exit contract; diagnostics are always written to stderr.
enum class ExitCode : int {
    kSuccess = 0,
    kUsage = 2,
    kInputIo = 3,
    kImport = 4,
    kCompile = 5,
    kOutput = 6,
    kInternal = 7,
};

struct Arguments {
    std::filesystem::path input;
    std::filesystem::path output_header;
    std::filesystem::path output_source;
    std::filesystem::path output_descriptor;
    std::string header_include;
    std::map<std::string, std::filesystem::path, std::less<>> imports;
    bool help = false;
};

void PrintUsage(std::ostream& output) {
    output
        << "Usage: minoc --input FILE --output_header FILE --output_source FILE\n"
        << "             --output_descriptor FILE [--header_include INCLUDE]\n"
        << "             [--import LOGICAL_PATH=FILE ...]\n"
        << "Exit codes: 0 success, 2 usage, 3 input I/O, 4 import allowlist,\n"
        << "            5 compile/codegen, 6 output commit, 7 internal.\n";
}

Result<std::string> OptionValue(int argc, char** argv, int& index,
                                std::string_view option) {
    const std::string_view argument(argv[index]);
    const std::string prefix = std::string(option) + "=";
    if (argument.starts_with(prefix)) {
        return std::string(argument.substr(prefix.size()));
    }
    if (argument == option) {
        if (index + 1 >= argc) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 std::string(option) + " requires a value");
        }
        ++index;
        return std::string(argv[index]);
    }
    return Status::Error(StatusCode::kNotFound);
}

Result<Arguments> ParseArguments(int argc, char** argv) {
    Arguments result;
    for (int i = 1; i < argc; ++i) {
        const std::string_view argument(argv[i]);
        if (argument == "--help" || argument == "-h") {
            result.help = true;
            continue;
        }
        bool matched = false;
        const auto read_option = [&](std::string_view name) -> Result<std::string> {
            return OptionValue(argc, argv, i, name);
        };
        auto assign_path = [&](std::string_view name,
                               std::filesystem::path& destination) -> Status {
            auto value = read_option(name);
            if (value.ok()) {
                matched = true;
                if (value->empty()) {
                    return Status::Error(StatusCode::kInvalidArgument,
                                         std::string(name) + " must not be empty");
                }
                destination = *value;
                return Status::Ok();
            }
            if (value.status().code() != StatusCode::kNotFound) {
                return value.status();
            }
            return Status::Ok();
        };

        Status status = assign_path("--input", result.input);
        if (!status.ok()) return status;
        if (!matched) status = assign_path("--output_header", result.output_header);
        if (!status.ok()) return status;
        if (!matched) status = assign_path("--output_source", result.output_source);
        if (!status.ok()) return status;
        if (!matched) {
            status = assign_path("--output_descriptor", result.output_descriptor);
        }
        if (!status.ok()) return status;
        if (matched) continue;

        auto include = read_option("--header_include");
        if (include.ok()) {
            if (include->empty()) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "--header_include must not be empty");
            }
            result.header_include = std::move(*include);
            continue;
        }
        if (include.status().code() != StatusCode::kNotFound) {
            return include.status();
        }

        auto import = read_option("--import");
        if (import.ok()) {
            const size_t equals = import->find('=');
            if (equals == std::string::npos || equals == 0 ||
                equals + 1 == import->size()) {
                return Status::Error(
                    StatusCode::kInvalidArgument,
                    "--import must use LOGICAL_PATH=FILE syntax");
            }
            const std::string logical = import->substr(0, equals);
            if (!result.imports.emplace(logical, import->substr(equals + 1)).second) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "duplicate --import logical path '" + logical + "'");
            }
            if (result.imports.size() > kMaxImports) {
                return Status::Error(StatusCode::kResourceExhausted,
                                     "too many --import arguments");
            }
            continue;
        }
        if (import.status().code() != StatusCode::kNotFound) {
            return import.status();
        }
        return Status::Error(StatusCode::kInvalidArgument,
                             "unknown argument '" + std::string(argument) + "'");
    }

    if (!result.help &&
        (result.input.empty() || result.output_header.empty() ||
         result.output_source.empty() || result.output_descriptor.empty())) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "all input and output arguments are required");
    }
    if (result.header_include.empty() && !result.output_header.empty()) {
        result.header_include = result.output_header.filename().string();
    }
    const std::set<std::filesystem::path> outputs = {
        result.output_header.lexically_normal(),
        result.output_source.lexically_normal(),
        result.output_descriptor.lexically_normal(),
    };
    if (!result.help && outputs.size() != 3) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "output paths must be distinct");
    }
    if (!result.help) {
        const std::filesystem::path input = result.input.lexically_normal();
        for (const auto& output : outputs) {
            if (output == input) {
                return Status::Error(StatusCode::kInvalidArgument,
                                     "an output path aliases the input");
            }
            for (const auto& [logical, import] : result.imports) {
                static_cast<void>(logical);
                if (output == import.lexically_normal()) {
                    return Status::Error(StatusCode::kInvalidArgument,
                                         "an output path aliases an import");
                }
            }
        }
    }
    return result;
}

Result<std::string> ReadBounded(const std::filesystem::path& path,
                                size_t max_bytes) {
    std::error_code error;
    const uintmax_t size = std::filesystem::file_size(path, error);
    if (error) {
        return Status::Error(StatusCode::kNotFound,
                             "cannot stat '" + path.string() + "': " +
                                 error.message());
    }
    if (size > max_bytes) {
        return Status::Error(StatusCode::kResourceExhausted,
                             "input exceeds byte limit: '" + path.string() + "'");
    }
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        return Status::Error(StatusCode::kNotFound,
                             "cannot open '" + path.string() + "'");
    }
    std::string contents(static_cast<size_t>(size), '\0');
    if (!contents.empty()) {
        input.read(contents.data(), static_cast<std::streamsize>(contents.size()));
    }
    if (!input || static_cast<size_t>(input.gcount()) != contents.size()) {
        return Status::Error(StatusCode::kUnavailable,
                             "short read from '" + path.string() + "'");
    }
    return contents;
}

struct SourceUnit {
    std::filesystem::path path;
    std::string contents;
};

struct CompiledUnit {
    CompiledSchema schema;
    std::vector<std::shared_ptr<const SchemaDescriptor>> closure;
};

class ImportCompiler {
public:
    ImportCompiler(SourceUnit root,
                   std::map<std::string, SourceUnit, std::less<>> imports)
        : root_(std::move(root)), imports_(std::move(imports)) {}

    Result<const CompiledUnit*> CompileRoot() {
        const Status status = Visit(kRootKey);
        if (!status.ok()) return status;
        return &compiled_.find(kRootKey)->second;
    }

    bool import_error() const noexcept { return import_error_; }

private:
    Status Visit(const std::string& logical_path) {
        const auto state = states_.find(logical_path);
        if (state != states_.end()) {
            if (state->second == 2) return Status::Ok();
            import_error_ = true;
            return Status::Error(StatusCode::kInvalidArgument,
                                 "import cycle involving '" + logical_path + "'");
        }
        states_[logical_path] = 1;
        const SourceUnit* source = nullptr;
        if (logical_path == kRootKey) {
            source = &root_;
        } else {
            const auto found = imports_.find(logical_path);
            if (found == imports_.end()) {
                import_error_ = true;
                return Status::Error(StatusCode::kPermissionDenied,
                                     "undeclared import '" + logical_path + "'");
            }
            source = &found->second;
        }

        auto ast = Parser::Parse(source->contents);
        if (!ast.ok()) {
            return Status::Error(ast.status().code(),
                                 source->path.string() + ": " +
                                     ast.status().ToString());
        }
        std::vector<std::string> dependencies;
        dependencies.reserve(ast->imports.size());
        for (const auto& import : ast->imports) {
            if (!imports_.contains(import.path)) {
                import_error_ = true;
                return Status::Error(StatusCode::kPermissionDenied,
                                     source->path.string() +
                                         ": undeclared import '" + import.path + "'");
            }
            dependencies.push_back(import.path);
        }
        std::sort(dependencies.begin(), dependencies.end());
        dependencies.erase(std::unique(dependencies.begin(), dependencies.end()),
                           dependencies.end());
        for (const std::string& dependency : dependencies) {
            const Status dependency_status = Visit(dependency);
            if (!dependency_status.ok()) return dependency_status;
        }

        std::map<std::string, std::shared_ptr<const SchemaDescriptor>, std::less<>>
            dependency_descriptors;
        for (const std::string& dependency : dependencies) {
            const CompiledUnit& unit = compiled_.find(dependency)->second;
            for (const auto& descriptor : unit.closure) {
                const std::string name(descriptor->aggregate().full_name());
                const auto existing = dependency_descriptors.find(name);
                if (existing != dependency_descriptors.end() &&
                    existing->second->identity().canonical_digest() !=
                        descriptor->identity().canonical_digest()) {
                    import_error_ = true;
                    return Status::Error(
                        StatusCode::kSchemaMismatch,
                        "import closure FQN '" + name +
                            "' maps to multiple digests");
                }
                dependency_descriptors.insert_or_assign(name, descriptor);
            }
        }
        CompileOptions options;
        for (const auto& [name, descriptor] : dependency_descriptors) {
            static_cast<void>(name);
            options.dependencies.push_back(descriptor);
        }
        auto schema = SchemaCompiler::Compile(source->contents, options);
        if (!schema.ok()) {
            return Status::Error(schema.status().code(),
                                 source->path.string() + ": " +
                                     schema.status().ToString());
        }

        CompiledUnit unit{std::move(*schema), options.dependencies};
        for (const auto& descriptor : unit.schema.types()) {
            unit.closure.push_back(descriptor);
        }
        std::sort(unit.closure.begin(), unit.closure.end(),
                  [](const auto& lhs, const auto& rhs) {
                      return lhs->aggregate().full_name() <
                             rhs->aggregate().full_name();
                  });
        for (size_t i = 1; i < unit.closure.size(); ++i) {
            if (unit.closure[i - 1]->aggregate().full_name() ==
                    unit.closure[i]->aggregate().full_name() &&
                unit.closure[i - 1]->identity().canonical_digest() !=
                    unit.closure[i]->identity().canonical_digest()) {
                import_error_ = true;
                return Status::Error(
                    StatusCode::kSchemaMismatch,
                    "compiled closure FQN maps to multiple digests");
            }
        }
        unit.closure.erase(
            std::unique(unit.closure.begin(), unit.closure.end(),
                        [](const auto& lhs, const auto& rhs) {
                            return lhs->aggregate().full_name() ==
                                   rhs->aggregate().full_name();
                        }),
            unit.closure.end());
        compiled_.emplace(logical_path, std::move(unit));
        states_[logical_path] = 2;
        return Status::Ok();
    }

    static constexpr char kRootKey[] = "<root>";
    SourceUnit root_;
    std::map<std::string, SourceUnit, std::less<>> imports_;
    std::map<std::string, int, std::less<>> states_;
    std::map<std::string, CompiledUnit, std::less<>> compiled_;
    bool import_error_ = false;
};

Result<std::vector<std::shared_ptr<const SchemaDescriptor>>> ExactClosure(
    const SchemaDescriptor& descriptor,
    std::span<const std::shared_ptr<const SchemaDescriptor>> available) {
    std::map<std::string, CanonicalDigest, std::less<>> expected;
    expected.emplace(std::string(descriptor.aggregate().full_name()),
                     descriptor.identity().canonical_digest());
    for (const auto& dependency : descriptor.dependencies()) {
        expected.insert_or_assign(std::string(dependency.full_name()),
                                  dependency.digest());
    }
    std::map<std::string, std::shared_ptr<const SchemaDescriptor>, std::less<>> found;
    for (const auto& candidate : available) {
        const std::string name(candidate->aggregate().full_name());
        const auto wanted = expected.find(name);
        if (wanted == expected.end()) continue;
        if (wanted->second != candidate->identity().canonical_digest()) {
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "layout closure dependency digest mismatch");
        }
        found.insert_or_assign(name, candidate);
    }
    if (found.size() != expected.size()) {
        return Status::Error(StatusCode::kNotFound,
                             "layout closure is missing a dependency");
    }
    std::vector<std::shared_ptr<const SchemaDescriptor>> result;
    result.reserve(found.size());
    for (auto& [name, candidate] : found) {
        static_cast<void>(name);
        result.push_back(std::move(candidate));
    }
    return result;
}

Result<std::vector<LayoutPlan>> PlanLayouts(const CompiledUnit& root) {
    std::vector<LayoutPlan> layouts;
    layouts.reserve(root.schema.types().size());
    for (const auto& descriptor : root.schema.types()) {
        auto exact = ExactClosure(*descriptor, root.closure);
        if (!exact.ok()) return exact.status();
        auto layout = LayoutPlanner::Plan(*descriptor, *exact);
        if (!layout.ok()) return layout.status();
        layouts.push_back(std::move(*layout));
    }
    return layouts;
}



int Fail(ExitCode code, const Status& status) {
    std::cerr << "minoc: " << status.ToString() << '\n';
    return static_cast<int>(code);
}

}  // namespace

int Run(int argc, char** argv) {
    auto arguments = ParseArguments(argc, argv);
    if (!arguments.ok()) {
        PrintUsage(std::cerr);
        return Fail(ExitCode::kUsage, arguments.status());
    }
    if (arguments->help) {
        PrintUsage(std::cout);
        return static_cast<int>(ExitCode::kSuccess);
    }

    auto root_contents = ReadBounded(arguments->input, kMaxFileBytes);
    if (!root_contents.ok()) return Fail(ExitCode::kInputIo, root_contents.status());
    size_t total_import_bytes = 0;
    std::map<std::string, SourceUnit, std::less<>> imports;
    for (const auto& [logical, path] : arguments->imports) {
        auto contents = ReadBounded(path, kMaxFileBytes);
        if (!contents.ok()) return Fail(ExitCode::kInputIo, contents.status());
        if (contents->size() > kMaxTotalImportBytes - total_import_bytes) {
            return Fail(ExitCode::kInputIo,
                        Status::Error(StatusCode::kResourceExhausted,
                                      "total import bytes exceed limit"));
        }
        total_import_bytes += contents->size();
        imports.emplace(logical, SourceUnit{path, std::move(*contents)});
    }
    const std::array<std::filesystem::path, 3> output_paths = {
        arguments->output_header,
        arguments->output_source,
        arguments->output_descriptor,
    };
    std::vector<std::filesystem::path> protected_paths = {arguments->input};
    for (const auto& [logical, path] : arguments->imports) {
        static_cast<void>(logical);
        protected_paths.push_back(path);
    }
    const Status paths = ValidateOutputPaths(output_paths, protected_paths);
    if (!paths.ok()) return Fail(ExitCode::kImport, paths);

    ImportCompiler compiler(
        SourceUnit{arguments->input, std::move(*root_contents)},
        std::move(imports));
    auto root = compiler.CompileRoot();
    if (!root.ok()) {
        return Fail(compiler.import_error() ? ExitCode::kImport
                                            : ExitCode::kCompile,
                    root.status());
    }
    auto layouts = PlanLayouts(**root);
    if (!layouts.ok()) return Fail(ExitCode::kCompile, layouts.status());

    CodeGeneratorOptions codegen_options;
    codegen_options.header_include = arguments->header_include;
    codegen_options.descriptor_closure = (*root)->closure;
    auto artifacts = CodeGenerator::Generate((*root)->schema, *layouts,
                                             codegen_options);
    if (!artifacts.ok()) return Fail(ExitCode::kCompile, artifacts.status());

    const std::array<OutputFile, 3> output_files = {{
        {arguments->output_header, artifacts->header},
        {arguments->output_source, artifacts->source},
        {arguments->output_descriptor, artifacts->descriptor},
    }};
    const Status commit = CommitOutputFiles(output_files);
    if (!commit.ok()) return Fail(ExitCode::kOutput, commit);
    return static_cast<int>(ExitCode::kSuccess);
}

int main(int argc, char** argv) {
    try {
        return Run(argc, argv);
    } catch (const std::bad_alloc&) {
        return Fail(ExitCode::kInternal,
                    Status::Error(StatusCode::kResourceExhausted,
                                  "out of memory"));
    } catch (const std::exception& error) {
        return Fail(ExitCode::kInternal,
                    Status::Error(StatusCode::kInternal, error.what()));
    } catch (...) {
        return Fail(ExitCode::kInternal,
                    Status::Error(StatusCode::kInternal,
                                  "unexpected internal error"));
    }
}
