"""Hermetic Bazel rule for Mino C++ schema generation."""

def _mino_cc_codegen_impl(ctx):
    args = ctx.actions.args()
    args.add("--input", ctx.file.src.path)
    args.add("--output_header", ctx.outputs.out_header.path)
    args.add("--output_source", ctx.outputs.out_source.path)
    args.add("--output_descriptor", ctx.outputs.out_descriptor.path)
    args.add("--header_include", ctx.outputs.out_header.short_path)
    for import_file in ctx.files.imports:
        # IDL imports are workspace-relative logical paths. Passing only this
        # explicit map makes the action sandbox an import allowlist.
        args.add("--import", import_file.short_path + "=" + import_file.path)

    ctx.actions.run(
        executable = ctx.executable._minoc,
        arguments = [args],
        inputs = depset(
            direct = [ctx.file.src] + ctx.files.imports,
        ),
        outputs = [
            ctx.outputs.out_header,
            ctx.outputs.out_source,
            ctx.outputs.out_descriptor,
        ],
        mnemonic = "MinoCodeGen",
        progress_message = "Generating C++ schema artifacts for %{label}",
    )
    return [
        DefaultInfo(files = depset([
            ctx.outputs.out_header,
            ctx.outputs.out_source,
        ])),
        OutputGroupInfo(descriptor = depset([ctx.outputs.out_descriptor])),
    ]

mino_cc_codegen = rule(
    implementation = _mino_cc_codegen_impl,
    attrs = {
        "src": attr.label(
            allow_single_file = [".mino"],
            mandatory = True,
            doc = "Root Mino IDL source.",
        ),
        "imports": attr.label_list(
            allow_files = [".mino"],
            doc = "Complete allowlist of transitive IDL imports.",
        ),
        "out_header": attr.output(mandatory = True),
        "out_source": attr.output(mandatory = True),
        "out_descriptor": attr.output(mandatory = True),
        "_minoc": attr.label(
            default = Label("//tools/minoc:minoc"),
            executable = True,
            cfg = "exec",
        ),
    },
    doc = "Generates deterministic C++ and descriptor artifacts from Mino IDL.",
)
