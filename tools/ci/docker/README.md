# Hermetic Schema CodeGen Docker preflight

This check builds and runs two isolated Linux user-space environments:

- Ubuntu 22.04 with GCC 12, one Bazel job, workspace `/workspace-gcc`;
- Ubuntu 24.04 with Clang 18, four Bazel jobs, workspace `/nested/workspace-clang`.

The image build phase downloads Bazel and external repositories. The comparison
containers then run with `--network=none`, without shared Bazel caches. Each
container collects the same 15 generated header, source, and descriptor outputs.
The host compares their logical-path SHA-256 manifests and root hashes.

Run from the repository root:

```sh
python3 tools/ci/run_schema_codegen_docker_check.py
```

Use `--out=/some/path` to retain evidence elsewhere, or `--skip-build` to reuse
the two previously built images.

This is a strong cross-environment preflight, but both containers still share a
host kernel, CPU architecture, and storage stack. The GitHub Actions
`Schema Extended Validation` workflow remains the formal cross-machine check.
