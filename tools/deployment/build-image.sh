#!/bin/sh
# Build a reviewed Mino image and emit digest, SPDX SBOM, and provenance evidence.
set -eu

if [ "$#" -ne 4 ]; then
    echo "usage: $0 <image-ref> <build-image@sha256:...> <runtime-image@sha256:...> <output-dir>" >&2
    exit 2
fi

image_ref=$1
build_image=$2
runtime_image=$3
output_dir=$4
case "$build_image" in *@sha256:*) ;; *) echo "build image must be digest-pinned" >&2; exit 2 ;; esac
case "$runtime_image" in *@sha256:*) ;; *) echo "runtime image must be digest-pinned" >&2; exit 2 ;; esac
command -v docker >/dev/null 2>&1 || { echo "docker is required" >&2; exit 2; }
command -v syft >/dev/null 2>&1 || { echo "syft is required for release SBOM generation" >&2; exit 2; }

mkdir -p "$output_dir"
created=$(date -u +%Y-%m-%dT%H:%M:%SZ)
revision=$(git rev-parse HEAD)
source_dirty=false
if ! git diff --quiet || ! git diff --cached --quiet ||
   [ -n "$(git ls-files --others --exclude-standard)" ]; then
    source_dirty=true
fi
version=${MINO_IMAGE_VERSION:-dev}
metadata="$output_dir/buildkit-metadata.json"
iid="$output_dir/image.iid"
sbom="$output_dir/mino.spdx.json"
provenance="$output_dir/provenance.json"

docker buildx build --load \
    --file tools/deployment/Dockerfile \
    --tag "$image_ref" \
    --build-arg "BUILD_IMAGE=$build_image" \
    --build-arg "RUNTIME_IMAGE=$runtime_image" \
    --build-arg "IMAGE_CREATED=$created" \
    --build-arg "IMAGE_REVISION=$revision" \
    --build-arg "IMAGE_VERSION=$version" \
    --metadata-file "$metadata" \
    --iidfile "$iid" \
    --provenance=mode=max \
    .

syft scan "$image_ref" --output "spdx-json=$sbom"
python3 tools/deployment/emit_image_provenance.py \
    --image-ref "$image_ref" \
    --build-image "$build_image" \
    --runtime-image "$runtime_image" \
    --revision "$revision" \
    --source-dirty "$source_dirty" \
    --created "$created" \
    --iid-file "$iid" \
    --metadata-file "$metadata" \
    --sbom-file "$sbom" \
    --output "$provenance"

image_digest=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["image_digest"])' "$provenance")
echo "image_digest=$image_digest"
echo "sbom=$sbom"
echo "provenance=$provenance"
