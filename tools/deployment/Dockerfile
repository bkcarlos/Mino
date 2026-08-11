# Mandatory immutable references. Release automation records both values in the
# provenance document produced by build-image.sh.
ARG BUILD_IMAGE
ARG RUNTIME_IMAGE

FROM ${BUILD_IMAGE} AS build
ARG BUILD_IMAGE
ARG RUNTIME_IMAGE
RUN case "${BUILD_IMAGE}" in *@sha256:*) ;; *) echo "BUILD_IMAGE must use @sha256" >&2; exit 2 ;; esac && \
    case "${RUNTIME_IMAGE}" in *@sha256:*) ;; *) echo "RUNTIME_IMAGE must use @sha256" >&2; exit 2 ;; esac
WORKDIR /src
COPY . .
# MODULE.bazel.lock is authoritative. Bazel's build/download cache remains in
# this discarded build stage and is never copied into the runtime image.
RUN bazel build --config=release --lockfile_mode=error \
    //tools/mino:mino \
    //tools/deployment:mino_deploy \
    //tools/deployment:mino_node

FROM ${RUNTIME_IMAGE} AS runtime
ARG BUILD_IMAGE
ARG RUNTIME_IMAGE
ARG IMAGE_CREATED
ARG IMAGE_REVISION
ARG IMAGE_VERSION
LABEL org.opencontainers.image.title="Mino controlled node runtime" \
      org.opencontainers.image.description="LocalBus, Monitoring, and Recorder node supervised by mino-deploy" \
      org.opencontainers.image.source="https://github.com/bkcarlos/Mino" \
      org.opencontainers.image.created="${IMAGE_CREATED}" \
      org.opencontainers.image.revision="${IMAGE_REVISION}" \
      org.opencontainers.image.version="${IMAGE_VERSION}" \
      org.opencontainers.image.base.name="${RUNTIME_IMAGE}" \
      org.mino.build.base="${BUILD_IMAGE}" \
      org.mino.sbom.format="spdx-json" \
      org.mino.runtime.contract="tools/deployment/image-contract.json"
COPY --from=build --chown=65532:65532 /src/bazel-bin/tools/mino/mino /usr/local/bin/mino
COPY --from=build --chown=65532:65532 /src/bazel-bin/tools/deployment/mino_deploy /usr/local/bin/mino-deploy
COPY --from=build --chown=65532:65532 /src/bazel-bin/tools/deployment/mino_node /usr/local/bin/mino-node
COPY --chown=0:0 configs/node.container-recorder.toml /etc/mino/node.toml
COPY --chown=0:0 configs/security-domains.toml /etc/mino/security-domains.toml
COPY --chown=0:0 tools/deployment/image-contract.json /usr/share/mino/image-contract.json
USER 65532:65532
WORKDIR /var/lib/mino
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD ["/usr/local/bin/mino-deploy", "probe", "--config", "/etc/mino/node.toml", "--kind", "health"]
ENTRYPOINT ["/usr/local/bin/mino-deploy"]
CMD ["start", "--config", "/etc/mino/node.toml", "--", "/usr/local/bin/mino-node", "--config", "/etc/mino/node.toml"]
