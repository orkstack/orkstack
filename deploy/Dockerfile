# OrkStack's release images, one target each: ork, orker and orkctl.
#
# `make release-images` builds them; deploy/Dockerfile.dockerignore admits only
# what they copy. The Go binaries are not compiled here: `make dist` builds
# them once for the release archives, and these images copy the same files
# from dist/bin/linux-<arch>/, so an image's binary is byte for byte the one
# in the archive and no target platform ever runs a RUN step. The web UI has
# no image of its own: `make dist` builds it first (`make ui`) and compiles it
# into ork, which serves it on its API port, so the ork image carries it
# inside its binary. Every base is pinned by its multi-arch index digest
# (AGENTS.md §11.1); a bump is a pull request that proves it.

# The engine and the CLI need nothing but CA certificates and tzdata, and run
# as a non-root user.
FROM gcr.io/distroless/static-debian13:nonroot@sha256:18f4cc26c9ffa7f20a359298be7deadac5c607c8e03a02b8119f1a5fc2f82c99 AS ork
ARG TARGETARCH
ARG VERSION=dev
LABEL org.opencontainers.image.title="ork" \
      org.opencontainers.image.description="The OrkStack engine: API, web UI, Temporal workflows, archive" \
      org.opencontainers.image.source="https://github.com/orkstack/orkstack" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}"
COPY LICENSE /usr/share/doc/orkstack/LICENSE
COPY --chmod=0755 dist/bin/linux-${TARGETARCH}/ork /usr/local/bin/ork
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/ork"]

FROM gcr.io/distroless/static-debian13:nonroot@sha256:18f4cc26c9ffa7f20a359298be7deadac5c607c8e03a02b8119f1a5fc2f82c99 AS orkctl
ARG TARGETARCH
ARG VERSION=dev
LABEL org.opencontainers.image.title="orkctl" \
      org.opencontainers.image.description="The OrkStack command line" \
      org.opencontainers.image.source="https://github.com/orkstack/orkstack" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}"
COPY LICENSE /usr/share/doc/orkstack/LICENSE
COPY --chmod=0755 dist/bin/linux-${TARGETARCH}/orkctl /usr/local/bin/orkctl
WORKDIR /work
ENTRYPOINT ["/usr/local/bin/orkctl"]

# The Orker drives the docker CLI against the host's daemon, through the
# socket its deployment mounts (docs/DEPLOY.md, "Running the published
# images"): that makes it root on the host whatever user it runs as, so it
# runs as root and says so. The CLI is the static binary of the docker image
# whose version this pins.
FROM docker:29.9.2-cli@sha256:5e6baf67325a79f64ed3bed6863954d5e05d0f3d4e57cbdb14809346814bbfdb AS docker-cli

FROM gcr.io/distroless/static-debian13:latest@sha256:15f8637509792e05b97d83451b0b6ccfa4abd548f997e406ac1e72c87fe1fa78 AS orker
ARG TARGETARCH
ARG VERSION=dev
LABEL org.opencontainers.image.title="orker" \
      org.opencontainers.image.description="The OrkStack worker: runs each task's OCI image" \
      org.opencontainers.image.source="https://github.com/orkstack/orkstack" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}"
COPY LICENSE /usr/share/doc/orkstack/LICENSE
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --chmod=0755 dist/bin/linux-${TARGETARCH}/orker /usr/local/bin/orker
USER root
ENTRYPOINT ["/usr/local/bin/orker"]
