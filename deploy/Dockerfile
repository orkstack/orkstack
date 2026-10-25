# OrkStack's release images, one target each: ork, orker, orkctl and ui.
#
# `make release-images` builds them; deploy/Dockerfile.dockerignore admits only
# what they copy. The Go binaries are not compiled here: `make dist` builds
# them once for the release archives, and these images copy the same files
# from dist/bin/linux-<arch>/, so an image's binary is byte for byte the one
# in the archive and no target platform ever runs a RUN step. Every base is
# pinned by its multi-arch index digest (AGENTS.md §11.1); a bump is a pull
# request that proves it.

# The engine and the CLI need nothing but CA certificates and tzdata, and run
# as a non-root user.
FROM gcr.io/distroless/static-debian13:nonroot@sha256:18f4cc26c9ffa7f20a359298be7deadac5c607c8e03a02b8119f1a5fc2f82c99 AS ork
ARG TARGETARCH
ARG VERSION=dev
LABEL org.opencontainers.image.title="ork" \
      org.opencontainers.image.description="The OrkStack engine: API, Temporal workflows, archive" \
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

# The UI: built once on the build platform (its output is JavaScript), with
# the repository's examples, which its gallery reads at build time.
FROM --platform=$BUILDPLATFORM oven/bun:1.4.3@sha256:ec06c3b6cea04192ae6770c434f668ca41d343ad19fa6472216c7b48be39c598 AS ui-build
WORKDIR /src/ui
COPY ui/package.json ui/bun.lock ui/bunfig.toml ./
RUN bun install --frozen-lockfile
COPY examples/ /src/examples/
COPY ui/ ./
RUN bun run build

FROM --platform=$BUILDPLATFORM oven/bun:1.4.3@sha256:ec06c3b6cea04192ae6770c434f668ca41d343ad19fa6472216c7b48be39c598 AS ui-deps
WORKDIR /deps
COPY ui/package.json ui/bun.lock ui/bunfig.toml ./
RUN bun install --frozen-lockfile --production

FROM node:26.12.0-trixie-slim@sha256:e556a144463cc8dc53afe8de6d2fa0db5c132fd39ffa232c17641bd40df2bf11 AS ui
ARG VERSION=dev
LABEL org.opencontainers.image.title="ui" \
      org.opencontainers.image.description="The OrkStack web UI" \
      org.opencontainers.image.source="https://github.com/orkstack/orkstack" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}"
WORKDIR /app
COPY LICENSE /usr/share/doc/orkstack/LICENSE
COPY --from=ui-deps /deps/node_modules ./node_modules
COPY --from=ui-build /src/ui/package.json ./package.json
COPY --from=ui-build /src/ui/build ./build
ENV NODE_ENV=production HOST=0.0.0.0 PORT=5173
USER node
EXPOSE 5173
ENTRYPOINT ["node", "build"]
