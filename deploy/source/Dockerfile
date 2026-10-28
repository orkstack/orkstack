# OrkStack built from this checkout, for the quickstart compose file at the
# repository root (compose.yaml, docs/GETTING_STARTED.md): one target each for
# ork, orker, orkctl and ui.
#
# The release images are deploy/Dockerfile's, built from the binaries
# `make dist` writes; these compile the same commands here instead, so a
# clone and Docker are all the quickstart needs. Each runtime stage is the
# release's own, on the same pinned base (TestSourceDockerfile_StagesAreTheReleasesOwn),
# and every base is pinned by its multi-arch index digest (AGENTS.md §11.1).
# deploy/source/Dockerfile.dockerignore admits only what these stages copy.

# The three commands, compiled once on the build platform for the target's
# architecture: static binaries, so the distroless bases run them as they are,
# with the release's build tags (internal/dist BuildTags: the time zone
# database linked in).
FROM --platform=$BUILDPLATFORM golang:1.27.3-alpine@sha256:d200d2dd76b03a99b52651fd1325aff7dcd35422c35c01bac56571b0507f3deb AS binaries
ARG TARGETARCH
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
COPY cmd/ cmd/
COPY gen/go/ gen/go/
COPY internal/ internal/
COPY pkg/ pkg/
RUN --mount=type=cache,target=/go/pkg/mod \
	--mount=type=cache,target=/root/.cache/go-build \
	CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH GOTOOLCHAIN=local \
	go build -trimpath -buildvcs=false -tags timetzdata -ldflags "-X main.version=${VERSION}" -o /out/ ./cmd/ork ./cmd/orker ./cmd/orkctl

FROM gcr.io/distroless/static-debian13:nonroot@sha256:18f4cc26c9ffa7f20a359298be7deadac5c607c8e03a02b8119f1a5fc2f82c99 AS ork
COPY LICENSE /usr/share/doc/orkstack/LICENSE
COPY --from=binaries /out/ork /usr/local/bin/ork
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/ork"]

FROM gcr.io/distroless/static-debian13:nonroot@sha256:18f4cc26c9ffa7f20a359298be7deadac5c607c8e03a02b8119f1a5fc2f82c99 AS orkctl
COPY LICENSE /usr/share/doc/orkstack/LICENSE
COPY --from=binaries /out/orkctl /usr/local/bin/orkctl
WORKDIR /work
ENTRYPOINT ["/usr/local/bin/orkctl"]

# The Orker drives the docker CLI against the host's daemon, through the
# socket compose.yaml mounts: that makes it root on the host whatever user it
# runs as, so it runs as root and says so.
FROM docker:29.9.2-cli@sha256:5e6baf67325a79f64ed3bed6863954d5e05d0f3d4e57cbdb14809346814bbfdb AS docker-cli

FROM gcr.io/distroless/static-debian13:latest@sha256:15f8637509792e05b97d83451b0b6ccfa4abd548f997e406ac1e72c87fe1fa78 AS orker
COPY LICENSE /usr/share/doc/orkstack/LICENSE
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=binaries /out/orker /usr/local/bin/orker
USER root
ENTRYPOINT ["/usr/local/bin/orker"]

# The UI, with the repository's examples, which its gallery reads at build
# time.
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
WORKDIR /app
COPY LICENSE /usr/share/doc/orkstack/LICENSE
COPY --from=ui-deps /deps/node_modules ./node_modules
COPY --from=ui-build /src/ui/package.json ./package.json
COPY --from=ui-build /src/ui/build ./build
ENV NODE_ENV=production HOST=0.0.0.0 PORT=5173
USER node
EXPOSE 5173
ENTRYPOINT ["node", "build"]
