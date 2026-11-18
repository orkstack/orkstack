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
FROM gcr.io/distroless/static-debian13:nonroot@sha256:aafe993665bf6200db38a02c11a9351e782a84b6a45e7f33b6695638c88e3931 AS ork
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

FROM gcr.io/distroless/static-debian13:nonroot@sha256:aafe993665bf6200db38a02c11a9351e782a84b6a45e7f33b6695638c88e3931 AS orkctl
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
FROM docker:29.9.3-cli@sha256:5a7af0dcb0e46c803efe9b45371dd21cb500e5b4453250394ac6f06d49b6e6f1 AS docker-cli

#
# org.orkstack.build is the commit its binary was built from, which `make dist`
# stamps into it: an Orker that offers device profiles runs this image as its
# probe image and refuses one whose label is not its own commit
# (docs/DEPLOY.md, "Running a device Orker").
FROM gcr.io/distroless/static-debian13:latest@sha256:1a64f46364620465ef9e9a8f5413757dfdf22695e050bbfd7303eeaf17a59383 AS orker
ARG TARGETARCH
ARG VERSION=dev
ARG BUILD=unknown
LABEL org.opencontainers.image.title="orker" \
      org.opencontainers.image.description="The OrkStack worker: runs each task's OCI image" \
      org.opencontainers.image.source="https://github.com/orkstack/orkstack" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}" \
      org.orkstack.build="${BUILD}"
COPY LICENSE /usr/share/doc/orkstack/LICENSE
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --chmod=0755 dist/bin/linux-${TARGETARCH}/orker /usr/local/bin/orker
USER root
ENTRYPOINT ["/usr/local/bin/orker"]
