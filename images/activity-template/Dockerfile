# images/activity-template/Dockerfile
#
# Same Dockerfile contract as activity-shell (§10.1): digest-pinned base (on
# EVERY stage, the throw-away one included), non-root USER, no VOLUME,
# exec-form ENTRYPOINT. Annotations sit above their instructions because
# Dockerfile has no inline comment — see the note in
# images/activity-shell/Dockerfile.
#
# The renderer is gomplate (https://github.com/hairyhenderson/gomplate), an
# ordinary standalone CLI, installed as its PUBLISHED RELEASE BINARY and
# verified against the release's SHA-256 before it is copied anywhere. There
# is no Go build stage: the bytes that run are the bytes the project released.
#
# The checksums live here, in the Dockerfile, never in a file downloaded beside
# the binary at build time — a checksum fetched from the same release would
# verify nothing. For the same reason GOMPLATE_BASE_URL may point at any
# mirror (an air-gapped builder passes
# --build-arg GOMPLATE_BASE_URL=<mirror>, e.g. through images/Makefile's
# DOCKER_BUILD_FLAGS): the bytes are verified whatever served them.
#
# TARGETARCH is set by BuildKit, so this build requires BuildKit (Docker 23+,
# where it is the default, or DOCKER_BUILDKIT=1). On the legacy builder it is
# empty and the build stops with "no pinned gomplate checksum for architecture
# 'unset'". An architecture without a pinned checksum fails the same way,
# explicitly.
FROM alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc AS gomplate
ARG TARGETARCH
ARG GOMPLATE_VERSION=4.3.3
# Where the release asset is fetched; an air-gapped builder points it at a mirror.
ARG GOMPLATE_BASE_URL=https://github.com/hairyhenderson/gomplate/releases/download
# From checksums-v4.3.3_sha256.txt of the v4.3.3 GitHub release.
ARG GOMPLATE_SHA256_AMD64=ca281666e86f2f09218c1653e1908f572c0e349e9de64cb4ea93ade9333f0596
ARG GOMPLATE_SHA256_ARM64=25bd73720ff470cec0dfaa31ca2c436b30f86142db9af68382ce64e0f3e7b9c5
RUN set -eu; \
    case "${TARGETARCH:-}" in \
      amd64) sum="$GOMPLATE_SHA256_AMD64" ;; \
      arm64) sum="$GOMPLATE_SHA256_ARM64" ;; \
      *) echo "no pinned gomplate checksum for architecture '${TARGETARCH:-unset}'" >&2; exit 1 ;; \
    esac; \
    wget -q -O /tmp/gomplate \
      "${GOMPLATE_BASE_URL}/v${GOMPLATE_VERSION}/gomplate_linux-${TARGETARCH}"; \
    echo "${sum}  /tmp/gomplate" | sha256sum -c -; \
    mkdir -p /out; install -m 0555 /tmp/gomplate /out/gomplate

FROM alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc

# jq builds the root context and checks JSON datasources before the render;
# tzdata is what makes a task's own TZ (say Europe/Berlin) mean that zone —
# without zone data, Go reads every zone name as UTC, silently.
RUN apk add --no-cache 'jq~1.7' tzdata

COPY --from=gomplate /out/gomplate /usr/local/bin/gomplate
COPY entry.sh /usr/local/bin/entry.sh
RUN chmod 0555 /usr/local/bin/entry.sh

# Image default: HOME is the per-attempt, writable, empty /orkstack/tmp rather
# than Docker's read-only /. A task's env: overrides it.
ENV HOME=/orkstack/tmp
# Image default: local time is UTC (time.ZoneName, .Local). A task's env:
# overrides it with any zone tzdata knows.
ENV TZ=UTC

# Never root.
USER 65532:65532

# The with: contract, declared (docs/ACTIVITY_IMAGES.md, "Declaring the
# interface"). Optional: the engine checks a document's with: against it at
# validate and apply; the container never reads it. One line, because the
# value is JSON; README.md's Interface section reproduces it verbatim.
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"ops":{"render":{"fields":{"src":{"type":"string","required":true},"data":{"type":"string"},"values":{"type":"object"},"datasources":{"type":"object"},"leftDelim":{"type":"string"},"rightDelim":{"type":"string"},"as":{"type":"string"}}}},"values":{"path":{"type":"string"},"sizeBytes":{"type":"number"},"sha256":{"type":"string"}}}'

# Exec form, mandatory.
ENTRYPOINT ["/usr/local/bin/entry.sh"]
