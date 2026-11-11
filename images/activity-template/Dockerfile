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
FROM alpine:3.24@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a AS gomplate
ARG TARGETARCH
ARG GOMPLATE_VERSION=5.3.1
# Where the release asset is fetched; an air-gapped builder points it at a mirror.
ARG GOMPLATE_BASE_URL=https://github.com/hairyhenderson/gomplate/releases/download
# From checksums-v5.3.1_sha256.txt of the v5.3.1 GitHub release.
ARG GOMPLATE_SHA256_AMD64=2a618bfb02a54dd3c76d92ae4375ccc1f33eeb0545d6565ea7c3388359cb93f8
ARG GOMPLATE_SHA256_ARM64=d00242bd08eda5badcea34491b392437237722e7d7e00f8ed05686124a43f61b
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

FROM alpine:3.24@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a

# jq builds the root context and checks JSON datasources before the render;
# tzdata is what makes a task's own TZ (say Europe/Berlin) mean that zone —
# without zone data, Go reads every zone name as UTC, silently.
# zlib~1.4: zlib 1.4 fixes CVE-2026-4738, and alpine 3.24.4, the newest 3.24
# base, still ships 1.3.2, so the package is upgraded here. Drop it once the
# pinned base carries zlib 1.4.
RUN apk add --no-cache 'zlib~1.4' 'jq~1.8' tzdata

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

# The with: contract, the icon and the descriptions, declared
# (docs/ACTIVITY_IMAGES.md, "Declaring the interface"). Optional: the engine
# checks a document's with: against the interface at validate and apply, and
# clients draw the icon and show the descriptions; the container never reads
# them. The three lines are generated from interface.ork.yaml by
# `go run ./internal/schemas/cmd/genschemas`: edit that file, never these lines.
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"ops":{"render":{"fields":{"as":{"type":"string"},"data":{"type":"string"},"datasources":{"type":"object"},"leftDelim":{"type":"string"},"rightDelim":{"type":"string"},"src":{"type":"string","required":true},"values":{"type":"object"}}}},"values":{"path":{"type":"string"},"sha256":{"type":"string"},"sizeBytes":{"type":"number"}}}'
LABEL org.orkstack.activity.icon='<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><rect x="2" y="2.5" width="12" height="11" rx="1.8" fill="none" stroke="currentColor" stroke-width="1.4"/><path d="M2 6h12M6.2 6v7.5" stroke="currentColor" stroke-width="1.4"/></svg>'
LABEL org.orkstack.activity.docs='{"schemaVersion":1,"description":"Renders one text file from a template with gomplate, from a JSON data file, inline values and named datasources. A key the template reads that the data does not hold is an error, not an empty string.","ops":{"render":{"description":"Renders `src` into one output artifact.","fields":{"as":"The output artifact\u0027s name. Defaults to the base name of `src` without `.tmpl`, `.tpl` or `.gotmpl`.","data":"A JSON file holding an object: the template\u0027s root data, and the datasource `data`.","datasources":"Datasource name to file path. A file ending in `.yaml` or `.yml` is read as YAML, any other as JSON.","leftDelim":"The opening delimiter to use instead of `{{`, for a file that holds `{{` itself. Set together with `rightDelim`.","rightDelim":"The closing delimiter to use instead of `}}`. Set together with `leftDelim`.","src":"The template file. A relative path resolves under `/orkstack/input`.","values":"An inline object deep-merged over `data` for the root data, and the datasource `values`."}}},"values":{"path":"The artifact the render wrote.","sha256":"The artifact\u0027s SHA-256 in hex.","sizeBytes":"The artifact\u0027s size in bytes."}}'

# Exec form, mandatory.
ENTRYPOINT ["/usr/local/bin/entry.sh"]
