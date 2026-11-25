# images/activity-template/Dockerfile
#
# Same Dockerfile contract as activity-shell (§10.1): digest-pinned base (on
# EVERY stage, the throw-away one included), non-root USER, no VOLUME,
# exec-form ENTRYPOINT. Annotations sit above their instructions because
# Dockerfile has no inline comment — see the note in
# images/activity-shell/Dockerfile.
#
# The renderer is gomplate (https://github.com/hairyhenderson/gomplate), an
# ordinary standalone CLI, taken as the static binary of the project's own
# image for the release, hairyhenderson/gomplate:v5.3.1, pinned by the digest
# of its multi-arch index (linux/amd64 and linux/arm64 among them). There is
# no Go build stage and no download: the bytes that run are the bytes the
# project published, fixed by that digest, and a registry pull is retried by
# the builder where a release asset fetched from github.com was refused with
# HTTP 429 on shared CI runners.
#
# An air-gapped builder mirrors that image by its digest and points the build
# at the copy, e.g. through images/Makefile's DOCKER_BUILD_FLAGS:
#   --build-context docker.io/hairyhenderson/gomplate:v5.3.1@sha256:<digest>=docker-image://<mirror>/gomplate@sha256:<digest>
FROM docker.io/hairyhenderson/gomplate:v5.3.1@sha256:9c73c3cacc598cc9d926c67c5548901a4a4ee2223d29a2dacc910a68a5dfd684 AS gomplate

FROM alpine:3.24@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf

# jq builds the root context and checks JSON datasources before the render;
# tzdata is what makes a task's own TZ (say Europe/Berlin) mean that zone —
# without zone data, Go reads every zone name as UTC, silently.
# zlib~1.4: zlib 1.4 fixes CVE-2026-4738, and alpine 3.24.4, the newest 3.24
# base, still ships 1.3.2, so the package is upgraded here. Drop it once the
# pinned base carries zlib 1.4.
# libcrypto3 and libssl3: 3.5.9-r1 fixes CVE-2026-88823, CVE-2026-95691,
# CVE-2026-95692 and CVE-2026-107496, and the same base still ships 3.5.9-r0.
# Drop them once the pinned base carries 3.5.9-r1.
RUN apk add --no-cache 'zlib~1.4' 'libcrypto3>=3.5.9-r1' 'libssl3>=3.5.9-r1' 'jq~1.8' tzdata

COPY --from=gomplate --chmod=0555 /gomplate /usr/local/bin/gomplate
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
