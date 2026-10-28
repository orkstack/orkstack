# images/activity-json/Dockerfile
#
# Same Dockerfile contract as activity-shell (§10.1): digest-pinned base,
# non-root USER, no VOLUME, exec-form ENTRYPOINT. Annotations sit above their
# instructions because Dockerfile has no inline comment — see the note in
# images/activity-shell/Dockerfile.
#
# jq is the whole toolchain: this image does not install a JSON library, it
# installs the JSON tool every first-party entry script already uses and lets
# a document point it at a document. The `~1.7` constraint admits Alpine's
# patch releases of jq 1.7 and refuses another minor version, so a rebuild
# cannot silently change what a filter computes.
#
# tzdata is what makes a task's own TZ (say Europe/Berlin) mean that zone:
# without zone data, musl reads every zone name as UTC, silently.
FROM alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc

RUN apk add --no-cache 'jq~1.7' tzdata

COPY entry.sh /usr/local/bin/entry.sh
RUN chmod 0555 /usr/local/bin/entry.sh

# Image default: HOME is the per-attempt, writable, empty /orkstack/tmp rather
# than Docker's read-only /, so jq finds no ~/.jq. A task's env: overrides it.
ENV HOME=/orkstack/tmp
# Image default: local time is UTC (localtime, strflocaltime). A task's env:
# overrides it with any zone tzdata knows.
ENV TZ=UTC

# Never root.
USER 65532:65532

# The with: contract, declared (docs/ACTIVITY_IMAGES.md, "Declaring the
# interface"). Optional: the engine checks a document's with: against it at
# validate and apply; the container never reads it. The line is generated
# from interface.ork.yaml by `go run ./internal/schemas/cmd/genschemas`: edit
# that file, never this line.
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"ops":{"merge":{"fields":{"as":{"type":"string"},"srcs":{"type":"array","required":true}}},"query":{"fields":{"as":{"type":"string"},"filter":{"type":"string","required":true},"gather":{"type":"string"},"nullInput":{"type":"boolean","default":false},"slurp":{"type":"boolean","default":false},"src":{"type":"string"},"srcs":{"type":"array"},"vars":{"type":"object"}}}},"values":{"path":{"type":"string"},"sha256":{"type":"string"},"sizeBytes":{"type":"number"}}}'

# Exec form, mandatory.
ENTRYPOINT ["/usr/local/bin/entry.sh"]
