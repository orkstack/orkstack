# images/activity-file/Dockerfile
#
# Same Dockerfile contract as activity-shell (§10.1): digest-pinned base,
# non-root USER, no VOLUME, exec-form ENTRYPOINT. Annotations sit above their
# instructions because Dockerfile has no inline comment — see the note in
# images/activity-shell/Dockerfile.
#
# The tools are jq, tar, gzip and sha256sum — all but jq already in busybox.
# Nothing else is installed, because AGENTS.md §9 says to keep the file
# activity small and Unix-like rather than growing a general file-processing
# framework.
FROM alpine:3.24@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a

RUN apk add --no-cache jq

COPY entry.sh /usr/local/bin/entry.sh
RUN chmod 0555 /usr/local/bin/entry.sh

# Never root.
USER 65532:65532

# The with: contract, declared (docs/ACTIVITY_IMAGES.md, "Declaring the
# interface"). Optional: the engine checks a document's with: against it at
# validate and apply; the container never reads it. The line is generated
# from interface.ork.yaml by `go run ./internal/schemas/cmd/genschemas`: edit
# that file, never this line.
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"ops":{"checksum":{"fields":{"src":{"type":"string","required":true}}},"concat":{"fields":{"as":{"type":"string","required":true},"srcs":{"type":"array","required":true}}},"copy":{"fields":{"as":{"type":"string"},"src":{"type":"string","required":true}}},"gunzip":{"fields":{"as":{"type":"string"},"src":{"type":"string","required":true}}},"gzip":{"fields":{"as":{"type":"string"},"src":{"type":"string","required":true}}},"tar":{"fields":{"as":{"type":"string"},"srcs":{"type":"array","required":true}}},"untar":{"fields":{"as":{"type":"string"},"member":{"type":"string","required":true},"src":{"type":"string","required":true}}}},"values":{"path":{"type":"string"},"sha256":{"type":"string"},"sizeBytes":{"type":"number"}}}'

# Exec form, mandatory.
ENTRYPOINT ["/usr/local/bin/entry.sh"]
