# images/activity-http/Dockerfile
#
# Same Dockerfile contract as activity-shell (§10.1): digest-pinned base,
# non-root USER, no VOLUME, exec-form ENTRYPOINT. Annotations sit above their
# instructions because Dockerfile has no inline comment — see the note in
# images/activity-shell/Dockerfile.
#
# jq reads and writes the JSON; curl performs the request. Neither is an SDK —
# they are the Unix tools for the job, and any image may use them.
FROM alpine:3.24@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a

RUN apk add --no-cache jq curl ca-certificates

COPY entry.sh /usr/local/bin/entry.sh
RUN chmod 0555 /usr/local/bin/entry.sh

# Never root.
USER 65532:65532

# The with: contract, declared (docs/ACTIVITY_IMAGES.md, "Declaring the
# interface"). Optional: the engine checks a document's with: against it at
# validate and apply; the container never reads it. The line is generated
# from interface.ork.yaml by `go run ./internal/schemas/cmd/genschemas`: edit
# that file, never this line.
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"fields":{"body":{"type":"string|object|array"},"captureBodyAs":{"type":"string"},"headers":{"type":"object"},"timeoutSeconds":{"type":"number"},"url":{"type":"string","required":true}},"ops":{"delete":{},"get":{},"head":{},"patch":{},"post":{},"put":{}},"defaultOp":"get","values":{"durationMs":{"type":"number"},"headers":{"type":"object"},"json":{"type":"object"},"sizeBytes":{"type":"number"},"status":{"type":"number"}}}'

# Exec form, mandatory.
ENTRYPOINT ["/usr/local/bin/entry.sh"]
