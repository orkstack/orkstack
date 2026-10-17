# images/activity-http/Dockerfile
#
# Same Dockerfile contract as activity-shell (§10.1): digest-pinned base,
# non-root USER, no VOLUME, exec-form ENTRYPOINT. Annotations sit above their
# instructions because Dockerfile has no inline comment — see the note in
# images/activity-shell/Dockerfile.
#
# jq reads and writes the JSON; curl performs the request. Neither is an SDK —
# they are the Unix tools for the job, and any image may use them.
FROM alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc

RUN apk add --no-cache jq curl ca-certificates

COPY entry.sh /usr/local/bin/entry.sh
RUN chmod 0555 /usr/local/bin/entry.sh

# Never root.
USER 65532:65532

# The with: contract, declared (docs/ACTIVITY_IMAGES.md, "Declaring the
# interface"). Optional: the engine checks a document's with: against it at
# validate and apply; the container never reads it. One line, because the
# value is JSON; README.md's Interface section reproduces it verbatim.
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"fields":{"url":{"type":"string","required":true},"headers":{"type":"object"},"body":{"type":"string|object|array"},"timeoutSeconds":{"type":"number"},"captureBodyAs":{"type":"string"}},"ops":{"get":{},"post":{},"put":{},"patch":{},"delete":{},"head":{}},"defaultOp":"get","values":{"status":{"type":"number"},"headers":{"type":"object"},"durationMs":{"type":"number"},"sizeBytes":{"type":"number"},"json":{"type":"object"}}}'

# Exec form, mandatory.
ENTRYPOINT ["/usr/local/bin/entry.sh"]
