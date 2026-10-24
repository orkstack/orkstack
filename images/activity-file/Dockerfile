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
FROM alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc

RUN apk add --no-cache jq

COPY entry.sh /usr/local/bin/entry.sh
RUN chmod 0555 /usr/local/bin/entry.sh

# Never root.
USER 65532:65532

# The with: contract, declared (docs/ACTIVITY_IMAGES.md, "Declaring the
# interface"). Optional: the engine checks a document's with: against it at
# validate and apply; the container never reads it. One line, because the
# value is JSON; README.md's Interface section reproduces it verbatim.
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"ops":{"copy":{"fields":{"src":{"type":"string","required":true},"as":{"type":"string"}}},"checksum":{"fields":{"src":{"type":"string","required":true}}},"gzip":{"fields":{"src":{"type":"string","required":true},"as":{"type":"string"}}},"gunzip":{"fields":{"src":{"type":"string","required":true},"as":{"type":"string"}}},"tar":{"fields":{"srcs":{"type":"array","required":true},"as":{"type":"string"}}},"untar":{"fields":{"src":{"type":"string","required":true},"member":{"type":"string","required":true},"as":{"type":"string"}}},"concat":{"fields":{"srcs":{"type":"array","required":true},"as":{"type":"string","required":true}}}},"values":{"path":{"type":"string"},"sizeBytes":{"type":"number"},"sha256":{"type":"string"}}}'

# Exec form, mandatory.
ENTRYPOINT ["/usr/local/bin/entry.sh"]
