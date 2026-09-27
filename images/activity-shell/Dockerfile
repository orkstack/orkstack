# images/activity-shell/Dockerfile
#
# The Dockerfile contract of docs/OCI_ACTIVITY_CONTRACT.md §10.1, applied.
# Every rule it names is visible here:
#
#   rule 1  ENTRYPOINT in exec form (shell form does not forward SIGTERM)
#   rule 3  USER is not root
#   rule 4  no VOLUME — it would shadow the workspace mounts
#   rule 5  the base image is digest-pinned inside the Dockerfile
#
# The annotations live above their instructions, never trailing them: a
# Dockerfile has no inline comment, so `USER 65532:65532   # never root`
# makes the group literally "65532   # never root" and the daemon refuses the
# container with "unable to find group".
#
# alpine:3.20's index digest below is the one §14.4 resolved and §14.15 used.
FROM alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc

# jq is the only "SDK": a JSON reader.
RUN apk add --no-cache jq

COPY entry.sh /usr/local/bin/entry.sh
RUN chmod 0555 /usr/local/bin/entry.sh

# Never root.
USER 65532:65532

# The with: contract, declared (docs/ACTIVITY_IMAGES.md, "Declaring the
# interface"). Optional: the engine checks a document's with: against it at
# validate and apply; the container never reads it. One line, because the
# value is JSON; README.md's Interface section reproduces it verbatim.
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"ops":{"exec":{"fields":{"command":{"type":"array","required":true}}},"script":{"fields":{"script":{"type":"string","required":true},"interpreter":{"type":"string"},"command":{"type":"array"}}},"eval":{"fields":{"command":{"type":"array","required":true}}}},"defaultOp":"exec","values":{"exitCode":{"type":"number"},"scriptPath":{"type":"string"},"value":{"type":"string"}}}'

# Exec form, mandatory.
ENTRYPOINT ["/usr/local/bin/entry.sh"]
