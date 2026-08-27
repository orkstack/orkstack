# images/activity-shell/Dockerfile
#
# The Dockerfile contract of docs/OCI_ACTIVITY_CONTRACT.md §10.1, applied.
# Every rule it names is visible here:
#
#   rule 3  USER is not root
#   rule 4  no VOLUME — it would shadow the workspace mounts
#   rule 5  the base image is digest-pinned inside the Dockerfile
#   rule 7  ENTRYPOINT in exec form (shell form does not forward SIGTERM)
#
# §10.1 prints this block with trailing annotations on each instruction. They
# are prose, not syntax: Dockerfile has no inline comment, so
# `USER 65532:65532   # never root` makes the group literally
# "65532   # never root" and the daemon refuses the container with
# "unable to find group". The annotations therefore live above their
# instructions here.
#
# alpine:3.20's index digest below is the one §14.4 resolved and §14.15 used.
FROM alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc

# jq is the only "SDK": a JSON reader.
RUN apk add --no-cache jq

COPY entry.sh /usr/local/bin/entry.sh
RUN chmod 0555 /usr/local/bin/entry.sh

# Never root.
USER 65532:65532

# Exec form, mandatory.
ENTRYPOINT ["/usr/local/bin/entry.sh"]
