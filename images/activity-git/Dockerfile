# images/activity-git/Dockerfile
#
# Same Dockerfile contract as activity-shell (§10.1): digest-pinned base,
# non-root USER, no VOLUME, exec-form ENTRYPOINT. Annotations sit above their
# instructions because Dockerfile has no inline comment — see the note in
# images/activity-shell/Dockerfile.
#
# git performs the checkout, jq reads and writes the JSON, and ca-certificates
# is what makes an https:// remote verifiable — this image speaks HTTPS only.
# openssh is deliberately absent: an SSH transport would mean a private key
# inside the container and a host-key decision nobody is there to make. The
# `~2.45` constraint admits Alpine's patch releases of git 2.45 and refuses
# another minor version, so a rebuild cannot silently change how a tarball is
# made.
FROM alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc

RUN apk add --no-cache 'git~2.45' 'jq~1.7' ca-certificates

COPY entry.sh /usr/local/bin/entry.sh
RUN chmod 0555 /usr/local/bin/entry.sh

# The credential helper is baked into the read-only image, never written into
# a writable mount at run time. It answers only a prompt for with.url's own
# origin.
COPY askpass /usr/local/libexec/activity-git/askpass
RUN chmod 0555 /usr/local/libexec/activity-git/askpass

# Image default: HOME is the per-attempt, writable, empty /orkstack/tmp — no
# ~/.gitconfig, no credential store. A task's env: overrides it.
ENV HOME=/orkstack/tmp
# Image default: git reads no /etc/gitconfig (the image ships none). A task's
# env: overrides it.
ENV GIT_CONFIG_NOSYSTEM=1
# Image default: git reads no global configuration. A task's env: overrides
# it, naming a file git then reads.
ENV GIT_CONFIG_GLOBAL=/dev/null
# Image default: a credential git cannot get fails at once ("terminal prompts
# disabled") instead of waiting for a terminal. A task's env: overrides it.
ENV GIT_TERMINAL_PROMPT=0
# Image default: git uses no transport but HTTPS, including on a redirect and
# for a submodule. A task's env: overrides it.
ENV GIT_ALLOW_PROTOCOL=https

# Never root.
USER 65532:65532

# The with: contract, declared (docs/ACTIVITY_IMAGES.md, "Declaring the
# interface"). Optional: the engine checks a document's with: against it at
# validate and apply; the container never reads it. One line, because the
# value is JSON; README.md's Interface section reproduces it verbatim.
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"fields":{"url":{"type":"string","required":true},"ref":{"type":"string"},"tokenSecret":{"type":"string"},"tokenUser":{"type":"string","default":"x-access-token"},"caFile":{"type":"string"}},"ops":{"clone":{"fields":{"depth":{"type":"number","default":1},"as":{"type":"string","default":"src.tar.gz"},"dest":{"type":"string"}}},"resolve":{}},"values":{"commit":{"type":"string"},"ref":{"type":"string"},"path":{"type":"string"},"sizeBytes":{"type":"number"},"sha256":{"type":"string"}}}'

# Exec form, mandatory.
ENTRYPOINT ["/usr/local/bin/entry.sh"]
