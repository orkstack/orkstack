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
# `~2.54` constraint admits Alpine's patch releases of git 2.54 and refuses
# another minor version, so a rebuild cannot silently change how a tarball is
# made.
FROM alpine:3.24@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a

RUN apk add --no-cache 'git~2.54' 'jq~1.8' ca-certificates

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

# The with: contract, the icon and the descriptions, declared
# (docs/ACTIVITY_IMAGES.md, "Declaring the interface"). Optional: the engine
# checks a document's with: against the interface at validate and apply, and
# clients draw the icon and show the descriptions; the container never reads
# them. The three lines are generated from interface.ork.yaml by
# `go run ./internal/schemas/cmd/genschemas`: edit that file, never these lines.
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"fields":{"caFile":{"type":"string"},"ref":{"type":"string"},"tokenSecret":{"type":"string"},"tokenUser":{"type":"string","default":"x-access-token"},"url":{"type":"string","required":true}},"ops":{"clone":{"fields":{"as":{"type":"string","default":"src.tar.gz"},"depth":{"type":"number","default":1},"dest":{"type":"string"}}},"resolve":{}},"values":{"commit":{"type":"string"},"path":{"type":"string"},"ref":{"type":"string"},"sha256":{"type":"string"},"sizeBytes":{"type":"number"}}}'
LABEL org.orkstack.activity.icon='<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><circle cx="4.5" cy="3.5" r="1.7" fill="none" stroke="currentColor" stroke-width="1.4"/><circle cx="4.5" cy="12.5" r="1.7" fill="none" stroke="currentColor" stroke-width="1.4"/><circle cx="11.5" cy="5.5" r="1.7" fill="none" stroke="currentColor" stroke-width="1.4"/><path d="M4.5 5.2v5.6M11.5 7.2c0 2.6-2.2 3.2-5.4 4.1" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"/></svg>'
LABEL org.orkstack.activity.docs='{"schemaVersion":1,"description":"Reads a git repository over HTTPS: checks one commit out as a source tarball or as a working tree in a pin, or resolves a ref to its commit. A token is read from a secret file and kept out of every argument, URL and log.","fields":{"caFile":"A PEM bundle in the workspace that replaces git\u0027s trust store for this task.","ref":"A branch, a tag, a full ref name or a full commit id. Without it, the remote\u0027s `HEAD`.","tokenSecret":"The name of a `secrets:` entry whose file holds the access token.","tokenUser":"The user name sent with the token, such as `oauth2` for GitLab or `x-token-auth` for Bitbucket.","url":"The repository\u0027s `https://` URL, with no credentials, query or fragment."},"ops":{"clone":{"description":"Checks one commit out, as a source tarball (`as`) or into a pin\u0027s shared directory (`dest`).","fields":{"as":"The source tarball\u0027s artifact name.","depth":"How many commits of history to fetch; `0` fetches all of it.","dest":"A directory under `/orkstack/shared` to check out into instead of a tarball; only for a pinned task."}},"resolve":{"description":"Resolves `ref` to its commit id and fetches nothing else."}},"values":{"commit":"The full id of the commit checked out or resolved.","path":"The tarball or the working tree the checkout wrote.","ref":"The remote ref that matched, or the commit id itself when `ref` was one.","sha256":"When `as` is used, the tarball\u0027s SHA-256 in hex.","sizeBytes":"When `as` is used, the tarball\u0027s size in bytes."}}'

# Exec form, mandatory.
ENTRYPOINT ["/usr/local/bin/entry.sh"]
