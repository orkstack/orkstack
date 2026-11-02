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

# The with: contract, the icon and the descriptions, declared
# (docs/ACTIVITY_IMAGES.md, "Declaring the interface"). Optional: the engine
# checks a document's with: against the interface at validate and apply, and
# clients draw the icon and show the descriptions; the container never reads
# them. The three lines are generated from interface.ork.yaml by
# `go run ./internal/schemas/cmd/genschemas`: edit that file, never these lines.
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"ops":{"checksum":{"fields":{"src":{"type":"string","required":true}}},"concat":{"fields":{"as":{"type":"string","required":true},"srcs":{"type":"array","required":true}}},"copy":{"fields":{"as":{"type":"string"},"src":{"type":"string","required":true}}},"gunzip":{"fields":{"as":{"type":"string"},"src":{"type":"string","required":true}}},"gzip":{"fields":{"as":{"type":"string"},"src":{"type":"string","required":true}}},"tar":{"fields":{"as":{"type":"string"},"srcs":{"type":"array","required":true}}},"untar":{"fields":{"as":{"type":"string"},"member":{"type":"string","required":true},"src":{"type":"string","required":true}}}},"values":{"path":{"type":"string"},"sha256":{"type":"string"},"sizeBytes":{"type":"number"}}}'
LABEL org.orkstack.activity.icon='<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><path d="M4 1.8h5L12.5 5.3v8.9H4z" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linejoin="round"/><path d="M9 1.8v3.5h3.5" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linejoin="round"/></svg>'
LABEL org.orkstack.activity.docs='{"schemaVersion":1,"description":"Seven small file operations: `copy`, `checksum`, `gzip`, `gunzip`, `tar`, `untar` and `concat`. Every op but `checksum` writes exactly one output artifact. Paths resolve inside the workspace, and a `..` component is refused.","ops":{"checksum":{"description":"Reports one file\u0027s size and SHA-256, and writes no artifact.","fields":{"src":"The file to read. A relative path resolves under `/orkstack/input`."}},"concat":{"description":"Joins files, in order, into one output artifact.","fields":{"as":"The output artifact\u0027s name; required for `concat`.","srcs":"The files to join, in order. Relative paths resolve under `/orkstack/input`."}},"copy":{"description":"Copies one file to an output artifact.","fields":{"as":"The output artifact\u0027s name. Defaults to the base name of `src`.","src":"The file to copy. A relative path resolves under `/orkstack/input`."}},"gunzip":{"description":"Decompresses one gzip file.","fields":{"as":"The output artifact\u0027s name. Defaults to the base name of `src` without `.gz`.","src":"The gzip file to decompress. A relative path resolves under `/orkstack/input`."}},"gzip":{"description":"Compresses one file with gzip.","fields":{"as":"The output artifact\u0027s name. Defaults to the base name of `src` plus `.gz`.","src":"The file to compress. A relative path resolves under `/orkstack/input`."}},"tar":{"description":"Packs files into one gzipped tar archive, each under its base name.","fields":{"as":"The output artifact\u0027s name. Defaults to `archive.tar.gz`.","srcs":"The files to pack, in order. Relative paths resolve under `/orkstack/input`."}},"untar":{"description":"Extracts one named member of a tar archive.","fields":{"as":"The output artifact\u0027s name. Defaults to the base name of `member`.","member":"The path, inside the archive, of the one file to extract.","src":"The tar archive. A relative path resolves under `/orkstack/input`."}}},"values":{"path":"The file the op wrote, or for `checksum` the file it read.","sha256":"SHA-256 of that file, in hex.","sizeBytes":"Size of that file, in bytes."}}'

# Exec form, mandatory.
ENTRYPOINT ["/usr/local/bin/entry.sh"]
