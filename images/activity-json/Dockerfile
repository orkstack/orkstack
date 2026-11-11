# images/activity-json/Dockerfile
#
# Same Dockerfile contract as activity-shell (§10.1): digest-pinned base,
# non-root USER, no VOLUME, exec-form ENTRYPOINT. Annotations sit above their
# instructions because Dockerfile has no inline comment — see the note in
# images/activity-shell/Dockerfile.
#
# jq is the whole toolchain: this image does not install a JSON library, it
# installs the JSON tool every first-party entry script already uses and lets
# a document point it at a document. The `~1.8` constraint admits Alpine's
# patch releases of jq 1.8 and refuses another minor version, so a rebuild
# cannot silently change what a filter computes.
#
# tzdata is what makes a task's own TZ (say Europe/Berlin) mean that zone:
# without zone data, musl reads every zone name as UTC, silently.
FROM alpine:3.24@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a

# zlib~1.4: zlib 1.4 fixes CVE-2026-4738, and alpine 3.24.4, the newest 3.24
# base, still ships 1.3.2, so the package is upgraded here. Drop it once the
# pinned base carries zlib 1.4.
RUN apk add --no-cache 'zlib~1.4' 'jq~1.8' tzdata

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

# The with: contract, the icon and the descriptions, declared
# (docs/ACTIVITY_IMAGES.md, "Declaring the interface"). Optional: the engine
# checks a document's with: against the interface at validate and apply, and
# clients draw the icon and show the descriptions; the container never reads
# them. The three lines are generated from interface.ork.yaml by
# `go run ./internal/schemas/cmd/genschemas`: edit that file, never these lines.
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"ops":{"merge":{"fields":{"as":{"type":"string"},"srcs":{"type":"array","required":true}}},"query":{"fields":{"as":{"type":"string"},"filter":{"type":"string","required":true},"gather":{"type":"string"},"nullInput":{"type":"boolean","default":false},"slurp":{"type":"boolean","default":false},"src":{"type":"string"},"srcs":{"type":"array"},"vars":{"type":"object"}}}},"values":{"path":{"type":"string"},"sha256":{"type":"string"},"sizeBytes":{"type":"number"}}}'
LABEL org.orkstack.activity.icon='<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><path d="M6 2.5c-1.6 0-2 .8-2 2v1.6c0 .9-.5 1.4-1.5 1.9 1 .5 1.5 1 1.5 1.9v1.6c0 1.2.4 2 2 2M10 2.5c1.6 0 2 .8 2 2v1.6c0 .9.5 1.4 1.5 1.9-1 .5-1.5 1-1.5 1.9v1.6c0 1.2-.4 2-2 2" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"/></svg>'
LABEL org.orkstack.activity.docs='{"schemaVersion":1,"description":"Runs one jq filter over JSON files, a fan-out\u0027s items or nothing at all, or deep-merges JSON objects: the default compute step. The one result is reported as `result`, or written as an artifact when `as` is set.","ops":{"merge":{"description":"Deep-merges JSON objects, left to right.","fields":{"as":"Writes the merged object as an output artifact of this name instead of reporting it as `result`.","srcs":"At least two files, each holding exactly one JSON object, merged in order."}},"query":{"description":"Runs `filter` over exactly one source: `src`, `srcs`, `gather` or `nullInput`.","fields":{"as":"Writes the result as an output artifact of this name instead of reporting it as `result`.","filter":"One jq program, such as `.items | length`, at most 64 KiB, that produces exactly one result.","gather":"A fan-out task\u0027s id: the filter reads its items\u0027 `values.json` objects as one array, in item order.","nullInput":"Runs the filter once with `null` as its input, reading no file.","slurp":"Only with `src`: reads every JSON value in the file as one array, for JSON Lines.","src":"One JSON file for the filter to read.","srcs":"JSON files the filter reads as one array of every value they hold, in order.","vars":"An object bound as the jq variable `$vars`, read as `$vars.region` or `$vars[\"a key\"]`; at most 64 KiB as compact JSON."}}},"values":{"path":"When `as` is set, the artifact the result was written to.","sha256":"When `as` is set, the artifact\u0027s SHA-256 in hex.","sizeBytes":"When `as` is set, the artifact\u0027s size in bytes."}}'

# Exec form, mandatory.
ENTRYPOINT ["/usr/local/bin/entry.sh"]
