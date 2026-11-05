# examples/custom-image-shell/Dockerfile — the same activity in POSIX shell.
# Build, push and pin it with (the quickstart's registry; make up's is at
# localhost:5001):
#   orkctl activity build examples/custom-image-shell --tag 127.0.0.1:5002/orkstack-examples/custom-image-shell:dev
FROM alpine:3.24@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a
# jq is this script's JSON library, as encoding/json is main.go's
RUN apk add --no-cache 'jq~1.8'
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod 0555 /usr/local/bin/entrypoint.sh
# never root
USER 65532:65532
# The with: contract, icon and descriptions (docs/ACTIVITY_IMAGES.md,
# "Declaring the interface"), generated from interface.ork.yaml by
# `go run ./internal/schemas/cmd/genschemas`; outside this repository, drop the
# lines and build with `--interface interface.ork.yaml`.
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"fields":{"as":{"type":"string","default":"report.txt"},"label":{"type":"string"},"src":{"type":"string"}},"values":{"argv":{"type":"array"},"artifact":{"type":"string"},"attempt":{"type":"number"},"each":{"type":"object"},"idempotencyKey":{"type":"string"},"label":{"type":"string"},"source":{"type":"string"},"stats":{"type":"object"},"taskId":{"type":"string"}}}'
LABEL org.orkstack.activity.icon=''
LABEL org.orkstack.activity.docs='{"schemaVersion":1,"description":"Reports on one file: its lines, words and bytes, written as a seven-line report artifact, plus what the task was given. A user\u0027s own activity, with no OrkStack SDK.","fields":{"as":"The report artifact\u0027s name.","label":"The label to report. Without it, one item of a fan-out over strings reports the item itself.","src":"The file to report on, by the `file` activity\u0027s path rules. Without it, no report is written."},"values":{"argv":"The container\u0027s arguments, never rendered.","artifact":"The report artifact\u0027s name, with `src`.","attempt":"Which attempt of the task this was.","each":"The fan-out item\u0027s index, count and item, for one item of a fan-out.","idempotencyKey":"The task\u0027s idempotency key, from `task.json`.","label":"The label reported, when there is one.","source":"The file reported on, with `src`.","stats":"The file\u0027s lines, words and bytes, with `src`.","taskId":"The task\u0027s id."}}'
# exec form
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
