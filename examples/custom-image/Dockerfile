# examples/custom-image/Dockerfile — a user's own activity image, in Go. It is
# not a first-party activity: it has no catalog name and imports nothing of
# OrkStack's. Build, push and pin it with:
#   orkctl activity build examples/custom-image --tag localhost:5001/orkstack-examples/custom-image:dev
FROM golang:1.27-alpine@sha256:8a5910f31396cd4d89662f56c68b3ae31d374308270a1c3bd96672ee5ed43414 AS build
WORKDIR /src
COPY main.go ./
ENV CGO_ENABLED=0
RUN go mod init example.invalid/custom-image && go build -trimpath -ldflags="-s -w" -o /out/report .

# scratch: the binary needs no libc, shell, CA bundle or zone data.
FROM scratch
COPY --from=build /out/report /report
# never root
USER 65532:65532
# The with: contract, read by validate and apply, and the icon and
# descriptions clients show (docs/ACTIVITY_IMAGES.md, "Declaring the
# interface"); the container never reads them. Generated from
# interface.ork.yaml by `go run ./internal/schemas/cmd/genschemas`; outside
# this repository, drop the lines and build with `--interface interface.ork.yaml`.
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"fields":{"as":{"type":"string","default":"report.txt"},"label":{"type":"string"},"src":{"type":"string"}},"values":{"argv":{"type":"array"},"artifact":{"type":"string"},"attempt":{"type":"number"},"each":{"type":"object"},"idempotencyKey":{"type":"string"},"label":{"type":"string"},"source":{"type":"string"},"stats":{"type":"object"},"taskId":{"type":"string"}}}'
LABEL org.orkstack.activity.icon=''
LABEL org.orkstack.activity.docs='{"schemaVersion":1,"description":"Reports on one file: its lines, words and bytes, written as a seven-line report artifact, plus what the task was given. A user\u0027s own activity, with no OrkStack SDK.","fields":{"as":"The report artifact\u0027s name.","label":"The label to report. Without it, one item of a fan-out over strings reports the item itself.","src":"The file to report on, by the `file` activity\u0027s path rules. Without it, no report is written."},"values":{"argv":"The container\u0027s arguments, never rendered.","artifact":"The report artifact\u0027s name, with `src`.","attempt":"Which attempt of the task this was.","each":"The fan-out item\u0027s index, count and item, for one item of a fan-out.","idempotencyKey":"The task\u0027s idempotency key, from `task.json`.","label":"The label reported, when there is one.","source":"The file reported on, with `src`.","stats":"The file\u0027s lines, words and bytes, with `src`.","taskId":"The task\u0027s id."}}'
# exec form: SIGTERM reaches the program itself
ENTRYPOINT ["/report"]
