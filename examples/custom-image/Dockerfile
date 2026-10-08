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
# The with: contract, read by validate and apply (docs/ACTIVITY_IMAGES.md,
# "Declaring the interface"); the container never reads it.
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"fields":{"label":{"type":"string"},"src":{"type":"string"},"as":{"type":"string","default":"report.txt"}},"values":{"idempotencyKey":{"type":"string"},"attempt":{"type":"number"},"taskId":{"type":"string"},"label":{"type":"string"},"argv":{"type":"array"},"source":{"type":"string"},"artifact":{"type":"string"},"stats":{"type":"object"},"each":{"type":"object"}}}'
# exec form: SIGTERM reaches the program itself
ENTRYPOINT ["/report"]
