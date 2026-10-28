# examples/custom-image-shell/Dockerfile — the same activity in POSIX shell.
#   orkctl activity build examples/custom-image-shell --tag localhost:5001/orkstack-examples/custom-image-shell:dev
FROM alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc
# jq is this script's JSON library, as encoding/json is main.go's
RUN apk add --no-cache 'jq~1.7'
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod 0555 /usr/local/bin/entrypoint.sh
# never root
USER 65532:65532
# The with: contract (docs/ACTIVITY_IMAGES.md, "Declaring the interface"),
# generated from interface.ork.yaml by `go run ./internal/schemas/cmd/genschemas`;
# outside this repository, drop the line and build with `--interface interface.ork.yaml`.
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"fields":{"as":{"type":"string","default":"report.txt"},"label":{"type":"string"},"src":{"type":"string"}},"values":{"argv":{"type":"array"},"artifact":{"type":"string"},"attempt":{"type":"number"},"each":{"type":"object"},"idempotencyKey":{"type":"string"},"label":{"type":"string"},"source":{"type":"string"},"stats":{"type":"object"},"taskId":{"type":"string"}}}'
# exec form
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
