# examples/custom-image-shell/Dockerfile — the same activity in POSIX shell.
#   orkctl activity build examples/custom-image-shell --tag localhost:5001/orkstack-examples/custom-image-shell:dev
FROM alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc
# jq is this script's JSON library, as encoding/json is main.go's
RUN apk add --no-cache 'jq~1.7'
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod 0555 /usr/local/bin/entrypoint.sh
# never root
USER 65532:65532
# The with: contract (docs/ACTIVITY_IMAGES.md, "Declaring the interface").
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"fields":{"label":{"type":"string"},"src":{"type":"string"},"as":{"type":"string","default":"report.txt"}},"values":{"idempotencyKey":{"type":"string"},"attempt":{"type":"number"},"taskId":{"type":"string"},"label":{"type":"string"},"argv":{"type":"array"},"source":{"type":"string"},"artifact":{"type":"string"},"stats":{"type":"object"},"each":{"type":"object"}}}'
# exec form
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
