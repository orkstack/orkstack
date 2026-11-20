# The mandatory Bash event-source fixture (docs/ORKER_SERVICE_CONTRACT.md):
# bash, jq and coreutils, and nothing of OrkStack's. It runs as whatever
# non-root uid the Orker gives it.
FROM alpine:3.24@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a
RUN apk add --no-cache 'zlib~1.4' bash jq coreutils findutils
COPY entry.sh /usr/local/bin/entry.sh
RUN chmod 0555 /usr/local/bin/entry.sh
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/entry.sh"]
LABEL org.orkstack.service.contract='1'
LABEL org.orkstack.service.profile='event-source/1'
LABEL org.orkstack.service.interface='{"schemaVersion":1,"profile":"event-source","eventSource":{"delivery":"DELIVERY_CLASS_AT_LEAST_ONCE","maxConcurrency":1,"deadLetter":true,"extendsLeases":true}}'
