# The hostile event-source fixtures: one image per MODE (entry.sh lists them
# and what each does), each failing exactly the check of `orkctl service
# test` its mode is about, or, for fill and nest, passing only because the
# answer filesystem's bound held.
#
#   docker build --build-arg MODE=ignore-term -t source-hostile-ignore-term test/fixtures/source-hostile
FROM alpine:3.24@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a
ARG MODE
RUN test -n "$MODE" && apk add --no-cache 'zlib~1.4' bash jq coreutils findutils && printf '%s' "$MODE" > /etc/hostile-mode
COPY entry.sh /usr/local/bin/entry.sh
RUN chmod 0555 /usr/local/bin/entry.sh
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/entry.sh"]
LABEL org.orkstack.service.contract='1'
LABEL org.orkstack.service.profile='event-source/1'
LABEL org.orkstack.service.interface='{"schemaVersion":1,"profile":"event-source","eventSource":{"delivery":"DELIVERY_CLASS_AT_LEAST_ONCE","maxConcurrency":1,"deadLetter":true,"extendsLeases":true}}'
