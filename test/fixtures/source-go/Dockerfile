# The Go event-source fixture (docs/EVENT_SOURCE_IMAGES.md): one static
# binary of the standard library, and nothing of OrkStack's. Build the binary
# into the build context first:
#
#   CGO_ENABLED=0 go build -o CONTEXT/source-go ./test/fixtures/source-go
#   docker build -f test/fixtures/source-go/Dockerfile CONTEXT
FROM gcr.io/distroless/static-debian13:nonroot@sha256:18f4cc26c9ffa7f20a359298be7deadac5c607c8e03a02b8119f1a5fc2f82c99
COPY source-go /usr/local/bin/source-go
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/source-go"]
LABEL org.orkstack.service.contract='1'
LABEL org.orkstack.service.profile='event-source/1'
LABEL org.orkstack.service.interface='{"schemaVersion":1,"profile":"event-source","eventSource":{"delivery":"DELIVERY_CLASS_AT_LEAST_ONCE","maxConcurrency":1,"deadLetter":true,"extendsLeases":true}}'
