# images/activity-http/Dockerfile
#
# Same Dockerfile contract as activity-shell (§10.1): digest-pinned base,
# non-root USER, no VOLUME, exec-form ENTRYPOINT. Annotations sit above their
# instructions because Dockerfile has no inline comment — see the note in
# images/activity-shell/Dockerfile.
#
# jq reads and writes the JSON; curl performs the request. Neither is an SDK —
# they are the Unix tools for the job, and any image may use them.
FROM alpine:3.24@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a

RUN apk add --no-cache jq curl ca-certificates

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
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"fields":{"body":{"type":"string|object|array"},"captureBodyAs":{"type":"string"},"headers":{"type":"object"},"timeoutSeconds":{"type":"number"},"url":{"type":"string","required":true}},"ops":{"delete":{},"get":{},"head":{},"patch":{},"post":{},"put":{}},"defaultOp":"get","values":{"durationMs":{"type":"number"},"headers":{"type":"object"},"json":{"type":"object"},"sizeBytes":{"type":"number"},"status":{"type":"number"}}}'
LABEL org.orkstack.activity.icon='<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><circle cx="8" cy="8" r="6" fill="none" stroke="currentColor" stroke-width="1.4"/><ellipse cx="8" cy="8" rx="2.5" ry="6" fill="none" stroke="currentColor" stroke-width="1.4"/><path d="M2.2 8h11.6" stroke="currentColor" stroke-width="1.4"/></svg>'
LABEL org.orkstack.activity.docs='{"schemaVersion":1,"description":"Performs one HTTP request and reports the response as data. Any response it receives is a success, whatever its status; the task fails only when the request could not be made at all.","fields":{"body":"The request body. A string is sent as written; a mapping or a list is sent as compact JSON, with a JSON content type unless `headers` sets one.","captureBodyAs":"Saves the response body as an output artifact of this name, a plain file name.","headers":"Request headers, one per key, such as `Accept: application/json`. Each value reaches curl as data, never as an argument.","timeoutSeconds":"How long the whole request may take, in seconds, before the task fails.","url":"The absolute URL to request."},"ops":{"delete":{"description":"Sends a DELETE request."},"get":{"description":"Sends a GET request; the op used when `op` is absent."},"head":{"description":"Sends a HEAD request: the response has headers and no body."},"patch":{"description":"Sends a PATCH request with `body`."},"post":{"description":"Sends a POST request with `body`."},"put":{"description":"Sends a PUT request with `body`."}},"values":{"durationMs":"Wall-clock time of the request, in milliseconds.","headers":"The response headers; a repeated header keeps its last value.","json":"The parsed response body, present only when it is valid JSON of at most 1 MiB.","sizeBytes":"Size of the response body, in bytes.","status":"The HTTP status code. A non-2xx status is reported here, not as a failure."}}'

# Exec form, mandatory.
ENTRYPOINT ["/usr/local/bin/entry.sh"]
