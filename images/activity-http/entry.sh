#!/bin/sh
# images/activity-http/entry.sh — one HTTP request, reported as data.
#
# Structure is activity-shell's (§10.2): `set -u` not `set -eu`, `umask 022`
# (rule 7), and the background-and-trap `run_supervised` (rule 2a) so SIGTERM
# reaches the workload AND values.json still gets written. run_supervised is
# never called inside $( ) or a pipeline (rule 2b).
set -u
umask 022

CONFIG=/orkstack/input/config.json
VALUES=/orkstack/output/values.json
ARTIFACTS=/orkstack/output/artifacts
TMP=/orkstack/tmp

# json_max_bytes bounds the `json` convenience field. The body artifact is not
# bounded here — the Orker's limits.artifactMaxBytes governs that.
JSON_MAX_BYTES=1048576

run_supervised() {
    "$@" &
    child=$!
    trap 'kill -TERM "$child" 2>/dev/null' TERM INT
    wait "$child"
    rc=$?
    if [ "$rc" -gt 128 ]; then
        wait "$child"
        rc=$?
    fi
    trap - TERM INT
    return "$rc"
}

fail() {
    echo "activity-http: $1" >&2
    exit 1
}

op=$(jq -r '.op // "get"' "$CONFIG")
case "$op" in
  get|post|put|patch|delete|head) ;;
  *) echo "activity-http: unknown op '$op'" >&2; exit 2 ;;
esac
method=$(printf '%s' "$op" | tr '[:lower:]' '[:upper:]')

url=$(jq -r '.url // empty' "$CONFIG")
[ -n "$url" ] || fail "with.url is required"

timeout=$(jq -r '.timeoutSeconds // empty' "$CONFIG")
capture_as=$(jq -r '.captureBodyAs // empty' "$CONFIG")

# Headers arrive as a JSON object. @sh quotes each rendered value, so no header
# value can inject an argument, exactly as `command` cannot in activity-shell.
eval "set -- $(jq -r '(.headers // {}) | to_entries | map("--header", "\(.key): \(.value)") | @sh' "$CONFIG")"

# A request body is written to a file rather than passed as an argument: it may
# be large, and it may contain anything.
#
# A body is sent as written: a string verbatim (jq -j adds no newline), a mapping or a
# list as compact JSON — every string leaf was rendered by the Orker before this runs,
# so a quote or a backslash in a value stays inside its string. A JSON body defaults
# Content-Type to application/json unless headers sets a content type.
if jq -e 'has("body") and .body != null' "$CONFIG" >/dev/null; then
    body_type=$(jq -r '.body | type' "$CONFIG")
    case "$body_type" in
      string) jq -j '.body' "$CONFIG" > "$TMP/request-body" ;;
      object|array)
          jq -cj '.body' "$CONFIG" > "$TMP/request-body"
          if ! jq -e '(.headers // {}) | keys | map(ascii_downcase) | index("content-type")' "$CONFIG" >/dev/null; then
              set -- "$@" --header "Content-Type: application/json"
          fi ;;
      *) fail "with.body must be a string, a mapping or a list, got $body_type" ;;
    esac
    set -- "$@" --data-binary "@$TMP/request-body"
fi

[ -n "$timeout" ] && set -- "$@" --max-time "$timeout"
[ "$method" = "HEAD" ] && set -- "$@" --head

# Informational, not diagnostic — stdout, so stderr stays reserved for
# fail()'s actual error messages (a log viewer that tints stderr, this
# platform's does, would otherwise paint a routine request red).
echo "http $method $url"

run_supervised curl \
    --silent --show-error \
    --request "$method" \
    --dump-header "$TMP/headers" \
    --output "$TMP/body" \
    --write-out '%{http_code} %{time_total}\n' \
    "$@" "$url" > "$TMP/meta"
rc=$?

# A transport failure is an activity failure: the request was not performed.
# A non-2xx RESPONSE is not — see the README. That distinction is this image's
# documented contract, not a platform rule.
if [ "$rc" -ne 0 ]; then
    fail "$method $url: curl exited $rc"
fi

status=$(cut -d' ' -f1 "$TMP/meta")
secs=$(cut -d' ' -f2 "$TMP/meta")
# A HEAD response has no body. curl's --head writes the header block to the
# output file, so measuring that would report the headers as a body size.
[ "$method" = "HEAD" ] && : > "$TMP/body"
size=$(wc -c < "$TMP/body" | tr -d ' ')

# Header lines only: the status line has no ": ", so it filters itself out.
# A repeated header keeps its last value.
tr -d '\r' < "$TMP/headers" > "$TMP/headers.clean"
jq -Rs '
  split("\n")
  | map(capture("^(?<k>[^:]+): (?<v>.*)$") // empty)
  | map({(.k): .v})
  | add // {}
' < "$TMP/headers.clean" > "$TMP/headers.json"

# `json` is present only when the body parses as JSON and is small enough to be
# worth carrying. Absence means "not JSON, or too large", never "empty".
if [ "$size" -le "$JSON_MAX_BYTES" ] && jq -e . "$TMP/body" >/dev/null 2>&1; then
    jq -n \
       --argjson status "$status" \
       --slurpfile headers "$TMP/headers.json" \
       --argjson secs "$secs" \
       --argjson size "$size" \
       --slurpfile body "$TMP/body" \
       '{status:$status, headers:$headers[0], durationMs:(($secs*1000000|round)/1000), sizeBytes:$size, json:$body[0]}' \
       > "$VALUES"
else
    jq -n \
       --argjson status "$status" \
       --slurpfile headers "$TMP/headers.json" \
       --argjson secs "$secs" \
       --argjson size "$size" \
       '{status:$status, headers:$headers[0], durationMs:(($secs*1000000|round)/1000), sizeBytes:$size}' \
       > "$VALUES"
fi

if [ -n "$capture_as" ]; then
    case "$capture_as" in
      */*|..|.) fail "captureBodyAs must be a plain file name, got '$capture_as'" ;;
    esac
    cp "$TMP/body" "$ARTIFACTS/$capture_as"
    chmod 0644 "$ARTIFACTS/$capture_as"
fi

echo "http $method $url -> $status ($size bytes)"
exit 0
