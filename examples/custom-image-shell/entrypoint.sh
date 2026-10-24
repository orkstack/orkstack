#!/bin/sh
# examples/custom-image-shell/entrypoint.sh — the activity of
# examples/custom-image (Go), in POSIX shell. A user's own image: no catalog
# name, nothing of OrkStack's linked in. It reads the files the platform
# writes, writes the files the platform reads, and exits with an ordinary
# status. jq is its JSON library, as encoding/json is main.go's; the work is
# plain shell. Given one configuration, both images write byte-identical
# reports.
#
# `set -u`, never `set -e`: every failure goes through fail(), which says why
# on stderr and exits 1. fail() is never called inside $( ), where its exit
# would end only the subshell.
set -u
umask 022

IMAGE=custom-image-shell
WORKSPACE=/orkstack
TASK=$WORKSPACE/input/task.json
CONFIG=$WORKSPACE/input/config.json
VALUES=$WORKSPACE/output/values.json
ARTIFACTS=$WORKSPACE/output/artifacts
MAX_NAME_BYTES=128

fail() {
    echo "$IMAGE: $1" >&2
    exit 1
}

# ------------------------------------------------------------------ task.json

[ -f "$TASK" ] || fail "read $TASK: no such file"
jq -e 'type == "object"' "$TASK" > /dev/null 2>&1 || fail "parse $TASK: not a JSON object"
schema=$(jq -r 'if has("schemaVersion") then .schemaVersion else "absent" end' "$TASK")
[ "$schema" = 1 ] || fail "task.json has schemaVersion $schema; this image understands 1"
jq -e '(.idempotencyKey | type) == "string" and .idempotencyKey != ""
       and (.task.id | type) == "string" and .task.id != ""' "$TASK" > /dev/null 2>&1 ||
    fail "task.json has no idempotencyKey or task.id"

# --------------------------------------------------------------- config.json

# The task's rendered `with:` block; `{}` when it set none. Every key is
# optional and read by presence, never with jq's `//`.
[ -f "$CONFIG" ] || fail "read $CONFIG: no such file"
jq -e 'type == "object"' "$CONFIG" > /dev/null 2>&1 || fail "parse $CONFIG: not a JSON object"
label=$(jq -r 'if has("label") then .label | strings else empty end' "$CONFIG")
src=$(jq -r 'if has("src") then .src | strings else empty end' "$CONFIG")
as=$(jq -r 'if has("as") then .as | strings else empty end' "$CONFIG")

# With no label configured, a fan-out item that is a JSON string is the label.
if [ -z "$label" ]; then
    label=$(jq -r 'if has("each") and (.each | has("item")) then .each.item | strings else empty end' "$TASK")
fi

# The task's command:, which the Orker passes verbatim and never renders,
# arrives as this script's arguments.
argv_json=$(jq -nc '$ARGS.positional' --args "$@") || fail "encode argv"

# ------------------------------------------------------------------- the work

# resolve_read applies activity-file's path rules and leaves the result in
# RESOLVED: a relative path resolves under /orkstack/input, an absolute one
# must be inside /orkstack, a `..` component is refused (never normalized),
# and the result must be a regular file — a symbolic link is refused.
RESOLVED=
resolve_read() {
    _p=$1
    [ -n "$_p" ] || fail "empty source path"
    case "$_p" in
        /*) ;;
        *) _p="$WORKSPACE/input/$_p" ;;
    esac
    case "/$_p/" in
        */../*) fail "source path may not contain '..': $1" ;;
    esac
    case "$_p" in
        "$WORKSPACE"/*) ;;
        *) fail "source path must be inside $WORKSPACE: $1" ;;
    esac
    { [ -f "$_p" ] && [ ! -L "$_p" ]; } || fail "not a regular file: $1"
    RESOLVED=$_p
}

check_name() {
    case "$1" in
        "" | . | ..) fail "artifact name may not be \"$1\"" ;;
        */*) fail "artifact name may not contain '/': $1" ;;
    esac
    _n=$(printf '%s' "$1" | wc -c | tr -d ' ')
    [ "$_n" -le "$MAX_NAME_BYTES" ] || fail "artifact name is $_n bytes, limit is $MAX_NAME_BYTES"
}

source_path=
stats_json=null
if [ -n "$src" ]; then
    resolve_read "$src"
    source_path=$RESOLVED
    [ -n "$as" ] || as=report.txt
    check_name "$as"

    # Lines are what a reader counts: every newline, plus one for a last line
    # without one. Words are runs of bytes that are not ASCII whitespace.
    bytes=$(LC_ALL=C wc -c < "$source_path" | tr -d ' ') || fail "read $source_path"
    words=$(LC_ALL=C wc -w < "$source_path" | tr -d ' ') || fail "read $source_path"
    lines=$(LC_ALL=C wc -l < "$source_path" | tr -d ' ') || fail "read $source_path"
    if [ "$bytes" -gt 0 ] && [ -n "$(tail -c 1 "$source_path")" ]; then
        lines=$((lines + 1))
    fi

    # The report is written before values.json.
    printf 'activity report\nlabel:   %s\nsource:  %s\nargv:    %s\nlines:   %s\nwords:   %s\nbytes:   %s\n' \
        "$label" "$source_path" "$argv_json" "$lines" "$words" "$bytes" \
        > "$ARTIFACTS/$as" || fail "write artifact $as"
    stats_json="{\"lines\":$lines,\"words\":$words,\"bytes\":$bytes}"
fi

# ---------------------------------------------------------------- values.json

# One jq program over task.json, so a fan-out item reaches values.json as the
# exact JSON task.json carries.
jq -c \
    --argjson argv "$argv_json" \
    --arg label "$label" \
    --arg source "$source_path" \
    --arg artifact "$as" \
    --argjson stats "$stats_json" \
    '{idempotencyKey,
      attempt: (if (.task | has("attempt")) then .task.attempt else 0 end),
      taskId: .task.id,
      argv: $argv}
     + (if $label != "" then {label: $label} else {} end)
     + (if $source != "" then {source: $source, artifact: $artifact, stats: $stats} else {} end)
     + (if has("each") then {each: (.each | {index, count, item})} else {} end)' \
    "$TASK" > "$VALUES" || fail "write values.json"

echo "$IMAGE: task $(jq -r '.task.id' "$TASK") wrote values.json"
exit 0
