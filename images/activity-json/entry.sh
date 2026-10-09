#!/bin/sh
# images/activity-json/entry.sh — two JSON operations, and no third.
#
# `query` runs ONE jq filter over one input — a file, several files read as one
# array, a fan-out's items, or nothing at all — and reports exactly one result;
# `merge` deep-merges several JSON objects left to right. There is deliberately
# no conversion op, no schema validation and no multi-filter pipeline: the DAG
# is the pipeline, and a second filter is a second task.
#
# Structure is activity-file's (§10.2): `set -u` not `set -eu`, `umask 022`
# (rule 7), and the background-and-trap `run_supervised` (rule 2a) so SIGTERM
# reaches jq. Every call to run_supervised is a plain invocation, with
# redirection at most, never inside $( ) or a pipeline (rule 2b).
#
# The environment is the task author's, and it is honoured: this script never
# clears, filters or re-executes it, so jq sees the image defaults (HOME, TZ)
# overridden by the task's env:. Because busybox sh imports every variable as a
# shell variable, every variable this script uses as its own state is assigned
# before it is read.
#
# No jq invocation here ever reads standard input: each carries -n or names at
# least one file. A forgotten source must fail, not filter an empty stdin.
set -u
umask 022

CONFIG=/orkstack/input/config.json
TASK=/orkstack/input/task.json
VALUES=/orkstack/output/values.json
INPUT=/orkstack/input
ARTIFACTS=/orkstack/output/artifacts
TMP=/orkstack/tmp

MAX_PROGRAM_BYTES=65536
MAX_VARS_BYTES=65536

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
    echo "activity-json: $1" >&2
    exit 1
}

# resolve_read turns a configured source into an absolute container path and
# leaves the result in RESOLVED. It is deliberately NOT a $( ) helper: `fail`
# inside a command substitution would exit only the subshell and let the script
# carry on with an empty path — the same subshell hazard as §10.1 rule 2b.
#
# Relative paths resolve under /orkstack/input; absolute paths must already
# be inside the workspace. A `..` component is refused rather than
# normalized, so a path can never be argued out of the workspace
# (images/activity-file/README.md, "Paths").
RESOLVED=
resolve_read() {
    _p=$1
    [ -n "$_p" ] || fail "empty source path"
    case "$_p" in
      /*) ;;
      *) _p="$INPUT/$_p" ;;
    esac
    case "/$_p/" in
      */../*) fail "source path may not contain '..': $1" ;;
    esac
    case "$_p" in
      /orkstack/*) ;;
      *) fail "source path must be inside /orkstack: $1" ;;
    esac
    [ -f "$_p" ] || fail "not a regular file: $_p"
    RESOLVED=$_p
}

# check_name enforces §3.5's artifact-name rules at the point of writing, so a
# bad name fails here with a clear message instead of at the Orker's scan.
check_name() {
    case "$1" in
      ""|.|..) fail "artifact name may not be '$1'" ;;
      */*) fail "artifact name may not contain '/': $1" ;;
    esac
    case "$1" in
      *..*) fail "artifact name may not contain '..': $1" ;;
    esac
    _n=$(printf '%s' "$1" | wc -c | tr -d ' ')
    [ "$_n" -le 128 ] || fail "artifact name is $_n bytes, limit is 128: $1"
}

# emit writes values.json for a path that is now final. This is the success
# report, not a diagnostic, so it goes to stdout: stderr is reserved for
# fail()'s actual error messages, and a log viewer that tints stderr (this
# platform's does) would otherwise paint every successful op red.
emit() {
    _path=$1
    _size=$(wc -c < "$_path" | tr -d ' ')
    _sha=$(sha256sum "$_path" | cut -d' ' -f1)
    jq -n --arg p "$_path" --argjson s "$_size" --arg h "$_sha" \
       '{path:$p, sizeBytes:$s, sha256:$h}' > "$VALUES"
    echo "activity-json: $_path ($_size bytes, sha256:$_sha)"
}

# key_type refuses a PRESENT with: key whose JSON type is not the one this
# image reads. Presence is decided by has(), never by jq's `//`, which would
# also replace `false` and `null` and so silently accept a value the author
# wrote. `null` and `false` are therefore refused like any other wrong type.
#
#   key_type <key> <jq type name> <what it must be>
key_type() {
    jq -e --arg k "$1" --arg t "$2" 'if has($k) then (.[$k] | type) == $t else true end' "$CONFIG" >/dev/null 2>&1 ||
        fail "with.$1 must be $3"
}

# has_key succeeds when config.json carries the key at all, whatever its value.
has_key() {
    jq -e --arg k "$1" 'has($k)' "$CONFIG" >/dev/null 2>&1
}

# path_list checks that a present array key holds only path strings and writes
# them, one per line, to the file named by $2. A newline inside an entry would
# split it into two paths, so it is refused rather than read.
path_list() {
    jq -e --arg k "$1" '.[$k] | all(type == "string" and (test("\n") | not))' "$CONFIG" >/dev/null 2>&1 ||
        fail "with.$1 must be an array of path strings"
    jq -r --arg k "$1" '.[$k][]' "$CONFIG" > "$2" || fail "reading with.$1 failed"
}

# report turns the single jq output now in $TMP/out into this task's result.
#
# Exactly ONE output is required: a filter that emits several values has no
# single result to report, and picking the first would be a silently wrong
# answer; one that emits none has nothing to report. Both fail, naming the
# count. `jq -c` prints one value per line, so the line count is the count.
report() {
    _count=$(wc -l < "$TMP/out" | tr -d ' ')
    if [ "$_count" -eq 0 ]; then
        fail "the filter produced no output; a task reports exactly one result"
    fi
    if [ "$_count" -ne 1 ]; then
        fail "the filter produced $_count outputs; a task reports exactly one result — collect them, e.g. '[ .[] | … ]'"
    fi

    if [ "$has_as" = no ]; then
        # Always under `result`, never spliced into the top level: a downstream
        # path is then predictable (.tasks.<id>.output.result…) whatever the
        # filter returns, and an array there is exactly what each.over reads.
        # The output file is jq's operand, so a number passes through with its
        # literal text intact (jq 1.7 preserves an unchanged number).
        jq -c '{result: .}' "$TMP/out" > "$TMP/values.json" || fail "writing values.json failed"
        mv "$TMP/values.json" "$VALUES" || fail "writing values.json failed"
        _size=$(wc -c < "$VALUES" | tr -d ' ')
        _limit=$(jq -r '.limits.valuesMaxBytes | numbers' "$TASK")
        # A hint, not a refusal: an over-size values.json is the Orker's to
        # refuse as ContractViolation/VALUES_TOO_LARGE, which is never retried.
        # Exiting 1 here would collapse it into ActivityFailed and let a retry
        # policy repeat a failure the same filter over the same files repeats.
        if [ -n "$_limit" ] && [ "$_size" -gt "$_limit" ]; then
            echo "activity-json: the result is $_size bytes, over this task's values limit of $_limit; write it as an artifact with as: <name>" >&2
        fi
        echo "activity-json: $op -> values.result ($_size bytes)"
        return 0
    fi

    # A JSON string becomes the string's own bytes plus a newline; anything
    # else becomes its compact JSON encoding plus a newline. Writing "hello"
    # with the quotes into a file the next task greps would be the wrong
    # artifact. Written under $TMP first, moved into artifacts/ only once whole.
    if jq -e 'type == "string"' "$TMP/out" >/dev/null 2>&1; then
        jq -r . "$TMP/out" > "$TMP/artifact" || fail "writing the artifact failed"
    else
        cp "$TMP/out" "$TMP/artifact" || fail "writing the artifact failed"
    fi
    mv "$TMP/artifact" "$ARTIFACTS/$as" || fail "writing the artifact failed"
    emit "$ARTIFACTS/$as"
}

# --------------------------------------------------------------- the version --

echo "activity-json: $(jq -n --version)"

# -------------------------------------------------------------------- the op --

op=$(jq -r 'if has("op") then .op | tostring else empty end' "$CONFIG")
case "$op" in
  query|merge) ;;
  "")
    echo "activity-json: with.op is required" >&2
    exit 2
    ;;
  *)
    echo "activity-json: unknown op '$op'" >&2
    exit 2
    ;;
esac

# ---------------------------------------------------------------- no secrets --
#
# A filter can read any file it can name (jq's `import … {search: …}` reads a
# .json file by absolute path), and its result becomes a value in Temporal
# history or an artifact in object storage. This image reads no secret, so a
# task that mounts one is refused rather than trusted not to copy it.
nsecrets=$(jq -r '.secrets | length' "$TASK")
if [ "$nsecrets" != 0 ]; then
    fail "this image reads no secret, and a task that mounts one is refused — a filter or template can read any file it can name, and its result becomes a value or an artifact; remove secrets: from this task"
fi

# ---------------------------------------------------------------- with: as --

key_type as string "a string"
as=
has_as=no
if has_key as; then
    has_as=yes
    as=$(jq -r '.as' "$CONFIG")
    check_name "$as"
fi

case "$op" in
  query)
    # Types first, so a wrongly typed key is named rather than misread.
    key_type filter string "a string"
    key_type src string "a string"
    key_type slurp boolean "a boolean"
    key_type srcs array "an array"
    key_type gather string "a string"
    key_type nullInput boolean "a boolean"
    key_type vars object "an object"

    # Exactly one source form. There is no implicit null input: a forgotten
    # src would otherwise run `.items | length` over null and report 0, a
    # silently wrong answer.
    forms=$(jq -r '[(if has("src") then "src" else empty end), (if has("srcs") then "srcs" else empty end), (if has("gather") then "gather" else empty end), (if .nullInput == true then "nullInput" else empty end)] | join(", ")' "$CONFIG")
    case "$forms" in
      "") fail "query needs exactly one source: with.src, with.srcs, with.gather or with.nullInput: true — none is set" ;;
      *,*) fail "query needs exactly one source: with.src, with.srcs, with.gather or with.nullInput: true — several are set ($forms)" ;;
    esac
    slurp=$(jq -r 'if has("slurp") then .slurp | tostring else "false" end' "$CONFIG")
    if [ "$slurp" = true ] && [ "$forms" != src ]; then
        fail "with.slurp: true reads every JSON value of with.src as one array, and applies only to with.src — not to with.$forms"
    fi

    # The program. It is written to a file and handed to jq with -f, so it
    # never becomes an argv element: a filter that begins with '-' can never be
    # parsed as a jq option, and 64 KiB of program is not one argument.
    has_key filter || fail "query requires with.filter"
    jq -j '.filter' "$CONFIG" > "$TMP/filter.jq" || fail "reading with.filter failed"
    nprog=$(wc -c < "$TMP/filter.jq" | tr -d ' ')
    [ "$nprog" -ge 1 ] || fail "with.filter is empty"
    [ "$nprog" -le "$MAX_PROGRAM_BYTES" ] || fail "with.filter is $nprog bytes, limit is $MAX_PROGRAM_BYTES"

    # Values reach the filter as the jq variable $vars, never spliced into the
    # program text. The type was checked above on config.json itself.
    vars=$(jq -c 'if has("vars") then .vars else {} end' "$CONFIG")
    nvars=$(printf '%s' "$vars" | wc -c | tr -d ' ')
    [ "$nvars" -le "$MAX_VARS_BYTES" ] || fail "with.vars is $nvars bytes as compact JSON, limit is $MAX_VARS_BYTES"

    case "$forms" in
      src)
        src=$(jq -r '.src' "$CONFIG")
        resolve_read "$src"; src=$RESOLVED
        if [ "$slurp" = true ]; then
            run_supervised jq -c --argjson vars "$vars" -s -f "$TMP/filter.jq" "$src" > "$TMP/out"
        else
            run_supervised jq -c --argjson vars "$vars" -f "$TMP/filter.jq" "$src" > "$TMP/out"
        fi
        jq_rc=$?
        ;;

      srcs)
        path_list srcs "$TMP/srcs"
        [ -s "$TMP/srcs" ] || fail "with.srcs names no file; use nullInput: true to run the filter on null"
        set --
        while IFS= read -r raw; do
            resolve_read "$raw"
            set -- "$@" "$RESOLVED"
        done < "$TMP/srcs"
        # One array: every JSON value of every file, in order.
        run_supervised jq -c --argjson vars "$vars" -s -f "$TMP/filter.jq" "$@" > "$TMP/out"
        jq_rc=$?
        ;;

      gather)
        # A fan-out's items, read from task.json — the contract's
        # authoritative list — never from a directory listing.
        gather=$(jq -r '.gather' "$CONFIG")
        case "$gather" in
          [a-z]*) ;;
          *) fail "with.gather must be a task id (^[a-z][a-z0-9-]*\$, at most 200 bytes): $gather" ;;
        esac
        case "$gather" in
          *[!a-z0-9-]*) fail "with.gather must be a task id (^[a-z][a-z0-9-]*\$, at most 200 bytes): $gather" ;;
        esac
        nid=$(printf '%s' "$gather" | wc -c | tr -d ' ')
        [ "$nid" -le 200 ] || fail "with.gather must be a task id (^[a-z][a-z0-9-]*\$, at most 200 bytes): $gather"

        count=$(jq -r --arg id "$gather" '[(if (.inputs | has("eachUpstream")) then .inputs.eachUpstream else [] end)[] | select(.taskId == $id) | .count] | if length == 0 then empty else .[0] end' "$TASK")
        [ -n "$count" ] || fail "task.json lists no fan-out items for '$gather' — gather reads a fan-out that this task names in its needs: and that succeeded; read an ordinary upstream with src: upstream/$gather/values.json"

        jq -r --arg id "$gather" '[.inputs.upstream[] | select(.taskId == $id and has("item"))] | sort_by(.item) | .[].valuesPath' "$TASK" > "$TMP/gather-paths" ||
            fail "reading task.json failed"
        staged=$(wc -l < "$TMP/gather-paths" | tr -d ' ')
        [ "$staged" = "$count" ] ||
            fail "task.json lists $staged item values files for fan-out '$gather', but its item count is $count"

        if [ "$count" = 0 ]; then
            jq -n '[]' > "$TMP/gathered.json" || fail "writing the gathered items failed"
        else
            set --
            while IFS= read -r raw; do
                resolve_read "$raw"
                set -- "$@" "$RESOLVED"
            done < "$TMP/gather-paths"
            jq -c -s . "$@" > "$TMP/gathered.json" || fail "reading the items of '$gather' failed"
        fi
        run_supervised jq -c --argjson vars "$vars" -f "$TMP/filter.jq" "$TMP/gathered.json" > "$TMP/out"
        jq_rc=$?
        ;;

      nullInput)
        run_supervised jq -c --argjson vars "$vars" -n -f "$TMP/filter.jq" > "$TMP/out"
        jq_rc=$?
        ;;
    esac
    [ "$jq_rc" -eq 0 ] || fail "the filter failed (jq exit $jq_rc)"
    report
    ;;

  merge)
    # Deep merge, left to right: jq's `*` recurses into objects, so a nested
    # key of a later file overrides only the leaf it names. Any other value —
    # arrays included — is replaced by the later file's, and a later null
    # replaces too.
    key_type srcs array "an array"
    has_key srcs || fail "merge requires with.srcs naming at least two JSON files"
    path_list srcs "$TMP/srcs"
    set --
    while IFS= read -r raw; do
        resolve_read "$raw"
        jq -e -s 'length == 1 and (.[0] | type) == "object"' "$RESOLVED" >/dev/null 2>&1 ||
            fail "merge source is not exactly one JSON object: $RESOLVED"
        set -- "$@" "$RESOLVED"
    done < "$TMP/srcs"
    [ "$#" -ge 2 ] || fail "merge requires with.srcs naming at least two JSON files, got $#"
    # $d is a jq variable, not a shell one, so the program stays single-quoted.
    # shellcheck disable=SC2016
    run_supervised jq -c -s 'reduce .[] as $d ({}; . * $d)' "$@" > "$TMP/out"
    jq_rc=$?
    [ "$jq_rc" -eq 0 ] || fail "the merge failed (jq exit $jq_rc)"
    report
    ;;
esac

exit 0
