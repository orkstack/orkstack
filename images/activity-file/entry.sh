#!/bin/sh
# images/activity-file/entry.sh — seven file operations, and no eighth.
#
# The operation set is AGENTS.md §9's exactly: copy, checksum, gzip, gunzip,
# tar, untar, concat. It is deliberately not extensible; a workload that needs
# more puts it in its own image.
#
# Structure is activity-shell's (§10.2): `set -u` not `set -eu`, `umask 022`
# (rule 7), and the background-and-trap `run_supervised` (rule 2a). Every call
# to run_supervised is a plain invocation, with redirection at most, never
# inside $( ) or a pipeline (rule 2b).
set -u
umask 022

CONFIG=/orkstack/input/config.json
VALUES=/orkstack/output/values.json
INPUT=/orkstack/input
ARTIFACTS=/orkstack/output/artifacts
TMP=/orkstack/tmp

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
    echo "activity-file: $1" >&2
    exit 1
}

# resolve_read turns a configured source into an absolute container path and
# leaves the result in RESOLVED. It is deliberately NOT a $( ) helper: `fail`
# inside a command substitution would exit only the subshell and let the script
# carry on with an empty path — the same subshell hazard as §10.1 rule 2b.
#
# Relative paths resolve under /orkstack/input (§10.2); absolute paths must
# already be inside the workspace. A `..` component is refused rather than
# normalized, so a path can never be argued out of the workspace.
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
    echo "activity-file: $_path ($_size bytes, sha256:$_sha)"
}

op=$(jq -r '.op // empty' "$CONFIG")
as=$(jq -r '.as // empty' "$CONFIG")

case "$op" in
  copy)
    resolve_read "$(jq -r '.src // empty' "$CONFIG")"; src=$RESOLVED
    [ -n "$as" ] || as=$(basename "$src")
    check_name "$as"
    run_supervised cp "$src" "$ARTIFACTS/$as" || fail "copy failed"
    emit "$ARTIFACTS/$as"
    ;;

  checksum)
    # checksum reads and reports; it produces no artifact, because there is
    # nothing new to store. `path` names the file that was read.
    resolve_read "$(jq -r '.src // empty' "$CONFIG")"; src=$RESOLVED
    emit "$src"
    ;;

  gzip)
    resolve_read "$(jq -r '.src // empty' "$CONFIG")"; src=$RESOLVED
    [ -n "$as" ] || as="$(basename "$src").gz"
    check_name "$as"
    run_supervised gzip -c "$src" > "$ARTIFACTS/$as" || fail "gzip failed"
    emit "$ARTIFACTS/$as"
    ;;

  gunzip)
    resolve_read "$(jq -r '.src // empty' "$CONFIG")"; src=$RESOLVED
    if [ -z "$as" ]; then
        as=$(basename "$src")
        case "$as" in *.gz) as=${as%.gz} ;; esac
    fi
    check_name "$as"
    run_supervised gunzip -c "$src" > "$ARTIFACTS/$as" || fail "gunzip failed"
    emit "$ARTIFACTS/$as"
    ;;

  tar)
    # `srcs` is a JSON array of paths. Members are stored under their base
    # names, so the archive has no leading workspace path in it.
    [ -n "$as" ] || as="archive.tar.gz"
    check_name "$as"

    jq -r '(.srcs // []) | .[]' "$CONFIG" > "$TMP/tar-srcs"
    [ -s "$TMP/tar-srcs" ] || fail "tar requires a non-empty with.srcs"
    mkdir -p "$TMP/tar-stage"
    while IFS= read -r raw; do
        resolve_read "$raw"
        cp "$RESOLVED" "$TMP/tar-stage/$(basename "$RESOLVED")" || fail "staging failed: $RESOLVED"
    done < "$TMP/tar-srcs"
    run_supervised tar -czf "$ARTIFACTS/$as" -C "$TMP/tar-stage" . || fail "tar failed"
    emit "$ARTIFACTS/$as"
    ;;

  untar)
    # Exactly one artifact per op, so untar names the member it extracts. A
    # whole hierarchy is not an artifact — §3.5 has no directory semantics.
    resolve_read "$(jq -r '.src // empty' "$CONFIG")"; src=$RESOLVED
    member=$(jq -r '.member // empty' "$CONFIG")
    [ -n "$member" ] || fail "untar requires with.member naming the file to extract"
    case "$member" in
      /*) fail "with.member must be a path inside the archive: $member" ;;
      *..*) fail "with.member may not contain '..': $member" ;;
    esac
    [ -n "$as" ] || as=$(basename "$member")
    check_name "$as"
    rm -rf "$TMP/untar"
    mkdir -p "$TMP/untar"
    run_supervised tar -xzf "$src" -C "$TMP/untar" "$member" || fail "untar failed"
    [ -f "$TMP/untar/$member" ] || fail "archive member is not a regular file: $member"
    cp "$TMP/untar/$member" "$ARTIFACTS/$as" || fail "extract failed"
    emit "$ARTIFACTS/$as"
    ;;

  concat)
    [ -n "$as" ] || fail "concat requires with.as naming the output artifact"
    check_name "$as"
    jq -r '(.srcs // []) | .[]' "$CONFIG" > "$TMP/concat-srcs"
    [ -s "$TMP/concat-srcs" ] || fail "concat requires a non-empty with.srcs"
    : > "$ARTIFACTS/$as"
    while IFS= read -r raw; do
        resolve_read "$raw"
        cat "$RESOLVED" >> "$ARTIFACTS/$as" || fail "concat failed at $RESOLVED"
    done < "$TMP/concat-srcs"
    emit "$ARTIFACTS/$as"
    ;;

  "")
    echo "activity-file: with.op is required" >&2
    exit 2
    ;;
  *)
    echo "activity-file: unknown op '$op'" >&2
    exit 2
    ;;
esac

exit 0
