#!/bin/sh
# images/activity-template/entry.sh — render one template file into one text
# artifact.
#
# The rendering is gomplate, a standalone templating CLI. This script is the
# /orkstack ABI around it, exactly as activity-json's is the ABI around jq: it
# reads config.json, confines and checks every path, builds the root context
# and the datasource arguments, supervises the render, and writes values.json.
# No OrkStack library is involved on either side of that line (§10.1).
#
# Structure is activity-file's (§10.2): `set -u` not `set -eu`, `umask 022`
# (rule 7), and the background-and-trap `run_supervised` (rule 2a) so SIGTERM
# reaches gomplate. Every call to run_supervised is a plain invocation, with
# redirection at most, never inside $( ) or a pipeline (rule 2b).
#
# The environment is the task author's, and it is honoured: this script never
# clears, filters or re-executes it. gomplate's own variables are gomplate's to
# honour — the delimiter variables, GOMPLATE_EXPERIMENTAL, and a configuration
# file named by its configuration variable, which gomplate merges UNDER the
# flags passed here. The one variable this script reads on purpose is
# GOMPLATE_TYPE_PARAM, so the type hints it writes follow a renamed parameter.
# Because busybox sh imports every variable as a shell variable, every
# variable this script uses as its own state is assigned before it is read.
#
# No jq invocation here ever reads standard input: each carries -n or names at
# least one file.
set -u
umask 022

CONFIG=/orkstack/input/config.json
TASK=/orkstack/input/task.json
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
    echo "activity-template: $1" >&2
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
    echo "activity-template: $_path ($_size bytes, sha256:$_sha)"
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

# file_url turns an absolute container path into the file:// URL gomplate
# reads, and leaves it in URL. Each path segment is percent-encoded on its
# own, so every byte of the path names exactly the file it says: `a%41.json`
# stays `a%41.json` (gomplate would otherwise decode it to `aA.json`), and a
# `?` or `#` in a file name never opens a query string or a fragment.
URL=
file_url() {
    URL="file://$(jq -rn --arg p "$1" '$p | split("/") | map(@uri) | join("/")')"
}

# media_type picks the parser gomplate uses for a datasource and leaves it in
# MEDIA: YAML for .yaml/.yml, JSON for everything else, extensionless files
# included. The hint is always passed explicitly — gomplate's own guess for an
# unknown suffix is "plain string", and `.field` on a string fails with a type
# error rather than a parse error.
MEDIA=
media_type() {
    case "$1" in
      *.yaml|*.yml) MEDIA=application/yaml ;;
      *) MEDIA=application/json ;;
    esac
}

# --------------------------------------------------------------- the version --

echo "activity-template: $(gomplate --version)"

# -------------------------------------------------------------------- the op --

op=$(jq -r 'if has("op") then .op | tostring else empty end' "$CONFIG")
case "$op" in
  render) ;;
  "")
    echo "activity-template: with.op is required" >&2
    exit 2
    ;;
  *)
    echo "activity-template: unknown op '$op'" >&2
    exit 2
    ;;
esac

# ---------------------------------------------------------------- no secrets --
#
# gomplate's file.Read reads any readable file, and a render becomes an
# artifact in object storage. This image reads no secret, so a task that
# mounts one is refused rather than trusted not to copy it.
nsecrets=$(jq -r '.secrets | length' "$TASK")
if [ "$nsecrets" != 0 ]; then
    fail "this image reads no secret, and a task that mounts one is refused — a filter or template can read any file it can name, and its result becomes a value or an artifact; remove secrets: from this task"
fi

# -------------------------------------------------------- with:, then paths --

key_type src string "a string"
key_type data string "a string"
key_type values object "an object"
key_type datasources object "an object"
key_type leftDelim string "a string"
key_type rightDelim string "a string"
key_type as string "a string"

has_key src || fail "render requires with.src naming the template file"
resolve_read "$(jq -r '.src' "$CONFIG")"
src=$RESOLVED

# The default output name is the template's, minus a template extension:
# `nginx.conf.tmpl` renders to `nginx.conf`, which is what the next task reads.
if has_key as; then
    as=$(jq -r '.as' "$CONFIG")
else
    as=$(basename "$src")
    case "$as" in
      *.tmpl) as=${as%.tmpl} ;;
      *.tpl) as=${as%.tpl} ;;
      *.gotmpl) as=${as%.gotmpl} ;;
    esac
fi
check_name "$as"

# Alternate delimiters, for templates of files that contain `{{` themselves.
# Both or neither, each 1–16 bytes with no whitespace, and different.
delims=no
if has_key leftDelim || has_key rightDelim; then
    jq -e 'has("leftDelim") and has("rightDelim")' "$CONFIG" >/dev/null 2>&1 ||
        fail "with.leftDelim and with.rightDelim are set together or not at all"
    jq -e '[.leftDelim, .rightDelim] | all(utf8bytelength >= 1 and utf8bytelength <= 16 and (test("\\s") | not))' "$CONFIG" >/dev/null 2>&1 ||
        fail "with.leftDelim and with.rightDelim must each be 1–16 bytes with no whitespace"
    jq -e '.leftDelim != .rightDelim' "$CONFIG" >/dev/null 2>&1 ||
        fail "with.leftDelim and with.rightDelim must differ"
    ldelim=$(jq -r '.leftDelim' "$CONFIG")
    rdelim=$(jq -r '.rightDelim' "$CONFIG")
    delims=yes
fi

# `data` is a file an upstream task produced; it must hold one JSON object,
# because it becomes dot and `.field` has to mean the same thing every time.
datafile=
if has_key data; then
    resolve_read "$(jq -r '.data' "$CONFIG")"
    datafile=$RESOLVED
    jq -e -s 'length == 1 and (.[0] | type) == "object"' "$datafile" >/dev/null 2>&1 ||
        fail "with.data must hold one JSON object: $datafile"
fi

# `values` is written inline in the document and rendered by the Orker like any
# other with: value.
inline=no
if has_key values; then
    jq -c '.values' "$CONFIG" > "$TMP/values.json" || fail "reading with.values failed"
    inline=yes
fi

# with.datasources: name -> path. Names are checked as a set first, inside jq,
# so no character of a name can split a record; then each path is fetched back
# by name and confined by exactly the same rules as src and data.
if has_key datasources; then
    jq -e '.datasources | all(type == "string")' "$CONFIG" >/dev/null 2>&1 ||
        fail "every with.datasources value must be a path string"
    badname=$(jq -r '[.datasources | keys[] | select(test("^[A-Za-z0-9_-]{1,64}$") | not)] | if length == 0 then empty else .[0] | @json end' "$CONFIG")
    [ -z "$badname" ] ||
        fail "a with.datasources name must match ^[A-Za-z0-9_-]{1,64}\$: $badname"
fi
jq -r 'if has("datasources") then .datasources | keys[] else empty end' "$CONFIG" > "$TMP/dsnames" ||
    fail "reading with.datasources failed"

# ------------------------------------------------------------ the arguments --
#
# gomplate's type parameter is `type`, unless the task's own environment
# renames it (GOMPLATE_TYPE_PARAM); the hints follow it, so renaming the
# parameter never changes how a datasource is typed.
tp=$(jq -rn --arg p "${GOMPLATE_TYPE_PARAM:-type}" '$p | @uri')

set --
if [ "$delims" = yes ]; then
    # Flags, so they beat both a configuration file's delimiters and the
    # delimiter variables, by gomplate's own precedence.
    set -- "$@" --left-delim "$ldelim" --right-delim "$rdelim"
fi

# The root context (dot): `values` deep-merged over `data` — jq's `*`, so an
# inline key overrides only the leaf it names — or either alone, or {}.
if [ -n "$datafile" ] && [ "$inline" = yes ]; then
    jq -c -s '.[0] * .[1]' "$datafile" "$TMP/values.json" > "$TMP/context.json" ||
        fail "merging with.data and with.values failed"
elif [ -n "$datafile" ]; then
    jq -c . "$datafile" > "$TMP/context.json" || fail "reading $datafile failed"
elif [ "$inline" = yes ]; then
    cp "$TMP/values.json" "$TMP/context.json" || fail "reading with.values failed"
else
    jq -n '{}' > "$TMP/context.json" || fail "writing the root context failed"
fi
file_url "$TMP/context.json"
set -- "$@" -c ".=$URL?$tp=application/json"

# The same two sources, also bound by name and unmerged.
if [ -n "$datafile" ]; then
    file_url "$datafile"
    set -- "$@" -d "data=$URL?$tp=application/json"
fi
if [ "$inline" = yes ]; then
    file_url "$TMP/values.json"
    set -- "$@" -d "values=$URL?$tp=application/json"
fi

while IFS= read -r dsname; do
    case "$dsname" in
      data|values) fail "with.datasources name '$dsname' is reserved — it is set from with.$dsname" ;;
    esac
    resolve_read "$(jq -r --arg n "$dsname" '.datasources[$n]' "$CONFIG")"
    media_type "$RESOLVED"
    if [ "$MEDIA" = application/json ]; then
        # Fail here, naming the datasource, rather than from inside a template
        # action. YAML is left to gomplate.
        jq empty "$RESOLVED" >/dev/null 2>&1 ||
            fail "datasource '$dsname' is not valid JSON: $RESOLVED"
    fi
    file_url "$RESOLVED"
    set -- "$@" -d "$dsname=$URL?$tp=$MEDIA"
done < "$TMP/dsnames"

# --------------------------------------------------------------- the render --
#
# Written to $TMP first and moved into artifacts/ only after gomplate exited 0
# AND left a regular file: gomplate streams its output, so a template that
# fails on line 3 has already written lines 1–2, and that truncated file stays
# in $TMP, never published.
#
# --missing-key error is gomplate's own default, written out because it is a
# contract this image promises. --chmod 0644 because the output would
# otherwise inherit the template's mode, and a 0400 artifact is unreadable to
# the Orker. No configuration-file flag is passed: gomplate reads a
# configuration file only when the task's own environment names one, and
# merges it under these flags by its own rules.
rm -f "$TMP/rendered"
run_supervised gomplate --missing-key error --chmod 0644 "$@" -f "$src" -o "$TMP/rendered" ||
    fail "rendering $src failed"
if [ ! -f "$TMP/rendered" ] || [ -L "$TMP/rendered" ]; then
    fail "gomplate exited 0 but wrote no output file — a configuration file named by GOMPLATE_CONFIG may have sent the output elsewhere (execPipe)"
fi
mv "$TMP/rendered" "$ARTIFACTS/$as" || fail "writing the artifact failed"
emit "$ARTIFACTS/$as"

exit 0
