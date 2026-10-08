#!/bin/sh
# images/activity-git/entry.sh — read-only git: clone a tree, or resolve a ref.
#
# Two operations, and both only READ a remote. Nothing here pushes, commits,
# tags or merges, and nothing here talks to a forge's API: this image is the
# checkout step of a pipeline, not a git client for workloads.
#
# The reason it is a first-party image at all is the token. A checkout under
# `shell op: exec` puts the credential in the URL or in argv, where `ps`, an
# error message and the container's own stdout can all see it. Here the token
# is a secrets FILE, and it reaches git only through the askpass helper baked
# into this image, which reads that file when git asks — and answers only a
# prompt for with.url's own origin. It is never an argument, never part of a
# URL, never an environment value of any process (only its file path is), and
# never printed.
#
# Structure is activity-file's (§10.2): `set -u` not `set -eu`, `umask 022`
# (rule 7), and the background-and-trap `run_supervised` (rule 2a) so SIGTERM
# reaches git. Every git command that contacts a remote, checks out or
# archives runs under run_supervised, as a plain invocation with redirection
# at most, never inside $( ) or a pipeline (rule 2b).
#
# The environment is the task author's, and it is honoured: this script never
# clears, filters or re-executes it, so git sees the image defaults
# (HOME, GIT_CONFIG_NOSYSTEM, GIT_CONFIG_GLOBAL, GIT_TERMINAL_PROMPT,
# GIT_ALLOW_PROTOCOL) overridden by the task's env:. A with: key beats the
# variable that configures the same thing (tokenSecret over GIT_ASKPASS, caFile
# over GIT_SSL_CAINFO). Exactly one variable is refused, explicitly: on a task
# with tokenSecret, a GIT_TRACE_REDACT that git itself does not read as true,
# because that is the one switch that makes git's traces print the token.
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
SECRETS=/orkstack/input/secrets
SHARED=/orkstack/shared
ASKPASS=/usr/local/libexec/activity-git/askpass

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
    echo "activity-git: $1" >&2
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
    echo "activity-git: $_path ($_size bytes, sha256:$_sha)"
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

# check_url is the URL rule, in order, and it never echoes the URL: a refused
# URL is exactly the one that may carry a credential. It leaves the URL in
# `url` and its authority (host[:port], exactly as written) in `authority`.
url=
authority=
check_url() {
    has_key url || fail "with.url is required"
    # Type and bytes, checked in jq on config.json before the value is read
    # into the shell, so a trailing newline cannot be stripped by $( ) and slip
    # through: 1–2048 bytes, every byte printable ASCII (0x21–0x7E).
    jq -e '(.url | type) == "string" and (.url | length) >= 1 and (.url | length) <= 2048 and (.url | explode | all(. >= 33 and . <= 126))' "$CONFIG" >/dev/null 2>&1 ||
        fail "with.url must be a non-empty string of printable ASCII with no whitespace"
    url=$(jq -r '.url' "$CONFIG")
    case "$url" in
      https://*) ;;
      *) fail "with.url must start with https:// — no other transport is offered" ;;
    esac
    # A query is where hosting services take tokens, and git cannot use one
    # (it appends its own path after it); a fragment means nothing to git.
    case "$url" in
      *'?'*|*'#'*) fail "with.url must not carry a query string (?) or fragment (#)" ;;
    esac
    _rest=${url#https://}
    authority=${_rest%%/*}
    case "$authority" in
      *@*) fail "with.url must not carry credentials — give the token with tokenSecret" ;;
    esac
    printf '%s' "$authority" | grep -Eq '^([A-Za-z0-9._-]+|\[[0-9A-Fa-f:.]+\])(:[0-9]{1,5})?$' ||
        fail "with.url must have a host[:port] authority"
}

# --------------------------------------------------------------- the version --

echo "activity-git: $(git --version)"

# -------------------------------------------------------------------- the op --

op=$(jq -r 'if has("op") then .op | tostring else empty end' "$CONFIG")
case "$op" in
  clone|resolve) ;;
  "")
    echo "activity-git: with.op is required" >&2
    exit 2
    ;;
  *)
    echo "activity-git: unknown op '$op'" >&2
    exit 2
    ;;
esac

# -------------------------------------------------- with:, before any remote --

key_type ref string "a string"
key_type tokenSecret string "a string"
key_type tokenUser string "a string"
key_type caFile string "a string"
key_type depth number "a number"
key_type as string "a string"
key_type dest string "a string"

check_url

ref=HEAD
if has_key ref; then
    ref=$(jq -r '.ref' "$CONFIG")
    [ -n "$ref" ] || fail "with.ref is empty; omit it to mean the remote's HEAD"
fi

user=x-access-token
if has_key tokenUser; then
    user=$(jq -r '.tokenUser' "$CONFIG")
    # It is matched inside git's prompt by the askpass helper, so it is a
    # restricted name, validated and never sanitized.
    jq -e '.tokenUser | test("^[A-Za-z0-9._~-]{1,128}$")' "$CONFIG" >/dev/null 2>&1 ||
        fail "with.tokenUser must match ^[A-Za-z0-9._~-]{1,128}\$: $user"
fi

if has_key tokenSecret; then
    secret=$(jq -r '.tokenSecret' "$CONFIG")
    jq -e '.tokenSecret | test("^[A-Za-z0-9._-]{1,128}$")' "$CONFIG" >/dev/null 2>&1 ||
        fail "with.tokenSecret must match ^[A-Za-z0-9._-]{1,128}\$ (it names $SECRETS/<name>): $secret"
    [ -f "$SECRETS/$secret" ] ||
        fail "with.tokenSecret names no mounted secret: expected a regular file at $SECRETS/$secret — declare it in the task's secrets:"

    # The one refusal. git's traces redact the Authorization header unless
    # GIT_TRACE_REDACT is false, so a false value would print the token into
    # this task's logs. The value is judged by git's OWN boolean parser —
    # `--file /dev/null` reads no configuration, so --default is what is read
    # — never by a grammar of this script's own. A line, not the whole output:
    # a task that routes git's traces to stdout adds trace lines to it.
    if [ -n "${GIT_TRACE_REDACT+x}" ]; then
        redact=$(git config --file /dev/null --type=bool --default "$GIT_TRACE_REDACT" --get activity-git.redact)
        redact_rc=$?
        if [ "$redact_rc" -ne 0 ] || ! printf '%s\n' "$redact" | grep -qx true; then
            fail "with.tokenSecret is set, and git does not read GIT_TRACE_REDACT='$GIT_TRACE_REDACT' as true — a false value makes git's traces print the token into this task's logs; remove it from env: or set it to true"
        fi
    fi

    # tokenSecret beats a GIT_ASKPASS of the task's own: a with: key is the
    # more specific statement. The helper is told the token's PATH, never its
    # value.
    GIT_ASKPASS=$ASKPASS
    ACTIVITY_GIT_TOKEN_FILE=$SECRETS/$secret
    ACTIVITY_GIT_USER=$user
    ACTIVITY_GIT_AUTHORITY=$authority
    export GIT_ASKPASS ACTIVITY_GIT_TOKEN_FILE ACTIVITY_GIT_USER ACTIVITY_GIT_AUTHORITY
fi

if has_key caFile; then
    # A PEM bundle that REPLACES git's trust store for this task. caFile beats
    # a GIT_SSL_CAINFO of the task's own; it does not re-enable verification
    # the task's environment turned off.
    resolve_read "$(jq -r '.caFile' "$CONFIG")"
    [ -r "$RESOLVED" ] || fail "with.caFile is not readable: $RESOLVED"
    GIT_SSL_CAINFO=$RESOLVED
    export GIT_SSL_CAINFO
fi

depth=1
as=
dest=
if [ "$op" = clone ]; then
    if has_key depth; then
        jq -e '.depth >= 0 and .depth == (.depth | floor)' "$CONFIG" >/dev/null 2>&1 ||
            fail "with.depth must be a non-negative integer"
        depth=$(jq -r '.depth' "$CONFIG")
        case "$depth" in
          ''|*[!0-9]*) fail "with.depth must be a non-negative integer" ;;
        esac
    fi
    if has_key as && has_key dest; then
        fail "clone takes with.as (a tarball artifact) or with.dest (a working tree in a pin), never both"
    fi
    if has_key dest; then
        dest=$(jq -r '.dest' "$CONFIG")
    else
        as=src.tar.gz
        if has_key as; then
            as=$(jq -r '.as' "$CONFIG")
        fi
        check_name "$as"
    fi
fi

# ------------------------------------------------------ talking to the remote --
#
# Defined only here, after every with: check above: nothing before this line
# contacts a remote, so the GIT_TRACE_REDACT refusal always comes first.

# ls_remote runs `git ls-remote [--symref] <url> <patterns>` against with.url
# under supervision, its output in $TMP/ls-remote. A first argument of
# --symref is an option, and git reads options only before the URL.
ls_remote() {
    if [ "${1:-}" = --symref ]; then
        shift
        run_supervised git ls-remote --symref "$url" "$@" > "$TMP/ls-remote"
    else
        run_supervised git ls-remote "$url" "$@" > "$TMP/ls-remote"
    fi
    _rc=$?
    [ "$_rc" -eq 0 ] || fail "git ls-remote against $url failed (git exit $_rc; git's message is above)"
}

# resolve_ref is ONE algorithm for both ops, and leaves the result in COMMIT
# and REFNAME, with BYID=yes when ref was a full commit id. A short name is
# resolved against exactly two candidates, refs/heads/X and refs/tags/X —
# `git ls-remote <url> main` matches by TAIL and also returns
# refs/heads/feature/main — and an annotated tag is peeled to its commit.
# Ambiguity is refused, never ordered.
COMMIT=
REFNAME=
BYID=no
resolve_ref() {
    case "$ref" in
      HEAD)
        ls_remote --symref HEAD
        COMMIT=$(jq -R -r 'split("\t") | select(length == 2 and .[1] == "HEAD" and (.[0] | startswith("ref: ") | not)) | .[0]' "$TMP/ls-remote")
        REFNAME=$(jq -R -r 'split("\t") | select(length == 2 and .[1] == "HEAD" and (.[0] | startswith("ref: "))) | .[0] | ltrimstr("ref: ")' "$TMP/ls-remote")
        [ -n "$REFNAME" ] || REFNAME=HEAD
        [ -n "$COMMIT" ] || fail "no HEAD on $url"
        return 0
        ;;
    esac
    _hex=no
    case "$ref" in
      *[!0-9a-f]*) ;;
      *) _hex=yes ;;
    esac
    _len=$(printf '%s' "$ref" | wc -c | tr -d ' ')
    if [ "$_hex" = yes ] && [ "$_len" -eq 40 ]; then
        COMMIT=$ref
        REFNAME=$ref
        BYID=yes
        return 0
    fi
    if [ "$_hex" = yes ] && [ "$_len" -eq 64 ]; then
        fail "SHA-256 repositories are not supported: ref must be a 40-hex commit id, a branch, a tag or a full ref name"
    fi
    case "$ref" in
      refs/*) ls_remote "$ref" "$ref^{}" ;;
      *) ls_remote "refs/heads/$ref" "refs/tags/$ref" "refs/tags/$ref^{}" ;;
    esac
    jq -R -r --arg r "$ref" 'split("\t") | select(length == 2) | select(if ($r | startswith("refs/")) then .[1] == $r else (.[1] == "refs/heads/" + $r or .[1] == "refs/tags/" + $r) end) | .[1]' "$TMP/ls-remote" > "$TMP/candidates" ||
        fail "reading git ls-remote's output failed"
    _n=$(wc -l < "$TMP/candidates" | tr -d ' ')
    if [ "$_n" -eq 0 ]; then
        fail "no branch or tag named '$ref' on $url"
    fi
    if [ "$_n" -gt 1 ]; then
        fail "'$ref' names both refs/heads/$ref and refs/tags/$ref on $url; write the full ref name"
    fi
    REFNAME=$(cat "$TMP/candidates")
    COMMIT=$(jq -R -r --arg n "$REFNAME^{}" 'split("\t") | select(length == 2 and .[1] == $n) | .[0]' "$TMP/ls-remote")
    [ -n "$COMMIT" ] ||
        COMMIT=$(jq -R -r --arg n "$REFNAME" 'split("\t") | select(length == 2 and .[1] == $n) | .[0]' "$TMP/ls-remote")
    [ -n "$COMMIT" ] || fail "could not read the commit of $REFNAME from git ls-remote"
}

# fetch_into fetches what resolve_ref chose into a fresh repository, under
# supervision, and leaves the commit actually fetched in FETCHED — the one
# reported, even if the ref moved after it was resolved.
FETCHED=
fetch_into() {
    git init --quiet "$1" || fail "git init failed"
    git -C "$1" remote add origin "$url" || fail "git remote add failed"
    if [ "$BYID" = yes ]; then
        _what=$COMMIT
    else
        _what=$REFNAME
    fi
    if [ "$depth" -eq 0 ]; then
        run_supervised git -C "$1" fetch --quiet --no-tags origin "$_what"
    else
        run_supervised git -C "$1" fetch --quiet --no-tags --depth "$depth" origin "$_what"
    fi
    _rc=$?
    if [ "$_rc" -ne 0 ]; then
        if [ "$BYID" = yes ]; then
            echo "activity-git: fetching commit $COMMIT failed (git's message is above); if this remote does not serve a commit by id, name the branch or tag in ref: instead" >&2
        fi
        fail "fetching '$_what' from $url failed (git exit $_rc)"
    fi
    FETCHED=$(git -C "$1" rev-parse 'FETCH_HEAD^{commit}') ||
        fail "could not read the fetched commit"
}

# ------------------------------------------------- a pinned working tree: dest --
#
# Only under /orkstack/shared of a pinned task: /orkstack/tmp is per-attempt
# and nothing else ever reads it. The checkout is staged beside dest and
# renamed into place, so dest exists only after a COMPLETE checkout, and a
# pinned retry that sees what a failed predecessor left can still succeed.
stage=
if [ -n "$dest" ]; then
    jq -e '.paths | has("shared")' "$TASK" >/dev/null 2>&1 ||
        fail "with.dest needs a pinned task (pin:) — /orkstack/shared exists only for one"
    case "$dest" in
      "$SHARED"/?*) ;;
      *) fail "with.dest must be $SHARED/<path>: $dest" ;;
    esac
    case "/$dest/" in
      */../*) fail "with.dest may not contain '..': $dest" ;;
    esac
    case "$dest" in
      *//*|*/) fail "with.dest may not contain an empty path component: $dest" ;;
    esac
    [ -d "$SHARED" ] || fail "$SHARED is not mounted for this task"

    # Every EXISTING component between /orkstack/shared and dest must be a
    # directory and not a symbolic link: an earlier member of the pin may have
    # created either, and the rename below must land inside /orkstack/shared.
    _p=$SHARED
    _restp=${dest#"$SHARED"/}
    while :; do
        case "$_restp" in
          */*) ;;
          *) break ;;
        esac
        _p="$_p/${_restp%%/*}"
        _restp=${_restp#*/}
        if [ -L "$_p" ]; then
            fail "with.dest: $_p is a symbolic link"
        fi
        if [ -e "$_p" ] && [ ! -d "$_p" ]; then
            fail "with.dest: $_p is not a directory"
        fi
    done

    if [ -L "$dest" ]; then
        fail "with.dest: $dest is a symbolic link"
    fi
    if [ -e "$dest" ]; then
        [ -d "$dest" ] || fail "with.dest already exists and is not a directory: $dest"
        [ -z "$(ls -A "$dest")" ] || fail "with.dest already exists and is not empty: $dest"
    fi

    mkdir -p "$(dirname "$dest")" || fail "could not create the parents of $dest"
    # The staging name is reserved: a previous attempt's leftovers are removed.
    stage="$(dirname "$dest")/.activity-git-$(basename "$dest")"
    rm -rf "$stage" || fail "could not remove $stage"
fi

# ------------------------------------------------------------------- the work --

resolve_ref

case "$op" in
  resolve)
    # A commit id is returned as itself without contacting the remote:
    # resolve does not verify a commit id; clone does.
    jq -n --arg c "$COMMIT" --arg r "$REFNAME" '{commit: $c, ref: $r}' > "$VALUES" ||
        fail "writing values.json failed"
    echo "activity-git: $REFNAME -> $COMMIT"
    ;;

  clone)
    if [ -n "$dest" ]; then
        fetch_into "$stage"
        run_supervised git -C "$stage" -c advice.detachedHead=false checkout --quiet --detach "$FETCHED" ||
            fail "checking out $FETCHED failed"
        if [ -d "$dest" ]; then
            rmdir "$dest" || fail "could not replace the empty directory $dest"
        fi
        # An atomic rename: both ends are under /orkstack/shared.
        mv "$stage" "$dest" || fail "could not move the checkout into $dest"
        jq -n --arg c "$FETCHED" --arg r "$REFNAME" --arg p "$dest" '{commit: $c, ref: $r, path: $p}' > "$VALUES" ||
            fail "writing values.json failed"
        echo "activity-git: $REFNAME -> $FETCHED checked out at $dest"
    else
        fetch_into "$TMP/repo"
        # git archive of the commit: members carry the commit time in tree
        # order with no ./ prefix, and git compresses in-process, so one commit
        # archived by one platform image under its default environment gives
        # one tarball digest on every run.
        run_supervised git -C "$TMP/repo" archive --format=tar.gz -o "$TMP/archive" "$FETCHED" ||
            fail "archiving $FETCHED failed"
        mv "$TMP/archive" "$ARTIFACTS/$as" || fail "writing the artifact failed"
        emit "$ARTIFACTS/$as"
        jq -c --arg c "$FETCHED" --arg r "$REFNAME" '{commit: $c, ref: $r} + .' "$VALUES" > "$TMP/values.json" ||
            fail "writing values.json failed"
        mv "$TMP/values.json" "$VALUES" || fail "writing values.json failed"
        echo "activity-git: $REFNAME -> $FETCHED"
    fi
    ;;
esac

exit 0
