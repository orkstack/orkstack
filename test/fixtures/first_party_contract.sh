#!/usr/bin/env bash
#
# first_party_contract.sh — the per-image contract fixtures of the first-party
# transform images: activity-json, activity-git and activity-template
# (docs/design first-party-transform-images §12.3).
#
# Like the mandatory Bash fixture (bash_activity_contract.sh, which stays
# standalone), it runs an image against the ABI with no Orkflow, no Temporal,
# no OrkStack binary and no SDK in any language: docker, jq, cat and a
# redirect. Every case builds one /orkstack workspace, runs the image under the
# contract's §9.2 flags with --network none, and checks what the image wrote.
# A case that stands for a task's `env:` adds `-e NAME=value` — exactly how the
# Orker passes `env:` to a container.
#
#   usage:  bash test/fixtures/first_party_contract.sh <json|git|template> <image>
#   exit:   0 all assertions passed
#           1 an assertion failed
#          77 a prerequisite is absent (AGENTS.md §11.1 skip convention);
#             ORK_REQUIRE_INFRA=1 turns that into a failure instead.
#
# There is no per-case skip: a missing prerequisite exits 77 BEFORE any case
# runs, so a completed run always prints `skipped: 0`.

set -uo pipefail

NAME=${1:-}
case "$NAME" in
  json|git|template) ;;
  *) printf 'usage: %s <json|git|template> <image>\n' "$0" >&2; exit 1 ;;
esac
IMAGE=${2:-localhost:5001/orkstack/activity-$NAME:dev}

GRACE=${ORK_ACTIVITY_TEST_GRACE:-5}          # cancellation grace period, seconds
DEADLINE=${ORK_ACTIVITY_TEST_DEADLINE:-30}   # bound on every wait, seconds

VALUES_MAX_BYTES=262144

# ---------------------------------------------------------------- reporting --

PASSED=0
FAILED=0
SKIPPED=0


bold()  { printf '\033[1m%s\033[0m\n' "$*"; }
info()  { printf '     %s\n' "$*"; }

section() {
    printf '\n'
    bold "── $1"
}

pass() { PASSED=$((PASSED + 1)); printf '  \033[32mPASS\033[0m %s\n' "$1"; }
fail() { FAILED=$((FAILED + 1)); printf '  \033[31mFAIL\033[0m %s\n' "$1"; }
skip() { SKIPPED=$((SKIPPED + 1)); printf '  \033[33mSKIP\033[0m %s\n' "$1"; }

check() {   # check <description> <expected> <actual>
    if [[ "$2" == "$3" ]]; then
        pass "$1"
    else
        fail "$1"
        info "expected: $2"
        info "actual:   $3"
    fi
}

contains() {  # contains <description> <needle> <haystack>
    if [[ "$3" == *"$2"* ]]; then
        pass "$1"
    else
        fail "$1"
        info "expected to contain: $2"
        info "actual: ${3:-(empty)}"
    fi
}

absent() {  # absent <description> <needle> <haystack>
    if [[ "$3" != *"$2"* ]]; then
        pass "$1"
    else
        fail "$1"
        info "must not contain: $2"
        info "actual: $3"
    fi
}

differs() {  # differs <description> <a> <b>
    if [[ -n "$2" && "$2" != "$3" ]]; then
        pass "$1"
    else
        fail "$1"
        info "first:  ${2:-(empty)}"
        info "second: ${3:-(empty)}"
    fi
}

# ------------------------------------------------------------ prerequisites --

skip_run() {
    printf '\n\033[33mSKIP\033[0m %s\n' "$1"
    if [[ "${ORK_REQUIRE_INFRA:-0}" == "1" ]]; then
        printf 'ORK_REQUIRE_INFRA=1, so a missing prerequisite is a failure.\n'
        exit 1
    fi
    exit 77
}

command -v docker >/dev/null 2>&1 || skip_run "docker is not installed"
docker version --format '{{.Server.Version}}' >/dev/null 2>&1 ||
    skip_run "the docker daemon is not reachable"
command -v jq >/dev/null 2>&1 || skip_run "jq is not installed"
command -v sha256sum >/dev/null 2>&1 || skip_run "sha256sum is not installed"
docker image inspect "$IMAGE" >/dev/null 2>&1 ||
    skip_run "image not present locally: $IMAGE (build it with: make -C images images)"

# ------------------------------------------------------------- the workspace --
#
# The workspace root MUST be on a path the container daemon can bind-mount
# (bash_activity_contract.sh explains why); probe it with a real bind mount.

bindable() {   # bindable <dir> — can the daemon actually bind-mount this?
    docker run --rm \
        --mount "type=bind,source=$1,destination=/probe,readonly" \
        alpine:3.20 true >/dev/null 2>&1
}

ROOT=
for candidate in \
    "${ORK_ACTIVITY_TEST_WORKDIR:-}" \
    "${ORK_ORKER_WORKDIR:-}" \
    "$PWD/.data/contract-fixture" \
    "$HOME/.orkstack/contract-fixture" \
    "${TMPDIR:-/tmp}"
do
    [[ -n "$candidate" ]] || continue
    mkdir -p "$candidate" 2>/dev/null || continue
    probe=$(mktemp -d "$candidate/bindprobe.XXXXXX" 2>/dev/null) || continue
    if bindable "$probe"; then
        rmdir "$probe"
        ROOT=$(mktemp -d "$candidate/ork-first-party.XXXXXX")
        break
    fi
    rmdir "$probe" 2>/dev/null
done

if [[ -z "$ROOT" ]]; then
    skip_run "InfrastructureError/WORKDIR_NOT_BINDABLE: the daemon cannot bind-mount any candidate workspace root. Set ORK_ACTIVITY_TEST_WORKDIR to a path the daemon shares."
fi

CONTAINERS=()

# shellcheck disable=SC2329  # invoked by the EXIT trap below
cleanup() {
    for c in "${CONTAINERS[@]:-}"; do
        [[ -n "$c" ]] && docker rm -f "$c" >/dev/null 2>&1
    done
    # The images run as 65532 and some cases leave directories that uid owns
    # (a git repository in tmp/, a checkout in shared/); a root container
    # opens them up so the host user can remove the whole root.
    docker run --rm --mount "type=bind,source=$ROOT,destination=/w" \
        alpine:3.20 chmod -R a+rwX /w >/dev/null 2>&1
    chmod -R u+rwX "$ROOT" 2>/dev/null
    rm -rf "$ROOT"
}
trap cleanup EXIT

# mkws builds one per-attempt workspace as §3.1 specifies and echoes its path.
# A case may adjust task.json with a jq filter — limits.valuesMaxBytes,
# paths.shared, secrets, inputs.upstream, inputs.eachUpstream — and stage
# files with `put` before the run; `seal` sets the modes when the image runs.
# A task.json carrying paths.shared gets a shared/ directory, mounted at
# /orkstack/shared.
#
#   mkws <config-json> [task.json jq filter]
mkws() {
    local config=$1 filter=${2:-.} w
    w=$(mktemp -d "$ROOT/ws.XXXXXX")

    mkdir -p "$w/input/secrets" "$w/input/artifacts" "$w/input/upstream"
    mkdir -p "$w/output/artifacts" "$w/tmp"

    jq -n --arg image "$IMAGE" --argjson deadline "$DEADLINE" --argjson grace "$GRACE" \
          --argjson vmax "$VALUES_MAX_BYTES" '{
      schemaVersion: 1,
      idempotencyKey: "default/ork-first-party-fixture-1/t1",
      run: {id: "ork-first-party-fixture-1", namespace: "default",
            orkflow: {name: "first-party-fixture", version: "0.0.0",
                      definitionDigest: "sha256:0000000000000000000000000000000000000000000000000000000000000000"}},
      task: {id: "t1", attempt: 1, pool: "main"},
      image: {reference: $image, pinned: $image,
              indexDigest: "sha256:0000000000000000000000000000000000000000000000000000000000000000"},
      limits: {timeoutSeconds: $deadline, cancellationGracePeriodSeconds: $grace,
               valuesMaxBytes: $vmax, artifactMaxCount: 256, artifactMaxBytes: 5368709120,
               logMaxBytes: 16777216, commitMaxBytes: 4194304},
      network: "none",
      paths: {input: "/orkstack/input", config: "/orkstack/input/config.json",
              runInput: "/orkstack/input/run.json", secrets: "/orkstack/input/secrets",
              output: "/orkstack/output", values: "/orkstack/output/values.json",
              artifacts: "/orkstack/output/artifacts", tmp: "/orkstack/tmp"},
      inputs: {artifacts: [], upstream: []},
      secrets: []
    }' | jq "$filter" > "$w/input/task.json"

    printf '%s\n' "$config" > "$w/input/config.json"
    printf '{}\n' > "$w/input/run.json"
    if [[ "$(jq -r '.paths | has("shared")' "$w/input/task.json")" == true ]]; then
        mkdir -p "$w/shared"
    fi
    printf '%s' "$w"
}

# put <w> <path under the workspace> <content> — stage a file (no newline added).
put() {
    mkdir -p "$(dirname "$1/$2")"
    printf '%s' "$3" > "$1/$2"
}

# seal sets the §3.1 modes explicitly rather than inheriting them.
seal() {
    local w=$1
    find "$w/input" -type f -exec chmod 0444 {} +
    find "$w/input" -type d -exec chmod 0555 {} +
    chmod 1777 "$w/output" "$w/output/artifacts" "$w/tmp"
    [[ -d "$w/shared" ]] && chmod 1777 "$w/shared"
    return 0
}

# docker_args emits the §9.2 profile for a workspace. Every flag is explicit.
docker_args() {
    local w=$1
    printf '%s\0' \
        --read-only \
        --cap-drop=ALL \
        --security-opt no-new-privileges \
        --pids-limit 512 \
        --memory 512m \
        --cpus 1 \
        --network none \
        --init \
        --workdir /orkstack \
        --label "ork.run=ork-first-party-fixture-1" \
        --tmpfs "/tmp:rw,nosuid,nodev,size=16m" \
        --mount "type=bind,source=$w/input,destination=/orkstack/input,readonly" \
        --mount "type=bind,source=$w/output,destination=/orkstack/output" \
        --mount "type=bind,source=$w/tmp,destination=/orkstack/tmp"
    if [[ -d "$w/shared" ]]; then
        printf '%s\0' --mount "type=bind,source=$w/shared,destination=/orkstack/shared"
    fi
}

# drun runs the image to completion and echoes its exit status. Arguments
# before `--` go to `docker run` (`-e NAME=value` for a task's env:,
# `--entrypoint …`); arguments after it follow the image. stdout lands in
# $w/stdout, stderr in $w/stderr, both in $w/container.log. With STDIN_DATA
# set, the container's stdin is a pipe holding it (`docker run -i`).
drun() {
    local w=$1; shift
    local pre=() post=() seen=0 a args=() rc
    for a in "$@"; do
        if (( seen )); then post+=("$a")
        elif [[ "$a" == -- ]]; then seen=1
        else pre+=("$a")
        fi
    done
    seal "$w"
    mapfile -d '' -t args < <(docker_args "$w")
    if [[ -n "${STDIN_DATA:-}" ]]; then
        printf '%s' "$STDIN_DATA" |
            docker run --rm -i "${args[@]}" "${pre[@]}" "$IMAGE" "${post[@]}" > "$w/stdout" 2> "$w/stderr"
        rc=${PIPESTATUS[1]}
    else
        docker run --rm "${args[@]}" "${pre[@]}" "$IMAGE" "${post[@]}" > "$w/stdout" 2> "$w/stderr" < /dev/null
        rc=$?
    fi
    cat "$w/stdout" "$w/stderr" > "$w/container.log"
    printf '%s' "$rc"
}

# run_detached starts the image and echoes the container id.
run_detached() {
    local w=$1 args=() cid
    seal "$w"
    mapfile -d '' -t args < <(docker_args "$w")
    cid=$(docker run -d "${args[@]}" "$IMAGE" 2>"$w/run.err")
    CONTAINERS+=("$cid")
    printf '%s' "$cid"
}

# await_process blocks until the container's process list shows <pattern>
# (the tool the case wants to interrupt is RUNNING, not merely about to be),
# or the deadline passes. A bounded wait on an observable, never a sleep.
await_process() {
    local cid=$1 pattern=$2 deadline=$((SECONDS + DEADLINE))
    while (( SECONDS < deadline )); do
        if docker top "$cid" 2>/dev/null | grep -qF -- "$pattern"; then
            return 0
        fi
        if [[ "$(docker inspect -f '{{.State.Running}}' "$cid" 2>/dev/null)" == "false" ]]; then
            return 1
        fi
    done
    return 1
}

# sigterm_case starts a workspace detached, waits for <process> to run, stops
# the container with the grace period, and checks it exited 1 inside it.
sigterm_case() {
    local label=$1 w=$2 process=$3 cid started elapsed status
    cid=$(run_detached "$w")
    if [[ -z "$cid" ]]; then
        fail "$label: container started"
        info "$(cat "$w/run.err" 2>/dev/null)"
        return
    fi
    if ! await_process "$cid" "$process"; then
        fail "$label: $process started within ${DEADLINE}s"
        info "logs: $(docker logs "$cid" 2>&1 | tr '\n' '|')"
        return
    fi
    started=$SECONDS
    docker stop --timeout "$GRACE" "$cid" >/dev/null 2>&1
    elapsed=$((SECONDS - started))
    status=$(docker inspect -f '{{.State.ExitCode}}' "$cid" 2>/dev/null)
    docker rm -f "$cid" >/dev/null 2>&1
    check "$label: exited 1 on SIGTERM, not 137 (SIGKILL) or 143" "1" "$status"
    if (( elapsed < GRACE )); then
        pass "$label: honored SIGTERM inside the ${GRACE}s grace period (${elapsed}s)"
    else
        fail "$label: took ${elapsed}s, i.e. it was SIGKILLed at the grace boundary"
    fi
}

values() { jq -c "${2:-.}" "$1/output/values.json" 2>/dev/null; }
artifacts() { find "$1/output/artifacts" -mindepth 1 -maxdepth 1 -printf '%f\n' 2>/dev/null | sort | tr '\n' ' ' | sed 's/ $//'; }
first_line() { head -n 1 "$1/stdout"; }

bold "First-party image contract — activity-$NAME"
info "image:     $IMAGE"
info "grace:     ${GRACE}s"
info "workspace: $ROOT"

# ============================================================================
# activity-json
# ============================================================================

cases_json() {
    local w w2 rc h1 h2 big

    section "J1 query src over an object"
    w=$(mkws '{"op":"query","src":"doc.json","filter":"{a: .a, n: (.b | length)}"}')
    put "$w" input/doc.json '{"a":"x","b":[1,2,3]}'
    rc=$(drun "$w")
    check "J1 exit 0" "0" "$rc"
    check "J1 values.json is {\"result\":…}" '{"result":{"a":"x","n":3}}' "$(values "$w")"
    check "J1 values.json is mode 0644" "644" "$(stat -c %a "$w/output/values.json" 2>/dev/null)"

    section "J2 srcs is one array; src with slurp reads JSON Lines"
    w=$(mkws '{"op":"query","srcs":["a.json","b.json"],"filter":"."}')
    put "$w" input/a.json '{"doc":1}'
    put "$w" input/b.json '{"doc":2}'
    rc=$(drun "$w")
    check "J2 srcs: the filter sees [doc1,doc2]" '{"result":[{"doc":1},{"doc":2}]}' "$(values "$w")"
    w=$(mkws '{"op":"query","src":"lines.json","slurp":true,"filter":"map(.n) | add"}')
    put "$w" input/lines.json $'{"n":1}\n{"n":2}\n{"n":3}\n'
    rc=$(drun "$w")
    check "J2 src + slurp: true reads every value as one array" '{"result":6}' "$(values "$w")"

    section "J3 gather a fan-out's items"
    local gather_task='.inputs.eachUpstream = [{taskId: "fan", count: 3}]
      | .inputs.upstream = [
          {taskId: "fan", item: 2, valuesPath: "/orkstack/input/upstream/fan/2/values.json"},
          {taskId: "fan", item: 0, valuesPath: "/orkstack/input/upstream/fan/0/values.json"},
          {taskId: "fan", item: 1, valuesPath: "/orkstack/input/upstream/fan/1/values.json"},
          {taskId: "plain", valuesPath: "/orkstack/input/upstream/plain/values.json"}]'
    w=$(mkws '{"op":"query","gather":"fan","filter":"map(.result)"}' "$gather_task")
    put "$w" input/upstream/fan/0/values.json '{"result":"zero"}'
    put "$w" input/upstream/fan/1/values.json '{"result":"one"}'
    put "$w" input/upstream/fan/2/values.json '{"result":"two"}'
    put "$w" input/upstream/plain/values.json '{"result":"plain"}'
    rc=$(drun "$w")
    check "J3 items in item order, whatever task.json's order" '{"result":["zero","one","two"]}' "$(values "$w")"
    w=$(mkws '{"op":"query","gather":"fan","filter":"."}' '.inputs.eachUpstream = [{taskId: "fan", count: 0}]')
    rc=$(STDIN_DATA='[1]' drun "$w")
    check "J3 zero items gather [] — and stdin holding [1] is never read" '{"result":[]}' "$(values "$w")"
    w=$(mkws '{"op":"query","gather":"fan","filter":"."}' \
        '.inputs.eachUpstream = [{taskId: "fan", count: 3}] | .inputs.upstream = [{taskId: "fan", item: 0, valuesPath: "/orkstack/input/upstream/fan/0/values.json"}]')
    put "$w" input/upstream/fan/0/values.json '{"result":"zero"}'
    rc=$(drun "$w")
    check "J3 count mismatch exits 1" "1" "$rc"
    contains "J3 count mismatch names both counts" "lists 1 item values files for fan-out 'fan', but its item count is 3" "$(cat "$w/stderr")"
    w=$(mkws '{"op":"query","gather":"nope","filter":"."}' "$gather_task")
    rc=$(drun "$w")
    check "J3 an id with no fan-out items exits 1" "1" "$rc"
    contains "J3 … with the gather message" "task.json lists no fan-out items for 'nope'" "$(cat "$w/stderr")"
    w=$(mkws '{"op":"query","gather":"plain","filter":"."}' "$gather_task")
    put "$w" input/upstream/plain/values.json '{"result":"plain"}'
    rc=$(drun "$w")
    check "J3 an ordinary upstream exits 1" "1" "$rc"
    contains "J3 … pointing at src:" "src: upstream/plain/values.json" "$(cat "$w/stderr")"

    section "J4 vars, nullInput and key types"
    w=$(mkws '{"op":"query","nullInput":true,"vars":{"limit":"3","on":true},"filter":"[$vars.limit, $vars.on, .]"}')
    rc=$(drun "$w")
    check "J4 \$vars is bound and the input is null" '{"result":["3",true,null]}' "$(values "$w")"
    w=$(mkws '{"op":"query","nullInput":true,"filter":"$vars"}')
    rc=$(drun "$w")
    check "J4 \$vars is {} when vars is absent" '{"result":{}}' "$(values "$w")"
    local v
    for v in '"x"' false null; do
        w=$(mkws "{\"op\":\"query\",\"nullInput\":true,\"filter\":\"1\",\"vars\":$v}")
        rc=$(drun "$w")
        check "J4 vars: $v exits 1" "1" "$rc"
        contains "J4 vars: $v is refused, never read as {}" "with.vars must be an object" "$(cat "$w/stderr")"
    done
    w=$(mkws '{"op":"query","nullInput":true,"filter":"1","as":false}')
    rc=$(drun "$w")
    check "J4 as: false exits 1" "1" "$rc"
    contains "J4 as: false names as" "with.as must be a string" "$(cat "$w/stderr")"
    w=$(mkws '{"op":"query","nullInput":"true","filter":"1"}')
    rc=$(drun "$w")
    check "J4 nullInput: \"true\" exits 1" "1" "$rc"
    contains "J4 nullInput: \"true\" names nullInput" "with.nullInput must be a boolean" "$(cat "$w/stderr")"
    w=$(mkws '{"op":"query","src":"doc.json","slurp":null,"filter":"1"}')
    put "$w" input/doc.json '{}'
    rc=$(drun "$w")
    check "J4 slurp: null exits 1" "1" "$rc"
    contains "J4 slurp: null names slurp" "with.slurp must be a boolean" "$(cat "$w/stderr")"
    w=$(mkws '{"op":null}')
    rc=$(drun "$w")
    check "J4 op: null exits 2" "2" "$rc"
    contains "J4 op: null is an unknown op" "unknown op 'null'" "$(cat "$w/stderr")"
    w=$(mkws '{"op":"query","src":"doc.json","nullInput":false,"filter":".a"}')
    put "$w" input/doc.json '{"a":1}'
    rc=$(drun "$w")
    check "J4 nullInput: false beside src runs (a boolean, not a source form)" '{"result":1}' "$(values "$w")"

    section "J5 exactly one source form"
    w=$(mkws '{"op":"query","filter":"1"}')
    rc=$(drun "$w")
    check "J5 no source form exits 1" "1" "$rc"
    contains "J5 … naming the four forms" "with.src, with.srcs, with.gather or with.nullInput: true" "$(cat "$w/stderr")"
    w=$(mkws '{"op":"query","filter":"1","src":"doc.json","nullInput":true}')
    put "$w" input/doc.json '{}'
    rc=$(drun "$w")
    check "J5 two source forms exit 1" "1" "$rc"
    contains "J5 … naming the forms" "several are set (src, nullInput)" "$(cat "$w/stderr")"
    w=$(mkws '{"op":"query","filter":"1","srcs":["doc.json"],"slurp":true}')
    put "$w" input/doc.json '{}'
    rc=$(drun "$w")
    check "J5 slurp: true with srcs exits 1" "1" "$rc"
    contains "J5 … naming the forms" "applies only to with.src" "$(cat "$w/stderr")"
    w=$(mkws '{"op":"query","filter":".","srcs":[]}')
    rc=$(STDIN_DATA='[1]' drun "$w")
    check "J5 srcs: [] exits 1 (stdin holding [1] is never read)" "1" "$rc"
    contains "J5 … naming srcs" "with.srcs names no file; use nullInput: true to run the filter on null" "$(cat "$w/stderr")"

    section "J6 exactly one output"
    w=$(mkws '{"op":"query","nullInput":true,"filter":"empty"}')
    rc=$(drun "$w")
    check "J6 empty exits 1" "1" "$rc"
    contains "J6 … no output" "the filter produced no output" "$(cat "$w/stderr")"
    w=$(mkws '{"op":"query","nullInput":true,"filter":"1,2"}')
    rc=$(drun "$w")
    check "J6 1,2 exits 1" "1" "$rc"
    contains "J6 … 2 outputs" "the filter produced 2 outputs" "$(cat "$w/stderr")"

    section "J7 as writes an artifact"
    w=$(mkws '{"op":"query","nullInput":true,"filter":"\"hello\"","as":"greeting.txt"}')
    rc=$(drun "$w")
    check "J7 a string result is its bytes plus \\n" "$(printf 'hello\n' | od -An -tx1)" "$(od -An -tx1 < "$w/output/artifacts/greeting.txt" 2>/dev/null)"
    w=$(mkws '{"op":"query","nullInput":true,"filter":"{a: 1, b: [true]}","as":"doc.json"}')
    rc=$(drun "$w")
    check "J7 anything else is compact JSON plus \\n" "$(printf '{"a":1,"b":[true]}\n' | od -An -tx1)" "$(od -An -tx1 < "$w/output/artifacts/doc.json" 2>/dev/null)"
    check "J7 values.json is {path,sizeBytes,sha256}" '["path","sha256","sizeBytes"]' "$(values "$w" 'keys')"
    check "J7 the sha256 matches the artifact" "$(sha256sum "$w/output/artifacts/doc.json" | cut -d' ' -f1)" "$(values "$w" '.sha256' | tr -d '"')"
    check "J7 path names the artifact" '"/orkstack/output/artifacts/doc.json"' "$(values "$w" '.path')"

    section "J8 merge"
    w=$(mkws '{"op":"merge","srcs":["a.json","b.json"]}')
    put "$w" input/a.json '{"x":{"keep":1,"leaf":1,"arr":[1,2]},"gone":1}'
    put "$w" input/b.json '{"x":{"leaf":2,"arr":[3]},"gone":null}'
    rc=$(drun "$w")
    check "J8 deep merge: leaf override, array replaced, later null wins" \
        '{"result":{"x":{"keep":1,"leaf":2,"arr":[3]},"gone":null}}' "$(values "$w")"
    w=$(mkws '{"op":"merge","srcs":["a.json"]}')
    put "$w" input/a.json '{}'
    rc=$(drun "$w")
    check "J8 one source exits 1" "1" "$rc"
    w=$(mkws '{"op":"merge","srcs":["a.json","arr.json"]}')
    put "$w" input/a.json '{}'
    put "$w" input/arr.json '[1]'
    rc=$(drun "$w")
    check "J8 a non-object source exits 1" "1" "$rc"
    contains "J8 … naming it" "/orkstack/input/arr.json" "$(cat "$w/stderr")"

    section "J9 path confinement"
    for v in ../etc/passwd /etc/passwd upstream; do
        w=$(mkws "{\"op\":\"query\",\"src\":\"$v\",\"filter\":\".\"}")
        rc=$(drun "$w")
        check "J9 src: $v exits 1" "1" "$rc"
    done

    section "J10 a task that mounts a secret is refused"
    w=$(mkws '{"op":"query","nullInput":true,"filter":"1"}' '.secrets = [{name: "tok", path: "/orkstack/input/secrets/tok"}]')
    put "$w" input/secrets/tok 'TOKSECRET'
    rc=$(drun "$w")
    check "J10 exit 1" "1" "$rc"
    contains "J10 the secrets sentence" "this image reads no secret, and a task that mounts one is refused" "$(cat "$w/stderr")"

    section "J11 the values-size hint"
    w=$(mkws '{"op":"query","nullInput":true,"filter":"[range(60)]"}' '.limits.valuesMaxBytes = 64')
    rc=$(drun "$w")
    check "J11 an over-limit result still exits 0 (the Orker refuses it, not the image)" "0" "$rc"
    check "J11 values.json is written" "true" "$(values "$w" 'has("result")')"
    contains "J11 stderr carries the as: hint" "write it as an artifact with as: <name>" "$(cat "$w/stderr")"

    section "J12 numbers — the image's half"
    w=$(mkws '{"op":"query","src":"n.json","filter":".n"}')
    put "$w" input/n.json '{"n":12345678901234567890}'
    rc=$(drun "$w")
    check "J12 an unchanged 20-digit integer keeps its literal text" '{"result":12345678901234567890}' "$(cat "$w/output/values.json" 2>/dev/null)"
    w=$(mkws '{"op":"query","src":"n.json","filter":".n","as":"n.json"}')
    put "$w" input/n.json '{"n":12345678901234567890}'
    rc=$(drun "$w")
    check "J12 … and as an artifact" "$(printf '12345678901234567890\n' | od -An -tx1)" "$(od -An -tx1 < "$w/output/artifacts/n.json" 2>/dev/null)"
    w=$(mkws '{"op":"query","nullInput":true,"vars":{"n":"1e+06"},"filter":"[$vars.n | tonumber, ($vars.n | tonumber | floor)]"}')
    rc=$(drun "$w")
    check "J12 a templated 1e+06: tonumber | floor recovers 1000000" '{"result":[1E+6,1000000]}' "$(cat "$w/output/values.json" 2>/dev/null)"

    section "J13 the environment reaches the filter"
    w=$(mkws '{"op":"query","nullInput":true,"filter":"[$ENV.HOME, $ENV.TZ, (0 | strflocaltime(\"%H\"))]"}')
    rc=$(drun "$w")
    check "J13(a) the image defaults" '{"result":["/orkstack/tmp","UTC","00"]}' "$(values "$w")"
    w=$(mkws '{"op":"query","nullInput":true,"filter":"[$ENV.FOO, (0 | strflocaltime(\"%H\"))]"}')
    rc=$(drun "$w" -e FOO=bar -e TZ=Europe/Berlin)
    check "J13(b) a task's variable is visible and its TZ resolves through tzdata" '{"result":["bar","01"]}' "$(values "$w")"
    w=$(mkws '{"op":"query","nullInput":true,"filter":"$ENV.HOSTNAME"}')
    rc=$(drun "$w")
    h1=$(values "$w" '.result')
    w2=$(mkws '{"op":"query","nullInput":true,"filter":"$ENV.HOSTNAME"}')
    rc=$(drun "$w2")
    h2=$(values "$w2" '.result')
    check "J13(c) HOSTNAME is a non-empty string" "true" "$(values "$w" '.result | type == "string" and length > 0')"
    differs "J13(c) HOSTNAME differs in two containers (the README's runtime-set variable)" "$h1" "$h2"

    section "J14 op"
    w=$(mkws '{"filter":"1"}')
    rc=$(drun "$w")
    check "J14 missing op exits 2" "2" "$rc"
    contains "J14 … with.op is required" "with.op is required" "$(cat "$w/stderr")"
    w=$(mkws '{"op":"transform"}')
    rc=$(drun "$w")
    check "J14 unknown op exits 2" "2" "$rc"

    section "J15 the version line"
    w=$(mkws '{"op":"query","nullInput":true,"filter":"1"}')
    rc=$(drun "$w")
    contains "J15 stdout's first line names jq 1.7" "activity-json: jq-1.7." "$(first_line "$w")"

    section "J16 SIGTERM reaches jq"
    w=$(mkws '{"op":"query","nullInput":true,"filter":"last(range(1e12))"}')
    sigterm_case "J16" "$w" "jq -c"

    section "J17 the filter is a program file"
    w=$(mkws '{"op":"query","nullInput":true,"filter":"-1 | . + 2"}')
    rc=$(drun "$w")
    check "J17 a filter beginning with '-' is a program, never an option" '{"result":1}' "$(values "$w")"
    big=$(jq -cn '{op: "query", nullInput: true, filter: (". #" + ("x" * 65533))}')
    w=$(mkws "$big")
    rc=$(drun "$w")
    check "J17 a 65536-byte filter runs" "0" "$rc"
    big=$(jq -cn '{op: "query", nullInput: true, filter: (". #" + ("x" * 65534))}')
    w=$(mkws "$big")
    rc=$(drun "$w")
    check "J17 a 65537-byte filter exits 1" "1" "$rc"
    contains "J17 … naming the size" "with.filter is 65537 bytes, limit is 65536" "$(cat "$w/stderr")"
    w=$(mkws '{"op":"query","nullInput":true,"filter":5}')
    rc=$(drun "$w")
    check "J17 filter: 5 exits 1" "1" "$rc"
}

# ============================================================================
# activity-template
# ============================================================================

cases_template() {
    local w w2 rc a1 a2 i l1 l2

    section "T1 data and values: merged as dot, unmerged by name"
    w=$(mkws '{"op":"render","src":"t.tmpl","data":"d.json","values":{"b":{"c":9}}}')
    put "$w" input/d.json '{"a":1,"b":{"c":2,"d":3}}'
    put "$w" input/t.tmpl $'{{ .a }} {{ .b.c }} {{ .b.d }}|{{ (ds "data").b.c }}|{{ (ds "values").b.c }} {{ has (ds "values") "a" }}\n'
    rc=$(drun "$w")
    check "T1 exit 0" "0" "$rc"
    check "T1 dot is data * values; ds data and ds values are each unmerged" "1 9 3|2|9 false" "$(cat "$w/output/artifacts/t" 2>/dev/null)"

    section "T2 JSON and YAML datasources"
    w=$(mkws '{"op":"render","src":"t.tmpl","datasources":{"list":"list.json","conf":"conf.yaml"}}')
    put "$w" input/list.json '[{"n":"a"},{"n":"b"}]'
    put "$w" input/conf.yaml $'port: 8080\n'
    put "$w" input/t.tmpl $'{{ range ds "list" }}[{{ .n }}]{{ end }} {{ (ds "conf").port }}\n'
    rc=$(drun "$w")
    check "T2 a top-level array ranged over, and a YAML datasource" "[a][b] 8080" "$(cat "$w/output/artifacts/t" 2>/dev/null)"

    section "T3 exact datasource file names"
    w=$(mkws '{"op":"render","src":"t.tmpl","datasources":{"sp":"a b.json","pct":"a%41.json","q":"x?y.json"}}')
    put "$w" "input/a b.json" '{"n":"space"}'
    put "$w" "input/a%41.json" '{"n":"percent"}'
    put "$w" "input/aA.json" '{"n":"decoy"}'
    put "$w" "input/x?y.json" '{"n":"query"}'
    put "$w" input/t.tmpl $'{{ (ds "sp").n }} {{ (ds "pct").n }} {{ (ds "q").n }}\n'
    rc=$(drun "$w")
    check "T3 each name reads its own file (never the decoy aA.json)" "space percent query" "$(cat "$w/output/artifacts/t" 2>/dev/null)"

    section "T4 a missing key fails and publishes nothing"
    w=$(mkws '{"op":"render","src":"t.tmpl"}')
    put "$w" input/t.tmpl $'line 1\nline 2\n{{ .nope }}\n'
    rc=$(drun "$w")
    check "T4 exit 1" "1" "$rc"
    contains "T4 stderr names the key" "nope" "$(cat "$w/stderr")"
    check "T4 output/artifacts/ is empty (no truncated file)" "" "$(artifacts "$w")"

    section "T5 default rescues an empty value, not an absent key"
    w=$(mkws '{"op":"render","src":"t.tmpl"}')
    put "$w" input/t.tmpl $'{{ index . "k" | default "d" }}\n'
    rc=$(drun "$w")
    check "T5 index . \"k\" | default \"d\" renders d" "d" "$(cat "$w/output/artifacts/t" 2>/dev/null)"
    w=$(mkws '{"op":"render","src":"t.tmpl"}')
    put "$w" input/t.tmpl $'{{ .k | default "d" }}\n'
    rc=$(drun "$w")
    check "T5 .k | default \"d\" fails" "1" "$rc"

    section "T6 alternate delimiters"
    w=$(mkws '{"op":"render","src":"t.tmpl","values":{"y":"Y"},"leftDelim":"[[","rightDelim":"]]"}')
    put "$w" input/t.tmpl $'{{ .x }} [[ .y ]]\n'
    rc=$(drun "$w")
    check "T6 {{ }} verbatim, [[ ]] substituted" "{{ .x }} Y" "$(cat "$w/output/artifacts/t" 2>/dev/null)"
    w=$(mkws '{"op":"render","src":"t.tmpl","leftDelim":"[["}')
    put "$w" input/t.tmpl 'x'
    rc=$(drun "$w")
    check "T6 half-set delimiters exit 1" "1" "$rc"
    w=$(mkws '{"op":"render","src":"t.tmpl","leftDelim":"%%","rightDelim":"%%"}')
    put "$w" input/t.tmpl 'x'
    rc=$(drun "$w")
    check "T6 equal delimiters exit 1" "1" "$rc"

    section "T7 the artifact is mode 0644"
    # The 0400 template must belong to the image's own uid to be readable at
    # all, so a first container (that uid) writes it into /orkstack/tmp.
    w=$(mkws '{"op":"render","src":"/orkstack/tmp/t.tmpl"}')
    rc=$(drun "$w" --entrypoint sh -- -c 'printf x > /orkstack/tmp/t.tmpl && chmod 0400 /orkstack/tmp/t.tmpl')
    check "T7 the template is mode 0400" "400" "$(stat -c %a "$w/tmp/t.tmpl" 2>/dev/null)"
    rc=$(drun "$w")
    check "T7 a 0400 template renders a 0644 artifact" "0:644" "$rc:$(stat -c %a "$w/output/artifacts/t" 2>/dev/null)"

    section "T8 a task that mounts a secret is refused"
    w=$(mkws '{"op":"render","src":"t.tmpl"}' '.secrets = [{name: "tok", path: "/orkstack/input/secrets/tok"}]')
    put "$w" input/t.tmpl 'x'
    put "$w" input/secrets/tok 'TOKSECRET'
    rc=$(drun "$w")
    check "T8 exit 1" "1" "$rc"
    contains "T8 the secrets sentence" "this image reads no secret" "$(cat "$w/stderr")"

    section "T9 datasource names and contents"
    for v in data 'a.b'; do
        w=$(mkws "{\"op\":\"render\",\"src\":\"t.tmpl\",\"datasources\":{\"$v\":\"d.json\"}}")
        put "$w" input/t.tmpl 'x'
        put "$w" input/d.json '{}'
        rc=$(drun "$w")
        check "T9 datasource name '$v' exits 1" "1" "$rc"
    done
    w=$(mkws '{"op":"render","src":"t.tmpl","datasources":{"broken":"d.json"}}')
    put "$w" input/t.tmpl 'x'
    put "$w" input/d.json '{"a":'
    rc=$(drun "$w")
    check "T9 a malformed JSON datasource exits 1" "1" "$rc"
    contains "T9 … naming it" "datasource 'broken' is not valid JSON" "$(cat "$w/stderr")"
    w=$(mkws '{"op":"render","src":"t.tmpl","data":"d.json"}')
    put "$w" input/t.tmpl 'x'
    put "$w" input/d.json '[1]'
    rc=$(drun "$w")
    check "T9 data holding an array exits 1" "1" "$rc"

    section "T10 the environment reaches gomplate; with: wins where both speak"
    w=$(mkws '{"op":"render","src":"t.tmpl"}')
    put "$w" input/t.tmpl $'{{ time.ZoneName }} {{ getenv "HOME" }}\n'
    rc=$(drun "$w")
    check "T10(a) the image defaults" "UTC /orkstack/tmp" "$(cat "$w/output/artifacts/t" 2>/dev/null)"
    local envs=(-e GREETING=hi -e TZ=Europe/Berlin -e 'GOMPLATE_LEFT_DELIM=[[' -e 'GOMPLATE_RIGHT_DELIM=]]')
    w=$(mkws '{"op":"render","src":"t.tmpl"}')
    put "$w" input/t.tmpl $'[[ getenv "GREETING" ]] [[ (time.Unix 0).Local.Format "15:04 MST" ]] {{ .x }}\n'
    rc=$(drun "$w" "${envs[@]}")
    check "T10(b) a task's variable, its TZ through tzdata, gomplate's delimiter variables" "hi 01:00 CET {{ .x }}" "$(cat "$w/output/artifacts/t" 2>/dev/null)"
    w=$(mkws '{"op":"render","src":"t.tmpl","leftDelim":"<<","rightDelim":">>"}')
    put "$w" input/t.tmpl $'<< getenv "GREETING" >> [[ .x ]]\n'
    rc=$(drun "$w" "${envs[@]}")
    check "T10(c) with.leftDelim/rightDelim beat the variables" "hi [[ .x ]]" "$(cat "$w/output/artifacts/t" 2>/dev/null)"
    w=$(mkws '{"op":"render","src":"t.tmpl"}')
    put "$w" input/t.tmpl $'{{ net.CIDRHost 1 "10.0.0.0/8" }}\n'
    rc=$(drun "$w" -e GOMPLATE_EXPERIMENTAL=true)
    check "T10(d) GOMPLATE_EXPERIMENTAL enables experimental functions" "10.0.0.1" "$(cat "$w/output/artifacts/t" 2>/dev/null)"
    w=$(mkws '{"op":"render","src":"t.tmpl"}')
    put "$w" input/t.tmpl $'{{ net.CIDRHost 1 "10.0.0.0/8" }}\n'
    rc=$(drun "$w")
    check "T10(d) without it the same template exits 1" "1" "$rc"
    contains "T10(d) … experimental mode not enabled" "experimental mode not enabled" "$(cat "$w/stderr")"
    w=$(mkws '{"op":"render","src":"t.tmpl","datasources":{"d":"d.txt"}}')
    put "$w" input/t.tmpl $'{{ (ds "d").a }}\n'
    put "$w" input/d.txt '{"a":1}'
    rc=$(drun "$w" -e GOMPLATE_TYPE_PARAM=t)
    check "T10(e) the type hint follows GOMPLATE_TYPE_PARAM" "1" "$(cat "$w/output/artifacts/t" 2>/dev/null)"

    local cfgenv=(-e GOMPLATE_CONFIG=/orkstack/input/c.yaml)
    w=$(mkws '{"op":"render","src":"t.tmpl","values":{"x":"X","y":"Y"}}')
    put "$w" input/c.yaml $'leftDelim: "[["\nrightDelim: "]]"\n'
    put "$w" input/t.tmpl $'[[ .x ]] {{ .y }}\n'
    rc=$(drun "$w" "${cfgenv[@]}")
    check "T10(f1) a configuration file reaches gomplate" "0:X {{ .y }}" "$rc:$(cat "$w/output/artifacts/t" 2>/dev/null)"
    w=$(mkws '{"op":"render","src":"t.tmpl","values":{"x":"X","y":"Y"},"leftDelim":"<<","rightDelim":">>"}')
    put "$w" input/c.yaml $'leftDelim: "[["\nrightDelim: "]]"\n'
    put "$w" input/t.tmpl $'<< .x >> [[ .y ]]\n'
    rc=$(drun "$w" "${cfgenv[@]}")
    check "T10(f2) with: delimiters beat the file's" "X [[ .y ]]" "$(cat "$w/output/artifacts/t" 2>/dev/null)"
    w=$(mkws '{"op":"render","src":"t.tmpl","values":{"x":"X","y":"Y"}}')
    put "$w" input/c.yaml $'chmod: "0600"\noutputFiles: [/orkstack/output/artifacts/stray]\n'
    put "$w" input/t.tmpl $'{{ .x }}\n'
    rc=$(drun "$w" "${cfgenv[@]}")
    check "T10(f3) exit 0" "0" "$rc"
    check "T10(f3) the image's --chmod wins" "644" "$(stat -c %a "$w/output/artifacts/t" 2>/dev/null)"
    check "T10(f3) the image's -o wins (no stray artifact)" "t" "$(artifacts "$w")"
    w=$(mkws '{"op":"render","src":"t.tmpl","values":{"x":"X","y":"Y"}}')
    put "$w" input/c.yaml $'missingKey: zero\n'
    put "$w" input/t.tmpl $'{{ .nope }}\n'
    rc=$(drun "$w" "${cfgenv[@]}")
    check "T10(f4) the file's missingKey is kept over --missing-key error (gomplate 4.3.3)" "0:<no value>" "$rc:$(cat "$w/output/artifacts/t" 2>/dev/null)"
    w=$(mkws '{"op":"render","src":"t.tmpl","values":{"x":"X","y":"Y"}}')
    put "$w" input/c.yaml $'execPipe: true\npostExec: [cat]\n'
    put "$w" input/t.tmpl $'{{ .x }}\n'
    rc=$(drun "$w" "${cfgenv[@]}")
    check "T10(f5) execPipe exits 1" "1" "$rc"
    contains "T10(f5) … wrote no output file" "wrote no output file" "$(cat "$w/stderr")"
    check "T10(f5) output/artifacts/ is empty" "" "$(artifacts "$w")"
    w=$(mkws '{"op":"render","src":"t.tmpl"}')
    put "$w" input/t.tmpl 'x'
    rc=$(drun "$w" -e GOMPLATE_CONFIG=/orkstack/input/none.yaml)
    check "T10(f6) a configuration file that cannot be opened exits 1" "1" "$rc"
    contains "T10(f6) … gomplate's own message" "config file requested, but couldn't be opened" "$(cat "$w/stderr")"
    check "T10(f6) output/artifacts/ is empty" "" "$(artifacts "$w")"

    section "T11 the version line"
    w=$(mkws '{"op":"render","src":"t.tmpl"}')
    put "$w" input/t.tmpl 'x'
    rc=$(drun "$w")
    check "T11 stdout's first line" "activity-template: gomplate version 4.3.3" "$(first_line "$w")"

    section "T12 the artifact name"
    w=$(mkws '{"op":"render","src":"app.conf.tmpl"}')
    put "$w" input/app.conf.tmpl 'x'
    rc=$(drun "$w")
    check "T12 as defaults to basename(src) minus .tmpl" "app.conf" "$(artifacts "$w")"
    w=$(mkws '{"op":"render","src":"t.tmpl","as":"../x"}')
    put "$w" input/t.tmpl 'x'
    rc=$(drun "$w")
    check "T12 an unsafe as exits 1" "1" "$rc"

    section "T13 op"
    w=$(mkws '{"src":"t.tmpl"}')
    rc=$(drun "$w")
    check "T13 missing op exits 2" "2" "$rc"
    w=$(mkws '{"op":"draw"}')
    rc=$(drun "$w")
    check "T13 unknown op exits 2" "2" "$rc"

    section "T14 SIGTERM reaches gomplate"
    w=$(mkws '{"op":"render","src":"t.tmpl"}')
    put "$w" input/t.tmpl '{{ range math.Seq 1 100000 }}{{ range math.Seq 1 100000 }}{{ end }}{{ end }}'
    sigterm_case "T14" "$w" "gomplate --missing-key"
    check "T14 no artifact" "" "$(artifacts "$w")"

    section "T15 path confinement for every path key"
    local spec
    for spec in '{"src":"../t.tmpl"}|../t.tmpl' '{"src":"t.tmpl","data":"/etc/passwd"}|/etc/passwd' \
                '{"src":"t.tmpl","data":"../input/x.json"}|../input/x.json' \
                '{"src":"t.tmpl","datasources":{"d":"upstream"}}|/orkstack/input/upstream' \
                '{"src":"t.tmpl","datasources":{"d":"/etc/hosts"}}|/etc/hosts'; do
        w=$(mkws "$(jq -cn --argjson w "${spec%%|*}" '{op: "render"} + $w')")
        put "$w" input/t.tmpl 'x'
        put "$w" input/x.json '{}'
        rc=$(drun "$w")
        check "T15 ${spec%%|*} exits 1" "1" "$rc"
        contains "T15 … naming ${spec#*|}" "${spec#*|}" "$(cat "$w/stderr")"
        check "T15 … and publishes nothing" "" "$(artifacts "$w")"
    done

    section "T16 the caching inventory's witness"
    local tmpl=$'{{ time.Since (time.Parse "2006" "2020") }}\n{{ time.Until (time.Parse "2006" "2100") }}\n{{ jq "now" . }}\n{{ include "file:///etc/hostname" }}\n{{ getenv "HOSTNAME" "none" }}\n'
    w=$(mkws '{"op":"render","src":"t.tmpl"}')
    put "$w" input/t.tmpl "$tmpl"
    a1=$(drun "$w")
    w2=$(mkws '{"op":"render","src":"t.tmpl"}')
    put "$w2" input/t.tmpl "$tmpl"
    a2=$(drun "$w2")
    check "T16 both renders exit 0" "0 0" "$a1 $a2"
    for i in 1 2 3 4 5; do
        l1=$(grep -v '^$' "$w/output/artifacts/t" 2>/dev/null | sed -n "${i}p")
        l2=$(grep -v '^$' "$w2/output/artifacts/t" 2>/dev/null | sed -n "${i}p")
        differs "T16 line $i differs between two containers with identical inputs" "$l1" "$l2"
    done

    section "T17 key types"
    for spec in '"values":false|values' '"datasources":null|datasources' '"data":false|data' '"as":false|as' \
                '"leftDelim":null,"rightDelim":"]]"|leftDelim'; do
        w=$(mkws "{\"op\":\"render\",\"src\":\"t.tmpl\",${spec%%|*}}")
        put "$w" input/t.tmpl 'x'
        rc=$(drun "$w")
        check "T17 ${spec%%|*} exits 1" "1" "$rc"
        contains "T17 … naming ${spec#*|}" "with.${spec#*|} must be" "$(cat "$w/stderr")"
    done
    w=$(mkws '{"op":null}')
    rc=$(drun "$w")
    check "T17 op: null exits 2" "2" "$rc"
}

# ============================================================================
# activity-git (offline; the network cases are the integration suites')
# ============================================================================

GIT_ID=0123456789abcdef0123456789abcdef01234567

git_resolve_cfg() { jq -cn --arg u "$1" --arg r "$GIT_ID" '{op: "resolve", url: $u, ref: $r}'; }

cases_git() {
    local w rc u out c sha1 sha2 sha3 v ctr

    section "G1 HTTPS only"
    for u in 'http://h/r.git' 'git://h/r.git' 'ssh://h/r.git' 'file:///r.git' 'ext::sh%20-c%20x' 'HTTPS://h/r.git'; do
        w=$(mkws "$(git_resolve_cfg "$u")")
        rc=$(drun "$w")
        check "G1 $u exits 1" "1" "$rc"
        contains "G1 … must start with https://" "with.url must start with https://" "$(cat "$w/stderr")"
    done

    section "G2 the URL rule — and the URL is never echoed"
    local rule_ascii="must be a non-empty string of printable ASCII with no whitespace"
    for u in 'https://h/r .git' $'https://h/r\t.git' $'https://h/r\n.git' $'https://h/r.git\n' $'https://h/r\x7f.git' 'https://h/ré.git' ''; do
        w=$(mkws "$(git_resolve_cfg "$u")")
        rc=$(drun "$w")
        check "G2(a) $(printf '%q' "$u") exits 1" "1" "$rc"
        contains "G2(a) … $rule_ascii" "$rule_ascii" "$(cat "$w/stderr")"
        if [[ -n "$u" ]]; then
            absent "G2(a) … the URL appears nowhere" "$u" "$(cat "$w/container.log")"
        fi
    done
    w=$(mkws '{"op":"resolve","url":5}')
    rc=$(drun "$w")
    contains "G2(a) url: 5 — $rule_ascii" "$rule_ascii" "$(cat "$w/stderr")"
    w=$(mkws '{"op":"resolve"}')
    rc=$(drun "$w")
    contains "G2(a) url absent — with.url is required" "with.url is required" "$(cat "$w/stderr")"
    for u in 'https://h/r.git?private_token=QSECRET' 'https://h/r.git#main'; do
        w=$(mkws "$(git_resolve_cfg "$u")")
        rc=$(drun "$w")
        check "G2(b) $u exits 1" "1" "$rc"
        contains "G2(b) … no query string or fragment" "must not carry a query string (?) or fragment (#)" "$(cat "$w/stderr")"
        absent "G2(b) … the URL appears nowhere" "$u" "$(cat "$w/container.log")"
        absent "G2(b) … QSECRET appears nowhere" "QSECRET" "$(cat "$w/container.log")"
    done
    for u in 'https://user:TOKSECRET@h/r.git' 'https://user@h/r.git'; do
        w=$(mkws "$(git_resolve_cfg "$u")")
        rc=$(drun "$w")
        check "G2(c) $u exits 1" "1" "$rc"
        contains "G2(c) … must not carry credentials" "must not carry credentials" "$(cat "$w/stderr")"
        absent "G2(c) … the URL appears nowhere" "$u" "$(cat "$w/container.log")"
        absent "G2(c) … TOKSECRET appears nowhere" "TOKSECRET" "$(cat "$w/container.log")"
    done
    for u in 'https:///r.git' 'https://h:abc/r.git' 'https://h:/r.git' 'https://h%40x/r.git'; do
        w=$(mkws "$(git_resolve_cfg "$u")")
        rc=$(drun "$w")
        check "G2(d) $u exits 1" "1" "$rc"
        contains "G2(d) … must have a host[:port] authority" "must have a host[:port] authority" "$(cat "$w/stderr")"
        absent "G2(d) … the URL appears nowhere" "$u" "$(cat "$w/container.log")"
    done
    for u in 'https://[::1]:8443/r.git' 'https://git.example.com/a/b%20c.git' 'https://h/a@b.git'; do
        w=$(mkws "$(git_resolve_cfg "$u")")
        rc=$(drun "$w")
        check "G2(e) $u passes the rule (resolve of a commit id, no remote contact)" "0" "$rc"
    done

    section "G3 as and dest are exclusive"
    w=$(mkws "$(jq -cn --arg r "$GIT_ID" '{op: "clone", url: "https://h/r.git", ref: $r, as: "a.tgz", dest: "/orkstack/shared/src"}')" \
        '.paths.shared = "/orkstack/shared"')
    rc=$(drun "$w")
    check "G3 as with dest exits 1" "1" "$rc"

    section "G4 dest: a pinned working tree"
    local clone_dest='{op: "clone", url: "https://h/r.git", ref: $r, dest: $d}'
    w=$(mkws "$(jq -cn --arg r "$GIT_ID" --arg d /orkstack/shared/src "$clone_dest")")
    rc=$(drun "$w")
    check "G4 dest without paths.shared exits 1" "1" "$rc"
    contains "G4 … needs a pinned task" "needs a pinned task" "$(cat "$w/stderr")"
    for v in /orkstack/tmp/x /orkstack/shared /orkstack/shared/a/../b; do
        w=$(mkws "$(jq -cn --arg r "$GIT_ID" --arg d "$v" "$clone_dest")" '.paths.shared = "/orkstack/shared"')
        rc=$(drun "$w")
        check "G4 dest: $v exits 1" "1" "$rc"
    done
    w=$(mkws "$(jq -cn --arg r "$GIT_ID" --arg d /orkstack/shared/src "$clone_dest")" '.paths.shared = "/orkstack/shared"')
    put "$w" shared/src/file 'x'
    rc=$(drun "$w")
    check "G4 a non-empty dest exits 1" "1" "$rc"
    contains "G4 … not empty" "already exists and is not empty" "$(cat "$w/stderr")"
    w=$(mkws "$(jq -cn --arg r "$GIT_ID" --arg d /orkstack/shared/a/b/src "$clone_dest")" '.paths.shared = "/orkstack/shared"')
    rc=$(drun "$w")
    check "G4 parents: the fetch is the first network command, and fails offline" "1" "$rc"
    check "G4 parents: /orkstack/shared/a/b/ holds only the staging directory" ".activity-git-src" \
        "$(find "$w/shared/a/b" -mindepth 1 -maxdepth 1 -printf '%f\n' 2>/dev/null | sort | tr '\n' ' ' | sed 's/ $//')"
    check "G4 parents: nothing half-made sits at dest" "absent" "$([[ -e "$w/shared/a/b/src" ]] && echo present || echo absent)"
    w=$(mkws "$(jq -cn --arg r "$GIT_ID" --arg d /orkstack/shared/l/src "$clone_dest")" '.paths.shared = "/orkstack/shared"')
    ln -s /orkstack/tmp "$w/shared/l"
    rc=$(drun "$w")
    check "G4 a symbolic-link parent exits 1" "1" "$rc"
    contains "G4 … naming it as a symbolic link" "/orkstack/shared/l is a symbolic link" "$(cat "$w/stderr")"
    check "G4 … and /orkstack/tmp stays empty" "" "$(find "$w/tmp" -mindepth 1 -maxdepth 1 -printf '%f ' 2>/dev/null)"
    w=$(mkws "$(jq -cn --arg r "$GIT_ID" --arg d /orkstack/shared/f/src "$clone_dest")" '.paths.shared = "/orkstack/shared"')
    put "$w" shared/f 'x'
    rc=$(drun "$w")
    check "G4 a regular-file parent exits 1" "1" "$rc"
    contains "G4 … naming it as not a directory" "/orkstack/shared/f is not a directory" "$(cat "$w/stderr")"

    section "G5 tokenSecret and tokenUser"
    w=$(mkws "$(jq -cn --arg r "$GIT_ID" '{op: "resolve", url: "https://h/r.git", ref: $r, tokenSecret: "a/b"}')")
    rc=$(drun "$w")
    check "G5 a malformed tokenSecret exits 1" "1" "$rc"
    contains "G5 … naming the expected path" "/orkstack/input/secrets/<name>" "$(cat "$w/stderr")"
    w=$(mkws "$(jq -cn --arg r "$GIT_ID" '{op: "resolve", url: "https://h/r.git", ref: $r, tokenSecret: "nope"}')")
    rc=$(drun "$w")
    check "G5 a tokenSecret that is not mounted exits 1" "1" "$rc"
    contains "G5 … naming the expected path" "/orkstack/input/secrets/nope" "$(cat "$w/stderr")"
    for v in 'a b' 'u@h'; do
        w=$(mkws "$(jq -cn --arg r "$GIT_ID" --arg u "$v" '{op: "resolve", url: "https://h/r.git", ref: $r, tokenUser: $u}')")
        rc=$(drun "$w")
        check "G5 tokenUser: '$v' exits 1" "1" "$rc"
        contains "G5 … naming tokenUser" "with.tokenUser" "$(cat "$w/stderr")"
    done

    section "G6 depth"
    for v in -1 '"x"'; do
        w=$(mkws "{\"op\":\"clone\",\"url\":\"https://h/r.git\",\"ref\":\"$GIT_ID\",\"depth\":$v}")
        rc=$(drun "$w")
        check "G6 depth: $v exits 1" "1" "$rc"
    done

    section "G7 op"
    w=$(mkws '{"url":"https://h/r.git"}')
    rc=$(drun "$w")
    check "G7 missing op exits 2" "2" "$rc"
    w=$(mkws '{"op":"push","url":"https://h/r.git"}')
    rc=$(drun "$w")
    check "G7 unknown op exits 2" "2" "$rc"

    section "G8 a commit id resolves to itself; a 64-hex id is refused"
    w=$(mkws "$(git_resolve_cfg https://h/r.git)")
    rc=$(drun "$w")
    check "G8 exit 0 under --network none (no remote contact)" "0" "$rc"
    check "G8 values.json" "{\"commit\":\"$GIT_ID\",\"ref\":\"$GIT_ID\"}" "$(values "$w")"
    w=$(mkws "$(jq -cn --arg r "$(printf 'a%.0s' $(seq 64))" '{op: "resolve", url: "https://h/r.git", ref: $r}')")
    rc=$(drun "$w")
    check "G8 a 64-hex ref exits 1" "1" "$rc"
    contains "G8 … SHA-256 repositories are not supported" "SHA-256 repositories are not supported" "$(cat "$w/stderr")"

    section "G9 the version line"
    w=$(mkws "$(git_resolve_cfg https://h/r.git)")
    rc=$(drun "$w")
    contains "G9 stdout's first line names git 2.45" "activity-git: git version 2.45." "$(first_line "$w")"

    section "G10 caFile"
    w=$(mkws "$(jq -cn --arg r "$GIT_ID" '{op: "resolve", url: "https://h/r.git", ref: $r, caFile: "missing.pem"}')")
    rc=$(drun "$w")
    check "G10 a missing caFile exits 1" "1" "$rc"

    section "G11 the tarball is reproducible on this image's git"
    w=$(mkws '{}')
    local ident=(-e GIT_AUTHOR_NAME=fixture -e GIT_AUTHOR_EMAIL=fixture@example.invalid
                 -e GIT_COMMITTER_NAME=fixture -e GIT_COMMITTER_EMAIL=fixture@example.invalid
                 -e GIT_AUTHOR_DATE=2026-01-02T03:04:05Z -e GIT_COMMITTER_DATE=2026-01-02T03:04:05Z)
    # (a) setup — the only container that changes a default: the file
    # transport, for its fetches.
    rc=$(drun "$w" -e GIT_ALLOW_PROTOCOL=file "${ident[@]}" --entrypoint sh -- -c '
        set -e
        cd /orkstack/tmp
        git init -q src
        mkdir -p src/d
        printf "hello\n" > src/README
        printf "f\n" > src/d/f
        printf "#!/bin/sh\n" > src/run.sh
        chmod 0755 src/run.sh
        git -C src add -A
        git -C src commit -q -m fixture
        c=$(git -C src rev-parse HEAD)
        git init -q r1
        git init -q r2
        git -C r1 fetch -q --depth 1 file:///orkstack/tmp/src "$c"
        git -C r2 fetch -q --depth 1 file:///orkstack/tmp/src "$c"
        echo "$c"')
    c=$(tail -n 1 "$w/stdout")
    check "G11(a) setup committed and fetched the fixture tree" "0" "$rc"
    # (b) archive — no -e at all: exactly the image's default environment.
    rc=$(drun "$w" --entrypoint sh -- -c '
        set -e
        cd /orkstack/tmp
        git -C r1 archive --format=tar.gz -o /orkstack/tmp/a1 "$1"
        g=$(command -v git)
        PATH=/nonexistent "$g" -C r2 archive --format=tar.gz -o /orkstack/tmp/a2 "$1"
        uname -m' sh "$c")
    ctr=$(tail -n 1 "$w/stdout")
    check "G11(b) both archives succeeded, the second under an empty PATH (git compresses in-process)" "0" "$rc"
    sha1=$(sha256sum "$w/tmp/a1" 2>/dev/null | cut -d' ' -f1)
    sha2=$(sha256sum "$w/tmp/a2" 2>/dev/null | cut -d' ' -f1)
    if [[ -n "$sha1" && "$sha1" == "$sha2" ]]; then
        pass "G11(b) two archives of one commit are byte-identical on $ctr (sha256:$sha1)"
    else
        fail "G11(b) two archives of one commit are byte-identical on ${ctr:-?}"
        info "a1: ${sha1:-(missing)}"
        info "a2: ${sha2:-(missing)}"
    fi
    check "G11(b) the gzip header's MTIME is zero" " 00 00 00 00" "$(od -An -tx1 -j4 -N4 "$w/tmp/a1" 2>/dev/null)"
    out=$(TZ=UTC tar -tvzf "$w/tmp/a1" 2>/dev/null)
    differs "G11(b) the archive lists members" "$(printf '%s\n' "$out" | grep -c .)" "0"
    check "G11(b) every member carries the commit time" "$(printf '%s\n' "$out" | grep -c .)" "$(printf '%s\n' "$out" | grep -c '2026-01-02 03:04')"
    check "G11(b) no member has a ./ prefix" "0" "$(tar -tzf "$w/tmp/a1" 2>/dev/null | grep -c '^\./')"
    # (c) the environment reaches git: tar.umask changes the bytes.
    rc=$(drun "$w" -e GIT_CONFIG_COUNT=1 -e GIT_CONFIG_KEY_0=tar.umask -e GIT_CONFIG_VALUE_0=0077 \
        --entrypoint sh -- -c 'cd /orkstack/tmp && git -C r1 archive --format=tar.gz -o /orkstack/tmp/a3 "$1"' sh "$c")
    sha3=$(sha256sum "$w/tmp/a3" 2>/dev/null | cut -d' ' -f1)
    differs "G11(c) under tar.umask=0077 the digest changes (sha256:${sha3:-?})" "$sha3" "$sha1"
    check "G11(c) … and no member keeps a group or other bit" "0" \
        "$(tar -tvzf "$w/tmp/a3" 2>/dev/null | cut -c5-10 | grep -vc '^------$')"
    # (d) the GIT_ALLOW_PROTOCOL default is in force: a fresh repository must
    # connect, and the file transport is refused.
    rc=$(drun "$w" --entrypoint sh -- -c 'cd /orkstack/tmp && git init -q r3 && git -C r3 fetch --depth 1 file:///orkstack/tmp/src "$1"' sh "$c")
    check "G11(d) a file:// fetch exits 128 under the image default" "128" "$rc"
    contains "G11(d) … transport 'file' not allowed" "transport 'file' not allowed" "$(cat "$w/stderr")"

    section "G12 key types"
    for v in '"ref":null|ref' '"depth":null|depth' '"tokenSecret":false|tokenSecret' '"as":false|as' '"caFile":null|caFile'; do
        w=$(mkws "{\"op\":\"clone\",\"url\":\"https://h/r.git\",${v%%|*}}")
        rc=$(drun "$w")
        check "G12 ${v%%|*} exits 1" "1" "$rc"
        contains "G12 … naming ${v#*|}" "with.${v#*|} must be" "$(cat "$w/stderr")"
    done
    w=$(mkws '{"op":null,"url":"https://h/r.git"}')
    rc=$(drun "$w")
    check "G12 op: null exits 2" "2" "$rc"

    section "G13 the askpass helper alone"
    local askenv=(-e ACTIVITY_GIT_AUTHORITY=h.example:8443 -e ACTIVITY_GIT_USER=x-access-token
                  -e ACTIVITY_GIT_TOKEN_FILE=/orkstack/input/secrets/t
                  --entrypoint /usr/local/libexec/activity-git/askpass)
    w=$(mkws '{}')
    put "$w" input/secrets/t $'TOKSECRET\n'
    rc=$(drun "$w" "${askenv[@]}" -- "Username for 'https://h.example:8443': ")
    check "G13 the URL's own username prompt is answered" "0:x-access-token" "$rc:$(cat "$w/stdout")"
    rc=$(drun "$w" "${askenv[@]}" -- "Password for 'https://x-access-token@h.example:8443': ")
    check "G13 the URL's own password prompt is answered" "0:TOKSECRET" "$rc:$(cat "$w/stdout")"
    for v in "Username for 'https://other.example:8443': " "Password for 'https://x-access-token@h.example:9443': " \
             "Password for 'https://x-access-token@h.example': " "Username for 'http://h.example:8443': "; do
        rc=$(drun "$w" "${askenv[@]}" -- "$v")
        check "G13 [$v] gets no answer (exit 1, empty stdout)" "1:" "$rc:$(cat "$w/stdout")"
        absent "G13 … TOKSECRET appears nowhere" "TOKSECRET" "$(cat "$w/container.log")"
    done

    section "G14 the GIT_TRACE_REDACT refusal is git's own boolean rule"
    local tok='.secrets = [{name: "t", path: "/orkstack/input/secrets/t"}]'
    local cfg_tok
    cfg_tok=$(jq -cn --arg r "$GIT_ID" '{op: "resolve", url: "https://h/r.git", ref: $r, tokenSecret: "t"}')
    for v in 0 00 false no OFF ''; do
        w=$(mkws "$cfg_tok" "$tok")
        put "$w" input/secrets/t $'TOKSECRET\n'
        rc=$(drun "$w" -e "GIT_TRACE_REDACT=$v")
        check "G14 GIT_TRACE_REDACT='$v' (git reads it as false) exits 1" "1" "$rc"
        contains "G14 … naming the variable and its value" "GIT_TRACE_REDACT='$v'" "$(cat "$w/stderr")"
        check "G14 … values.json absent" "absent" "$([[ -e "$w/output/values.json" ]] && echo present || echo absent)"
        absent "G14 … the token appears nowhere" "TOKSECRET" "$(cat "$w/container.log")"
    done
    w=$(mkws "$cfg_tok" "$tok")
    put "$w" input/secrets/t $'TOKSECRET\n'
    rc=$(drun "$w" -e GIT_TRACE_REDACT=maybe)
    check "G14 GIT_TRACE_REDACT=maybe (git cannot read it) exits 1" "1" "$rc"
    contains "G14 … git's own line" "bad boolean config value" "$(cat "$w/stderr")"
    contains "G14 … and the image's line" "GIT_TRACE_REDACT='maybe'" "$(cat "$w/stderr")"
    for v in true 1 ON 2 -1; do
        w=$(mkws "$cfg_tok" "$tok")
        put "$w" input/secrets/t $'TOKSECRET\n'
        rc=$(drun "$w" -e "GIT_TRACE_REDACT=$v")
        check "G14 GIT_TRACE_REDACT=$v (git reads it as true) exits 0" "0" "$rc"
        absent "G14 … the token appears nowhere" "TOKSECRET" "$(cat "$w/container.log")"
    done
    w=$(mkws "$cfg_tok" "$tok")
    put "$w" input/secrets/t $'TOKSECRET\n'
    rc=$(drun "$w" -e GIT_TRACE_REDACT=1 -e GIT_TRACE2=/dev/stdout)
    check "G14 trace lines on stdout do not confuse the check" "0" "$rc"
    w=$(mkws "$(git_resolve_cfg https://h/r.git)")
    rc=$(drun "$w" -e GIT_TRACE_REDACT=0)
    check "G14 GIT_TRACE_REDACT=0 without tokenSecret: no token, nothing refused" "0" "$rc"
    w=$(mkws "$cfg_tok" "$tok")
    put "$w" input/secrets/t $'TOKSECRET\n'
    rc=$(drun "$w" -e GIT_TRACE_CURL=1 -e GIT_CURL_VERBOSE=1)
    check "G14 tracing itself is honoured" "0" "$rc"
    absent "G14 … the token appears nowhere" "TOKSECRET" "$(cat "$w/container.log")"
}

"cases_$NAME"

# ---------------------------------------------------------------- the verdict --

printf '\n'
bold "── Result"
info "image:   $IMAGE"
info "passed:  $PASSED"
info "failed:  $FAILED"
info "skipped: $SKIPPED"
printf '\n'

if (( FAILED > 0 )); then
    printf '\033[31mFAIL\033[0m %s does not satisfy the activity-%s contract\n' "$IMAGE" "$NAME"
    exit 1
fi
printf '\033[32mPASS\033[0m %s satisfies the activity-%s contract\n' "$IMAGE" "$NAME"
exit 0
