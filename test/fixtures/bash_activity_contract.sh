#!/usr/bin/env bash
#
# bash_activity_contract.sh — the mandatory Bash fixture of the OCI Activity
# Contract test suite (docs/OCI_ACTIVITY_CONTRACT.md §10.3, AGENTS.md §10).
#
# It runs an image against the ABI with no Orkflow, no Temporal, no OrkStack
# binary and no SDK in any language. Its dependencies are docker, jq, cat and a
# redirect. AGENTS.md §10: "If Bash needs an SDK, the architecture has failed."
# This file is that sentence, executable.
#
#   usage:  bash test/fixtures/bash_activity_contract.sh [image]
#   exit:   0 all assertions passed
#           1 an assertion failed
#          77 a prerequisite is absent (AGENTS.md §11.1 skip convention);
#             ORK_REQUIRE_INFRA=1 turns that into a failure instead.
#
# The six assertions are §10.3's, and each exists because a probe found a real
# failure it would have caught:
#
#   1  container startup and SIGTERM handling, ON EVERY OP        §14.14, §14.18
#   2  /orkstack/input is mounted read-only                       §14.3
#   3  a valid /orkstack context: task.json complete per §3.2     §3.2
#   4  values.json is written, is an object, is within the limit  §3.4
#   5  artifact names: valid UTF-8, <= 128 bytes, regular files   §3.5, §14.16
#   6  exit codes pass through, and a timeout kills a hung image  §14.13, §14.5
#
# Assertion 1 runs against every op the image declares rather than one
# representative shape: the previous draft of §10.2's entry script passed on
# `exec` and failed on `eval` at exit 143 with no values.json and empty logs.
# A per-shape test would not have caught that; a per-op test does (§14.18).

set -uo pipefail

IMAGE=${1:-${ORK_ACTIVITY_TEST_IMAGE:-localhost:5001/orkstack/activity-shell:dev}}

# Ops this fixture knows how to drive for activity-shell. An image declaring a
# different op set supplies it here.
OPS=${ORK_ACTIVITY_TEST_OPS:-"exec eval"}

GRACE=${ORK_ACTIVITY_TEST_GRACE:-5}     # cancellation grace period, seconds
DEADLINE=${ORK_ACTIVITY_TEST_DEADLINE:-30}   # bound on every wait, seconds

VALUES_MAX_BYTES=262144
ARTIFACT_MAX_COUNT=256
ARTIFACT_MAX_BYTES=5368709120
ARTIFACT_NAME_MAX_BYTES=128

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
docker image inspect "$IMAGE" >/dev/null 2>&1 ||
    skip_run "image not present locally: $IMAGE (build it with: make -C images images)"

# ------------------------------------------------------------- the workspace --
#
# The workspace root MUST be on a path the container daemon can bind-mount, and
# that is not a given: on macOS with a VM-hosted daemon, $HOME is shared and
# /var/folders — where mktemp puts things — is not (§3.1). A run rooted there
# fails every single task with "bind source path does not exist", which looks
# like an image defect and is not one.
#
# So the fixture does what §3.1 requires of the Orker itself: probe the root
# with a real bind mount at startup and report WORKDIR_NOT_BINDABLE up front,
# rather than failing every task later. Candidates are tried in order.

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
        ROOT=$(mktemp -d "$candidate/ork-contract.XXXXXX")
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
    chmod -R u+rwX "$ROOT" 2>/dev/null
    rm -rf "$ROOT"
}
trap cleanup EXIT

# mkws builds one per-attempt workspace exactly as §3.1 specifies and echoes
# its path. Modes are set explicitly rather than inherited: §14.3's first probe
# failed because it trusted mount defaults.
#
#   mkws <config-json> [task-id]
mkws() {
    local config=$1 task=${2:-t1} w
    w=$(mktemp -d "$ROOT/ws.XXXXXX")

    mkdir -p "$w/input/secrets" "$w/input/artifacts" "$w/input/upstream"
    mkdir -p "$w/output/artifacts" "$w/tmp"

    cat > "$w/input/task.json" <<JSON
{
  "schemaVersion": 1,
  "idempotencyKey": "default/ork-contract-fixture-1/$task",
  "run": {
    "id": "ork-contract-fixture-1",
    "namespace": "default",
    "orkflow": {
      "name": "contract-fixture",
      "version": "0.0.0",
      "definitionDigest": "sha256:0000000000000000000000000000000000000000000000000000000000000000"
    }
  },
  "task": { "id": "$task", "attempt": 1, "pool": "main" },
  "image": {
    "reference": "$IMAGE",
    "pinned": "$IMAGE",
    "indexDigest": "sha256:0000000000000000000000000000000000000000000000000000000000000000"
  },
  "limits": {
    "timeoutSeconds": $DEADLINE,
    "cancellationGracePeriodSeconds": $GRACE,
    "valuesMaxBytes": $VALUES_MAX_BYTES,
    "artifactMaxCount": $ARTIFACT_MAX_COUNT,
    "artifactMaxBytes": $ARTIFACT_MAX_BYTES,
    "logMaxBytes": 16777216,
    "commitMaxBytes": 4194304
  },
  "network": "none",
  "paths": {
    "input": "/orkstack/input",
    "config": "/orkstack/input/config.json",
    "runInput": "/orkstack/input/run.json",
    "secrets": "/orkstack/input/secrets",
    "output": "/orkstack/output",
    "values": "/orkstack/output/values.json",
    "artifacts": "/orkstack/output/artifacts",
    "tmp": "/orkstack/tmp"
  },
  "inputs": { "artifacts": [], "upstream": [] },
  "secrets": []
}
JSON

    printf '%s\n' "$config" > "$w/input/config.json"
    printf '{}\n' > "$w/input/run.json"

    chmod 0444 "$w/input/task.json" "$w/input/config.json" "$w/input/run.json"
    chmod 0555 "$w/input" "$w/input/secrets" "$w/input/artifacts" "$w/input/upstream"
    chmod 1777 "$w/output" "$w/output/artifacts" "$w/tmp"

    printf '%s' "$w"
}

# docker_args emits the §9.2 profile for a workspace. Every flag is explicit;
# none is implicit.
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
        --label "ork.run=ork-contract-fixture-1" \
        --tmpfs "/tmp:rw,nosuid,nodev,size=16m" \
        --mount "type=bind,source=$w/input,destination=/orkstack/input,readonly" \
        --mount "type=bind,source=$w/output,destination=/orkstack/output" \
        --mount "type=bind,source=$w/tmp,destination=/orkstack/tmp"
}

# run_sync runs the image to completion under a deadline and echoes its exit
# status. Stdout+stderr land in $w/container.log.
run_sync() {
    local w=$1 image=${2:-$IMAGE} args=() rc
    mapfile -d '' -t args < <(docker_args "$w")
    docker run --rm "${args[@]}" "$image" > "$w/container.log" 2>&1
    rc=$?
    printf '%s' "$rc"
}

# run_detached starts the image and echoes the container id.
run_detached() {
    local w=$1 image=${2:-$IMAGE} args=() cid
    mapfile -d '' -t args < <(docker_args "$w")
    cid=$(docker run -d "${args[@]}" "$image" 2>"$w/run.err")
    CONTAINERS+=("$cid")
    printf '%s' "$cid"
}

# await_ready blocks until the workload has signalled readiness, or the
# deadline passes. This is a bounded wait on an observable condition, never a
# fixed sleep: the fixture does not guess how long a container takes to start.
#
# Readiness is a FILE in the workspace, not a line in `docker logs`, and that
# is forced by the contract rather than a preference. Under `op=eval` the entry
# script redirects the workload's stdout to a file and cats it only after the
# workload exits (§10.2) — correctly, since that is what keeps the durable
# stdout.log non-empty. So a workload that loops forever prints nothing to the
# log stream until it is stopped, and a log-watching fixture would deadlock
# against its own SIGTERM. The workspace bind mount is visible to the host
# immediately, on every op.
await_ready() {
    local w=$1 cid=$2 deadline=$((SECONDS + DEADLINE))
    while (( SECONDS < deadline )); do
        [[ -e "$w/tmp/ready" ]] && return 0
        if [[ "$(docker inspect -f '{{.State.Running}}' "$cid" 2>/dev/null)" == "false" ]]; then
            return 1   # exited before ever becoming ready
        fi
    done
    return 1
}

# --------------------------------------------------- the contract validators --
#
# These are the Orker's own §3.4/§3.5 rules, restated in ten lines of shell.
# That they fit is the point: a workload author can read them, and a broken
# platform-side implementation is falsified by the same file that documents it.
# Each echoes a machine-readable reason code, or OK.

validate_values() {
    local w=$1
    local f="$w/output/values.json" size
    [[ -e "$f" ]]                || { echo "VALUES_MISSING"; return; }
    [[ -r "$f" ]]                || { echo "OUTPUT_NOT_READABLE"; return; }
    size=$(wc -c < "$f" | tr -d ' ')
    (( size <= VALUES_MAX_BYTES )) || { echo "VALUES_TOO_LARGE"; return; }
    jq -e . "$f" >/dev/null 2>&1 || { echo "VALUES_MALFORMED"; return; }
    [[ "$(jq -r 'type' "$f")" == "object" ]] || { echo "VALUES_MALFORMED"; return; }
    echo "OK"
}

validate_artifacts() {
    local w=$1
    local d="$w/output/artifacts" count=0 name size
    [[ -d "$d" ]] || { echo "OK"; return; }
    while IFS= read -r -d '' entry; do
        name=${entry##*/}
        count=$((count + 1))

        # Regular files only. The scan is not recursive (§3.5).
        if [[ -L "$entry" || ! -f "$entry" ]]; then
            echo "ARTIFACT_NOT_A_FILE:$name"; return
        fi
        [[ -r "$entry" ]] || { echo "OUTPUT_NOT_READABLE:$name"; return; }

        # Names must be valid UTF-8: ArtifactRef.name is a proto string, and
        # both protojson and binary proto refuse invalid UTF-8 (§14.16). The
        # check is here, at the scan, so it costs no bandwidth.
        if ! printf '%s' "$name" | iconv -f UTF-8 -t UTF-8 >/dev/null 2>&1; then
            echo "ARTIFACT_NAME_NOT_UTF8:$(printf '%s' "$name" | od -An -tx1 | tr -d ' \n')"
            return
        fi
        (( $(printf '%s' "$name" | wc -c) <= ARTIFACT_NAME_MAX_BYTES )) ||
            { echo "ARTIFACT_NAME_TOO_LONG:$name"; return; }
        # A name is one path component. '.', '..' and any '/' are refused
        # rather than normalized, so a name can never address another
        # directory.
        case "$name" in
            .|..|*/*) echo "ARTIFACT_NAME_INVALID:$name"; return ;;
        esac

        size=$(wc -c < "$entry" | tr -d ' ')
        (( size <= ARTIFACT_MAX_BYTES )) || { echo "ARTIFACT_LIMIT_EXCEEDED:$name"; return; }
    done < <(find "$d" -mindepth 1 -maxdepth 1 -print0 | sort -z)
    (( count <= ARTIFACT_MAX_COUNT )) || { echo "ARTIFACT_LIMIT_EXCEEDED:count=$count"; return; }
    echo "OK"
}

# ---------------------------------------------------------------- the image --

# A workload that traps TERM, announces both events, and exits 0. §14.14 used
# this exact shape, and the 0.2s poll is the WORKLOAD's own responsiveness to a
# signal, not the fixture waiting on a guess.
#
# It touches /orkstack/tmp/ready so the fixture has an observable that does not
# depend on the log stream — see await_ready.
TRAPPING_WORKLOAD='trap "echo WORKLOAD-GOT-SIGTERM; exit 0" TERM; echo WORKLOAD-UP; : > /orkstack/tmp/ready; while :; do sleep 0.2; done'
DEAF_WORKLOAD='trap "" TERM; echo WORKLOAD-UP-DEAF; : > /orkstack/tmp/ready; while :; do sleep 0.2; done'

# cfg builds a config.json for an op and an argv array.
#
# The argv elements travel through stdin, NUL-separated, rather than through
# jq's --args: after --args, jq still parses a leading-dash token as one of its
# own options, so `cfg exec sh -c "..."` silently lost the -c and ran
# `sh <script>`, which sh then tried to open as a file. Anything that can be
# mistaken for a flag has to arrive as data.
cfg() {
    local op=$1; shift
    local argv='[]'
    if (( $# > 0 )); then
        argv=$(printf '%s\0' "$@" | jq -Rs 'split("\u0000")[:-1]')
    fi
    jq -cn --arg op "$op" --argjson command "$argv" '{op:$op, command:$command}'
}

bold "OCI Activity Contract — Bash fixture"
info "image:     $IMAGE"
info "ops:       $OPS"
info "grace:     ${GRACE}s"
info "workspace: $ROOT"

# ============================================================================
section "1. Container startup and SIGTERM handling, on every op"
# ============================================================================
#
# Both halves must hold, per op: the workload OBSERVES SIGTERM, and
# values.json EXISTS afterwards. §14.14 shape (a) fails both; shape (c) fails
# the second. Only background+trap+wait passes, which is why §10.2's script is
# written the way it is.

for op in $OPS; do
    w=$(mkws "$(cfg "$op" sh -c "$TRAPPING_WORKLOAD")" "sigterm-$op")
    cid=$(run_detached "$w")

    if [[ -z "$cid" ]]; then
        fail "op=$op: container started"
        info "$(cat "$w/run.err" 2>/dev/null)"
        continue
    fi
    if ! await_ready "$w" "$cid"; then
        fail "op=$op: workload started within ${DEADLINE}s"
        info "logs: $(docker logs "$cid" 2>&1 | tr '\n' '|')"
        docker rm -f "$cid" >/dev/null 2>&1
        continue
    fi
    pass "op=$op: container started and the workload began running"

    started=$SECONDS
    docker stop --timeout "$GRACE" "$cid" >/dev/null 2>&1
    elapsed=$((SECONDS - started))
    status=$(docker inspect -f '{{.State.ExitCode}}' "$cid" 2>/dev/null)
    logs=$(docker logs "$cid" 2>&1)
    docker rm -f "$cid" >/dev/null 2>&1

    contains "op=$op: the WORKLOAD observed SIGTERM" "WORKLOAD-GOT-SIGTERM" "$logs"
    contains "op=$op: workload stdout reached the log stream" "WORKLOAD-UP" "$logs"
    check    "op=$op: exited cleanly on the signal, not 143 (128+SIGTERM)" "0" "$status"
    if (( elapsed < GRACE )); then
        pass "op=$op: honored SIGTERM inside the ${GRACE}s grace period (${elapsed}s)"
    else
        fail "op=$op: took ${elapsed}s, i.e. it was SIGKILLed at the grace boundary"
    fi
    check "op=$op: values.json exists after cancellation" "OK" "$(validate_values "$w")"
done

# ============================================================================
section "2. /orkstack/input is mounted read-only"
# ============================================================================

# `touch`, not `: > file`: with `: > f 2>/dev/null` the shell opens f BEFORE it
# applies 2>/dev/null, so the "Read-only file system" diagnostic escapes to the
# real stderr and the else-branch marker is never the only evidence. touch is
# an ordinary command, so its stderr is redirected normally.
probe='if touch /orkstack/input/nope 2>/dev/null; then echo INPUT-WRITABLE; else echo INPUT-READONLY; fi'
w=$(mkws "$(cfg exec sh -c "$probe")" "readonly")
rc=$(run_sync "$w")
logs=$(cat "$w/container.log")
contains "the container was refused a write to /orkstack/input" "INPUT-READONLY" "$logs"
if [[ -e "$w/input/nope" ]]; then
    fail "no file leaked into the host input directory"
else
    pass "no file leaked into the host input directory"
fi

rootfs='if touch /rootfs-probe 2>/dev/null; then echo ROOTFS-WRITABLE; else echo ROOTFS-READONLY; fi'
w=$(mkws "$(cfg exec sh -c "$rootfs")" "rootfs")
rc=$(run_sync "$w")
contains "the container root filesystem is read-only" "ROOTFS-READONLY" "$(cat "$w/container.log")"

w=$(mkws "$(cfg exec sh -c 'if touch /orkstack/output/w && touch /orkstack/tmp/w; then echo RW-OK; fi')" "rw")
rc=$(run_sync "$w")
contains "output/ and tmp/ are writable by the image's own UID" "RW-OK" "$(cat "$w/container.log")"

# ============================================================================
section "3. A valid /orkstack context: task.json complete against §3.2"
# ============================================================================
#
# The container copies task.json out; the host validates it. Doing it this way
# proves the CONTAINER can read it, and still reports which field is missing.

w=$(mkws "$(cfg exec sh -c 'cp /orkstack/input/task.json /orkstack/output/artifacts/seen-task.json')" "ctx")
rc=$(run_sync "$w")
seen="$w/output/artifacts/seen-task.json"

if [[ ! -r "$seen" ]]; then
    fail "the container could read /orkstack/input/task.json"
    info "exit=$rc logs: $(cat "$w/container.log")"
else
    pass "the container could read /orkstack/input/task.json"

    check "task.json is valid JSON" "0" "$(jq -e . "$seen" >/dev/null 2>&1; echo $?)"
    check "schemaVersion is 1" "1" "$(jq -r '.schemaVersion' "$seen")"

    # Every field §3.2 marks required, by its dotted path.
    #
    # `. as $doc` first: inside map/select the input is the PATH STRING, so a
    # bare getpath indexes the string and jq aborts with "Cannot index string".
    # That failure printed to stderr and left stdout empty, which compared
    # equal to "no missing fields" — a green assertion that checked nothing.
    missing=$(jq -r '
      . as $doc
      | [ "schemaVersion","idempotencyKey",
          "run.id","run.namespace","run.orkflow.name","run.orkflow.version",
          "run.orkflow.definitionDigest",
          "task.id","task.attempt","task.pool",
          "image.reference","image.pinned","image.indexDigest",
          "limits.timeoutSeconds","limits.cancellationGracePeriodSeconds",
          "limits.valuesMaxBytes","limits.artifactMaxCount",
          "limits.artifactMaxBytes","limits.logMaxBytes","limits.commitMaxBytes",
          "network",
          "paths.input","paths.config","paths.runInput","paths.secrets",
          "paths.output","paths.values","paths.artifacts","paths.tmp",
          "inputs.artifacts","inputs.upstream","secrets" ]
      | map(. as $p | select(($doc | getpath($p | split("."))) == null))
      | join(",")' "$seen" 2>&1)
    check "every §3.2 required field is present" "" "$missing"

    # Falsify the check itself: a document with a field removed must be caught.
    jq 'del(.limits.commitMaxBytes)' "$seen" > "$w/tmp/holed.json"
    holed=$(jq -r '
      . as $doc
      | ["limits.commitMaxBytes","paths.tmp"]
      | map(. as $p | select(($doc | getpath($p | split("."))) == null))
      | join(",")' "$w/tmp/holed.json" 2>&1)
    check "the required-field check actually detects a missing field" \
        "limits.commitMaxBytes" "$holed"

    check "inputs.artifacts is [] and never null" "array" "$(jq -r '.inputs.artifacts | type' "$seen")"
    check "inputs.upstream  is [] and never null" "array" "$(jq -r '.inputs.upstream | type' "$seen")"
    check "secrets          is [] and never null" "array" "$(jq -r '.secrets | type' "$seen")"
    check "all eight paths.* keys are present" "8" "$(jq -r '.paths | length' "$seen")"
    check "network is default or none" "true" \
        "$(jq -r '.network == "default" or .network == "none"' "$seen")"
    check "idempotencyKey is {namespace}/{runId}/{taskId}" \
        "default/ork-contract-fixture-1/ctx" "$(jq -r '.idempotencyKey' "$seen")"
    check "no secret VALUES are present, only names and paths" "true" \
        "$(jq -r '[.secrets[] | has("value")] | any | not' "$seen")"
fi

# ============================================================================
section "4. values.json is written, is an object, and is within the limit"
# ============================================================================

w=$(mkws "$(cfg exec sh -c 'echo HELLO-ON-STDOUT')" "values")
rc=$(run_sync "$w")
check "a successful run writes values.json" "OK" "$(validate_values "$w")"
check "values.json is a JSON object at the top level" "object" \
    "$(jq -r 'type' "$w/output/values.json" 2>/dev/null)"
check "the exec op reports the child's exit code" "0" \
    "$(jq -r '.exitCode' "$w/output/values.json" 2>/dev/null)"
contains "workload stdout reached the container log stream" "HELLO-ON-STDOUT" "$(cat "$w/container.log")"

# The negative. An image that exits 0 writing nothing must be caught, not
# silently accepted — otherwise every later assertion is vacuous. Run a bare
# base image so this falsifies the CHECKER, not the image under test.
w=$(mkws '{}' "novalues")
rc=$(run_sync "$w" "alpine:3.20")
check "exit 0 with no values.json is caught as VALUES_MISSING" \
    "VALUES_MISSING" "$(validate_values "$w")"

w=$(mkws '{}' "badvalues")
rc=$(run_sync "$w" "alpine:3.20")
printf '[1,2,3]' > "$w/output/values.json"
check "a top-level array is caught as VALUES_MALFORMED" \
    "VALUES_MALFORMED" "$(validate_values "$w")"
printf 'not json at all' > "$w/output/values.json"
check "unparseable bytes are caught as VALUES_MALFORMED" \
    "VALUES_MALFORMED" "$(validate_values "$w")"
head -c $((VALUES_MAX_BYTES + 1)) /dev/zero | tr '\0' 'x' > "$w/output/values.json"
check "over ${VALUES_MAX_BYTES}B is caught as VALUES_TOO_LARGE" \
    "VALUES_TOO_LARGE" "$(validate_values "$w")"

# ============================================================================
section "5. Artifact names: valid UTF-8, <= 128 bytes, regular files only"
# ============================================================================

w=$(mkws "$(cfg exec sh -c 'printf REPORT > /orkstack/output/artifacts/report.bin; printf B > /orkstack/output/artifacts/a.txt')" "names")
rc=$(run_sync "$w")
check "an ordinary artifact name is accepted" "OK" "$(validate_artifacts "$w")"
check "the artifact name round-trips exactly" "report.bin" \
    "$(basename "$(find "$w/output/artifacts" -name 'report.bin')")"
check "artifacts are ordered by name bytes ascending" "a.txt report.bin" \
    "$(find "$w/output/artifacts" -mindepth 1 -maxdepth 1 -exec basename {} \; | sort | tr '\n' ' ' | sed 's/ $//')"

# A directory in output/artifacts/ — the scan is not recursive (§3.5).
w=$(mkws "$(cfg exec sh -c 'mkdir -p /orkstack/output/artifacts/adir; printf x > /orkstack/output/artifacts/adir/inner')" "dir")
rc=$(run_sync "$w")
check "a directory is rejected as ARTIFACT_NOT_A_FILE" \
    "ARTIFACT_NOT_A_FILE:adir" "$(validate_artifacts "$w")"

# A symlink, likewise.
w=$(mkws "$(cfg exec sh -c 'ln -s /etc/passwd /orkstack/output/artifacts/link')" "link")
rc=$(run_sync "$w")
check "a symlink is rejected as ARTIFACT_NOT_A_FILE" \
    "ARTIFACT_NOT_A_FILE:link" "$(validate_artifacts "$w")"

# Over-long name.
LONGNAME=$(printf 'n%.0s' $(seq 1 200))
w=$(mkws "$(cfg exec sh -c "printf x > /orkstack/output/artifacts/$LONGNAME")" "long")
rc=$(run_sync "$w")
check "a ${#LONGNAME}-byte name is rejected as ARTIFACT_NAME_TOO_LONG" \
    "ARTIFACT_NAME_TOO_LONG:$LONGNAME" "$(validate_artifacts "$w")"

# Invalid UTF-8. Not every host filesystem can even hold such a name — macOS
# APFS rejects it outright — so a failure to CREATE it is a skip, not a
# failure. Where it can be created, it must be rejected at the scan (§14.16).
# shellcheck disable=SC2016  # the $( ) must be evaluated INSIDE the container
w=$(mkws "$(cfg exec sh -c 'printf x > "/orkstack/output/artifacts/$(printf "report\377.bin")" || echo CANNOT-CREATE')" "utf8")
rc=$(run_sync "$w")
if [[ "$(cat "$w/container.log")" == *CANNOT-CREATE* ]] ||
   [[ -z "$(find "$w/output/artifacts" -mindepth 1 -maxdepth 1 2>/dev/null)" ]]; then
    skip "an invalid-UTF-8 name could not be created on this filesystem"
    info "host: $(uname -s); §14.16's check is unreachable here"
else
    result=$(validate_artifacts "$w")
    contains "an invalid-UTF-8 name is rejected as ARTIFACT_NAME_NOT_UTF8" \
        "ARTIFACT_NAME_NOT_UTF8" "$result"
fi

# ============================================================================
section "6. Exit codes pass through, and a timeout kills a hung container"
# ============================================================================
#
# §14.13: a started container's status passes through verbatim, and a workload
# may choose 125/126/127 for its own diagnostics. Nothing interprets them.

for code in 0 1 42 125 126 127; do
    w=$(mkws "$(cfg exec sh -c "exit $code")" "exit-$code")
    rc=$(run_sync "$w")
    check "op=exec: workload exit $code is reported verbatim" "$code" "$rc"
    check "op=exec: values.json is written even on exit $code" "OK" "$(validate_values "$w")"
done

# The eval op's documented contract: a non-zero child is DATA, so the activity
# succeeds and the code appears in values.json. This is one image's rule, not a
# platform rule, and asserting it here keeps it honest.
w=$(mkws "$(cfg eval sh -c 'echo MEASURED; exit 7')" "eval-nonzero")
rc=$(run_sync "$w")
check "op=eval: a non-zero child is data, so the activity exits 0" "0" "$rc"
check "op=eval: the child's status is reported in values.exitCode" "7" \
    "$(jq -r '.exitCode' "$w/output/values.json" 2>/dev/null)"
check "op=eval: the child's stdout is captured as values.value" "MEASURED" \
    "$(jq -r '.value' "$w/output/values.json" 2>/dev/null)"
contains "op=eval: stdout ALSO reached the log stream, not just values.json" \
    "MEASURED" "$(cat "$w/container.log")"

# An unknown op is refused, not guessed at.
w=$(mkws '{"op":"nosuchop"}' "unknown-op")
rc=$(run_sync "$w")
check "an unknown op is refused with exit 2" "2" "$rc"

# The argv-array injection check (§10.2 rule 8, measured §14.18). Metacharacters
# in a command element must be data.
w=$(mkws "$(cfg exec echo 'hello; touch /orkstack/output/artifacts/PWNED')" "injection")
rc=$(run_sync "$w")
contains "shell metacharacters in argv are printed as data" \
    "hello; touch /orkstack/output/artifacts/PWNED" "$(cat "$w/container.log")"
if [[ -e "$w/output/artifacts/PWNED" ]]; then
    fail "the injected payload created no file"
else
    pass "the injected payload created no file (argv array is a real mitigation)"
fi

# Timeout: an image that ignores SIGTERM is SIGKILLed at the grace boundary and
# reports 137 (§14.5). The fixture must observe that rather than hang.
w=$(mkws "$(cfg exec sh -c "$DEAF_WORKLOAD")" "timeout")
cid=$(run_detached "$w")
if ! await_ready "$w" "$cid"; then
    fail "the SIGTERM-ignoring workload started"
else
    started=$SECONDS
    docker stop --timeout "$GRACE" "$cid" >/dev/null 2>&1
    elapsed=$((SECONDS - started))
    status=$(docker inspect -f '{{.State.ExitCode}}' "$cid" 2>/dev/null)
    docker rm -f "$cid" >/dev/null 2>&1

    check "a SIGTERM-ignoring container is SIGKILLed and reports 137" "137" "$status"
    if (( elapsed >= GRACE )); then
        pass "the full ${GRACE}s grace period was honored before SIGKILL (${elapsed}s)"
    else
        fail "SIGKILL arrived after ${elapsed}s, before the ${GRACE}s grace boundary"
    fi
    check "a killed container leaves no values.json" "VALUES_MISSING" "$(validate_values "$w")"
fi

# ---------------------------------------------------------------- the verdict --

printf '\n'
bold "── Result"
info "image:   $IMAGE"
info "passed:  $PASSED"
info "failed:  $FAILED"
info "skipped: $SKIPPED"
printf '\n'

if (( FAILED > 0 )); then
    printf '\033[31mFAIL\033[0m %s does not satisfy the OCI Activity Contract\n' "$IMAGE"
    exit 1
fi
printf '\033[32mPASS\033[0m %s satisfies the OCI Activity Contract\n' "$IMAGE"
exit 0
