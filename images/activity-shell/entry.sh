#!/bin/sh
# images/activity-shell/entry.sh — the whole SDK is jq and a redirect.
#
# This script is docs/OCI_ACTIVITY_CONTRACT.md §10.2, transcribed. It is not
# illustrative: §14.18 built this exact text into an image and exercised it on
# every op, cancelled and successful. The draft before it failed two of those
# cases. Do not "improve" it without re-running §14.18's harness.
set -u
umask 022        # rule 7: outputs must be readable by the Orker (§3.1)

CONFIG=/orkstack/input/config.json
VALUES=/orkstack/output/values.json
TMP=/orkstack/tmp

op=$(jq -r '.op // "exec"' "$CONFIG")

# `command` is a JSON ARRAY — argv, not a shell string (rule 8). jq's @sh
# quotes each element, so `eval set --` cannot be escaped by any value in it;
# the substitution runs before any workload starts, so no trap is at risk here.
eval "set -- $(jq -r '(.command // []) | @sh' "$CONFIG")"

# run_supervised runs its ARGUMENTS as the workload, in the background, with a
# TERM trap that forwards to the child, and returns the child's real status.
#
# MUST NOT be called inside $( ) or on either side of a pipeline (rule 2b):
# both create a subshell, which resets the trap and loses cancellation.
run_supervised() {
    "$@" &
    child=$!
    trap 'kill -TERM "$child" 2>/dev/null' TERM INT
    wait "$child"
    rc=$?
    # POSIX: a trapped signal makes `wait` return >128 IMMEDIATELY, before the
    # child has actually finished. Wait again on that path only — a second
    # unconditional wait would discard the status when no signal arrived.
    if [ "$rc" -gt 128 ]; then
        wait "$child"
        rc=$?
    fi
    trap - TERM INT
    return "$rc"
}

case "$op" in
  exec)
    # The child's exit status IS the activity's exit status. No magic codes.
    run_supervised "$@"
    rc=$?
    printf '{"exitCode":%d}\n' "$rc" > "$VALUES"
    exit "$rc"
    ;;
  script)
    # `.script` is the program text; `.interpreter` overrides the shebang.
    jq -r '.script' "$CONFIG" > "$TMP/script"
    chmod 0755 "$TMP/script"
    interp=$(jq -r '.interpreter // empty' "$CONFIG")
    if [ -n "$interp" ]; then
        run_supervised "$interp" "$TMP/script" "$@"
    elif head -c 2 "$TMP/script" | grep -q '#!'; then
        run_supervised "$TMP/script" "$@"
    else
        run_supervised sh "$TMP/script" "$@"
    fi
    rc=$?
    jq -n --arg p "$TMP/script" --argjson rc "$rc" \
       '{scriptPath:$p, exitCode:$rc}' > "$VALUES"
    exit "$rc"
    ;;
  eval)
    # `eval` is documented to treat a non-zero exit as DATA: the evaluation
    # ran, so the activity succeeded. This is one image's contract, not a
    # platform rule, and it is the only way to express it under exit-code
    # semantics without the platform inventing a magic code.
    #
    # REDIRECTION, not $( ): a substitution would put run_supervised in a
    # subshell and lose the trap (rule 2b), and it would also swallow the
    # workload's stdout so §3.6's durable stdout.log came out empty.
    run_supervised "$@" > "$TMP/stdout"
    rc=$?
    cat "$TMP/stdout"          # stdout still reaches the container log stream
    value=$(cat "$TMP/stdout") # safe: no supervised child is running now
    jq -n --arg v "$value" --argjson rc "$rc" '{value:$v, exitCode:$rc}' > "$VALUES"
    exit 0
    ;;
  *)
    echo "activity-shell: unknown op '$op'" >&2
    exit 2
    ;;
esac
