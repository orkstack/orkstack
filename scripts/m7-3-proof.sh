#!/usr/bin/env bash
# Regenerate every proof artifact for milestone M7.3 (the generic `op` field).
#
#   make infra-up            # Temporal must be listening on localhost:7233
#   ./scripts/m7-3-proof.sh
#
# Writes proof/M7.3/{test.log,examples-transcript.txt,documentation-review.txt}.
# proof/M7.3/README.md is written by hand and is not regenerated here.
set -euo pipefail

cd "$(dirname "$0")/.."
PROOF_DIR="proof/M7.3"
TRACE_ADDR="${ORK_TRACE_ADDR:-:18099}"
TRACE_SERVER="http://localhost${TRACE_ADDR}"
ORKCTL=(./bin/orkctl --server "$TRACE_SERVER")

# Artifacts must land in MinIO, not on the local filesystem: the milestone's
# live evidence is a real Temporal + MinIO stack. Every other S3 default in
# pkg/artifacts/config.go already matches deploy/docker-compose.yml.
export ORK_ARTIFACT_BACKEND="${ORK_ARTIFACT_BACKEND:-s3}"
S3_ENDPOINT="${ORK_S3_ENDPOINT:-http://localhost:9000}"
S3_BUCKET="${ORK_S3_BUCKET:-orkstack-artifacts}"

mkdir -p "$PROOF_DIR"

if [ "$ORK_ARTIFACT_BACKEND" = "s3" ] \
  && ! curl -fsS -o /dev/null --max-time 5 "$S3_ENDPOINT/minio/health/live"; then
  echo "refusing to run: MinIO is not answering at $S3_ENDPOINT" >&2
  echo "start it with 'make infra-up', then re-run" >&2
  exit 1
fi

# The engine, the Orker and every integration test poll the same ork-tasks
# queue: a stray process steals this run's activities (AGENTS.md §6).
if pgrep -f 'bin/ork' >/dev/null 2>&1; then
  echo "refusing to run: a bin/ork* process is already polling ork-tasks" >&2
  echo "kill it first (see AGENTS.md §6), then re-run" >&2
  exit 1
fi

## ---- gates ----
{
  echo "# M7.3 proof — $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "host:   $(uname -srm)"
  echo "go:     $(go version)"
  echo "branch: $(git rev-parse --abbrev-ref HEAD)"
  echo
  echo "== make lint (go vet + gofmt) =="
} >"$PROOF_DIR/test.log"
make lint 2>&1 | tee -a "$PROOF_DIR/test.log"

echo "== make build ==" >>"$PROOF_DIR/test.log"
make build 2>&1 | tee -a "$PROOF_DIR/test.log"

echo "== go test ./... -count=1 -v ==" >>"$PROOF_DIR/test.log"
go test ./... -count=1 -v 2>&1 | tee -a "$PROOF_DIR/test.log" | grep -E '^(ok|FAIL)' || true

# -p 1: every package under test polls the shared ork-tasks queue.
echo "== go test -tags=integration -p 1 ./... -count=1 -v ==" >>"$PROOF_DIR/test.log"
go test -tags=integration -p 1 ./... -count=1 -v -timeout 900s 2>&1 \
  | tee -a "$PROOF_DIR/test.log" | grep -E '^(ok|FAIL)' || true

## ---- live stack ----
# ORK_ARTIFACT_BACKEND=s3 is exported above, so both binaries store artifacts
# in MinIO.
ORK_HTTP_ADDR="$TRACE_ADDR" ./bin/ork >/tmp/m7-3-ork.log 2>&1 &
ORK_PID=$!
./bin/orker >/tmp/m7-3-orker.log 2>&1 &
ORKER_PID=$!
cleanup() {
  kill -TERM "$ORK_PID" "$ORKER_PID" 2>/dev/null || true
  wait "$ORK_PID" "$ORKER_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for BOTH binaries: the engine serves the API, the Orker polls ork-tasks.
for _ in $(seq 1 60); do
  if "${ORKCTL[@]}" orkflow list >/dev/null 2>&1 \
    && grep -q 'orker started' /tmp/m7-3-orker.log; then
    break
  fi
  sleep 1
done

## ---- every shipped example, end to end ----
# One orkflow per plugin whose op no plugin implements: each must fail
# permanently with a message quoting the value and listing the supported ops.
BAD_DIR="$(mktemp -d)"
cat >"$BAD_DIR/unknown-op-http.ork.yaml" <<'YAML'
name: unknown-op-http
version: "0.1.0"
description: An op the http plugin does not implement must fail loudly.
tasks:
  - id: frobnicate
    plugin: http
    with:
      op: frobnicate
      url: "http://127.0.0.1:1/never"
YAML
cat >"$BAD_DIR/unknown-op-shell.ork.yaml" <<'YAML'
name: unknown-op-shell
version: "0.1.0"
description: An op the shell plugin does not implement must fail loudly.
tasks:
  - id: frobnicate
    plugin: shell
    with:
      op: frobnicate
      command: "touch /tmp/m7-3-should-not-exist"
YAML
cat >"$BAD_DIR/unknown-op-file.ork.yaml" <<'YAML'
name: unknown-op-file
version: "0.1.0"
description: An op the file plugin does not implement must fail loudly.
tasks:
  - id: frobnicate
    plugin: file
    with:
      op: frobnicate
      path: should-not-exist.txt
YAML
rm -f /tmp/m7-3-should-not-exist

{
  echo "# M7.3 examples transcript — every shipped example against a live engine"
  echo "# $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  echo "# artifact backend: $ORK_ARTIFACT_BACKEND  bucket: $S3_BUCKET  endpoint: $S3_ENDPOINT"
  curl -fsS -o /dev/null -w '# MinIO health: HTTP %{http_code}\n' "$S3_ENDPOINT/minio/health/live" || true
  echo
  echo "\$ ./bin/ork &   # ORK_HTTP_ADDR=$TRACE_ADDR"
  head -3 /tmp/m7-3-ork.log
  echo
  echo "\$ ./bin/orker &"
  head -3 /tmp/m7-3-orker.log
  echo
  for example in examples/*.ork.yaml; do
    name="$(sed -n 's/^name: //p' "$example" | head -1)"
    echo "################################################################"
    echo "# $example  ->  orkflow $name"
    echo "################################################################"
    echo
    echo "\$ cat $example"
    cat "$example"
    echo
    echo "\$ orkctl apply $example"
    "${ORKCTL[@]}" apply "$example"
    echo
    echo "\$ orkctl run submit $name --watch"
    "${ORKCTL[@]}" run submit "$name" --watch 2>&1 || echo "!! run failed"
    echo
    run_id="$("${ORKCTL[@]}" run list --json 2>/dev/null | python3 -c '
import json,sys
runs = json.load(sys.stdin)
runs = runs.get("runs", runs) if isinstance(runs, dict) else runs
print(runs[0].get("id", "") if runs else "")
')"
    echo "\$ orkctl run get $run_id --json   # task outputs and artifacts: what each op produced"
    "${ORKCTL[@]}" run get "$run_id" --json 2>&1 | tee /tmp/m7-3-run.json | python3 -c '
import json,sys
run = json.load(sys.stdin)
run = run.get("run", run)
print(json.dumps({
    "id": run.get("id"),
    "status": run.get("status"),
    "tasks": [
        {"id": t.get("taskId") or t.get("id"), "status": t.get("status"),
         "output": t.get("output"),
         "artifacts": [{"name": a.get("name"), "uri": a.get("uri"),
                        "size_bytes": a.get("sizeBytes")}
                       for a in (t.get("artifacts") or [])]}
        for t in (run.get("taskRuns") or run.get("tasks") or [])
    ],
}, indent=2))
' || true
    # Every artifact URI above points into MinIO. Pull one back out of the
    # object store over plain HTTP to show the bytes really live there.
    object_url="$(python3 -c '
import json,sys
run = json.load(open("/tmp/m7-3-run.json"))
run = run.get("run", run)
for task in (run.get("taskRuns") or []):
    for artifact in (task.get("artifacts") or []):
        uri = artifact.get("uri", "")
        if uri.startswith("s3://"):
            print(sys.argv[1].rstrip("/") + "/" + uri[len("s3://"):])
            raise SystemExit
' "$S3_ENDPOINT" 2>/dev/null)"
    if [ -n "$object_url" ]; then
      echo "\$ curl -sS $object_url | head -c 200   # the artifact, read back from MinIO"
      curl -fsS "$object_url" | head -c 200 || echo "(artifact fetch failed)"
      echo
    fi
    echo
  done

  echo "################################################################"
  echo "# unknown op: rejected permanently, before any side effect"
  echo "################################################################"
  for bad in "$BAD_DIR"/unknown-op-*.ork.yaml; do
    bad_name="$(sed -n 's/^name: //p' "$bad" | head -1)"
    echo
    echo "\$ cat $(basename "$bad")"
    cat "$bad"
    echo
    echo "\$ orkctl apply $(basename "$bad")"
    "${ORKCTL[@]}" apply "$bad"
    echo
    echo "\$ orkctl run submit $bad_name --watch   # expected to FAIL"
    "${ORKCTL[@]}" run submit "$bad_name" --watch 2>&1 | grep -E 'TASK_FAILED|RUN_FAILED|finished with status' \
      || echo "(run failed as expected)"
  done
  echo
  echo "# the shell task above would have created /tmp/m7-3-should-not-exist"
  echo "\$ ls /tmp/m7-3-should-not-exist"
  ls /tmp/m7-3-should-not-exist 2>&1 || echo "absent: an unknown op never ran the command"
} >"$PROOF_DIR/examples-transcript.txt" 2>&1
rm -rf "$BAD_DIR"

## ---- documentation review ----
# Cross-check the prose against the ops the plugins actually advertise: every
# op a document claims must exist in the plugin's Describe() enum, and vice
# versa. scripts/m7-3-doc-review.py exits non-zero on any disagreement.
{
  echo "# M7.3 documentation review — docs vs. the ops the code advertises"
  echo "# $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  echo "== files that must exist =="
  for f in docs/PLUGIN_DESIGN.md pkg/plugins/http/README.md \
           pkg/plugins/shell/README.md pkg/plugins/file/README.md \
           examples/hello.ork.yaml examples/http-post.ork.yaml \
           examples/file-read.ork.yaml examples/shell-eval.ork.yaml; do
    if [ -f "$f" ]; then printf 'PRESENT  %s (%s bytes)\n' "$f" "$(wc -c <"$f" | tr -d ' ')";
    else printf 'MISSING  %s\n' "$f"; fi
  done
  echo
  echo "\$ orkctl plugin list   # what a live Orker serves"
  "${ORKCTL[@]}" plugin list
  echo
  echo "\$ scripts/m7-3-doc-review.py"
  python3 scripts/m7-3-doc-review.py || echo "!! documentation review failed"
} >"$PROOF_DIR/documentation-review.txt" 2>&1

cleanup
trap - EXIT
echo "proof written to $PROOF_DIR"
