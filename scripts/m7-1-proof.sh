#!/usr/bin/env bash
# Regenerate every proof artifact for milestone M7.1 (ork rebrand).
#
#   make infra-up            # Temporal must be listening on localhost:7233
#   ./scripts/m7-1-proof.sh
#
# Writes proof/M7.1/{test.log,git-diff-summary.txt,binary.txt,behavior-trace.txt}.
set -euo pipefail

cd "$(dirname "$0")/.."
PROOF_DIR="proof/M7.1"
TRACE_ADDR="${ORK_TRACE_ADDR:-:18099}"
TRACE_SERVER="http://localhost${TRACE_ADDR}"

mkdir -p "$PROOF_DIR"

if pgrep -f 'bin/ork' >/dev/null 2>&1; then
  echo "refusing to run: a bin/ork* process is already polling ork-tasks" >&2
  echo "kill it first (see AGENTS.md §6), then re-run" >&2
  exit 1
fi

{
  echo "# M7.1 proof — $(date -u +%Y-%m-%dT%H:%M:%SZ)"
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

# Diff the whole milestone, not just the worktree: anchored at the branch point
# so the summary stays correct after the work is committed.
BASE="$(git merge-base "${ORK_DIFF_BASE:-main}" HEAD 2>/dev/null || echo HEAD)"
{
  echo "# M7.1 — git diff summary (rename-aware), base $BASE"
  echo
  echo "\$ git diff -M --stat $BASE"
  echo
  git diff -M --stat "$BASE"
  echo
  echo "\$ git diff -M --summary $BASE   # renames"
  echo
  git diff -M --summary "$BASE"
  echo
  echo "\$ git status --porcelain   # anything still uncommitted"
  echo
  git status --porcelain
} >"$PROOF_DIR/git-diff-summary.txt"

{
  echo "# M7.1 binary artifact — bin/ork"
  echo
  echo "Built by \`make build\` (Makefile target: go build -o bin/ork ./cmd/ork)."
  echo "The binary itself is not committed: /bin/ is gitignored (see .gitignore)."
  echo "Rebuild it with \`make build\`; this transcript is the identity evidence."
  echo
  echo "\$ ls -l bin/"
  ls -l bin/
  echo
  echo "\$ shasum -a 256 bin/ork"
  shasum -a 256 bin/ork
  echo
  echo "\$ go version -m bin/ork | head -20"
  go version -m bin/ork | head -20 || true
  echo
  echo "\$ ORK_TEMPORAL_ADDRESS=127.0.0.1:1 ./bin/ork   # boot proves identity + ork-main default"
  # ork exits non-zero on the unreachable address; that IS the evidence.
  ORK_TEMPORAL_ADDRESS=127.0.0.1:1 ./bin/ork 2>&1 | head -3 || true
} >"$PROOF_DIR/binary.txt" 2>&1

ORK_HTTP_ADDR="$TRACE_ADDR" ./bin/ork >/tmp/m7-1-ork.log 2>&1 &
ORK_PID=$!
./bin/orker >/tmp/m7-1-orker.log 2>&1 &
ORKER_PID=$!
cleanup() {
  kill -TERM "$ORK_PID" "$ORKER_PID" 2>/dev/null || true
  wait "$ORK_PID" "$ORKER_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for BOTH binaries: the engine serves the API, the Orker polls ork-tasks.
for _ in $(seq 1 60); do
  if ./bin/orkctl --server "$TRACE_SERVER" orkflow list >/dev/null 2>&1 \
    && grep -q 'orker started' /tmp/m7-1-orker.log; then
    break
  fi
  sleep 1
done

INVALID_DIR="$(mktemp -d)"
cat >"$INVALID_DIR/bad-name.ork.yaml" <<'YAML'
name: Bad_Name
version: "0.1.0"
tasks:
  - id: a
    plugin: shell
    with: {command: "echo 1"}
YAML
cat >"$INVALID_DIR/no-tasks.ork.yaml" <<'YAML'
name: ok-name
version: "0.1.0"
tasks: []
YAML

{
  echo "# M7.1 behavior trace — ork- run ids on the ork-main namespace / ork-tasks queue"
  echo
  echo "\$ ./bin/ork &   # ORK_HTTP_ADDR=$TRACE_ADDR, defaults elsewhere"
  head -3 /tmp/m7-1-ork.log
  echo
  echo "\$ ./bin/orker &"
  head -3 /tmp/m7-1-orker.log
  echo
  echo "\$ ./bin/orkctl --server $TRACE_SERVER apply examples/hello.ork.yaml"
  ./bin/orkctl --server "$TRACE_SERVER" apply examples/hello.ork.yaml
  echo
  echo "\$ ./bin/orkctl --server $TRACE_SERVER orkflow list"
  ./bin/orkctl --server "$TRACE_SERVER" orkflow list
  echo
  echo "\$ ./bin/orkctl --server $TRACE_SERVER run submit hello-ork --watch"
  ./bin/orkctl --server "$TRACE_SERVER" run submit hello-ork --watch 2>&1 | head -40 || true
  echo
  echo "# rejection surface: the validator names the entity an orkflow"
  echo
  echo "\$ ./bin/orkctl --server $TRACE_SERVER apply bad-name.ork.yaml   # name: Bad_Name"
  ./bin/orkctl --server "$TRACE_SERVER" apply "$INVALID_DIR/bad-name.ork.yaml" 2>&1 || true
  echo
  echo "\$ ./bin/orkctl --server $TRACE_SERVER apply no-tasks.ork.yaml   # tasks: []"
  ./bin/orkctl --server "$TRACE_SERVER" apply "$INVALID_DIR/no-tasks.ork.yaml" 2>&1 || true
} >"$PROOF_DIR/behavior-trace.txt" 2>&1
rm -rf "$INVALID_DIR"

cleanup
trap - EXIT
echo "proof written to $PROOF_DIR"
