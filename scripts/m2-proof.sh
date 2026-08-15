#!/usr/bin/env bash
# Regenerate every proof artifact for milestone M2 (engine).
#
#   make infra-up            # Temporal must be listening on localhost:7233
#   ./scripts/m2-proof.sh
#
# Writes proof/M2/{test.log,event-stream.json,engine-smoke.log}.
set -euo pipefail

cd "$(dirname "$0")/.."
PROOF_DIR="proof/M2"
SMOKE_ADDR="${ORK_SMOKE_ADDR:-:18080}"
SMOKE_HOST="127.0.0.1${SMOKE_ADDR}"

mkdir -p "$PROOF_DIR"
: >"$PROOF_DIR/test.log"

{
  echo "# M2 proof run"
  echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "go:   $(go version)"
  echo "temporal address: ${ORK_TEMPORAL_ADDRESS:-localhost:7233}"
  echo
} | tee -a "$PROOF_DIR/test.log"

echo "== make lint ==" | tee -a "$PROOF_DIR/test.log"
set +e
make lint 2>&1 | tee -a "$PROOF_DIR/test.log"
LINT_STATUS=${PIPESTATUS[0]}
set -e
if ((LINT_STATUS == 0)); then
  echo "lint: OK" | tee -a "$PROOF_DIR/test.log"
else
  echo "lint: BLOCKED by baseline pkg/plugin/plugin_test.go gofmt conflict (exit $LINT_STATUS)" \
    | tee -a "$PROOF_DIR/test.log"
fi

echo "== unit + workflow tests: go test ./... ==" | tee -a "$PROOF_DIR/test.log"
go test ./... -count=1 -v 2>&1 | tee -a "$PROOF_DIR/test.log"

echo "== integration tests: go test -tags=integration ./... ==" | tee -a "$PROOF_DIR/test.log"
ORK_PROOF_EVENT_FILE="$PWD/$PROOF_DIR/event-stream.json" \
  go test -tags=integration ./... -count=1 -v -run 'Integration' 2>&1 | tee -a "$PROOF_DIR/test.log"

echo "== coverage ==" | tee -a "$PROOF_DIR/test.log"
go test ./internal/... -count=1 -coverprofile="$PROOF_DIR/coverage.out" 2>&1 | tee -a "$PROOF_DIR/test.log"
go tool cover -func="$PROOF_DIR/coverage.out" | tail -1 | tee -a "$PROOF_DIR/test.log"

echo "== ork engine smoke check ==" | tee "$PROOF_DIR/engine-smoke.log"
ORK_HTTP_ADDR="$SMOKE_ADDR" go run ./cmd/ork >>"$PROOF_DIR/engine-smoke.log" 2>&1 &
ENGINE_PID=$!
cleanup() {
  pkill -TERM -P "$ENGINE_PID" 2>/dev/null || true
  kill -TERM "$ENGINE_PID" 2>/dev/null || true
  wait "$ENGINE_PID" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 60); do
  if curl -fsS -o /dev/null -X POST -H 'Content-Type: application/json' -d '{}' \
    "http://${SMOKE_HOST}/orkstack.v1.OrkflowService/ListOrkflows" 2>/dev/null; then
    break
  fi
  sleep 1
done

{
  echo
  echo "--- ListOrkflows (health/list) ---"
  curl -fsS -X POST -H 'Content-Type: application/json' -d '{}' \
    "http://${SMOKE_HOST}/orkstack.v1.OrkflowService/ListOrkflows"
  echo
  echo "--- Apply examples/hello.ork.yaml ---"
  python3 -c 'import json;print(json.dumps({"yaml":open("examples/hello.ork.yaml").read()}))' >/tmp/ork-apply.json
  curl -fsS -X POST -H 'Content-Type: application/json' --data-binary @/tmp/ork-apply.json \
    "http://${SMOKE_HOST}/orkstack.v1.OrkflowService/ApplyOrkflow" | head -c 400
  echo
  echo "--- ListOrkflows after apply ---"
  curl -fsS -X POST -H 'Content-Type: application/json' -d '{}' \
    "http://${SMOKE_HOST}/orkstack.v1.OrkflowService/ListOrkflows" | head -c 200
  echo
  echo "--- ListPlugins ---"
  curl -fsS -X POST -H 'Content-Type: application/json' -d '{}' \
    "http://${SMOKE_HOST}/orkstack.v1.PluginService/ListPlugins"
  echo
} 2>&1 | tee -a "$PROOF_DIR/engine-smoke.log"

cleanup
trap - EXIT
sleep 1
echo "--- engine log tail (graceful shutdown) ---" >>"$PROOF_DIR/engine-smoke.log"
echo "proof written to $PROOF_DIR"
if ((LINT_STATUS != 0)); then
  echo "proof completed, but M2 remains blocked because make lint failed" >&2
  exit "$LINT_STATUS"
fi
