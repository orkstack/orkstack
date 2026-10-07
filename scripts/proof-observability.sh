#!/usr/bin/env bash
# The `observability` proof gate (AGENTS.md §11.2): does a real run through
# `ork` and `orker` with telemetry switched on actually show up in Loki,
# Tempo and Prometheus? Every claim below is a query answered and asserted,
# not a screenshot or an eyeball check.
#
# scripts/proof.sh invokes this with ORK_PROOF_DIR already set and its own
# prerequisites already probed (docker, Temporal, MinIO, the local registry
# and its images, the observability profile's own four services, and no
# stray bin/ork or bin/orker) — this script does not re-check any of that; it
# only assumes ORK_PROOF_DIR is set and writable, per §11.2 ("prerequisites
# probed BEFORE any output file is opened").
#
# ork and orker are started DIRECTLY here, not through scripts/stack.sh,
# because each needs its OWN ORK_ADMIN_ADDR (:9464 / :9465) — something
# stack.sh does not currently let `make up` give them (see
# deploy/observability/README.md).
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
REPO="$PWD"

OUT="${ORK_PROOF_DIR:?ORK_PROOF_DIR must be set (run this through scripts/proof.sh)}/observability"
mkdir -p "$OUT"

# Namespaces are constrained to ^[a-z][a-z0-9-]*$ (no uppercase, no ':'),
# so the stamp used for both the namespace and the orkflow version is
# digits-only — %Y%m%dT%H%M%SZ's 'T'/'Z' would otherwise be refused.
STAMP="$(date -u +%Y%m%d%H%M%S)"
NS="proof-$STAMP"
ENGINE_URL="http://127.0.0.1:8080"
COLLECTOR="http://127.0.0.1:4318"
LOKI="http://127.0.0.1:3100"
TEMPO="http://127.0.0.1:3200"
PROM="http://127.0.0.1:9090"
ORKCTL=("$REPO/bin/orkctl" --server "$ENGINE_URL")

say() { printf '%s\n' "$*"; }
fail() { say "FAIL: $*"; exit 1; }

# ---- SUMMARY.md, appended to as evidence is gathered so a failing run ------
# leaves behind exactly what it managed to check, not nothing.
SUMMARY="$OUT/SUMMARY.md"
{
	echo "# observability gate evidence"
	echo
	echo "- namespace: $NS"
	echo "- date:      $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"$SUMMARY"

record() { echo "- **$2**: $1" >>"$SUMMARY"; } # <description> <pass|fail>
assert() {                                     # <description> <command...>
	local desc="$1"
	shift
	if "$@" >/dev/null 2>&1; then
		record "$desc" pass
		say "  pass: $desc"
	else
		record "$desc" fail
		fail "$desc"
	fi
}

# ---- ork and orker, started directly, killed on any exit -------------------
ORK_PID=""
ORKER_PID=""
cleanup() {
	[ -n "$ORK_PID" ] && kill "$ORK_PID" 2>/dev/null
	[ -n "$ORKER_PID" ] && kill "$ORKER_PID" 2>/dev/null
	sleep 1
	[ -n "$ORK_PID" ] && kill -9 "$ORK_PID" 2>/dev/null
	[ -n "$ORKER_PID" ] && kill -9 "$ORKER_PID" 2>/dev/null
}
trap cleanup EXIT

say "== build =="
make build >"$OUT/build.log" 2>&1 || fail "make build: see $OUT/build.log"

WORK_DIR="$REPO/.data/work"
mkdir -p "$WORK_DIR"

# local_activity_catalog mirrors scripts/stack.sh's function of the same
# name. Not sourced: stack.sh's own trailing `case "${1:-}"` would run (and
# `die`) the moment it was sourced with no matching argument.
local_activity_catalog() {
	local registry="${ORK_LOCAL_REGISTRY:-localhost:5001}" name doc=""
	for name in $(make -s -C "$REPO/images" print-activities); do
		curl -sfo /dev/null -H 'Accept: application/vnd.oci.image.index.v1+json' \
			"http://$registry/v2/orkstack/activity-$name/manifests/dev" || return 0
		doc="$doc  $name: $registry/orkstack/activity-$name:dev
"
	done
	printf 'activities:\n%s' "$doc"
}
CATALOG="$(local_activity_catalog)"
[ -n "$CATALOG" ] || fail "no first-party activity images in the local registry (make -C images push)"

# The telemetry block this phase proves end-to-end: a config file rather than
# the OTEL_*/ORK_ADMIN_ADDR/ORK_ORKER_TASK_LOG_EXPORT environment variables
# it used before this mechanism existed (docs/DEPLOY.md §1). ORK_ARTIFACT_BACKEND,
# ORK_ORKER_WORKDIR, ORK_HTTP_ADDR and ORK_ACTIVITY_CATALOG stay environment
# variables here — this gate is about the telemetry path, not a rewrite of
# every setting this script passes.
CONFIG_FILE="$OUT/orkstack.yaml"
cat >"$CONFIG_FILE" <<EOF
telemetry:
  log_format: json
  admin_addr: {ork: "127.0.0.1:9464", orker: "127.0.0.1:9465"}
  exporters:
    logs: otlp
    metrics: [prometheus, otlp]
    traces: otlp
  otlp:
    endpoint: $COLLECTOR
  batch:
    metric_export_interval: 5000
orker:
  task_log_export: true
EOF

say "== starting orker (admin 127.0.0.1:9465, OTLP -> $COLLECTOR, --config $CONFIG_FILE) =="
env \
	ORK_ARTIFACT_BACKEND=s3 \
	ORK_ORKER_WORKDIR="$WORK_DIR" \
	"$REPO/bin/orker" --config "$CONFIG_FILE" >"$OUT/orker.log" 2>&1 &
ORKER_PID=$!

say "== starting ork (admin 127.0.0.1:9464, OTLP -> $COLLECTOR, --config $CONFIG_FILE) =="
env \
	ORK_ARTIFACT_BACKEND=s3 \
	ORK_HTTP_ADDR=:8080 \
	ORK_ACTIVITY_CATALOG="$CATALOG" \
	"$REPO/bin/ork" --config "$CONFIG_FILE" >"$OUT/ork.log" 2>&1 &
ORK_PID=$!

# wait_ready NAME PORT PID — poll /readyz for up to 60s, failing fast if the
# process has already died so a crash loop is not mistaken for slow startup.
wait_ready() {
	local name="$1" port="$2" pid="$3" code
	local i
	for i in $(seq 1 120); do
		kill -0 "$pid" 2>/dev/null || fail "$name exited during startup; see $OUT/$name.log"
		code="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/readyz" 2>/dev/null)"
		if [ "$code" = "200" ]; then
			say "  $name ready"
			return 0
		fi
		sleep 0.5
	done
	fail "$name did not answer 200 on :$port/readyz within 60s; see $OUT/$name.log"
}
wait_ready orker 9465 "$ORKER_PID"
wait_ready ork 9464 "$ORK_PID"

# ---- 2. admin surface -------------------------------------------------------
{
	for pair in ork:9464 orker:9465; do
		name="${pair%%:*}"
		port="${pair##*:}"
		echo "== $name (:$port) =="
		echo "healthz: $(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/healthz")"
		echo "readyz:  $(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/readyz")"
		echo "pprof:   $(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/debug/pprof/")"
	done
} >"$OUT/admin.txt"
assert "ork /healthz and /readyz answer 200, /debug/pprof/ answers 404 (ORK_PPROF_ENABLED unset)" \
	bash -c "grep -A3 '== ork ' '$OUT/admin.txt' | grep -q 'healthz: 200' && grep -A3 '== ork ' '$OUT/admin.txt' | grep -q 'readyz:  200' && grep -A3 '== ork ' '$OUT/admin.txt' | grep -q 'pprof:   404'"
assert "orker /healthz and /readyz answer 200, /debug/pprof/ answers 404" \
	bash -c "grep -A3 '== orker ' '$OUT/admin.txt' | grep -q 'healthz: 200' && grep -A3 '== orker ' '$OUT/admin.txt' | grep -q 'readyz:  200' && grep -A3 '== orker ' '$OUT/admin.txt' | grep -q 'pprof:   404'"

# ---- 3. one real run ---------------------------------------------------------
say "== namespace $NS =="
"${ORKCTL[@]}" namespace create "$NS" --telemetry-attribute team=proof \
	>"$OUT/namespace-create.txt" 2>&1 || fail "namespace create: see $OUT/namespace-create.txt"

ORKFLOW_FILE="$OUT/hello-$STAMP.ork.yaml"
sed "s/^version: .*/version: \"$STAMP\"/" "$REPO/examples/hello.ork.yaml" >"$ORKFLOW_FILE"
grep -q "version: \"$STAMP\"" "$ORKFLOW_FILE" || fail "could not stamp a unique version into $ORKFLOW_FILE"

"${ORKCTL[@]}" -n "$NS" apply "$ORKFLOW_FILE" >"$OUT/apply.txt" 2>&1 \
	|| fail "apply: see $OUT/apply.txt"

"${ORKCTL[@]}" -n "$NS" run submit hello-ork --json >"$OUT/submit.json" 2>"$OUT/submit.log" \
	|| fail "run submit: see $OUT/submit.log"
RUN_ID="$(jq -r '.run.id // empty' "$OUT/submit.json")"
[ -n "$RUN_ID" ] || fail "run submit produced no run id: see $OUT/submit.json"
say "  run $RUN_ID submitted"
echo "- run: $RUN_ID" >>"$SUMMARY"

status=""
for i in $(seq 1 360); do
	"${ORKCTL[@]}" -n "$NS" run get "$RUN_ID" --json >"$OUT/run.json" 2>"$OUT/run-get.log"
	status="$(jq -r '.run.status // empty' "$OUT/run.json" 2>/dev/null)"
	case "$status" in
	SUCCEEDED | FAILED | CANCELED) break ;;
	esac
	sleep 0.5
done
[ "$status" = "SUCCEEDED" ] || fail "run $RUN_ID ended '$status' (want SUCCEEDED) after 180s; see $OUT/run.json"
say "  run $RUN_ID: SUCCEEDED"

# ---- 4. evidence queries -----------------------------------------------------

# (a) Metrics — the direct-scrape path (ORK_ADMIN_ADDR's own /metrics).
say "== metrics =="
curl -s "http://127.0.0.1:9465/metrics" >"$OUT/orker-metrics.txt"
assert "orker /metrics: ork_orker_tasks_total{...,outcome=\"succeeded\",...}" \
	grep -qE 'ork_orker_tasks_total\{[^}]*outcome="succeeded"[^}]*\}' "$OUT/orker-metrics.txt"
assert "orker /metrics: ork_orker_task_log_records_total" \
	grep -q '^ork_orker_task_log_records_total' "$OUT/orker-metrics.txt"

curl -s "http://127.0.0.1:9464/metrics" >"$OUT/ork-metrics.txt"
assert "ork /metrics: ork_build_info" \
	grep -q '^ork_build_info' "$OUT/ork-metrics.txt"

# (b) Loki — the OTLP log path, via the Collector.
say "== loki (polling up to 60s) =="
loki_query_range() { # <logql query> -> writes response to stdout
	local now start
	now=$(($(date +%s) * 1000000000))
	start=$((now - 15 * 60 * 1000000000))
	curl -s --get "$LOKI/loki/api/v1/query_range" \
		--data-urlencode "query=$1" \
		--data-urlencode "start=$start" \
		--data-urlencode "end=$now" \
		--data-urlencode "limit=100"
}
poll_loki_streams() { # <logql query> <out file> -> 0 once >=1 stream returned
	local query="$1" out="$2" i n
	for i in $(seq 1 120); do
		loki_query_range "$query" >"$out"
		n="$(jq -r '(.data.result | length) // 0' "$out" 2>/dev/null || echo 0)"
		[ "${n:-0}" -ge 1 ] 2>/dev/null && return 0
		sleep 0.5
	done
	return 1
}
poll_loki_streams "{service_name=\"orker\"} | ork_run_id=\"$RUN_ID\"" "$OUT/loki-orker.json"
assert "Loki: {service_name=\"orker\"} | ork_run_id=\"$RUN_ID\" returns >=1 stream" \
	bash -c "[ \"\$(jq -r '(.data.result | length) // 0' '$OUT/loki-orker.json')\" -ge 1 ]"

poll_loki_streams "{service_name=\"ork\"} | run_id=\"$RUN_ID\" | event_type=\"run_succeeded\"" "$OUT/loki-ork.json"
assert "Loki: {service_name=\"ork\"} | run_id=\"$RUN_ID\" | event_type=\"run_succeeded\" returns >=1 stream" \
	bash -c "[ \"\$(jq -r '(.data.result | length) // 0' '$OUT/loki-ork.json')\" -ge 1 ]"
# ork_ns_team can arrive as an indexed stream label or, depending on this
# Loki's structured-metadata handling, as per-line metadata (the 3rd element
# of a values tuple) — check both places rather than assume one.
loki_stream_has_label() { # <json file> <label> <value>
	jq -e --arg k "$2" --arg v "$3" '
		.data.result[]? | select(
			(.stream[$k]? == $v) or
			(any(.values[]?; length > 2 and (.[2][$k]? == $v)))
		)' "$1" >/dev/null
}
assert "Loki: that stream carries ork_ns_team=\"proof\"" \
	loki_stream_has_label "$OUT/loki-ork.json" ork_ns_team proof

# (c) Tempo — the OTLP trace path.
say "== tempo (polling up to 60s) =="
TRACE_ID=""
for i in $(seq 1 120); do
	curl -s --get "$TEMPO/api/search" --data-urlencode "q={ span.ork.run_id = \"$RUN_ID\" }" \
		>"$OUT/tempo-search.json"
	TRACE_ID="$(jq -r '.traces[0].traceID // empty' "$OUT/tempo-search.json" 2>/dev/null)"
	[ -n "$TRACE_ID" ] && break
	sleep 0.5
done
assert "Tempo: /api/search{span.ork.run_id=\"$RUN_ID\"} returns a trace id" \
	bash -c "[ -n '$TRACE_ID' ]"
echo "- trace: $TRACE_ID" >>"$SUMMARY"

curl -s "$TEMPO/api/traces/$TRACE_ID" >"$OUT/tempo-trace.json"
# The trace API's JSON shape is OTLP's own (resourceSpans/scopeSpans/spans);
# flatten every span name and every resource's service.name in one pass so
# the counts below don't depend on which of Tempo's historical key spellings
# (camelCase vs snake_case) this version emits.
jq '[.batches[]?, .resourceSpans[]?] | map({
      service: ([.resource.attributes[]? | select(.key=="service.name") | .value.stringValue] | first),
      spans: [(.scopeSpans // .instrumentationLibrarySpans)[]?.spans[]?.name]
    })' "$OUT/tempo-trace.json" >"$OUT/tempo-trace-summary.json" 2>/dev/null \
	|| echo '[]' >"$OUT/tempo-trace-summary.json"

ATTEMPT_SPANS="$(jq '[.[].spans[] | select(. == "ork.task.attempt")] | length' "$OUT/tempo-trace-summary.json")"
PHASE_SPANS="$(jq '[.[].spans[] | select(. == "ork.task.phase")] | length' "$OUT/tempo-trace-summary.json")"
TRACE_SERVICES="$(jq '[.[].service] | unique | length' "$OUT/tempo-trace-summary.json")"
assert "Tempo trace $TRACE_ID: 3 ork.task.attempt spans (found $ATTEMPT_SPANS)" \
	bash -c "[ '$ATTEMPT_SPANS' = 3 ]"
assert "Tempo trace $TRACE_ID: >=30 ork.task.phase spans (found $PHASE_SPANS)" \
	bash -c "[ '$PHASE_SPANS' -ge 30 ]"
assert "Tempo trace $TRACE_ID: spans from both ork and orker (found $TRACE_SERVICES)" \
	bash -c "[ '$TRACE_SERVICES' -ge 2 ]"

# (d) Prometheus — the scrape path (targets up) and the OTLP path (a metric
# that only exists because it arrived over OTLP, not the /metrics scrape).
say "== prometheus (polling up to 90s) =="
prom_query() { curl -s --get "$PROM/api/v1/query" --data-urlencode "query=$1"; }

poll_prom() { # <promql> <out file>
	local query="$1" out="$2" i n
	for i in $(seq 1 180); do
		prom_query "$query" >"$out"
		n="$(jq -r '(.data.result | length) // 0' "$out" 2>/dev/null || echo 0)"
		[ "${n:-0}" -ge 1 ] 2>/dev/null && return 0
		sleep 0.5
	done
	return 1
}
poll_prom 'ork_runs_total{status="succeeded"}' "$OUT/prom-ork-runs.json"
assert "Prometheus: ork_runs_total{status=\"succeeded\"} has a series" \
	bash -c "[ \"\$(jq -r '(.data.result | length) // 0' '$OUT/prom-ork-runs.json')\" -ge 1 ]"

poll_prom 'temporal_activity_execution_latency_seconds_count' "$OUT/prom-temporal-latency.json"
assert "Prometheus: temporal_activity_execution_latency_seconds_count exists (OTLP path)" \
	bash -c "[ \"\$(jq -r '(.data.result | length) // 0' '$OUT/prom-temporal-latency.json')\" -ge 1 ]"

curl -s "$PROM/api/v1/targets" >"$OUT/prom-targets.json"
assert "Prometheus: the ork (9464) and orker (9465) scrape targets are up" \
	bash -c "jq -e '[.data.activeTargets[]? | select(.scrapePool==\"ork\" or .scrapePool==\"orker\") | select(.health==\"up\")] | length >= 2' '$OUT/prom-targets.json' >/dev/null"

GRAFANA_URL="http://localhost:3000/explore?left=%7B%22datasource%22:%22Tempo%22,%22queries%22:%5B%7B%22query%22:%22$TRACE_ID%22,%22queryType%22:%22traceql%22%7D%5D,%22range%22:%7B%22from%22:%22now-1h%22,%22to%22:%22now%22%7D%7D"
{
	echo
	echo "## Grafana"
	echo
	echo "$GRAFANA_URL"
} >>"$SUMMARY"

# ---- 5. zero-config control --------------------------------------------------
# Same binaries, same namespace, no OTEL_*/ORK_ADMIN_ADDR/ORK_LOG_* at all:
# docs/DEPLOY.md §7 promises byte-identical behaviour to a deployment that
# has never heard of telemetry. Prove it by tearing the telemetry-on
# processes down and starting the zero-config pair in their place.
say "== zero-config control =="
cleanup
trap - EXIT
ORK_PID=""
ORKER_PID=""
trap cleanup EXIT

env ORK_ARTIFACT_BACKEND=s3 ORK_ORKER_WORKDIR="$WORK_DIR" \
	"$REPO/bin/orker" >"$OUT/orker-plain.log" 2>&1 &
ORKER_PID=$!
env ORK_ARTIFACT_BACKEND=s3 ORK_HTTP_ADDR=:8080 ORK_ACTIVITY_CATALOG="$CATALOG" \
	"$REPO/bin/ork" >"$OUT/ork-plain.log" 2>&1 &
ORK_PID=$!

ready=1
for i in $(seq 1 120); do
	kill -0 "$ORK_PID" 2>/dev/null || fail "plain ork exited during startup; see $OUT/ork-plain.log"
	kill -0 "$ORKER_PID" 2>/dev/null || fail "plain orker exited during startup; see $OUT/orker-plain.log"
	if "${ORKCTL[@]}" orkflow list >/dev/null 2>"$OUT/orkflow-list.log"; then
		ready=0
		break
	fi
	sleep 0.5
done
[ "$ready" -eq 0 ] || fail "plain ork never answered 'orkctl orkflow list' within 60s; see $OUT/orkflow-list.log"

assert "zero-config: nothing listens on 127.0.0.1:9464" \
	bash -c "! curl -s -o /dev/null --max-time 1 http://127.0.0.1:9464/healthz"
assert "zero-config: nothing listens on 127.0.0.1:9465" \
	bash -c "! curl -s -o /dev/null --max-time 1 http://127.0.0.1:9465/healthz"
assert "zero-config: ork.log's first line is text (time=...), not JSON" \
	bash -c "head -n1 '$OUT/ork-plain.log' | grep -qE '^time='"

"${ORKCTL[@]}" -n "$NS" run submit hello-ork --json >"$OUT/submit-plain.json" 2>"$OUT/submit-plain.log" \
	|| fail "zero-config run submit: see $OUT/submit-plain.log"
PLAIN_RUN_ID="$(jq -r '.run.id // empty' "$OUT/submit-plain.json")"
[ -n "$PLAIN_RUN_ID" ] || fail "zero-config run submit produced no run id: see $OUT/submit-plain.json"

plain_status=""
for i in $(seq 1 360); do
	"${ORKCTL[@]}" -n "$NS" run get "$PLAIN_RUN_ID" --json >"$OUT/run-plain.json" 2>"$OUT/run-get-plain.log"
	plain_status="$(jq -r '.run.status // empty' "$OUT/run-plain.json" 2>/dev/null)"
	case "$plain_status" in
	SUCCEEDED | FAILED | CANCELED) break ;;
	esac
	sleep 0.5
done
assert "zero-config: run $PLAIN_RUN_ID SUCCEEDED" bash -c "[ '$plain_status' = 'SUCCEEDED' ]"

"${ORKCTL[@]}" -n "$NS" run logs "$PLAIN_RUN_ID" >"$OUT/run-logs-plain.txt" 2>"$OUT/run-logs-plain.log"
assert "zero-config: 'orkctl run logs' prints at least one line" \
	bash -c "[ -s '$OUT/run-logs-plain.txt' ]"

echo "- zero-config run: $PLAIN_RUN_ID" >>"$SUMMARY"
echo >>"$SUMMARY"
echo "Result: **PASS**" >>"$SUMMARY"
say
say "observability: PASS -> $SUMMARY"
