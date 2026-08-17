#!/usr/bin/env bash
# The whole OrkStack dev stack, up or down in one command.
#
#   scripts/stack.sh up      infra + engine + orker + UI, then wait for health
#   scripts/stack.sh down    stop what up started (leaves data alone)
#   scripts/stack.sh status  what is running, and where
#
# Everything it starts is recorded in .run/: one pidfile and one log per
# process. down kills exactly those pids, then sweeps by name for anything
# started by hand, because a stray Orker steals activities from every later
# run and looks like a code bug (AGENTS.md §6).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
REPO="$PWD"
RUN_DIR="$REPO/.run"
COMPOSE=(docker compose -f "$REPO/deploy/docker-compose.yml")

ENGINE_ADDR="${ORK_HTTP_ADDR:-:8080}"
ENGINE_URL="http://127.0.0.1:${ENGINE_ADDR##*:}"
UI_PORT="${ORK_UI_PORT:-5173}"
UI_URL="http://localhost:$UI_PORT"

say() { printf '%s\n' "$*"; }
die() { printf '%s\n' "$*" >&2; exit 1; }

pidfile() { echo "$RUN_DIR/$1.pid"; }
logfile() { echo "$RUN_DIR/$1.log"; }

# alive NAME — true when the recorded pid is still running.
alive() {
	local pf
	pf="$(pidfile "$1")"
	[ -f "$pf" ] || return 1
	kill -0 "$(cat "$pf")" 2>/dev/null
}

# start NAME CMD... — run in the background, record pid and log, and confirm
# it is still alive a moment later. A process that exits during startup (the
# Orker losing a race to dial Temporal is the one seen in practice) must not
# be reported as started: "up" then lies, and the missing worker shows up much
# later as a run that never leaves RUNNING.
start() {
	local name="$1"
	shift
	if alive "$name"; then
		say "  $name already running (pid $(cat "$(pidfile "$name")"))"
		return 0
	fi
	mkdir -p "$RUN_DIR"
	local attempt pid
	for attempt in 1 2 3; do
		"$@" >"$(logfile "$name")" 2>&1 &
		pid=$!
		echo "$pid" >"$(pidfile "$name")"
		sleep 2
		if kill -0 "$pid" 2>/dev/null; then
			say "  $name started (pid $pid), log $(logfile "$name")"
			return 0
		fi
		say "  $name exited during startup (attempt $attempt/3)"
		sed -n '$p' "$(logfile "$name")" | sed 's/^/    /'
	done
	rm -f "$(pidfile "$name")"
	die "$name would not stay up; full log at $(logfile "$name")"
}

# stop NAME — terminate politely, then firmly.
stop() {
	local name="$1" pf pid
	pf="$(pidfile "$name")"
	[ -f "$pf" ] || return 0
	pid="$(cat "$pf")"
	if kill -0 "$pid" 2>/dev/null; then
		kill "$pid" 2>/dev/null || true
		for _ in $(seq 1 40); do
			kill -0 "$pid" 2>/dev/null || break
			sleep 0.25
		done
		kill -9 "$pid" 2>/dev/null || true
		say "  $name stopped (pid $pid)"
	fi
	rm -f "$pf"
}

# wait_for LABEL SECONDS CMD... — poll until the command succeeds.
wait_for() {
	local label="$1" seconds="$2"
	shift 2
	for _ in $(seq 1 $((seconds * 2))); do
		if "$@" >/dev/null 2>&1; then
			say "  $label ready"
			return 0
		fi
		sleep 0.5
	done
	die "timed out waiting for $label (see $RUN_DIR/*.log)"
}

cmd_up() {
	say "== infrastructure (Temporal, MinIO) =="
	"${COMPOSE[@]}" up -d
	wait_for "temporal" 60 docker exec orkstack-temporal-1 \
		temporal operator cluster health --address 127.0.0.1:7233
	wait_for "minio" 60 curl -sf http://localhost:9000/minio/health/live

	say "== binaries =="
	make build >/dev/null
	say "  bin/ork bin/orker bin/orkctl"

	say "== engine and orker =="
	# Both processes get the SAME artifact backend. The engine archives runs
	# and stores the orkflow catalogue through it, the Orker writes task
	# artifacts through it, and the default is local (./.data/artifacts). Set
	# it on one process only and the stack splits its brain: artifacts in
	# MinIO, archived runs on local disk, and `make reset` wiping the volume
	# leaves runs that a fresh engine still lists.
	local backend="${ORK_ARTIFACT_BACKEND:-s3}"
	start orker env ORK_ARTIFACT_BACKEND="$backend" "$REPO/bin/orker"
	start ork env ORK_ARTIFACT_BACKEND="$backend" ORK_HTTP_ADDR="$ENGINE_ADDR" "$REPO/bin/ork"
	wait_for "engine api" 60 "$REPO/bin/orkctl" --server "$ENGINE_URL" orkflow list
	# Plugin discovery is an activity the engine schedules onto the Orker's
	# queue, so this answers only when the Orker is actually polling. A
	# liveness check on the process alone would pass with an Orker that dialled
	# Temporal, failed, and is sitting there useless.
	wait_for "orker (via engine plugin discovery)" 60 \
		"$REPO/bin/orkctl" --server "$ENGINE_URL" plugin list

	say "== ui =="
	if [ ! -d "$REPO/ui/node_modules" ]; then
		say "  installing ui dependencies"
		(cd "$REPO/ui" && bun install >/dev/null)
	fi
	start ui env ENGINE_API_URL="$ENGINE_URL" \
		bash -c "cd '$REPO/ui' && exec bun run dev --port $UI_PORT --strictPort"
	wait_for "ui" 90 curl -sf -o /dev/null "$UI_URL/orkflows"

	say ""
	say "OrkStack is up."
	say "  UI             $UI_URL"
	say "  Engine API     $ENGINE_URL"
	say "  Temporal UI    http://localhost:8233"
	say "  MinIO console  http://localhost:9001  (orkstack / orkstack-secret)"
	say ""
	say "  logs           $RUN_DIR/{ork,orker,ui}.log"
	say "  stop           make down"
}

cmd_down() {
	say "== stopping engine, orker and ui =="
	stop ui
	stop ork
	stop orker
	# A worker started by hand outlives its pidfile and steals activities from
	# every later run, so sweep by name too (AGENTS.md §6).
	pkill -f "$REPO/bin/orker" 2>/dev/null || true
	pkill -f "$REPO/bin/ork$" 2>/dev/null || true
	pkill -f "vite dev --port $UI_PORT" 2>/dev/null || true
	sleep 1

	say "== stopping infrastructure =="
	"${COMPOSE[@]}" down

	local stray
	stray="$(pgrep -fl "$REPO/bin/ork" 2>/dev/null || true)"
	[ -z "$stray" ] || die "processes survived: $stray"
	say "OrkStack is down. Data is kept; make reset wipes it."
}

cmd_status() {
	local any=0
	for name in orker ork ui; do
		if alive "$name"; then
			say "  $name    running (pid $(cat "$(pidfile "$name")"))"
			any=1
		else
			say "  $name    stopped"
		fi
	done
	say ""
	"${COMPOSE[@]}" ps --format 'table {{.Service}}\t{{.Status}}' 2>/dev/null || true
	[ "$any" -eq 1 ] && say "" && say "  UI $UI_URL   engine $ENGINE_URL"
	return 0
}

case "${1:-}" in
up) cmd_up ;;
down) cmd_down ;;
status) cmd_status ;;
*) die "usage: $(basename "$0") {up|down|status}" ;;
esac
