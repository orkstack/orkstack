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

# The Orker's per-attempt work directory. It holds materialized artifacts and
# the container plugin's ephemeral mounts, so it must be bind-mountable BY THE
# DOCKER DAEMON, not merely writable here — and when the daemon lives in a VM
# those are different things. The Orker's own default is os.TempDir(), which on
# macOS is /var/folders/... and which colima, Lima and Rancher Desktop do not
# share into the VM: the container plugin then dies with "bind source path does
# not exist" on a path that plainly exists. Every one of those VMs shares the
# user's home, and the repo is under it, so .data/ is the one location that
# works everywhere without asking. `make reset` wipes it with the rest of .data.
WORK_DIR="${ORK_ORKER_WORKDIR:-$REPO/.data/work}"

# The image the mount probe uses. It is the container example's own image, so a
# machine that can run the example already has it and the probe pulls nothing.
PROBE_IMAGE="${ORK_PROBE_IMAGE:-alpine:3.20}"

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

# check_workdir_mountable — warn when the daemon cannot bind-mount WORK_DIR.
#
# The container plugin needs the Orker's work directory to exist INSIDE the
# daemon's world. When the daemon runs in a VM it sees only the paths that VM
# shares, and Docker reports an unshared path as "bind source path does not
# exist" — which reads like a missing file and sends you looking in the wrong
# place entirely. Better to say it here, once, at startup, than to have it
# surface as a failed task twenty minutes into a run.
#
# Never fatal: the rest of the stack does not need Docker, and a machine with
# no daemon runs every other plugin perfectly well.
check_workdir_mountable() {
	command -v docker >/dev/null 2>&1 || return 0
	docker info >/dev/null 2>&1 || return 0
	# Do not pull. A machine that can run a containerized task already has an
	# image, and `make up` should not reach for the network to check a path.
	docker image inspect "$PROBE_IMAGE" >/dev/null 2>&1 || {
		say "  work dir $WORK_DIR (mount probe skipped: $PROBE_IMAGE not present locally)"
		return 0
	}
	local probe="$WORK_DIR/.mount-probe"
	mkdir -p "$probe"
	if docker run --rm --mount "type=bind,src=$probe,dst=/probe,readonly" \
		"$PROBE_IMAGE" true >/dev/null 2>&1; then
		rmdir "$probe" 2>/dev/null || true
		say "  work dir $WORK_DIR (daemon can bind-mount it)"
		return 0
	fi
	rmdir "$probe" 2>/dev/null || true
	say ""
	say "  WARNING: the docker daemon cannot bind-mount $WORK_DIR."
	say "  Tasks using the 'container' plugin will fail with"
	say "  \"bind source path does not exist\" even though the path is right here."
	say "  The daemon runs in a VM that does not share this path. Point"
	say "  ORK_ORKER_WORKDIR at a directory the VM does share — on macOS that is"
	say "  normally somewhere under \$HOME — and run 'make up' again."
	say "  colima: 'colima ssh -- mount | grep virtiofs' lists what it shares."
	say ""
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
	# The UI is served from one port and the engine listens on another, so every
	# WebSocket the browser opens is cross-origin. The engine's default is
	# same-origin only — right for production, fatal here: without this the
	# upgrade is refused, the run page never goes live and its event log stays
	# empty, while a refresh still works because that reads through the
	# same-origin Connect proxy instead of the socket.
	local ws_origins="${ORK_WS_ALLOWED_ORIGINS:-localhost:$UI_PORT,127.0.0.1:$UI_PORT}"
	mkdir -p "$WORK_DIR"
	check_workdir_mountable
	start orker env ORK_ARTIFACT_BACKEND="$backend" ORK_ORKER_WORKDIR="$WORK_DIR" \
		"$REPO/bin/orker"
	start ork env ORK_ARTIFACT_BACKEND="$backend" ORK_HTTP_ADDR="$ENGINE_ADDR" \
		ORK_WS_ALLOWED_ORIGINS="$ws_origins" "$REPO/bin/ork"
	wait_for "engine api" 60 "$REPO/bin/orkctl" --server "$ENGINE_URL" orkflow list
	# DescribeOrker is an activity the engine schedules onto the Orker's queue
	# (design §6.5), so a live count answers only when the Orker is actually
	# polling. A liveness check on the process alone would pass with an Orker
	# that dialled Temporal, failed, and is sitting there useless.
	#
	# The COUNT is what is waited on, not the command's exit status: ListPools
	# narrows rather than refuses, so it exits 0 for a pool with no Orker at
	# all. This replaces the `orkctl plugin list` probe, which went away with
	# PluginService.
	wait_for "orker (via the engine's DescribeOrker probe)" 60 \
		bash -c "\"$REPO/bin/orkctl\" --server \"$ENGINE_URL\" pool list --json | grep -q '\"liveOrkerCount\": [1-9]'"

	say "== ui =="
	if [ ! -d "$REPO/ui/node_modules" ]; then
		say "  installing ui dependencies"
		(cd "$REPO/ui" && bun install >/dev/null)
	fi
	start ui env ENGINE_API_URL="$ENGINE_URL" \
		bash -c "cd '$REPO/ui' && exec bun run dev --port $UI_PORT --strictPort"
	wait_for "ui" 90 curl -sf -o /dev/null "$UI_URL/orkflows"

	# The browser's own handshake, made the way the browser makes it. Serving a
	# page proves nothing about the socket that page opens: the UI can render
	# perfectly while every run sits there never going live. A refused upgrade
	# answers 403 here rather than 101.
	local ws_probe
	ws_probe="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 \
		-H 'Connection: Upgrade' -H 'Upgrade: websocket' \
		-H 'Sec-WebSocket-Version: 13' -H 'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==' \
		-H "Origin: $UI_URL" \
		"$ENGINE_URL/ws/runs/ork-probe-nonexistent?from_sequence=0" || true)"
	case "$ws_probe" in
	404) say "  websocket ready (origin $UI_URL accepted)" ;;
	403) die "the engine refuses websockets from $UI_URL (ORK_WS_ALLOWED_ORIGINS=$ws_origins); the UI would never go live" ;;
	*) die "unexpected websocket probe response: $ws_probe" ;;
	esac
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
