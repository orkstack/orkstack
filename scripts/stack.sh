#!/usr/bin/env bash
# The whole OrkStack dev stack, up or down in one command.
#
#   scripts/stack.sh up      infra + engine + orker + UI, then wait for health
#   scripts/stack.sh down    stop what up started (leaves data alone)
#   scripts/stack.sh status  what is running, and where
#
# Everything it starts is recorded in .run/: one pidfile and one log per
# process, each the leader of a process group of its own. down stops exactly
# those groups, then stops any other process whose executable IS this
# worktree's bin/ork or bin/orker (read from /proc/<pid>/exe, never matched by
# name or pattern), because a stray Orker started by hand steals activities
# from every later run and looks like a code bug. Nothing else on the host —
# another worktree's stack, a proof, a test's binaries — is touched
# (AGENTS.md §11.1).
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
# each activity container's workspace mount, so it must be bind-mountable BY THE
# DOCKER DAEMON, not merely writable here — and when the daemon lives in a VM
# those are different things. The Orker's own default is os.TempDir(), which on
# macOS is /var/folders/... and which colima, Lima and Rancher Desktop do not
# share into the VM: the container start then dies with "bind source path does
# not exist" on a path that plainly exists. Every one of those VMs shares the
# user's home, and the repo is under it, so .data/ is the one location that
# works everywhere without asking. `make reset` wipes it with the rest of .data.
WORK_DIR="${ORK_ORKER_WORKDIR:-$REPO/.data/work}"

# The image the mount probe uses. A small image most machines already hold, so
# the probe usually pulls nothing.
PROBE_IMAGE="${ORK_PROBE_IMAGE:-alpine:3.20}"

say() { printf '%s\n' "$*"; }
die() { printf '%s\n' "$*" >&2; exit 1; }

pidfile() { echo "$RUN_DIR/$1.pid"; }
logfile() { echo "$RUN_DIR/$1.log"; }

# worktree_binary_pids — the pids whose executable is this worktree's
# bin/ork or bin/orker, by identity: /proc/<pid>/exe resolved and compared
# exactly, so bin/orkctl, another worktree's binaries and a proof's
# proof/<name>/bin never match.
# Without /proc (macOS), ps's comm column is the executable's full path for a
# process started by its absolute path, and is compared just as exactly.
worktree_binary_pids() {
	local p exe pid
	if [ -e /proc/self/exe ]; then
		for p in /proc/[0-9]*; do
			exe="$(readlink "$p/exe" 2>/dev/null)" || continue
			exe="${exe% (deleted)}"
			case "$exe" in
			"$REPO/bin/ork" | "$REPO/bin/orker") echo "${p#/proc/}" ;;
			esac
		done
		return 0
	fi
	ps -axo pid=,comm= 2>/dev/null | while read -r pid exe; do
		case "$exe" in
		"$REPO/bin/ork" | "$REPO/bin/orker") echo "$pid" ;;
		esac
	done
}

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
		# Its own session and process group, so stop reaches what it spawns
		# (bun's vite) and nothing else. A background job of this
		# non-interactive shell is never a group leader, so setsid does not
		# fork and $! is the new group's leader. Without setsid (macOS) the
		# process shares this shell's group and stop signals its pid alone.
		if command -v setsid >/dev/null 2>&1; then
			setsid "$@" >"$(logfile "$name")" 2>&1 &
		else
			"$@" >"$(logfile "$name")" 2>&1 &
		fi
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

# stop NAME — terminate the recorded process group politely, then firmly. A
# pidfile written before the stack started each process in its own group
# names a plain pid; that pid alone is signalled.
stop() {
	local name="$1" pf pid target
	pf="$(pidfile "$name")"
	[ -f "$pf" ] || return 0
	pid="$(cat "$pf")"
	target="$pid"
	if [ "$(ps -o pgid= -p "$pid" 2>/dev/null | tr -d ' ')" = "$pid" ]; then
		target="-$pid"
	fi
	if kill -0 -- "$target" 2>/dev/null; then
		kill -TERM -- "$target" 2>/dev/null || true
		for _ in $(seq 1 40); do
			kill -0 -- "$target" 2>/dev/null || break
			sleep 0.25
		done
		kill -KILL -- "$target" 2>/dev/null || true
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
# Every activity container bind-mounts its workspace out of the Orker's work
# directory, so that directory has to exist INSIDE the daemon's world. When the
# daemon runs in a VM it sees only the paths that VM shares, and Docker reports
# an unshared path as "bind source path does not exist" — which reads like a
# missing file and sends you looking in the wrong place entirely. Better to say
# it here, once, at startup, than to have it surface as a failed task twenty
# minutes into a run.
#
# Never fatal: ork and the UI start without Docker; only task execution needs
# it.
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
	say "  Every task will fail with"
	say "  \"bind source path does not exist\" even though the path is right here."
	say "  The daemon runs in a VM that does not share this path. Point"
	say "  ORK_ORKER_WORKDIR at a directory the VM does share — on macOS that is"
	say "  normally somewhere under \$HOME — and run 'make up' again."
	say "  colima: 'colima ssh -- mount | grep virtiofs' lists what it shares."
	say ""
}

# local_activity_catalog — the ORK_ACTIVITY_CATALOG document for this machine.
#
# §4.6 of the OCI Activity Contract puts this decision here: `make -C images push`
# pushes each first-party activity to localhost:5001/orkstack/activity-<name>:dev
# and the dev stack points ORK_ACTIVITY_CATALOG at that registry. The
# BUILT-IN catalog keeps naming the published ghcr.io references, which is what
# a real deployment must resolve; the local stack overrides it rather than the
# default weakening to something that only exists on a developer's laptop.
#
# It prints nothing when the images are not in the local registry, so the
# engine falls back to the published defaults and an apply of `uses: shell`
# fails loudly at apply time (§4.3) instead of silently running something else.
local_activity_catalog() {
	local registry="${ORK_LOCAL_REGISTRY:-localhost:5001}" name doc=""
	for name in $(make -s -C "$REPO/images" print-activities); do
		curl -sfo /dev/null \
			-H 'Accept: application/vnd.oci.image.index.v1+json' \
			"http://$registry/v2/orkstack/activity-$name/manifests/dev" || return 0
		doc="$doc  $name: $registry/orkstack/activity-$name:dev
"
	done
	printf 'activities:\n%s' "$doc"
}

# wants_admin_addr — true when the operator exported anything that turns
# telemetry on (any OTEL_* variable, or ORK_ADMIN_ADDR itself). Only then does
# the generated config file gain a telemetry.admin_addr entry: a zero-config
# `make up` (nothing OTEL_*, no ORK_ADMIN_ADDR) must write a file that says
# nothing about telemetry at all, so its boot log and its absent admin
# listener stay byte-identical to before this file existed (docs/DEPLOY.md
# §7's zero-config promise, proven by scripts/proof-observability.sh's
# "zero-config control" phase).
wants_admin_addr() {
	[ -n "${ORK_ADMIN_ADDR:-}" ] && return 0
	env | grep -q '^OTEL_' && return 0
	return 1
}

# write_run_config BACKEND WORKDIR HTTP_ADDR WS_ORIGINS CATALOG — renders
# .run/orkstack.yaml from exactly the five values `up` already computes
# today (backend, workdir, http_addr, ws_allowed_origins, and the local
# activity catalog), plus telemetry.admin_addr when wants_admin_addr says so.
# Both binaries are started against this one file with --config; the
# ambient environment still wins over anything it sets
# (config.Document.Apply's precedence — docs/DEPLOY.md §1), so a variable
# exported by hand behaves exactly as it did before this file existed.
write_run_config() {
	local backend="$1" workdir="$2" http_addr="$3" ws_origins="$4" catalog="$5"
	mkdir -p "$RUN_DIR"
	# engine.ws_allowed_origins is a KindList leaf (internal/config/config.go):
	# the file wants an actual YAML sequence, not the comma-joined string the
	# ORK_WS_ALLOWED_ORIGINS environment variable itself takes — Load renders
	# a YAML list back to that same comma-joined form once loaded, so the two
	# spellings carry identical meaning, just not the same syntax.
	local ws_yaml_list="[]"
	if [ -n "$ws_origins" ]; then
		ws_yaml_list="[$(printf '%s' "$ws_origins" | sed 's/,/, /g')]"
	fi
	{
		echo "# Generated by scripts/stack.sh — edit the environment or the script,"
		echo "# not this file; 'make up' overwrites it every time."
		echo "storage:"
		echo "  backend: $backend"
		echo "orker:"
		echo "  workdir: $workdir"
		echo "engine:"
		echo "  http_addr: \"$http_addr\""
		echo "  ws_allowed_origins: $ws_yaml_list"
		if [ -n "$catalog" ]; then
			printf '%s\n' "$catalog"
		fi
		if wants_admin_addr; then
			echo "telemetry:"
			echo "  admin_addr: {ork: \"127.0.0.1:9464\", orker: \"127.0.0.1:9465\"}"
		fi
	} >"$RUN_DIR/orkstack.yaml"
}

cmd_up() {
	say "== infrastructure (Temporal, SeaweedFS) =="
	"${COMPOSE[@]}" up -d
	wait_for "temporal" 60 docker exec orkstack-temporal-1 \
		temporal operator cluster health --address 127.0.0.1:7233
	wait_for "object store (SeaweedFS)" 60 sh -c 'code=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:9000/); [ "$code" = 200 ] || [ "$code" = 403 ]'

	say "== binaries =="
	make build >/dev/null
	say "  bin/ork bin/orker bin/orkctl"

	say "== engine and orker =="
	# Both processes get the SAME artifact backend. The engine archives runs
	# and stores the orkflow catalogue through it, the Orker writes task
	# artifacts through it, and the default is local (./.data/artifacts). Set
	# it on one process only and the stack splits its brain: artifacts in
	# the object store, archived runs on local disk, and `make reset` wiping the volume
	# leaves runs that a fresh engine still lists.
	local backend="${ORK_ARTIFACT_BACKEND:-s3}"
	# The UI is served from one port and the engine listens on another, so every
	# WebSocket the browser opens is cross-origin. The engine's default is
	# same-origin only — right for production, fatal here: without this the
	# upgrade is refused, the run page never goes live and its event log stays
	# empty, while a refresh still works because that reads through the
	# same-origin Connect proxy instead of the socket.
	local ws_origins="${ORK_WS_ALLOWED_ORIGINS:-localhost:$UI_PORT,127.0.0.1:$UI_PORT}"
	local catalog="${ORK_ACTIVITY_CATALOG:-$(local_activity_catalog)}"
	if [ -z "$catalog" ]; then
		say "  no first-party activity images in the local registry;"
		say "  'uses: shell' will resolve against ghcr.io — run 'make -C images push'"
	fi
	mkdir -p "$WORK_DIR"
	check_workdir_mountable
	write_run_config "$backend" "$WORK_DIR" "$ENGINE_ADDR" "$ws_origins" "$catalog"
	start orker "$REPO/bin/orker" --config "$RUN_DIR/orkstack.yaml"
	start ork "$REPO/bin/ork" --config "$RUN_DIR/orkstack.yaml"
	wait_for "engine api" 60 "$REPO/bin/orkctl" --server "$ENGINE_URL" orkflow list
	# DescribeOrker is an activity the engine schedules onto the Orker's queue
	# (docs/OCI_ACTIVITY_CONTRACT.md §6.5), so a live count answers only when the Orker is actually
	# polling. A liveness check on the process alone would pass with an Orker
	# that dialled Temporal, failed, and is sitting there useless.
	#
	# The COUNT is what is waited on, not the command's exit status: ListPools
	# narrows rather than refuses, so it exits 0 for a pool with no Orker at
	# all.
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
	say "  SeaweedFS filer  http://localhost:8888  (S3 API: http://localhost:9000)"
	say ""
	say "  logs           $RUN_DIR/{ork,orker,ui}.log"
	say "  stop           make down"
}

cmd_down() {
	say "== stopping engine, orker and ui =="
	stop ui
	stop ork
	stop orker
	# A worker started by hand from this worktree's bin/ outlives any pidfile
	# and steals activities from every later run. It is found by the identity
	# of its executable, never by name or pattern (AGENTS.md §11.1).
	local pid stray
	for pid in $(worktree_binary_pids); do
		say "  stopping pid $pid: it runs this worktree's bin/ork or bin/orker, and make up did not start it"
		kill -TERM "$pid" 2>/dev/null || true
	done
	for _ in $(seq 1 40); do
		[ -n "$(worktree_binary_pids)" ] || break
		sleep 0.25
	done

	say "== stopping infrastructure =="
	"${COMPOSE[@]}" down

	stray="$(worktree_binary_pids | tr '\n' ' ')"
	[ -z "$stray" ] || die "processes running this worktree's bin/ork or bin/orker survived: pids $stray"
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
