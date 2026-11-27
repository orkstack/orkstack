#!/usr/bin/env bash
# The whole OrkStack dev stack, up or down in one command.
#
#   scripts/stack.sh up      infra + engine + orker + UI dev server, then wait for health
#   scripts/stack.sh down    stop what up started (leaves data alone)
#   scripts/stack.sh status  what is running, and where
#
# Everything it starts is recorded in .run/: one pidfile and one log per
# process, each the leader of a process group of its own. A pidfile holds the
# pid and the process's start time (/proc/<pid>/stat), and down stops exactly
# those groups, each only while its pid still has that start time, so a pid the
# kernel has since given another process is never signalled. Then it stops any
# other process whose executable IS this worktree's bin/ork or bin/orker (read
# from /proc/<pid>/exe, never matched by name or pattern), because a stray
# Orker started by hand steals activities from every later run and looks like
# a code bug. ork and orker make themselves undumpable, which hides
# /proc/<pid>/exe from their own user, so such a stray is found only when it
# runs with process.allow_debug or predates that: a process make up never
# recorded is otherwise not found. Nothing else on the host — another
# worktree's stack, a proof, a test's binaries — is touched (AGENTS.md §11.1).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
REPO="$PWD"
RUN_DIR="$REPO/.run"
COMPOSE=(docker compose -f "$REPO/deploy/docker-compose.yml")

# OBSERVABILITY=1 (make up-observability) adds the compose observability
# profile, its Collector, Loki, Tempo, Prometheus and Grafana, and points ork
# and orker at the Collector over OTLP: every signal is pushed, nothing is
# scraped, and no admin listener is opened. TASK_LOGS=1 also exports task
# logs. .run/observability records that this stack started the profile, so
# make down stops it only then (deploy/observability/README.md).
OBSERVABILITY="${OBSERVABILITY:-0}"
OBSERVABILITY_MARK="$RUN_DIR/observability"
observability_env() {
	export OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT:-http://127.0.0.1:4318}"
	export OTEL_METRICS_EXPORTER="${OTEL_METRICS_EXPORTER:-otlp}"
	export OTEL_LOGS_EXPORTER="${OTEL_LOGS_EXPORTER:-otlp}"
	export OTEL_TRACES_EXPORTER="${OTEL_TRACES_EXPORTER:-otlp}"
	export OTEL_METRIC_EXPORT_INTERVAL="${OTEL_METRIC_EXPORT_INTERVAL:-15000}"
	if [ "${TASK_LOGS:-0}" = 1 ]; then
		export ORK_ORKER_TASK_LOG_EXPORT=true
	fi
}

# Loopback, as deploy/docker-compose.yml publishes the infrastructure: the
# engine's API has no login, and only this host's own clients need it. The
# binary's own default stays every interface; ORK_HTTP_ADDR=:8080 make up asks
# for that here (CONTRIBUTING.md).
ENGINE_ADDR="${ORK_HTTP_ADDR:-127.0.0.1:8080}"
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

# The stack authenticates by default (CONTRIBUTING.md, "The development
# stack"), as a deployment does: the engine in enforce; Dex, where people sign
# in (deploy/dex/config.yaml's users); a development issuer `dev` whose key
# pair lives under .data/identity; and two tokens test/tools/devtoken signs
# afresh at every `make up`, each living thirty days: dev-admin, which
# auth.admins lists and orkctl presents through ORK_TOKEN_FILE, and
# dev-orker, the Orker's own, bound to the orker role at main by
# auth.orkers. IDENTITY=0 runs it asserted instead: no Dex, nothing verified.
IDENTITY="${IDENTITY:-1}"
case "$IDENTITY" in
0) IDENTITY="" ;;
1) ;;
*) echo "IDENTITY is 1 (the default: the stack authenticates) or 0 (asserted), not \"$IDENTITY\"" >&2; exit 2 ;;
esac
IDENTITY_DIR="$REPO/.data/identity"
DEV_ORKER_ID="dev-orker"
# OIDC_ADDR is where Dex, the development identity provider, is published
# (test/tools/dexrun, deploy/dex/config.yaml; its issuer is
# https://$OIDC_ADDR/dex).
OIDC_ADDR="${OIDC_ADDR:-127.0.0.1:5556}"
DEX_DIR="$IDENTITY_DIR/dex"

# With IDENTITY=1 the Orker has a storage key of its own, as an Orker of a
# deployment must for task identity (docs/DEPLOY.md, "An Orker's own storage
# credential"): a SeaweedFS identity named for this worktree, granted the
# documented Orker policy on this stack's bucket alone (test/tools/seaweedscope)
# through the shared store's own `s3.configure`, which changes no other
# identity and not the key the engine signs with. make down removes it. A
# bucket shaped like a proof's own (internal/testsupport/s3bucket) is refused.
STORE_BUCKET="${ORK_S3_BUCKET:-orkstack-artifacts}"
STORE_CONTAINER="orkstack-seaweedfs-1"
STORE_URL="http://localhost:9000"
ORKER_STORE_SECRET="$IDENTITY_DIR/orker-store.secret"
ORKER_STORE_MARK="$RUN_DIR/orker-store-user"
# TASK_AUDIENCE is the one token audience the default namespace allows, the
# one deploy/dev/identity.ork.yaml asks for.
TASK_AUDIENCE="dev.orkstack.invalid"

# The image the mount probe uses. A small image most machines already hold, so
# the probe usually pulls nothing.
PROBE_IMAGE="${ORK_PROBE_IMAGE:-alpine:3.24}"

say() { printf '%s\n' "$*"; }
die() { printf '%s\n' "$*" >&2; exit 1; }

pidfile() { echo "$RUN_DIR/$1.pid"; }
logfile() { echo "$RUN_DIR/$1.log"; }

# proc_identity PID — what tells this process from any other that ever had its
# pid: on Linux the boot's id and the process's start time (field 22 of
# /proc/<pid>/stat, which an undumpable process still shows its own user);
# without /proc (macOS), what ps says it started at. Empty when there is no
# such process, or it is a zombie, which has ended.
proc_identity() {
	local stat state start boot
	if [ -e /proc/self/stat ]; then
		stat="$(sed -E 's/^.*\) //' "/proc/$1/stat" 2>/dev/null)" || return 0
		read -r state _ <<<"$stat" || return 0
		[ -n "$state" ] && [ "$state" != Z ] || return 0
		start="$(awk '{print $20}' <<<"$stat")"
		boot="$(cat /proc/sys/kernel/random/boot_id 2>/dev/null)"
		[ -n "$start" ] && echo "$start@$boot"
	else
		ps -o lstart= -p "$1" 2>/dev/null | tr -s ' ' | tr ' ' '_'
	fi
	return 0
}

# recorded NAME — the pid NAME's pidfile records, when that pid is still the
# process it recorded: the same identity. Nothing otherwise, and nothing for a
# pidfile that holds no identity (one an earlier release wrote) or a pid that
# is not a positive number: what cannot be confirmed is never signalled.
recorded() {
	local pf pid ident
	pf="$(pidfile "$1")"
	[ -f "$pf" ] || return 1
	read -r pid ident <"$pf" || true
	[[ "${pid:-}" =~ ^[1-9][0-9]*$ ]] || return 1
	[ -n "${ident:-}" ] || return 1
	[ "$(proc_identity "$pid")" = "$ident" ] || return 1
	echo "$pid"
}

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

# alive NAME — true when the recorded process is still running: its pid, with
# the start time recorded with it.
alive() { recorded "$1" >/dev/null; }

# start NAME CMD... — run in the background, record pid and log, and confirm
# it is still alive a moment later. A process that exits during startup (the
# Orker losing a race to dial Temporal is the one seen in practice) must not
# be reported as started: "up" then lies, and the missing worker shows up much
# later as a run that never leaves RUNNING.
start() {
	local name="$1"
	shift
	if alive "$name"; then
		say "  $name already running (pid $(recorded "$name"))"
		return 0
	fi
	mkdir -p "$RUN_DIR"
	local attempt pid ident
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
		ident=""
		for _ in 1 2 3 4 5; do
			ident="$(proc_identity "$pid")"
			[ -z "$ident" ] && kill -0 "$pid" 2>/dev/null || break
			sleep 0.1
		done
		if [ -z "$ident" ] && kill -0 "$pid" 2>/dev/null; then
			# Running, but what would tell it apart later cannot be read: this
			# shell's own child, signalled at once, never left unrecorded.
			say "  $name started but its identity cannot be read (attempt $attempt/3); stopping it"
			kill -KILL "$pid" 2>/dev/null || true
		elif [ -n "$ident" ]; then
			echo "$pid $ident" >"$(pidfile "$name")"
			sleep 2
			if alive "$name"; then
				say "  $name started (pid $pid), log $(logfile "$name")"
				return 0
			fi
			# Still the process it started but not up: it goes, with its group
			# when it leads one, and only while it is confirmed. One that has
			# ended took its record with it; what its group left is not touched.
			if [ "$(proc_identity "$pid")" = "$ident" ]; then
				if [ "$(ps -o pgid= -p "$pid" 2>/dev/null | tr -d ' ')" = "$pid" ]; then
					kill -KILL -- "-$pid" 2>/dev/null || true
				else
					kill -KILL "$pid" 2>/dev/null || true
				fi
			fi
		fi
		say "  $name exited during startup (attempt $attempt/3)"
		sed -n '$p' "$(logfile "$name")" | sed 's/^/    /'
	done
	rm -f "$(pidfile "$name")"
	die "$name would not stay up; full log at $(logfile "$name")"
}

# stop NAME — terminate the recorded process group politely, then firmly,
# and signal it only while it is confirmed to be what make up started. Its
# leader still has the identity recorded with it (recorded), or, once the
# leader has ended, the group it led still has members: a pid that names a
# live group is never given to a new process, so that group is still the one
# make up started. A pid the kernel has given another process, a pidfile that
# records no identity, and a pid that is not a positive number are left
# alone. The Orker is given 15 minutes: its 60 s stop grace, then the
# hand-back of what still runs (docs/DEPLOY.md, "Stop timeouts"); an idle one
# exits at once.
stop() {
	local name="$1" pf pid ident group="" ticks=40 gone=""
	[ "$name" = orker ] && ticks=3600
	pf="$(pidfile "$name")"
	[ -f "$pf" ] || return 0
	read -r pid ident <"$pf" || true
	if recorded "$name" >/dev/null; then
		if [ "$(ps -o pgid= -p "$pid" 2>/dev/null | tr -d ' ')" = "$pid" ]; then
			group=1
		fi
	elif command -v setsid >/dev/null 2>&1 && [[ "${pid:-}" =~ ^[1-9][0-9]*$ ]] && [ -n "${ident:-}" ] &&
		[ -z "$(proc_identity "$pid")" ] && kill -0 -- "-$pid" 2>/dev/null; then
		# Its leader, which setsid made a group leader, has ended and the group
		# it led still has members (bun's vite): still what make up started,
		# since a pid that names a live group is never given to another
		# process. Without setsid (macOS) the recorded pid led no group.
		group=1
	else
		say "  $name is not running (its recorded pid is gone, now another process's, or unconfirmed; left alone)"
		rm -f "$pf"
		return 0
	fi
	stop_signal "$name" "$pid" "$group" TERM
	for _ in $(seq 1 "$ticks"); do
		stop_confirmed "$name" "$pid" "$group" || { gone=1; break; }
		sleep 0.25
	done
	if [ -z "$gone" ]; then
		stop_signal "$name" "$pid" "$group" KILL
		for _ in $(seq 1 20); do
			stop_confirmed "$name" "$pid" "$group" || { gone=1; break; }
			sleep 0.25
		done
	fi
	if [ -z "$gone" ]; then
		say "  $name still runs after SIGKILL (pid $pid); its pidfile is kept"
		return 1
	fi
	say "  $name stopped (pid $pid)"
	rm -f "$pf"
}

# stop_confirmed NAME PID GROUP — what stop may still signal is the process it
# recorded: its leader with the recorded identity, or, for a group whose
# leader has ended, that group while it has members (no process can hold the
# leader's pid meanwhile).
stop_confirmed() {
	recorded "$1" >/dev/null && return 0
	[ -n "$3" ] && [ -z "$(proc_identity "$2")" ] && kill -0 -- "-$2" 2>/dev/null
}

# stop_signal NAME PID GROUP SIG — SIG to the recorded group, or to its pid
# alone, and only while stop_confirmed says it is still the one recorded.
stop_signal() {
	stop_confirmed "$1" "$2" "$3" || return 0
	if [ -n "$3" ]; then
		kill "-$4" -- "-$2" 2>/dev/null || true
	else
		kill "-$4" "$2" 2>/dev/null || true
	fi
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
# Never fatal: ork and the UI dev server start without Docker; only task
# execution needs it.
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
	# The observability profile is pushed to; it needs no listener.
	[ "$OBSERVABILITY" = 1 ] && return 1
	env | grep -q '^OTEL_' && return 0
	return 1
}

# write_run_config BACKEND WORKDIR HTTP_ADDR WS_ORIGINS CATALOG — renders
# .run/orkstack.yaml from exactly the five values `up` computes (backend,
# workdir, http_addr, ws_allowed_origins — empty unless exported, which is the
# engine's same-origin default — and the local activity catalog), plus
# telemetry.admin_addr when wants_admin_addr says so.
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
		# The development store's key. No binary falls back to one, and the
		# engine reads its definitions from the store whatever the backend.
		echo "  s3:"
		if [ -n "$IDENTITY" ]; then
			# The engine's key, and the Orker's own (grant_orker_store_key).
			echo "    access_key: {ork: orkstack, orker: $(orker_store_user)}"
			echo "    secret_key: {ork: orkstack-secret, orker: {file: $ORKER_STORE_SECRET}}"
		else
			echo "    access_key: orkstack"
			echo "    secret_key: orkstack-secret"
		fi
		echo "orker:"
		echo "  workdir: $workdir"
		# A minute's stop grace, so `make down` lets a running task finish
		# briefly and never waits a day; stop() gives the Orker 15 minutes.
		echo "  stop:"
		echo "    grace: 60s"
		if [ -n "$IDENTITY" ]; then
			echo "  id: $DEV_ORKER_ID"
			echo "  engine:"
			echo "    url: $ENGINE_URL"
			echo "    auth:"
			echo "      method: token_file"
			echo "      token_file: $IDENTITY_DIR/orker.token"
		fi
		# A pin host, so examples/pinned-checkout.ork.yaml runs here as shipped.
		echo "  pins:"
		echo "    max_concurrent: 1"
		# The development stack delivers webhooks to local stubs.
		echo "webhooks:"
		echo "  allowed_networks: [127.0.0.0/8, \"::1/128\"]"
		echo "engine:"
		echo "  http_addr: \"$http_addr\""
		echo "  ws_allowed_origins: $ws_yaml_list"
		if [ -n "$catalog" ]; then
			printf '%s\n' "$catalog"
		fi
		if [ -n "$IDENTITY" ]; then
			echo "auth:"
			echo "  mode: enforce"
			echo "  admins:"
			echo "    - claims:dev:sub=dev-admin"
			echo "    - group:people:dev-admins"
			echo "  orkers:"
			echo "    - subject: claims:dev:sub=$DEV_ORKER_ID"
			echo "      pool: main"
			echo "  issuers:"
			echo "    dev:"
			echo "      issuer: orkstack-dev"
			echo "      kind: service"
			echo "      dedicated: true"
			echo "      audiences:"
			echo "        - orkstack"
			echo "      algorithms:"
			echo "        - ES256"
			echo "      max_lifetime: 720h"
			echo "      orker_id_claim: orker_id"
			echo "      jwks_file: $IDENTITY_DIR/dev-issuer.jwks.json"
			# People sign in to the UI and orkctl at Dex (the development
			# identity provider, deploy/dex/config.yaml's users).
			echo "    people:"
			echo "      issuer: https://$OIDC_ADDR/dex"
			echo "      kind: user"
			echo "      audiences:"
			echo "        - orkstack-web"
			echo "        - orkctl"
			echo "      algorithms:"
			echo "        - RS256"
			echo "      username_claim: email"
			echo "      require_verified_email: true"
			echo "      groups_claim: groups"
			echo "      max_lifetime: 2h"
			echo "      ca_file: $DEX_DIR/tls/ca.pem"
			echo "      login:"
			echo "        client_id: orkstack-web"
			echo "        client_secret:"
			echo "          source: file:$DEX_DIR/web-client.secret"
			echo "        cli_client_id: orkctl"
			echo "        device_flow: true"
			echo "        scopes:"
			echo "          - openid"
			echo "          - email"
			echo "          - groups"
			# Dex issues a refresh token only when the login asks for it.
			echo "          - offline_access"
			echo "  public_url: $UI_URL"
			echo "  signin:"
			echo "    key_files:"
			echo "      - $IDENTITY_DIR/signin.key"
			# enforce needs an engine signing key: it signs what one engine
			# hands another, and the task identity issuer's tokens. The
			# issuer is a name nothing resolves, so the engine serves its
			# discovery itself, on its own address, and never asks the issuer
			# for it. Nothing here caches the keys, so a new one signs at
			# once. The Orker's storage key is its own and scoped, so an
			# unscoped one is refused, as a deployment's default.
			echo "identity:"
			echo "  issuer: https://orkstack-dev.invalid"
			echo "  serve_discovery: true"
			echo "  signing:"
			echo "    key_files:"
			echo "      - $IDENTITY_DIR/engine-signing.key"
			echo "    algorithm: ES256"
			echo "    activation_delay: 0s"
			echo "    verify_published: false"
		fi
		if wants_admin_addr; then
			echo "telemetry:"
			echo "  admin_addr: {ork: \"127.0.0.1:9464\", orker: \"127.0.0.1:9465\"}"
		fi
	} >"$RUN_DIR/orkstack.yaml"
}

cmd_up() {
	if [ "$OBSERVABILITY" = 1 ]; then
		say "== infrastructure (Temporal, SeaweedFS) and the observability profile =="
		"${COMPOSE[@]}" --profile observability up -d --wait
		mkdir -p "$RUN_DIR"
		: >"$OBSERVABILITY_MARK"
		observability_env
		[ "${TASK_LOGS:-0}" = 1 ] && say "  task logs are exported too (TASK_LOGS=1): everything a task prints reaches Loki"
	else
		say "== infrastructure (Temporal, SeaweedFS) =="
		"${COMPOSE[@]}" up -d
	fi
	wait_for "temporal" 60 docker exec orkstack-temporal-1 \
		temporal operator cluster health --address 127.0.0.1:7233
	# The bucket, not just the port: on an empty volume the store serves S3 for
	# seconds before it creates its -bucket, and the engine and the Orker each
	# exit in that gap (deploy/docker-compose.yml).
	wait_for "object store (SeaweedFS)" 60 curl -fs -o /dev/null --head --aws-sigv4 aws:amz:us-east-1:s3 --user orkstack:orkstack-secret http://localhost:9000/orkstack-artifacts

	say "== binaries =="
	make build >/dev/null
	say "  bin/ork bin/orker bin/orkctl"
	if [ -n "$IDENTITY" ]; then
		setup_identity
		grant_orker_store_key
	fi

	say "== engine and orker =="
	# Both processes get the SAME artifact backend. The engine archives runs
	# and stores the orkflow catalogue through it, the Orker writes task
	# artifacts through it, and the default is local (./.data/artifacts). Set
	# it on one process only and the stack splits its brain: artifacts in
	# the object store, archived runs on local disk, and `make reset` wiping the volume
	# leaves runs that a fresh engine still lists.
	local backend="${ORK_ARTIFACT_BACKEND:-s3}"
	# The browser talks to one origin. Here that is the UI dev server, which
	# proxies the Connect calls, /v1/ and the WebSockets to the engine with the
	# browser's Host header intact (ui/vite.config.ts), so the engine sees each
	# socket's Origin match its Host and admits it under its same-origin
	# default, exactly as it admits the UI it serves itself. Nothing is allowed
	# beyond that unless ORK_WS_ALLOWED_ORIGINS is exported.
	local ws_origins="${ORK_WS_ALLOWED_ORIGINS:-}"
	local catalog="${ORK_ACTIVITY_CATALOG:-$(local_activity_catalog)}"
	if [ -z "$catalog" ]; then
		say "  no first-party activity images in the local registry;"
		say "  'uses: shell' will resolve against ghcr.io — run 'make -C images push'"
	fi
	mkdir -p "$WORK_DIR"
	check_workdir_mountable
	write_run_config "$backend" "$WORK_DIR" "$ENGINE_ADDR" "$ws_origins" "$catalog"
	if [ -n "$IDENTITY" ]; then
		# The Orker asks the engine who its credential is at boot, so the
		# engine comes first.
		start ork "$REPO/bin/ork" --config "$RUN_DIR/orkstack.yaml"
		wait_for "engine api" 60 "$REPO/bin/orkctl" --server "$ENGINE_URL" orkflow list
		start orker "$REPO/bin/orker" --config "$RUN_DIR/orkstack.yaml"
	else
		start orker "$REPO/bin/orker" --config "$RUN_DIR/orkstack.yaml"
		start ork "$REPO/bin/ork" --config "$RUN_DIR/orkstack.yaml"
		wait_for "engine api" 60 "$REPO/bin/orkctl" --server "$ENGINE_URL" orkflow list
	fi
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
	if [ -n "$IDENTITY" ]; then
		# The default namespace's tasks may ask for one audience; adding one
		# the policy holds already changes nothing.
		"$REPO/bin/orkctl" --server "$ENGINE_URL" namespace update default --audience "$TASK_AUDIENCE" >/dev/null ||
			die "the default namespace was not given the task audience $TASK_AUDIENCE"
		say "  the default namespace's tasks may hold an identity for $TASK_AUDIENCE"
	fi

	say "== ui (dev server) =="
	if [ ! -d "$REPO/ui/node_modules" ]; then
		say "  installing ui dependencies"
		(cd "$REPO/ui" && bun install >/dev/null)
	fi
	# ORK_SERVER names the engine the dev server proxies to, as it does for
	# orkctl.
	start ui env ORK_SERVER="$ENGINE_URL" \
		bash -c "cd '$REPO/ui' && exec bun run dev --port $UI_PORT --strictPort"
	wait_for "ui" 90 curl -sf -o /dev/null "$UI_URL/orkflows"

	# The browser's own handshake, made the way the browser makes it: to the
	# dev server, from its origin, so the probe crosses the dev server's proxy
	# to the engine as the run page's socket does. Serving a page proves
	# nothing about the socket that page opens: the UI can render perfectly
	# while every run sits there never going live. The probe's run does not
	# exist, so an accepted upgrade answers 404; a refused one answers 403.
	local ws_probe
	ws_probe="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 \
		-H 'Connection: Upgrade' -H 'Upgrade: websocket' \
		-H 'Sec-WebSocket-Version: 13' -H 'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==' \
		-H "Origin: $UI_URL" \
		"$UI_URL/ws/runs/ork-probe-nonexistent?from_sequence=0" || true)"
	case "$ws_probe" in
	404) say "  websocket ready (through the dev server, origin $UI_URL accepted)" ;;
	401) [ -n "$IDENTITY" ] || die "the engine asked the websocket probe for a credential, and IDENTITY=0 runs it asserted"
		say "  websocket reaches the engine, which asks for a credential (the UI signs in first)" ;;
	403) die "the engine refuses the websockets the dev server proxies from $UI_URL: the proxy must pass the browser's Host through (ui/vite.config.ts); the UI would never go live" ;;
	*) die "unexpected websocket probe response through the dev server: $ws_probe" ;;
	esac
	say ""
	say "OrkStack is up."
	[ "$OBSERVABILITY" = 1 ] && say "  Grafana        http://localhost:3000  (dashboards: OrkStack, OrkStack / Run)"
	say "  UI             $UI_URL  (the dev server, with hot reload)"
	say "  Engine         $ENGINE_URL  (the API; the UI too, once built in with make ui)"
	say "  Temporal UI    http://localhost:8233"
	say "  SeaweedFS filer  http://localhost:8888  (S3 API: http://localhost:9000)"
	say ""
	say "  logs           $RUN_DIR/{ork,orker,ui}.log"
	if [ -n "$IDENTITY" ]; then
		say ""
		say "  The engine enforces. Present the development admin's token to orkctl:"
		say "    export ORK_TOKEN_FILE=$IDENTITY_DIR/admin.token"
		say "  Both tokens live thirty days; make up signs them afresh."
		say "  Or sign in at $UI_URL as dev@example.test, password \"password\" (an admin; deploy/dex/config.yaml"
		say "  lists the other users), and in a terminal with"
		say "    orkctl --server $ENGINE_URL login --issuer people --issuer-ca-file $DEX_DIR/tls/ca.pem"
		say "  A task that holds an identity, and prints its token's claims:"
		say "    orkctl --server $ENGINE_URL orkflow create deploy/dev/identity.ork.yaml"
		say "    orkctl --server $ENGINE_URL run submit dev-identity"
	fi
	say "  stop           make down"
}

# setup_identity makes the development issuer's key pair and the engine's
# signing key once, and signs the
# admin's and the Orker's tokens afresh, and exports the admin's for the
# orkctl calls this script makes.
setup_identity() {
	say "== identity (IDENTITY=0 turns it off) =="
	(cd "$REPO" && go build -o "$REPO/bin/devtoken" ./test/tools/devtoken)
	"$REPO/bin/devtoken" init -dir "$IDENTITY_DIR"
	# The sign-in key: 32 random bytes, made once.
	mkdir -p "$IDENTITY_DIR"
	if [ ! -s "$IDENTITY_DIR/signin.key" ]; then
		(umask 077 && head -c 32 /dev/urandom >"$IDENTITY_DIR/signin.key")
	fi
	start_dex
	"$REPO/bin/devtoken" sign -dir "$IDENTITY_DIR" -iss orkstack-dev -aud orkstack -sub dev-admin \
		-ttl 720h -out "$IDENTITY_DIR/admin.token"
	"$REPO/bin/devtoken" sign -dir "$IDENTITY_DIR" -iss orkstack-dev -aud orkstack -sub "$DEV_ORKER_ID" \
		-claim "orker_id=$DEV_ORKER_ID" -ttl 720h -out "$IDENTITY_DIR/orker.token"
	export ORK_TOKEN_FILE="$IDENTITY_DIR/admin.token"
	say "  issuer dev, tokens under $IDENTITY_DIR"
}

# DEX_CID records the container start_dex started: stop_dex stops that
# container, and no other.
DEX_CID="$RUN_DIR/dex.cid"

# start_dex runs Dex, the development identity provider, from the image
# deploy/dex-image.txt pins (pulled here when the daemon lacks it) with the
# users and clients deploy/dex/config.yaml holds, through test/tools/dexrun,
# which makes its certificate and the web client's secret under $DEX_DIR
# once. A Dex this worktree started before is stopped first.
start_dex() {
	local image out id
	(cd "$REPO" && go build -o "$REPO/bin/dexrun" ./test/tools/dexrun)
	stop_dex
	image="$(grep -v '^#' "$REPO/deploy/dex-image.txt" | grep -v '^$' | head -n1)"
	[ -n "$image" ] || die "deploy/dex-image.txt names no image"
	if ! docker image inspect "$image" >/dev/null 2>&1; then
		say "  pulling $image"
		docker pull -q "$image" >/dev/null || die "cannot pull $image"
	fi
	out="$(cd "$REPO" && "$REPO/bin/dexrun" start -dir "$DEX_DIR" -addr "$OIDC_ADDR" \
		-redirect "$UI_URL/v1/auth/callback/people")" || die "Dex did not start (bin/dexrun start)"
	id="$(printf '%s' "$out" | sed -n 's/.*"id":"\([0-9a-f]*\)".*/\1/p')"
	[ -n "$id" ] || die "bin/dexrun printed no container id: $out"
	mkdir -p "$RUN_DIR"
	printf '%s\n' "$id" >"$DEX_CID"
	say "  identity provider (Dex) at https://$OIDC_ADDR/dex"
}

# stop_dex stops the Dex start_dex recorded, by its container id; a container
# that is already gone is not an error.
stop_dex() {
	local id
	[ -f "$DEX_CID" ] || return 0
	id="$(head -n1 "$DEX_CID")"
	if [ -n "$id" ] && docker container inspect "$id" >/dev/null 2>&1; then
		docker stop -t 5 "$id" >/dev/null || return 1
	fi
	rm -f "$DEX_CID"
}

# orker_store_user is this worktree's Orker's SeaweedFS identity and access
# key: one per worktree, so two worktrees' stacks never share or remove each
# other's.
orker_store_user() {
	local sum
	sum="$( (printf '%s' "$REPO" | sha256sum 2>/dev/null || printf '%s' "$REPO" | shasum -a 256) | cut -c1-12)"
	case "$sum" in
	[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]) ;;
	*) die "neither sha256sum nor shasum hashed the worktree's path" ;;
	esac
	printf 'orkstack-dev-orker-%s' "$sum"
}

# store_shell CMD runs one command of SeaweedFS's `weed shell` in the shared
# store's container, its input on stdin, so a secret is on no command line of
# this host. s3.configure reads, changes and writes the store's one identity
# list, so every worktree's change is made under one host-wide lock, where
# flock exists.
STORE_LOCK="${XDG_RUNTIME_DIR:-/tmp}/orkstack-store-identities.lock"
store_shell() {
	if command -v flock >/dev/null 2>&1; then
		printf '%s\n' "$1" | flock -w 60 "$STORE_LOCK" docker exec -i "$STORE_CONTAINER" weed shell
	else
		printf '%s\n' "$1" | docker exec -i "$STORE_CONTAINER" weed shell
	fi
}

# store_answer KEY SECRET is the HTTP status the store answers a signed read of
# a key under metadata/probes/ that does not exist: 404 to a key the Orker's
# policy grants, 403 to one it does not reach.
# store_listing is the store's identity list, s3.configure's own JSON. It
# holds every identity's secrets, so it is only ever piped to jq -e, whose
# answer is its exit status.
store_listing() {
	store_shell "s3.configure" 2>/dev/null | sed -n '/^{/,$p'
}

# The key is given to curl on stdin (-K -), never on its command line.
store_answer() {
	printf 'user = "%s:%s"\n' "$1" "$2" | curl -s -K - -o /dev/null -w '%{http_code}' \
		--aws-sigv4 aws:amz:us-east-1:s3 "$STORE_URL/$STORE_BUCKET/metadata/probes/make-up-$$-$RANDOM"
}

# grant_orker_store_key gives this worktree's Orker its own key, made once
# under .data/identity, with the documented policy on STORE_BUCKET, and waits
# until the store lists it with exactly that policy and honours it. weed
# shell's own exit status says little, so the store is asked. Granting the
# same again changes nothing.
grant_orker_store_key() {
	local user actions secret
	case "$STORE_BUCKET" in
	orkstack-*-[0-9]*-[0-9]*) die "ORK_S3_BUCKET=$STORE_BUCKET is shaped like a proof's own bucket; make up grants nothing on it" ;;
	esac
	user="$(orker_store_user)"
	(cd "$REPO" && go build -o "$REPO/bin/seaweedscope" ./test/tools/seaweedscope)
	actions="$("$REPO/bin/seaweedscope" -bucket "$STORE_BUCKET")" && [ -n "$actions" ] ||
		die "no SeaweedFS actions for bucket $STORE_BUCKET (bin/seaweedscope)"
	if [ ! -s "$ORKER_STORE_SECRET" ]; then
		(umask 077 && head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' >"$ORKER_STORE_SECRET")
	fi
	secret="$(cat "$ORKER_STORE_SECRET")"
	[ -n "$secret" ] || die "$ORKER_STORE_SECRET is empty: remove it, and make up makes another"
	mkdir -p "$RUN_DIR"
	# A key this worktree was granted under another name (it has moved) is
	# removed first, so none is left behind on the shared store.
	if [ -f "$ORKER_STORE_MARK" ] && [ "$(head -n1 "$ORKER_STORE_MARK")" != "$user" ]; then
		revoke_orker_store_key || die "the key this worktree was granted before, $(head -n1 "$ORKER_STORE_MARK"), was not removed"
	fi
	printf '%s\n' "$user" >"$ORKER_STORE_MARK"
	# Its output repeats the store's identities, secrets and all: never shown.
	store_shell "s3.configure -user=$user -access_key=$user -secret_key=$secret -actions=$actions -apply" >/dev/null 2>&1 ||
		die "the store did not take the Orker's key ($STORE_CONTAINER, weed shell s3.configure)"
	store_listing | jq -e --arg u "$user" --arg a "$actions" \
		'[.identities[]? | select(.name == $u)] | length == 1 and ((.[0].actions // []) | sort) == ($a | split(",") | sort)' >/dev/null ||
		die "the store does not list the Orker's key $user with the Orker policy on $STORE_BUCKET"
	local _
	for _ in $(seq 1 30); do
		if [ "$(store_answer "$user" "$secret")" = 404 ]; then
			say "  the Orker's own storage key $user, scoped to $STORE_BUCKET"
			return 0
		fi
		sleep 0.5
	done
	die "the store does not honour the Orker's key $user on $STORE_BUCKET"
}

# revoke_orker_store_key removes the key grant_orker_store_key recorded, and
# no other, while the store runs, and keeps the record until the store no
# longer lists it; with no record, or no running store, there is nothing to
# remove now.
revoke_orker_store_key() {
	local user
	[ -f "$ORKER_STORE_MARK" ] || return 0
	user="$(head -n1 "$ORKER_STORE_MARK")"
	# The record is this worktree's own, so the identity it names is too,
	# even where the worktree has since moved and is named another.
	case "$user" in
	orkstack-dev-orker-[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]) ;;
	*) say "  $ORKER_STORE_MARK names no key make up grants: left as it is"; return 1 ;;
	esac
	if [ "$(docker container inspect -f '{{.State.Running}}' "$STORE_CONTAINER" 2>/dev/null)" != true ]; then
		say "  the store is not running: the Orker's key $user stays, and make down removes it once the store runs"
		return 0
	fi
	store_shell "s3.configure -user=$user -delete -apply" >/dev/null 2>&1 || true
	if ! store_listing | jq -e --arg u "$user" '(.identities | type) == "array" and all(.identities[]; .name != $u)' >/dev/null; then
		say "  the store still lists the Orker's storage key $user"
		return 1
	fi
	rm -f -- "$ORKER_STORE_MARK"
	say "  the Orker's storage key $user removed"
}

cmd_down() {
	local survived=""
	say "== stopping engine, orker and ui =="
	stop ui || survived="$survived ui"
	stop ork || survived="$survived ork"
	stop orker || survived="$survived orker"
	stop_dex || survived="$survived dex"
	# A worker started by hand from this worktree's bin/ outlives any pidfile
	# and steals activities from every later run. It is found by the identity
	# of its executable, never by name or pattern (AGENTS.md §11.1), and only
	# where its own user may read it: an undumpable ork or orker that make up
	# did not start is not found.
	local pid stray
	for pid in $(worktree_binary_pids); do
		say "  stopping pid $pid: it runs this worktree's bin/ork or bin/orker, and make up did not start it"
		kill -TERM "$pid" 2>/dev/null || true
	done
	for _ in $(seq 1 40); do
		[ -n "$(worktree_binary_pids)" ] || break
		sleep 0.25
	done

	revoke_orker_store_key || survived="$survived orker-store-key"

	say "== stopping infrastructure =="
	if [ -e "$OBSERVABILITY_MARK" ]; then
		"${COMPOSE[@]}" --profile observability down
		rm -f -- "$OBSERVABILITY_MARK"
	else
		"${COMPOSE[@]}" down
	fi

	# What make up recorded is checked by its record; a stray by its exe,
	# which an undumpable one hides (AGENTS.md §11.1).
	[ -z "$survived" ] || die "what make up started survived:$survived (its record is kept in $RUN_DIR)"
	stray="$(worktree_binary_pids | tr '\n' ' ')"
	[ -z "$stray" ] || die "processes running this worktree's bin/ork or bin/orker survived: pids $stray"
	say "OrkStack is down. Data is kept; make reset wipes it."
}

cmd_status() {
	local any=0
	for name in orker ork ui; do
		if alive "$name"; then
			say "  $name    running (pid $(recorded "$name"))"
			any=1
		else
			say "  $name    stopped"
		fi
	done
	if [ -f "$DEX_CID" ] && docker container inspect "$(head -n1 "$DEX_CID")" >/dev/null 2>&1; then
		say "  dex      running (container $(head -c 12 "$DEX_CID"))"
	fi
	say ""
	"${COMPOSE[@]}" ps --format 'table {{.Service}}\t{{.Status}}' 2>/dev/null || true
	[ "$any" -eq 1 ] && say "" && say "  UI (dev server) $UI_URL   engine $ENGINE_URL"
	return 0
}

case "${1:-}" in
up) cmd_up ;;
down) cmd_down ;;
status) cmd_status ;;
*) die "usage: $(basename "$0") {up|down|status}" ;;
esac
