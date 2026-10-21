#!/usr/bin/env bash
# Produce the evidence for a change (AGENTS.md §11.2), in one of two tiers,
# with every gate's full output kept under one fresh, gitignored directory.
#
#   scripts/proof.sh [NAME]                     full proof    -> proof/NAME/
#   scripts/proof.sh --tier feature [--base REF] [--tests PKG:REGEX]... \
#                    [--specs FILE]... [NAME]   feature proof -> proof/NAME-feature/
#   scripts/proof.sh --tier nightly|release [NAME]
#   scripts/proof.sh --part PART --plan FILE ...  one CI part -> proof/<id>-part-PART-aN/
#
# A full, release or nightly proof runs its gates on one host and then writes
# one part (`local`) and merges it with `proofrun merge`, the same verdict the
# CI proof job runs over many parts. A --part invocation is one part of a CI
# proof: plan, build, fast, integration:K, ui-e2e:K, activity-fixture:A or
# observability. See docs/PROOFS.md.
#
# The FULL proof is the merge gate (§11.3) and runs everything: lint, build,
# unit, vet-integration, this tree's activity images pushed under a tag of the
# proof's own, every test of every integration package (as separate processes,
# PROOF_JOBS at a time), the activity contract fixture on those images, the UI
# type-check, unit and Playwright suites, and the observability gate whenever
# the tree differs from main (the branch it merges into) in a telemetry path.
#
# The FEATURE proof is a branch's evidence, never merge evidence: the fast
# gates, the change's evidence (the tests and specs that prove it) and its
# regression set, as test/tools/proofrun select decides from the diff against
# the merge base with --base (default main) and scripts/proof-scope.tsv.
#
# Exit status: 0 pass, 1 a gate failed, 2 an invalid invocation (an unknown
# base, an existing proof directory, missing evidence, a pattern that matches
# nothing, a path the scope table refuses), 77 a prerequisite is absent.
# Arguments and prerequisites are checked BEFORE any output file is opened, so
# a refusal never leaves behind anything that reads like evidence.
#
# Each proof runs in its own process group with its own binaries
# (proof/NAME/bin) and its own Temporal container; scripts/proof-stop.sh
# proof/NAME stops exactly those. Nothing here kills by name or pattern.
#
# Environment:
#   PROOF_JOBS=4        integration test processes at a time (1 = one after another)
#   PROOF_UI_WORKERS=1  Playwright workers; stays 1 until the browser runs in
#                       a network namespace of its own
#   PROOF_GATES=a,b     run only these gates — debugging only, never merge-eligible
#   PROOF_TEMPORAL=own  `shared` uses make infra-up's Temporal — debugging only,
#                       never merge-eligible
#   ORK_TEST_IMAGE_TAG  set from outside: resolve those activity images instead
#                       of pushing this tree's — never merge-eligible
#
# proof/ is gitignored on purpose: the evidence is regenerated per change and
# quoted in the commit or pull request; what the repository keeps is the
# reproduction — the tests, specs and this script.
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
REPO="$PWD"
SELF="$REPO/scripts/proof.sh"

# ---- its own session and process group ------------------------------------------
# A proof and everything it starts share one process group that holds nothing
# else — not the shell that started it, not the other commands of a pipeline
# it is part of — so stopping it stops exactly its own processes. The script
# re-runs itself in a new session and forwards INT, TERM and HUP to that
# group; asynchronous children of a non-interactive shell ignore SIGINT, so
# the forward is always a TERM.
if [ "${PROOF_SESSION:-}" != 1 ] || [ "$(ps -o sid= -p $$ | tr -d ' ')" != "$$" ]; then
	PROOF_SESSION=1 setsid "$SELF" "$@" &
	child=$!
	trap 'kill -TERM -- -"$child" 2>/dev/null || kill -TERM "$child" 2>/dev/null' INT TERM HUP
	while :; do
		wait "$child"
		status=$?
		kill -0 "$child" 2>/dev/null || break
	done
	exit "$status"
fi

say() { printf '%s\n' "$*"; }
invalid() {
	say "proof: $*" >&2
	exit 2
}
usage() { sed -n '2,39p' "$SELF" | sed 's/^# \{0,1\}//'; }

# ---- arguments (exit 2) -------------------------------------------------------
TIER=full
BASE=main
BASE_GIVEN=""
TESTS=()
SPECS=()
NAME=""
PART=""
PLAN=""
UI_PLAN=""
LAYOUTS=""
PROOF_ID=""
ATTEMPT=1
BASE_COMMIT=""
MERGE_BASE=""
SHARDS=""
UI_SHARDS=""
BASE_COMMIT_GIVEN=""
while [ $# -gt 0 ]; do
	case "$1" in
	--tier) [ $# -ge 2 ] || invalid "--tier needs a value"; TIER="$2"; shift 2 ;;
	--tier=*) TIER="${1#*=}"; shift ;;
	--base) [ $# -ge 2 ] || invalid "--base needs a value"; BASE="$2"; BASE_GIVEN=1; shift 2 ;;
	--base=*) BASE="${1#*=}"; BASE_GIVEN=1; shift ;;
	--tests) [ $# -ge 2 ] || invalid "--tests needs PKG:REGEX"; TESTS+=("$2"); shift 2 ;;
	--tests=*) TESTS+=("${1#*=}"); shift ;;
	--specs) [ $# -ge 2 ] || invalid "--specs needs a file"; SPECS+=("$2"); shift 2 ;;
	--specs=*) SPECS+=("${1#*=}"); shift ;;
	--part) [ $# -ge 2 ] || invalid "--part needs a name"; PART="$2"; shift 2 ;;
	--part=*) PART="${1#*=}"; shift ;;
	--plan) [ $# -ge 2 ] || invalid "--plan needs a file"; PLAN="$2"; shift 2 ;;
	--plan=*) PLAN="${1#*=}"; shift ;;
	--ui-plan) [ $# -ge 2 ] || invalid "--ui-plan needs a file"; UI_PLAN="$2"; shift 2 ;;
	--ui-plan=*) UI_PLAN="${1#*=}"; shift ;;
	--layouts) [ $# -ge 2 ] || invalid "--layouts needs a directory"; LAYOUTS="$2"; shift 2 ;;
	--layouts=*) LAYOUTS="${1#*=}"; shift ;;
	--proof-id) [ $# -ge 2 ] || invalid "--proof-id needs a value"; PROOF_ID="$2"; shift 2 ;;
	--proof-id=*) PROOF_ID="${1#*=}"; shift ;;
	--attempt) [ $# -ge 2 ] || invalid "--attempt needs a number"; ATTEMPT="$2"; shift 2 ;;
	--attempt=*) ATTEMPT="${1#*=}"; shift ;;
	--base-commit) [ $# -ge 2 ] || invalid "--base-commit needs a sha"; BASE_COMMIT="$2"; BASE_COMMIT_GIVEN=1; shift 2 ;;
	--base-commit=*) BASE_COMMIT="${1#*=}"; BASE_COMMIT_GIVEN=1; shift ;;
	--shards) [ $# -ge 2 ] || invalid "--shards needs a number"; SHARDS="$2"; shift 2 ;;
	--shards=*) SHARDS="${1#*=}"; shift ;;
	--ui-shards) [ $# -ge 2 ] || invalid "--ui-shards needs a number"; UI_SHARDS="$2"; shift 2 ;;
	--ui-shards=*) UI_SHARDS="${1#*=}"; shift ;;
	-h | --help) usage; exit 0 ;;
	-*) invalid "unknown option $1 (see --help)" ;;
	*)
		[ -z "$NAME" ] || invalid "one proof name only: $NAME, then $1"
		NAME="$1"
		shift
		;;
	esac
done
case "$TIER" in
full | feature | nightly | release) ;;
*) invalid "--tier must be full, feature, nightly or release, not '$TIER'" ;;
esac
if [ -n "$PART" ] && [ "$TIER" = feature ]; then
	invalid "--part is a full, release or nightly proof's part; a feature proof runs on one host"
fi
if [ "$TIER" = full ] && { [ ${#TESTS[@]} -gt 0 ] || [ ${#SPECS[@]} -gt 0 ]; }; then
	invalid "--tests and --specs name a feature proof's evidence; the full proof runs everything"
fi
# The full proof diffs against main, the branch a candidate fast-forwards:
# that diff decides whether the observability gate is due, so a base of the
# caller's choosing (HEAD, say) could leave it out of a merge-eligible proof.
if [ "$TIER" = full ] && [ -n "$BASE_GIVEN" ]; then
	invalid "--base chooses a feature proof's change set; the full proof always diffs against main, the branch it merges into"
fi
for tool in git go; do
	if ! command -v "$tool" >/dev/null 2>&1; then
		say "proof: prerequisites absent, nothing measured, nothing written:
  - $tool on PATH"
		exit 77
	fi
done
if [ -n "$PART" ] && [ "$PART" != plan ]; then
	# Only the plan part reads history. Every other part takes the base and
	# merge base the plan recorded; on a shallow CI checkout neither commit
	# exists locally, and checking them would refuse a correct part.
	[ -n "$BASE_COMMIT" ] || invalid "--part $PART needs --base-commit (the plan's base_commit)"
else
	if [ -n "$BASE_COMMIT" ]; then
		git rev-parse --verify --quiet "$BASE_COMMIT^{commit}" >/dev/null || invalid "--base-commit $BASE_COMMIT is not a commit"
		BASE_COMMIT="$(git rev-parse "$BASE_COMMIT^{commit}")"
		git merge-base --is-ancestor "$BASE_COMMIT" HEAD || invalid "--base-commit $BASE_COMMIT is not an ancestor of HEAD: the proof must test a tree that lands"
		BASE="$(git rev-parse --abbrev-ref "$BASE_COMMIT" 2>/dev/null || echo main)"
		[ "$BASE" = "HEAD" ] && BASE=main
	else
		git rev-parse --verify --quiet "$BASE^{commit}" >/dev/null || invalid "--base $BASE is not a commit"
		BASE_COMMIT="$(git rev-parse "$BASE^{commit}")"
	fi
	if [ -z "$MERGE_BASE" ]; then
		MERGE_BASE="$(git merge-base "$BASE_COMMIT" HEAD)" || invalid "no merge base between $BASE_COMMIT and HEAD"
	fi
fi
if [ -n "$PART" ] && [ "$PART" != plan ]; then
	BASE="${BASE:-main}"
fi

if [ -z "$NAME" ]; then
	NAME="$(git branch --show-current 2>/dev/null | tr '/' '-')"
	NAME="${NAME:-detached}-$(date -u +%Y%m%dT%H%M%SZ)"
fi
[[ "$NAME" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || invalid "proof name '$NAME' must be letters, digits, '.', '_' or '-'"
if [ "$TIER" = feature ] && [[ "$NAME" != *-feature ]]; then
	NAME="$NAME-feature"
fi
if [ -n "$PART" ]; then
	PART_SLUG="${PART//:/-}"
	DIR="$REPO/proof/$NAME-part-$PART_SLUG-a$ATTEMPT"
else
	DIR="$REPO/proof/$NAME"
fi
[ -e "$DIR" ] && invalid "proof/$(basename "$DIR") already exists: a proof never reuses a directory, so yesterday's output can never be mistaken for today's"

PROOF_JOBS="${PROOF_JOBS:-4}"
PROOF_UI_WORKERS="${PROOF_UI_WORKERS:-1}"
PROOF_TEMPORAL="${PROOF_TEMPORAL:-own}"
[[ "$PROOF_JOBS" =~ ^[1-9][0-9]*$ ]] || invalid "PROOF_JOBS must be a positive integer"
[[ "$PROOF_UI_WORKERS" =~ ^[1-9][0-9]*$ ]] || invalid "PROOF_UI_WORKERS must be a positive integer"
case "$PROOF_TEMPORAL" in own | shared) ;; *) invalid "PROOF_TEMPORAL must be own or shared" ;; esac
REGISTRY="${ORK_LOCAL_REGISTRY:-localhost:5001}"
if [ -z "$PROOF_ID" ]; then
	PROOF_ID="pr-$(printf '%s' "$DIR" | sha256sum | cut -c1-12)"
fi
SHARDS="${SHARDS:-1}"
UI_SHARDS="${UI_SHARDS:-1}"
[[ "$SHARDS" =~ ^[1-9][0-9]*$ ]] || invalid "--shards must be a positive integer"
[[ "$UI_SHARDS" =~ ^[1-9][0-9]*$ ]] || invalid "--ui-shards must be a positive integer"
[[ "$ATTEMPT" =~ ^[1-9][0-9]*$ ]] || invalid "--attempt must be a positive integer"
IMAGE_TAG_OWN=1
if [ -n "${ORK_TEST_IMAGE_TAG:-}" ]; then
	IMAGE_TAG="$ORK_TEST_IMAGE_TAG"
	IMAGE_TAG_OWN=""
else
	IMAGE_TAG="$PROOF_ID"
fi

# proofrun decides the selection before anything is written, so it is built
# first, into a temporary directory, and moved into the proof's bin/ later.
PR_TMP="$(mktemp -d)"
PR="$PR_TMP/proofrun"
if ! go build -o "$PR" ./test/tools/proofrun; then
	rm -rf "$PR_TMP"
	say "proof: cannot build test/tools/proofrun"
	exit 1
fi

# One part of a CI proof: its own host, its own gates, a part.json for the
# proof job's merger. It never decides a verdict (docs/PROOFS.md).
if [ -n "$PART" ]; then
	# shellcheck source=scripts/proof-part.sh
	. "$REPO/scripts/proof-part.sh"
	part_main "$PART"
	exit $?
fi

ALL_GATES="temporal binaries lint build unit vet-integration images integration activity-fixture ui-check ui-unit ui-e2e observability"
declare -A RUN=()
SPEC_ARGS=()
OBS_PATHS=""
SEL_TMP=""
cleanup_tmp() {
	if [ -n "$SEL_TMP" ]; then rm -f "$SEL_TMP"; fi
	if [ -n "$PR_TMP" ]; then rm -rf "$PR_TMP"; fi
}
trap cleanup_tmp EXIT

if [ "$TIER" = feature ]; then
	# The selection is part of the invocation: an unmapped path, missing
	# evidence, a pattern that matches nothing — each is exit 2, and nothing
	# has been written yet.
	SEL_TMP="$(mktemp)"
	sel_args=(-base "$BASE" -out "$SEL_TMP")
	for t in "${TESTS[@]}"; do sel_args+=(-tests "$t"); done
	for s in "${SPECS[@]}"; do sel_args+=(-specs "$s"); done
	"$PR" select "${sel_args[@]}"
	sel_status=$?
	[ "$sel_status" -eq 0 ] || exit "$sel_status"
	for g in lint build unit vet-integration; do RUN[$g]=1; done
	while IFS='=' read -r key value; do
		case "$key" in
		integration | ui_check | ui_unit | ui_e2e | activity_fixture | observability)
			if [ "$value" = yes ]; then RUN[${key//_/-}]=1; fi
			;;
		specs)
			if [ -n "$value" ] && [ "$value" != ALL ]; then
				read -r -a SPEC_ARGS <<<"$value"
			fi
			;;
		esac
	done < <("$PR" gates -select "$SEL_TMP")
else
	for g in lint build unit vet-integration images integration activity-fixture ui-check ui-unit ui-e2e; do RUN[$g]=1; done
	OBS_PATHS="$("$PR" touches -base "$BASE" -action observability)"
	case $? in
	0) RUN[observability]=1 ;;
	1) ;;
	*) invalid "cannot tell whether the tree touches a telemetry path" ;;
	esac
	if [ "$TIER" = nightly ]; then
		# The nightly tier runs the deferred tests and nothing else.
		RUN=()
		RUN[integration]=1
	fi
	if [ "$TIER" = release ]; then
		# The pre-release gate runs every gate, observability included.
		RUN[observability]=1
	fi
fi
if [ -n "${PROOF_GATES:-}" ]; then
	# A full proof runs exactly the gates named (and the setup they need); a
	# feature proof runs the named gates its selection has. Either way the
	# proof is not merge-eligible.
	for g in ${PROOF_GATES//,/ }; do
		[[ " $ALL_GATES " == *" $g "* ]] || invalid "PROOF_GATES names an unknown gate '$g' (gates: $ALL_GATES)"
	done
	if [ "$TIER" = full ]; then
		RUN=()
		for g in ${PROOF_GATES//,/ }; do RUN[$g]=1; done
	else
		for g in "${!RUN[@]}"; do
			[[ ",$PROOF_GATES," == *",$g,"* ]] || unset "RUN[$g]"
		done
	fi
fi
runs() { [ -n "${RUN[$1]:-}" ]; }
# What the stack gates need first: this tree's images, the binaries, a Temporal.
if runs integration || runs ui-e2e || runs activity-fixture || runs observability; then
	if [ -n "$IMAGE_TAG_OWN" ]; then RUN[images]=1; fi
fi
if runs integration || runs ui-e2e || runs observability; then
	RUN[binaries]=1
	RUN[temporal]=1
fi

# ---- prerequisites, decided before a single file is written (exit 77) -------
tcp_open() { (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null; }
store_health() {
	local code
	code="$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9000/ 2>/dev/null)"
	[ "$code" = 200 ] || [ "$code" = 403 ]
}
missing=""
need() { # <what> <command...>
	if ! "${@:2}" >/dev/null 2>&1; then
		missing="${missing}
  - $1"
	fi
}
MANIFEST_ACCEPT='Accept: application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json'
manifest() { # <activity> -> 0 when REGISTRY holds it under IMAGE_TAG
	curl -sf -o /dev/null -H "$MANIFEST_ACCEPT" "http://$REGISTRY/v2/orkstack/activity-$1/manifests/$IMAGE_TAG"
}
if runs temporal || runs images || runs activity-fixture; then
	need "curl on PATH" command -v curl
	need "a docker daemon (docker info)" docker info
	need "the dev object store on 127.0.0.1:9000 (SeaweedFS; make infra-up)" store_health
	need "the local registry on $REGISTRY (make infra-up)" curl -sf -o /dev/null "http://$REGISTRY/v2/"
fi
if runs temporal; then
	if [ "$PROOF_TEMPORAL" = shared ]; then
		need "Temporal on 127.0.0.1:7233 (make infra-up; PROOF_TEMPORAL=shared)" tcp_open 127.0.0.1 7233
	else
		need "docker compose (the proof's Temporal is deploy/docker-compose.yml's temporal service)" docker compose version
	fi
fi
if runs images; then
	need "make on PATH" command -v make
	need "docker buildx (make -C images push)" docker buildx version
elif runs temporal || runs activity-fixture; then
	for activity in $(make -s -C "$REPO/images" print-activities); do
		need "activity-$activity:$IMAGE_TAG in $REGISTRY (ORK_TEST_IMAGE_TAG was set from outside)" manifest "$activity"
	done
fi
if runs integration; then
	need "the cosign CLI on PATH (image-policy interop test)" command -v cosign
fi
if runs ui-check || runs ui-unit || runs ui-e2e; then
	need "bun on PATH" command -v bun
	need "ui/node_modules (cd ui && bun install)" test -d "$REPO/ui/node_modules"
fi
if runs ui-e2e; then
	need "flock on PATH (the Playwright gate's host lock)" command -v flock
	need "the temporal CLI on PATH (ui/e2e/admin.spec.ts induces a real cluster failure with it)" command -v temporal
fi
if runs observability; then
	need "flock on PATH (the observability gate's host lock)" command -v flock
	need "jq on PATH" command -v jq
	need "the OTel Collector on 127.0.0.1:4318 (make infra-up-observability)" \
		curl -s -o /dev/null http://127.0.0.1:4318/v1/logs
	need "Loki on 127.0.0.1:3100 (make infra-up-observability)" curl -sf -o /dev/null http://127.0.0.1:3100/ready
	need "Tempo on 127.0.0.1:3200 (make infra-up-observability)" curl -sf -o /dev/null http://127.0.0.1:3200/ready
	need "Prometheus on 127.0.0.1:9090 (make infra-up-observability)" curl -sf -o /dev/null http://127.0.0.1:9090/-/ready
fi
if [ -n "$missing" ]; then
	say "proof: prerequisites absent, nothing measured, nothing written:$missing"
	exit 77
fi

# ---- the evidence directory ---------------------------------------------------
mkdir -p "$REPO/proof"
mkdir "$DIR" 2>/dev/null || invalid "proof/$NAME already exists: a proof never reuses a directory"
mkdir "$DIR/artifacts" "$DIR/bin"
if [ -n "$SEL_TMP" ]; then
	mv "$SEL_TMP" "$DIR/selection.json"
	chmod 644 "$DIR/selection.json"
	SEL_TMP=""
fi
mv "$PR" "$DIR/bin/proofrun"
rm -rf "$PR_TMP"
PR_TMP=""
PR="$DIR/bin/proofrun"

proc_start() { sed -E 's/^.*\) //' "/proc/$1/stat" 2>/dev/null | awk '{print $20}'; }
printf '%s %s %s %s\n' "$$" "$$" "$(proc_start $$)" "$PROOF_ID" >"$DIR/proof.pid"

# tree_hash is the tree of the WORKING TREE — staged and untracked files
# included, ignored ones excluded — through a temporary index: the exact
# content the proof measured, whatever is committed.
tree_hash() {
	local idx
	idx="$(mktemp)"
	rm -f "$idx"
	GIT_INDEX_FILE="$idx" git read-tree HEAD &&
		GIT_INDEX_FILE="$idx" git add -A . &&
		GIT_INDEX_FILE="$idx" git write-tree
	rm -f "$idx"
}
is_clean() { [ -z "$(git status --porcelain --untracked-files=all)" ]; }
TREE_START="$(tree_hash)"
CLEAN_START=false
if is_clean; then CLEAN_START=true; fi
git status --short --untracked-files=all >"$DIR/worktree-start.txt" 2>&1
git diff --stat "$MERGE_BASE" >"$DIR/diff.stat" 2>/dev/null || true
START_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
LOAD_START="$(cut -d' ' -f1-3 /proc/loadavg 2>/dev/null)"

say "proof: $TIER proof -> $DIR (id $PROOF_ID, tree $TREE_START$([ "$CLEAN_START" = true ] || echo ', NOT clean'))"

# ---- cleanup of this proof's own group -------------------------------------------
TEMPORAL_NAME="orkstack-proof-$PROOF_ID-temporal"
TEMPORAL_STARTED=""
SAMPLER=""
IMAGES_BUILT=""
on_exit() {
	local code=$?
	trap '' TERM INT HUP
	if [ -n "$SAMPLER" ]; then kill "$SAMPLER" 2>/dev/null; fi
	if [ -n "$TEMPORAL_STARTED" ]; then docker stop "$TEMPORAL_NAME" >/dev/null 2>&1; fi
	# The daemon's copies of this proof's own activity-image tags. They are
	# this proof's alone (pr-<hash of its directory>), and the registry keeps
	# the pushed images, so a re-run against the tag still resolves them.
	if [ -n "$IMAGES_BUILT" ]; then
		local a
		for a in $(make -s -C "$REPO/images" print-activities 2>/dev/null); do
			docker image rm "$REGISTRY/orkstack/activity-$a:$IMAGE_TAG" >/dev/null 2>&1
		done
	fi
	# Anything still running in this proof's own process group: ours by
	# construction, and nothing else's.
	kill -TERM -- -$$ 2>/dev/null
	exit "$code"
}
trap on_exit EXIT
trap 'say "proof: stopped by a signal"; exit 143' TERM INT HUP

# The resident memory of this proof's process group, sampled.
(
	while :; do
		ps -o rss= -g $$ 2>/dev/null | awk -v t="$(date +%s)" '{s+=$1} END {print t "\t" s}'
		sleep 30
	done
) >"$DIR/rss.tsv" &
SAMPLER=$!

GATES_TSV="$DIR/gates.tsv"
: >"$GATES_TSV"
run() { # <gate> <log> <command...>
	local gate="$1" logname="$2" logfile="$DIR/$2"
	shift 2
	say "== $gate: $*"
	local started=$SECONDS result=pass code=0
	"$@" >"$logfile" 2>&1 || code=$?
	[ "$code" -eq 0 ] || result=fail
	printf '%s\t%s\t%s\t%s\t%s\n' "$gate" "$result" "$((SECONDS - started))" "$logname" "$*" >>"$GATES_TSV"
	if [ "$result" = pass ]; then
		say "   $gate: pass ($((SECONDS - started))s) -> $logfile"
	else
		say "   $gate: FAIL exit $code ($((SECONDS - started))s) -> $logfile"
		tail -n 40 "$logfile" | sed 's/^/      /'
	fi
	return "$code"
}
BG=()
run_bg() { # run, in the background; wait_bg collects them
	run "$@" &
	BG+=($!)
}
wait_bg() {
	local pid
	for pid in "${BG[@]}"; do wait "$pid"; done
	BG=()
}

# ---- P0: Temporal, images, binaries ---------------------------------------------
TEMPORAL_ADDRESS="127.0.0.1:7233"
TEMPORAL_IMAGE=""
SWEPT=""
# sweep_stale stops every proof Temporal whose owning proof process is gone:
# a proof that was SIGKILLed, or cut off by a usage limit, never ran its trap.
sweep_stale() {
	local id owner host rest pid start name
	for id in $(docker ps -q --filter label=orkstack.proof 2>/dev/null); do
		owner="$(docker inspect -f '{{index .Config.Labels "orkstack.proof.owner"}}' "$id" 2>/dev/null)"
		host="${owner%%:*}"
		rest="${owner#*:}"
		pid="${rest%%:*}"
		start="${rest#*:}"
		[ "$host" = "$(hostname)" ] || continue
		if [ -z "$pid" ] || [ "$(proc_start "$pid")" != "$start" ]; then
			name="$(docker inspect -f '{{.Name}}' "$id" 2>/dev/null)"
			name="${name#/}"
			if docker stop "$name" >/dev/null 2>&1; then
				say "proof: stopped stale $name (its proof, pid $pid, is gone)"
				SWEPT="$SWEPT $name"
			fi
		fi
	done
}
start_temporal() {
	local args port i
	if [ "$PROOF_TEMPORAL" = shared ]; then
		say "PROOF_TEMPORAL=shared: using 127.0.0.1:7233 — this proof is not merge-eligible"
		return 0
	fi
	sweep_stale
	mapfile -t args < <("$PR" temporal-args -compose deploy/docker-compose.yml)
	if [ ${#args[@]} -lt 1 ]; then
		say "no temporal service image in deploy/docker-compose.yml"
		return 1
	fi
	TEMPORAL_IMAGE="${args[0]}"
	say "starting $TEMPORAL_NAME: ${args[*]}"
	docker run -d --rm --name "$TEMPORAL_NAME" \
		--label "orkstack.proof=$PROOF_ID" \
		--label "orkstack.proof.owner=$(hostname):$$:$(proc_start $$)" \
		-p 127.0.0.1::7233 "${args[@]}" || return 1
	TEMPORAL_STARTED=1
	port="$(docker port "$TEMPORAL_NAME" 7233/tcp | head -n1 | awk -F: '{print $NF}')"
	if [ -z "$port" ]; then
		say "no published port for $TEMPORAL_NAME"
		return 1
	fi
	TEMPORAL_ADDRESS="127.0.0.1:$port"
	for i in $(seq 1 240); do
		if docker exec "$TEMPORAL_NAME" temporal operator cluster health --address 127.0.0.1:7233 2>/dev/null | grep -q SERVING; then
			say "Temporal $TEMPORAL_IMAGE serving at $TEMPORAL_ADDRESS ($(docker image inspect --format '{{index .RepoDigests 0}}' "$TEMPORAL_IMAGE" 2>/dev/null))"
			return 0
		fi
		sleep 0.5
	done
	say "$TEMPORAL_NAME never reported SERVING within 120s"
	docker logs "$TEMPORAL_NAME" 2>&1 | tail -n 40
	return 1
}
record_images() {
	local a digest
	: >"$DIR/images.tsv"
	for a in $(make -s -C images print-activities); do
		digest="$(curl -sfI -H "$MANIFEST_ACCEPT" "http://$REGISTRY/v2/orkstack/activity-$a/manifests/$IMAGE_TAG" |
			tr -d '\r' | awk -F': ' 'tolower($1)=="docker-content-digest" {print $2}')"
		if [ -z "$digest" ]; then
			say "activity-$a:$IMAGE_TAG has no digest in $REGISTRY"
			return 1
		fi
		printf '%s\t%s\t%s\n' "$a" "$REGISTRY/orkstack/activity-$a:$IMAGE_TAG" "$digest" | tee -a "$DIR/images.tsv"
	done
}
push_images() {
	make -C images push TAG="$IMAGE_TAG" REGISTRY="$REGISTRY" || return 1
	record_images
}
build_binaries() {
	local pkg
	for pkg in ./cmd/ork ./cmd/orker ./cmd/orkctl ./test/tools/cosign-sign ./test/tools/deaf-poller; do
		go build -o "$DIR/bin/$(basename "$pkg")" "$pkg" || return 1
		say "built $DIR/bin/$(basename "$pkg") from $pkg"
	done
}

if runs temporal; then run temporal temporal.log start_temporal; fi
if runs images; then
	IMAGES_BUILT=1
	run images images.log push_images
elif [ -z "$IMAGE_TAG_OWN" ] && { runs temporal || runs activity-fixture; }; then
	record_images >"$DIR/images.log" 2>&1
fi
if runs binaries; then run binaries binaries.log build_binaries; fi

# What every stack gate sees. The unit gate sees none of it.
STACK_ENV=(
	ORK_PROOF_ID="$PROOF_ID"
	ORK_PROOF_DIR="$DIR/artifacts"
	ORK_TEMPORAL_ADDRESS="$TEMPORAL_ADDRESS"
	ORK_TEST_BIN_DIR="$DIR/bin"
	ORK_TEST_IMAGE_TAG="$IMAGE_TAG"
	ORK_TEST_REGISTRY="$REGISTRY"
)

# ---- P1: the fast gates, alone ----------------------------------------------------
# The unit suites run with nothing else on the host's CPU from this proof:
# mocked-environment tests can be load-sensitive.
#
# The gate lists are anchored to the base (AGENTS.md §11.2): an entry the
# change itself adds, or removes, excuses nothing until the base carries it.
git show "$BASE_COMMIT:scripts/proof-nightly-tests.txt" >"$DIR/nightly-base.txt" 2>/dev/null || : >"$DIR/nightly-base.txt"
git show "$BASE_COMMIT:scripts/proof-quarantine.txt" >"$DIR/quarantine-base.txt" 2>/dev/null || : >"$DIR/quarantine-base.txt"
git show "$BASE_COMMIT:scripts/proof-host-speed.tsv" >"$DIR/host-speed-base.txt" 2>/dev/null || : >"$DIR/host-speed-base.txt"
# The gate files this change edits, and the rows its excusing lists add or
# remove (plan §6.4), computed whatever gates run. A failure is written into
# the file as an error entry, never as "no gate file changed", and makes the
# proof ineligible.
if ! "$PR" touches -base "$BASE_COMMIT" -gate-files >"$DIR/lists-changed.json" 2>"$DIR/lists-changed.log"; then
	say "WARNING: the gate files this change edits could not be listed: $(cat "$DIR/lists-changed.log")"
fi
if runs lint; then run lint lint.log make lint; fi
if runs build; then run build build.log go build ./...; fi
if runs unit; then
	run unit unit.log env ORK_PROOF_DIR="$DIR/artifacts" "$PR" unit -out "$DIR" \
		-quarantine scripts/proof-quarantine.txt -quarantine-base "$DIR/quarantine-base.txt"
fi
if runs vet-integration; then run vet-integration vet-integration.log go vet -tags=integration ./...; fi

# ---- P2/P3: integration, with the Docker-only and UI-only gates beside it ---------
in_ui() { (cd "$REPO/ui" && "$@"); }
if runs activity-fixture; then
	run_bg activity-fixture activity-fixture.log make -C images test TAG="$IMAGE_TAG" REGISTRY="$REGISTRY"
fi
if runs ui-check; then run_bg ui-check ui-check.log in_ui bun run check; fi
if runs ui-unit; then run_bg ui-unit ui-unit.log in_ui bun test test/; fi
if runs integration; then
	integ_args=(-out "$DIR" -jobs "$PROOF_JOBS" -namespace-prefix "$PROOF_ID" -tier "$TIER" -proof-id "$PROOF_ID" -base-commit "$BASE_COMMIT" -merge-base "$MERGE_BASE" -print-progress -quarantine scripts/proof-quarantine.txt -quarantine-base "$DIR/quarantine-base.txt" -host-speed scripts/proof-host-speed.tsv -host-speed-base "$DIR/host-speed-base.txt" -lists-changed "$DIR/lists-changed.json")
	case "$TIER" in
	full) integ_args+=(-nightly-mode defer -nightly-base "$DIR/nightly-base.txt") ;;
	release) integ_args+=(-nightly-mode include) ;;
	nightly) integ_args+=(-nightly-mode only) ;;
	esac
	if [ "$TIER" = feature ]; then integ_args+=(-select "$DIR/selection.json"); fi
	if { [ -n "$OBS_PATHS" ] && [ "$TIER" = full ]; } || [ "$TIER" = release ]; then integ_args+=(-observability-required); fi
	# A skip is not a pass: ORK_REQUIRE_INFRA turns every prerequisite skip
	# into a failure now that the prerequisites were probed above, and the
	# ledger fails any skip scripts/proof-allowed-skips.txt does not name.
	run integration integration.log env "${STACK_ENV[@]}" ORK_REQUIRE_INFRA=1 "$PR" integration "${integ_args[@]}"
fi
wait_bg

# ---- P4: Playwright ------------------------------------------------------------------
# Spec files still listen on fixed ports, share the development bucket's
# deployment-wide documents, and drive a browser in the host's network
# namespace (AGENTS.md §11.1), so two proofs' specs must never overlap. A
# host-wide lock enforces it: a second proof waits here, rather than running
# beside the first and failing on a port, a document or a network change.
UI_LOCK="${XDG_RUNTIME_DIR:-/tmp}/orkstack-ui-e2e.lock"
ui_e2e() {
	{
		if ! flock -w 7200 9; then
			say "another proof's Playwright specs held $UI_LOCK for two hours"
			return 1
		fi
		say "holding $UI_LOCK: one proof's Playwright specs at a time on this host"
		ui_e2e_locked
	} 9>"$UI_LOCK"
}
ui_e2e_locked() {
	local pw="$REPO/ui/node_modules/.bin/playwright" code=0
	if ! (cd "$REPO/ui" && env "${STACK_ENV[@]}" ORK_PROOF_DIR="$DIR/artifacts/ui" \
		"$pw" test --list --reporter=json "${SPEC_ARGS[@]}" >"$DIR/ui-e2e-list.json"); then
		say "playwright test --list failed"
		return 1
	fi
	"$PR" ui-plan -list "$DIR/ui-e2e-list.json" -costs scripts/proof-ui-costs.tsv \
		-shards 1 -out "$DIR/ui-plan.json" >>"$DIR/ui-e2e.log" 2>&1 || { say "proofrun ui-plan failed"; return 1; }
	(cd "$REPO/ui" && env "${STACK_ENV[@]}" ORK_PROOF_DIR="$DIR/artifacts/ui" \
		PLAYWRIGHT_JSON_OUTPUT_FILE="$DIR/ui-e2e-report.json" \
		PLAYWRIGHT_JSON_OUTPUT_NAME="$DIR/ui-e2e-report.json" \
		"$pw" test --workers "$PROOF_UI_WORKERS" --reporter=list,json "${SPEC_ARGS[@]}") || code=1
	"$PR" ui-ledger -list "$DIR/ui-e2e-list.json" -report "$DIR/ui-e2e-report.json" \
		-log "$DIR/ui-e2e.log" -config "$REPO/ui/playwright.config.ts" -out "$DIR" \
		-part local -host-speed scripts/proof-host-speed.tsv -host-speed-base "$DIR/host-speed-base.txt" || code=1
	return "$code"
}
if runs ui-e2e; then run ui-e2e ui-e2e.log ui_e2e; fi

# ---- P5: observability ------------------------------------------------------------------
if runs observability; then
	# One real run through ork and orker with telemetry switched on; every
	# signal it should have produced is queried back out of Loki, Tempo and
	# Prometheus and asserted. Its admin ports are fixed (Prometheus scrapes
	# them), so the script holds a host-wide lock.
	run observability observability.log env "${STACK_ENV[@]}" scripts/proof-observability.sh
fi

# ---- P6: the verdict ----------------------------------------------------------------------
kill "$SAMPLER" 2>/dev/null
SAMPLER=""
TREE_END="$(tree_hash)"
CLEAN_END=false
if is_clean; then CLEAN_END=true; fi
LOAD_END="$(cut -d' ' -f1-3 /proc/loadavg 2>/dev/null)"
PEAK_RSS_MB="$(awk -F'\t' '$2>m {m=$2} END {printf "%.0f", m/1024}' "$DIR/rss.tsv" 2>/dev/null)"

record_args=(
	-dir "$DIR" -tier "$TIER" -name "$NAME"
	-commit "$(git rev-parse HEAD)" -branch "$(git branch --show-current)"
	-tree-start "$TREE_START" -tree-end "$TREE_END"
	-clean-start="$CLEAN_START" -clean-end="$CLEAN_END"
	-base-ref "$BASE" -base-commit "$BASE_COMMIT" -merge-base "$MERGE_BASE"
	-jobs "$PROOF_JOBS" -ui-workers "$PROOF_UI_WORKERS"
)
if runs temporal; then
	record_args+=(-temporal "$PROOF_TEMPORAL" -temporal-image "$TEMPORAL_IMAGE" -temporal-address "$TEMPORAL_ADDRESS")
fi
if [ -f "$DIR/images.tsv" ]; then
	record_args+=(-image-tag "$IMAGE_TAG" -images "$DIR/images.tsv")
	if [ -n "$IMAGE_TAG_OWN" ]; then record_args+=(-image-tag-own); fi
fi
if [ -n "${PROOF_GATES:-}" ]; then record_args+=(-gates-override "$PROOF_GATES"); fi
if { [ "$TIER" = full ] && [ -n "$OBS_PATHS" ]; } || [ "$TIER" = release ]; then record_args+=(-observability-required); fi

README="$DIR/README.md"
if [ "$TIER" != feature ]; then
	# The full, release and nightly tiers have one verdict path: one part,
	# merged by the same code the CI proof job runs over many (plan §2.7).
	mkdir -p "$DIR/parts/local-a1"
	record_args+=(-part local -attempt 1 -out-dir "$DIR/parts/local-a1" -plan "$DIR/integration-plan.json")
	if [ -f "$DIR/ui-plan.json" ]; then record_args+=(-ui-plan "$DIR/ui-plan.json"); fi
	"$PR" record "${record_args[@]}" >"$DIR/.part-record.txt" 2>&1
	record_status=$?
	merge_args=(-plan "$DIR/integration-plan.json" -parts "$DIR/parts" -out "$DIR" -name "$NAME")
	if [ -f "$DIR/ui-plan.json" ]; then merge_args+=(-ui-plan "$DIR/ui-plan.json"); fi
	"$PR" merge "${merge_args[@]}" >"$DIR/.merge.txt" 2>&1
	status=$?
	[ "$record_status" -eq 0 ] || status=1
	[ "$status" -le 1 ] || status=1
	say
	say "proof: $(tail -n1 "$README") -> $README"
	exit "$status"
fi
record_args+=(-select "$DIR/selection.json")
VERDICT="$DIR/.verdict.md"
"$PR" record "${record_args[@]}" >"$VERDICT"
status=$?
[ "$status" -le 1 ] || status=1

{
	if [ "$TIER" = feature ]; then
		echo "SCOPED FEATURE PROOF — NOT MERGE EVIDENCE"
		echo
		echo "# Feature proof: $NAME"
	else
		echo "# Proof: $NAME"
	fi
	echo
	if [ "$TIER" = full ]; then
		echo "- tier:   full (the merge gate, AGENTS.md §11.3)"
	else
		echo "- tier:   feature (a branch's evidence, never merge evidence)"
	fi
	echo "- commit: $(git rev-parse HEAD)"
	echo "- tree:   $TREE_START (the working tree, staged and untracked files included); clean at start: $CLEAN_START; at end: $CLEAN_END, tree $TREE_END"
	echo "- base:   $BASE = $BASE_COMMIT; merge base $MERGE_BASE"
	echo "- branch: $(git branch --show-current 2>/dev/null || echo unknown)"
	echo "- date:   $START_DATE → $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "- host:   $(hostname) ($(uname -sm)), $(nproc 2>/dev/null) cores; load $LOAD_START → $LOAD_END; peak RSS of the proof's process group ${PEAK_RSS_MB:-?} MiB"
	echo "- go:     $(go version | cut -d' ' -f3)"
	echo "- proof id: $PROOF_ID; PROOF_JOBS=$PROOF_JOBS PROOF_UI_WORKERS=$PROOF_UI_WORKERS${PROOF_GATES:+ PROOF_GATES=$PROOF_GATES}"
	if runs temporal; then
		echo "- Temporal: $PROOF_TEMPORAL${TEMPORAL_IMAGE:+, $TEMPORAL_IMAGE} at $TEMPORAL_ADDRESS${SWEPT:+; stale proof containers stopped:$SWEPT}"
	fi
	if [ -f "$DIR/images.tsv" ]; then
		if [ -n "$IMAGE_TAG_OWN" ]; then
			echo "- activity images: tag $IMAGE_TAG, this proof's own, pushed from this tree"
		else
			echo "- activity images: tag $IMAGE_TAG, supplied from outside"
		fi
		awk -F'\t' '{print "  - " $2 " = " $3}' "$DIR/images.tsv"
	fi
	if [ "$TIER" = full ]; then
		if [ -n "$OBS_PATHS" ]; then
			echo "- observability gate: due — the tree touches telemetry paths since $BASE:"
			printf '%s\n' "$OBS_PATHS" | sed '/^$/d; s/^/  - /'
		else
			echo "- observability gate: not due — the tree touches no telemetry path since $BASE"
		fi
	fi
	echo
	echo "## Working tree at start"
	echo
	echo '```'
	cat "$DIR/worktree-start.txt"
	echo '```'
	echo
	echo "## Gates"
	echo
	while IFS=$'\t' read -r gate result seconds logname command; do
		echo "- **$gate**: $result in ${seconds}s — \`$command\` → \`$logname\`"
	done <"$GATES_TSV"
	for g in $ALL_GATES; do
		if ! runs "$g" && ! grep -q "^$g	" "$GATES_TSV"; then
			if [ -n "${PROOF_GATES:-}" ]; then
				echo "- **$g**: not run — PROOF_GATES"
			elif [ "$TIER" = feature ]; then
				echo "- **$g**: not run — outside this change's scope (see Scope)"
			else
				echo "- **$g**: not run — not due"
			fi
		fi
	done
	echo
	if [ "$TIER" = feature ]; then
		"$PR" feature-report -select "$DIR/selection.json" -dir "$DIR"
	fi
	echo "## Artifacts"
	echo
	echo '```'
	(cd "$DIR" && find . -type f -not -path './bin/*' -not -name '.verdict.md' | sort)
	echo '```'
	echo
	cat "$VERDICT"
} >"$README"
rm -f "$VERDICT"

say
say "proof: $(tail -n1 "$README") -> $README"
exit "$status"
