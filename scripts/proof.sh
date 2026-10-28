#!/usr/bin/env bash
# Produce the evidence for a change (AGENTS.md §11.2), in one of two tiers,
# with every gate's full output kept under one fresh, gitignored directory.
#
#   scripts/proof.sh [NAME]                     full proof    -> proof/NAME/
#   scripts/proof.sh --tier feature [--base REF] [--tests PKG:REGEX]... \
#                    [--specs FILE]... [NAME]   feature proof -> proof/NAME-feature/
#   scripts/proof.sh --tier nightly|release [NAME]
#   scripts/proof.sh --part PART --plan FILE [--setup-log FILE] ...
#                                               one CI part -> proof/<id>-part-PART-aN/
#
# A full, release or nightly proof runs its gates on one host and then writes
# one part (`local`) and merges it with `proofrun merge`, the same verdict the
# CI proof job runs over many parts. A --part invocation is one part of a CI
# proof: plan, build, fast, race, race-repeat, fuzz, integration:K, ui-e2e:K,
# activity-fixture:A or observability. See docs/PROOFS.md.
#
# The FULL proof is the merge gate (§11.3) and runs everything: lint, build,
# unit, vet-integration, this tree's activity images pushed under a tag of the
# proof's own, every test of every integration package (as separate processes,
# PROOF_JOBS at a time), the activity contract fixture on those images, the UI
# type-check, unit and Playwright suites, the site's check and build, the race
# gate (the unit suites under the race detector, -short), and the
# observability gate whenever the tree differs from main in a telemetry path.
#
# The FEATURE proof is a branch's evidence, never merge evidence: the fast
# gates, the change's evidence (the tests and specs that prove it) and its
# regression set, as test/tools/proofrun select decides from the diff against
# the merge base with --base (default main) and scripts/proof-scope.tsv; and
# the race gate over the packages the change touches, when it touches any.
#
# The NIGHTLY proof runs the deferred tests and spec files, and two lanes of
# its own: the race repeat (the concurrency-heavy packages' unit suites under
# the race detector, RACE_REPEAT_COUNT times) and the fuzz lane (every Fuzz
# target for FUZZTIME). Both run alone, before the integration phase.
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
#   PROOF_JOBS=6        integration test processes at a time (1 = one after another;
#                       measured clean at 6 on the proof host in two full
#                       proofs, docs/PROOFS.md §2.1)
#   PROOF_UI_WORKERS=2  Playwright workers: spec files at a time (the browser
#                       runs in a network namespace of its own, and every
#                       file owns its ports, bucket and namespace)
#   PROOF_GATES=a,b     run only these gates — debugging only, never merge-eligible
#   PROOF_TEMPORAL=own  `shared` uses make infra-up's Temporal — debugging only,
#                       never merge-eligible
#   FUZZTIME=2m         how long the nightly fuzz lane runs each Fuzz target: a
#                       whole number of s, m or h, or of iterations (100x)
#   PROOF_FAIL_FAST=1   stop at the first failing gate: the fast gates run first,
#                       one after another, and a failure in any gate stops the
#                       gates still running and starts no other. 0 runs every
#                       gate to its end, as before; it changes no eligibility rule
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
usage() { sed -n '2,46p' "$SELF" | sed 's/^# \{0,1\}//'; }

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
SETUP_LOG=""
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
	--setup-log) [ $# -ge 2 ] || invalid "--setup-log needs a file"; SETUP_LOG="$2"; shift 2 ;;
	--setup-log=*) SETUP_LOG="${1#*=}"; shift ;;
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
# The runner's setup log (.github/actions/proof-fetch): every fetch attempt
# made before a CI part, copied into the part's directory so its part.json and
# the merged proof carry it. A local proof fetches nothing before itself.
if [ -n "$SETUP_LOG" ] && [ -z "$PART" ]; then
	invalid "--setup-log is a CI part's record of its runner's setup; a local proof has none"
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

PROOF_JOBS="${PROOF_JOBS:-6}"
PROOF_UI_WORKERS="${PROOF_UI_WORKERS:-2}"
PROOF_TEMPORAL="${PROOF_TEMPORAL:-own}"
PROOF_FAIL_FAST="${PROOF_FAIL_FAST:-1}"
[[ "$PROOF_JOBS" =~ ^[1-9][0-9]*$ ]] || invalid "PROOF_JOBS must be a positive integer"
[[ "$PROOF_UI_WORKERS" =~ ^[1-9][0-9]*$ ]] || invalid "PROOF_UI_WORKERS must be a positive integer"
case "$PROOF_TEMPORAL" in own | shared) ;; *) invalid "PROOF_TEMPORAL must be own or shared" ;; esac
case "$PROOF_FAIL_FAST" in 0 | 1) ;; *) invalid "PROOF_FAIL_FAST must be 1 (the default) or 0" ;; esac
FUZZTIME="${FUZZTIME:-2m}"
[[ "$FUZZTIME" =~ ^[1-9][0-9]*(s|m|h|x)$ ]] || invalid "FUZZTIME must be a whole number of s, m or h, or of iterations (100x), not '$FUZZTIME'"
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

# The binaries every stack gate runs, in a local proof and in a CI part
# (scripts/proof-part.sh), linked by one `go build`: an -o that ends in a
# slash writes each main package's executable into that directory, named
# after its last path element (`go help build`), so the nine links share one
# build graph instead of nine.
STACK_BINARIES=(./cmd/ork ./cmd/orker ./cmd/orkctl ./test/tools/cosign-sign ./test/tools/deaf-poller ./test/tools/bucket-ensure ./test/tools/reclaim-run ./test/tools/generations ./test/tools/archive-legacy-run ./test/tools/archive-sized-run)

# The nightly race repeat (docs/PROOFS.md §2.3), local and in a CI part: the
# unit suites of the concurrency-heavy packages under the race detector,
# -short, RACE_REPEAT_COUNT times in one run, so a race that one run in several
# exposes is seen. The race gate itself runs every package once.
RACE_REPEAT_COUNT=3
RACE_REPEAT_PACKAGES=(./internal/engine/... ./internal/orker/... ./internal/api/... ./pkg/...)

# race_toolchain: the race detector links C code, so it needs cgo and a C
# compiler (go help build, -race). A gate that needs it refuses to start
# without, rather than failing in its first compile.
race_toolchain() {
	[ "$(go env CGO_ENABLED 2>/dev/null)" = 1 ] || return 1
	local cc
	cc="$(go env CC 2>/dev/null)"
	command -v "${cc%% *}" >/dev/null 2>&1
}

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

ALL_GATES="temporal binaries integration-compile lint build unit vet-integration images integration activity-fixture ui-check ui-unit site race race-repeat fuzz ui-e2e observability"
declare -A RUN=()
SPEC_ARGS=()
RACE_PACKAGES=()
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
		integration | ui_check | ui_unit | site | ui_e2e | activity_fixture | observability)
			if [ "$value" = yes ]; then RUN[${key//_/-}]=1; fi
			;;
		race_packages)
			# The race gate runs for the packages the change touches, and
			# only when it touches any.
			if [ -n "$value" ]; then
				RUN[race]=1
				read -r -a RACE_PACKAGES <<<"$value"
			fi
			;;
		specs)
			if [ -n "$value" ] && [ "$value" != ALL ]; then
				read -r -a SPEC_ARGS <<<"$value"
			fi
			;;
		esac
	done < <("$PR" gates -select "$SEL_TMP")
else
	for g in lint build unit vet-integration images integration activity-fixture ui-check ui-unit site race ui-e2e; do RUN[$g]=1; done
	OBS_PATHS="$("$PR" touches -base "$BASE" -action observability)"
	case $? in
	0) RUN[observability]=1 ;;
	1) ;;
	*) invalid "cannot tell whether the tree touches a telemetry path" ;;
	esac
	if [ "$TIER" = nightly ]; then
		# The nightly tier runs the deferred tests and spec files and its two
		# lanes, and nothing else: the integration tests
		# scripts/proof-nightly-tests.txt names, the Playwright specs
		# scripts/proof-nightly-specs.txt names, the race repeat and the
		# fuzz lane.
		RUN=()
		RUN[integration]=1
		RUN[race-repeat]=1
		RUN[fuzz]=1
		mapfile -t SPEC_ARGS < <(grep -v '^[[:space:]]*#' scripts/proof-nightly-specs.txt 2>/dev/null |
			awk -F'\t' 'NF >= 4 && $1 != "" { sub(/^ui\//, "", $1); print $1 }')
		if [ ${#SPEC_ARGS[@]} -gt 0 ]; then RUN[ui-e2e]=1; fi
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
if runs integration; then RUN[integration-compile]=1; fi

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
# The unit gate needs it too: proofrun's own tests run go test -race, and a
# feature proof whose change touches no Go package races nothing and still runs
# the unit gate.
if runs race || runs race-repeat || runs unit; then
	need "cgo for the race detector: CGO_ENABLED=1 and a C compiler on PATH (go env CC)" race_toolchain
fi
if runs ui-check || runs ui-unit || runs ui-e2e || runs site; then
	need "bun on PATH" command -v bun
fi
if runs ui-check || runs ui-unit || runs ui-e2e; then
	need "ui/node_modules (cd ui && bun install)" test -d "$REPO/ui/node_modules"
fi
if runs site; then
	# The site's scripts (docs sync, token drift check, link check) and astro
	# itself are Node programs that bun starts.
	need "node on PATH (the site's scripts and astro run on it)" command -v node
	need "site/node_modules (cd site && bun install --frozen-lockfile)" test -d "$REPO/site/node_modules"
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

# ---- what dead proofs left -----------------------------------------------------
# A proof that was stopped or killed, and a failing test process, leave
# buckets that hold the object store's volumes, work roots and containers.
# Every proof removes them first, before its own directory exists: only what a
# process that is gone left, never what a running proof recorded (`proofrun
# clean`, docs/PROOFS.md §10). Housekeeping never fails the proof.
"$PR" clean -remove >"$PR_TMP/cleanup.log" 2>&1 || :
say "   $(grep -m1 '^cleanup:' "$PR_TMP/cleanup.log" || echo 'cleanup: did not finish (cleanup.log says why)')"

# ---- the evidence directory ---------------------------------------------------
mkdir -p "$REPO/proof"
mkdir "$DIR" 2>/dev/null || invalid "proof/$NAME already exists: a proof never reuses a directory"
mkdir "$DIR/artifacts" "$DIR/bin"
mv "$PR_TMP/cleanup.log" "$DIR/cleanup.log"
grep -m1 '^cleanup:' "$DIR/cleanup.log" >"$DIR/setup.log" || echo 'cleanup: did not finish (cleanup.log says why)' >"$DIR/setup.log"
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

# The runner-health measurement a CI part makes first (scripts/proof-part.sh,
# AGENTS.md §11.2), made here for information only: the same numbers,
# appended to setup.log (after the cleanup's summary line, which opens it)
# and written to runner-health.json, recorded in proof.json. The class is
# `local`, which scripts/proof-runner-floors.tsv names no floor for, because
# this host's floors are its measured costs themselves; it never changes a
# local verdict, and a measurement that cannot run is reported, not failed.
say "== runner-health: class local (information only)"
"$PR" runner-health -class local -floors scripts/proof-runner-floors.tsv -out "$DIR/runner-health.json" >>"$DIR/setup.log" 2>&1 ||
	say "   runner-health: the measurement could not run (exit $?); see $DIR/setup.log"
grep '^runner-health:' "$DIR/setup.log" | sed 's/^/   /'

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
	# The Playwright suite's browser-server container (ui/e2e/browser-server.ts)
	# carries this proof's id; its global teardown stops it, unless the runner
	# was stopped first (fail-fast, a signal), in which case this does.
	docker ps -q --filter "label=orkstack.proof=$PROOF_ID" --filter label=orkstack.e2e=browser-server 2>/dev/null |
		xargs -r docker stop -t 5 >/dev/null 2>&1
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
	local started=$SECONDS result=pass code=0 shown="$*"
	if [ -n "${RUN_TRACK:-}" ]; then
		# A gate that runs beside another: its command's pid is recorded, so
		# stop_running_gates can end it, by that pid and nothing else, when
		# another gate fails.
		"$@" >"$logfile" 2>&1 &
		local cpid=$!
		printf '%s\n' "$cpid" >"$DIR/running.$gate"
		wait "$cpid" || code=$?
		rm -f "$DIR/running.$gate"
		if [ "$code" -ne 0 ] && [ -f "$DIR/stop.$gate" ]; then
			result=stopped
			shown="$(cat "$DIR/stop.$gate")"
		fi
	else
		"$@" >"$logfile" 2>&1 || code=$?
	fi
	if [ "$code" -ne 0 ] && [ "$result" != stopped ]; then result=fail; fi
	printf '%s\t%s\t%s\t%s\t%s\n' "$gate" "$result" "$((SECONDS - started))" "$logname" "$shown" >>"$GATES_TSV"
	case "$result" in
	pass) say "   $gate: pass ($((SECONDS - started))s) -> $logfile" ;;
	stopped) say "   $gate: $shown ($((SECONDS - started))s) -> $logfile" ;;
	*)
		say "   $gate: FAIL exit $code ($((SECONDS - started))s) -> $logfile"
		tail -n 40 "$logfile" | sed 's/^/      /'
		;;
	esac
	return "$code"
}

# Fail fast (PROOF_FAIL_FAST, on by default). A failed full proof can never
# become eligible — a proof is never rerun — so every minute after its first
# failing gate is wasted. FF_TRIGGER is that gate; once it is set no gate
# starts, the gates still running are stopped, and the verdict is written from
# what ran (the gates stopped or never started are recorded as such, and a
# proof that has one fails). With PROOF_FAIL_FAST=0 nothing here stops anything.
FF_TRIGGER=""
ff_check() {
	{ [ "$PROOF_FAIL_FAST" = 1 ] && [ -z "$FF_TRIGGER" ]; } || return 0
	FF_TRIGGER="$(awk -F'\t' '$2 == "fail" {print $1; exit}' "$GATES_TSV")"
	if [ -n "$FF_TRIGGER" ]; then
		say "proof: gate $FF_TRIGGER failed: no further gate starts, and the ones running are stopped (PROOF_FAIL_FAST=0 runs them all)"
	fi
}
step() { # <gate> <log> <command...>: a gate run alone, unless the proof has stopped
	runs "$1" || return 0
	[ -z "$FF_TRIGGER" ] || return 0
	run "$@"
	ff_check
}

BG=()
run_bg() { # run beside the others; wait_bg collects them
	(RUN_TRACK=1 run "$@") &
	BG+=($!)
}
step_bg() { # <gate> <log> <command...>: run_bg, unless the gate is not due or the proof has stopped
	runs "$1" || return 0
	[ -z "$FF_TRIGGER" ] || return 0
	run_bg "$@"
}
kill_tree() { # <pid>: it and its descendants, found by parent pid
	local child
	for child in $(ps -o pid= --ppid "$1" 2>/dev/null); do kill_tree "$child"; done
	kill -KILL "$1" 2>/dev/null
}
descendants() { # <pid>: the pids under it, found by parent pid
	local child
	for child in $(ps -o pid= --ppid "$1" 2>/dev/null); do
		printf '%s\n' "$child"
		descendants "$child"
	done
}
# stop_running_gates ends the gates running beside each other: SIGTERM to the
# command pid each recorded (proofrun integration, which then writes its
# ledger with every unfinished test as having no outcome, and make), a minute
# of grace, then SIGKILL to what is left of that pid's tree. A setup member is
# a shell function, whose subshell a SIGTERM ends at once while the docker or
# go command it waited on runs on, orphaned: so the pids under each command
# are found before the SIGTERM, and those still alive once it has ended are
# sent a SIGTERM of their own. Only pids this script recorded, or found under
# them by parent pid, are signalled — never a name or a pattern (AGENTS.md
# §11.1).
stop_running_gates() { # <failed gate>
	local f gate pid p k i alive pids=() kids=()
	for f in "$DIR"/running.*; do
		[ -e "$f" ] || continue
		gate="${f##*/running.}"
		pid="$(cat "$f" 2>/dev/null)"
		{ [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; } || continue
		printf 'stopped: %s failed\n' "$1" >"$DIR/stop.$gate"
		say "proof: stopping $gate (pid $pid): $1 failed"
		mapfile -t -O "${#kids[@]}" kids < <(descendants "$pid")
		kill -TERM "$pid" 2>/dev/null
		pids+=("$pid")
	done
	for i in $(seq 1 120); do
		alive=""
		for p in "${pids[@]}"; do
			if kill -0 "$p" 2>/dev/null; then alive=1; fi
		done
		[ -n "$alive" ] || break
		sleep 0.5
	done
	if [ -n "$alive" ]; then
		for p in "${pids[@]}"; do kill_tree "$p"; done
	fi
	for k in "${kids[@]}"; do kill -TERM "$k" 2>/dev/null; done
}
wait_bg() {
	local done_pid p rest
	while [ ${#BG[@]} -gt 0 ]; do
		done_pid=""
		wait -n -p done_pid "${BG[@]}" 2>/dev/null
		[ -n "$done_pid" ] || break
		rest=()
		for p in "${BG[@]}"; do
			if [ "$p" != "$done_pid" ]; then rest+=("$p"); fi
		done
		BG=("${rest[@]}")
		if [ -z "$FF_TRIGGER" ]; then
			ff_check
			if [ -n "$FF_TRIGGER" ]; then stop_running_gates "$FF_TRIGGER"; fi
		fi
	done
	BG=()
}

# ---- P1: the fast gates, one after another, before anything long ---------------
# Lint, build, unit, vet, the two UI gates and the site gate run alone and in
# this order, before the Temporal, the images and the binaries the long gates
# need, and before any of them starts: a failure here is known in minutes, and
# the proof stops with nothing long started. The unit suites also run with
# nothing else on the host's CPU from this proof: mocked-environment tests can
# be load-sensitive.
#
# The gate lists are anchored to the base (AGENTS.md §11.2): an entry the
# change itself adds, or removes, excuses nothing until the base carries it.
git show "$BASE_COMMIT:scripts/proof-nightly-tests.txt" >"$DIR/nightly-base.txt" 2>/dev/null || : >"$DIR/nightly-base.txt"
git show "$BASE_COMMIT:scripts/proof-nightly-specs.txt" >"$DIR/nightly-specs-base.txt" 2>/dev/null || : >"$DIR/nightly-specs-base.txt"
git show "$BASE_COMMIT:scripts/proof-quarantine.txt" >"$DIR/quarantine-base.txt" 2>/dev/null || : >"$DIR/quarantine-base.txt"
git show "$BASE_COMMIT:scripts/proof-host-speed.tsv" >"$DIR/host-speed-base.txt" 2>/dev/null || : >"$DIR/host-speed-base.txt"
# The gate files this change edits, and the rows its excusing lists add or
# remove (plan §6.4), computed whatever gates run. A failure is written into
# the file as an error entry, never as "no gate file changed", and makes the
# proof ineligible.
if ! "$PR" touches -base "$BASE_COMMIT" -gate-files >"$DIR/lists-changed.json" 2>"$DIR/lists-changed.log"; then
	say "WARNING: the gate files this change edits could not be listed: $(cat "$DIR/lists-changed.log")"
fi
in_ui() { (cd "$REPO/ui" && "$@"); }
in_site() { (cd "$REPO/site" && "$@"); }
step lint lint.log make lint
step build build.log go build ./...
step unit unit.log env ORK_PROOF_DIR="$DIR/artifacts" "$PR" unit -out "$DIR" \
	-quarantine scripts/proof-quarantine.txt -quarantine-base "$DIR/quarantine-base.txt"
step vet-integration vet-integration.log go vet -tags=integration ./...
# Both UI gates read ui/.svelte-kit: `bun test` resolves $lib through its
# tsconfig, and `bun run check` regenerates it with svelte-kit sync. Generate
# it once before either runs, so a fresh checkout starts both from the same
# files; sync rewrites only what changed, so the check's own sync then writes
# nothing.
if [ -z "$FF_TRIGGER" ] && { runs ui-check || runs ui-unit; }; then
	in_ui ./node_modules/.bin/svelte-kit sync >"$DIR/ui-sync.log" 2>&1 || say "svelte-kit sync failed: $DIR/ui-sync.log"
fi
step ui-check ui-check.log in_ui bun run check
step ui-unit ui-unit.log in_ui bun test test/
# The site renders the repository's markdown, the examples, the generated
# defaults and API reference, and takes its style from ui/src/tokens.css; one
# command checks it (the docs sync, the token drift check, astro check) and
# builds it (link validation included). It needs no Temporal and no Docker.
step site site.log in_site bash -c 'bun run check && bun run build'
# The race gate and the nightly lanes run alone, after the cheap gates and
# before the setup: the detector multiplies every test's time several times
# over, so a race run beside other gates would only make both load-sensitive,
# and a race found here ends the proof before the long gates start. A full
# proof runs every package; a feature proof the packages the change touches.
# None sets ORK_PROOF_DIR: the measurements the unit gate writes there are the
# unit gate's, and one taken under the detector is evidence of nothing.
step race race.log "$PR" unit -race -short -out "$DIR" "${RACE_PACKAGES[@]}"
step race-repeat race-repeat.log "$PR" unit -race -short -count "$RACE_REPEAT_COUNT" -out "$DIR" "${RACE_REPEAT_PACKAGES[@]}"
step fuzz fuzz.log "$PR" fuzz -out "$DIR" -fuzztime "$FUZZTIME"

# ---- P0: the setup, one concurrent group: Temporal, images, binaries, test binaries --
# Everything the long gates need, started together once the fast gates have
# passed, and collected by wait_bg: a failure of any member stops the others
# (stop_running_gates) and the proof, and each member is its own gates.tsv
# row as before, with a `setup` row for the group's wall. Nothing here is
# load-sensitive — no test runs until the group has ended — and the unit gate
# ran alone before it. A member runs in a subshell, so what start_temporal
# learns reaches the proof through temporal.env.
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
	port="$(docker port "$TEMPORAL_NAME" 7233/tcp | head -n1 | awk -F: '{print $NF}')"
	if [ -z "$port" ]; then
		say "no published port for $TEMPORAL_NAME"
		return 1
	fi
	TEMPORAL_ADDRESS="127.0.0.1:$port"
	for i in $(seq 1 240); do
		if docker exec "$TEMPORAL_NAME" temporal operator cluster health --address 127.0.0.1:7233 2>/dev/null | grep -q SERVING; then
			say "Temporal $TEMPORAL_IMAGE serving at $TEMPORAL_ADDRESS ($(docker image inspect --format '{{index .RepoDigests 0}}' "$TEMPORAL_IMAGE" 2>/dev/null))"
			printf 'TEMPORAL_ADDRESS=%q\nTEMPORAL_IMAGE=%q\nSWEPT=%q\n' "$TEMPORAL_ADDRESS" "$TEMPORAL_IMAGE" "$SWEPT" >"$DIR/temporal.env"
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
	go build -o "$DIR/bin/" "${STACK_BINARIES[@]}" || return 1
	for pkg in "${STACK_BINARIES[@]}"; do
		if [ ! -x "$DIR/bin/$(basename "$pkg")" ]; then
			say "go build wrote no $DIR/bin/$(basename "$pkg") for $pkg"
			return 1
		fi
		say "built $DIR/bin/$(basename "$pkg") from $pkg"
	done
}

if [ -z "$FF_TRIGGER" ]; then
	SETUP_STARTED=$SECONDS
	SETUP_MEMBERS=""
	for g in temporal images binaries integration-compile; do
		if runs "$g"; then SETUP_MEMBERS="$SETUP_MEMBERS $g"; fi
	done
	# Set here, in the proof's own shell, before the member starts: on_exit
	# stops the container by its exact name, whichever member failed.
	if runs temporal && [ "$PROOF_TEMPORAL" = own ]; then TEMPORAL_STARTED=1; fi
	if runs images; then IMAGES_BUILT=1; fi
	step_bg temporal temporal.log start_temporal
	step_bg images images.log push_images
	step_bg binaries binaries.log build_binaries
	compile_args=(-out "$DIR/bin/test")
	if [ "$TIER" = feature ]; then compile_args+=(-select "$DIR/selection.json"); fi
	step_bg integration-compile integration-compile.log "$PR" compile "${compile_args[@]}"
	if [ -z "$IMAGE_TAG_OWN" ] && { runs temporal || runs activity-fixture; }; then
		record_images >"$DIR/images.log" 2>&1
	fi
	wait_bg
	if [ -f "$DIR/temporal.env" ]; then
		# shellcheck source=/dev/null
		. "$DIR/temporal.env"
	fi
	if [ -n "$SETUP_MEMBERS" ]; then
		setup_result=pass
		for g in $SETUP_MEMBERS; do
			if [ "$(awk -F'\t' -v g="$g" '$1 == g {print $2; exit}' "$GATES_TSV")" != pass ]; then setup_result=fail; fi
		done
		for g in $SETUP_MEMBERS; do
			awk -F'\t' -v g="$g" '$1 == g {print g ": " $2 " in " $3 "s -> " $4; exit}' "$GATES_TSV"
		done >>"$DIR/setup.log"
		printf '%s\t%s\t%s\t%s\t%s\n' setup "$setup_result" "$((SECONDS - SETUP_STARTED))" setup.log "concurrent:$SETUP_MEMBERS" >>"$GATES_TSV"
		say "   setup: $setup_result ($((SECONDS - SETUP_STARTED))s wall, concurrent:$SETUP_MEMBERS)"
	fi
fi

# What every stack gate sees. The unit gate sees none of it.
STACK_ENV=(
	ORK_PROOF_ID="$PROOF_ID"
	ORK_PROOF_DIR="$DIR/artifacts"
	ORK_TEMPORAL_ADDRESS="$TEMPORAL_ADDRESS"
	ORK_TEST_BIN_DIR="$DIR/bin"
	ORK_TEST_IMAGE_TAG="$IMAGE_TAG"
	ORK_TEST_REGISTRY="$REGISTRY"
)

# ---- P2: integration, with the Docker-only activity fixture beside it -----------
# The only gates that run beside tests. If either fails, the other is stopped
# (wait_bg, stop_running_gates) and Playwright and observability never start.
#
# The fixture starts only once the integration runner has finished its HEAVY
# jobs (proofrun writes integration-heavy-done between the heavy phase and the
# ordinary one): a heavy test asserts absolute wall-clock bounds measured on a
# quiet host, and the fixture's containers are exactly the load it must not
# see. If the integration gate ends without the marker (it failed or was
# stopped first), the wait ends with it. `exec` makes the fixture's own pid the
# one run() recorded, so stop_running_gates reaches make and not a wrapper.
fixture_after_heavy() {
	if runs integration; then
		while [ ! -e "$DIR/integration-heavy-done" ] && ! grep -q "^integration"$'\t' "$GATES_TSV"; do
			sleep 2
		done
	fi
	exec make -C images test TAG="$IMAGE_TAG" REGISTRY="$REGISTRY"
}

# ---- P3: the UI build, during the ordinary integration jobs ----------------------
# The vite build the Playwright gate's global setup would otherwise make first
# (ui/e2e/build-ui.ts, which global-setup.ts calls), and the listing the gate
# plans from, made while the ordinary integration jobs run: only once
# integration-heavy-done exists, the fixture's policy, so never beside a heavy
# test, or once the integration gate has passed without one. Both are only a
# head start and never a gate: the global setup rebuilds whenever the hash of
# the build's inputs differs from the one beside ui/build, which a failed or
# interrupted build never leaves, and the gate lists for itself when no
# listing was written. The gate waits for this to end before it starts.
UI_PREBUILD=""
ui_prebuild() {
	local started
	while [ ! -e "$DIR/integration-heavy-done" ] && ! grep -q "^integration"$'\t' "$GATES_TSV"; do
		sleep 2
	done
	if [ ! -e "$DIR/integration-heavy-done" ] && ! grep -q "^integration"$'\tpass\t' "$GATES_TSV"; then
		say "integration ended before its ordinary jobs: no pre-build, the Playwright gate builds the UI"
		return 0
	fi
	started=$SECONDS
	if ! (cd "$REPO/ui" && bun e2e/build-ui.ts); then
		say "ui-prebuild: the UI build failed after $((SECONDS - started))s: the Playwright gate builds it"
		return 1
	fi
	say "ui-prebuild: ui/build ready after $((SECONDS - started))s"
	if (cd "$REPO/ui" && env "${STACK_ENV[@]}" ORK_PROOF_DIR="$DIR/artifacts/ui" \
		"$REPO/ui/node_modules/.bin/playwright" test --list --reporter=json "${SPEC_ARGS[@]}" >"$DIR/ui-e2e-list.json.pre"); then
		mv "$DIR/ui-e2e-list.json.pre" "$DIR/ui-e2e-list.json"
		say "ui-prebuild: listed the specs into $DIR/ui-e2e-list.json"
	else
		rm -f "$DIR/ui-e2e-list.json.pre"
		say "ui-prebuild: playwright test --list failed: the Playwright gate lists for itself"
	fi
}
if [ -z "$FF_TRIGGER" ]; then
	if runs activity-fixture; then
		run_bg activity-fixture activity-fixture.log fixture_after_heavy
	fi
	if runs integration; then
		integ_args=(-out "$DIR" -bin "$DIR/bin/test" -jobs "$PROOF_JOBS" -namespace-prefix "$PROOF_ID" -tier "$TIER" -proof-id "$PROOF_ID" -base-commit "$BASE_COMMIT" -merge-base "$MERGE_BASE" -print-progress -quarantine scripts/proof-quarantine.txt -quarantine-base "$DIR/quarantine-base.txt" -host-speed scripts/proof-host-speed.tsv -host-speed-base "$DIR/host-speed-base.txt" -lists-changed "$DIR/lists-changed.json")
		case "$TIER" in
		full) integ_args+=(-nightly-mode defer -nightly-base "$DIR/nightly-base.txt") ;;
		release) integ_args+=(-nightly-mode include) ;;
		nightly) integ_args+=(-nightly-mode only) ;;
		esac
		if [ "$TIER" = feature ]; then integ_args+=(-select "$DIR/selection.json"); fi
		if { [ -n "$OBS_PATHS" ] && [ "$TIER" = full ]; } || [ "$TIER" = release ]; then integ_args+=(-observability-required); fi
		# The first failing test stops the run, and its ledger still lists
		# every test: the unfinished ones as having no outcome.
		if [ "$PROOF_FAIL_FAST" = 1 ]; then integ_args+=(-fail-fast); fi
		# A skip is not a pass: ORK_REQUIRE_INFRA turns every prerequisite skip
		# into a failure now that the prerequisites were probed above, and the
		# ledger fails any skip scripts/proof-allowed-skips.txt does not name.
		run_bg integration integration.log env "${STACK_ENV[@]}" ORK_REQUIRE_INFRA=1 "$PR" integration "${integ_args[@]}"
		if runs ui-e2e; then
			ui_prebuild >"$DIR/ui-prebuild.log" 2>&1 &
			UI_PREBUILD=$!
		fi
	fi
	wait_bg
fi
# The pre-build ends before the Playwright gate starts, so the two never
# build ui/build at once; a proof that has stopped ends it instead.
if [ -n "$UI_PREBUILD" ]; then
	if [ -n "$FF_TRIGGER" ]; then
		for p in $(descendants "$UI_PREBUILD"); do kill -TERM "$p" 2>/dev/null; done
		kill -TERM "$UI_PREBUILD" 2>/dev/null
		wait "$UI_PREBUILD" 2>/dev/null
	else
		say "== ui-prebuild: waiting for the UI pre-build -> $DIR/ui-prebuild.log"
		wait "$UI_PREBUILD"
		grep '^ui-prebuild:\|^integration ended' "$DIR/ui-prebuild.log" | sed 's/^/   /'
	fi
	UI_PREBUILD=""
fi

# ---- P4: Playwright ------------------------------------------------------------------
# Spec files run PROOF_UI_WORKERS at a time: the browser runs in a network
# namespace of its own (ui/e2e/browser-server.ts, started by the suite's
# global setup from the image ui/e2e/browser-image.txt pins, pulled here when
# the host lacks it), and every file owns its bucket and its Temporal
# namespace. Each file still listens on FIXED ports and names fixed Orker
# work roots, unique across files but not across proofs (AGENTS.md §11.1), so
# two proofs' specs must never overlap: a host-wide lock enforces it, and a
# second proof waits here rather than failing on a port.
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
browser_image() { grep -v '^#' "$REPO/ui/e2e/browser-image.txt" | grep -v '^$' | head -n1; }
ui_e2e_locked() {
	local pw="$REPO/ui/node_modules/.bin/playwright" code=0 image
	image="$(browser_image)"
	if ! docker image inspect "$image" >/dev/null 2>&1; then
		say "pulling the browser-server image $image"
		docker pull "$image" >"$DIR/browser-image-pull.log" 2>&1 || { say "cannot pull $image: $DIR/browser-image-pull.log"; return 1; }
	fi
	say "browser server image: $image"
	if [ -s "$DIR/ui-e2e-list.json" ]; then
		say "playwright test --list: listed by the UI pre-build during the ordinary integration jobs ($DIR/ui-e2e-list.json)"
	elif ! (cd "$REPO/ui" && env "${STACK_ENV[@]}" ORK_PROOF_DIR="$DIR/artifacts/ui" \
		"$pw" test --list --reporter=json "${SPEC_ARGS[@]}" >"$DIR/ui-e2e-list.json"); then
		say "playwright test --list failed"
		return 1
	fi
	# The full tier defers the spec files both nightly spec lists carry; the
	# nightly tier runs the listed ones alone; the pre-release gate and a
	# feature proof run what they list. The plan names the files to pass
	# Playwright when it leaves a listed one out.
	# A feature proof's listing holds only its selected specs, so the list's
	# rows cannot be checked against it: it plans with no list.
	local plan_mode=include plan_specs=scripts/proof-nightly-specs.txt
	case "$TIER" in
	full) plan_mode=defer ;;
	nightly) plan_mode=only ;;
	feature) plan_specs="" ;;
	esac
	"$PR" ui-plan -list "$DIR/ui-e2e-list.json" -costs scripts/proof-ui-costs.tsv \
		-shards 1 -out "$DIR/ui-plan.json" -nightly-mode "$plan_mode" \
		-nightly-specs "$plan_specs" -nightly-specs-base "$DIR/nightly-specs-base.txt" \
		-run-files-out "$DIR/ui-run-files.txt" >>"$DIR/ui-e2e.log" 2>&1 || { say "proofrun ui-plan failed"; return 1; }
	local -a run_specs=()
	mapfile -t run_specs <"$DIR/ui-run-files.txt"
	if [ ${#run_specs[@]} -eq 0 ]; then run_specs=("${SPEC_ARGS[@]}"); fi
	# With one worker everything runs in one invocation. With more, the
	# parallel project runs first and the two solo projects — the spec files
	# that assert a render budget, which only a quiet host can measure
	# (ui/playwright.config.ts, `projects`) — run after it on one worker; the
	# ledger reads both reports as one run. Spec files come BEFORE --project:
	# Playwright's --project takes every following word as a project name, so
	# a file after it is refused as an unknown project. --pass-with-no-tests,
	# because a selection may hold no file of one project; the ledger still
	# fails every listed test without an outcome.
	local reports=()
	if [ "$PROOF_UI_WORKERS" -gt 1 ]; then
		# One browser server for both invocations, started here and stopped
		# here; each invocation's global setup finds it in the environment
		# (ui/e2e/global-setup.ts) and the second reuses the first's UI build
		# by its inputs' hash (ui/e2e/build-hash.ts).
		local server
		if server="$(cd "$REPO/ui" && bun e2e/browser-server.ts start "$PROOF_ID" 2>>"$DIR/ui-e2e.log")"; then
			BROWSER_WS="$(printf '%s' "$server" | sed -n 's/.*"wsEndpoint":"\([^"]*\)".*/\1/p')"
			BROWSER_CONTAINER="$(printf '%s' "$server" | sed -n 's/.*"container":"\([^"]*\)".*/\1/p')"
			say "browser server for both invocations: $BROWSER_WS (container ${BROWSER_CONTAINER:0:12})"
		else
			say "cannot start the browser server once for the gate: each invocation starts its own"
			BROWSER_WS=""
			BROWSER_CONTAINER=""
		fi
		playwright_run "$DIR/ui-e2e-report.json" "$PROOF_UI_WORKERS" "${run_specs[@]}" --pass-with-no-tests --project chromium || code=1
		reports+=(-report "$DIR/ui-e2e-report.json")
		if [ "$code" -eq 0 ] || [ "$PROOF_FAIL_FAST" = 0 ]; then
			playwright_run "$DIR/ui-e2e-report-solo.json" 1 "${run_specs[@]}" --pass-with-no-tests --project chromium-solo-each-fan-out --project chromium-solo-run-page-ceiling || code=1
			reports+=(-report "$DIR/ui-e2e-report-solo.json")
		fi
		if [ -n "$BROWSER_CONTAINER" ]; then docker stop -t 5 "$BROWSER_CONTAINER" >/dev/null 2>&1; fi
		BROWSER_WS=""
		BROWSER_CONTAINER=""
	else
		playwright_run "$DIR/ui-e2e-report.json" 1 "${run_specs[@]}" || code=1
		reports+=(-report "$DIR/ui-e2e-report.json")
	fi
	"$PR" ui-ledger -list "$DIR/ui-e2e-list.json" "${reports[@]}" -plan "$DIR/ui-plan.json" \
		-log "$DIR/ui-e2e.log" -config "$REPO/ui/playwright.config.ts" -out "$DIR" \
		-part local -host-speed scripts/proof-host-speed.tsv -host-speed-base "$DIR/host-speed-base.txt" || code=1
	return "$code"
}
# playwright_run <report> <workers> <playwright args...>: one invocation of the
# suite, its JSON report at <report>, its list output appended to the gate's log.
BROWSER_WS=""
BROWSER_CONTAINER=""
playwright_run() {
	local report="$1" workers="$2"
	shift 2
	(cd "$REPO/ui" && env "${STACK_ENV[@]}" ORK_PROOF_DIR="$DIR/artifacts/ui" \
		${BROWSER_WS:+ORK_E2E_BROWSER_WS="$BROWSER_WS"} ${BROWSER_CONTAINER:+ORK_E2E_BROWSER_CONTAINER="$BROWSER_CONTAINER"} \
		PLAYWRIGHT_JSON_OUTPUT_FILE="$report" \
		PLAYWRIGHT_JSON_OUTPUT_NAME="$report" \
		"$REPO/ui/node_modules/.bin/playwright" test --workers "$workers" --reporter=list,json "$@")
}
step ui-e2e ui-e2e.log ui_e2e

# ---- P5: observability ------------------------------------------------------------------
# One real run through ork and orker with telemetry switched on; every
# signal it should have produced is queried back out of Loki, Tempo and
# Prometheus and asserted. Its admin ports are fixed (Prometheus scrapes
# them), so the script holds a host-wide lock.
step observability observability.log env "${STACK_ENV[@]}" scripts/proof-observability.sh

# ---- a proof that stopped: the gates it never started ------------------------------------
# Each is recorded with the gate that stopped the proof, so the verdict names
# it and the proof fails: a gate that did not run is never a pass.
if [ -n "$FF_TRIGGER" ]; then
	for g in $ALL_GATES; do
		if runs "$g" && ! grep -q "^$g"$'\t' "$GATES_TSV"; then
			printf '%s\tnot-started\t0\t-\tnot started: %s failed\n' "$g" "$FF_TRIGGER" >>"$GATES_TSV"
		fi
	done
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
if [ -f "$DIR/runner-health.json" ]; then record_args+=(-runner-health "$DIR/runner-health.json"); fi
if { [ "$TIER" = full ] && [ -n "$OBS_PATHS" ]; } || [ "$TIER" = release ]; then record_args+=(-observability-required); fi

README="$DIR/README.md"
if [ "$TIER" != feature ]; then
	# The full, release and nightly tiers have one verdict path: one part,
	# merged by the same code the CI proof job runs over many (plan §2.7).
	mkdir -p "$DIR/parts/local-a1"
	record_args+=(-part local -attempt 1 -out-dir "$DIR/parts/local-a1")
	merge_args=(-plan "$DIR/integration-plan.json" -parts "$DIR/parts" -out "$DIR" -name "$NAME")
	# A proof that stopped before `proofrun integration` ran has no plan: the
	# merger then writes its failing verdict from the local part alone.
	if [ -f "$DIR/integration-plan.json" ]; then
		record_args+=(-plan "$DIR/integration-plan.json")
	else
		merge_args+=(-stopped)
	fi
	if [ -f "$DIR/ui-plan.json" ]; then record_args+=(-ui-plan "$DIR/ui-plan.json"); fi
	"$PR" record "${record_args[@]}" >"$DIR/.part-record.txt" 2>&1
	record_status=$?
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
		case "$result" in
		not-started) echo "- **$gate**: not started — $command" ;;
		stopped) echo "- **$gate**: stopped after ${seconds}s — $command → \`$logname\`" ;;
		*) echo "- **$gate**: $result in ${seconds}s — \`$command\` → \`$logname\`" ;;
		esac
	done <"$GATES_TSV"
	if [ -n "$FF_TRIGGER" ]; then
		echo
		echo "The proof stopped when **$FF_TRIGGER** failed (PROOF_FAIL_FAST): the gates marked stopped were ended and the ones marked not started never began. A stopped proof is a failed proof; docs/PROOFS.md says how to read it."
	fi
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
