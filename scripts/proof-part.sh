#!/usr/bin/env bash
# proof-part.sh — one part of a CI proof, run by `scripts/proof.sh --part NAME`.
#
# It is sourced by proof.sh after the part's directory, proof id and flags are
# known, and never runs on its own. Each part is its own proof host: its own
# Temporal, its own image tag, its own gates.tsv, and a part.json the proof
# job merges with the others (docs/PROOFS.md).
#
# A part never decides a verdict. `proofrun merge` does, from every part.

part_say() { printf '%s\n' "$*"; }

part_store_health() {
	local code
	code="$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9000/ 2>/dev/null)"
	[ "$code" = 200 ] || [ "$code" = 403 ]
}

part_need() { # <what> <command...>
	if ! "${@:2}" >/dev/null 2>&1; then
		printf '  - %s\n' "$1" >>"$PART_MISSING"
	fi
}

part_require() {
	: >"$PART_MISSING"
	case "$PART" in
	plan)
		part_need "git on PATH" command -v git
		part_need "go on PATH" command -v go
		part_need "bun on PATH" command -v bun
		part_need "ui/node_modules (cd ui && bun install)" test -d "$REPO/ui/node_modules"
		part_need "ui/node_modules/.bin/playwright" test -x "$REPO/ui/node_modules/.bin/playwright"
		;;
	build)
		part_need "docker on PATH" command -v docker
		part_need "docker buildx (make -C images oci)" docker buildx version
		part_need "make on PATH" command -v make
		part_need "an integration plan (--plan)" test -f "$PLAN"
		;;
	fast)
		part_need "go on PATH" command -v go
		# The unit gate's tests include proofrun's, which run go test -race.
		part_need "cgo for the race detector: CGO_ENABLED=1 and a C compiler on PATH (go env CC)" race_toolchain
		part_need "bun on PATH" command -v bun
		part_need "ui/node_modules (cd ui && bun install)" test -d "$REPO/ui/node_modules"
		part_need "node on PATH (the site's scripts and astro run on it)" command -v node
		part_need "site/node_modules (cd site && bun install --frozen-lockfile)" test -d "$REPO/site/node_modules"
		part_need "make on PATH" command -v make
		;;
	race | race-repeat)
		part_need "go on PATH" command -v go
		part_need "cgo for the race detector: CGO_ENABLED=1 and a C compiler on PATH (go env CC)" race_toolchain
		;;
	fuzz)
		part_need "go on PATH" command -v go
		;;
	integration:*)
		part_need "docker on PATH" command -v docker
		part_need "docker compose" docker compose version
		part_need "cosign on PATH (image-policy interop test)" command -v cosign
		part_need "the dev object store on 127.0.0.1:9000 (SeaweedFS; make infra-up)" part_store_health
		part_need "the local registry on $REGISTRY (make infra-up)" curl -sf -o /dev/null "http://$REGISTRY/v2/"
		part_need "a layout directory (--layouts)" test -d "$LAYOUTS"
		part_need "an integration plan (--plan)" test -f "$PLAN"
		;;
	ui-e2e:*)
		part_need "docker on PATH" command -v docker
		part_need "docker compose" docker compose version
		part_need "bun on PATH" command -v bun
		part_need "node on PATH (Playwright and the built UI run on it)" command -v node
		part_need "the temporal CLI on PATH (ui/e2e/admin.spec.ts)" command -v temporal
		part_need "jq on PATH (the shard's file list)" command -v jq
		part_need "flock on PATH (the Playwright gate's host lock)" command -v flock
		part_need "the dev object store on 127.0.0.1:9000 (SeaweedFS; make infra-up)" part_store_health
		part_need "the local registry on $REGISTRY (make infra-up)" curl -sf -o /dev/null "http://$REGISTRY/v2/"
		part_need "an integration plan (--plan)" test -f "$PLAN"
		part_need "a UI plan (--ui-plan)" test -f "$UI_PLAN"
		part_need "a layout directory (--layouts)" test -d "$LAYOUTS"
		# The suite's browser is the pinned container (ui/e2e/browser-server.ts);
		# the host's own Chromium is needed only by the shard whose files drive
		# one, and is refused here, explicitly, rather than by the spec's
		# "Executable doesn't exist" after the stack has started.
		if [ -f "$UI_PLAN" ] && part_ui_shard_drives_host_chromium "${PART#ui-e2e:}"; then
			part_need "Playwright's Chromium on this host (cd ui && npx playwright install chromium): this shard holds a spec driven on the host's Chromium" part_host_chromium
		fi
		;;
	activity-fixture:*)
		part_need "an integration plan (--plan)" test -f "$PLAN"
		part_need "docker on PATH" command -v docker
		part_need "the dev object store on 127.0.0.1:9000 (SeaweedFS; make infra-up)" part_store_health
		part_need "the local registry on $REGISTRY (make infra-up)" curl -sf -o /dev/null "http://$REGISTRY/v2/"
		part_need "a layout directory (--layouts)" test -d "$LAYOUTS"
		;;
	observability)
		part_need "an integration plan (--plan)" test -f "$PLAN"
		part_need "docker on PATH" command -v docker
		part_need "docker compose" docker compose version
		part_need "jq on PATH" command -v jq
		part_need "flock on PATH" command -v flock
		part_need "the dev object store on 127.0.0.1:9000 (SeaweedFS; make infra-up)" part_store_health
		part_need "the OTel Collector on 127.0.0.1:4318 (make infra-up-observability)" \
			curl -s -o /dev/null http://127.0.0.1:4318/v1/logs
		part_need "Loki on 127.0.0.1:3100 (make infra-up-observability)" curl -sf -o /dev/null http://127.0.0.1:3100/ready
		part_need "Tempo on 127.0.0.1:3200 (make infra-up-observability)" curl -sf -o /dev/null http://127.0.0.1:3200/ready
		part_need "Prometheus on 127.0.0.1:9090 (make infra-up-observability)" curl -sf -o /dev/null http://127.0.0.1:9090/-/ready
		;;
	esac
	if [ -s "$PART_MISSING" ]; then
		part_say "proof: prerequisites absent, nothing measured, nothing written:"
		cat "$PART_MISSING"
		return 77
	fi
	return 0
}

# part_ui_shard_files prints the spec files of UI shard $1, as the UI plan
# lists them (ui/… paths), one per line.
part_ui_shard_files() { jq -r --argjson k "$1" '.shards[] | select(.index == $k) | .files[]' "$UI_PLAN" 2>/dev/null; }

# part_ui_shard_drives_host_chromium: UI shard $1 holds a spec driven on the
# HOST's Chromium — run-page-ceiling.spec.ts, the one project without a
# connection to the container (ui/playwright.config.ts), or
# network-change.spec.ts, which launches a host-namespace Chromium as its
# positive control. The same rule decides the `Chromium (host)` step of
# .github/workflows/proof.yml; every other shard runs the container alone.
part_ui_shard_drives_host_chromium() { part_ui_shard_files "$1" | grep -qE '(network-change|run-page-ceiling|run-page-reduced-ceiling)\.spec\.ts$'; }

# part_host_chromium: Playwright's own Chromium is installed on this host, at
# the path playwright-core resolves for it.
part_host_chromium() {
	(cd "$REPO/ui" && node -e 'const fs = require("fs"); fs.accessSync(require("playwright-core").chromium.executablePath(), fs.constants.X_OK)')
}

# part_runner_class names the runner for scripts/proof-runner-floors.tsv:
# the runner image and its vCPU count on a GitHub-hosted runner, because the
# all-core and memory floors depend on the runner's size; `self-hosted` and
# `local` have no row and are measured for information only.
part_runner_class() {
	if [ "${RUNNER_ENVIRONMENT:-}" = github-hosted ] && [ -n "${ImageOS:-}" ]; then
		printf 'github-hosted/%s/%scpu\n' "$ImageOS" "$(nproc)"
	elif [ "${GITHUB_ACTIONS:-}" = true ]; then
		printf 'self-hosted\n'
	else
		printf 'local\n'
	fi
}

# part_runner_health is the runner-health floor (AGENTS.md §11.2): the FIRST
# thing a part does, before any gate. About fourteen seconds measure this
# runner's work disk, CPUs and memory against the floors
# scripts/proof-runner-floors.tsv declares for its class, into
# runner-health.json beside the part's records. It returns 1 only for a
# STACK part below a floor: the part then records itself and stops, and the
# merger classifies it runner-degraded, exactly as a part whose runner
# GitHub lost — no outcome of the tree, not a proof of the tree, and that
# one job may run again. The plan, build and fast parts measure for
# information. A measurement that cannot run (exit 2) is reported and the
# part runs on, judged as above the floor: the floor only ever turns a
# result into "not a proof", never a failure into a pass.
part_runner_health() {
	local class code=0
	class="$(part_runner_class)"
	part_say "== runner-health: class $class against scripts/proof-runner-floors.tsv"
	"$PR" runner-health -class "$class" -floors scripts/proof-runner-floors.tsv -out "$DIR/runner-health.json" >"$DIR/runner-health.log" 2>&1 || code=$?
	sed 's/^/   /' "$DIR/runner-health.log"
	case "$code" in
	0) return 0 ;;
	1)
		if part_floor_binds; then
			part_say "   runner-health: this runner is BELOW the floor; the part stops before any gate and is not a proof of the tree (re-run this job alone)"
			# On GitHub, one annotation of fixed words, the only way the rerun
			# planner (.github/workflows/proof-rerun.yml, `proofrun rerun-plan`)
			# can tell this job from a failed one without downloading its
			# artifact. It is evidence for running the job again, never for a
			# verdict: the merger re-derives degradation from
			# runner-health.json. No variable text, so it is matched whole
			# (runnerBelowFloorAnnotation, test/tools/proofrun/rerunplan.go).
			if [ "${GITHUB_ACTIONS:-}" = true ]; then
				printf '::error::%s\n' "proof: runner below the floor, no gate ran"
			fi
			return 1
		fi
		part_say "   runner-health: this runner is below the floor; the $PART part measures for information and runs on"
		return 0
		;;
	*)
		part_say "   runner-health: the measurement itself failed (exit $code); the part runs on, judged as above the floor"
		return 0
		;;
	esac
}

# part_floor_binds: the floor stops this part. The stack parts run the
# tests whose bounds a degraded runner would fail; the plan, build and fast
# parts record their measurement and run on.
part_floor_binds() {
	case "$PART" in
	integration:* | ui-e2e:* | activity-fixture:* | observability) return 0 ;;
	esac
	return 1
}

part_tree_hash() {
	local idx
	idx="$(mktemp)"
	rm -f "$idx"
	GIT_INDEX_FILE="$idx" git read-tree HEAD &&
		GIT_INDEX_FILE="$idx" git add -A . &&
		GIT_INDEX_FILE="$idx" git write-tree
	rm -f "$idx"
}
part_is_clean() { [ -z "$(git status --porcelain --untracked-files=all)" ]; }

part_proc_start() { sed -E 's/^.*\) //' "/proc/$1/stat" 2>/dev/null | awk '{print $20}'; }

part_run() { # <gate> <log> <command...>
	local gate="$1" logname="$2" logfile="$DIR/$2"
	shift 2
	part_say "== $gate: $*"
	local started=$SECONDS result=pass code=0
	"$@" >"$logfile" 2>&1 || code=$?
	[ "$code" -eq 0 ] || result=fail
	printf '%s\t%s\t%s\t%s\t%s\n' "$gate" "$result" "$((SECONDS - started))" "$logname" "$*" >>"$DIR/gates.tsv"
	if [ "$result" = pass ]; then
		part_say "   $gate: pass ($((SECONDS - started))s) -> $logfile"
	else
		part_say "   $gate: FAIL exit $code ($((SECONDS - started))s) -> $logfile"
		tail -n 40 "$logfile" | sed 's/^/      /'
	fi
	return "$code"
}

part_record() { # <status>
	local status="$1" args
	args=(
		-dir "$DIR" -part "$PART" -attempt "$ATTEMPT" -tier "$TIER" -name "$NAME"
		-commit "$(git rev-parse HEAD)" -branch "$(git branch --show-current 2>/dev/null)"
		-tree-start "$TREE_START" -tree-end "$TREE_END"
		-clean-start="$CLEAN_START" -clean-end="$CLEAN_END"
		-base-ref "$BASE" -base-commit "$BASE_COMMIT" -merge-base "$MERGE_BASE"
		-jobs "$PROOF_JOBS" -ui-workers "$PROOF_UI_WORKERS"
		-status "$status"
	)
	[ -n "$PLAN" ] && args+=(-plan "$PLAN")
	[ -n "$UI_PLAN" ] && args+=(-ui-plan "$UI_PLAN")
	[ -f "$DIR/setup.log" ] && args+=(-setup-log "$DIR/setup.log")
	[ -f "$DIR/runner-health.json" ] && args+=(-runner-health "$DIR/runner-health.json")
	[ -f "$DIR/images.tsv" ] && args+=(-image-tag "$PROOF_ID" -images "$DIR/images.tsv" -image-tag-own)
	[ -n "$PART_TEMPORAL_RAN" ] && args+=(-temporal own -temporal-image "$TEMPORAL_IMAGE" -temporal-address "$TEMPORAL_ADDRESS")
	[ "$OBSERVABILITY_REQUIRED" = 1 ] && args+=(-observability-required)
	"$PR" record "${args[@]}"
}

part_start_temporal() {
	local args port i
	mapfile -t args < <("$PR" temporal-args -compose deploy/docker-compose.yml)
	[ ${#args[@]} -ge 1 ] || { part_say "no temporal service image in deploy/docker-compose.yml"; return 1; }
	TEMPORAL_IMAGE="${args[0]}"
	TEMPORAL_NAME="orkstack-proof-$PROOF_ID-temporal"
	part_say "starting $TEMPORAL_NAME: ${args[*]}"
	docker run -d --rm --name "$TEMPORAL_NAME" \
		--label "orkstack.proof=$PROOF_ID" \
		--label "orkstack.proof.owner=$(hostname):$$:$(part_proc_start $$)" \
		-p 127.0.0.1::7233 "${args[@]}" || return 1
	PART_TEMPORAL_RAN=1
	port="$(docker port "$TEMPORAL_NAME" 7233/tcp | head -n1 | awk -F: '{print $NF}')"
	[ -n "$port" ] || { part_say "no published port for $TEMPORAL_NAME"; return 1; }
	TEMPORAL_ADDRESS="127.0.0.1:$port"
	for i in $(seq 1 240); do
		if docker exec "$TEMPORAL_NAME" temporal operator cluster health --address 127.0.0.1:7233 2>/dev/null | grep -q SERVING; then
			part_say "Temporal $TEMPORAL_IMAGE serving at $TEMPORAL_ADDRESS"
			return 0
		fi
		sleep 0.5
	done
	part_say "$TEMPORAL_NAME never reported SERVING within 120s"
	docker logs "$TEMPORAL_NAME" 2>&1 | tail -n 40
	return 1
}

# As scripts/proof.sh build_binaries: one `go build` links all nine into bin/.
part_binaries() {
	local pkg
	go build -o "$DIR/bin/" "${STACK_BINARIES[@]}" || return 1
	for pkg in "${STACK_BINARIES[@]}"; do
		if [ ! -x "$DIR/bin/$(basename "$pkg")" ]; then
			part_say "go build wrote no $DIR/bin/$(basename "$pkg") for $pkg"
			return 1
		fi
	done
}

part_push_images() {
	"$PR" push-layout -layouts "$LAYOUTS" -registry "$REGISTRY" -namespace orkstack -tag "$PROOF_ID" -out "$DIR/images.tsv" || return 1
	local a ref digest
	while IFS=$'\t' read -r a ref digest; do
		docker pull "$ref" >/dev/null || return 1
	done <"$DIR/images.tsv"
}

part_cleanup() {
	local code=$?
	trap '' TERM INT HUP
	if [ -n "$PART_TEMPORAL_RAN" ]; then docker stop "$TEMPORAL_NAME" >/dev/null 2>&1; fi
	if [ -n "$PART_IMAGES" ]; then
		local a
		for a in $(make -s -C "$REPO/images" print-activities 2>/dev/null); do
			docker image rm "$REGISTRY/orkstack/activity-$a:$PROOF_ID" >/dev/null 2>&1
		done
	fi
	kill -TERM -- -$$ 2>/dev/null
	exit "$code"
}

part_main() { # <part>
	PART="$1"
	case "$PART" in
	plan | build | fast | race | race-repeat | fuzz | observability | integration:[0-9]* | ui-e2e:[0-9]* | activity-fixture:shell | activity-fixture:json | activity-fixture:git | activity-fixture:template) ;;
	*) part_say "proof: unknown part '$PART'"; return 2 ;;
	esac
	PART_MISSING="$(mktemp)"
	part_require || { local rc=$?; rm -f "$PART_MISSING"; return "$rc"; }
	rm -f "$PART_MISSING"
	trap part_cleanup EXIT
	trap 'part_say "proof: part stopped by a signal"; exit 143' TERM INT HUP

	mkdir -p "$DIR/artifacts" "$DIR/bin"
	# The runner's setup log (.github/actions/proof-fetch): every fetch
	# attempt made before this part, kept with the part so part.json and the
	# merged proof show a download or pull that needed its second attempt.
	if [ -n "${SETUP_LOG:-}" ] && [ -f "$SETUP_LOG" ]; then
		cp "$SETUP_LOG" "$DIR/setup.log"
	fi
	mv "$PR" "$DIR/bin/proofrun"
	PR="$DIR/bin/proofrun"
	printf '%s %s %s %s\n' "$$" "$$" "$(part_proc_start $$)" "$PROOF_ID" >"$DIR/proof.pid"

	PART_TEMPORAL_RAN=""
	TEMPORAL_IMAGE=""
	TEMPORAL_ADDRESS=""
	TEMPORAL_NAME=""
	TREE_START="$(part_tree_hash)"
	CLEAN_START=false
	if part_is_clean; then CLEAN_START=true; fi
	git status --short --untracked-files=all >"$DIR/worktree-start.txt" 2>&1
	: >"$DIR/gates.tsv"
	START_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	part_say "proof: part $PART attempt $ATTEMPT of $TIER -> $DIR (id $PROOF_ID, tree $TREE_START$([ "$CLEAN_START" = true ] || echo ', NOT clean'))"

	# The runner-health floor runs FIRST, before any gate: a stack part on a
	# runner below the floor records itself and stops here.
	if ! part_runner_health; then
		TREE_END="$(part_tree_hash)"
		CLEAN_END=false
		if part_is_clean; then CLEAN_END=true; fi
		part_record 1 || true
		part_say "proof: part $PART -> $DIR/part.json (runner below the floor: no gate ran)"
		return 1
	fi

	OBSERVABILITY_REQUIRED=0
	[ "$TIER" = release ] && OBSERVABILITY_REQUIRED=1
	STACK_ENV=(
		ORK_PROOF_ID="$PROOF_ID"
		ORK_PROOF_DIR="$DIR/artifacts"
		ORK_TEST_BIN_DIR="$DIR/bin"
		ORK_TEST_IMAGE_TAG="$PROOF_ID"
		ORK_TEST_REGISTRY="$REGISTRY"
	)
	PART_IMAGES=""

	case "$PART" in
	plan)
		local obs=0
		[ "$TIER" = release ] && obs=1
		if [ "$obs" = 0 ] && "$PR" touches -base "$BASE_COMMIT" -action observability >"$DIR/observability-paths.txt" 2>/dev/null; then
			obs=1
		fi
		OBSERVABILITY_REQUIRED="$obs"
		git show "$BASE_COMMIT:scripts/proof-nightly-tests.txt" >"$DIR/nightly-base.txt" 2>/dev/null || : >"$DIR/nightly-base.txt"
		git show "$BASE_COMMIT:scripts/proof-nightly-specs.txt" >"$DIR/nightly-specs-base.txt" 2>/dev/null || : >"$DIR/nightly-specs-base.txt"
		git show "$BASE_COMMIT:scripts/proof-quarantine.txt" >"$DIR/quarantine-base.txt" 2>/dev/null || : >"$DIR/quarantine-base.txt"
		git show "$BASE_COMMIT:scripts/proof-host-speed.tsv" >"$DIR/host-speed-base.txt" 2>/dev/null || : >"$DIR/host-speed-base.txt"
		local mode=defer
		[ "$TIER" = release ] && mode=include
		[ "$TIER" = nightly ] && mode=only
		# The gate files this change edits (plan §6.4). A failure is written
		# into the file as an error entry, never as "no gate file changed",
		# and makes the proof ineligible.
		if ! "$PR" touches -base "$BASE_COMMIT" -gate-files >"$DIR/lists-changed.json" 2>"$DIR/lists-changed.log"; then
			part_say "WARNING: the gate files this change edits could not be listed: $(cat "$DIR/lists-changed.log")"
		fi
		local plan_args=(
			-plan-only -ci -out "$DIR" -lists-changed "$DIR/lists-changed.json" -jobs "$PROOF_JOBS" -shards "$SHARDS"
			-matrix-out "$DIR/int-matrix.json" -tier "$TIER" -proof-id "$PROOF_ID"
			-base-commit "$BASE_COMMIT" -merge-base "$MERGE_BASE" -ui-shards "$UI_SHARDS"
			-nightly-mode "$mode" -nightly-base "$DIR/nightly-base.txt"
			-quarantine scripts/proof-quarantine.txt -quarantine-base "$DIR/quarantine-base.txt"
			-host-speed scripts/proof-host-speed.tsv -host-speed-base "$DIR/host-speed-base.txt"
			-observability-required="$OBSERVABILITY_REQUIRED"
		)
		[ -n "${PROOF_PR_NUMBER:-}" ] && plan_args+=(-pr-number "$PROOF_PR_NUMBER" -pr-head-sha "${PROOF_PR_HEAD:-}" -pr-base-sha "${PROOF_PR_BASE:-}" -pr-contains-base "${PROOF_PR_CONTAINS_BASE:-false}")
		part_run plan plan.log "$PR" integration "${plan_args[@]}" || true
		if [ -f "$DIR/integration-plan.json" ]; then
			PLAN="$DIR/integration-plan.json"
			(cd "$REPO/ui" && "$REPO/ui/node_modules/.bin/playwright" test --list --reporter=json >"$DIR/ui-list.json") >>"$DIR/plan.log" 2>&1 || true
			if [ -s "$DIR/ui-list.json" ]; then
				# The full tier's UI shards never hold a spec file both nightly
				# spec lists carry; the nightly tier's hold the listed ones alone.
				part_run ui-plan ui-plan.log "$PR" ui-plan -list "$DIR/ui-list.json" -costs scripts/proof-ui-costs.tsv \
					-shards "$UI_SHARDS" -out "$DIR/ui-plan.json" -matrix-out "$DIR/ui-matrix.json" \
					-nightly-mode "$mode" -nightly-specs scripts/proof-nightly-specs.txt \
					-nightly-specs-base "$DIR/nightly-specs-base.txt" || true
				UI_PLAN="$DIR/ui-plan.json"
			fi
		fi
		;;
	build)
		PART_IMAGES=1
		part_run images images.log make -C images oci TAG="$PROOF_ID" OUT="$DIR/layouts" || true
		if [ -d "$DIR/layouts" ]; then
			part_run images-digests images-digests.log "$PR" layout-digests -layouts "$DIR/layouts" \
				-registry "$REGISTRY" -namespace orkstack -tag "$PROOF_ID" -out "$DIR/images.tsv" || true
		fi
		;;
	fast)
		part_run lint lint.log make lint || true
		part_run build build.log go build ./... || true
		git show "$BASE_COMMIT:scripts/proof-quarantine.txt" >"$DIR/quarantine-base.txt" 2>/dev/null || : >"$DIR/quarantine-base.txt"
		part_run unit unit.log env ORK_PROOF_DIR="$DIR/artifacts" "$PR" unit -out "$DIR" \
			-quarantine scripts/proof-quarantine.txt -quarantine-base "$DIR/quarantine-base.txt" || true
		# The part's artifact is its directory. The unit log and events — a
		# green run's were 137 MB and 296 MB — stay in it only when the unit
		# gate failed; the ledger, unit-tests.json, holds every test's outcome
		# either way (docs/PROOFS.md §7), and the merger reads the ledger.
		if grep -q $'^unit\tpass\t' "$DIR/gates.tsv"; then
			rm -f "$DIR/unit.log" "$DIR/unit.events.jsonl"
			part_say "unit passed: unit.log and unit.events.jsonl left out of the part"
		fi
		part_run vet-integration vet-integration.log go vet -tags=integration ./... || true
		(cd "$REPO/ui" && bun run check >"$DIR/ui-check.log" 2>&1) && printf 'ui-check\tpass\t0\tui-check.log\tbun run check\n' >>"$DIR/gates.tsv" || printf 'ui-check\tfail\t0\tui-check.log\tbun run check\n' >>"$DIR/gates.tsv"
		(cd "$REPO/ui" && bun test test/ >"$DIR/ui-unit.log" 2>&1) && printf 'ui-unit\tpass\t0\tui-unit.log\tbun test test/\n' >>"$DIR/gates.tsv" || printf 'ui-unit\tfail\t0\tui-unit.log\tbun test test/\n' >>"$DIR/gates.tsv"
		# The site's check and build, recorded as the UI gates are.
		(cd "$REPO/site" && bun run check && bun run build) >"$DIR/site.log" 2>&1 && printf 'site\tpass\t0\tsite.log\tbun run check && bun run build\n' >>"$DIR/gates.tsv" || printf 'site\tfail\t0\tsite.log\tbun run check && bun run build\n' >>"$DIR/gates.tsv"
		;;
	race)
		# The unit suites under the race detector, on a runner of its own (the
		# fast part holds the gates that run without it). Every package, once.
		part_run race race.log "$PR" unit -race -short -out "$DIR" || true
		# As the fast part trims its unit log: the ledger holds every test's
		# outcome, and the merger reads the ledger.
		if grep -q $'^race\tpass\t' "$DIR/gates.tsv"; then
			rm -f "$DIR/race.log" "$DIR/race.events.jsonl"
			part_say "race passed: race.log and race.events.jsonl left out of the part"
		fi
		;;
	race-repeat)
		# The nightly race repeat: the concurrency-heavy packages, -count times.
		part_run race-repeat race-repeat.log "$PR" unit -race -short -count "$RACE_REPEAT_COUNT" -out "$DIR" "${RACE_REPEAT_PACKAGES[@]}" || true
		if grep -q $'^race-repeat\tpass\t' "$DIR/gates.tsv"; then
			rm -f "$DIR/race-repeat.log" "$DIR/race-repeat.events.jsonl"
			part_say "race-repeat passed: race-repeat.log and race-repeat.events.jsonl left out of the part"
		fi
		;;
	fuzz)
		# The nightly fuzz lane: every Fuzz target, sharing FUZZ_BUDGET. A
		# crasher is kept under crashers/ in the part, which the part's
		# artifact carries.
		part_run fuzz fuzz.log "$PR" fuzz -out "$DIR" -budget "$FUZZ_BUDGET" || true
		;;
	integration:*)
		local k="${PART#integration:}"
		PART_TEMPORAL_RAN=""
		part_run temporal temporal.log part_start_temporal || true
		STACK_ENV+=(ORK_TEMPORAL_ADDRESS="$TEMPORAL_ADDRESS")
		part_run binaries binaries.log part_binaries || true
		part_run images images.log part_push_images || true
		PART_IMAGES=1
		git show "$BASE_COMMIT:scripts/proof-quarantine.txt" >"$DIR/quarantine-base.txt" 2>/dev/null || : >"$DIR/quarantine-base.txt"
		part_run integration integration.log env "${STACK_ENV[@]}" ORK_REQUIRE_INFRA=1 \
			"$PR" integration -plan "$PLAN" -shard "$k" -out "$DIR" -jobs "$PROOF_JOBS" \
			-quarantine scripts/proof-quarantine.txt -quarantine-base "$DIR/quarantine-base.txt" \
			-namespace-prefix "$PROOF_ID-s$k" -print-progress || true
		;;
	ui-e2e:*)
		local k="${PART#ui-e2e:}" image
		# The browser-server image the suite's global setup runs Chromium in
		# (ui/e2e/browser-server.ts), pinned by digest in ui/e2e/browser-image.txt.
		image="$(grep -v '^#' "$REPO/ui/e2e/browser-image.txt" | grep -v '^$' | head -n1)"
		docker image inspect "$image" >/dev/null 2>&1 || docker pull "$image" >"$DIR/browser-image-pull.log" 2>&1 || part_say "proof: cannot pull $image"
		PART_TEMPORAL_RAN=""
		part_run temporal temporal.log part_start_temporal || true
		STACK_ENV+=(ORK_TEMPORAL_ADDRESS="$TEMPORAL_ADDRESS")
		part_run binaries binaries.log part_binaries || true
		part_run images images.log part_push_images || true
		PART_IMAGES=1
		local -a files=()
		mapfile -t files < <(jq -r --argjson k "$k" '.shards[] | select(.index == $k) | .files[]' "$UI_PLAN")
		if [ ${#files[@]} -eq 0 ]; then
			part_say "proof: UI shard $k holds no file"
			TREE_END="$(part_tree_hash)"; CLEAN_END=false; part_is_clean && CLEAN_END=true
			part_record 1
			return 1
		fi
		local -a rel=()
		local f
		for f in "${files[@]}"; do rel+=("${f#ui/}"); done
		(
			cd "$REPO/ui"
			env "${STACK_ENV[@]}" ORK_PROOF_DIR="$DIR/artifacts/ui" \
				"$REPO/ui/node_modules/.bin/playwright" test --list --reporter=json "${rel[@]}" >"$DIR/ui-e2e-list.json"
		) >"$DIR/ui-e2e.log" 2>&1 || true
		# As scripts/proof.sh ui_e2e_locked: one invocation on one worker; with
		# more workers the parallel project first, then the solo projects (the
		# render-budget files) on one worker, and the ledger reads both reports.
		# The shard's files come before --project, which would otherwise take
		# them as project names; a shard may hold no file of one project.
		part_playwright() { # <report> <workers> <args...>
			local report="$1" workers="$2"
			shift 2
			(
				cd "$REPO/ui"
				env "${STACK_ENV[@]}" ORK_PROOF_DIR="$DIR/artifacts/ui" \
					PLAYWRIGHT_JSON_OUTPUT_FILE="$report" \
					PLAYWRIGHT_JSON_OUTPUT_NAME="$report" \
					"$REPO/ui/node_modules/.bin/playwright" test --workers "$workers" --reporter=list,json "$@"
			) >>"$DIR/ui-e2e.log" 2>&1
		}
		local reports=(-report "$DIR/ui-e2e-report.json")
		if [ "$PROOF_UI_WORKERS" -gt 1 ]; then
			part_playwright "$DIR/ui-e2e-report.json" "$PROOF_UI_WORKERS" "${rel[@]}" --pass-with-no-tests --project chromium || true
			part_playwright "$DIR/ui-e2e-report-solo.json" 1 "${rel[@]}" --pass-with-no-tests --project chromium-solo-each-fan-out --project chromium-solo-run-page-ceiling || true
			reports+=(-report "$DIR/ui-e2e-report-solo.json")
		else
			part_playwright "$DIR/ui-e2e-report.json" 1 "${rel[@]}" || true
		fi
		part_run ui-e2e ui-e2e-ledger.log "$PR" ui-ledger -list "$DIR/ui-e2e-list.json" \
			"${reports[@]}" -plan "$UI_PLAN" -log "$DIR/ui-e2e.log" \
			-config "$REPO/ui/playwright.config.ts" -out "$DIR" -shard "$k" -part "$PART" \
			-host-speed scripts/proof-host-speed.tsv -host-speed-base "$(dirname "$UI_PLAN")/host-speed-base.txt" || true
		;;
	activity-fixture:*)
		local a="${PART#activity-fixture:}"
		PART_TEMPORAL_RAN=""
		part_run images images.log part_push_images || true
		PART_IMAGES=1
		part_run activity-fixture activity-fixture.log make -C images contract ACTIVITY="$a" TAG="$PROOF_ID" REGISTRY="$REGISTRY" || true
		;;
	observability)
		PART_TEMPORAL_RAN=""
		part_run temporal temporal.log part_start_temporal || true
		STACK_ENV+=(ORK_TEMPORAL_ADDRESS="$TEMPORAL_ADDRESS")
		part_run binaries binaries.log part_binaries || true
		part_run images images.log part_push_images || true
		PART_IMAGES=1
		part_run observability observability.log env "${STACK_ENV[@]}" scripts/proof-observability.sh || true
		;;
	esac

	local code=0
	if grep -q $'\tfail\t' "$DIR/gates.tsv" 2>/dev/null; then code=1; fi
	TREE_END="$(part_tree_hash)"
	CLEAN_END=false
	if part_is_clean; then CLEAN_END=true; fi
	part_record "$code" || true
	part_say "proof: part $PART -> $DIR/part.json (gates: $(wc -l <"$DIR/gates.tsv" 2>/dev/null || echo 0))"
	return "$code"
}
