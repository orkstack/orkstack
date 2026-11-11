# OrkStack — developer entrypoints. Single Go module; UI is a Bun project.
SHELL := /bin/bash
# Where `make tools` installs buf, and therefore where `make proto` has to
# find it on PATH. `go install` writes to GOBIN when it is set and to
# GOPATH/bin otherwise — and those are different
# directories the moment a toolchain manager is involved, because mise and
# friends export GOBIN to a version-specific path while GOPATH still points at
# $HOME/go. Assuming GOPATH/bin put the tools on PATH only by accident: it held
# when the manager's GOBIN happened to be on PATH already, and left `buf: not
# found` behind when it was not. GOPATH defaults to $HOME/go, so the fallback
# is the same directory `go install` would have chosen.
GOBIN_DIR := $(shell go env GOBIN)
ifeq ($(GOBIN_DIR),)
GOBIN_DIR := $(HOME)/go/bin
endif
export PATH := $(GOBIN_DIR):$(PATH)

COMPOSE := docker compose -f deploy/docker-compose.yml

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n",$$1,$$2}'

## ---- codegen ----
.PHONY: proto
proto: ## Regenerate Go/TS and docs/API.md from proto
	buf lint
	buf generate
	go run ./internal/apidocs/cmd/genapidocs

# PROTO_BASE is the release the proto contract is checked against: the first,
# so no change breaks a client of any release since. buf runs from the image
# deploy/buf-image.txt pins, with no network, against the tag's own tree,
# which git exports; buf.yaml's breaking rules decide.
PROTO_BASE ?= v0.1.0
# A literal #, which a recipe-free line would otherwise read as a comment.
HASH := \#
BUF_IMAGE = $(shell grep -v -e '^$(HASH)' -e '^$$' deploy/buf-image.txt | head -1)
# BUF_VERSION is that image's release, which `make tools` installs too, so the
# buf that lints and generates is the buf that checks.
BUF_VERSION = $(patsubst bufbuild/buf:%,%,$(firstword $(subst @, ,$(BUF_IMAGE))))

.PHONY: proto-breaking
proto-breaking: ## Refuse a proto change that breaks the release PROTO_BASE's contract (default v0.1.0), with the pinned buf
	@test -n "$(BUF_IMAGE)" || { echo "proto-breaking: deploy/buf-image.txt names no image" >&2; exit 2; }
	@git rev-parse -q --verify "refs/tags/$(PROTO_BASE)^{commit}" >/dev/null || \
		{ echo "proto-breaking: no tag $(PROTO_BASE) in this clone; fetch it: git fetch origin tag $(PROTO_BASE)" >&2; exit 2; }
	@set -euo pipefail; base="$$(mktemp -d)"; trap 'rm -rf "$$base"' EXIT; \
	git archive "refs/tags/$(PROTO_BASE)" buf.yaml proto | tar -x -C "$$base"; \
	docker run --rm --network none --user "$$(id -u):$$(id -g)" -e HOME=/tmp \
		-v "$(CURDIR):/work:ro" -v "$$base:/base:ro" -w /work "$(BUF_IMAGE)" breaking --against /base; \
	echo "proto-breaking: the proto contract keeps $(PROTO_BASE)'s"

.PHONY: config-defaults
config-defaults: ## Regenerate deploy/orkstack.defaults.yaml from the code's defaults
	go run ./internal/config/cmd/gendefaults > deploy/orkstack.defaults.yaml

.PHONY: tsgen
tsgen: ## Install UI codegen deps and regenerate Go+TS clients from proto
	cd ui && bun install
	ui/node_modules/.bin/buf lint
	ui/node_modules/.bin/buf generate
	go run ./internal/apidocs/cmd/genapidocs

.PHONY: tools
# buf is installed at the release deploy/buf-image.txt pins (BUF_VERSION), the
# buf `make proto-breaking` checks with, and ui/package.json must pin that same
# release for @bufbuild/buf, the buf `make tsgen` runs: the recipe refuses the
# two apart. buf is not a go.mod tool directive because its module graph would
# join this module's. The Go protoc plugins and the linters are tool
# directives and need no install.
tools: ## Install buf, the codegen driver `make proto` runs
	@test -n "$(BUF_VERSION)" || { echo "tools: deploy/buf-image.txt names no buf release" >&2; exit 2; }
	@v="$$(sed -n 's/^[[:space:]]*"@bufbuild\/buf": "\([0-9][0-9.]*\)",*$$/\1/p' ui/package.json)"; \
	test "$$v" = "$(BUF_VERSION)" || { echo "tools: ui/package.json pins @bufbuild/buf '$$v', deploy/buf-image.txt pins buf $(BUF_VERSION)" >&2; exit 2; }
	go install github.com/bufbuild/buf/cmd/buf@v$(BUF_VERSION)

## ---- build ----
#
# VERSION is what a built binary calls itself: `orkctl version` and the admin
# surface's engine_version both report it. It is derived from the tree rather
# than hand-maintained — a release tag when the commit has one, the short
# revision otherwise, `-dirty` when the working tree is not clean — so two
# builds of two different trees can never claim the same identity. A tree
# with no git at all falls back to "dev", which is exactly what the source
# default in cmd/ork and cmd/orkctl already says.
#
# Override it for a release build: `make build VERSION=vMAJOR.MINOR.PATCH`.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
GO_LDFLAGS := -X main.version=$(VERSION)

.PHONY: build
build: ## Build all binaries into ./bin
	@mkdir -p bin
	go build -ldflags '$(GO_LDFLAGS)' -o bin/ork      ./cmd/ork
	go build -ldflags '$(GO_LDFLAGS)' -o bin/orker    ./cmd/orker
	go build -ldflags '$(GO_LDFLAGS)' -o bin/orkctl   ./cmd/orkctl

# The web UI as ork embeds it (internal/webui): built in the pinned bun image
# by deploy/source/Dockerfile's ui stage, the one the quickstart's ork
# compiles in, so it needs Docker and no Bun here, and stamped with the
# commit's time (SOURCE_DATE_EPOCH, below), so one commit builds the same
# bytes. It replaces what internal/webui/dist holds but its committed
# .gitkeep; an ork built without it serves a placeholder page, and `make
# dist` runs it first, so a release's ork always carries it.
.PHONY: ui
ui: ## Build the web UI into internal/webui/dist, where ork embeds it
	@set -euo pipefail; out="$$(mktemp -d)"; trap 'rm -rf "$$out"' EXIT; \
	docker buildx build -f deploy/source/Dockerfile --target ui \
	  $(if $(strip $(SOURCE_DATE_EPOCH)),--build-arg SOURCE_DATE_EPOCH=$(SOURCE_DATE_EPOCH)) \
	  --output "type=local,dest=$$out" .; \
	test -f "$$out/index.html" || { echo "make ui: the build wrote no index.html" >&2; exit 1; }; \
	find internal/webui/dist -mindepth 1 -maxdepth 1 ! -name .gitkeep -exec rm -rf {} +; \
	cp -R "$$out/." internal/webui/dist/; \
	echo "make ui: the UI's build is in internal/webui/dist; the next build of ork embeds it"

.PHONY: tidy
tidy: ## go mod tidy
	go mod tidy

## ---- test ----
.PHONY: test
test: ## Run unit tests
	go test ./... -count=1

# The race gate's run, without its ledger (docs/PROOFS.md §2.3): the unit suites
# under the race detector with -short, which skips the scale tests the unit
# gate runs without it. RACE_COUNT=3 RACE_PACKAGES='./internal/engine/...'
# is the shape of the nightly repeat. The detector needs cgo and a C compiler.
# go test's -timeout covers a test binary across every -count repeat, so it is
# RACE_MINUTES_PER_RUN times RACE_COUNT, as the gate gives it
# (raceTimeoutPerRun, test/tools/proofrun/run.go; a test holds the two equal).
RACE_COUNT ?= 1
RACE_PACKAGES ?= ./...
RACE_MINUTES_PER_RUN ?= 30
RACE_TIMEOUT ?= $(shell echo $$(( $(RACE_MINUTES_PER_RUN) * $(RACE_COUNT) )))m
.PHONY: test-race
test-race: ## Run the unit tests under the race detector, -short (RACE_COUNT=N repeats, RACE_PACKAGES='./pkg/...' narrows)
	go test -race -short -count=$(RACE_COUNT) -timeout $(RACE_TIMEOUT) $(RACE_PACKAGES)

# The nightly fuzz lane's run (docs/PROOFS.md §2.3): every Fuzz target of the
# module, sharing FUZZ_BUDGET between them (or FUZZTIME each, when it is set), a
# ledger and a log per target under FUZZ_OUT, and the crasher input of a target
# that fails kept under FUZZ_OUT/crashers.
FUZZ_BUDGET ?= 45m
FUZZTIME ?=
FUZZ_OUT ?= .data/fuzz/$(shell date -u +%Y%m%dT%H%M%SZ)
.PHONY: fuzz
fuzz: ## Fuzz every Fuzz target, sharing FUZZ_BUDGET (default 45m) or FUZZTIME each; crashers are kept under FUZZ_OUT
	go run ./test/tools/proofrun fuzz -out $(FUZZ_OUT) $(if $(FUZZTIME),-fuzztime $(FUZZTIME),-budget $(FUZZ_BUDGET))

.PHONY: cover
cover: ## Run tests with coverage -> coverage.out
	go test ./... -count=1 -coverprofile=coverage.out
	go tool cover -func=coverage.out | tail -1

## ---- whole stack ----
.PHONY: up
up: ## Start everything: Temporal, SeaweedFS, engine, orker and the UI
	@scripts/stack.sh up

.PHONY: down
down: ## Stop everything `make up` started (data is kept)
	@scripts/stack.sh down

.PHONY: status
status: ## Show what is running, and where
	@scripts/stack.sh status

.PHONY: logs
logs: ## Tail the engine, orker and UI logs
	@tail -f .run/ork.log .run/orker.log .run/ui.log

.PHONY: reset
reset: ## Wipe ALL local state — Temporal history, object-store data, build output
	@scripts/reset.sh

.PHONY: reset-force
reset-force: ## reset without the confirmation prompt
	@scripts/reset.sh --force

## ---- infra ----
.PHONY: infra-up
infra-up: ## Start Temporal + SeaweedFS (the dev S3 store), and wait until they are healthy
	$(COMPOSE) up -d --wait
	@echo "Temporal UI: http://localhost:8233  |  SeaweedFS filer: http://localhost:8888"

.PHONY: infra-down
infra-down: ## Stop infrastructure
	$(COMPOSE) down

.PHONY: infra-logs
infra-logs: ## Tail infrastructure logs
	$(COMPOSE) logs -f

.PHONY: infra-up-observability
infra-up-observability: ## Start Temporal + SeaweedFS + the observability profile: collector, Loki, Tempo, Prometheus, Grafana, and wait for them
	$(COMPOSE) --profile observability up -d --wait
	@echo "Temporal UI: http://localhost:8233  |  SeaweedFS filer: http://localhost:8888  |  Grafana: http://localhost:3000"
	@echo "Point ork/orker at it: see docs/DEPLOY.md, \"Telemetry\", for the OTEL_*/ORK_ADMIN_ADDR env recipe."

.PHONY: infra-down-observability
infra-down-observability: ## Stop infrastructure, including the observability profile
	$(COMPOSE) --profile observability down

## ---- run ----
# The development store's key: no binary falls back to one, and the store
# refuses any other. A key already set is kept.
run-ork run-orker: export ORK_S3_ACCESS_KEY ?= orkstack
run-ork run-orker: export ORK_S3_SECRET_KEY ?= orkstack-secret

.PHONY: run-ork
run-ork: ## Run the ork engine (Connect API + WS + Temporal client)
	go run ./cmd/ork

.PHONY: run-orker
run-orker: ## Run an Orker (Temporal activity worker + OCI runtime)
	go run ./cmd/orker

## ---- quality ----
# staticcheck is the version go.mod pins (a `tool` directive), run with its
# default checks over the module twice: once as built, once with the
# integration tag the integration suites are vetted with. A symbol used only by
# an integration-tagged file is dead to the first run and live to the second,
# so each run answers for the files it compiles and the tree needs both clean.
.PHONY: lint
lint: ## Run go vet, the gofmt check and staticcheck (pinned in go.mod; default checks, with and without the integration tag)
	go vet ./...
	@test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './gen/*'))" || (echo "gofmt needed:"; gofmt -l $$(find . -name '*.go' -not -path './gen/*'); exit 1)
	go tool staticcheck ./...
	go tool staticcheck -tags=integration ./...

# govulncheck is the version go.mod pins. It reads the vulnerability database
# over the network, so it is in neither `lint` nor any proof gate: a network
# failure must never fail a proof. ci.yml and nightly.yml run it. It reports
# only the vulnerabilities whose vulnerable symbols the code can reach.
.PHONY: vuln
vuln: ## Scan for reachable known vulnerabilities with the pinned govulncheck (needs the network; in no lint or proof gate)
	go tool govulncheck ./...

# vuln-js and vuln-images read advisory databases over the network too, so like
# vuln they are in no lint or proof gate; nightly.yml runs both. vuln-js audits
# the UI's and the site's lockfiles with bun audit, which installs nothing: it
# lists every advisory and fails on one of high or critical severity,
# development dependencies included, because svelte and SvelteKit compile into
# the UI's bundle.
.PHONY: vuln-js
vuln-js: ## Audit the UI's and the site's JavaScript dependencies with bun audit, failing on a high or critical advisory (needs the network; in no lint or proof gate)
	@failed=; for d in ui site; do \
	  echo "bun audit ($$d): every advisory"; \
	  (cd $$d && bun audit) || true; \
	  echo "bun audit ($$d): high or critical, which fail the audit"; \
	  (cd $$d && bun audit --audit-level=high) || failed="$$failed $$d"; \
	done; \
	if [ -n "$$failed" ]; then echo "make vuln-js: a high or critical advisory, or an audit that could not run, in:$$failed (bun audit fix --dry-run names the fixed versions)" >&2; exit 1; fi

# vuln-images builds what a release of this commit publishes (VULN_IMAGES: the
# release images and every activity images/Makefile builds, as its
# print-activities names them) for linux/amd64 as OCI layouts, as release.yml
# builds them, and scans each with the Grype image
# deploy/grype-image.txt pins, by the policy deploy/grype.yaml states, with no
# capability, no new privilege and a read-only root.
# LAYOUTS=<dir> scans the layouts already built there instead. Grype's
# database, about 3 GB unpacked, is downloaded into VULN_IMAGES_DIR (on disk,
# never a tmpfs /tmp) and kept for the next scan.
VULN_ACTIVITIES = $(shell MAKEFLAGS= $(MAKE) -s --no-print-directory -C images print-activities)
VULN_IMAGES = $(RELEASE_IMAGES) $(addprefix activity-,$(VULN_ACTIVITIES))
VULN_IMAGES_DIR ?= $(CURDIR)/.data/vuln-images
.PHONY: vuln-images
vuln-images: ## Build the release images for linux/amd64 (or take LAYOUTS=<dir>) and scan each with the pinned Grype, failing on a fixed high or critical vulnerability (needs the network; in no lint or proof gate)
	@set -eu; layouts='$(if $(strip $(LAYOUTS)),$(abspath $(LAYOUTS)),$(VULN_IMAGES_DIR)/layouts)'; \
	if [ -z '$(strip $(LAYOUTS))' ]; then \
	  $(MAKE) dist VERSION=v0.0.0; \
	  $(MAKE) release-images VERSION=v0.0.0 OUT="$$layouts" PLATFORMS=linux/amd64; \
	fi; \
	grype="$$(grep -v '^#' deploy/grype-image.txt | head -1)"; \
	mkdir -p '$(VULN_IMAGES_DIR)/db' '$(VULN_IMAGES_DIR)/tmp'; failed=; \
	[ -n '$(strip $(VULN_ACTIVITIES))' ] || { echo "make vuln-images: images/Makefile names no activity to scan" >&2; exit 1; }; \
	for t in $(VULN_IMAGES); do \
	  echo "grype: $$t"; \
	  docker run --rm --user "$$(id -u):$$(id -g)" --cap-drop=ALL --security-opt no-new-privileges --read-only \
	    -e GRYPE_DB_CACHE_DIR=/db -e TMPDIR=/scratch \
	    -v '$(VULN_IMAGES_DIR)/db:/db' -v '$(VULN_IMAGES_DIR)/tmp:/scratch' \
	    -v "$$layouts:/layouts:ro" -v '$(CURDIR)/deploy/grype.yaml:/grype.yaml:ro' \
	    "$$grype" -c /grype.yaml --platform linux/amd64 "oci-dir:/layouts/$$t" || failed="$$failed $$t"; \
	done; \
	if [ -n "$$failed" ]; then echo "make vuln-images: a fixed high or critical vulnerability, or a scan that could not run, in:$$failed" >&2; exit 1; fi

# A published release's notes are its CHANGELOG.md section, word for word
# (docs/PROOFS.md §10). Both targets read the releases with gh over the
# network, so like vuln they are in no lint or proof gate; nightly.yml runs
# release-notes-check. release-notes-publish is the one way to change a
# release's notes: it shows the diff and asks before it edits the release.
.PHONY: release-notes-check
release-notes-check: ## Check every published release's notes against its CHANGELOG.md section, naming the first line that differs (needs the network and gh; in no lint or proof gate)
	@scripts/release-notes.sh check

.PHONY: release-notes-publish
release-notes-publish: ## Republish a release's notes from its CHANGELOG.md section, keeping its image footer, after a diff and a yes: VERSION=vX.Y.Z (needs gh)
	@test -n "$(VERSION)" || (echo "release-notes-publish needs VERSION=vX.Y.Z"; exit 2)
	@scripts/release-notes.sh publish $(VERSION)

.PHONY: fmt
fmt: ## Format Go code
	gofmt -w $$(find . -name '*.go' -not -path './gen/*')

# GIT_STATE_PACKAGES hold the unit tests that run git, most of them to read
# the repository's own state: HEAD, the index, the ignore rules. go test's cache
# records only the files the test process itself opens inside the module, so it
# never sees what git reads. make check runs these packages uncached, so a
# change only to what git tracks or ignores re-runs them;
# TestMakeCheck_RunsGitStatePackagesUncached keeps the list whole.
GIT_STATE_PACKAGES := ./internal/docscheck ./internal/engine ./test/tools/proofrun

.PHONY: check
check: ## The contributor's fast path, no Docker: gofmt, vet, vet the integration suites, unit tests (-short, test cache kept but for GIT_STATE_PACKAGES), the UI's type-check and unit tests
	$(MAKE) lint
	go vet -tags=integration ./...
	go test -short $$(go list ./... | grep -vxF "$$(go list $(GIT_STATE_PACKAGES))")
	go test -short -count=1 $(GIT_STATE_PACKAGES)
	cd ui && bun install --frozen-lockfile && bun run check && bun test test/

.PHONY: merge-check
merge-check: build lint test ## Gate a merge: build, lint, unit, vet the integration suites; with PROOF=proof/<name>, also require that full proof of exactly this tree, and with NIGHTLY=proof/<name> verify that nightly proof of main's tip (AGENTS.md §11.3; none is required)
	go vet -tags=integration ./...
	@echo "merge-check OK: the tree builds, passes lint and unit tests, and its integration suites compile"
ifneq ($(strip $(PROOF)),)
	go run ./test/tools/proofrun verify -proof $(PROOF) $(if $(NIGHTLY),-nightly $(NIGHTLY))
else
	@echo "NOTE: COMPILE, not run. Without PROOF=proof/<name> this gate needs no"
	@echo "      infrastructure, and an integration test can be red while it is green."
	@echo "      A merge needs a full proof of the candidate: by default CI's, from a"
	@echo "      validate/full/<name> push (docs/PROOFS.md §10), or make proof; then"
	@echo "      make merge-check PROOF=proof/<name> (AGENTS.md §11.3)."
endif

.PHONY: test-integration
test-integration: ## Run every integration suite as concurrent test processes, PROOF_JOBS at a time, with a ledger (needs: make infra-up && make -C images push)
	@scripts/integration.sh

.PHONY: api-reference
api-reference: ## Regenerate docs/API.md from the proto sources' comments
	go run ./internal/apidocs/cmd/genapidocs

.PHONY: site
site: ## Check and build the site (site/): drift check, astro check, astro build, link checks -> site/dist
	cd site && bun install --frozen-lockfile && bun run check && bun run build

.PHONY: site-media
site-media: ## Regenerate the site's screenshots and recordings from the running dev stack (needs: make infra-up, make -C images push, make up)
	site/media/run.sh

.PHONY: cli-reference
cli-reference: ## Regenerate docs/CLI.md from the Cobra tree
	ORK_WRITE_CLI_REFERENCE=1 go test ./cmd/orkctl -run '^TestCLIReference_IsGenerated$$' -count=1

## ---- evidence (AGENTS.md §11.2) ----
.PHONY: proof
proof: ## The full proof, the merge gate: every gate, every test, every spec -> proof/<NAME>/ (gitignored)
	@scripts/proof.sh $(NAME)

.PHONY: proof-ui
proof-ui: ## Alias of proof: the full proof already runs the UI type-check, unit and Playwright suites
	@scripts/proof.sh $(NAME)

.PHONY: proof-nightly
proof-nightly: ## The nightly tier: the deferred scale and soak tests and specs, the race repeat and the fuzz lane, on one host -> proof/<NAME>-nightly/
	@scripts/proof.sh --tier nightly $(if $(NAME),$(NAME),nightly)

.PHONY: proof-release
proof-release: ## The pre-release gate: every gate, observability always -> proof/<NAME>-release/
	@scripts/proof.sh --tier release $(if $(NAME),$(NAME),release)

# Release artifacts (docs/UPGRADING.md, "Release artifacts"). release.yml
# runs these for every release; each also runs by hand. VERSION must be
# vMAJOR.MINOR.PATCH here.
#   make dist             the web UI (make ui), then the binaries, their
#                         archives and SHA256SUMS -> dist/; ork embeds the UI
#   make release-images   the release images as OCI layouts -> OUT/<image>/:
#                         ork, orker, orkctl and the activities (needs make
#                         dist; PLATFORMS defaults to this machine)
#   make release-smoke    builds both for this machine, runs compose.yaml on
#                         those images and requires a finished run
#   make quickstart-smoke runs the README's Quickstart as written on a fresh
#                         clone of this working tree (no VERSION)
#   make examples-smoke   the same, then every shipped example with no inputs;
#                         reaches httpbin.org and github.com
DIST ?= dist
PLATFORMS ?=
RELEASE_IMAGES := ork orker orkctl
SOURCE_DATE_EPOCH ?= $(shell git log -1 --format=%ct 2>/dev/null)
# Every archive and image is stamped with the commit's time, never a clock's.
need-commit-time = $(if $(strip $(SOURCE_DATE_EPOCH)),,$(error no commit time: run from a git checkout or set SOURCE_DATE_EPOCH))
# Every release binary is stamped with the commit it came from, which it reports
# (`orkctl version`), because a release builds with -buildvcs=false.
SOURCE_COMMIT ?= $(shell git rev-parse HEAD 2>/dev/null)
need-commit = $(if $(strip $(SOURCE_COMMIT)),,$(error no commit: run from a git checkout or set SOURCE_COMMIT))

.PHONY: dist
dist: ui ## The web UI (make ui), then release binaries, archives and SHA256SUMS for VERSION=vX.Y.Z -> dist/ (gitignored)
	$(need-commit-time)
	$(need-commit)
	go run ./internal/dist/cmd/dist build -version $(VERSION) -commit $(SOURCE_COMMIT) -out $(DIST) -commit-time $(SOURCE_DATE_EPOCH)

.PHONY: release-images
release-images: ## Release images (ork, orker, orkctl, activities) as OCI layouts: VERSION=vX.Y.Z OUT=<dir> [PLATFORMS=linux/amd64,linux/arm64]
	$(if $(strip $(OUT)),,$(error make release-images needs OUT=<directory> for the layouts))
	$(need-commit-time)
	@mkdir -p '$(abspath $(OUT))'
	@for t in $(RELEASE_IMAGES); do \
	  echo "building $$t -> $(abspath $(OUT))/$$t"; \
	  rm -rf "$(abspath $(OUT))/$$t"; \
	  SOURCE_DATE_EPOCH=$(SOURCE_DATE_EPOCH) docker buildx build --provenance=false --sbom=false \
	    $(if $(strip $(PLATFORMS)),--platform $(PLATFORMS)) --build-arg VERSION=$(VERSION) \
	    -f deploy/Dockerfile --target $$t \
	    --output "type=oci,dest=$(abspath $(OUT))/$$t,tar=false,rewrite-timestamp=true" . || exit 1; \
	done
	$(MAKE) -C images oci OUT='$(abspath $(OUT))' PLATFORMS='$(PLATFORMS)' \
	  LABELS='org.opencontainers.image.source=https://github.com/orkstack/orkstack org.opencontainers.image.licenses=MIT org.opencontainers.image.version=$(VERSION)'

.PHONY: release-smoke
release-smoke: ## Build the release for this machine and run compose.yaml on its images end to end: VERSION=vX.Y.Z
	@scripts/quickstart-smoke.sh --release $(VERSION)

.PHONY: quickstart-smoke
quickstart-smoke: ## Run the README's Quickstart as written on a fresh clone of this working tree, end to end
	@scripts/quickstart-smoke.sh

.PHONY: examples-smoke
examples-smoke: ## Run the Quickstart, then every shipped example with no inputs on that fresh stack (reaches httpbin.org and github.com)
	@scripts/quickstart-smoke.sh --examples

.PHONY: release-tag
release-tag: ## Tag HEAD only after a passing release proof of it: VERSION=vX.Y.Z PROOF=proof/<name>
	@test -n "$(VERSION)" || (echo "release-tag needs VERSION=vX.Y.Z"; exit 2)
	@test -n "$(PROOF)" || (echo "release-tag needs PROOF=proof/<name>"; exit 2)
	go run ./test/tools/proofrun verify -release -tag $(VERSION) -proof $(PROOF)
	@git tag -a $(VERSION) -m "OrkStack $(VERSION) — proof $(PROOF), tree $$(git rev-parse HEAD^{tree})"

.PHONY: proof-observability
proof-observability: ## Only the observability gate, with what it needs (needs: make infra-up-observability); never merge-eligible
	@PROOF_GATES=observability scripts/proof.sh $(NAME)

.PHONY: github-merge-gate
github-merge-gate: ## Check (APPLY=1 applies) the required proof check; not enforced until the apply is run; refuses a private repository
	@APPLY=$(or $(APPLY),0) QUEUE=$(or $(QUEUE),0) CODEOWNER_REVIEW=$(or $(CODEOWNER_REVIEW),1) scripts/github-merge-gate.sh

.PHONY: proof-stop
proof-stop: ## Stop one proof and everything it started: PROOF=proof/<name>
	@scripts/proof-stop.sh $(PROOF)

.PHONY: proof-clean
proof-clean: ## List what test processes that are gone left (buckets, work roots, containers); REMOVE=1 removes it
	@scripts/proof-clean.sh $(if $(REMOVE),--remove)

.PHONY: proof-costs
proof-costs: ## Regenerate scripts/proof-test-costs.tsv and scripts/proof-ui-costs.tsv from a full proof: FROM=proof/<name>
	go run ./test/tools/proofrun costs -from $(FROM)

