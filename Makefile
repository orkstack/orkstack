# OrkStack — developer entrypoints. Single Go module; UI is a Bun project.
SHELL := /bin/bash
# Where `make tools` installs the codegen binaries, and therefore what
# `make proto` and `make tsgen` have to find on PATH. `go install` writes to
# GOBIN when it is set and to GOPATH/bin otherwise — and those are different
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
tools: ## Install codegen + lint tooling
	go install github.com/bufbuild/buf/cmd/buf@latest
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install connectrpc.com/connect/cmd/protoc-gen-connect-go@latest

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

.PHONY: tidy
tidy: ## go mod tidy
	go mod tidy

## ---- test ----
.PHONY: test
test: ## Run unit tests
	go test ./... -count=1

.PHONY: test-race
test-race: ## Run tests with the race detector
	go test ./... -race -count=1

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
infra-up: ## Start Temporal + SeaweedFS (the dev S3 store)
	$(COMPOSE) up -d
	@echo "Temporal UI: http://localhost:8233  |  SeaweedFS filer: http://localhost:8888"

.PHONY: infra-down
infra-down: ## Stop infrastructure
	$(COMPOSE) down

.PHONY: infra-logs
infra-logs: ## Tail infrastructure logs
	$(COMPOSE) logs -f

.PHONY: infra-up-observability
infra-up-observability: ## Start Temporal + SeaweedFS + the observability profile: collector, Loki, Tempo, Prometheus, Grafana
	$(COMPOSE) --profile observability up -d
	@echo "Temporal UI: http://localhost:8233  |  SeaweedFS filer: http://localhost:8888  |  Grafana: http://localhost:3000"
	@echo "Point ork/orker at it: see docs/DEPLOY.md, \"Telemetry\", for the OTEL_*/ORK_ADMIN_ADDR env recipe."

.PHONY: infra-down-observability
infra-down-observability: ## Stop infrastructure, including the observability profile
	$(COMPOSE) --profile observability down

## ---- run ----
.PHONY: run-ork
run-ork: ## Run the ork engine (Connect API + WS + Temporal client)
	go run ./cmd/ork

.PHONY: run-orker
run-orker: ## Run an Orker (Temporal activity worker + OCI runtime)
	go run ./cmd/orker

## ---- quality ----
.PHONY: lint
lint: ## Run go vet + gofmt check
	go vet ./...
	@test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './gen/*'))" || (echo "gofmt needed:"; gofmt -l $$(find . -name '*.go' -not -path './gen/*'); exit 1)

.PHONY: fmt
fmt: ## Format Go code
	gofmt -w $$(find . -name '*.go' -not -path './gen/*')

.PHONY: check
check: ## The contributor's fast path, no Docker: gofmt, vet, vet the integration suites, unit tests (-short, test cache kept)
	$(MAKE) lint
	go vet -tags=integration ./...
	go test -short ./...

.PHONY: merge-check
merge-check: build lint test ## Gate a merge: build, lint, unit, vet the integration suites; with PROOF=proof/<name>, also require that full proof of exactly this tree (AGENTS.md §11.3)
	go vet -tags=integration ./...
	@echo "merge-check OK: the tree builds, passes lint and unit tests, and its integration suites compile"
ifneq ($(strip $(PROOF)),)
	go run ./test/tools/proofrun verify -proof $(PROOF) $(if $(NIGHTLY),-nightly $(NIGHTLY))
else
	@echo "NOTE: COMPILE, not run. Without PROOF=proof/<name> this gate needs no"
	@echo "      infrastructure, and an integration test can be red while it is green."
	@echo "      A merge needs a full proof of the candidate: make proof, then"
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

.PHONY: proof-feature
proof-feature: ## A feature proof of this branch, never merge evidence: TESTS='PKG:REGEX ...' SPECS='FILE ...' [BASE=main] -> proof/<NAME>-feature/
	@scripts/proof.sh --tier feature $(if $(BASE),--base $(BASE)) $(foreach t,$(TESTS),--tests '$(t)') $(foreach s,$(SPECS),--specs '$(s)') $(NAME)

.PHONY: proof-ui
proof-ui: ## Alias of proof: the full proof already runs the UI type-check, unit and Playwright suites
	@scripts/proof.sh $(NAME)

.PHONY: proof-nightly
proof-nightly: ## The nightly tier: only the deferred scale and soak tests and specs, on one host -> proof/<NAME>-nightly/
	@scripts/proof.sh --tier nightly $(if $(NAME),$(NAME),nightly)

.PHONY: proof-release
proof-release: ## The pre-release gate: every gate, observability always -> proof/<NAME>-release/
	@scripts/proof.sh --tier release $(if $(NAME),$(NAME),release)

# Release artifacts (docs/UPGRADING.md, "Release artifacts"). release.yml
# runs these three for every release; each also runs by hand. VERSION must be
# vMAJOR.MINOR.PATCH here.
#   make dist             the binaries, their archives and SHA256SUMS -> dist/
#   make release-images   the release images as OCI layouts -> OUT/<image>/
#                         (needs make dist; PLATFORMS defaults to this machine)
#   make release-smoke    builds both for this machine, runs the quickstart
#                         bundle on them and requires a finished run
DIST ?= dist
PLATFORMS ?=
RELEASE_IMAGES := ork orker orkctl ui
SOURCE_DATE_EPOCH ?= $(shell git log -1 --format=%ct 2>/dev/null)
# Every archive and image is stamped with the commit's time, never a clock's.
need-commit-time = $(if $(strip $(SOURCE_DATE_EPOCH)),,$(error no commit time: run from a git checkout or set SOURCE_DATE_EPOCH))

.PHONY: dist
dist: ## Release binaries, archives and SHA256SUMS for VERSION=vX.Y.Z -> dist/ (gitignored)
	$(need-commit-time)
	go run ./internal/dist/cmd/dist build -version $(VERSION) -out $(DIST) -commit-time $(SOURCE_DATE_EPOCH)

.PHONY: release-images
release-images: ## Release images (ork, orker, orkctl, ui, activities) as OCI layouts: VERSION=vX.Y.Z OUT=<dir> [PLATFORMS=linux/amd64,linux/arm64]
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
release-smoke: ## Build the release for this machine and run the quickstart bundle on it end to end: VERSION=vX.Y.Z
	@scripts/release-smoke.sh $(VERSION)

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
github-merge-gate: ## Check (APPLY=1 applies) the required proof check; not enforced until the repository is public
	@APPLY=$(or $(APPLY),0) QUEUE=$(or $(QUEUE),0) CODEOWNER_REVIEW=$(or $(CODEOWNER_REVIEW),1) scripts/github-merge-gate.sh

.PHONY: proof-stop
proof-stop: ## Stop one proof and everything it started: PROOF=proof/<name>
	@scripts/proof-stop.sh $(PROOF)

.PHONY: proof-clean
proof-clean: ## List what test processes that are gone left (buckets, work roots, containers); REMOVE=1 removes it
	@scripts/proof-clean.sh $(if $(REMOVE),--remove)

.PHONY: proof-slow-list
proof-slow-list: ## Regenerate scripts/proof-test-costs.tsv and scripts/proof-slow-tests.txt from a full proof: FROM=proof/<name>
	go run ./test/tools/proofrun costs -from $(FROM)

.PHONY: proof-overhead
proof-overhead: ## Report where an existing proof's wall-clock went and its per-process overhead, running nothing: FROM=proof/<name> [OUT=FILE.md]
	go run ./test/tools/proofrun overhead -from $(FROM) $(if $(OUT),-out $(OUT))

.PHONY: proof-flakes
proof-flakes: ## Derive per-test flake rates from the proof ledgers: FROM='proof/*' [REPO=.]
	go run ./test/tools/proofrun flakes -from '$(or $(FROM),proof/*)' $(if $(REPO),-repo $(REPO))

