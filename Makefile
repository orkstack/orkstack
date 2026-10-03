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
proto: ## Regenerate Go/TS from proto
	buf lint
	buf generate

.PHONY: config-defaults
config-defaults: ## Regenerate deploy/orkstack.defaults.yaml from the code's defaults
	go run ./internal/config/cmd/gendefaults > deploy/orkstack.defaults.yaml

.PHONY: tsgen
tsgen: ## Install UI codegen deps and regenerate Go+TS clients from proto
	cd ui && bun install
	ui/node_modules/.bin/buf lint
	ui/node_modules/.bin/buf generate

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
up: ## Start everything: Temporal, MinIO, engine, orker and the UI
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
reset: ## Wipe ALL local state — Temporal history, MinIO objects, build output
	@scripts/reset.sh

.PHONY: reset-force
reset-force: ## reset without the confirmation prompt
	@scripts/reset.sh --force

## ---- infra ----
.PHONY: infra-up
infra-up: ## Start Temporal + MinIO
	$(COMPOSE) up -d
	@echo "Temporal UI: http://localhost:8233  |  MinIO console: http://localhost:9001"

.PHONY: infra-down
infra-down: ## Stop infrastructure
	$(COMPOSE) down

.PHONY: infra-logs
infra-logs: ## Tail infrastructure logs
	$(COMPOSE) logs -f

.PHONY: infra-up-observability
infra-up-observability: ## Start Temporal + MinIO + the observability profile: collector, Loki, Tempo, Prometheus, Grafana
	$(COMPOSE) --profile observability up -d
	@echo "Temporal UI: http://localhost:8233  |  MinIO console: http://localhost:9001  |  Grafana: http://localhost:3000"
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

.PHONY: merge-check
merge-check: build lint test ## Gate a merge: build + lint + unit tests + compile the integration suites
	go vet -tags=integration ./...
	@echo "merge-check OK: the merged tree builds, passes lint and unit tests, and its integration suites compile"
	@echo "NOTE: COMPILE, not run. merge-check needs no infrastructure, so an"
	@echo "      integration test can be red while this gate is green. Run"
	@echo "      'make test-integration' with the stack up before releasing."

.PHONY: test-integration
test-integration: ## Run the integration suites against live infra (needs: make infra-up && make -C images push)
	ORK_REQUIRE_INFRA=1 go test -tags=integration -p 1 -timeout 90m -count=1 ./...

## ---- evidence ----
.PHONY: proof
proof: ## Produce the evidence for a change under proof/<name>/ (gitignored): lint, build, unit, integration
	@scripts/proof.sh $(NAME)

.PHONY: proof-ui
proof-ui: ## proof, plus the UI type-check, unit and Playwright suites with screenshots
	@PROOF_GATES=unit,integration,ui scripts/proof.sh $(NAME)

.PHONY: proof-observability
proof-observability: ## proof (unit) + the observability gate against the compose profile (needs: make infra-up-observability)
	@PROOF_GATES=unit,observability scripts/proof.sh $(NAME)
