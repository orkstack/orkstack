# OrkStack — developer entrypoints. Single Go module; UI is a Bun project.
SHELL := /bin/bash
GOPATH_BIN := $(shell go env GOPATH)/bin
export PATH := $(GOPATH_BIN):$(PATH)

COMPOSE := docker compose -f deploy/docker-compose.yml

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n",$$1,$$2}'

## ---- codegen ----
.PHONY: proto
proto: ## Regenerate Go/TS from proto
	buf lint
	buf generate

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
.PHONY: build
build: ## Build all binaries into ./bin
	@mkdir -p bin
	go build -o bin/orkengine ./cmd/orkengine
	go build -o bin/orker    ./cmd/orker
	go build -o bin/orkctl   ./cmd/orkctl

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
cover: ## Run tests with coverage -> proof/coverage.out
	@mkdir -p proof
	go test ./... -count=1 -coverprofile=proof/coverage.out
	go tool cover -func=proof/coverage.out | tail -1

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

## ---- run ----
.PHONY: run-engine
run-engine: ## Run the engine (Connect API + WS + Temporal client)
	go run ./cmd/orkengine

.PHONY: run-orker
run-orker: ## Run an Orker (generic Temporal worker + plugins)
	go run ./cmd/orker

## ---- quality ----
.PHONY: lint
lint: ## Run go vet + gofmt check
	go vet ./...
	@test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './gen/*'))" || (echo "gofmt needed:"; gofmt -l $$(find . -name '*.go' -not -path './gen/*'); exit 1)

.PHONY: fmt
fmt: ## Format Go code
	gofmt -w $$(find . -name '*.go' -not -path './gen/*')
