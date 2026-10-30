# Contributing to OrkStack

Read [AGENTS.md](./AGENTS.md) first — it is the authoritative engineering
contract. This file is the quick checklist.

## Prerequisites

Trying OrkStack needs only Docker and git: the quickstart builds the whole
stack in containers from a clone ([docs/GETTING_STARTED.md](./docs/GETTING_STARTED.md)).
Developing it needs:

- **Docker** with Compose v2, for the infrastructure, every task and the
  integration suites;
- **Go**, the version in [`go.mod`](./go.mod), to build `ork`, `orker` and
  `orkctl`. A Go-only build of `ork` serves a placeholder page in place of
  the UI until `make ui` builds the UI into it, in Docker;
- **Bun**, the version `.github/workflows/proof.yml` pins, for the UI's dev
  server, checks and tests, and the site;
- **make**, **git** and **curl**.

## The local development stack

The stack built from this checkout, for working on OrkStack itself: the
engine, the Orker and the UI's dev server run on the host, so a change is a
rebuild away, and a change to the UI is on the page as soon as it is saved.
It uses the quickstart's ports (8080, 8233), and 5173 for the UI's dev
server, so stop one before starting the other:

```bash
make infra-up        # Temporal, SeaweedFS and a local registry, in Docker
make -C images push  # the first-party activity images, into that registry
make up              # build ork, orker and orkctl; start them and the UI's dev server
```

`make infra-up` starts [`deploy/docker-compose.yml`](./deploy/docker-compose.yml):
a Temporal development server (set to see a drained release in seconds, not
the default minutes: docs/DEPLOY.md §4, "Underneath"), SeaweedFS as the
S3-compatible object store, and a local OCI registry on `localhost:5001`.
`make -C images push` builds the first-party activities (`http`, `shell`,
`file`, `json`, `git`, `template`) and pushes them to that registry; the stack
points the activity catalog at them, so an Orkflow that `uses: shell` runs
the images this checkout built.
`make up` builds `bin/ork`, `bin/orker` and `bin/orkctl`, starts the engine,
one Orker and the UI's dev server (Vite, with hot reload), and waits until
each one answers: the engine's API, an Orker polling its pool, the dev server
and a live-event socket opened through it. When it prints "OrkStack is up":

| What | Where |
|---|---|
| UI (Vite dev server, hot reload; API and WebSocket calls go to the engine) | http://localhost:5173 |
| Engine API, and the UI built into `bin/ork` at `/` (a placeholder until `make ui`) | http://localhost:8080 |
| Temporal UI | http://localhost:8233 |
| SeaweedFS filer | http://localhost:8888 (S3 API on http://localhost:9000) |

`make status` shows what is running, and `make logs` tails the engine, Orker
and UI logs. Then `./bin/orkctl apply examples/each-fan-out.ork.yaml` and the
rest of [docs/GETTING_STARTED.md](./docs/GETTING_STARTED.md) work as written,
with `./bin/orkctl` for `orkctl`.

```bash
make down    # stop what make up started; data is kept
make reset   # wipe every local run, object and build output
```

`make down` also stops the infrastructure `make infra-up` started, which
other checkouts on the same machine may be using. `make reset` returns the
machine to a vanilla state: it deletes Temporal history, every object-store
object (artifacts, archived runs, and the Orkflow catalogue), the local
artifact store and all build output. The repository itself is untouched.

The individual steps are there when you want them: `make tools`,
`make proto`, `make infra-up`, `make build`, `make run-ork`, `make run-orker`.
`make infra-up-observability` adds a local Grafana stack
([deploy/observability](./deploy/observability/README.md)).
To run the UI by hand, `cd ui && bun run dev`: Vite serves it with hot
reload on port 5173 and forwards the Connect calls (`/orkstack.v1.`), the
plain-HTTP routes (`/v1/`) and the WebSockets (`/ws/`) to the engine
`ORK_SERVER` names (default `http://127.0.0.1:8080`, as for `orkctl`). It
keeps the `Host` header, so the engine admits the page's sockets as
same-origin, as it does for the UI it serves itself.

The engine serves the UI at `/` on its own port, from the build compiled into
`bin/ork`. `make ui` writes that build: it builds the UI in Docker, in the
pinned Bun image (`deploy/source/Dockerfile`, its `ui` stage), into
`internal/webui/dist/`, and the next `make build` embeds it; it needs no Bun
on the host. To serve a build from disk instead, run `cd ui && bun run build`
and start the engine with `ORK_UI_DIR=ui/build`. A `bin/ork` built with
neither serves a placeholder page at `/` that says this, and its API works as
always ([docs/DEPLOY.md](./docs/DEPLOY.md), "The web UI").

Configuration needs nothing for a laptop: `make up` runs on defaults alone.
Past that, `ork` and `orker` read one shared YAML file (`--config PATH`), with
`ORK_`/`OTEL_` environment variables overriding it; see
[docs/DEPLOY.md](./docs/DEPLOY.md), "Configuration", and the generated
[`deploy/orkstack.defaults.yaml`](./deploy/orkstack.defaults.yaml).

### The release artifacts

`make dist VERSION=vX.Y.Z` builds what a release publishes as files: `orkctl`
for Linux, macOS and Windows, `ork` and `orker` for Linux, each archived, and
`SHA256SUMS`, under `dist/`. It runs `make ui` first, because `ork` carries
the UI, and refuses to build an `ork` without it.
`make release-images VERSION=vX.Y.Z OUT=<dir>` builds the release images
(`ork`, `orker`, `orkctl` and the six activities; there is no UI image) as
OCI layouts from those binaries, for this machine unless `PLATFORMS` says
otherwise. `make release-smoke VERSION=vX.Y.Z` builds both
for this machine and runs the quickstart's `compose.yaml` on those images end
to end, by digest, in a compose project, registry and ports of its own. It
signs each image with a throwaway key, by the cosign image and flags the
release signs with (`deploy/cosign-image.txt`), and runs an Orkflow in a
namespace that requires that key, so OrkStack's own verifier accepts what a
release signs.
`make quickstart-smoke` runs the README's Quickstart as written on a fresh
clone of the working tree, which builds the stack from source; `release.yml`
runs both before it publishes anything, and `nightly.yml` runs the quickstart
smoke on `main` every night ([docs/PROOFS.md](./docs/PROOFS.md)). One
dispatch of `release.yml` from `main` proves, builds and smokes a release,
checks that the nightly of its commit passed, and then waits for one approval
of the `release` environment before it tags and publishes; a dispatch from any
other branch is a dry run that never asks (docs/PROOFS.md §10, "Tag a
release").

## Fast checks (no Docker, under two minutes warm)

`make check` runs `make lint` (`go vet`, gofmt and staticcheck),
`go vet -tags=integration` over the integration suites, and the unit suites
with `-short`, which skips the scale tests and the tests that compile binaries;
it keeps the test cache, except for the packages whose unit tests run git
(`GIT_STATE_PACKAGES`: the documentation checks, the engine's golden-fixture
check and the proof harness), which it runs with `-count=1`. Go's cache records
only the files the test process itself opens inside the module, never what git
reads in a subprocess (HEAD, the index, `info/exclude`, a global ignore file),
so a cached pass there could survive a change to what git tracks or ignores.
Run those packages with `-count=1` by hand too.
Every proof runs those, without `-short`. Run it before every push. For the UI:
`cd ui && bun run check && bun run test:unit`. For the site (`site/`, which
also renders every doc): `make site`.

`make lint` runs staticcheck at the version `go.mod` pins as a `tool`
directive, so `go tool staticcheck` needs no install step. It runs the default
checks twice, over the module as built and again with `-tags=integration`,
because a symbol that only an integration-tagged file uses is unused to the
first run and live to the second. Fix a finding in the code. Where staticcheck
is wrong, for example a deprecation whose named replacement does not cover the
use, put `//lint:ignore CHECK reason` on the line before the flagged one and
say in the reason why; do not disable a whole check.

`make vuln` runs the pinned `govulncheck`, which reports the known
vulnerabilities in your dependencies and the standard library whose vulnerable
code the module can reach. It reads the vulnerability database over the
network, so it is in neither `make lint` nor any proof, and a network failure
cannot fail one. CI's `vuln` job and the nightly workflow run it. A report names
the fixed version: bump the module to exactly that version, then run
`make check`. A vulnerability the code cannot reach is shown only with
`go tool govulncheck -show verbose ./...`.

When a test is listed in `scripts/proof-quarantine.txt`, its failure is that
known flake: tell its owner if it is new to you. Never re-run a failing test until it
passes; a second run is the flip the list exists to count, not a remedy.

Start infra only when touching Temporal or storage: `make infra-up`. On a
Linux host, do the one-time `host.docker.internal` setup in "Linux hosts"
below first.

## Linux hosts

The runtime-facing suites (`internal/orker`, `cmd/orker`, `orkctl task run`
and `test/fixtures/bash_activity_contract.sh`) run on whichever CLI the
environment names, so they can be run on Podman as well as Docker: set
`ORK_DOCKER_CLI=podman` for the podman CLI (and `ORK_DOCKER_BIN` if it is not
`podman` on `PATH`), or `DOCKER_HOST` to Podman's socket for the docker CLI
against it. Podman needs `insecure = true` for `localhost:5001` in
`registries.conf` and the host requirements of docs/DEPLOY.md, "Running on
Podman". Podman may write under `~/.config/containers` and
`~/.local/share/containers`; restore them when done. CI runs them on Podman in
`podman.yml`.

Docker Desktop (macOS, Windows) provides `host.docker.internal` inside
containers; plain Docker Engine on Linux does not. Several integration and
Playwright tests start a stub server on the host and have an activity
container call it at that name, so on Linux run once:

```bash
# make the name resolve inside containers (containers use the host's
# resolver on the docker0 gateway, which serves /etc/hosts)
echo '172.17.0.1 host.docker.internal' | sudo tee -a /etc/hosts

# if ufw is active with a default-deny input policy, let containers on the
# default bridge reach host TCP ports
sudo ufw allow in on docker0 from 172.17.0.0/16 proto tcp

# verify
docker run --rm alpine:3.20 getent hosts host.docker.internal
```

`172.17.0.1` is the gateway of Docker's default bridge; a host with a
customized `docker0` uses its own gateway address.

The same applies to the observability stack (`make infra-up-observability`).
Its Prometheus container scrapes `ork` and `orker` at
`host.docker.internal:9464` and `:9465`, but it sits on the compose network,
and on Linux a listener on `127.0.0.1` cannot be reached from a container. To
scrape a stack you run by hand:

1. Bind each process's admin listener to the gateway: `ORK_ADMIN_ADDR=172.17.0.1:9464`
   for `ork`, `172.17.0.1:9465` for `orker`.
2. Enable the `prometheus` metrics exporter (`OTEL_METRICS_EXPORTER=prometheus`;
   see `docs/DEPLOY.md` §1).
3. Attach Prometheus to the default bridge:

```bash
docker network connect bridge "$(docker ps -q --filter label=com.docker.compose.service=prometheus)"
```

The observability proof gate (`scripts/proof-observability.sh`) does the same
for its own run, and detaches Prometheus afterwards only if it attached it.

This is development and proof setup only: never bind a deployment's admin
listeners to the bridge gateway, and nothing in `deploy/` does. While a
listener is bound there, every container on Docker's default bridge can reach
it, and that includes the activity containers an Orker starts with network
`default`. What they can reach is the admin surface of that `ork` or
`orker`: `/metrics`, `/healthz`, `/readyz`, and, under
`ORK_PPROF_ENABLED=true`, `/debug/pprof/` (profiles, goroutine stacks and the
process's command line). It is not the Connect API,
not Temporal and not object storage, but an activity needs none of it
(AGENTS.md §15), so unbind it (unset `ORK_ADMIN_ADDR`, or set it back to
`127.0.0.1:…`) when you are done. The proof gate binds there only for its own
run, with its own `ork` and `orker`, and stops them when it ends.

## What CI runs on your pull request

- **`check`** — `make check`, on every push and every pull request, with
  `DOCKER_HOST` pointing at nothing to prove the fast path needs no Docker.
- **`vuln`** — `make vuln`, on every push and every pull request. It needs the
  network and is in no proof; the nightly workflow runs it too, and a
  vulnerability it reports makes that night's run red.
- **`feature`** — optional: the feature tier, run only with the `ci:feature`
  label, on a push to `validate/feature/**`, or by dispatch. It runs the fast
  gates, the tests and specs that prove your change, and its regression set,
  and gates nothing: the `proof` check does. Name evidence the selection
  cannot find in your PR description:

  ```text
  Proof-tests: internal/engine:TestIntegration_X
  Proof-specs: ui/e2e/x.spec.ts
  ```

  These are the same values as `make proof-feature TESTS=… SPECS=…`. A change
  to a path only the full proof covers (`scripts/`, `.github/`, the Makefiles,
  `test/tools/proofrun/`, …) skips this job; the `proof` check proves it.
- **`proof`** — the full proof, split across runners, the only required
  check. Read the result from the `proof-<run>-a<attempt>` artifact's
  `proof.json` (`result`, `merge_eligible`, `ineligible_reasons`); the job
  summary is written from it. It proves exactly the tree that lands, so a
  branch that does not contain `main`'s tip is asked to update.

Once the gate is enforced, a change to the gate's own files (the lists under
`scripts/proof-*`, `.github/`, `test/tools/proofrun/`, …) needs the
maintainer's approval. That is GitHub's code-owner ruleset, not a CI job.

**You never need to run the full proof locally.** CI runs it. `make proof`
keeps working, and is equivalent, for maintainers who merge locally. A local
proof runs the fast gates first and stops at its first failing gate, still
writing a failing verdict (`PROOF_FAIL_FAST=0` runs every gate to its end).

Everything else — tiers, sharding, artifacts, reading a failed proof,
procedures — is in [docs/PROOFS.md](./docs/PROOFS.md).

## The loop

1. Cross-process type change? Edit `proto/`, run `make proto`, commit generated
   code in the same change.
2. New configuration setting? Add it to `internal/config`'s `Schema`, to
   `docs/DEPLOY.md`'s configuration table, and run `make config-defaults`
   (AGENTS.md §11.1). Changed a DSL or automation key, or a first-party
   image's interface label? Run `go run ./internal/schemas/cmd/genschemas`.
3. Write a behavior test first (AGENTS.md §11.1).
4. Implement until it passes.
5. Run `make check`; for concurrent code, `make test-race` too (the proof's
   race gate: the unit tests under the race detector, `-short`; it needs cgo
   and a C compiler); for Temporal or storage changes, run the affected
   integration tests against `make infra-up`
   (`scripts/integration.sh ./internal/engine -run '^TestIntegration_X$'`, or
   `make test-integration` for all of them).
6. Update the docs the change makes false, and add the CHANGELOG.md
   `[Unreleased]` entry a user-visible change needs (AGENTS.md §11.1; "The
   changelog", below). The
   site's pages (`site/content/`) are short pages written for readers that
   cite the repository docs they summarise: update the page that describes a
   behaviour you change, and give a new doc under `docs/` a site page that
   cites it (the site's build refuses an uncited one). A change to the UI's look changes
   `ui/src/tokens.css`, never the site, and regenerates the site's
   screenshots (`make site-media`) when they would show it.
7. Optionally, check the change's evidence with a feature proof that names the
   tests and specs proving it:
   `make proof-feature TESTS='internal/engine:TestIntegration_X' SPECS='ui/e2e/x.spec.ts'`.
   It writes `proof/<name>-feature/`, gitignored, and refuses (exit 2) a
   change it cannot account for. It gates nothing; the full proof does.
8. Commit with a Conventional Commit that names the tests and specs that
   prove the change (and quotes the feature proof, if you ran one); keep
   `main` buildable.
9. Merge through AGENTS.md §11.3. CI's `proof` check is the merge gate; a
   local `make proof` is the equivalent path.

## The changelog

CHANGELOG.md follows Keep a Changelog 1.1.0. A user-visible change adds its
entry under `## [Unreleased]` in the same pull request, in the subsection of
its kind (`### Added`, `### Changed`, `### Deprecated`, `### Removed`,
`### Fixed` or `### Security`), and marks a breaking change **BREAKING**
(AGENTS.md §11.1, step 7). Write it for the people who read the release: at a
release the `[Unreleased]` section becomes the version's own,
`## [X.Y.Z] - YYYY-MM-DD`, with an empty `[Unreleased]` above it, and that
section's text is the GitHub release's notes, word for word. Link to a
document by its absolute URL: a relative link does not resolve on the release
page. `make check` refuses a heading or a subsection of any other shape
(`internal/docscheck`), and `release.yml` refuses a release while entries are
still under `[Unreleased]`; docs/PROOFS.md §10, "Write the release notes", is
the release's side of it. 0.1.0, the first release, says only "First
release.".

## Fuzzing

A parser of input OrkStack does not control that is added or changed gets a
native Go fuzz target (`func FuzzXxx(f *testing.F)`, AGENTS.md §11.1) beside
its table tests; one that has none is not yet covered, not exempt. The seeds
run as ordinary tests in `make check` and every proof. To search for more, run
one target at a time:

```bash
go test -run='^$' -fuzz='^FuzzParse_AcceptedTextIsAFilterThatFormatsBackToItself$' -fuzztime=3m ./internal/listfilter
go test -list '^Fuzz' ./...     # every target
```

A target is a test to the feature proof: a changed one is evidence, and
`make proof-feature TESTS='internal/listfilter:FuzzParse.*'` names it.
`make fuzz` runs them all, sharing `FUZZ_BUDGET` between them.

A failing input is written under `testdata/fuzz/<FuzzName>/`. Fix the code the
input exposed, not the test, and commit the file: it then runs in every unit
gate. A property is an invariant the code or its reference doc states (a round
trip, a canonical form that compiles again, an independent count of what a
verifier accepted), not only "does not panic".

## Definition of DONE

A change is done when it is on `main` and a full proof passed on the exact
tree `main` then points at — the `proof` check of the pull request (or merge
group) that landed it, or a local `make proof` — and when every reference doc
that describes what changed says the new thing, with a CHANGELOG.md
`[Unreleased]` entry for a user-visible change. Done is judged by the merge
gate: the few scale tests listed in `scripts/proof-nightly-tests.txt`, and
the specs listed in `scripts/proof-nightly-specs.txt`, run nightly and before
a release. A failure there is a regression on `main`: fixing it, or reverting
what broke it, is the next work, but it holds no other merge, and no release
is tagged until the pre-release proof, which runs them all, passes;
`release.yml` also refuses a commit whose own nightly did not pass. A feature
proof means "ready for the merge proof", never "done". No proof, not done.

## Style

- Go: `gofmt`, wrapped errors, `context.Context` first, no panics in libraries.
- Keep the Temporal workflow layer deterministic (AGENTS.md §2.1). A change to
  what `RunWorkflow` commands or sends must keep every recorded history
  replaying; see
  [internal/engine/testdata/history/README.md](./internal/engine/testdata/history/README.md).
- Small, focused commits and PRs.
