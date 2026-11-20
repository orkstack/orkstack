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
- **make**, **git**, **curl**, **bash** and **jq**: the proof harness's own
  tests run its shell functions and the workflows' jq checks, and fail
  without them.

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
S3-compatible object store, and a local OCI registry on `localhost:5001`. It
returns once each passes its health check, which for the store means its bucket
is ready. `make -C images push` builds the first-party activities (`http`, `shell`,
`file`, `json`, `git`, `template`) and pushes them to that registry; the stack
points the activity catalog at them, so an Orkflow that `uses: shell` runs
the images this checkout built.
`make up` builds `bin/ork`, `bin/orker` and `bin/orkctl`, starts the engine,
one Orker, which is a pin host, and the UI's dev server (Vite, with hot reload), and waits until
each one answers: the engine's API, an Orker polling its pool, the dev server
and a live-event socket opened through it. When it prints "OrkStack is up":

| What | Where |
|---|---|
| UI (Vite dev server, hot reload; API and WebSocket calls go to the engine) | http://localhost:5173 |
| Engine API, and the UI built into `bin/ork` at `/` (a placeholder until `make ui`) | http://localhost:8080 |
| Temporal UI | http://localhost:8233 |
| SeaweedFS filer | http://localhost:8888 (S3 API on http://localhost:9000) |

Every port the infrastructure publishes (`deploy/docker-compose.yml`, the
observability profile's included) is bound to `127.0.0.1`: Temporal's
frontend, the object store with its development key, the unauthenticated
registry and Grafana's anonymous viewer are reachable from this machine and
from no other. Nothing in the stack, the suites or the proofs needs more: an
activity container reaches only a test's own server on the host, by
`host.docker.internal` ("Linux hosts" below), and the observability
profile's services reach each other on the compose network.
`TestDevCompose_PublishesOnlyOnLoopback` holds the rule.

`make up` starts the engine on `127.0.0.1:8080` for the same reason: whatever
reaches it, signed in, can start a run on this machine's Docker, and with
`IDENTITY=0` it asks nobody to sign in.
`orkctl` and the UI's dev server reach it on loopback, and the suites and the
proofs start engines of their own, at addresses of their own.
`ORK_HTTP_ADDR=:8080 make up` opens it to other machines; an engine already
running keeps its address until `make down`.
`TestDevStack_EngineListensOnlyOnLoopback` holds the default.

`make up` runs the stack the way a deployment that authenticates does: the
engine in `enforce`, with a development issuer `dev` whose key pair
`test/tools/devtoken` makes once under `.data/identity/`, beside the engine's
own signing key (`identity.signing.key_files`, which `enforce` requires), and
two tokens it
signs afresh at every `make up`, each living thirty days. `dev-admin` is in
`auth.admins`; `dev-orker` is the Orker's own credential
(`orker.engine.auth.method: token_file`), bound to the `orker` role at `main`
by `auth.orkers`, so the engine starts before the Orker. Present the admin's
token to `orkctl` with the line `make up` prints,
`export ORK_TOKEN_FILE=.data/identity/admin.token`. People sign in at a
second issuer, `people`: Dex, a real OpenID Connect provider, which `make up`
runs in a container from the image `deploy/dex-image.txt` pins (pulled once)
and publishes on `127.0.0.1:5556` (`OIDC_ADDR`; its issuer is
`https://127.0.0.1:5556/dex`), with its CA, certificate and web client secret
under `.data/identity/dex/`. Its users and clients are in
`deploy/dex/config.yaml`: sign in as `dev@example.test`, password `password`,
in the group `dev-admins`, an admin by `auth.admins`; `admin@example.test`
(`admins`) and `vera@example.test` (`viewers`) are there for the tests. Dex
offers no token revocation and no end-session endpoint, so signing out ends
the OrkStack sign-in, and Dex's own login form asks again at the next. Sign in to the UI at its dev server, which is `auth.public_url`, or in
a terminal with `./bin/orkctl --server http://127.0.0.1:8080 login --issuer
people --issuer-ca-file .data/identity/dex/tls/ca.pem`. `make down` stops
Dex with the rest. The sign-in key is
`.data/identity/signin.key`, made once. `make up IDENTITY=0` runs the stack
in `asserted` mode instead: no Dex, no sign-in, and nothing verified; stop the
stack first (`make down`), since an engine refuses to start below the mode a
deployment's bucket records, and lowering it is `orkctl admin auth lower`
(docs/AUTHENTICATION.md).

`make status` shows what is running, and `make logs` tails the engine, Orker
and UI logs. Then `./bin/orkctl orkflow create examples/each-fan-out.ork.yaml` and the
rest of [docs/GETTING_STARTED.md](./docs/GETTING_STARTED.md) work as written,
with `./bin/orkctl` for `orkctl`.

```bash
make down    # stop what make up started; data is kept
make reset   # wipe every local run, object and build output
```

`make down` also stops the infrastructure `make infra-up` started, which
other checkouts on the same machine may be using. It stops the `ork` and
`orker` that `make up` started, by the pid and start time it recorded; one
you started by hand from `bin/` is not found, because both make themselves
undumpable (docs/DEPLOY.md, "The processes themselves"), so stop it yourself. A process `make up` started that survives `make down` fails it, and its pidfile stays in `.run/`. `make reset` returns the
machine to a vanilla state: it deletes Temporal history, every object-store
object (artifacts, archived runs, and the Orkflow catalogue), the
observability profile's containers and volumes, the local artifact store,
the Orker's work directory (a workspace an Orker left read-only included) and
all build output. The repository itself is untouched.

The individual steps are there when you want them: `make tools` (buf at
the release `deploy/buf-image.txt` pins), `make proto`, `make infra-up`, `make build`, `make run-ork`, `make run-orker`.
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
the UI, and refuses to build an `ork` without it. Each binary is stamped with
the version and the commit the checkout is at, which `orkctl version`, `ork
--version` and `orker --version` report; from a tree with no git, pass
`SOURCE_COMMIT=<sha>` beside `SOURCE_DATE_EPOCH`.
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
`make examples-smoke` runs what `make quickstart-smoke` runs, then every shipped
example with no inputs on that stack; it reaches `httpbin.org` and `github.com`,
so it runs as a nightly job of its own.
`make quickstart-smoke` runs the README's Quickstart as written on a fresh
clone of the working tree, which builds the stack from source; `release.yml`
runs both before it publishes anything, and `nightly.yml` runs the quickstart
smoke on `main` every night ([docs/PROOFS.md](./docs/PROOFS.md)). One
dispatch of `release.yml` from `main` proves, builds and smokes a release,
checks that the nightly of its commit passed, and then waits for one approval
of the `release` environment before it tags and publishes; a dispatch from any
other branch is a dry run that never asks (docs/PROOFS.md §10, "Tag a
release").

## The development object store

The store `make infra-up` runs checks keys and refuses an upload to a bucket that
does not exist, as production does, so a wrong `ORK_S3_ACCESS_KEY`,
`ORK_S3_SECRET_KEY` or `ORK_S3_BUCKET` fails on your machine with a 403 or a 404.
No binary falls back to its key: `make up`, `make run-ork`, `make run-orker`, the
integration tests and the proofs each set it, so a binary you start yourself needs
`ORK_S3_ACCESS_KEY=orkstack` and `ORK_S3_SECRET_KEY=orkstack-secret`.
A request by hand signs with the development keys:

```bash
curl --head --aws-sigv4 aws:amz:us-east-1:s3 --user orkstack:orkstack-secret http://localhost:9000/orkstack-artifacts
```

A store started before it checked keys accepts any: run `make infra-up` to
recreate it. Its data stays.

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
Every proof runs those, without `-short`. It ends with the UI's gates, the ones
a full proof's `ui-check` and `ui-unit` run: `bun install --frozen-lockfile`,
`bun run check` and `bun test test/` in `ui/`, about 20 s together. So it needs
Bun, and a first install needs the network. Run it before every push. For the
site (`site/`, which also renders every doc): `make site`.

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

`make vuln-js` and `make vuln-images` are the scan's other two lanes, and need
the network too, so the nightly workflow runs them and no proof does.
`make vuln-js` runs `bun audit` over `ui/` and `site/`, development
dependencies included, and fails on a high or critical advisory that
`deploy/bun-audit.yaml` does not waive; `bun audit fix --dry-run` names the
fixed versions. `make vuln-images` builds
the images a release publishes, for linux/amd64, and scans each with the
pinned Grype (`deploy/grype-image.txt`, policy `deploy/grype.yaml`): a fixed
vulnerability of high or critical severity in an OS package or a bundled
third-party binary fails it. OrkStack's own Go code is `make vuln`'s. Grype's
database takes about 3 GB under `.data/vuln-images`.

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

Some Docker installs provide `host.docker.internal` inside containers; plain
Docker Engine on Linux does not. Several integration and
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
docker run --rm alpine:3.24 getent hosts host.docker.internal
```

`172.17.0.1` is the gateway of Docker's default bridge; a host with a
customized `docker0` uses its own gateway address.

Docker Hub limits anonymous pulls per IP. A host that runs many proofs, or
shares its address, can point its Docker daemon at `mirror.gcr.io`, the
public cache of Docker Hub CI runners use (docs/PROOFS.md §5): add
`"registry-mirrors": ["https://mirror.gcr.io"]` to `/etc/docker/daemon.json`,
beside any keys already there, restart Docker, and check that
`docker info` lists it under Registry Mirrors. It is optional, and the
host's own choice: a pull by digest is verified either way.

### The usb-serial test device

The device-profile integration test passes a host character device into an
activity container, so it needs one of the usb-serial class
(docs/DEPLOY.md, "Running a device Orker"): the node `/dev/ttyUSB199`, at the
major `188` the `usbserial` driver registers as `ttyUSB`. It is a bare node
with no adapter behind it, and nothing opens it. The drift test needs a
second node, `/dev/ttyUSB198` (188:198), owned by you rather than root: it
moves the node to another group you belong to (`docker`) and back, which an
Orker reads as the device changing, and it does that under a host-wide lock so
two runs never move it at once. Only that test uses it. Without them,
`make test-integration`, `scripts/integration.sh` and `make proof` fail that
tests by name; a by-hand `go test -tags=integration` without
`ORK_REQUIRE_INFRA=1` skips them. Set it up once, and the host remakes it at
every boot:

```bash
# load the driver that registers major 188 as ttyUSB
echo usbserial | sudo tee /etc/modules-load.d/orkstack.conf
# the nodes, remade at every boot, in your own group; the drift test's is yours
printf '%s\n' "c /dev/ttyUSB199 0660 root $(id -gn) - 188:199" \
  "c /dev/ttyUSB198 0660 $(id -un) $(id -gn) - 188:198" | sudo tee /etc/tmpfiles.d/orkstack.conf

# apply now, without a reboot, and verify
sudo modprobe usbserial
sudo systemd-tmpfiles --create /etc/tmpfiles.d/orkstack.conf
grep '^188 ttyUSB$' /proc/devices && ls -l /dev/ttyUSB199 /dev/ttyUSB198
```

On a kernel that builds the USB serial drivers as extra modules (Ubuntu's
cloud kernels among them), install `linux-modules-extra-$(uname -r)` first.
CI's proof-host step does the same for each runner. Device profiles are
Linux-host-only: an Orker reads `/proc/devices` and the host's own device
numbers, and Docker Desktop on macOS passes only its Linux VM's devices, never
the Mac's, so this test does not run there.

The observability stack (`make infra-up-observability`, `make up-observability`)
needs none of this: `ork` and `orker` push every signal to its Collector on
`127.0.0.1:4318`, and its Prometheus scrapes nothing, so no container reaches
the host. Keep admin listeners on `127.0.0.1`.

### The GPU host

The tests `scripts/proof-gpu-tests.txt` lists need a GPU host: this project's
proof host, with an AMD and an Intel GPU. Every other proof records them
`host-deferred`, and a change that touches a path the list names merges only
with a passing GPU-host proof of its tree (docs/PROOFS.md §2.4, "The GPU-host
tier"). To make one, set the host up once. `scripts/gpu-host-setup.sh` never
runs sudo: it prints the root steps the host still needs, or, with
`--script`, writes them as one script to read and then run:

```bash
scripts/gpu-host-setup.sh --script >/tmp/gpu-setup.sh
less /tmp/gpu-setup.sh
sudo sh /tmp/gpu-setup.sh
```

The steps add you to the `orkstack-gpu` group, which a session already
running does not have until you log in again. To run a command with it now,
pipe it to `newgrp`; the proof's preflight reads `id -nG` and exits 77 without
the group:

```bash
echo 'make proof-gpu' | newgrp orkstack-gpu
```

## What CI runs on your pull request

- **`check`** — `make check`, on every push, except to a Dependabot branch, and every pull request, with
  `DOCKER_HOST` pointing at nothing to prove the fast path needs no Docker.
- **`vuln`** — `make vuln`, on every push, except to a Dependabot branch, and every pull request. It needs the
  network and is in no proof; the nightly workflow runs it too, with
  `make vuln-js` and `make vuln-images`, and a vulnerability any of them
  reports makes that night's run red.
- **`pr-title`** — refuses a pull request title that is not a Conventional
  Commit, because your pull request lands as one squash commit whose subject is
  its title and whose body is its description. A required check, beside
  `proof`. Edit the title and it runs again; nothing needs pushing.
- **`proof`** — the full proof, split across runners, the required check that
  proves the change. Read the result from the `proof-<run>-a<attempt>` artifact's
  `proof.json` (`result`, `merge_eligible`, `ineligible_reasons`); the job
  summary is written from it. It proves exactly the tree that lands, so a
  branch that does not contain `main`'s tip is asked to update. When a gate
  of its `fast` part fails, the run is cancelled, so a lint, build or unit
  failure ends it early.

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
   code in the same change. `make proto-breaking` (Docker, and the `v0.1.0`
   tag in your clone) checks the change keeps the first release's contract,
   as CI's `proto` job does.
2. New configuration setting? Add it to `internal/config`'s `Schema`, to
   `docs/DEPLOY.md`'s configuration table, and run `make config-defaults`
   (AGENTS.md §11.1). Changed a DSL or automation key, or a first-party
   image's interface label? Run `go run ./internal/schemas/cmd/genschemas`,
   then, if an Orkflow or automation schema changed, `cd ui && bun run
   gen:validators`. A bump of ajv or ajv-formats needs that second step too.
3. Write a behavior test first (AGENTS.md §11.1).
4. Implement until it passes.
5. Run `make check`; for concurrent code, `make test-race` too (the proof's
   race gate: the unit tests under the race detector, `-short`; it needs cgo
   and a C compiler); for Temporal or storage changes, run the affected
   integration tests against `make infra-up`
   (`scripts/integration.sh ./internal/engine -run '^TestIntegration_X$'`, or
   `make test-integration` for all of them).
6. Update the docs the change makes false, and, when the change may affect
   the people who run or use OrkStack, add its CHANGELOG.md `[Unreleased]`
   entry (AGENTS.md §11.1, step 7; "The changelog", below). The
   site's pages (`site/content/`) are short pages written for readers that
   cite the repository docs they summarise: update the page that describes a
   behaviour you change, and give a new doc under `docs/` a site page that
   cites it (the site's build refuses an uncited one). A change to the UI's look changes
   `ui/src/tokens.css`, never the site. The site's screenshots and recordings
   are not regenerated in the change: `make site-media` runs once on the final
   tree before the release that ships it (docs/PROOFS.md §10, "Regenerate the
   site's media").
7. Commit with a Conventional Commit that names the tests and specs that
   prove the change; keep `main` buildable. A branch may hold several: what
   lands is one squash commit.
8. Open a pull request whose title is that Conventional Commit, such as
   `fix(orker): keep the shared directory on a retry`, and whose description
   names the tests and specs: the title becomes the subject of the squash
   commit and the description its body. The types are `feat`, `fix`, `docs`,
   `chore`, `test`, `ci`, `refactor`, `perf`, `build`, `style` and `revert`; the
   scope and a breaking change's `!` are optional.
9. Merge through AGENTS.md §11.3. CI's `proof` check is the merge gate, and
   `pr-title` holds the title; a local `make proof` is the equivalent path for
   a maintainer, who lands the squash commit by hand.

## The changelog

CHANGELOG.md follows Keep a Changelog 1.1.0 and is written for the people
who run or use OrkStack. A change that cannot affect them gets no entry,
however large: documentation edits; CI, workflows and the proof harness,
unless the change alters what a release publishes; tests; refactors; a
cosmetic change to the UI; internal tooling, such as the development stack
(`make up`); and a dependency bump that cannot affect them. A change that may
affect them (behaviour, the DSL,
the CLI, the API and its error reasons, configuration, the activity contract,
storage layout, what a release publishes and any security fix to it, a
vulnerable dependency's bump among them even when they see nothing change, a
dependency bump that changes what they see, a deprecation, a breaking change)
adds an entry under `## [Unreleased]` in the same pull request, in the
subsection of its kind (`### Added`, `### Changed`, `### Deprecated`,
`### Removed`, `### Fixed` or `### Security`), once it passes one test: would
a person who runs or uses OrkStack care about this at all? Keep what changes
what they type, configure, see or depend on, or needs action from them, and
always every **BREAKING** change and security fix; leave out internal minutiae,
such as a removed example, the quickstart's internals, a reworded message
nobody scripts against or a metric label only an internal dashboard sees. An
entry is always in one
format: one line in plain words on what changed for them and what they must do
about it, if anything, at most 200 characters before its closing links; then,
in one closing parenthesis, an absolute link to the doc that holds the detail,
and, once changes land through pull requests, a link to its pull request. Leave
field names, flags and edge cases to the doc it links. A breaking change's
entry is marked **BREAKING** and listed first in its subsection. A version's
section opens with `### Features`, a short list of its headline items, closed
by one `**Upgrading:**` line on what to read before upgrading; the
subsections after it stay the full record, so a headline item has its own
entry there too (AGENTS.md §11.1, step 7). Write it for
the people who read the release: at a release the `[Unreleased]` section becomes
the version's own,
`## [X.Y.Z] - YYYY-MM-DD`, with an empty `[Unreleased]` above it, and that
section's text is the GitHub release's notes, word for word, above a footer
the release adds with each image's digest. Link to a document by its absolute
URL: a relative link does not resolve on the release page. The page, footer
included, must stay within GitHub's 125,000 characters, or the release is
refused before anything is built. `make check` refuses a heading or a subsection of any other shape, and an
entry over the limit or on more than one line (`internal/docscheck`), and `release.yml` refuses a release while entries are
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

Its seeds run in every unit gate, so the full proof runs a changed target too.
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
`[Unreleased]` entry for a change that may affect the people who run or use
OrkStack. Done is judged by the merge gate: the few scale tests listed in
`scripts/proof-nightly-tests.txt`, and the specs listed in
`scripts/proof-nightly-specs.txt`, run nightly and before a release. A
failure there is a regression on `main`: fixing it, or reverting what broke
it, is the next work, but it holds no other merge, and no release is tagged
until the pre-release proof, which runs them all, passes; `release.yml` also
refuses a commit whose own nightly did not pass. No proof, not done.

## Style

- Go: `gofmt`, wrapped errors, `context.Context` first, no panics in libraries.
- Keep the Temporal workflow layer deterministic (AGENTS.md §2.1). A change to
  what `RunWorkflow` commands or sends must keep every recorded history
  replaying; see
  [internal/engine/testdata/history/README.md](./internal/engine/testdata/history/README.md).
- Small, focused commits and PRs.
