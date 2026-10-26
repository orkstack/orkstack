# Getting started

This page takes a machine with Docker to a finished run: the whole stack from
a release's images, and one Orkflow applied, planned, run and inspected from
the CLI and the UI. Docker is the only prerequisite. Everything here is a
local setup; [DEPLOY.md](./DEPLOY.md) is the reference for a real deployment,
and [CONTRIBUTING.md](../CONTRIBUTING.md) builds and runs OrkStack from
source.

OrkStack is pre-release: stable, but breaking changes may be introduced until
1.0, and each is listed in [CHANGELOG.md](../CHANGELOG.md). The commands below
download a published release. Until the first release is published there is
nothing to download yet; build and run from source instead
([CONTRIBUTING.md](../CONTRIBUTING.md), "The local development stack").

<!-- site-media: e2e-demo -->

## What you need

- **Docker** with Compose v2: Docker Engine, or Docker Desktop. The stack
  runs in containers, and so does every task.
- **curl** and **tar**, to download the release.

Nothing else: no toolchain and no database of its own. OrkStack runs on a
Temporal cluster and S3-compatible object storage, and the quickstart starts
one of each. This page is checked on Linux with Docker Engine; Docker Desktop
on macOS and Windows is not checked yet.

## 1. Download the quickstart

```bash
curl -fsSLO https://github.com/orkstack/orkstack/releases/latest/download/orkstack-quickstart.tar.gz
tar -xzf orkstack-quickstart.tar.gz
cd orkstack-quickstart
```

The bundle holds a `compose.yaml`, a `.env` that names the release's images by
version and digest, and the example Orkflows under `examples/`. To check the
download first, fetch the release's checksums beside it and run
`sha256sum --ignore-missing -c SHA256SUMS`
(`https://github.com/orkstack/orkstack/releases/latest/download/SHA256SUMS`).

## 2. Start OrkStack

```bash
docker compose up -d --wait
```

This pulls the release's images from `ghcr.io/orkstack`, starts Temporal,
SeaweedFS as the S3-compatible object store, the engine, one Orker and the
UI, and returns once each one passes its health check. Everything listens on
this machine's loopback only:

| What | Where |
|---|---|
| UI | http://localhost:5173 |
| Engine API | http://localhost:8080 |
| Temporal UI | http://localhost:8233 |

If a port is taken, set `ORKSTACK_UI_PORT`, `ORKSTACK_API_PORT` or
`ORKSTACK_TEMPORAL_UI_PORT` in `.env`. `docker compose ps` shows what is
running, and `docker compose logs -f ork orker` follows the engine's and the
Orker's logs.

The Orker runs each task as a container on this machine's Docker, through the
Docker socket the compose file mounts into it. Whoever controls that socket
controls the machine, so read [SECURITY_MODEL.md](./SECURITY_MODEL.md) before
running this anywhere but your own machine.

Listening on loopback keeps other machines out, but it is not a login. Any
program on this machine can reach the API and start a run, and so can a web
page you open, by DNS rebinding. Stop the stack with `docker compose down`
when you are done with it. The object store's keys in the compose file
(`orkstack` / `orkstack-secret`) are placeholders, and only the stack's own
services can reach the store.

## 3. Get orkctl

Run it from its image, with nothing to install:

```bash
alias orkctl='docker compose run --rm orkctl'
```

The alias works from the bundle's directory, and the container sees that
directory read-only, which is enough for `orkctl apply examples/…`. Or
install the binary for your system from the same release
(`orkctl-linux-amd64.tar.gz`, `orkctl-linux-arm64.tar.gz`,
`orkctl-darwin-amd64.tar.gz`, `orkctl-darwin-arm64.tar.gz`,
`orkctl-windows-amd64.zip`, `orkctl-windows-arm64.zip`):

```bash
curl -fsSLO https://github.com/orkstack/orkstack/releases/latest/download/orkctl-linux-amd64.tar.gz
tar -xzf orkctl-linux-amd64.tar.gz orkctl
sudo install -m 0755 orkctl /usr/local/bin/orkctl
```

Either way, `orkctl version` names the release.

## 4. Apply an Orkflow

An Orkflow is a YAML document. [`examples/each-fan-out.ork.yaml`](../examples/each-fan-out.ork.yaml)
computes a list in one task, converts each element as its own durable item,
and merges the results:

```bash
orkctl apply examples/each-fan-out.ork.yaml
```

Applying compiles the document, checks it (names, the dependency graph, every
task's configuration), resolves each image to a digest, and stores an
immutable version. Nothing runs yet.

<!-- site-media: ui-orkflow -->

## 5. Plan it, then run it

```bash
orkctl run plan each-fan-out
orkctl run submit each-fan-out --watch
```

`run plan` says which tasks would run and in what order, and starts nothing.
`run submit --watch` starts a run and follows its events until it ends. Each
task is an ordinary container: it reads its inputs under `/orkstack/input`,
writes its results under `/orkstack/output`, and exits.

Open the run in the UI to watch the same run as a drawing of its tasks:

<!-- site-media: ui-run-live -->

The fan-out task runs one item per element of the list, two at a time
(`max_parallel: 2`), each with its own attempts and logs:

<!-- site-media: ui-fanout -->

## 6. Inspect what it did

```bash
orkctl run logs <run-id> --task merge
orkctl run artifacts <run-id>
```

`run logs` prints a task's stdout and stderr, and `run artifacts` lists the
files the run's tasks wrote, which live in object storage rather than in the
engine. The run page shows the same: every task's attempts, timing, values,
logs and artifacts.

<!-- site-media: ui-run-finished -->

## 7. Stop, or start over

```bash
docker compose down      # stop; the object store keeps its data
docker compose down -v   # stop and delete the stored data too
```

The quickstart's Temporal keeps its history in memory, so a stopped stack
forgets its runs' histories; the Orkflows and the artifacts are in the object
store, which `down` keeps unless you add `-v`.

## Where next

- [Examples](../examples/README.md) — every example Orkflow and what it
  needs to run.
- [DSL.md](./DSL.md) — every key of an Orkflow.
- [ORKFLOW_PATTERNS.md](./ORKFLOW_PATTERNS.md) — the rules the examples
  follow, and why.
- [OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md) — what a container
  must do to be an activity, for writing your own image.
- [CONTRIBUTING.md](../CONTRIBUTING.md) — build OrkStack from source and run
  the development stack.
