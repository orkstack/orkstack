# Getting started

This page takes a fresh clone to a finished run on your own machine: the
whole local stack, the first-party activity images, and one Orkflow applied,
planned, run and inspected from the CLI and the UI. Everything here is
development setup; [DEPLOY.md](./DEPLOY.md) is the reference for a real
deployment.

OrkStack is pre-release: stable, but breaking changes may be introduced until
1.0, and each is listed in [CHANGELOG.md](../CHANGELOG.md).

<!-- site-media: e2e-demo -->

## What you need

- **Docker** — Docker Engine or Docker Desktop, with Compose. The stack's
  infrastructure runs in containers, and so does every task.
- **Go** — the version in [`go.mod`](../go.mod) (`go 1.27`), to build `ork`,
  `orker` and `orkctl`.
- **Bun** — to install and serve the UI.
- **make**, **git** and **curl**.

Nothing else: OrkStack needs no database of its own. It runs on a Temporal
cluster and S3-compatible object storage, and the local stack starts one of
each.

## 1. Start the infrastructure

```bash
git clone https://github.com/orkstack/orkstack.git
cd orkstack
make infra-up
```

`make infra-up` starts [`deploy/docker-compose.yml`](../deploy/docker-compose.yml):
a Temporal development server, SeaweedFS as the S3-compatible object store,
and a local OCI registry on `localhost:5001`.

## 2. Push the first-party activity images

```bash
make -C images push
```

This builds the first-party activities (`http`, `shell`, `file`, `json`,
`git`, `template`) and pushes them to the local registry. An Orkflow names
them by catalog name (`uses: shell`); the local stack points that catalog at
the images you just pushed. Without this step an apply of an Orkflow that
uses them fails, because their published references are not reachable from a
development stack ([OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md),
§4.6).

## 3. Start OrkStack

```bash
make up
```

`make up` builds `bin/ork`, `bin/orker` and `bin/orkctl`, starts the engine,
one Orker and the UI, and waits until each one answers: the engine's API, an
Orker polling its pool, the UI and its live-event socket. When it prints
"OrkStack is up":

| What | Where |
|---|---|
| UI | http://localhost:5173 |
| Engine API | http://localhost:8080 |
| Temporal UI | http://localhost:8233 |
| SeaweedFS filer | http://localhost:8888 (S3 API on http://localhost:9000) |

`make status` shows what is running, and `make logs` tails the engine, Orker
and UI logs.

## 4. Apply an Orkflow

An Orkflow is a YAML document. [`examples/each-fan-out.ork.yaml`](../examples/each-fan-out.ork.yaml)
computes a list in one task, converts each element as its own durable item,
and merges the results:

```bash
./bin/orkctl apply examples/each-fan-out.ork.yaml
```

Applying compiles the document, checks it (names, the dependency graph, every
task's configuration), resolves each image to a digest, and stores an
immutable version. Nothing runs yet.

<!-- site-media: ui-orkflow -->

## 5. Plan it, then run it

```bash
./bin/orkctl run plan each-fan-out
./bin/orkctl run submit each-fan-out --watch
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
./bin/orkctl run logs <run-id> --task merge
./bin/orkctl run artifacts <run-id>
```

`run logs` prints a task's stdout and stderr, and `run artifacts` lists the
files the run's tasks wrote, which live in object storage rather than in the
engine. The run page shows the same: every task's attempts, timing, values,
logs and artifacts.

<!-- site-media: ui-run-finished -->

## 7. Stop, or start over

```bash
make down    # stop everything make up started; data is kept
make reset   # wipe every local run, object and build output
```

## More about the local stack

`make reset` returns the machine to a vanilla state: it deletes Temporal
history, every object-store object (artifacts, archived runs, and the
Orkflow catalogue), the local artifact store and all build output. The
repository itself is untouched.

The individual steps are there when you want them: `make tools`,
`make proto`, `make infra-up`, `make build`, `make run-ork`, `make run-orker`.
`make infra-up-observability` adds a local Grafana stack
([deploy/observability](../deploy/observability/README.md)).
To run the UI by hand, `cd ui && bun run dev`: its server proxies Connect calls
to `ENGINE_API_URL` (default `http://127.0.0.1:8080`), and the browser opens
WebSockets to `PUBLIC_ENGINE_WS_URL` (default the page's host on `:8080`).

Configuration needs nothing for a laptop: `make up` runs on defaults alone.
Past that, `ork` and `orker` read one shared YAML file (`--config PATH`), with
`ORK_`/`OTEL_` environment variables overriding it; see
[DEPLOY.md](./DEPLOY.md), "Configuration", and the generated
[`deploy/orkstack.defaults.yaml`](../deploy/orkstack.defaults.yaml).

On a Linux host, the test suites and the observability profile need one more
step of host setup ([CONTRIBUTING.md](../CONTRIBUTING.md), "Linux hosts").

## Where next

- [Examples](../examples/README.md) — every example Orkflow and what it
  needs to run.
- [DSL.md](./DSL.md) — every key of an Orkflow.
- [ORKFLOW_PATTERNS.md](./ORKFLOW_PATTERNS.md) — the rules the examples
  follow, and why.
- [OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md) — what a container
  must do to be an activity, for writing your own image.
