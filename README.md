<h1 align="center">OrkStack</h1>

<p align="center">
  A thin, Temporal-backed durable execution platform. Author <em>Orkflows</em>
  in YAML and run any OCI image on a fleet of disposable workers
  (<em>Orkers</em>), without a workload SDK.
</p>

---

> Status: pre-release; breaking changes are listed in
> [CHANGELOG.md](./CHANGELOG.md).

## Why

- **Durable orchestration on Temporal** — retries, timeouts, timers, signals
  and replay come from Temporal; OrkStack adds no second state machine.
- **OCI execution** — Orkers run ordinary container images through a stable
  filesystem contract; workloads need no OrkStack SDK.
- **Artifacts by reference** — files flow between tasks through local or
  S3-compatible storage, never through the control plane.
- **One contract, everywhere** — a single Connect/proto API powers the `ork`
  engine, the `orkctl` CLI, and the browser UI, with realtime events over
  WebSocket.

```
YAML Orkflow → ork engine (Temporal) → Orker → OCI image
                      │                          │
              orkctl / SvelteKit UI        object storage
```

See [ARCHITECTURE.md](./ARCHITECTURE.md) for the components and data flow.

## Features

**Authoring** ([DSL reference](./docs/DSL.md),
[patterns](./docs/ORKFLOW_PATTERNS.md))

- A DAG of tasks (`needs:`), typed `inputs:`, templated `with:` and
  `condition`, retries and timeouts; immutable versions pinned by a SHA-256
  definition digest.
- `pool:` selects which Orkers run a task; `pin:` keeps a group of tasks on
  one Orker sharing `/orkstack/shared`.
- `each:` fans one task out over an array, one item per element.
- `uses: orkflow:<name>@<version>` composes another Orkflow as a child run,
  returning its declared `outputs:`.
- `await:` pauses a task until it is resolved from outside (an approval, an
  external completion, a one-time callback) or its deadline passes.
- `cache:` reuses a prior successful result for identical inputs (a memo).
- `join:` decides when a task may start from how its needs ended; failure
  handlers run compensation in a failing run.
- Images may declare their `with:` interface (`orkctl activity inspect`);
  promoting a version that breaks the Orkflow's declared interface needs an
  explicit acknowledgement (`orkctl orkflow compat`, `callers`).

**Starting runs**

- `orkctl run submit` (idempotency keys, `--origin-ref`), `run again`, and
  `run plan` to see which tasks would run without starting anything.
- Triggers: a bearer-token plain-HTTP starter
  ([ORKFLOW_TRIGGERS.md](./docs/ORKFLOW_TRIGGERS.md)).
- Schedules on Temporal Schedules: cron or interval, backfill, manual fire
  ([ORKFLOW_SCHEDULES.md](./docs/ORKFLOW_SCHEDULES.md)).
- Every run records its origin: what started it, a caller reference and the
  asserted principal.

**Operating** ([DEPLOY.md](./docs/DEPLOY.md))

- Namespaces with allowed pools, run retention, admission (`hold`,
  `max_running`), reserved cpu/memory budgets, allowed secret sources, a
  required image signer and a telemetry policy.
- A deployment image policy: require digests, allow registries, cosign keys.
- Releases: `orkctl release status|ramp|promote|rollback|wait` rolls a new
  build out without breaking running runs.
- Orker fleet: `orkctl orker list|get|cordon|uncordon|drain`.
- Declarative automation: one YAML document per namespace for its settings,
  schedules, triggers and webhooks, applied with `orkctl apply` and exported
  with `orkctl export` ([AUTOMATION.md](./docs/AUTOMATION.md)).
- Webhooks scoped by namespace, Orkflow and task, with signed bodies,
  delivery records and redelivery.
- A control-plane audit trail (`orkctl admin audit`).
- OpenTelemetry logs, metrics and traces, off by default, with named sinks
  per namespace.

**Inspecting** ([STATS.md](./docs/STATS.md),
[RUN_EVIDENCE.md](./docs/RUN_EVIDENCE.md))

- `orkctl run get|watch|logs|attempts|timing|inspect|diff|artifacts`: live
  attempt state, per-phase timing, every retry, and run-to-run comparison.
- Artifact provenance: whether a recorded artifact's bytes remain and which
  archived runs recorded the same object.
- `orkctl stats` with failure fingerprints, a flake index, reserved usage and
  cache hit rates — all derived on read.
- Evidence bundles: `orkctl run evidence export|verify`, verifiable offline.
- `orkctl task run` replays one task locally against a run's recorded bytes;
  `orkctl activity test` checks an image against the activity contract.

## Quickstart (dev)

```bash
make up             # Temporal, SeaweedFS, a registry, engine, orker and UI, health-checked
./bin/orkctl apply examples/hello.ork.yaml
./bin/orkctl run plan hello-ork     # which tasks would run; starts nothing
./bin/orkctl run submit hello-ork --watch
make down           # stop everything (data is kept)
```

An Orkflow is a YAML document that begins with `apiVersion: orkstack/v1` and
`kind: Orkflow`; `examples/hello.ork.yaml` is a small one, and
[docs/DSL.md](./docs/DSL.md) is the reference.

[`examples/README.md`](./examples/README.md) lists every example and what it
needs to run.

- UI: http://localhost:5173
- Engine API: http://localhost:8080
- Temporal UI: http://localhost:8233
- SeaweedFS filer: http://localhost:8888 (S3 API on http://localhost:9000)

`make status` shows what is running, `make logs` tails the engine, Orker and
UI logs, and `make reset` returns the machine to a vanilla state — it deletes
Temporal history, every object-store object (artifacts, archived runs, and the
Orkflow catalogue), the local artifact store and all build output. The
repository itself is untouched.

The individual steps are still there when you want them: `make tools`,
`make proto`, `make infra-up`, `make build`, `make run-ork`, `make run-orker`.
`make infra-up-observability` adds a local Grafana stack
([deploy/observability](./deploy/observability/README.md)).
To run the UI by hand, `cd ui && bun run dev`: its server proxies Connect calls
to `ENGINE_API_URL` (default `http://127.0.0.1:8080`), and the browser opens
WebSockets to `PUBLIC_ENGINE_WS_URL` (default the page's host on `:8080`).

Configuration needs nothing for a laptop — `make up` runs on defaults alone.
Past that, `ork` and `orker` read one shared YAML file (`--config PATH`), with
`ORK_`/`OTEL_` environment variables overriding it; see
[docs/DEPLOY.md](./docs/DEPLOY.md), "Configuration", and the generated
[`deploy/orkstack.defaults.yaml`](./deploy/orkstack.defaults.yaml).

### Linux hosts

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

## Contributing

Read [AGENTS.md](./AGENTS.md) (the engineering contract, including the
repository layout in §12) and [CONTRIBUTING.md](./CONTRIBUTING.md). Every
change ships with behavior tests and with evidence, in two tiers (AGENTS.md
§11.2): on a branch, `make proof-feature TESTS=… SPECS=…` runs the fast gates
plus the tests and specs that prove the change; to merge, `make proof` runs
every gate, every test and every spec against the real stack on the merged
tree (§11.3). Both keep their output under `proof/<name>/`, which is
gitignored; the commit or pull request quotes it. No proof, no done.

## Docs

| Document | What it covers |
|---|---|
| [ARCHITECTURE.md](./ARCHITECTURE.md) | Components, the engine's pieces, and how a run flows through them |
| [AGENTS.md](./AGENTS.md) | The engineering contract: boundaries, vocabulary, invariants, evidence |
| [CONTRIBUTING.md](./CONTRIBUTING.md) | The change checklist |
| [docs/DSL.md](./docs/DSL.md) | The Orkflow DSL: every key, validation rule and runtime behaviour |
| [docs/ORKFLOW_PATTERNS.md](./docs/ORKFLOW_PATTERNS.md) | The rules the shipped examples follow, and why |
| [docs/OCI_ACTIVITY_CONTRACT.md](./docs/OCI_ACTIVITY_CONTRACT.md) | The normative engine↔Orker↔container contract: filesystem ABI, commit protocol, failure categories, security profile |
| [docs/ACTIVITY_IMAGES.md](./docs/ACTIVITY_IMAGES.md) | The first-party images, the `op` convention, and writing your own |
| [docs/JSON.md](./docs/JSON.md) | Why runtime data is JSON, and the enum dialect clients share |
| [docs/ORKFLOW_TRIGGERS.md](./docs/ORKFLOW_TRIGGERS.md) | Inbound triggers: plain-HTTP run starters with bearer tokens |
| [docs/ORKFLOW_SCHEDULES.md](./docs/ORKFLOW_SCHEDULES.md) | Schedules on Temporal Schedules: specs, fires, backfill, overlap |
| [docs/AUTOMATION.md](./docs/AUTOMATION.md) | Automation documents: a namespace's settings, schedules, triggers and webhooks as YAML |
| [docs/schemas/](./docs/schemas/) | JSON Schemas of the authored documents (generated) |
| [docs/PROOFS.md](./docs/PROOFS.md) | Proofs and CI: the tiers, the plan and its shards, the job graph, artifacts and recovery procedures |
| [docs/DEPLOY.md](./docs/DEPLOY.md) | Topology, configuration, releases and rollback, operations, telemetry |
| [docs/STORAGE.md](./docs/STORAGE.md) | What is persisted where: the object-store key layout and its invariants |
| [docs/STATS.md](./docs/STATS.md) | Where stats, failure fingerprints, flakes, usage and cache numbers come from |
| [docs/RUN_EVIDENCE.md](./docs/RUN_EVIDENCE.md) | The run evidence bundle format and offline verification |
| [docs/UI_CLI_PARITY.md](./docs/UI_CLI_PARITY.md) | The operator experience contract the UI and CLI share |
| [examples/README.md](./examples/README.md) | Every example Orkflow and what it needs to run |
| [deploy/observability/README.md](./deploy/observability/README.md) | The optional local Grafana/Loki/Tempo/Prometheus profile |
| [internal/engine/testdata/history/README.md](./internal/engine/testdata/history/README.md) | The recorded histories that guard replay compatibility |

## License

MIT — see [LICENSE](./LICENSE).
