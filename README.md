<h1 align="center">OrkStack</h1>

<p align="center">
  A powerful, Temporal-backed orkflow engine. Author <em>orkflows</em> in a simple
  YAML DSL; run them durably on a fleet of pluggable workers (<em>Orkers</em>);
  drive and observe them from a modern CLI and a realtime UI.
</p>

---

> Status: pre-release, building toward **v0.0.1**. See [milestones](#milestones).

## Why

- **Durable orchestration on Temporal** — retries, timeouts, and replay for free.
- **Pluggable execution** — Orkers run plugins (`http`, `shell`, `file`, …) with
  a fixed task contract: input payload + artifacts → output payload + artifacts.
- **Artifacts by reference** — files flow between tasks through local or S3/MinIO
  storage, not through the control plane.
- **One contract, everywhere** — a single Connect/proto API powers the `ork`
  engine, the Go CLI, and the browser UI, with realtime events over WebSocket.

## Architecture

See [ARCHITECTURE.md](./ARCHITECTURE.md). In short:

```
DSL yaml → ork engine (Temporal) → Orker → Plugin → artifacts (local/S3)
                 │  realtime events (Connect stream + WebSocket)
        CLI (orkctl) / SvelteKit UI
```

## Quickstart (dev)

```bash
make up             # Temporal + MinIO + engine + orker + UI, health-checked
./bin/orkctl apply examples/hello.ork.yaml
./bin/orkctl run submit hello-ork --watch
make down           # stop everything (data is kept)
```

- UI: http://localhost:5173
- Engine API: http://localhost:8080
- Temporal UI: http://localhost:8233
- MinIO console: http://localhost:9001 (`orkstack` / `orkstack-secret`)

`make status` shows what is running, `make logs` tails all three processes,
and `make reset` returns the machine to a vanilla state — it deletes Temporal
history, every MinIO object (artifacts, archived runs, and the orkflow
catalogue), the local artifact store and all build output. The repository,
including `proof/`, is untouched.

The individual steps are still there when you want them: `make tools`,
`make proto`, `make infra-up`, `make build`, `make run-ork`, `make run-orker`.

## Repository layout

See [AGENTS.md](./AGENTS.md) §2. The short version: `proto/` is the contract,
`internal/` is the engine/orker/dsl, `pkg/` holds the plugin SDK, plugins, and
artifact store, `cmd/` are the binaries, `ui/` is the SvelteKit app.

## Contributing

Read [AGENTS.md](./AGENTS.md) (engineering contract) and
[CONTRIBUTING.md](./CONTRIBUTING.md). Every change ships with behavior tests and
a reproducible proof artifact under `proof/`.

## Docs

- [DSL reference](./docs/DSL.md)
- [Plugin reference](./docs/PLUGINS.md)
- [Plugin design: the `op` pattern](./docs/PLUGIN_DESIGN.md)
- [Architecture](./ARCHITECTURE.md)

## Milestones

| id | area | status |
|----|------|--------|
| M0 | Foundation: contracts, infra, conventions | in progress |
| M1 | Artifact storage (local + S3/MinIO) | planned |
| M2 | Engine: Temporal + DSL + Connect + WS | planned |
| M3 | Orker + plugins (http/shell/file) | planned |
| M4 | CLI (orkctl) | planned |
| M5 | UI (SvelteKit realtime) | planned |
| M6 | E2E + proof + v0.0.1 release | planned |

## License

MIT — see [LICENSE](./LICENSE).
