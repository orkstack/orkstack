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
make tools          # install codegen tooling (buf, protoc-gen-*)
make proto          # generate Go/TS from proto
make infra-up       # start Temporal + MinIO (docker compose)
make build          # build ork, orker, orkctl
make run-ork        # terminal 1
make run-orker      # terminal 2
./bin/orkctl apply examples/hello.ork.yaml
./bin/orkctl run submit hello-ork --watch
```

- Temporal UI: http://localhost:8233
- MinIO console: http://localhost:9001 (`orkstack` / `orkstack-secret`)

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
