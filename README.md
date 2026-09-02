<h1 align="center">OrkStack</h1>

<p align="center">
  A thin, Temporal-backed durable execution platform. Author <em>Orkflows</em>
  in YAML and run any OCI image on a fleet of disposable workers
  (<em>Orkers</em>), without a workload SDK.
</p>

---

> Status: pre-release.

## Why

- **Durable orchestration on Temporal** — retries, timeouts, and replay for free.
- **OCI execution** — Orkers run ordinary container images through a stable
  filesystem contract; workloads need no OrkStack SDK.
- **Artifacts by reference** — files flow between tasks through local or S3/MinIO
  storage, not through the control plane.
- **One contract, everywhere** — a single Connect/proto API powers the `ork`
  engine, the Go CLI, and the browser UI, with realtime events over WebSocket.

## Architecture

See [ARCHITECTURE.md](./ARCHITECTURE.md). In short:

```
DSL YAML → ork engine (Temporal) → Orker → OCI image
                 │                           │
        CLI / SvelteKit UI            object storage
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
catalogue), the local artifact store and all build output. The repository
itself is untouched.

The individual steps are still there when you want them: `make tools`,
`make proto`, `make infra-up`, `make build`, `make run-ork`, `make run-orker`.

## Repository layout

See [AGENTS.md](./AGENTS.md) §12. The short version: `proto/` is the wire
contract, `internal/` holds engine/Orker/DSL implementation, `images/` holds
first-party OCI activities, `pkg/` holds reusable storage/logging code, `cmd/`
contains the binaries, and `ui/` is the SvelteKit app.

## Contributing

Read [AGENTS.md](./AGENTS.md) (engineering contract) and
[CONTRIBUTING.md](./CONTRIBUTING.md). Every change ships with behavior tests
and with evidence: `make proof` runs every gate against the real stack and
keeps the output under `proof/<name>/`, which is gitignored; the commit or
pull request quotes it. No proof, no done.

## Docs

- [Architecture](./ARCHITECTURE.md)
- [DSL reference](./docs/DSL.md)
- [OCI Activity Contract](./docs/OCI_ACTIVITY_CONTRACT.md)
- [Activity images and the `op` convention](./docs/ACTIVITY_IMAGES.md)
- [Deploying OrkStack](./docs/DEPLOY.md)
- [Storage layout](./docs/STORAGE.md)
- [JSON enum dialect](./docs/JSON.md)
- [Stats](./docs/STATS.md)

## License

MIT — see [LICENSE](./LICENSE).
