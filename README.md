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
./bin/orkctl run plan hello-ork     # which tasks would run; starts nothing
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

### Linux hosts

Docker Desktop (macOS, Windows) provides `host.docker.internal` inside
containers automatically; plain Docker Engine on Linux does not. Several
integration and Playwright tests start a stub server on the host and have an
activity container call it at `host.docker.internal`, so on Linux run once:

```bash
# make the name resolve inside containers (containers use the host's
# resolver on the docker0 gateway, which serves /etc/hosts)
echo '172.17.0.1 host.docker.internal' | sudo tee -a /etc/hosts

# if ufw is active with a default-deny input policy, let containers on the
# default bridge reach host TCP ports
sudo ufw allow in on docker0 from 172.17.0.0/16 proto tcp
```

Verify:

```bash
docker run --rm alpine:3.20 getent hosts host.docker.internal
```

`172.17.0.1` is the gateway of Docker's default bridge subnet; a host with a
customized `docker0` uses its own gateway address. macOS needs none of this;
Docker Desktop already provides the name.

Configuration needs none of this for a laptop — `make up` runs on defaults
alone. Past that, `ork` and `orker` read one shared YAML file (`--config
PATH`), with `ORK_`/`OTEL_` environment variables overriding it; see
[docs/DEPLOY.md](./docs/DEPLOY.md) §1.

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
