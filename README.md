<h1 align="center">OrkStack</h1>

<p align="center">
  Durable workflows without workflow code. Write an <em>Orkflow</em> in YAML,
  and every step runs as an ordinary OCI container on Temporal, with no SDK
  in your images.
</p>

<p align="center">
  <a href="https://orkstack.github.io/orkstack/"><strong>Documentation</strong></a>
  ·
  <a href="./docs/GETTING_STARTED.md">Getting started</a>
  ·
  <a href="./examples/README.md">Examples</a>
  ·
  <a href="./CHANGELOG.md">Changelog</a>
</p>

---

> **Status: pre-release.** Stable, but breaking changes may be introduced
> until 1.0. Each one is listed in [CHANGELOG.md](./CHANGELOG.md), marked
> **BREAKING**.

## What it is

OrkStack is a thin contract between three boring parts:

| Part | Owns |
|---|---|
| **Temporal** | execution: scheduling, retries, timers, signals, durable state |
| **Object storage** (S3-compatible) | data: artifacts, logs, immutable records |
| **OCI containers** | computation: any image, run by a disposable worker (an *Orker*) |

```text
YAML Orkflow ──apply──> ork ──compile──> Temporal ──dispatch──> Orker
                                                                  │
                                                             OCI image
                                                                  │
                                                           object storage
```

A task receives files, writes files, prints logs and exits. OrkStack adds no
second state machine and no database of its own; the `ork` engine, the
`orkctl` CLI and the browser UI share one Connect API.
[ARCHITECTURE.md](./ARCHITECTURE.md) shows how a run flows through them.

## An Orkflow

[`examples/each-fan-out.ork.yaml`](./examples/each-fan-out.ork.yaml): compute
a list in one task, convert each element as its own durable item, then merge
the results.

```yaml
apiVersion: orkstack/v1
kind: Orkflow
name: each-fan-out
version: "0.3.0"
description: "List parts in a json task, convert each part as its own item, merge them."

tasks:
  - id: list
    uses: json
    network: none
    with:
      op: query
      nullInput: true
      vars:
        parts: [{name: a}, {name: b}, {name: c}, {name: d}]
      filter: '$vars.parts'

  - id: convert
    uses: shell
    needs: [list]
    each:
      over: '{{ .tasks.list.output.result }}'
      as: part
      max_items: 50
      max_parallel: 2
    retry: { maximum_attempts: 2 }
    with:
      op: script
      interpreter: sh
      script: |
        set -eu
        # `sh` quotes the item's value for the shell (AGENTS.md §6).
        printf '%s\n' {{ sh .each.part.name }} > /orkstack/output/artifacts/part.txt
        echo converted {{ sh .each.part.name }} "({{ .each.index }} of {{ .each.count }})"

  - id: merge
    uses: shell
    needs: [convert]
    with:
      op: script
      interpreter: sh
      script: |
        set -eu
        cat /orkstack/input/artifacts/convert/*/part.txt | tee /orkstack/output/artifacts/merged.txt
```

## Quickstart

You need Docker, Go and Bun ([what you need](./docs/GETTING_STARTED.md#what-you-need)).

```bash
make infra-up        # Temporal, SeaweedFS and a local registry
make -C images push  # the first-party activity images, into that registry
make up              # the engine, an Orker and the UI, health-checked
./bin/orkctl apply examples/each-fan-out.ork.yaml
./bin/orkctl run submit each-fan-out --watch
```

Then open the UI at http://localhost:5173. `make down` stops everything and
keeps the data. [Getting started](./docs/GETTING_STARTED.md) walks through
each step.

## What it does not do

- It never claims exactly-once execution: tasks run at least once, with a
  stable idempotency key for systems that need one.
- It has no SDK, no worker service to write, and no callback protocol: the
  filesystem is the activity API.
- It is not a visual designer, a scripting language or a connector catalogue.

[LIMITATIONS.md](./docs/LIMITATIONS.md) lists every hard limit and deliberate
omission, with its setting and its reason.

## Docs

The documentation is published at
**[orkstack.github.io/orkstack](https://orkstack.github.io/orkstack/)**, built
from these files.

- **Start:** [Getting started](./docs/GETTING_STARTED.md) ·
  [Examples](./examples/README.md) · [Architecture](./ARCHITECTURE.md)
- **Authoring:** [Orkflow DSL](./docs/DSL.md) ·
  [Patterns](./docs/ORKFLOW_PATTERNS.md) · [JSON and enums](./docs/JSON.md) ·
  [Schemas](./docs/schemas/)
- **Activities:** [OCI activity contract](./docs/OCI_ACTIVITY_CONTRACT.md) ·
  [Activity images](./docs/ACTIVITY_IMAGES.md)
- **Starting runs:** [Triggers](./docs/ORKFLOW_TRIGGERS.md) ·
  [Schedules](./docs/ORKFLOW_SCHEDULES.md) ·
  [Automation documents](./docs/AUTOMATION.md) · [Webhooks](./docs/WEBHOOKS.md)
- **Operating:** [Deployment](./docs/DEPLOY.md) ·
  [Production checklist](./docs/PRODUCTION_CHECKLIST.md) ·
  [Security model](./docs/SECURITY_MODEL.md) ·
  [Upgrading](./docs/UPGRADING.md) · [Limitations](./docs/LIMITATIONS.md) ·
  [Troubleshooting](./docs/TROUBLESHOOTING.md) ·
  [Storage](./docs/STORAGE.md) · [Stats](./docs/STATS.md) ·
  [Run evidence](./docs/RUN_EVIDENCE.md) ·
  [UI and CLI parity](./docs/UI_CLI_PARITY.md) ·
  [Observability](./deploy/observability/README.md)
- **Reference:** [orkctl](./docs/CLI.md) · [API](./docs/API.md) ·
  [Error reasons](./docs/REASONS.md) · [Changelog](./CHANGELOG.md)
- **Contributing:** [Contributing](./CONTRIBUTING.md) ·
  [Engineering contract](./AGENTS.md) · [Proofs and CI](./docs/PROOFS.md) ·
  [How OrkStack is proven](./docs/PROVEN.md) ·
  [Replay fixtures](./internal/engine/testdata/history/README.md)

## Contributing

Read [CONTRIBUTING.md](./CONTRIBUTING.md) and [AGENTS.md](./AGENTS.md), the
engineering contract. Every change ships with behavior tests and with
evidence: `make proof-feature` on a branch, and a full `make proof` of the
merged tree before it lands. No proof, no done.

## License

MIT — see [LICENSE](./LICENSE).
