# Namespaces

Learn what a namespace separates, which namespace a command uses, and which limits you can set on one.

```bash
orkctl namespace create payments --default-pool main --allow-pool main --max-running 20
orkctl -n payments apply examples/each-fan-out.ork.yaml
```

The first command creates a namespace with a pool and a cap on runs in flight. The second applies an Orkflow into it.

## What a namespace separates

A namespace is a scope. An Orkflow's name is unique within one namespace, so two namespaces can each hold a `deploy` and never see each other's. Runs are scoped the same way: `orkctl run get` finds a run only in the namespace the command acts in.

A command acts in the namespace given by `-n`. If there is none, it uses `ORK_NAMESPACE`, then the namespace saved by `orkctl namespace use`, then `default`.

The built-in `default` namespace always resolves, without a document to create it. Deleting it is refused.

## Limits you can set

These settings belong to a namespace. When a run is submitted, OrkStack checks `hold`, then `max-running`, then an enforced budget. A refused run leaves nothing behind.

| Setting | What it does |
|---|---|
| `--max-running N` | Refuses a new run when `N` runs of the namespace are already in flight. `0` means no limit. |
| `--hold "REASON"` | Closes the namespace to new runs. Runs already accepted finish. `--release-hold` reopens it. |
| `--budget-cpu-seconds`, `--budget-gib-seconds`, `--budget-period` | Bounds the cpu and memory the namespace reserves over a rolling period. Soft budgets only report. `--budget-enforce` makes them refuse new runs. |
| `--run-retention` | How long a finished run stays readable, counted from when it finished. Enforced only when the operator turns on run cleanup. |
| `--allow-pool` | The pools the namespace's tasks may name. |
| `--allow-secret` | The secret sources the namespace's tasks may name. |
| `--require-signed-by` | Requires every image in the namespace to be signed by a named key. |
| `--telemetry-sink` | Where the namespace's telemetry goes. |

The cap on runs in flight is a guard, not a lock. Runs started at the same moment on several engine replicas can land a few over the cap.

## See also

- [docs/DEPLOY.md, Namespace admission](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#namespace-admission-hold-and-max_running): how `hold` and `max_running` are checked.
- [docs/DEPLOY.md, Namespace budgets](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#namespace-budgets-reserved-consumption-soft-or-enforced): what a budget counts, and how to enforce one.
- [docs/DEPLOY.md, Run retention](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#run-retention-a-deployment-maximum-a-value-per-namespace): the deployment maximum, and the value per namespace.
- [docs/DSL.md, namespace](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#namespace): how a document chooses its namespace.
