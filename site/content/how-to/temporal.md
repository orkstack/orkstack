# Temporal

Temporal holds the state of every live run. OrkStack connects to a Temporal cluster you run, and each environment gets its own namespace in it.

## Example

Both binaries read the same two values. Use one Temporal namespace per environment:

```yaml
temporal:
  address: temporal.internal.example:7233
  namespace: ork-prod
```

## Connect

| Setting | Default | Notes |
|---|---|---|
| `temporal.address` | `localhost:7233` | The Temporal frontend. |
| `temporal.namespace` | `ork-main` | The partition for this deployment. OrkStack registers it at boot. Nothing crosses it. |

Set the same values on every `ork` and every `orker`. OrkStack has no TLS or credential setting for the connection, so a hosted Temporal Cloud is not supported yet.

Run one OrkStack deployment per Temporal namespace. Two deployments in one namespace would share internal routing and fight over it.

## Match the cluster's history limits

A run stops with `RUN_HISTORY_LIMIT` before the cluster's own limit, so the engine has to know that limit:

- `engine.history.max_events` defaults to `51200`, and `engine.history.max_bytes` to `50 MiB`.
- If an operator changed the cluster's limits, set these to match. A value above the real limit defeats the guard. A value below it stops runs early.

Both are read at submit, so a change applies to new runs.

## Retention is derived, not set

OrkStack sets the cluster's own retention from the deployment's maximum run retention plus a margin. Do not set that retention on the cluster by hand. Use [Retention](/docs/operate/retention/) to change how long runs are kept.

## What the cluster shows

A run that carries on past its history budget is a chain of executions in the cluster. Every OrkStack surface, including `orkctl` and the web UI, shows it as one run with one id and one event stream. To cancel a run, use `orkctl run cancel`. End a run through the cluster only as the upgrade procedure directs.

At boot, each engine asks the cluster to index the run fields that run lists filter on. If the cluster refuses, the engine logs a warning. Run lists then walk the whole index, and a list that cannot finish that walk fails rather than returning a short one.

## Back up both stores

OrkStack holds no state in its processes. Temporal holds live runs, and the bucket holds definitions, artifacts and archives. OrkStack ships no backup or restore command, so plan backups for both.

## Limits

A hosted Temporal Cloud is not supported yet, because the connection has no TLS or credential setting. Run the cluster yourself, with its own backups and upgrades.

## See also

- [DEPLOY.md, "0. Topology — what you run, and where state lives"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#0-topology--what-you-run-and-where-state-lives): the cluster's place in the deployment
- [DEPLOY.md, "5. Ops notes"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#5-ops-notes): one namespace per environment, and the run indexes the engine registers at boot
- [DEPLOY.md, "Run retention: a deployment maximum, a value per namespace"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#run-retention-a-deployment-maximum-a-value-per-namespace): the cluster's derived retention
- [DEPLOY.md, "Runs that span several executions: what the cluster shows"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#runs-that-span-several-executions-what-the-cluster-shows): the cluster's view of a run that carries on
- [DEPLOY.md, "1. Configuration"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#1-configuration): the history limits and every Temporal key
- [LIMITATIONS.md, "Engine, cluster and gateway"](https://github.com/orkstack/orkstack/blob/main/docs/LIMITATIONS.md#engine-cluster-and-gateway): what is not supported yet
- [PRODUCTION_CHECKLIST.md, "Temporal"](https://github.com/orkstack/orkstack/blob/main/docs/PRODUCTION_CHECKLIST.md#temporal): the items to check
- Settings: [Engine settings](/docs/configuration/engine/)
