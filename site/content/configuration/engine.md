# Engine settings

These settings configure `ork`, the engine: its API and web UI listener, the limits on runs and history, run cleanup, and the caches behind the stats. `ork` and `orker` both read the Temporal, pool and deployment settings, so those are noted here as well.

## Decide these first

| Setting | Why a deployment must look at it |
|---|---|
| [`engine.history.max_events`](/docs/configuration/#engine-history-max-events) | Match the cluster's real per-run history limit. A value above it defeats the guard, and a value below it stops runs early. |
| [`engine.allowed_hosts`](/docs/configuration/#engine-allowed-hosts) | Every name with a dot that the API is reached by. Any other `Host` is refused with `403`. |
| [`engine.cleanup.enabled`](/docs/configuration/#engine-cleanup-enabled) | Off keeps every run. On deletes each finished run's stored data at its namespace's retention. |
| [`engine.orkflow_max_tasks`](/docs/configuration/#engine-orkflow-max-tasks) | The most tasks one Orkflow may declare. The default is the largest size measured. Above it, the deployment runs unmeasured. |
| [`engine.ws_allowed_origins`](/docs/configuration/#engine-ws-allowed-origins) | Set it only for a browser page served from another origin, or behind a proxy that rewrites `Host`. |

Both binaries also read these settings, which are listed with the rest: [`temporal.address`](/docs/configuration/#temporal-address), [`temporal.namespace`](/docs/configuration/#temporal-namespace), [`pool.name`](/docs/configuration/#pool-name), [`deployment.name`](/docs/configuration/#deployment-name) and [`deployment.build_id`](/docs/configuration/#deployment-build-id).

## Every engine setting

<!-- site-generated: settings-table-engine -->

## See also

- [DEPLOY.md, "1. Configuration"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#1-configuration): the full table, each key's variable and default
- [DEPLOY.md, "The API listener's bounds"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#the-api-listeners-bounds): the host, origin and request size rules
- [DEPLOY.md, "The web UI"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#the-web-ui): the UI's origin and the proxy rules
- [DEPLOY.md, "Task ceiling and the flat-stage bound"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#task-ceiling-and-the-flat-stage-bound): how the task limit relates to the history limit
- [DEPLOY.md, "Retention cleanup and bucket lifecycle rules"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#retention-cleanup-and-bucket-lifecycle-rules): what the cleanup settings do
- Setting details: [Setting details](/docs/configuration/details/)
- All settings: [All settings](/docs/configuration/)
