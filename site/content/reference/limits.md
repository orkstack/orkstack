# Limits

The bounds OrkStack enforces on payloads, runs, tasks and containers. Each row names the setting that moves it, or says the bound is fixed. Values are the defaults.

## Payloads and artifacts

| Limit | Value | Setting |
|---|---|---|
| Workflow start payload: a run's input, its definition and its plan | 256 KiB each | Fixed |
| Definition too large for a start | Sent by reference when over 256 KiB. Its copy in the run is at most 2 MiB, compressed | `ORK_DEFINITION_BY_REFERENCE` (on) |
| `values.json` of one task | 256 KiB | `ORK_VALUES_MAX_BYTES` |
| Artifacts per attempt | 256 | `ORK_ARTIFACT_MAX_COUNT` |
| Size of one artifact | 5 GiB | `ORK_ARTIFACT_MAX_BYTES` |
| Durable log, per stream, per attempt | 16 MiB | `ORK_LOG_MAX_BYTES` |
| Live log, per attempt | 16 MiB, then one truncation notice | `ORK_ORKER_LOG_MAX_BYTES` |
| Serialized completion commit | 4 MiB | `ORK_COMMIT_MAX_BYTES` |
| Trigger request body | 1 MiB | Fixed |
| Hold reason | 512 bytes | Fixed |
| Names: namespaces, Orkflows, task ids and pools | 200 bytes, `^[a-z][a-z0-9-]*$` | Fixed |

## Workflow shape

| Limit | Value | Setting |
|---|---|---|
| Tasks per Orkflow | 4,000. Above it, the deployment runs unmeasured and the boot warns | `ORK_ORKFLOW_MAX_TASKS` |
| Fan-out items per Orkflow | 1,000 in total, the sum of every `max_items` | `ORK_EACH_MAX_ITEMS`. Up to 10,000. Above 1,000 needs continuation on |
| `max_items` of one fan-out | 1 to 10,000 | `each.max_items` on the task |
| Activities, child runs and pins pending at once, per run | 1,000. Allowed range 16 to 1,800 | `ORK_RUN_MAX_IN_FLIGHT` |
| Levels of child runs | 4 | `ORK_COMPOSE_MAX_DEPTH`. Can only be lowered |
| Await deadline | 1 second to 30 days | `timeout_seconds` on the `await:` |
| Plain tasks in one stage that start together | More than about 4,222 may stop with `RUN_HISTORY_LIMIT` in their first history. Staying under it is not a guarantee | History, continuation and in-flight settings |
| Chain of plain tasks joined by default | About 1,500, refused at apply | Fixed |

## Run history and saved state

| Limit | Value | Setting |
|---|---|---|
| History of one run | 51,200 events and 50 MiB | `ORK_HISTORY_MAX_EVENTS`, `ORK_HISTORY_MAX_BYTES` |
| A run carries on into a fresh history after | 8,192 events or 8 MiB of growth, at its next idle moment | `ORK_CONTINUATION_EVENTS`, `ORK_CONTINUATION_BYTES` |
| Saved state | 8 MiB at most | `ORK_CONTINUATION_BYTES` |
| Restoring saved state | 1 hour. Allowed range 1 minute to 8 hours | `ORK_CONTINUATION_RESTORE_TIMEOUT` |
| New tasks held when the history, plus the room its failure path needs, reaches | 60% of the history limit | Fixed |
| Largest measured Orkflow | 4,000 tasks | `ORK_ORKFLOW_MAX_TASKS` |

## Execution, retries and cancellation

| Limit | Value | Setting |
|---|---|---|
| Attempts | 1, unless the task sets `retry:` | `retry:` on the task |
| Retry interval, when `retry:` sets none | 1 second first, doubling, at most 100 seconds | `retry:` on the task |
| Container timeout | 300 seconds | `timeout_seconds` on the task |
| Cancellation grace period | 30 seconds | `cancellation_grace_period_seconds` on the task |
| Heartbeat interval | 5 seconds. Allowed range 1 to 10 seconds | `ORK_ORKER_HEARTBEAT_INTERVAL` |
| Heartbeat timeout, for a lost Orker | 30 seconds | Fixed |
| Pin creation | 5 minutes. Allowed range 1 second to 24 hours | `ORK_PIN_CREATION_TIMEOUT` |
| Submit idempotency key | 24 hours | `ORK_IDEMPOTENCY_TTL` |
| Reuse of a cached result | At most 168 hours from its commit | `ORK_CACHE_MAX_TTL` |

## Images and containers

| Limit | Value | Setting |
|---|---|---|
| Tasks one Orker runs at once | 16 | `ORK_ORKER_MAX_CONCURRENT` |
| CPU per task | 1 by default, 8 at most. A request above the cap is refused | `ORK_ORKER_DEFAULT_CPU`, `ORK_ORKER_MAX_CPU` |
| Memory per task | 1 GiB by default, 16 GiB at most. A request above the cap is refused | `ORK_ORKER_DEFAULT_MEMORY`, `ORK_ORKER_MAX_MEMORY` |
| Processes per container | 512 | `ORK_ORKER_PIDS_LIMIT` |
| `/tmp` inside a container | 64 MiB | `ORK_ORKER_TMPFS_BYTES` |
| Image pull | 5 minutes | `ORK_DOCKER_PULL_TIMEOUT` |
| Network modes | `default` and `none` | Fixed |

## Not built yet

| Missing | What it means for you |
|---|---|
| Temporal Cloud | The engine takes an address and a namespace. It has no TLS or credential setting for the connection. |
| Engine TLS | The API serves plain HTTP. Terminate TLS in front of it. |
| Caller authentication | None in OrkStack. A gateway authenticates callers. |
| Container runtimes other than Docker | Docker is the only runtime. |
| Network profiles | Only `default` and `none` exist. |
| Progress reports from a container | Use stdout and stderr. Containers send no heartbeats. |
| Reclaiming artifact bytes | Cleanup does not remove them. A bucket lifecycle rule does. |

## See also

- [LIMITATIONS.md, “Payloads and artifacts”](https://github.com/orkstack/orkstack/blob/main/docs/LIMITATIONS.md#payloads-and-artifacts): each bound and its source
- [LIMITATIONS.md, “Run history and saved state”](https://github.com/orkstack/orkstack/blob/main/docs/LIMITATIONS.md#run-history-and-saved-state): the history rules
- [LIMITATIONS.md, “Not built yet”](https://github.com/orkstack/orkstack/blob/main/docs/LIMITATIONS.md#not-built-yet): the full list
- [DEPLOY.md, “Task ceiling and the flat-stage bound”](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#task-ceiling-and-the-flat-stage-bound): how the task ceiling is derived
- Settings: [Configuration](/docs/reference/configuration/), [Orkflow keys](/docs/reference/orkflow-keys/)
