# Limitations

Each limit on this page is deliberate: the engineering contract fixes it, or it is a default a deployment may move, and the row says which, and names the setting where there is one. The reasoning is in [AGENTS.md](../AGENTS.md), and each row links the section that gives the bound and why it is there.

Deployment settings are the configuration keys of [DEPLOY.md](./DEPLOY.md), "1. Configuration", with their environment variables in capitals. Task settings, such as `retry:` and `timeout_seconds`, are written in the Orkflow. [deploy/orkstack.defaults.yaml](../deploy/orkstack.defaults.yaml) is the generated reference for every default.

## Pre-release status

OrkStack is pre-release: stable, but breaking changes may be introduced until 1.0. Each breaking change is marked **BREAKING** in [CHANGELOG.md](../CHANGELOG.md), and [UPGRADING.md](./UPGRADING.md) says what that means for an upgrade. No release or tag exists yet, and this page is not a claim that OrkStack is production-ready; [PRODUCTION_CHECKLIST.md](./PRODUCTION_CHECKLIST.md) says what that would take.

## Payloads and artifacts

Workflow payloads are small by contract. Anything large is an artifact, which is a file the Orker uploads and a run only references.

| Limit | Value or rule | Setting that moves it | Source |
|---|---|---|---|
| Workflow start payload | 256 KiB serialized at most. A run's input, its start definition and its plan are each refused at submit above it. Anything larger must be an artifact | none, fixed | [AGENTS.md](../AGENTS.md) §2.2, §7 |
| Definition too large to start | With the setting on, a definition above 256 KiB travels by reference. Its copy in the run is at most a quarter of the saved-state bound, compressed (2 MiB by default). A larger definition is refused at apply and at submit | `engine.definition_by_reference` (`ORK_DEFINITION_BY_REFERENCE`, on by default); `engine.continuation.bytes` | [DSL.md](./DSL.md), "How many tasks an Orkflow may declare"; [DEPLOY.md](./DEPLOY.md), "1. Configuration" |
| One run tree's plan | About 2,050 awaits with a callback, or 381 composed tasks of a one-task child, fill the 256 KiB plan. Past that a submit is refused, naming the plan's size | none, fixed | [DSL.md](./DSL.md), "How many tasks an Orkflow may declare" |
| Structured output, `values.json` | 256 KiB at most, valid UTF-8. A larger file is a `ContractViolation` (`VALUES_TOO_LARGE`). Only a file of 4 KiB or less is carried inline, for display | `engine.limits.values_max_bytes` (`ORK_VALUES_MAX_BYTES`), pinned at submit | [OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "3.4 `output/values.json`"; "1. Ownership: who knows what" |
| Artifacts per attempt | 256 at most | `engine.limits.artifact_max_count` (`ORK_ARTIFACT_MAX_COUNT`) | [OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "3.5 `output/artifacts/`" |
| Artifact size | 5 GiB each | `engine.limits.artifact_max_bytes` (`ORK_ARTIFACT_MAX_BYTES`) | [OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "3.5 `output/artifacts/`" |
| Artifact shape | Regular files directly inside the artifacts directory. A directory, symlink, device, socket or FIFO is refused, the scan does not recurse, and names must be UTF-8. Archive a tree first, for example as `workspace.tar.gz` | none, fixed | [OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "3.5 `output/artifacts/`"; [AGENTS.md](../AGENTS.md) §3.1 |
| Durable logs | 16 MiB per stream (stdout and stderr) per attempt | `engine.limits.log_max_bytes` (`ORK_LOG_MAX_BYTES`) | [DEPLOY.md](./DEPLOY.md), "1. Configuration" |
| Live log stream | 16 MiB per task attempt. Past it the Orker stores one truncation notice and nothing more | `orker.log.max_bytes` (`ORK_ORKER_LOG_MAX_BYTES`) | [DEPLOY.md](./DEPLOY.md), "Live task logs: where they are, what bounds them, how long they stay" |
| Completion commit | 4 MiB serialized | `engine.limits.commit_max_bytes` (`ORK_COMMIT_MAX_BYTES`) | [DEPLOY.md](./DEPLOY.md), "1. Configuration" |
| Trigger invoke | The body is one JSON object of 1 MiB at most. An origin reference is 256 bytes at most | none, fixed | [ORKFLOW_TRIGGERS.md](./ORKFLOW_TRIGGERS.md), "Invoking a trigger" |
| Schedule text | A description or a pause reason is 256 bytes at most | none, fixed | [ORKFLOW_SCHEDULES.md](./ORKFLOW_SCHEDULES.md), "Surfaces" |
| Names | Namespaces, Orkflows, task ids and pools match `^[a-z][a-z0-9-]*$` and are at most 200 bytes. A name is validated, never sanitised | none, fixed | [AGENTS.md](../AGENTS.md) §7 |
| Hold reason | 512 bytes at most, one printable line | none, fixed | [DEPLOY.md](./DEPLOY.md), "Namespace admission: `hold` and `max_running`" |

## Workflow shape

| Limit | Value or rule | Setting that moves it | Source |
|---|---|---|---|
| Tasks per Orkflow | 4,000 by default, a whole number of 1 or more. Checked at validate, apply and plan, and again at submit, so lowering the setting refuses runs of an Orkflow applied above the new value | `engine.orkflow_max_tasks` (`ORK_ORKFLOW_MAX_TASKS`) | [DSL.md](./DSL.md), "How many tasks an Orkflow may declare"; [DEPLOY.md](./DEPLOY.md), "1. Configuration" |
| Above the default | The default is the largest size measured. A deployment that goes above it runs unmeasured, and the boot warns, naming the measured size | `engine.orkflow_max_tasks` | [DEPLOY.md](./DEPLOY.md), "Task ceiling and the flat-stage bound" |
| A stage of plain tasks that start together | At the default settings, more than 4,222 such tasks may stop `RUN_HISTORY_LIMIT` in their first history, explicitly. Staying within the figure is necessary, not sufficient | the history, continuation and in-flight settings in the next section | [DSL.md](./DSL.md), "Validation rules", rule 43; [DEPLOY.md](./DEPLOY.md), "Task ceiling and the flat-stage bound" |
| A chain joined by default | Refused at apply past about 1,500 plain tasks, because each task's upstream references must fit one activity input | none, fixed | [DSL.md](./DSL.md), "Validation rules", rule 44 |
| Fan-out items | Each fan-out's `max_items` is from 1 to 10,000. The sum over one Orkflow is at most `engine.each_max_items`, 1,000 by default. Above 1,000 the setting needs `engine.continuation.enabled` | `engine.each_max_items` (`ORK_EACH_MAX_ITEMS`) | [DSL.md](./DSL.md), "Fan-out (`each:`)"; [DEPLOY.md](./DEPLOY.md), "1. Configuration" |
| Pending work per run | A run keeps at most 1,000 activities, child runs and pins pending at once (16 to 1,800). The rest wait `PENDING` and start as running work finishes | `engine.run_max_in_flight` (`ORK_RUN_MAX_IN_FLIGHT`) | [DEPLOY.md](./DEPLOY.md), "A run that pauses new tasks" |
| Composition depth | A run may start 4 levels of child runs. A deployment may lower this, never raise it | `engine.compose_max_depth` (`ORK_COMPOSE_MAX_DEPTH`) | [DEPLOY.md](./DEPLOY.md), "1. Configuration" |
| Await deadline | From 1 second to 30 days | `await.timeout_seconds` on the task | [DSL.md](./DSL.md), "Validation rules", rule 16 |
| Failure handler that runs a container | At most 32 needs | none, fixed | [DSL.md](./DSL.md), "Validation rules", rule 42 |
| Failure handler | Cannot be pinned, fanned out or cached | none, fixed | [DSL.md](./DSL.md), "Validation rules", rule 39 |

## Run history and saved state

The cluster ends a run that exceeds its history limit, and records no reason for it. OrkStack watches the size of each run's history, and stops the run first, explicitly, when it cannot carry on.

| Limit | Value or rule | Setting that moves it | Source |
|---|---|---|---|
| History per run | 51,200 events and 50 MiB by default. A run that nears either stops `FAILED` with `RUN_HISTORY_LIMIT`, keeping back what its failure path still needs | `engine.history.max_events` (`ORK_HISTORY_MAX_EVENTS`); `engine.history.max_bytes` (`ORK_HISTORY_MAX_BYTES`) | [DEPLOY.md](./DEPLOY.md), "1. Configuration"; [DSL.md](./DSL.md), "How many tasks an Orkflow may declare" |
| Cluster ends the run first | The one known gap: a failure path that lands many large results at once can reach the cluster's size limit first. The run still reads `RUN_HISTORY_LIMIT`, not a cancel | `engine.history.max_bytes` | [DEPLOY.md](./DEPLOY.md), "1. Configuration" |
| Carrying on | A run whose history has grown by 8,192 events or 8 MiB carries on into a fresh history at its next moment with nothing running. Its saved state carries over | `engine.continuation.enabled` (`ORK_CONTINUATION_ENABLED`, on by default); `engine.continuation.events`; `engine.continuation.bytes` | [DEPLOY.md](./DEPLOY.md), "A run that pauses new tasks" |
| No such moment | A run with no such moment holds new tasks, never running ones, once its history plus its failure-path reserve reaches 60% of the cluster's limit. It carries on when the running work finishes | none, fixed: the 60% line is pinned into each run's plan | [DEPLOY.md](./DEPLOY.md), "A run that pauses new tasks" |
| Saved state | 8 MiB at most. A larger saved state means the run stays in its current history and may stop `RUN_HISTORY_LIMIT`, naming the size | `engine.continuation.bytes` | [DSL.md](./DSL.md), "How many tasks an Orkflow may declare"; [DEPLOY.md](./DEPLOY.md), "1. Configuration" |
| Inline outputs | The saved state holds every task's inline output. At 4,000 tasks it fits while tasks return about 2,368 bytes of inline values on average | `engine.continuation.bytes` | [DSL.md](./DSL.md), "How many tasks an Orkflow may declare" |
| Restoring saved state | A carried run that cannot read its saved state within the window fails `RUN_STATE_UNAVAILABLE`, explicitly. The window is 1 minute to 8 hours, and 1 hour by default | `engine.continuation.restore_timeout` (`ORK_CONTINUATION_RESTORE_TIMEOUT`) | [DEPLOY.md](./DEPLOY.md), "1. Configuration" |
| Measured size | 4,000 tasks is the largest size measured. The measurement runs in the nightly and pre-release proofs, not in every merge proof | `engine.orkflow_max_tasks` | [DEPLOY.md](./DEPLOY.md), "Task ceiling and the flat-stage bound"; [PROOFS.md](./PROOFS.md) |

## Execution, retries and cancellation

| Limit | Value or rule | Setting that moves it | Source |
|---|---|---|---|
| At-least-once execution | Every activity may run more than once, for example after a retry or a lost Orker. OrkStack never claims exactly-once execution of external side effects. A durable commit only stops a second container run once the result is stored | none, fixed | [AGENTS.md](../AGENTS.md) §4; [OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "5.1 What a commit means and does not mean" |
| Idempotency key | `{namespace}/{runId}/{taskId}`, with the item index added for a fan-out item. It is stable across the retries of one logical activity | none, fixed | [OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "3.2 `input/task.json`" |
| Idempotency claim | A submit key stays bound for 24 hours | `engine.idempotency_ttl` (`ORK_IDEMPOTENCY_TTL`) | [DEPLOY.md](./DEPLOY.md), "1. Configuration" |
| Attempts | A task without `retry:` makes exactly one attempt. Inside `retry:`, an unset interval is 1 second, the coefficient 2.0 and the maximum interval 100 seconds | `retry:` on the task | [DSL.md](./DSL.md), "Tasks" |
| Container timeout | 300 seconds by default | `timeout_seconds` on the task | [DSL.md](./DSL.md), "Tasks" |
| Timed-out container | `TimedOut` is terminal by default and is not retried, because the same wall clock would time out again. A heartbeat timeout, which means a lost Orker, is retried under the task's policy | none, fixed | [OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "8.1 The five categories" |
| Cancellation | SIGTERM, then SIGKILL after the grace period. The grace period is 30 seconds by default | `cancellation_grace_period_seconds` on the task | [AGENTS.md](../AGENTS.md) §3.3; [DSL.md](./DSL.md), "Tasks" |
| Cancellation latency | A running task learns of cancellation within one heartbeat interval, 5 seconds by default, from 1 to 10 seconds | `orker.heartbeat_interval` (`ORK_ORKER_HEARTBEAT_INTERVAL`) | [DEPLOY.md](./DEPLOY.md), "1. Configuration" |
| Temporal frontend outage | A running container survives an outage of up to one heartbeat interval | `orker.heartbeat_interval` | [DEPLOY.md](./DEPLOY.md), "1. Configuration" |
| Lost Orker, ordinary task | The heartbeat timeout is 30 seconds. The attempt is retried under the task's policy, on a live Orker | `retry:` on the task | [OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "8.1 The five categories" |
| Pinned task | Never fails over. If its pin host is lost, the pin fails `PIN_HOST_LOST` and is not reopened on another Orker | none, fixed | [AGENTS.md](../AGENTS.md) §5; [DSL.md](./DSL.md), "Pins" |
| Pin creation | A five-minute creation timeout by default. A pool whose pin hosts stay full for that long fails the triggering task `PIN_UNAVAILABLE`. That task never reached an Orker, so its scheduled and started times stay unset. A pool whose pin hosts are all cordoned fails after about five minutes whatever the setting says | `engine.pin_creation_timeout` (`ORK_PIN_CREATION_TIMEOUT`), 1 second to 24 hours | [DSL.md](./DSL.md), "Pins" |
| Pin hosts | A pool hosts pins only where its Orkers set a pin limit above zero. The default is zero, so a document that pins is refused at submit into a pool with no pin host | `orker.pins.max_concurrent` (`ORK_ORKER_MAX_PINS`), set on every Orker of the pool | [DEPLOY.md](./DEPLOY.md), "Pin hosting is a pool-wide setting" |
| Empty pool | A submit into a pool with no live Orkers is refused, unless the caller passes `allow_empty_pool` | the submit flag, `allow_empty_pool` | [DEPLOY.md](./DEPLOY.md), "Running M Orker pools" |
| Failure handlers | A failing run still ends `FAILED` with its original reason. An operator's cancel starts no failure handler | none, fixed | [ORKFLOW_PATTERNS.md](./ORKFLOW_PATTERNS.md), "Undo what was done: failure handlers and sagas" |

## Authoring language

An Orkflow says what runs. It does not hold control flow beyond its DAG, and it does not compute.

| Limit | Value or rule | Source |
|---|---|---|
| No loops | There is no loop, back-edge or `while`. Polling is a bounded retry policy, periodic work is a schedule, and iteration is `each:` | [ORKFLOW_PATTERNS.md](./ORKFLOW_PATTERNS.md), "Repeat without loops" |
| No `continue_on_error` and no Orkflow-level `on_failure:` | A failure that should not fail the run is reported as data by the workload, and a `condition:` reads it | [DSL.md](./DSL.md), "Joins and failure handlers (`join:`)" |
| No schedule, trigger or webhook keys | These belong to the namespace, through `orkctl schedule`, `orkctl trigger`, `orkctl webhook` or an automation document | [DSL.md](./DSL.md), "What an Orkflow does not declare" |
| Templates | Go `text/template` with three helpers, `artifact`, `env` and `sh`, and Go's built-in functions. There is no expression or scripting language, and computation runs in a task | [ORKFLOW_PATTERNS.md](./ORKFLOW_PATTERNS.md), "The rule: the platform moves data, tasks compute"; [DSL.md](./DSL.md), "Templating" |
| Composition stays in one namespace | A composed task names one pinned version in its own namespace. The child receives values, never files. There is no retry or timeout for the child as a whole | [DSL.md](./DSL.md), "Composition (`uses: orkflow:`)" |
| Runtime data is JSON on the wire | The API, the CLI and the UI take JSON only. There is no YAML input mode, and [AGENTS.md](../AGENTS.md) §3.1 forbids adding one | [JSON.md](./JSON.md), "Why JSON, not YAML"; [AGENTS.md](../AGENTS.md) §3.1 |
| Authored runtime data is decoded strictly | A plain value YAML readers resolve differently (`yes`, `0755`, `1:30`, a date), a number JSON cannot hold exactly, and tags, anchors, aliases and merge keys are refused, naming the path | [JSON.md](./JSON.md), "Data written in an authored document" |

## Images and runtime

| Limit | Value or rule | Setting that moves it | Source |
|---|---|---|---|
| Container runtime | Docker is the only runtime shipped. It sits behind one seam, and no other runtime is built | none; `orker.docker.bin` (`ORK_DOCKER_BIN`) names the CLI only | [ARCHITECTURE.md](../ARCHITECTURE.md), "Orker (`internal/orker`, `cmd/orker`)"; [OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "1. Ownership: who knows what" |
| Local runs | `orkctl task run` and `orkctl activity test` start a container on the machine they run on, so they need a Linux or macOS host with a Docker daemon. On Windows both refuse at the start, with that reason. Every other `orkctl` command works there | none, fixed | [CLI.md](./CLI.md), "`orkctl task run`" and "`orkctl activity test`" |
| Network modes | `default` and `none` only. Named, Orker-defined network profiles are left for later | none, fixed | [DSL.md](./DSL.md), "Tasks"; [AGENTS.md](../AGENTS.md) §6 |
| Forbidden container settings | Privileged mode, host namespaces, host network, device passthrough, arbitrary host mounts, the Docker socket, and the Orker's own configuration, credentials or workspace root. No configuration key exists for any of them | none, fixed | [OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "9.1 Forbidden, without exception"; [AGENTS.md](../AGENTS.md) §6 |
| Credentials in a container | An activity container receives no Temporal, object-storage or OrkStack control-plane credentials, unless the user supplies them as workload secrets | none, fixed | [AGENTS.md](../AGENTS.md) §2.3 |
| Image identity | A tag is resolved to a digest before execution, and a running execution never switches images because a tag moved | none, fixed | [AGENTS.md](../AGENTS.md) §5 |
| Resource requests | A request above the Orker's cap is refused, never clamped. The caps default to 8 CPU and 16 GiB. A task that requests nothing gets 1 CPU and 1 GiB | `orker.resources.max_cpu`, `orker.resources.max_memory`, `orker.resources.default_cpu`, `orker.resources.default_memory` | [OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "9.3 Resource policy"; [DEPLOY.md](./DEPLOY.md), "1. Configuration" |
| Process count and scratch space | 512 processes per container and a 64 MiB temporary filesystem by default | `orker.resources.pids_limit` (`ORK_ORKER_PIDS_LIMIT`); `orker.resources.tmpfs_bytes` (`ORK_ORKER_TMPFS_BYTES`) | [DEPLOY.md](./DEPLOY.md), "1. Configuration" |
| Tasks per Orker | 16 at once by default. The rest wait in the pool for a slot | `orker.max_concurrent` (`ORK_ORKER_MAX_CONCURRENT`) | [DEPLOY.md](./DEPLOY.md), "Sizing an Orker's task ceiling" |

## Engine, cluster and gateway

| Limit | Value or rule | Setting that moves it | Source |
|---|---|---|---|
| Temporal Cloud | Not supported yet. The engine connects with the frontend address and the namespace only, and no TLS or credential setting exists for the connection. The authentication and TLS a hosted cluster needs are not built | `temporal.address`, `temporal.namespace` | [DEPLOY.md](./DEPLOY.md), "1. Configuration"; `internal/config/config.go` (the `temporal.*` entries); `internal/telemetry/boot.go` (`ClientOptions`) |
| Engine TLS | The API listener serves plain HTTP/1.1 and unencrypted HTTP/2, and the engine has no TLS setting. Terminate TLS in front of it | none | [ARCHITECTURE.md](../ARCHITECTURE.md), "API (`internal/api`, served by `cmd/ork`)"; `internal/api/server.go` (`NewServer`) |
| Caller authentication | OrkStack does not authenticate callers. Only the trigger invoke and the await callback check a bearer token. Authenticating callers is the deployment's gateway's job | none; the gateway | [ARCHITECTURE.md](../ARCHITECTURE.md), "API (`internal/api`, served by `cmd/ork`)" |
| Principal | The `Ork-Principal` header is recorded verbatim on audit records and origins. It is asserted, never verified | none | [AGENTS.md](../AGENTS.md) §7; [DEPLOY.md](./DEPLOY.md), "Attributing control-plane writes" |
| Trigger and callback rate | No rate limit, no queue and no retry on either route. The gateway bounds exposure, and a namespace's hold and cap bound what it admits | the gateway; a namespace's `--hold` and `--max-running` | [ORKFLOW_TRIGGERS.md](./ORKFLOW_TRIGGERS.md), "What triggers do not do" |
| Webhook delivery | Best-effort. A subscriber's outage never fails a run | none, fixed | [ORKFLOW_TRIGGERS.md](./ORKFLOW_TRIGGERS.md), "Trigger versus webhook" |
| Object storage | The store must implement conditional `PutObject`. An Orker probes it at boot and refuses to start on a store that cannot | none, fixed | [DEPLOY.md](./DEPLOY.md), "The bucket stops being optional at the second replica"; `cmd/orker/main.go` (`ProbeCommitStore`) |
| Local artifact backend | `local` is one directory per process. A replicated deployment cannot use it | `storage.backend` (`ORK_ARTIFACT_BACKEND`, default `local`) | [DEPLOY.md](./DEPLOY.md), "0. Topology — what you run, and where state lives" |
| Admission | `max_running` is a read followed by a start, with no lock, so a burst can land a few runs over the cap. An enforced budget is best-effort in the same way | `--max-running`; `--budget-enforce` | [DEPLOY.md](./DEPLOY.md), "Namespace admission: `hold` and `max_running`"; "Namespace budgets: reserved consumption, soft or enforced" |
| Usage | Reservation multiplied by held time, never measured utilisation. A soft budget notifies nothing | none, fixed | [DEPLOY.md](./DEPLOY.md), "Namespace budgets: reserved consumption, soft or enforced"; [STATS.md](./STATS.md), "8. Usage" |
| Run retention | Enforced only when cleanup is on. Off by default, so every run is kept for the deployment maximum plus the enforcement margin | `engine.cleanup.enabled` (`ORK_CLEANUP_ENABLED`, `false` by default) | [DEPLOY.md](./DEPLOY.md), "Run retention: a deployment maximum, a value per namespace" |
| Artifact bytes | Cleanup does not reclaim workload artifact bytes. A bucket lifecycle rule on `orkflows/` (and `objects/` on the local backend) is the operational answer | bucket lifecycle rule | [DEPLOY.md](./DEPLOY.md), "Retention cleanup and bucket lifecycle rules" |
| Audit trail | Nothing reclaims it. OrkStack has no setting for it | a bucket lifecycle rule on `metadata/audit/`, if you want one | [DEPLOY.md](./DEPLOY.md), "Attributing control-plane writes" |
| Run identifiers | A run id is single-use for all time. Nothing may parse one or derive meaning from its parts. Order runs by `created_at` | none, fixed | [DEPLOY.md](./DEPLOY.md), "5. Ops notes" |
| Idempotency scope | A key is scoped to the namespace it was submitted into | none, fixed | [DEPLOY.md](./DEPLOY.md), "5. Ops notes" |
| Schedule overlap | The `terminate-other` policy is not offered. OrkStack does not decide overlaps; the cluster does | none, fixed | [ORKFLOW_SCHEDULES.md](./ORKFLOW_SCHEDULES.md), "Temporal owns every piece of schedule state" |
| Backfill | A backfill window is at most 366 days | none, fixed | [ORKFLOW_SCHEDULES.md](./ORKFLOW_SCHEDULES.md), "Surfaces" |

## Deliberate omissions

The contract names these as non-goals, and says not to build them ([AGENTS.md](../AGENTS.md) §13).

- A visual workflow designer, an AI agent builder, a plugin marketplace and a business forms platform.
- A custom scripting language, a complex expression language, and an embedded Python or JavaScript runtime.
- A controller or a watch loop. An automation document is applied once by a client, and nothing stores a desired state for OrkStack to reconcile.
- An OrkStack-owned execution database, scheduler, event bus or state machine. Temporal owns execution, and object storage holds data ([AGENTS.md](../AGENTS.md) §2).
- A required durable system beyond Temporal and S3-compatible object storage ([AGENTS.md](../AGENTS.md) §2.2).
- A workload SDK, a sidecar, a message queue, a custom RPC framework or an application worker daemon. A workload is an OCI image that reads and writes files ([AGENTS.md](../AGENTS.md) §1).
- Keyless image signing, transparency logs, attestations and a policy language. Image signatures are key-based cosign signatures over the simple-signing payload ([DEPLOY.md](./DEPLOY.md), "Image supply-chain policy").

## Not built yet

These are deferred. Nothing on this list is promised by a date.

- Temporal Cloud, with the authentication and TLS a hosted cluster needs.
- Named, Orker-defined network profiles. Only `default` and `none` exist ([AGENTS.md](../AGENTS.md) §6).
- Container runtimes other than Docker ([ARCHITECTURE.md](../ARCHITECTURE.md), "Orker (`internal/orker`, `cmd/orker`)").
- A structured progress channel from a container. Plain stdout and stderr are the required interface, and the container implements no heartbeat ([AGENTS.md](../AGENTS.md) §3.3).
- Reading cosign's newer signature bundle format. OrkStack reads the key-based `sha256-<hex>.sig` layout only. cosign 3 writes the bundle by default, and cosign 3.1 marks the flags that still write the `.sig` layout (and that the release signs with) deprecated ([DEPLOY.md](./DEPLOY.md), "Image supply-chain policy").
- Automatic reclamation of workload artifact bytes by the cleanup worker. It would mean a scan of the whole reference index, so the cleanup worker does not do it, and a bucket lifecycle rule is the current answer ([DEPLOY.md](./DEPLOY.md), "Retention cleanup and bucket lifecycle rules").
