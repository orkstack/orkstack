# Observe a deployment

Send metrics, traces and logs from `ork` and `orker` to a collector you already run. Nothing is exported until you set a variable. Telemetry never fails a task or a run.

## Example

Set these on every `ork` and every `orker`:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector.internal.example:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_METRICS_EXPORTER=otlp
export OTEL_TRACES_EXPORTER=otlp
export OTEL_LOGS_EXPORTER=otlp
```

To try this on a laptop first, run `make up-observability`: it starts OrkStack with a local Collector, Loki, Tempo, Prometheus and Grafana, and sends them everything. That command needs a clone of the repository, and it runs from the clone's folder. Grafana is then at `http://localhost:3000`, with two dashboards, OrkStack and OrkStack / Run, on this machine only: every port the profile publishes is bound to `127.0.0.1`.

## What each kind of telemetry gives you

| Signal | Turn it on with | What you get |
|---|---|---|
| Metrics | `OTEL_METRICS_EXPORTER=otlp`, or `prometheus` | Counts and durations of runs, tasks and refusals. |
| Traces | `OTEL_TRACES_EXPORTER=otlp` | One trace per run and its child runs, across `ork` and every Orker that ran their tasks. |
| Logs | `OTEL_LOGS_EXPORTER=otlp` | Process logs. |
| Task output | `ORK_ORKER_TASK_LOG_EXPORT=true` on each Orker, with logs on | Each line of task stdout and stderr, as a log record. |
| Process log format | `ORK_LOG_FORMAT=json` | JSON lines. The default is text. |

With `OTEL_METRICS_EXPORTER=prometheus`, set `ORK_ADMIN_ADDR` and let Prometheus scrape `/metrics` on it.

## Follow a run in a trace

A run's trace starts with the request that submitted it, or with the schedule that fired it. Its child runs join it. Each run has an `ork.run` span, made when the run ends. Each attempt has an `ork.task.attempt` span.

- To find a run and everything below it, match `ork.run_id` or `ork.root_run_id` with the run's ID.
- `Run.trace_id` in the API is the run's trace.
- A task reads its own span under `trace` in `task.json`. Pass `traceparent` on to what you call, to see your own work in the same trace.
- A run's spans go to the destination its namespace chose.

A request that carries a `traceparent` starts a trace of its own, linked to the caller's. To join the caller's trace instead, set this on every `ork`, and only if you trust whoever sends it:

```bash
export ORK_TELEMETRY_TRUST_INBOUND_TRACE=true
```

## The admin listener

`ORK_ADMIN_ADDR` opens a listener for `/healthz`, `/readyz`, `/metrics` and, when `ORK_PPROF_ENABLED=true`, `/debug/pprof/`. It is never the API port. Bind it to a private address. Leave `ORK_PPROF_ENABLED` off unless you are profiling. An Orker's memory holds resolved secret values.

The listener answers only names it knows in a request's `Host`: an IP address, `localhost`, a name without a dot, and the host `ORK_ADMIN_ADDR` names. It refuses any other name with `403`. If Prometheus scrapes by a name with a dot, such as `host.docker.internal` or a Service's full name, list that name in `ORK_ADMIN_ALLOWED_HOSTS` on the binary it scrapes. 0.1.0 had no such check, so set it before you upgrade from it.

## Metrics to alert on

| Metric | Labels | Counts |
|---|---|---|
| `ork.runs` | `status`, `ork.run.resumption` | Each run when it reaches a final state, and a resumed run again at each later ending, labelled `ork.run.resumption="true"`. Leave those out to count runs. |
| `ork.run.duration` | `status`, `ork.run.resumption` | Time from start to finish of each run, recorded at the same endings. |
| `ork.tasks` | `ork.pool`, `status`, `failure_category` | Each task, once, when it reaches a final state. |
| `ork.task.duration` | `ork.pool`, `status` | Time from dispatch to finish of each task. |
| `ork.orker.tasks` | `outcome`, `ork.failure.reason` | Each attempt, on the Orker that ran it. |
| `ork.runs.refused` | `ork.namespace`, `ork.refusal.reason` | Submissions refused by a hold, a cap or a budget. |
| `ork.audit.records` | `action`, `outcome` | Control-plane writes. A rising `dropped` rate means the audit store cannot be written. |
| `ork.orker.capacity.reserved.cpu`, `.memory` | `ork.orker_id` | What stands reserved on an Orker that declares a capacity. `ork.orker.capacity.cpu` and `.memory` are the capacity itself. |
| `ork.orker.admission.wait.duration` | `ork.orker_id`, `ork.admission.bound` | How long an Orker went without room to ask for another task: `capacity`, or `slots` when every task slot was taken. |
| `ork.orker.store_outage.active` | `ork.orker_id` | 1 while an Orker has cordoned itself because object storage does not answer. `ork.orker.store_outage.transitions` counts each entry and exit. |
| `ork.orker.running_tasks`, `.open_pins`, `.max_concurrent` | `ork.orker_id`, `ork.pool` | An Orker's tasks running now, its open pins and how many tasks it runs at once. Sum them by `ork.pool`. |
| `ork.orker.cordoned` | `ork.orker_id`, `ork.pool`, `ork.cordon.cause` | 1 while that cause of a cordon holds, else 0: `operator`, `store_outage` or `stopping`. |
| `ork.pool.backlog.tasks`, `ork.pool.backlog.age` | `ork.pool` | Tasks waiting for an Orker of the pool, and how long the oldest has waited. Every engine reads them, so take `max` by `ork.pool`. Scale a pool on these. |
| `ork.pool.pollers` | `ork.pool` | Orkers that asked the pool for work recently. A busy, cordoned or stopping Orker asks for none, so 0 beside a backlog means add Orkers. |

Prometheus shows the dotted names with underscores, and counters with a `_total` suffix. For example, `ork.runs.refused` appears as `ork_runs_refused_total`.

Metrics are at least once. A retry can record a transition twice. Use `rate()` or `increase()` over a window, not a raw total.

## Send one namespace elsewhere

Declare a second destination under `telemetry.sinks` in the configuration file. Then choose it for one namespace:

```bash
orkctl namespace update payments --telemetry-sink tenant-collector
```

## Limits

- Exporters that cannot reach their collector log the failure and count it. They do not block the process.
- High-cardinality labels, such as run and task ids, are off by default. Turn them on with `ORK_TELEMETRY_HIGH_CARDINALITY=true` only if you accept the label cost.
- A duration can arrive late, because it comes from the run's own timestamps.

## See also

- [DEPLOY.md, “7. Telemetry”](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#7-telemetry): the admin listener, exporters and recipes
- [DEPLOY.md, “Traces”](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#traces): where a trace starts, every span's attributes, and when a span is missing
- [DEPLOY.md, “Lifecycle metrics”](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#lifecycle-metrics): every run and task metric, with its labels
- [DEPLOY.md, “Declaring an Orker's capacity”](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#declaring-an-orkers-capacity): the capacity metrics
- [deploy/observability/README.md](https://github.com/orkstack/orkstack/blob/main/deploy/observability/README.md): the local observability stack and its example queries
- Settings: [Configuration](/docs/configuration/)
