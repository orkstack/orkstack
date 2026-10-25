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

To try this on a laptop first, start a local Collector, Loki, Tempo, Prometheus and Grafana with `make infra-up-observability`. Grafana is then at `http://localhost:3000`.

## What each kind of telemetry gives you

| Signal | Turn it on with | What you get |
|---|---|---|
| Metrics | `OTEL_METRICS_EXPORTER=otlp`, or `prometheus` | Counts and durations of runs, tasks and refusals. |
| Traces | `OTEL_TRACES_EXPORTER=otlp` | One trace per run, across `ork` and every Orker that ran its tasks. |
| Logs | `OTEL_LOGS_EXPORTER=otlp` | Process logs. |
| Task output | `ORK_ORKER_TASK_LOG_EXPORT=true` on each Orker, with logs on | Each line of task stdout and stderr, as a log record. |
| Process log format | `ORK_LOG_FORMAT=json` | JSON lines. The default is text. |

With `OTEL_METRICS_EXPORTER=prometheus`, set `ORK_ADMIN_ADDR` and let Prometheus scrape `/metrics` on it.

## The admin listener

`ORK_ADMIN_ADDR` opens a listener for `/healthz`, `/readyz`, `/metrics` and, when `ORK_PPROF_ENABLED=true`, `/debug/pprof/`. It is never the API port. Bind it to a private address. Leave `ORK_PPROF_ENABLED` off unless you are profiling. An Orker's memory holds resolved secret values.

## Metrics to alert on

| Metric | Labels | Counts |
|---|---|---|
| `ork.runs` | `status` | Each run, once, when it reaches a final state. |
| `ork.run.duration` | `status` | Time from start to finish of each run. |
| `ork.tasks` | `ork.pool`, `status`, `failure_category` | Each task, once, when it reaches a final state. |
| `ork.task.duration` | `ork.pool`, `status` | Time from dispatch to finish of each task. |
| `ork.orker.tasks` | `outcome`, `ork.failure.reason` | Each attempt, on the Orker that ran it. |
| `ork.runs.refused` | `ork.namespace`, `ork.refusal.reason` | Submissions refused by a hold, a cap or a budget. |
| `ork.audit.records` | `action`, `outcome` | Control-plane writes. A rising `dropped` rate means the audit store cannot be written. |

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
- [DEPLOY.md, “Lifecycle metrics”](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#lifecycle-metrics): every run and task metric, with its labels
- [deploy/observability/README.md](https://github.com/orkstack/orkstack/blob/main/deploy/observability/README.md): the local observability stack and its example queries
- Settings: [Configuration](/docs/reference/configuration/)
