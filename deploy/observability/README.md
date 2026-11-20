# Observability profile

An optional compose profile — an OpenTelemetry Collector, Loki, Tempo,
Prometheus and Grafana — for looking at `ork` and `orker`'s OpenTelemetry
output on a laptop. It never starts on its own: plain `docker compose up`,
`make infra-up` and `make up` are unaffected. [docs/DEPLOY.md](../../docs/DEPLOY.md),
"Telemetry", explains every setting used below.

## Bring it up

```bash
make up-observability
```

That is `make up` with this profile: `ork` and `orker` push their logs,
traces and metrics to the Collector, and Grafana opens at
http://localhost:3000. `TASK_LOGS=1 make up-observability` exports task logs
too. `make down` stops what it started, the profile included.

For the profile alone, with `ork` and `orker` run by hand:

```bash
make infra-up-observability
# or: docker compose -f deploy/docker-compose.yml --profile observability up -d --wait
```

This also starts Temporal, SeaweedFS and the registry, as `make infra-up` does.
It returns once those three pass their health checks and the profile's services,
which have none, are running. `make infra-down-observability` stops everything, including the profile.

## Point `ork` and `orker` at it

Save this as `orkstack.yaml` and pass it to both binaries with
`--config orkstack.yaml`:

```yaml
telemetry:
  log_format: json
  exporters:
    logs: otlp
    metrics: otlp
    traces: otlp        # spans: ork.task.attempt, ork.task.phase, and the engine's own
  otlp:
    endpoint: http://localhost:4318
    protocol: http/protobuf
orker:
  task_log_export: true   # ship task stdout/stderr as log records too
```

`service_name` defaults to `ork` / `orker` per binary, which is what this
profile expects. Every key has an `ORK_*`/`OTEL_*` environment equivalent
(`docs/DEPLOY.md`, "Configuration"); the environment overrides the file.

Every signal is pushed: the Collector writes metrics to Prometheus through
its OTLP receiver, and Prometheus scrapes nothing (`prometheus.yml`), so no
container needs a route to the host and the binaries need no admin listener.
An admin listener (`telemetry.admin_addr`) still serves `/metrics` to a
scraper of your own; the profile does not read it, and
`make up-observability` opens one only when `ORK_ADMIN_ADDR` is set.

## Where things are

The profile publishes two ports, both on `127.0.0.1` only: the Collector's
OTLP/http and Grafana, whose anonymous visitor is a Viewer that can read every
signal and change nothing. Loki, Tempo and Prometheus are reached through
Grafana. `make infra-up-observability` also publishes them on loopback
(`compose.backends.yaml`), because the observability proof gate reads them
directly; add `-f deploy/observability/compose.backends.yaml` to a compose
command for the same.

| what | where |
|---|---|
| Grafana | http://localhost:3000 (anonymous Viewer, local use only) |
| Collector OTLP/http | http://localhost:4318 |
| Prometheus | http://localhost:9090 (`make infra-up-observability` only) |
| Loki | http://localhost:3100 (`make infra-up-observability` only) |
| Tempo | http://localhost:3200 (`make infra-up-observability` only) |

The Collector refuses data before it runs out of memory (`memory_limiter`),
queues and retries what a backend does not take at once, and pushes its own
metrics (`otelcol_*`: accepted, refused, queued, failed) straight to
Prometheus. Its receiver has no auth, which only a loopback-only port makes
acceptable.

### Before production

This profile is for one machine. A production Collector needs bearer or mTLS
auth on its receiver and a network that activity containers cannot reach, and
Grafana needs real sign-in. The kit receives only what is pushed to it, so
Temporal's own server metrics, which Temporal offers only for scraping, are
not in it: point your own Prometheus at the Temporal server for them.

## Dashboards

Grafana provisions two dashboards, read-only, in its OrkStack folder:

- **OrkStack** (`/d/orkstack`): a top row that always loads (runs per
  minute, failure ratio, open runs, pollers and Orkers, tasks waiting, the
  oldest waiting task, Temporal and object storage, telemetry age), then
  collapsed rows, each named by the question it answers: runs and tasks,
  Orkers and pools, the cache, schedules, webhooks, HTTP triggers,
  concurrency limits, the engine with Temporal and object storage, and
  retention cleanup with the artifact GC. Its variables are the namespace, the Orkflow and the
  pool; the Orkflow variable lists names only while `telemetry.orkflow_label`
  is on (the default). Open runs, pollers and tasks waiting come from the
  engine's censuses, whose first pass waits a random part of its interval
  (up to a minute for open runs), so after an engine starts those tiles stay
  empty until it has run.
- **OrkStack / Run** (`/d/orkstack-run?var-run=<run-id>`): one run's spans,
  the engine's events, its attempts' usage with each task's memory against
  its limit, its tasks' logs and its webhook deliveries, with a link to the
  run's page in the OrkStack UI.

They are generated from `internal/dashboards` with Grafana's own builder;
change them there and run `go run ./internal/dashboards/cmd/gendashboards`,
never by hand or from the UI. `$namespace` filters the view and isolates
nothing: one tenant's data apart from another's needs a sink of its own.

## Alerts

The profile's Prometheus loads OrkStack's alert and recording rules from
`prometheus/rules/` (`rule_files:` in `prometheus.yml`), so its Alerts page
shows what fires. The profile runs no Alertmanager: to be paged, load the same
files into your own Prometheus and route them, the Watchdog to a dead man's
switch. Every alert's runbook is in [docs/RUNBOOKS.md](../../docs/RUNBOOKS.md).
The files are generated (`make alerts`) and checked by promtool at this
profile's pinned Prometheus (`make alerts-check`).

## Example queries

Loki, one Orker's task-log lines for one run (LogQL):

```
{service_name="orker"} | ork_run_id="<run-id>"
```

Prometheus, p95 activity latency by activity type (PromQL):

```
histogram_quantile(0.95, sum(rate(temporal_activity_execution_latency_bucket[5m])) by (le, activity_type))
```

## Find the trace from a log line

Every process log line carries `trace_id` when a span is live on its context,
including the engine's structured "run event" line. In Grafana, read the
`trace_id` of a Loki line (the query above, or search by `run_id`), then paste
it into the Tempo data source's "Search by trace ID", or open the line and
follow its Trace link. The trace shows the
run's own span with every `ork.task.attempt` and `ork.task.phase` span it
started, across however many Orkers ran its tasks. From a span, its logs and
its namespace's metrics are one link away (`grafana/datasources.yaml`).

## Named sinks

To route one namespace's signals through a named sink
(`ORK_TELEMETRY_SINKS`, or `telemetry.sinks:` in the file above):

```yaml
sinks:
  laptop-collector:
    endpoint: http://localhost:4318
```

```bash
orkctl namespace update payments --telemetry-sink laptop-collector
```

It points at the same Collector only because a laptop has one; in a real
deployment a named sink is a genuinely different destination. See
`docs/DEPLOY.md`, "Named sinks".

## Evidence

`make proof-observability` (with this profile up) starts `ork` and `orker`
with telemetry on, runs `examples/each-fan-out.ork.yaml`, and queries Loki, Tempo,
Prometheus and Grafana back to assert the run's signals arrived
(`scripts/proof-observability.sh`). Every full and release proof runs the
same gate (AGENTS.md §11.2). It also reads the two provisioned dashboards
through Grafana and holds their top row to an answer, asking for a
census-fed tile only once that census reports its first pass
(`ork_census_last_success_seconds` above 0). The engine's API listens
on a free port, but the gate checks the two admin listeners on fixed ports
(9464, 9465), so it holds a host-wide lock (`$XDG_RUNTIME_DIR/orkstack-observability.lock`)
and two proofs never run it at once.
