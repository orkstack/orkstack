# Observability profile

An optional compose profile — an OpenTelemetry Collector, Loki, Tempo,
Prometheus and Grafana — for looking at `ork` and `orker`'s OpenTelemetry
output on a laptop. It never starts on its own: plain `docker compose up`,
`make infra-up` and `make up` are unaffected. [docs/DEPLOY.md](../../docs/DEPLOY.md),
"Telemetry", explains every setting used below.

## Bring it up

```bash
make infra-up-observability
# or: docker compose -f deploy/docker-compose.yml --profile observability up -d --wait
```

This also starts Temporal, SeaweedFS and the registry, as `make infra-up` does.
It returns once those three pass their health checks and the profile's services,
which have none, are running. `make infra-down-observability` stops everything, including the profile.

## Point `ork` and `orker` at it

Save this as `orkstack.yaml` and pass it to both binaries with
`--config orkstack.yaml`. `admin_addr` is written per binary because it is the
one setting the two must not share:

```yaml
telemetry:
  log_format: json
  admin_addr:
    ork: ":9464"
    orker: ":9465"
  admin_allowed_hosts:
    - host.docker.internal   # the name Prometheus scrapes by (below)
  exporters:
    logs: otlp
    metrics:
      - prometheus
      - otlp
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

With `make up`, the binaries run from a generated `.run/orkstack.yaml`. When
any `OTEL_*` variable (or `ORK_ADMIN_ADDR`) is exported, that file gains an
`admin_addr:` with `ork: "127.0.0.1:9464"` and `orker: "127.0.0.1:9465"`;
export the exporter and OTLP variables above and the rest follows. An exported
`ORK_ADMIN_ADDR` applies to both binaries, so set admin ports through the file
instead.

Metrics reach Prometheus twice: the Collector pushes OTLP metrics to it, and
it scrapes each binary's `/metrics` on `host.docker.internal:9464`/`9465`
(`prometheus.yml`). Either path alone is enough. The admin listeners answer
only names they know in a request's `Host`, and `host.docker.internal` has a
dot, so the file above lists it in `admin_allowed_hosts:`, and `make up`
lists it in the generated file beside `admin_addr:`. Without it, every
scrape is refused `403`, and the binaries log `request refused: Host not
allowed` with `listener="admin listener"`.

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
Grafana needs real sign-in.

## Dashboards

Grafana provisions two dashboards, read-only, in its OrkStack folder:

- **OrkStack** (`/d/orkstack`): a top row that always loads (runs per
  minute, failure ratio, open runs, pollers and Orkers, tasks waiting, the
  oldest waiting task, Temporal and object storage, telemetry age), then
  collapsed rows, each named by the question it answers: runs and tasks,
  Orkers and pools, the cache, the engine with Temporal and object storage,
  and the artifact GC. Its variables are the namespace, the Orkflow and the
  pool; the Orkflow variable lists names only while `telemetry.orkflow_label`
  is on (the default).
- **OrkStack / Run** (`/d/orkstack-run?var-run=<run-id>`): one run's spans,
  the engine's events, its attempts' usage and its tasks' logs, with a link
  to the run's page in the OrkStack UI.

They are generated from `internal/dashboards` with Grafana's own builder;
change them there and run `go run ./internal/dashboards/cmd/gendashboards`,
never by hand or from the UI. `$namespace` filters the view and isolates
nothing: one tenant's data apart from another's needs a sink of its own.

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
with telemetry on, runs `examples/each-fan-out.ork.yaml`, and queries Loki, Tempo
and Prometheus back to assert the run's signals arrived
(`scripts/proof-observability.sh`). Every full and release proof runs the
same gate (AGENTS.md §11.2). The engine's API listens on a free port, but the two
admin ports are the fixed targets `prometheus.yml` scrapes (9464, 9465), so
the gate holds a host-wide lock (`$XDG_RUNTIME_DIR/orkstack-observability.lock`)
and two proofs never run it at once.
