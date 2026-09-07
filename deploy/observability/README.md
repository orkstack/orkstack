# Observability profile

An optional compose profile — a Collector, Loki, Tempo, Prometheus and
Grafana — for looking at `ork` and `orker`'s OpenTelemetry output on a laptop.
It never starts on its own: plain `docker compose up` / `make infra-up` /
`make up` are unaffected. See `docs/DEPLOY.md`, "Telemetry", for what each
variable below does and for the recipes this profile follows.

## Bring it up

```bash
make infra-up-observability
# or: docker compose -f deploy/docker-compose.yml --profile observability up -d
```

This also starts Temporal and MinIO, same as `make infra-up`.

`make infra-down-observability` stops everything, including the profile.

## Point `ork` and `orker` at it

Save this as `orkstack.yaml` and pass it to both binaries with
`--config orkstack.yaml` (`make run-ork` / `make run-orker`, or your own
process manager). One file is enough for both — `admin_addr` is written as
`{ork:, orker:}` because that is the one setting the two binaries must not
share (docs/DEPLOY.md §1):

```yaml
telemetry:
  log_format: json
  admin_addr: {ork: ":9464", orker: ":9465"}
  exporters:
    logs: otlp
    metrics: [prometheus, otlp]
    traces: otlp        # spans: ork.task.attempt, ork.task.phase, and the engine's own
  otlp:
    endpoint: http://localhost:4318
    protocol: http/protobuf
orker:
  task_log_export: true   # ship task stdout/stderr as log records too
```

`service_name` needs no entry: it defaults to `ork` / `orker` per binary,
which is exactly what this profile wants.

If you drive the stack with `make up`, this is already done for you: it
writes `.run/orkstack.yaml` from the values it computes and starts both
binaries with `--config .run/orkstack.yaml` — check that file to see the
resolved config, or edit it directly for a one-off run.

### or, with environment variables

The same profile, exported instead of written to a file (`scripts/stack.sh`
does not currently let `make up` give the two binaries different
`ORK_ADMIN_ADDR` values, so drive them separately for this):

```bash
# ork
export ORK_LOG_FORMAT=json
export OTEL_SERVICE_NAME=ork
export OTEL_LOGS_EXPORTER=otlp
export OTEL_METRICS_EXPORTER=prometheus,otlp
export OTEL_TRACES_EXPORTER=otlp        # spans: ork.task.attempt, ork.task.phase, and the engine's own
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export ORK_ADMIN_ADDR=:9464

# orker (separate shell/process — note the different admin port and service name)
export ORK_LOG_FORMAT=json
export OTEL_SERVICE_NAME=orker
export OTEL_LOGS_EXPORTER=otlp
export OTEL_METRICS_EXPORTER=prometheus,otlp
export OTEL_TRACES_EXPORTER=otlp
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export ORK_ADMIN_ADDR=:9465
export ORK_ORKER_TASK_LOG_EXPORT=true   # ship task stdout/stderr as log records too
```

Prometheus scrapes `ORK_ADMIN_ADDR` directly (`deploy/observability/prometheus.yml`),
so `prometheus,otlp` above is belt-and-suspenders: drop `prometheus` from
`OTEL_METRICS_EXPORTER` if you only want the scrape path and don't need
`ORK_ADMIN_ADDR` served at all — except that a `/metrics` listener costs
nothing you weren't already paying for once `ORK_ADMIN_ADDR` is set for
`/healthz`/`/readyz`.

## Where things are

| what | where |
|---|---|
| Grafana | http://localhost:3000 (anonymous admin — local use only) |
| Prometheus | http://localhost:9090 |
| Loki | http://localhost:3100 |
| Tempo | http://localhost:3200 |
| Collector OTLP/http | http://localhost:4318 |

## Example queries

Loki, one Orker's task-log lines for one run (LogQL):

```
{service_name="orker"} | ork_run_id="<run-id>"
```

Prometheus, task activity latency by outcome (PromQL):

```
histogram_quantile(0.95, sum(rate(temporal_activity_execution_latency_bucket[5m])) by (le, activity_type))
```

## Find the trace from a log line

Every process log line carries `trace_id` when one is live on the context it
was logged from, including the engine's structured "run event" line. In
Grafana:

1. Open the Loki log line in question (the query above, or search by
   `run_id`) and read its `trace_id` field.
2. Switch to the Tempo data source and paste that `trace_id` into "Search by
   trace ID". (This profile's Loki data source, `grafana/datasources.yaml`,
   provisions no derived field for this yet — a one-click log→trace link is a
   `jsonData.derivedFields` entry away, pointed at the Tempo data source, for
   whoever wants to add it.)
3. The trace shows the run's own span alongside every `ork.task.attempt` and
   `ork.task.phase` span it started, across however many Orkers ran its tasks.

## Named sinks

To try `ORK_TELEMETRY_SINKS` against this same stack — routing one namespace's
signals through the Collector this profile already runs, distinctly from the
process default:

```yaml
sinks:
  default:
    endpoint: http://localhost:4318
  laptop-collector:
    endpoint: http://localhost:4318
```

```bash
orkctl namespace update payments --telemetry-sink laptop-collector
```

Both entries point at the same Collector here only because there is one
Collector on a laptop; against a real deployment `laptop-collector` (or
`datadog`, or whatever name is chosen) would name a second, genuinely
different destination — see `docs/DEPLOY.md`, "Named sinks".

## Proof

`make proof-observability` (with this profile already up) is the evidence
gate for all of the above: it starts `ork` and `orker` with telemetry on,
runs `examples/hello.ork.yaml` for real, then queries Loki, Tempo and
Prometheus back and asserts what it finds, rather than trusting that a
dashboard merely looks populated — see `scripts/proof-observability.sh` and
AGENTS.md §11.2.


