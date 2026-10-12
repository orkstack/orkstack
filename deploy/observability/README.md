# Observability profile

An optional compose profile — an OpenTelemetry Collector, Loki, Tempo,
Prometheus and Grafana — for looking at `ork` and `orker`'s OpenTelemetry
output on a laptop. It never starts on its own: plain `docker compose up`,
`make infra-up` and `make up` are unaffected. [docs/DEPLOY.md](../../docs/DEPLOY.md),
"Telemetry", explains every setting used below.

## Bring it up

```bash
make infra-up-observability
# or: docker compose -f deploy/docker-compose.yml --profile observability up -d
```

This also starts Temporal, MinIO and the registry, as `make infra-up` does.
`make infra-down-observability` stops everything, including the profile.

## Point `ork` and `orker` at it

Save this as `orkstack.yaml` and pass it to both binaries with
`--config orkstack.yaml`. `admin_addr` is written per binary because it is the
one setting the two must not share:

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

`service_name` defaults to `ork` / `orker` per binary, which is what this
profile expects. Every key has an `ORK_*`/`OTEL_*` environment equivalent
(`docs/DEPLOY.md`, "Configuration"); the environment overrides the file.

With `make up`, the binaries run from a generated `.run/orkstack.yaml`. When
any `OTEL_*` variable (or `ORK_ADMIN_ADDR`) is exported, that file gains
`admin_addr: {ork: "127.0.0.1:9464", orker: "127.0.0.1:9465"}`; export the
exporter and OTLP variables above and the rest follows. An exported
`ORK_ADMIN_ADDR` applies to both binaries, so set admin ports through the file
instead.

Metrics reach Prometheus twice: the Collector pushes OTLP metrics to it, and
it scrapes each binary's `/metrics` on `host.docker.internal:9464`/`9465`
(`prometheus.yml`). Either path alone is enough.

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

Prometheus, p95 activity latency by activity type (PromQL):

```
histogram_quantile(0.95, sum(rate(temporal_activity_execution_latency_bucket[5m])) by (le, activity_type))
```

## Find the trace from a log line

Every process log line carries `trace_id` when a span is live on its context,
including the engine's structured "run event" line. In Grafana, read the
`trace_id` of a Loki line (the query above, or search by `run_id`), then paste
it into the Tempo data source's "Search by trace ID". The trace shows the
run's own span with every `ork.task.attempt` and `ork.task.phase` span it
started, across however many Orkers ran its tasks. (The provisioned Loki data
source, `grafana/datasources.yaml`, has no derived field for a one-click
log→trace link; a `jsonData.derivedFields` entry pointed at Tempo would add
one.)

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
with telemetry on, runs `examples/hello.ork.yaml`, and queries Loki, Tempo
and Prometheus back to assert the run's signals arrived
(`scripts/proof-observability.sh`). The full proof runs the same gate whenever
the tree changes a telemetry path, and a feature proof runs it when the change
does (AGENTS.md §11.2). The engine's API listens on a free port, but the two
admin ports are the fixed targets `prometheus.yml` scrapes (9464, 9465), so
the gate holds a host-wide lock (`$XDG_RUNTIME_DIR/orkstack-observability.lock`)
and two proofs never run it at once.
