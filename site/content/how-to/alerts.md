# Alert on a deployment

Load OrkStack's Prometheus alert rules, route them, and follow each alert's runbook. The rules are optional: a deployment runs the same without them.

## Example

Copy the rule files into your Prometheus and load them:

```yaml
rule_files:
  - /etc/prometheus/rules/orkstack-*.yml
```

The files are in the repository, under `deploy/observability/prometheus/rules/`: one `orkstack-alerts-<area>.yml` per area, plus `orkstack-records.yml`, the recording rules the alerts read. Every alert's `runbook_url` opens its entry in the runbooks.

## What the rules expect

- Metrics reach Prometheus over OTLP through an OpenTelemetry Collector, with `ork.component`, `ork.deployment` and `ork.build_id` promoted to labels. Without them the rules still work for one deployment, but cannot tell deployments, or engines from Orkers, apart.
- The Collector's own metrics reach the same Prometheus, so a stopped Collector raises an alert of its own.
- In production, the Collector's receiver checks who sends. Anything that can push to it can fake the series an alert reads.

## Route them

| Label | Values |
|---|---|
| `severity` | `critical`, `warning`, `info`, and `none` for the Watchdog |
| `component` | the area: `telemetry`, `pools`, `runs`, `archive`, `storage`, `api`, `temporal`, `schedules`, `webhooks`, `triggers`, `cleanup`, `gc` |

Every alert's name starts with `OrkStack`. Send `OrkStackWatchdog` to a dead man's switch: it always fires, so when it stops arriving, Prometheus or Alertmanager is down.

## When one fires

Open its runbook. Each entry says what the alert means, what it costs, how to confirm it with `orkctl` without any dashboard, and what to do. An alert that resolves has not necessarily recovered: some read gauges that stop when their process stops. Run the confirm step.

## Limits

- Rules on per-namespace series see only the namespaces whose telemetry reaches this Prometheus. The deployment-wide alerts read series that every namespace reports.
- The rules are checked against the metric names the code emits and against promtool, at the Prometheus version the local profile pins.

## See also

- [RUNBOOKS.md, "Using the rules"](https://github.com/orkstack/orkstack/blob/main/docs/RUNBOOKS.md#using-the-rules): files, labels and annotations
- [RUNBOOKS.md, "Alerts"](https://github.com/orkstack/orkstack/blob/main/docs/RUNBOOKS.md#alerts): every alert's runbook
- [DEPLOY.md, "7. Telemetry"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#7-telemetry): the metrics the rules read
