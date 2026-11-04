# Telemetry settings

These settings cover process logs, the admin listener that serves health and metrics, and the OpenTelemetry exporters. Everything is off by default: a deployment that sets none of them exports nothing and opens no listener.

## Decide these first

| Setting | Why a deployment must look at it |
|---|---|
| [`telemetry.admin_addr`](/docs/configuration/#telemetry-admin-addr) | Opens the admin listener for `/healthz`, `/readyz` and `/metrics`. Keep it on a private address. It is never the API port. |
| [`telemetry.admin_allowed_hosts`](/docs/configuration/#telemetry-admin-allowed-hosts) | Lists the names with a dot that a scraper uses to reach the admin listener. Any other `Host` is refused with `403`. |
| [`telemetry.exporters.metrics`](/docs/configuration/#telemetry-exporters-metrics) | Selects `prometheus`, `otlp` or `none`. `prometheus` requires `ORK_ADMIN_ADDR`. |
| [`telemetry.exporters.logs`](/docs/configuration/#telemetry-exporters-logs) | Sends the process's own log lines to an OTLP collector when set to `otlp`. |
| [`telemetry.high_cardinality`](/docs/configuration/#telemetry-high-cardinality) | Adds run and task labels to metrics. Leave it off unless you accept the label cost. |

Telemetry never fails a task or a run. An exporter that cannot reach its collector is logged and counted, and a record the exporter cannot take is dropped.

## Every telemetry setting

<!-- site-generated: settings-table-telemetry -->

## See also

- [DEPLOY.md, "7. Telemetry"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#7-telemetry): what is exported, and when
- [DEPLOY.md, "Recipes"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#recipes): collector configurations
- [DEPLOY.md, "Named sinks"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#named-sinks): sending a namespace's telemetry to a named destination
- [DEPLOY.md, "Per-namespace policy"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#per-namespace-policy): choosing a sink per namespace
- [DEPLOY.md, "High-cardinality labels"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#high-cardinality-labels): the label cost
- [DEPLOY.md, "Task logs"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#task-logs): exporting task output as log records
- [PRODUCTION_CHECKLIST.md, "Observability"](https://github.com/orkstack/orkstack/blob/main/docs/PRODUCTION_CHECKLIST.md#observability): the items to check
- Setting details: [Setting details](/docs/configuration/details/)
- All settings: [All settings](/docs/configuration/)
