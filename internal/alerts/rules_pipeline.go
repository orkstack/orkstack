package alerts

// The Collector's counters that say it lost data on the way.
var collectorLoss = []string{
	"otelcol_exporter_send_failed_metric_points_total", "otelcol_exporter_send_failed_log_records_total", "otelcol_exporter_send_failed_spans_total",
	"otelcol_exporter_enqueue_failed_metric_points_total", "otelcol_exporter_enqueue_failed_log_records_total", "otelcol_exporter_enqueue_failed_spans_total",
	"otelcol_receiver_refused_metric_points_total", "otelcol_receiver_refused_log_records_total", "otelcol_receiver_refused_spans_total",
}

func pipelineRules() []Rule {
	const g, c = "pipeline", "telemetry"
	up := Series{Series: "otelcol_process_uptime_seconds_total", Values: "0+60x90"}
	return []Rule{
		{
			Name: "Watchdog", Group: g, Component: c, Severity: None, For: "0m",
			Expr:        "vector(1)",
			Summary:     "Prometheus is evaluating OrkStack's alert rules.",
			Description: "This alert always fires. Route it to a dead man's switch: when it stops arriving, Prometheus or its Alertmanager is down.",
			Meaning:     "Prometheus is up and evaluating these rules; the alert is meant to fire for ever.",
			Impact:      "None while it fires. When it stops, no other alert can reach you.",
			Confirm:     "kit: the external dead man's switch shows the Watchdog arriving.",
			Fix:         "Nothing while it fires. When the switch reports it missing, restore Prometheus or its Alertmanager.",
			Cases:       []Case{{Name: "always firing", EvalAt: "1m", Input: []Series{up}, Fires: []map[string]string{{}}}},
		},
		{
			Name: "TelemetryPipelineDown", Group: g, Component: c, Severity: Critical, For: "2m",
			Expr:        "absent_over_time(otelcol_process_uptime_seconds_total[5m])",
			Summary:     "The OpenTelemetry Collector reports nothing.",
			Description: "No Collector has reported its own metrics for 5 minutes, so no OrkStack metric can arrive either, and every alert that reads one is blind.",
			Meaning:     "The Collector that receives OrkStack's OTLP is stopped, or its own metrics no longer reach Prometheus.",
			Impact:      "Every metric-based alert is blind. OrkStack itself keeps running: telemetry never blocks a task or a run.",
			Confirm:     "kit: the Collector's process and its logs. `orkctl admin info` still answers, which shows the engine is up.",
			Fix:         "Start the Collector again; check its memory limit and its exporter to Prometheus. Nothing in OrkStack needs a restart.",
			Related:     []string{"EngineAllDown", "TelemetryDropping"},
			Cases: []Case{
				{Name: "Collector stopped", EvalAt: "20m", Input: []Series{{Series: "otelcol_process_uptime_seconds_total", Values: "0+60x10"}},
					Fires: []map[string]string{{}}, Also: []Expect{{Rule: "EngineAllDown"}}},
				{Name: "Collector running", EvalAt: "20m", Input: []Series{up}},
			},
		},
		{
			Name: "TelemetryDropping", Group: g, Component: c, Severity: Warning, For: "10m",
			Expr:        anyRate("10m", collectorLoss...),
			Summary:     "The OpenTelemetry Collector is dropping telemetry.",
			Description: "The Collector has refused, failed to queue or failed to send metrics, logs or spans for 10 minutes.",
			Meaning:     "A backend refuses what the Collector sends (a rate limit such as Loki's 429, or colliding series names), or the Collector refuses what it receives (its memory limit).",
			Impact:      "Dashboards and alerts miss data; task logs exported through the Collector may be incomplete. Runs are unaffected.",
			Confirm:     "kit: `otelcol_exporter_send_failed_*`, `otelcol_exporter_enqueue_failed_*` and `otelcol_receiver_refused_*` series say which signal and which exporter.",
			Fix:         "Raise the refusing backend's limits or the Collector's queue and memory limit; for a name collision, find the two sources sending one name.",
			Related:     []string{"TelemetryPipelineDown", "TelemetryExportErrors"},
			Cases: []Case{
				fire0("Loki refuses logs", "30m", Series{Series: `otelcol_exporter_send_failed_log_records_total{exporter="otlphttp/loki"}`, Values: "0+10x40"}),
				fire0("two exporter metrics with identical labels", "30m",
					Series{Series: "otelcol_exporter_send_failed_spans_total", Values: "0+10x40"},
					Series{Series: "otelcol_exporter_send_failed_metric_points_total", Values: "0+10x40"}),
				quiet("nothing dropped", "30m", Series{Series: "otelcol_exporter_send_failed_log_records_total", Values: "5x40"}),
			},
		},
		{
			Name: "TelemetryExportErrors", Group: g, Component: c, Severity: Warning, For: "0m",
			Expr:        NEW("ork_telemetry_export_errors_total", "15m", "ork_telemetry_signal") + " or " + NEW(`ork_orker_task_log_exports_total{outcome="error"}`, "15m"),
			Summary:     "An OrkStack process failed to export telemetry.",
			Description: `An engine or Orker failed to export {{ $labels.ork_telemetry_signal }} to its collector in the last 15 minutes.`,
			Meaning:     "A process's OTLP exporter could not reach its collector, or the collector refused the export. The process logs it and counts it, and never retries in line.",
			Impact:      "The process's telemetry for that window is missing. Nothing it runs is affected.",
			Confirm:     "the engine or Orker log line `telemetry export failed`; kit: `ork_telemetry_export_errors_total` by signal.",
			Fix:         "Check the process's `OTEL_EXPORTER_OTLP_*` endpoint and credentials, and the Collector's receiver.",
			Related:     []string{"TelemetryPipelineDown", "TelemetryDropping"},
			Cases:       newCases("ork_telemetry_export_errors_total", `ork_telemetry_signal="metrics"`, out("ork_telemetry_signal", "metrics")),
		},
		{
			Name: "TelemetryRecordsDropped", Group: g, Component: c, Severity: Warning, For: "0m",
			Expr:        NEW(`ork_telemetry_records_dropped_total{reason!="pending"}`, "15m", "ork_telemetry_signal", "reason"),
			Summary:     "An engine dropped telemetry for a namespace.",
			Description: `An engine dropped {{ $labels.ork_telemetry_signal }} records for a namespace, for the reason "{{ $labels.reason }}".`,
			Meaning:     "A namespace's telemetry policy names a sink this process does not declare (`sink_unknown`), or its records could not be routed (`unrouted`, `lookup_failed`, `instruments_failed`). Such records are dropped and counted, never sent to the default sink. A namespace whose policy is still loading (`pending`) is not alerted.",
			Impact:      "That namespace's runs report nothing to its sink; the operator twins on the default sink still count them.",
			Confirm:     "`orkctl namespace get NS` shows the namespace's telemetry policy; compare its sink with the deployment's `ORK_TELEMETRY_SINKS`.",
			Fix:         "Declare the sink on every engine and Orker, or change the namespace's policy to a sink they declare.",
			Related:     []string{"NamespacesNotExported"},
			Cases: append(newCases("ork_telemetry_records_dropped_total", `ork_telemetry_signal="logs",reason="sink_unknown"`, out("ork_telemetry_signal", "logs", "reason", "sink_unknown")),
				quiet("a new namespace: pending", "30m", s("ork_telemetry_records_dropped_total", `ork_telemetry_signal="logs",reason="pending"`, "0+1x40"))),
		},
		{
			Name: "EngineAllDown", Group: g, Component: c, Severity: Critical, For: "2m",
			Expr: "(absent_over_time(" + ENG + "[5m]) or (count " + by() + " (max_over_time(" + ENG + "[1h])) unless " + on() +
				" count " + by() + " (max_over_time(" + ENG + "[5m])))) and on () (" + PipeOK + ")",
			Summary:     "No OrkStack engine reports metrics.",
			Description: "No engine of the deployment has reported metrics for 5 minutes, while the telemetry pipeline itself is up and exporting.",
			Meaning:     "Every engine process is down, or every one has lost its route to the Collector. The rule is quiet while the pipeline is down, which TelemetryPipelineDown reports instead.",
			Impact:      "If the engines are down, nothing can be submitted and nothing new is dispatched; running containers carry on and commit. If only their telemetry is cut, nothing is affected but what you can see.",
			Confirm:     "`orkctl admin info` answers when an engine is up: then the engines are healthy and only their telemetry is cut.",
			Fix:         "Start the engines, or restore their OTLP endpoint. An engine whose `orkctl admin info` answers is not down.",
			Related:     []string{"TelemetryPipelineDown", "TelemetryExportErrors"},
			Cases: []Case{
				{Name: "every engine stopped", EvalAt: "40m", Input: []Series{up, s("ork_build_info", "", "1x20"), s("ork_build_info", `ork_orker_id="o1"`, "1x90")},
					Fires: []map[string]string{{}, out()}},
				quiet("pipeline down", "40m", s("ork_build_info", "", "1x20")),
				quiet("exporter failing", "40m", up, Series{Series: "otelcol_exporter_send_failed_metric_points_total", Values: "0+10x90"}, s("ork_build_info", "", "1x20")),
				quiet("one late push", "23m", up, s("ork_build_info", "", "1x20 _x2 1x20")),
				{Name: "one of two deployments down", EvalAt: "40m", Input: []Series{up, s("ork_build_info", "", "1x20"),
					{Series: `ork_build_info{ork_deployment="e"}`, Values: "1x90"}}, Fires: []map[string]string{out()}},
			},
		},
		{
			Name: "MixedReleases", Group: g, Component: c, Severity: Info, For: "2h",
			Expr:        "count " + by() + " (count " + by("ork_build_id") + " (ork_build_info)) > 1",
			Summary:     "Processes of more than one release are running.",
			Description: "Engines and Orkers of more than one build have run side by side for 2 hours.",
			Meaning:     "A release rollout is in progress, or one was left half done.",
			Impact:      "None by itself: a run keeps the release it started on. A rollout left half done keeps old releases' processes running.",
			Confirm:     "`orkctl release status` shows the current, ramping and previous releases and whether each is drained.",
			Fix:         "Finish the rollout (`orkctl release promote`) or roll it back; stop a previous release's processes only once `orkctl release wait` reports it drained.",
			Cases: []Case{
				{Name: "two builds for 3 hours", Interval: "5m", EvalAt: "3h", Input: []Series{s("ork_build_info", `ork_build_id="a"`, "1x40"), s("ork_build_info", `ork_build_id="b"`, "1x40")}, Fires: []map[string]string{out()}},
				{Name: "one build", Interval: "5m", EvalAt: "3h", Input: []Series{s("ork_build_info", `ork_build_id="a"`, "1x40")}},
			},
		},
		{
			Name: "NamespacesNotExported", Group: g, Component: c, Severity: Info, For: "1h",
			Expr:        "max " + by() + ` (ork_telemetry_namespaces{routing="named_without_metrics"}) > 0`,
			Summary:     "Some namespaces export no metrics.",
			Description: "Some namespaces' telemetry policies name a sink that exports no metrics, so their run and task metrics reach no Prometheus.",
			Meaning:     "A namespace's telemetry policy selects a named sink with metrics off. Its runs are still counted by the operator twins on the default sink.",
			Impact:      "The per-namespace rules in this file are blind to those namespaces.",
			Confirm:     "`orkctl namespace get NS` shows each namespace's telemetry policy.",
			Fix:         "Nothing, if that was the intent. Otherwise give the sink metrics, or point the namespace at one that has them.",
			Related:     []string{"TelemetryRecordsDropped"},
			Cases: []Case{
				fire("two namespaces", "90m", out(), s("ork_telemetry_namespaces", `routing="named_without_metrics"`, "2x100")),
				quiet("none", "90m", s("ork_telemetry_namespaces", `routing="named_without_metrics"`, "0x100")),
			},
		},
		{
			Name: "MetricOverflow", Group: g, Component: c, Severity: Info, For: "1h",
			Expr:        "count " + by() + ` ({otel_metric_overflow="true", __name__=~"ork_.*"}) > 0`,
			Summary:     "An OrkStack metric hit its cardinality limit.",
			Description: "At least one OrkStack instrument has more series than the cardinality limit, so its excess series are folded into one overflow series.",
			Meaning:     "An instrument recorded more distinct label sets than `ORK_TELEMETRY_METRIC_CARDINALITY_LIMIT` allows, usually from high-cardinality labels turned on.",
			Impact:      "The folded series lose their labels; totals are still right.",
			Confirm:     "kit: `{otel_metric_overflow=\"true\"}` names the instrument.",
			Fix:         "Turn off `ORK_TELEMETRY_HIGH_CARDINALITY` or object labels, or raise the limit knowing the cost.",
			Cases: []Case{
				fire("an overflow series", "90m", out(), s("ork_tasks_total", `otel_metric_overflow="true"`, "0+1x100")),
				quiet("no overflow", "90m", s("ork_tasks_total", `ork_namespace="n"`, "0+1x100")),
			},
		},
		{
			Name: "CensusStale", Group: g, Component: c, Severity: Info, For: "15m",
			Expr:        "time() - max " + by("ork_census") + " (ork_census_last_success_seconds) > 600",
			Summary:     "An engine census has not succeeded for 10 minutes.",
			Description: `No engine's "{{ $labels.ork_census }}" census has succeeded for over 10 minutes, so the gauges it reports are stale.`,
			Meaning:     "A census reads the cluster's index (open runs, schedules, pool backlogs, pending webhook deliveries, telemetry policies) on a budget. It fails while Temporal's visibility is unavailable. Its 10 minutes are twice the longest census interval the settings allow (`ORK_POOL_METRICS_INTERVAL` is at most 5 minutes).",
			Impact:      "The gauges the census reports stop moving, and the alerts that read them (pool backlogs, schedules, webhook deliveries) are blind.",
			Confirm:     "the engine log names the failing census; kit: `ork_census_last_success_seconds` by `ork_census`.",
			Fix:         "Restore Temporal's visibility; see Temporal outages above. If it is skipped rather than failing, see CensusSkipped.",
			Related:     []string{"CensusSkipped", "#temporal-outages"},
			Cases: []Case{
				fire("never succeeded", "40m", out("ork_census", "runs_open"), s("ork_census_last_success_seconds", `ork_census="runs_open"`, "0x50")),
				quiet("succeeding", "40m", s("ork_census_last_success_seconds", `ork_census="runs_open"`, "0+60x50")),
			},
		},
		{
			Name: "CensusSkipped", Group: g, Component: c, Severity: Info, For: "30m",
			Expr:        "sum " + by("ork_census") + " (increase(ork_census_skipped_total[30m])) > 0",
			Summary:     "An engine census is skipping passes.",
			Description: `The "{{ $labels.ork_census }}" census skipped passes for 30 minutes because its request budget was spent.`,
			Meaning:     "The censuses share a budget of requests per minute to the cluster (`ORK_TELEMETRY_CENSUS_REQUESTS_PER_MINUTE`); a large deployment can spend it.",
			Impact:      "The census's gauges update less often.",
			Confirm:     "kit: `ork_census_skipped_total` by `ork_census`.",
			Fix:         "Raise the census budget if the cluster can take it, or accept slower gauges.",
			Related:     []string{"CensusStale"},
			Cases: []Case{
				fire("skipping", "80m", out("ork_census", "schedules"), s("ork_census_skipped_total", `ork_census="schedules"`, "0+1x90")),
				quiet("not skipping", "80m", s("ork_census_skipped_total", `ork_census="schedules"`, "5x90")),
			},
		},
	}
}

// fire0 is a firing case whose alert carries no labels of its own.
func fire0(name, evalAt string, in ...Series) Case {
	return Case{Name: name, EvalAt: evalAt, Input: in, Fires: []map[string]string{{}}}
}
