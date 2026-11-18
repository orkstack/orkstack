package dashboards

import "strings"

// The OrkStack dashboard (uid orkstack): a top row that always loads, then
// collapsed area rows, each named by the question it answers (prod-kit
// design §2.3). Rows whose metrics are not yet exported (schedules,
// webhooks, HTTP and event triggers, concurrency limits, resumes, retention
// cleanup) are added with the change that exports them.

// sel selects runs by the shared variables. When runs can be resumed and a
// resumed run counts again (ork.run.resumed), sel gains
// ork_run_resumed!="true"; TestDashboards_RunCountsExcludeResumed requires it
// the moment the label is exported, and refuses it before.
const sel = `ork_namespace=~"$namespace",ork_orkflow_name=~"$orkflow"`

// pools selects by the pool variable.
const pools = `ork_pool=~"$pool"`

// with puts a selector inside a metric's braces: with("ork_runs_total",
// `status="failed"`, sel).
func with(metric string, matchers ...string) string {
	return metric + "{" + strings.Join(matchers, ",") + "}"
}

// runViewLink opens one run's dashboard from a log line's run_id field.
var runViewLink = link{field: "run_id", title: "Open the run", url: "/d/" + UIDRun + "?var-run=${__value.raw}"}

func topRow() []panel {
	runs := with("ork_runs_total", sel)
	failed := with("ork_runs_total", `status="failed"`, sel)
	backlog := `max by (ork_pool) (ork_pool_backlog_tasks{` + pools + `})`
	polling := `max by (ork_pool) (ork_pool_pollers{` + pools + `})`
	return []panel{
		stat("Runs / min", "Runs that finished, per minute, over the last 5 minutes.", unitShort,
			qi(`sum(rate(`+runs+`[5m])) * 60 or vector(0)`, "runs")).withThresholds(neutral()...),
		stat("Failure ratio, 15 m", "The share of runs that finished in the last 15 minutes and failed.", unitPercent,
			qi(`(sum(rate(`+failed+`[15m])) or vector(0)) / (sum(rate(`+runs+`[15m])) > 0)`, "failed")).
			withThresholds(okWarnFail(0.05, 0.2)...).withNoValue("no runs"),
		stat("Open runs", "Runs open in the deployment now, each counted once.", unitShort,
			qi(`max(ork_runs_open)`, "open")).withThresholds(neutral()...).withNoValue("not measured"),
		stat("Pollers", "Orkers that asked a pool for work in the last 90 seconds. A busy, cordoned or stopping Orker asks for none.", unitShort,
			qi(`sum(`+polling+`)`, "pollers")).withThresholds(neutral()...).withNoValue("not measured"),
		stat("Orkers", "Orkers exporting metrics, each counted once. It can lag up to 5 minutes behind one that stopped.", unitShort,
			qi(`count(count by (ork_orker_id) (ork_orker_present{`+pools+`})) or vector(0)`, "orkers")).withThresholds(neutral()...),
		stat("Tasks waiting", "Tasks waiting for an Orker, across the selected pools.", unitShort,
			qi(`sum(`+backlog+`)`, "waiting")).withThresholds(okWarnFail(1, 100)...).withNoValue("not measured"),
		stat("Oldest waiting task", "How long the longest-waiting task of the selected pools has waited.", unitSeconds,
			qi(`max(ork_pool_backlog_age_seconds{`+pools+`})`, "oldest")).withThresholds(okWarnFail(60, 900)...).withNoValue("not measured"),
		stat("Pools with work and nobody polling", "Pools with tasks waiting and no Orker asking for work: add Orkers, or uncordon them.", unitShort,
			qi(`count((`+backlog+` > 0) unless on (ork_pool) (`+polling+` > 0)) or (sum(`+backlog+`) * 0)`, "pools")).withThresholds(zeroOKElseFail()...).withNoValue("not measured"),
		stat("Temporal reachable", "The share of the engines' and Orkers' requests to Temporal that succeeded over 5 minutes.", unitPercent,
			qi(`1 - (sum(rate(temporal_request_failure_total[5m])) or vector(0)) / (sum(rate(temporal_request_total[5m])) > 0)`, "reachable")).
			withThresholds([]threshold{{0, colorFail}, {0.95, colorWait}, {0.99, colorOK}}...).withNoValue("no requests"),
		stat("Object store outage", "1 while any Orker has stopped taking work because object storage does not answer.", unitBool,
			qi(`max(ork_orker_store_outage_active)`, "outage")).withThresholds(zeroOKElseFail()...).withNoValue("no Orker reports"),
		stat("Finished runs not archived", "Finished runs the archive sweep could not confirm archived. They are lost if object storage stays down until the cluster deletes them.", unitShort,
			qi(`max(ork_archive_sweep_unconfirmed)`, "at risk")).withThresholds(zeroOKElseFail()...).withNoValue("not measured"),
		stat("Firing alerts", "OrkStack alerts firing now, by severity. The watchdog, which always fires, is left out.", unitShort,
			qi(`count by (severity) (ALERTS{alertstate="firing",alertname=~"OrkStack.*",alertname!="OrkStackWatchdog"})`, "{{severity}}")).
			withThresholds(zeroOKElseFail()...).withNoValue("none firing, or no alert rules loaded"),
		stat("Telemetry age", "Seconds since the newest build-info point arrived. Growing means the telemetry pipeline has stopped.", unitSeconds,
			qi(`time() - max(timestamp(ork_build_info))`, "age")).withThresholds(okWarnFail(60, 300)...).withNoValue("nothing arrives"),
		stat("Namespaces not exported here", "Namespaces whose records go to a sink of their own that exports no metrics; their runs are counted only in the deployment-wide figures.", unitShort,
			qi(`max(ork_telemetry_namespaces{routing="named_without_metrics"}) or vector(0)`, "namespaces")).withThresholds(neutral()...),
	}
}

func areaRows() []row {
	return []row{
		{title: "Are runs and tasks succeeding?", panels: []panel{
			series("Runs by status", "Runs that finished, per second, by status.", unitOps,
				q(`sum by (status) (rate(`+with("ork_runs_total", sel)+`[$__rate_interval]))`, "{{status}}")),
			series("Run duration", "Run duration from start to finish, at the 50th, 95th and 99th percentiles.", unitSeconds,
				q(`histogram_quantile(0.5, sum by (le) (rate(`+with("ork_run_duration_seconds_bucket", sel)+`[$__rate_interval])))`, "p50"),
				q(`histogram_quantile(0.95, sum by (le) (rate(`+with("ork_run_duration_seconds_bucket", sel)+`[$__rate_interval])))`, "p95"),
				q(`histogram_quantile(0.99, sum by (le) (rate(`+with("ork_run_duration_seconds_bucket", sel)+`[$__rate_interval])))`, "p99")),
			series("Attempts that failed, by reason", "Attempts that ended in failure on the Orkers, the ten most frequent reasons, across every namespace.", unitOps,
				q(`topk(10, sum by (ork_failure_reason) (rate(ork_deployment_orker_tasks_total{outcome="failed"}[$__rate_interval])))`, "{{ork_failure_reason}}")),
			series("Submissions refused", "Submissions the admission gate refused, by reason: a hold, a cap on running runs, or a budget.", unitOps,
				q(`sum by (ork_refusal_reason) (rate(ork_runs_refused_total{ork_namespace=~"$namespace"}[$__rate_interval]))`, "{{ork_refusal_reason}}")),
			series("Tasks by status and failure category", "Tasks that finished, per second, by status and, for a failure, its category.", unitOps,
				q(`sum by (status, failure_category) (rate(ork_tasks_total{ork_namespace=~"$namespace",`+pools+`}[$__rate_interval]))`, "{{status}} {{failure_category}}")),
			logs("Recent run failures", "The engine's run_failed lines, newest first. A namespace exported to a sink of its own shows its run id and namespace only.",
				q(`{service_name=~".*ork"} | event_type="run_failed"`, "")).sized(24, 8).withLinks(runViewLink),
			stat("Task log export", "Whether any Orker exports task logs; when none does, the Run view shows no task logs.", unitBool,
				qi(`max(ork_orker_task_log_export_enabled)`, "exporting")).withThresholds([]threshold{{0, colorWait}, {1, colorOK}}...).withNoValue("no Orker"),
		}},
		{title: "Are there enough Orkers for the work?", panels: []panel{
			series("Tasks waiting, pollers and Orkers, by pool", "Per pool: tasks waiting, Orkers asking for work, and Orkers exporting metrics.", unitShort,
				q(`max by (ork_pool) (ork_pool_backlog_tasks{`+pools+`})`, "{{ork_pool}} waiting"),
				q(`max by (ork_pool) (ork_pool_pollers{`+pools+`})`, "{{ork_pool}} pollers"),
				q(`count by (ork_pool) (ork_orker_present{`+pools+`})`, "{{ork_pool}} Orkers")).sized(24, 8),
			series("Oldest waiting task, by pool", "How long each pool's longest-waiting task has waited. One that never clears while Orkers poll may belong to a release whose Orkers were stopped.", unitSeconds,
				q(`max by (ork_pool) (ork_pool_backlog_age_seconds{`+pools+`})`, "{{ork_pool}}")),
			series("Tasks arriving and handed out, by pool", "The cluster's own recent rates for each pool.", unitOps,
				q(`max by (ork_pool) (ork_pool_tasks_add_rate_per_second{`+pools+`})`, "{{ork_pool}} arriving"),
				q(`max by (ork_pool) (ork_pool_tasks_dispatch_rate_per_second{`+pools+`})`, "{{ork_pool}} handed out")),
			series("Running against slots, by pool", "Tasks running now beside how many the pool's Orkers run at once.", unitShort,
				q(`sum by (ork_pool) (ork_orker_running_tasks{`+pools+`})`, "{{ork_pool}} running"),
				q(`sum by (ork_pool) (ork_orker_max_concurrent{`+pools+`})`, "{{ork_pool}} slots")),
			series("Cordoned Orkers, by cause", "Orkers that take no new work, by why: an operator, an object-store outage, or stopping.", unitShort,
				q(`sum by (ork_cordon_cause) (ork_orker_cordoned{`+pools+`})`, "{{ork_cordon_cause}}")),
			series("Wait before an Orker begins a task, p95", "Seconds from the cluster scheduling a task's first attempt to an Orker beginning it, at the 95th percentile.", unitSeconds,
				q(`histogram_quantile(0.95, sum by (le, ork_pool) (rate(ork_deployment_orker_task_wait_duration_seconds_bucket{`+pools+`}[$__rate_interval])))`, "{{ork_pool}}")),
			series("CPU reserved against capacity", "Per Orker that declares a capacity, the cpu it admits tasks within and what stands reserved; the 20 most reserved.", unitShort,
				q(`topk(20, sum by (ork_orker_id) (ork_orker_capacity_reserved_cpu))`, "{{ork_orker_id}} reserved"),
				q(`topk(20, sum by (ork_orker_id) (ork_orker_capacity_cpu))`, "{{ork_orker_id}} capacity")),
			series("Open pins, by pool", "Pins held open now: groups of tasks sharing one Orker.", unitShort,
				q(`sum by (ork_pool) (ork_orker_open_pins{`+pools+`})`, "{{ork_pool}}")),
		}},
		{title: "Is the cache saving work?", panels: []panel{
			series("Memo hit ratio, by namespace", "Of cached tasks' memo lookups that were not bypassed, the share that hit.", unitPercent,
				q(`sum by (ork_namespace) (rate(ork_orker_memo_lookups_total{ork_cache_outcome="hit",ork_namespace=~"$namespace"}[$__rate_interval])) / sum by (ork_namespace) (rate(ork_orker_memo_lookups_total{ork_cache_outcome!="bypassed",ork_namespace=~"$namespace"}[$__rate_interval]))`, "{{ork_namespace}}")),
			series("Lookups by outcome and miss reason", "Cached tasks' memo lookups, by outcome and, for a miss, why.", unitOps,
				q(`sum by (ork_cache_outcome, ork_cache_miss_reason) (rate(ork_orker_memo_lookups_total{ork_namespace=~"$namespace"}[$__rate_interval]))`, "{{ork_cache_outcome}} {{ork_cache_miss_reason}}")),
			series("Memo step duration, p95", "How long the memo step took, at the 95th percentile.", unitSeconds,
				q(`histogram_quantile(0.95, sum by (le) (rate(ork_orker_task_phase_duration_seconds_bucket{ork_phase="memo"}[$__rate_interval])))`, "p95")),
			series("Memo writes by outcome", "Memo writes after a miss; a failed one costs only the next run's hit.", unitOps,
				q(`sum by (outcome) (rate(ork_orker_memo_writes_total{ork_namespace=~"$namespace"}[$__rate_interval]))`, "{{outcome}}")),
		}},
		{title: "Is the engine, Temporal and object storage healthy?", panels: []panel{
			series("API requests by code", "Requests to the engine's API, per second, by result code.", unitOps,
				q(`sum by (rpc_code) (rate(rpc_server_call_duration_seconds_count[$__rate_interval]))`, "{{rpc_code}}")),
			series("API latency", "API request duration at the 95th and 99th percentiles.", unitSeconds,
				q(`histogram_quantile(0.95, sum by (le) (rate(rpc_server_call_duration_seconds_bucket[$__rate_interval])))`, "p95"),
				q(`histogram_quantile(0.99, sum by (le) (rate(rpc_server_call_duration_seconds_bucket[$__rate_interval])))`, "p99")),
			series("Live streams", "Run event streams the engines hold open, by kind.", unitShort,
				q(`sum by (ork_stream_kind) (ork_api_streams)`, "{{ork_stream_kind}}")),
			series("Requests to Temporal", "Requests the engines and Orkers made to Temporal, and those that failed.", unitOps,
				q(`sum(rate(temporal_request_total[$__rate_interval]))`, "requests"),
				q(`sum(rate(temporal_request_failure_total[$__rate_interval]))`, "failed")),
			series("Long runs carrying on", "Runs carried forward into a fresh history, by outcome.", unitOps,
				q(`sum by (outcome) (rate(ork_run_continuations_total{ork_namespace=~"$namespace"}[$__rate_interval]))`, "{{outcome}}")),
			series("Saved state written, p95", "The size of what a run's carrying-on wrote, at the 95th percentile.", unitBytes,
				q(`histogram_quantile(0.95, sum by (le) (rate(ork_run_continuation_state_bytes_bucket[$__rate_interval])))`, "p95")),
			series("Object store requests by operation", "Requests the engines and Orkers made to object storage, by operation and outcome.", unitOps,
				q(`sum by (ork_store_operation, outcome) (rate(ork_store_requests_total[$__rate_interval]))`, "{{ork_store_operation}} {{outcome}}")),
			series("Object store latency, p95", "Object store request duration at the 95th percentile, by operation.", unitSeconds,
				q(`histogram_quantile(0.95, sum by (le, ork_store_operation) (rate(ork_store_request_duration_seconds_bucket[$__rate_interval])))`, "{{ork_store_operation}}")),
			series("Store clock skew", "The engines' clocks against the object store's: the largest and smallest offset any engine reports.", unitSeconds,
				q(`max(ork_store_clock_skew_seconds)`, "largest skew"),
				q(`min(ork_store_clock_skew_seconds)`, "smallest skew")),
			series("Audit records", "Control-plane writes recorded, and those the audit store could not take.", unitOps,
				q(`sum by (outcome) (rate(ork_audit_records_total[$__rate_interval]))`, "{{outcome}}")),
			series("Telemetry that could not be sent", "Export errors, and records dropped because their namespace's sink is unknown.", unitOps,
				q(`sum by (ork_telemetry_signal) (rate(ork_telemetry_export_errors_total[$__rate_interval]))`, "{{ork_telemetry_signal}} errors"),
				q(`sum by (reason) (rate(ork_telemetry_records_dropped_total[$__rate_interval]))`, "dropped: {{reason}}")),
			series("Census passes skipped", "Periodic reads that feed gauges and were skipped by their budget; a gauge whose census is skipped is stale.", unitOps,
				q(`sum by (ork_census) (rate(ork_census_skipped_total[$__rate_interval]))`, "{{ork_census}}")),
		}},
		{title: "Is the artifact GC reclaiming space?", panels: []panel{
			stat("GC mode", "What the artifact GC does: off, dry_run (reports what it would delete) or delete.", unitNone,
				qi(`max by (mode) (ork_gc_mode) == 1`, "{{mode}}")).withThresholds(neutral()...).withNoValue("not reported"),
			stat("GC held", "1 while the GC deletes nothing for a reason it reports.", unitBool,
				qi(`max(ork_gc_held) or vector(0)`, "held")).withThresholds([]threshold{{0, colorOK}, {1, colorWait}}...),
			series("Reclaimed", "Bytes the GC deleted across every namespace, by kind of object.", unitBytes,
				q(`sum by (ork_gc_object_class) (rate(ork_deployment_gc_bytes_total{outcome="deleted"}[$__rate_interval]))`, "{{ork_gc_object_class}}")),
			series("Would delete (dry run)", "What the last complete dry-run cycle would have deleted.", unitBytes,
				q(`max(ork_gc_would_delete_bytes)`, "would delete")),
			series("Low-water age", "The age of the oldest open or unarchived run, which holds back reclamation of everything retired after it.", unitSeconds,
				q(`max(ork_gc_lowwater_age_seconds)`, "low water"),
				q(`max(ork_gc_lag_seconds)`, "lag")),
			series("Bytes written by OrkStack against reclaimed", "Store bytes OrkStack's own flows wrote, beside what the GC reclaimed. Never the bucket's total.", unitBytes,
				q(`sum(rate(ork_store_bytes_total{ork_store_operation=~"put|multipart"}[$__rate_interval]))`, "written"),
				q(`sum(rate(ork_deployment_gc_bytes_total{outcome="deleted"}[$__rate_interval]))`, "reclaimed")),
		}},
	}
}
