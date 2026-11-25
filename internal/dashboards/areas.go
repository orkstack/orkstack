package dashboards

import "github.com/orkstack/orkstack/internal/alerts"

// The area rows of the OrkStack dashboard that each read one area's own
// instruments: schedules, webhooks, HTTP triggers, concurrency limits, and
// the retention cleanup and artifact GC (prod-kit design §2.3.4, R4 to R7
// and R10). A gauge every engine reports for the whole deployment is read
// with max by, never summed, so two replicas never count it twice.

// ns selects by the namespace variable.
const ns = `ork_namespace=~"$namespace"`

// rate is a per-second rate of a counter over the panel's interval.
func rate(series string) string { return `rate(` + series + `[$__rate_interval])` }

// p95 is a histogram's 95th percentile, grouped by le and the labels given.
func p95(bucket, by string) string {
	return `histogram_quantile(0.95, sum by (le` + by + `) (` + rate(bucket) + `))`
}

func schedulesRow() row {
	next := `max by (ork_namespace, ork_schedule) (` + with("ork_schedule_next_fire_seconds", ns) + `)`
	return row{title: "Are schedules firing on time?", panels: []panel{
		series("Fires by outcome", "Schedule fires the engines prepared, by outcome and, for a refused one, why. An unavailable fire is tried again.", unitOps,
			q(`sum by (outcome, ork_refusal_reason) (`+rate(with("ork_schedule_fires_total", ns))+`)`, "{{outcome}} {{ork_refusal_reason}}")),
		series("Busiest schedules", "The ten schedules that fired most over the panel's interval. A schedule is named only while telemetry.object_labels is on.", unitShort,
			q(`topk(10, sum by (ork_namespace, ork_schedule) (increase(`+with("ork_schedule_fires_total", ns)+`[$__rate_interval])))`, "{{ork_namespace}} {{ork_schedule}}")),
		series("Fire lag, p95", "How long after its nominal time a fire was prepared, at the 95th percentile. A backfill is as late as the time it stands in for.", unitSeconds,
			q(p95(with("ork_schedule_fire_lag_seconds_bucket", ns), ""), "p95")),
		series("Actions the cluster skipped", "Schedule actions the cluster did not take, by why: an overlap policy, a missed catch-up window, or a full buffer.", unitOps,
			q(`sum by (ork_schedule_skip_reason) (`+rate(with("ork_schedule_actions_skipped_total", ns))+`)`, "{{ork_schedule_skip_reason}}")),
		series("Requests by action", "Schedule requests that succeeded, by action: what people changed, and fires and backfills asked by hand.", unitOps,
			q(`sum by (action) (`+rate(with("ork_schedule_requests_total", ns))+`)`, "{{action}}")),
		series("Schedules by state", "Schedules active and paused.", unitShort,
			q(`sum by (state) (max by (ork_namespace, state) (`+with("ork_schedules", ns)+`))`, "{{state}}")),
		stat("Overdue schedules", "Namespaces whose soonest next fire is more than 10 minutes in the past: the cluster is not firing their schedules. With telemetry.object_labels on, each schedule counts.", unitShort,
			qi(`count(`+next+` < time() - 600) or vector(0)`, "overdue")).withThresholds(zeroOKElseFail()...),
		table("The 20 soonest fires", "Seconds until the soonest next fire of each namespace, or of each schedule while telemetry.object_labels is on, soonest first.", unitSeconds,
			qi(`bottomk(20, `+next+` - time())`, "{{ork_namespace}} {{ork_schedule}}")),
		series("Schedule census", "The share of schedules the last census read, and 1 while the census cannot read them; then the schedule gauges are stale.", unitPercent,
			q(`max(ork_schedule_census_coverage_ratio)`, "coverage"),
			q(`max(ork_schedule_census_unavailable_ratio)`, "unavailable")),
	}}
}

func webhooksRow() row {
	attempts := with("ork_webhook_attempts_total", ns)
	return row{title: "Are webhooks delivered?", panels: []panel{
		series("Deliveries by outcome", "Webhook deliveries that settled: delivered, or failed for good after their last attempt.", unitOps,
			q(`sum by (outcome) (`+rate(with("ork_webhook_deliveries_total", ns))+`)`, "{{outcome}}")),
		series("Busiest webhooks", "The ten webhooks with the most deliveries over the panel's interval. A webhook is named only while telemetry.object_labels is on.", unitShort,
			q(`topk(10, sum by (ork_namespace, ork_webhook_id) (increase(`+with("ork_webhook_deliveries_total", ns)+`[$__rate_interval])))`, "{{ork_namespace}} {{ork_webhook_id}}")),
		series("Attempts by answer", "Delivery attempts by what answered: the receiver's status class, a transport error, a request never sent, or one refused.", unitOps,
			q(`sum by (ork_webhook_class, outcome) (`+rate(attempts)+`)`, "{{ork_webhook_class}} {{outcome}}")),
		series("Attempts by number", "Attempts by their place in the delivery's retry ladder.", unitOps,
			q(`sum by (ork_webhook_attempt) (`+rate(attempts)+`)`, "attempt {{ork_webhook_attempt}}")),
		series("Attempts per delivery, p95", "How many attempts a settled delivery took, at the 95th percentile.", unitShort,
			q(p95(with("ork_webhook_delivery_attempts_bucket", ns), ""), "p95")),
		series("Delivery latency, p95", "How long after it was queued a delivery settled, at the 95th percentile, by outcome.", unitSeconds,
			q(p95(with("ork_webhook_delivery_latency_seconds_bucket", ns), ", outcome"), "{{outcome}}")),
		series("Request duration, p95", "How long one attempt's request took, at the 95th percentile, by answer.", unitSeconds,
			q(p95(with("ork_webhook_attempt_duration_seconds_bucket", ns), ", ork_webhook_class"), "{{ork_webhook_class}}")),
		series("Deliveries pending, by age", "Deliveries queued and not yet settled, counted by whether they are older than each age.", unitShort,
			q(`max by (ork_age_over) (ork_webhook_deliveries_pending)`, "older than {{ork_age_over}}")),
		series("Deliveries not queued", "Deliveries that could not be queued and so made no request, by why.", unitOps,
			q(`sum by (reason) (`+rate(with("ork_webhook_not_queued_total", ns))+`)`, "{{reason}}")),
		logs("Recent delivery failures", "The engine's lines for deliveries that failed for good, newest first. Never the URL.",
			q(`{service_name=~".*ork"} |= "webhook delivery failed"`, "")).sized(24, 8).withLinks(runViewLink),
	}}
}

func triggersRow() row {
	return row{title: "Are triggers starting runs?", panels: []panel{
		series("HTTP trigger invocations by outcome", "Invocations of HTTP triggers, per second, by namespace and outcome. One that named no trigger carries no namespace.", unitOps,
			q(`sum by (ork_namespace, outcome) (`+rate(with("ork_trigger_invocations_total", ns))+`)`, "{{ork_namespace}} {{outcome}}")).sized(16, 8),
		stat("Invocations that started no run, 1 h", "HTTP trigger invocations in the last hour that started no run, whatever the reason.", unitShort,
			qi(alerts.NEW(with("ork_trigger_invocations_total", ns, `outcome!="started"`), "1h")+` or vector(0)`, "not started")).
			withThresholds(okWarnFail(1, 50)...).sized(8, 8),
	}}
}

func concurrencyRow() row {
	byKey := `(ork_namespace, ork_concurrency_limit, ork_concurrency_level)`
	byLimit := `(ork_namespace, ork_concurrency_limit)`
	g := func(metric, by string) string { return `max by ` + by + ` (` + with(metric, ns) + `)` }
	return row{title: "Are concurrency limits holding work back?", panels: []panel{
		series("Slots held against the limit", "Per limit and level, the slots held now beside the slots its active keys admit.", unitShort,
			q(g("ork_concurrency_slots_used", byKey), "{{ork_namespace}} {{ork_concurrency_limit}} {{ork_concurrency_level}} held"),
			dash(g("ork_concurrency_slots_limit", byKey), "{{ork_namespace}} {{ork_concurrency_limit}} {{ork_concurrency_level}} limit")).sized(24, 8),
		series("Keys active and full", "Per limit, keys holding or queueing anything, and those with every slot held.", unitShort,
			q(g("ork_concurrency_keys_active", byLimit), "{{ork_namespace}} {{ork_concurrency_limit}} active"),
			q(g("ork_concurrency_keys_full", byLimit), "{{ork_namespace}} {{ork_concurrency_limit}} full")),
		series("Queued", "Runs and tasks waiting for a slot, per limit and level, and runs queued per namespace.", unitShort,
			q(g("ork_concurrency_queue_depth", byKey), "{{ork_namespace}} {{ork_concurrency_limit}} {{ork_concurrency_level}}"),
			q(g("ork_concurrency_queued_runs", "(ork_namespace)"), "{{ork_namespace}} runs")),
		series("Requests for a slot by outcome", "Requests for a slot, per second, by strategy and how each was answered: granted, queued, refused or skipped, or a limit that could not answer.", unitOps,
			q(`sum by (ork_concurrency_strategy, ork_concurrency_outcome) (`+rate(with("ork_concurrency_arrivals_total", ns))+`)`, "{{ork_concurrency_strategy}} {{ork_concurrency_outcome}}")),
		series("Waits ended by outcome", "Waits in line that ended, per second, by how: started, canceled, timed out, superseded, or a limit that could not answer.", unitOps,
			q(`sum by (ork_concurrency_level, ork_concurrency_outcome) (`+rate(with("ork_concurrency_exits_total", ns))+`)`, "{{ork_concurrency_level}} {{ork_concurrency_outcome}}")),
		series("Wait in line, p95", "How long a wait in line lasted before it ended, at the 95th percentile, per limit and level.", unitSeconds,
			q(p95(with("ork_concurrency_queue_wait_seconds_bucket", ns), ", ork_concurrency_limit, ork_concurrency_level"), "{{ork_concurrency_limit}} {{ork_concurrency_level}}")),
		series("Oldest queued", "How long the oldest queued entry has waited, per limit and level.", unitSeconds,
			q(g("ork_concurrency_queue_oldest_age_seconds", byKey), "{{ork_namespace}} {{ork_concurrency_limit}} {{ork_concurrency_level}}")),
		series("Oldest queued, share of its bound", "The oldest queued entry's wait as a share of how long it may wait; at 1 it is refused.", unitPercent,
			q(g("ork_concurrency_queue_oldest_timeout_fraction", byKey), "{{ork_namespace}} {{ork_concurrency_limit}} {{ork_concurrency_level}}")),
		series("Holders and gates in trouble", "Holders whose runs keep failing their workflow tasks, gates checking new holders after dropping a fence, and gates rebuilding their record.", unitShort,
			q(g("ork_concurrency_stalled_holders", byLimit), "{{ork_namespace}} {{ork_concurrency_limit}} stalled holders"),
			q(g("ork_concurrency_fence_degraded", byLimit), "{{ork_namespace}} {{ork_concurrency_limit}} fence degraded"),
			q(g("ork_concurrency_rebuilding", "(ork_namespace, ork_concurrency_state)"), "rebuilding, {{ork_concurrency_state}}")),
		series("Slots taken back", "Slots and places in line a limit took back, per second, because their run no longer holds or waits for them, by why, and the checks of a run that failed, whose slot or place is kept.", unitOps,
			q(`sum by (ork_concurrency_limit, ork_concurrency_outcome) (`+rate(with("ork_concurrency_reclaimed_total", ns))+`)`, "{{ork_concurrency_limit}} {{ork_concurrency_outcome}}"),
			dash(`sum by (ork_concurrency_limit) (`+rate(with("ork_concurrency_verify_errors_total", ns))+`)`, "{{ork_concurrency_limit}} check failed")),
		series("Limit starts", "Keys that became active, per second, and the rebuilds of their record: skipped after a clean end, done, or an attempt that failed and is tried again.", unitOps,
			q(`sum(`+rate(with("ork_concurrency_gates_started_total", ns))+`)`, "started"),
			q(`sum by (ork_concurrency_outcome) (`+rate(with("ork_concurrency_rebuilt_total", ns))+`)`, "rebuild {{ork_concurrency_outcome}}")),
		series("Concurrency census", "Gates the last census read and those that did not answer; 1 while it stopped at its gate ceiling, could not run, or a namespace's gate did not answer.", unitShort,
			q(`max(ork_concurrency_census_gates)`, "gates"),
			q(`max(ork_concurrency_census_unanswered)`, "unanswered"),
			q(`max(ork_concurrency_census_truncated)`, "truncated"),
			q(`max(ork_concurrency_census_unavailable)`, "unavailable"),
			q(`max by (ork_namespace) (`+with("ork_concurrency_census_partial", ns)+`)`, "{{ork_namespace}} partial"),
			dash(`sum(increase(ork_concurrency_census_failures_total[$__rate_interval]))`, "passes that could not run")),
		series("Concurrency census age", "How long the last census pass took, and how old each engine's snapshot is beside the census interval.", unitSeconds,
			q(`max(ork_concurrency_census_duration_seconds)`, "pass"),
			q(`max(ork_concurrency_census_local_age_seconds)`, "snapshot age"),
			q(`max(ork_concurrency_census_interval_seconds)`, "interval")),
		stat("Concurrency index", "1 when the cluster does not index concurrency limits, so the census and submit's checks cannot count them.", unitBool,
			qi(`max(ork_concurrency_index_unavailable)`, "unavailable")).withThresholds(zeroOKElseFail()...).withNoValue("not reported"),
	}}
}

// cleanupPanels open the last row: retention cleanup, then the archive
// sweep that runs on its interval.
func cleanupPanels() []panel {
	return []panel{
		stat("Retention cleanup", "Whether retention cleanup retires runs past their namespace's retention.", unitBool,
			qi(`max(ork_cleanup_enabled_ratio)`, "on")).withThresholds([]threshold{{0, colorWait}, {1, colorOK}}...).withNoValue("not reported"),
		stat("Cleanup interval and buffer", "The cleanup interval, and how far past the maximum run retention the cluster's own backstop sits.", unitSeconds,
			qi(`max(ork_cleanup_interval_seconds)`, "interval"),
			qi(`max(ork_cleanup_retention_buffer_seconds)`, "buffer")).withThresholds(neutral()...),
		series("Runs cleanup decided on", "Runs a cleanup pass decided on, by outcome, in the selected namespaces and in all of them.", unitOps,
			q(`sum by (outcome) (`+rate(with("ork_cleanup_runs_total", ns))+`)`, "{{outcome}}"),
			q(`sum by (outcome) (`+rate("ork_deployment_cleanup_runs_total")+`)`, "all namespaces: {{outcome}}")),
		series("Objects removed by cleanup", "Run-scoped objects retirement removed.", unitOps,
			q(`sum(`+rate(with("ork_cleanup_objects_total", ns))+`)`, "objects")),
		series("Cleanup passes", "Cleanup passes by outcome.", unitOps,
			q(`sum by (outcome) (`+rate("ork_cleanup_passes_total")+`)`, "{{outcome}}")),
		series("Cleanup pass duration, p95", "How long a cleanup pass took, at the 95th percentile.", unitSeconds,
			q(p95("ork_cleanup_pass_duration_seconds_bucket", ""), "p95")),
		series("Cleanup behind retention", "How far past its due time the most overdue run cleanup left unretired is, and how long since the last complete walk.", unitSeconds,
			q(`max by (ork_namespace) (`+with("ork_cleanup_retention_lag_seconds", ns)+`)`, "{{ork_namespace}}"),
			q(`max(ork_deployment_cleanup_retention_lag_seconds)`, "all namespaces"),
			q(`(time() - max(ork_cleanup_last_pass_seconds)) and on () (max(ork_cleanup_last_pass_seconds) > 0)`, "since the last complete walk")),
		series("Cleanup errors by step", "Cleanup failures by the step that failed; a later pass tries again.", unitOps,
			q(`sum by (ork_cleanup_step) (`+rate("ork_cleanup_errors_total")+`)`, "{{ork_cleanup_step}}")),
		stat("Never completed", "1 for each walk that has not finished once since its engine started: the archive sweep's full pass, cleanup's complete walk, and a GC pass. Alerts fire on the same.", unitShort,
			qi(`max(ork_archive_sweep_last_success_seconds) == bool 0`, "archive sweep"),
			qi(`max(ork_cleanup_last_pass_seconds) == bool 0`, "cleanup walk"),
			qi(`max(ork_gc_last_pass_seconds) == bool 0`, "GC pass")).
			withThresholds([]threshold{{0, colorOK}, {1, colorWait}}...).withNoValue("not reported").sized(8, 4),
		series("Archive sweep", "Finished runs the sweep could not confirm archived; 1 while it could not read the run index to its end, or while two intervals reach the cluster's retention.", unitShort,
			q(`max(ork_archive_sweep_unconfirmed)`, "unconfirmed"),
			q(`max(ork_archive_sweep_list_failed)`, "index not read"),
			q(`max(ork_archive_sweep_interval_too_long)`, "interval too long")),
		series("Archive sweep timing", "Seconds since the last pass that saw everything, and, while runs are unconfirmed, until the first of them expires.", unitSeconds,
			q(`(time() - max(ork_archive_sweep_last_success_seconds)) and on () (max(ork_archive_sweep_last_success_seconds) > 0)`, "since a full pass"),
			q(`min(ork_archive_sweep_earliest_expiry_seconds) and on () (max(ork_archive_sweep_unconfirmed) > 0)`, "until the first expiry")),
		series("Runs the archive sweep came to", "Finished runs with no archive, by what the sweep did: archived, failed (tried again), or expired (lost).", unitOps,
			q(`sum by (outcome) (`+rate("ork_archive_sweep_runs_total")+`)`, "{{outcome}}")),
	}
}

// gcPanels follow the GC's mode and reclaimed bytes in the last row.
func gcPanels() []panel {
	return []panel{
		stat("GC grace", "How long a retired run's objects stay before the GC may reclaim them.", unitSeconds,
			qi(`max(ork_gc_grace_seconds)`, "grace")).withThresholds(neutral()...).withNoValue("not reported"),
		stat("Bucket keeps versions", "1 when the bucket keeps versions, so a GC delete only adds a delete marker and frees nothing.", unitBool,
			qi(`max(ork_gc_store_versioned_ratio)`, "versioned")).withThresholds([]threshold{{0, colorOK}, {1, colorWait}}...).withNoValue("not reported"),
		series("Why the GC is held", "1 for each reason the last pass deleted nothing.", unitShort,
			q(`max by (ork_gc_hold_reason) (ork_gc_held) == 1`, "{{ork_gc_hold_reason}}")),
		series("Objects by outcome", "Objects the GC dealt with in delete mode, by outcome and why, in the selected namespaces and in all of them.", unitOps,
			q(`sum by (outcome, ork_gc_reason) (`+rate(with("ork_gc_objects_total", ns))+`)`, "{{outcome}} {{ork_gc_reason}}"),
			q(`sum by (outcome) (`+rate("ork_deployment_gc_objects_total")+`)`, "all namespaces: {{outcome}}")),
		series("Reclaimed by namespace", "Bytes the GC deleted, by namespace.", unitBytes,
			q(`sum by (ork_namespace) (`+rate(with("ork_gc_bytes_total", ns, `outcome="deleted"`))+`)`, "{{ork_namespace}}")),
		series("Would delete and kept (dry run)", "The last complete dry-run cycle: objects it would delete, and objects it would keep, by why.", unitShort,
			q(`sum(max by (ork_namespace, ork_gc_object_class) (`+with("ork_gc_would_delete_objects", ns)+`))`, "would delete"),
			q(`sum by (ork_gc_reason) (max by (ork_namespace, ork_gc_object_class, ork_gc_reason) (`+with("ork_gc_kept_objects", ns)+`))`, "kept: {{ork_gc_reason}}")),
		series("Would delete in all namespaces (dry run)", "The last complete dry-run cycle's bytes across every namespace.", unitBytes,
			q(`max(ork_deployment_gc_would_delete_bytes)`, "would delete")),
		series("GC passes", "GC passes by outcome.", unitOps,
			q(`sum by (outcome) (`+rate("ork_gc_passes_total")+`)`, "{{outcome}}")),
		series("GC pass duration, p95", "How long a GC pass took, at the 95th percentile.", unitSeconds,
			q(p95("ork_gc_pass_duration_seconds_bucket", ""), "p95")),
		series("Retired runs the GC came to", "Retired runs by outcome: finished, carried over, waiting on another run or the low-water mark, or held for good.", unitOps,
			q(`sum by (outcome) (`+rate("ork_gc_runs_total")+`)`, "{{outcome}}")),
		series("Runs due and waiting", "Retired runs the last pass carried over, and those waiting on another run or the low-water mark.", unitShort,
			q(`max(ork_gc_due_runs)`, "due"),
			q(`max(ork_gc_waiting_runs)`, "waiting")),
		series("Since the GC last moved", "Seconds since the last pass, and since the last one that finished a run or deleted an object.", unitSeconds,
			q(`(time() - max(ork_gc_last_pass_seconds)) and on () (max(ork_gc_last_pass_seconds) > 0)`, "since a pass"),
			q(`(time() - max(ork_gc_last_progress_seconds)) and on () (max(ork_gc_last_progress_seconds) > 0)`, "since progress")),
		series("Low-water mark", "1 for the reason the last pass could not compute its low-water mark, and how many open runs its walk read.", unitShort,
			q(`max by (ork_gc_lowwater_cause) (ork_gc_lowwater_unknown) == 1`, "unknown: {{ork_gc_lowwater_cause}}"),
			q(`max(ork_gc_lowwater_chains)`, "open runs walked")),
	}
}

// usage selects the Orkers' attempt usage summaries in Loki, one line per
// attempt, by structured metadata.
const usage = `{service_name=~".*orker"} | ork_record="usage" | ork_usage_kind="summary"`

// runsMorePanels follow the runs row's counts: the duration's distribution,
// integrity failures, attempts, and the tasks that retried or used most.
func runsMorePanels() []panel {
	return []panel{
		heatmap("Run duration distribution", "How long runs took, bucket by bucket; an exemplar opens the trace of one run of that length. A resumed run's later endings are left out.", unitSeconds,
			`sum by (le) (increase(`+with("ork_run_duration_seconds_bucket", distinctRuns, sel)+`[$__interval]))`),
		series("Integrity failures", "Attempts that found stored data not what was written, by reason, across every namespace, the first of a reason included. Never a workload's fault: shown even at zero.", unitShort,
			q(alerts.NEW(`ork_deployment_orker_tasks_total{ork_failure_reason=~"`+alerts.IntegrityReasons+`"}`, "$__rate_interval", "ork_failure_reason")+` or vector(0)`, "{{ork_failure_reason}}")).
			withNoValue("none"),
		series("Attempts a task took, p95", "The attempt number tasks finished on, at the 95th percentile, by pool.", unitShort,
			q(p95(with("ork_task_attempts_bucket", ns, pools), ", ork_pool"), "{{ork_pool}}")),
		series("Most-retried tasks", "The ten tasks whose attempts reached the highest attempt number, from the Orkers' usage summaries.", unitShort,
			q(`topk(10, max by (ork_namespace, ork_task_id) (max_over_time(`+usage+` | ork_namespace=~"$namespace" | unwrap ork_attempt [$__interval])))`, "{{ork_namespace}} {{ork_task_id}}")).on(loki),
		series("Tasks that used the most CPU", "The ten tasks whose attempts used the most CPU seconds, from the Orkers' usage summaries.", unitSeconds,
			q(`topk(10, sum by (ork_namespace, ork_task_id) (sum_over_time(`+usage+` | ork_namespace=~"$namespace" | unwrap ork_usage_cpu_seconds [$__interval])))`, "{{ork_namespace}} {{ork_task_id}}")).on(loki),
	}
}

// orkersMorePanels follow the Orkers row's capacity panels: what runs now,
// why nobody polls, admission, phases, and what failed around an attempt.
func orkersMorePanels() []panel {
	return []panel{
		series("Live CPU of running tasks", "The CPU cores running attempts' containers use now: across every Orker, and on the busiest one.", unitShort,
			q(`sum(ork_orker_containers_cpu)`, "all Orkers"),
			q(`max(ork_orker_containers_cpu)`, "busiest Orker")),
		series("Live memory of running tasks", "The memory running attempts' containers use now, across every Orker and on the largest one, and how many containers were measured.", unitBytes,
			q(`sum(ork_orker_containers_memory_bytes)`, "all Orkers"),
			q(`max(ork_orker_containers_memory_bytes)`, "largest Orker"),
			q(`sum(ork_orker_containers_measured)`, "containers measured")),
		series("Memory reserved, share of capacity", "Per Orker that declares a capacity, the share of the memory it admits tasks within that stands reserved; the 20 most reserved.", unitPercent,
			q(`topk(20, max by (ork_orker_id) (ork_orker_capacity_reserved_memory_bytes / (ork_orker_capacity_memory_bytes > 0)))`, "{{ork_orker_id}}")),
		series("Most over-reserved tasks", "The ten tasks whose attempts' peak memory was the smallest share of what they reserved: lower their request.", unitPercent,
			q(`bottomk(10, max by (ork_namespace, ork_task_id) (max_over_time(`+usage+` | ork_namespace=~"$namespace" | ork_usage_memory_peak_exact="true" | unwrap ork_usage_memory_peak_bytes [$__interval])) / max by (ork_namespace, ork_task_id) (max_over_time(`+usage+` | ork_namespace=~"$namespace" | unwrap ork_reserved_memory_bytes [$__interval])))`, "{{ork_namespace}} {{ork_task_id}}")).on(loki),
		series("Why nobody polls", "Per pool: Orkers exporting metrics that are not asking for work, beside those cordoned. A busy Orker asks for none either: see slots in use.", unitShort,
			q(`clamp_min(count by (ork_pool) (ork_orker_present{`+pools+`}) - (max by (ork_pool) (ork_pool_pollers{`+pools+`}) or count by (ork_pool) (ork_orker_present{`+pools+`}) * 0), 0)`, "{{ork_pool}} not polling"),
			q(`sum by (ork_pool) (ork_orker_cordoned{`+pools+`})`, "{{ork_pool}} cordoned")),
		series("Slots in use, by pool", "The share of each pool's slots its running tasks fill; at 1 its Orkers ask for no new work.", unitPercent,
			q(`sum by (ork_pool) (ork_orker_running_tasks{`+pools+`}) / (sum by (ork_pool) (ork_orker_max_concurrent{`+pools+`}) > 0)`, "{{ork_pool}}")),
		series("Pools whose backlog is not measured", "Pools with Orkers but no backlog reading: name them in engine.pool_metrics.pools.", unitShort,
			q(`count(count by (ork_pool) (ork_orker_present{`+pools+`}) unless on (ork_pool) max by (ork_pool) (ork_pool_backlog_tasks)) or vector(0)`, "pools")),
		series("Admission wait, p95", "How long an Orker's poll waited for room before it was granted, at the 95th percentile, by what bound it, and attempts refused for a request no Orker admits.", unitSeconds,
			q(p95("ork_orker_admission_wait_duration_seconds_bucket", ", ork_admission_bound"), "{{ork_admission_bound}}"),
			q(`sum(`+rate("ork_orker_admission_refusals_total")+`)`, "refused per second")),
		series("Attempt phases, p95", "How long each phase of an attempt took on the Orkers, at the 95th percentile.", unitSeconds,
			q(p95(with("ork_orker_task_phase_duration_seconds_bucket", pools), ", ork_phase"), "{{ork_phase}}")).sized(24, 8),
		series("Task log export", "Task log records the Orkers exported, by outcome, and export batches by outcome.", unitOps,
			q(`sum by (outcome) (`+rate("ork_orker_task_log_records_total")+`)`, "records {{outcome}}"),
			q(`sum by (outcome) (`+rate("ork_orker_task_log_exports_total")+`)`, "batches {{outcome}}")),
		series("Attempt records", "Attempt records the Orkers wrote, by outcome; one not written leaves an attempt without its record.", unitOps,
			q(`sum by (outcome) (`+rate(with("ork_orker_attempt_records_total", pools))+`)`, "{{outcome}}")),
		series("Left behind or unmeasured", "Attempts whose workspace could not be removed, whose container's end could not be watched, whose usage could not be measured or sampled, and those stopped at their memory limit.", unitOps,
			q(`sum(`+rate("ork_orker_workspace_cleanup_failures_total")+`)`, "workspace not removed"),
			q(`sum by (reason) (`+rate("ork_orker_stop_watch_failures_total")+`)`, "end not watched: {{reason}}"),
			q(`sum(`+rate("ork_orker_task_usage_unmeasured_total")+`)`, "usage unmeasured"),
			q(`sum(`+rate("ork_orker_usage_sampler_failures_total")+`)`, "sampler failed"),
			q(`sum(`+rate("ork_orker_task_oom_kills_total")+`)`, "stopped at the memory limit")),
		series("Object store outages", "Orkers entering and leaving their store-outage cordon.", unitOps,
			q(`sum by (ork_transition) (`+rate("ork_orker_store_outage_transitions_total")+`)`, "{{ork_transition}}")),
		table("Builds running", "Each build of ork and orker exporting metrics now, and how many processes run it.", unitShort,
			qi(`count by (job, ork_build_id) (ork_build_info)`, "{{job}} {{ork_build_id}}")),
	}
}

// cacheMorePanels follow the cache row's ratios: by Orkflow, and what a hit
// saved.
func cacheMorePanels() []panel {
	return []panel{
		series("Cached tasks by Orkflow and outcome", "Tasks of cached steps that finished, per second, by Orkflow and memo outcome.", unitOps,
			q(`sum by (ork_orkflow_name, ork_cache_outcome) (`+rate(with("ork_tasks_total", sel, `ork_cache_outcome!=""`))+`)`, "{{ork_orkflow_name}} {{ork_cache_outcome}}")),
		series("Results reused", "Memo hits that reused a result and started no container, per second, by pool.", unitOps,
			q(`sum by (ork_pool) (`+rate(with("ork_orker_memo_reused_total", pools))+`)`, "{{ork_pool}}")),
		series("Memo step duration against its timeout", "The memo step's 95th percentile beside its 30-second timeout, after which it is a miss.", unitSeconds,
			q(p95(`ork_orker_task_phase_duration_seconds_bucket{ork_phase="memo"}`, ""), "p95"),
			dash(`vector(30)`, "timeout")),
	}
}

// engineMorePanels follow the engine row's API and store panels: carrying
// on, Temporal's long polls, the Go runtime, the Collector, and the alert
// pipeline's own heartbeat.
func engineMorePanels() []panel {
	return []panel{
		series("Carrying on: write, restore and hold, p95", "How long a switch's write and a whole restore took, and how long runs held new tasks first, at the 95th percentile.", unitSeconds,
			q(p95(with("ork_run_continuation_duration_seconds_bucket", ns), ", phase"), "{{phase}}"),
			q(p95(with("ork_run_continuation_hold_duration_seconds_bucket", ns), ""), "hold")),
		series("Saved-state reads, p95", "The attempt each successful read of a saved state succeeded on, at the 95th percentile.", unitShort,
			q(p95(with("ork_run_continuation_read_attempts_bucket", ns), ""), "p95")),
		series("Temporal long polls", "The engines' and Orkers' long polls to Temporal, and those that failed.", unitOps,
			q(`sum(rate(temporal_long_request_total[$__rate_interval]))`, "polls"),
			q(`sum(rate(temporal_long_request_failure_total[$__rate_interval]))`, "failed")),
		series("Goroutines", "Goroutines of the largest process of each service.", unitShort,
			q(`max by (job) (go_goroutine_count)`, "{{job}}")),
		series("Go memory in use", "Memory the Go runtime holds in the largest process of each service.", unitBytes,
			q(`max by (job) (go_memory_used_bytes)`, "{{job}}")),
		series("Processes seen", "Processes of each service exporting metrics now, beside how many the last hour saw: more in the hour means restarts.", unitShort,
			q(`count by (job) (ork_build_info)`, "{{job}} now"),
			q(`count by (job) (count_over_time(ork_build_info[1h]))`, "{{job}} in the hour")),
		series("Collector intake", "Points, records and spans the Collector accepted, refused and failed, per second.", unitOps,
			q(`sum(rate(otelcol_receiver_accepted_metric_points_total[$__rate_interval])) + sum(rate(otelcol_receiver_accepted_log_records_total[$__rate_interval])) + sum(rate(otelcol_receiver_accepted_spans_total[$__rate_interval]))`, "accepted"),
			q(`(sum(rate(otelcol_receiver_refused_metric_points_total[$__rate_interval])) + sum(rate(otelcol_receiver_refused_log_records_total[$__rate_interval])) + sum(rate(otelcol_receiver_refused_spans_total[$__rate_interval]))) or vector(0)`, "refused"),
			q(`(sum(rate(otelcol_receiver_failed_metric_points_total[$__rate_interval])) + sum(rate(otelcol_receiver_failed_log_records_total[$__rate_interval])) + sum(rate(otelcol_receiver_failed_spans_total[$__rate_interval]))) or vector(0)`, "failed")),
		series("Collector exports waiting", "Items the Collector holds to send, against how many it can hold.", unitShort,
			q(`sum(otelcol_exporter_queue_size)`, "queued"),
			q(`sum(otelcol_exporter_queue_capacity)`, "capacity")),
		stat("Alert pipeline", "1 while the watchdog alert, which always fires, reaches Prometheus: the rules are loaded and evaluating.", unitBool,
			qi(`max(ALERTS{alertname="OrkStackWatchdog",alertstate="firing"}) or vector(0)`, "watchdog")).
			withThresholds([]threshold{{0, colorFail}, {1, colorOK}}...),
	}
}
