# Runbooks

This page is what to do when one of OrkStack's alerts fires, how to back up
and restore a deployment, and how a Temporal outage looks and ends. Every
alert's `runbook_url` points at its entry here.

The alert rules are optional deploy material, beside the observability kit's
dashboards ([deploy/observability/README.md](../deploy/observability/README.md)).
Nothing in this page needs the kit: every **Confirm** step that can
be checked without it names an `orkctl` command, and the rest say "kit" (a
dashboard or a query in your own Prometheus) or name an engine log line.

## Using the rules

The rules are files Prometheus loads with `rule_files:`, under
`deploy/observability/prometheus/rules/`, generated from `internal/alerts`
and checked by promtool at the Prometheus the kit pins
(`scripts/check-alerts.sh`):

| File | Holds |
|---|---|
| `orkstack-alerts-<area>.yml` | the alerts of one area, one file per section below |
| `orkstack-records.yml` | the recording rules the alerts read |

Load them all, for example with `/etc/prometheus/rules/orkstack-*.yml` under
`rule_files:`. Each `*_test.yaml` beside them is a file's promtool test:
every alert has a case that fires it and one that leaves it quiet.

What your Prometheus needs:

- metrics that arrive over OTLP, through an OpenTelemetry Collector;
- the resource attributes `ork.component`, `ork.deployment` and `ork.build_id`
  promoted to labels (`otlp.promote_resource_attributes`). Without them those
  labels are empty on every series: the rules still hold for one
  deployment, but cannot tell one deployment from another, nor engines'
  series from Orkers' in the per-component alerts;
- the Collector's own metrics (`otelcol_*`) in the same Prometheus, so a
  stopped Collector raises an alert of its own.

In production, the Collector's receiver must authenticate its senders (the
kit's, on loopback, does not). Anything that can push to it can forge the
series these alerts read, so every **Confirm** step checks with `orkctl`,
never with a metric alone.

Every alert carries two labels, `severity` (`critical`, `warning`, `info`, or
`none` for the Watchdog) and `component` (the area), and four annotations:
`summary`, `description`, `runbook_url` and `dashboard`, the OrkStack
dashboard's path under your Grafana. Every alert is prefixed `OrkStack`, so
one Alertmanager route catches them all. Each aggregation keeps
`ork_deployment`, so one Prometheus can watch several deployments.

**Route the Watchdog to a dead man's switch.** `OrkStackWatchdog` always
fires. When it stops arriving, Prometheus or its Alertmanager is down, and no
other alert can tell you.

**Alerts that fire on a counter's growth** (most of the at-once alerts) fire
when it grew within the alert's window, or when a new process reports it
above zero. They stay quiet while Prometheus or the deployment is younger
than that window, so a fresh Prometheus never pages on old counts.

**An alert resolving is not a recovery check.** Many alerts read gauges that
vanish with the process that reports them, so an alert can resolve because
its producer stopped. Each entry's **Confirm** step is the check.

**Rules on namespaced series cover only the namespaces exported to this
sink.** A namespace whose telemetry policy names another sink reports its own
runs there; the deployment-wide alerts read the operator twins
(`ork_deployment_*`), which every namespace reports to the default sink
([DEPLOY.md](./DEPLOY.md), "7. Telemetry").

## Temporal outages

Temporal is the only durable execution authority, so while it is unreachable
nothing new can start, and nothing that has started is lost.

**Telling an outage apart:**

- `OrkStackTemporalUnreachable` and `OrkStackTemporalPollFailures` fire;
- `orkctl admin info` still answers, so the engine is up;
- `orkctl pool list` shows `unknown (unreachable)` where a pool's count would
  be, while `orkctl orker list` still lists the fleet.

| Area | What happens while Temporal is unreachable |
|---|---|
| Submits | Answered `Unavailable`, and nothing is recorded: a start whose answer was lost may still land, so it is never recorded as a failure. Retrying with the **same** `--idempotency-key` is safe: the retry answers the run that landed, or starts it if none did ([DEPLOY.md](./DEPLOY.md), "5. Ops notes"). A submit made without a key cannot be made safe, so callers should submit with one. |
| Cancels and schedule changes | Answered `Unavailable` or `ResourceExhausted`. |
| HTTP triggers | Answered unavailable. |
| Running containers | Left alone while the cluster is silent for less than the heartbeat timeout. Past it, the Orker stops the container and the attempt is retried under its policy, never reported cancelled. |
| Schedules | No fires. Afterwards, Temporal catches up the fires that fall within the schedule's `catchup_window`; a schedule that sets none gets the cluster's default. |
| Webhooks | Deliveries wait, then resume. |
| Archive sweep, cleanup, artifact GC | Each pass fails closed and holds; nothing is reset. The first clean pass after recovery does what was due. |
| Reads | Reads of live runs fail; finished runs past retention are still served from the archive. |

**Never needed:** editing a record, deleting commits or markers, terminating
runs, restarting Orkers, or changing the GC mode.

**Recovery, each step with its check:**

1. `orkctl pool list` shows counts again, not `unknown (unreachable)`.
2. `orkctl run list` answers.
3. `orkctl run watch RUN` on a run that was running resumes with no gap in
   its sequence numbers.
4. `orkctl run attempts RUN -t TASK` shows the retries of attempts the
   outage ended.
5. `orkctl schedule get NAME` shows the catch-up fires.
6. `orkctl webhook deliveries ID` drains.
7. Callers retry their `Unavailable` submits with the same idempotency key;
   each retry answers one run id. For a submit made without a key, look in
   `orkctl run list` for a run of that Orkflow started then before submitting
   it again; such a start can land late, so this may not settle it.

| Partial failure | Effect |
|---|---|
| One frontend | error spikes that clear on their own |
| History service | some runs stall while others progress |
| Matching service | no task is dispatched: pools read 0 pollers while `orkctl orker list` shows the fleet (`OrkStackPoolNotPolling`) |
| Visibility | counts and censuses fail (`OrkStackCensusStale`); the artifact GC holds its pass |
| Slow persistence | everything slows; `OrkStackTasksWaiting` may fire |

## Backup and disaster recovery

A deployment's durable state is in two places, and both need a backup:

- **Temporal**, by its own procedure for the persistence it runs on.
- **The object-store bucket**, by versioning with point-in-time restore (the
  recommended way), by replication, or by a snapshot that is consistent across
  the whole bucket. Each gives an instant T_s that every restored key reflects.
  A snapshot of separate volumes taken at different moments gives none, and
  is not a backup of the bucket.

Add two lifecycle rules to the bucket: `AbortIncompleteMultipartUpload`, and,
with versioning, `NoncurrentVersionExpiration`, so deleted objects stop
costing space ([DEPLOY.md](./DEPLOY.md), "Retention cleanup and bucket
lifecycle rules").

**Back up Temporal first, at T_t, then the bucket, at T_s ≥ T_t.** OrkStack
writes every object before the history record that references it, so a
bucket at least as new as Temporal holds every object Temporal names
([AGENTS.md](../AGENTS.md), "4.1 Durable output commit").

**Restore, of either store:**

1. Stop every engine and Orker.
2. Turn retention cleanup off (`ORK_CLEANUP_ENABLED=false`), which also stops
   the artifact GC. Keep it off until step 7.
3. Restore Temporal to T_t and the bucket to T_s ≥ T_t, and write both
   instants down.
4. Start one engine and no Orkers. Reconcile the cluster with the bucket:

   ```bash
   orkctl admin restore reconcile --temporal-restored-at T_t --dry-run
   orkctl admin restore reconcile --temporal-restored-at T_t
   ```

   It lists the runs retention cleanup retired after T_t whose executions
   the restore brought back, and deletes those executions where the bucket
   holds the run's archive, which then answers for the run; it never deletes
   a running execution or an object. It also lists the runs the cluster
   shows open whose finished archive the bucket holds: they run their tail
   again, events and webhooks included. It needs `system.restore`, and
   refuses all but a dry run while the engine's retention cleanup is on.
   Deleting those executions changes nothing a resume may do: whether a
   retired run may be resumed is decided from its archive and its retention,
   never from the cluster.
5. Re-apply every automation document, so the schedules match what you
   declared: `orkctl automation apply --dry-run`, then
   `orkctl automation apply`.
6. Check: `orkctl run list` answers; `orkctl schedule list` shows your
   schedules; `orkctl admin gc preview` shows cleanup off.
7. Start the Orkers and turn cleanup on again. If the artifact GC runs in
   `delete` mode, set it to `dry_run` first, and return it to `delete` only
   once `orkctl admin gc preview` shows no hold and a known low-water mark.

Runs reopened by the restore are open again, so they hold the GC's low-water
mark; runs whose archives the bucket lost are archived again by the archive
sweep, and the GC waits for a clean sweep.

**What a restore loses:** everything Temporal recorded after T_t:
resolutions of awaited tasks, idempotency claims and resumes after T_t, and
the runs that finished after T_t replay their tail, emitting their events and
webhooks again. Commits written after T_t are adopted when their tasks run
again; tasks that had not committed run again under the same
`idempotencyKey`, so their external side effects may repeat
([AGENTS.md](../AGENTS.md), "4. Durability, retries, and completion commits").

**Avoid a bucket older than Temporal (T_s < T_t).** It fails explicitly
rather than silently: objects written after T_s are missing, and the tasks
that read them fail with an input-unavailable reason; archives written after
T_s are written again by the archive sweep from Temporal's record.

## The archive-sweep risk

A finished run with no archive is lost if object storage stays down until the
cluster's delete time for that run, which is fixed when the run closes
([AGENTS.md](../AGENTS.md), "15. Product invariants"). The archive sweep
reports that risk on every pass, and these alerts carry it:
`OrkStackArchiveUnconfirmed`, `OrkStackArchiveLossImminent`,
`OrkStackArchiveSweepBlind` and `OrkStackRunLost`.

1. Restore object storage: nothing else lowers the risk. The retention
   buffer helps only runs that close later.
2. Confirm the sweep has caught up: the unconfirmed count is present and 0,
   and its last success is recent (kit), and the engine logs no
   `finished runs are not confirmed archived`.
3. For each run the engine logged as lost, export what is left of its
   evidence with `orkctl run evidence export RUN` while the cluster still
   holds it.

## Alerts

<!-- BEGIN GENERATED ALERTS: go run ./internal/alerts/cmd/genalerts -->

## Telemetry pipeline and liveness

### OrkStackWatchdog

Severity none (always firing); fires at once. Prometheus is evaluating OrkStack's alert rules.

- **What it means:** Prometheus is up and evaluating these rules; the alert is meant to fire for ever.
- **Impact:** None while it fires. When it stops, no other alert can reach you.
- **Confirm:** kit: the external dead man's switch shows the Watchdog arriving.
- **Fix:** Nothing while it fires. When the switch reports it missing, restore Prometheus or its Alertmanager.

### OrkStackTelemetryPipelineDown

Severity critical; fires after 2m. The OpenTelemetry Collector reports nothing.

- **What it means:** The Collector that receives OrkStack's OTLP is stopped, or its own metrics no longer reach Prometheus.
- **Impact:** Every metric-based alert is blind. OrkStack itself keeps running: telemetry never blocks a task or a run.
- **Confirm:** kit: the Collector's process and its logs. `orkctl admin info` still answers, which shows the engine is up.
- **Fix:** Start the Collector again; check its memory limit and its exporter to Prometheus. Nothing in OrkStack needs a restart.
- **Related:** [OrkStackEngineAllDown](#orkstackenginealldown), [OrkStackTelemetryDropping](#orkstacktelemetrydropping)

### OrkStackTelemetryDropping

Severity warning; fires after 10m. The OpenTelemetry Collector is dropping telemetry.

- **What it means:** A backend refuses what the Collector sends (a rate limit such as Loki's 429, or colliding series names), or the Collector refuses what it receives (its memory limit).
- **Impact:** Dashboards and alerts miss data; task logs exported through the Collector may be incomplete. Runs are unaffected.
- **Confirm:** kit: `otelcol_exporter_send_failed_*`, `otelcol_exporter_enqueue_failed_*` and `otelcol_receiver_refused_*` series say which signal and which exporter.
- **Fix:** Raise the refusing backend's limits or the Collector's queue and memory limit; for a name collision, find the two sources sending one name.
- **Related:** [OrkStackTelemetryPipelineDown](#orkstacktelemetrypipelinedown), [OrkStackTelemetryExportErrors](#orkstacktelemetryexporterrors)

### OrkStackTelemetryExportErrors

Severity warning; fires at once. An OrkStack process failed to export telemetry.

- **What it means:** A process's OTLP exporter could not reach its collector, or the collector refused the export. The process logs it and counts it, and never retries in line.
- **Impact:** The process's telemetry for that window is missing. Nothing it runs is affected.
- **Confirm:** the engine or Orker log line `telemetry export failed`; kit: `ork_telemetry_export_errors_total` by signal.
- **Fix:** Check the process's `OTEL_EXPORTER_OTLP_*` endpoint and credentials, and the Collector's receiver.
- **Related:** [OrkStackTelemetryPipelineDown](#orkstacktelemetrypipelinedown), [OrkStackTelemetryDropping](#orkstacktelemetrydropping)

### OrkStackTelemetryRecordsDropped

Severity warning; fires at once. An engine dropped telemetry for a namespace.

- **What it means:** A namespace's telemetry policy names a sink this process does not declare (`sink_unknown`), or its records could not be routed (`unrouted`, `lookup_failed`, `instruments_failed`). Such records are dropped and counted, never sent to the default sink. A namespace whose policy is still loading (`pending`) is not alerted.
- **Impact:** That namespace's runs report nothing to its sink; the operator twins on the default sink still count them.
- **Confirm:** `orkctl namespace get NS` shows the namespace's telemetry policy; compare its sink with the deployment's `ORK_TELEMETRY_SINKS`.
- **Fix:** Declare the sink on every engine and Orker, or change the namespace's policy to a sink they declare.
- **Related:** [OrkStackNamespacesNotExported](#orkstacknamespacesnotexported)

### OrkStackEngineAllDown

Severity critical; fires after 2m. No OrkStack engine reports metrics.

- **What it means:** Every engine process is down, or every one has lost its route to the Collector. The rule is quiet while the pipeline is down, which TelemetryPipelineDown reports instead.
- **Impact:** If the engines are down, nothing can be submitted and nothing new is dispatched; running containers carry on and commit. If only their telemetry is cut, nothing is affected but what you can see.
- **Confirm:** `orkctl admin info` answers when an engine is up: then the engines are healthy and only their telemetry is cut.
- **Fix:** Start the engines, or restore their OTLP endpoint. An engine whose `orkctl admin info` answers is not down.
- **Related:** [OrkStackTelemetryPipelineDown](#orkstacktelemetrypipelinedown), [OrkStackTelemetryExportErrors](#orkstacktelemetryexporterrors)

### OrkStackMixedReleases

Severity info; fires after 2h. Processes of more than one release are running.

- **What it means:** A release rollout is in progress, or one was left half done.
- **Impact:** None by itself: a run keeps the release it started on. A rollout left half done keeps old releases' processes running.
- **Confirm:** `orkctl release status` shows the current, ramping and previous releases and whether each is drained.
- **Fix:** Finish the rollout (`orkctl release promote`) or roll it back; stop a previous release's processes only once `orkctl release wait` reports it drained.

### OrkStackNamespacesNotExported

Severity info; fires after 1h. Some namespaces export no metrics.

- **What it means:** A namespace's telemetry policy selects a named sink with metrics off. Its runs are still counted by the operator twins on the default sink.
- **Impact:** The per-namespace rules in this file are blind to those namespaces.
- **Confirm:** `orkctl namespace get NS` shows each namespace's telemetry policy.
- **Fix:** Nothing, if that was the intent. Otherwise give the sink metrics, or point the namespace at one that has them.
- **Related:** [OrkStackTelemetryRecordsDropped](#orkstacktelemetryrecordsdropped)

### OrkStackMetricOverflow

Severity info; fires after 1h. An OrkStack metric hit its cardinality limit.

- **What it means:** An instrument recorded more distinct label sets than `ORK_TELEMETRY_METRIC_CARDINALITY_LIMIT` allows, usually from high-cardinality labels turned on.
- **Impact:** The folded series lose their labels; totals are still right.
- **Confirm:** kit: `{otel_metric_overflow="true"}` names the instrument.
- **Fix:** Turn off `ORK_TELEMETRY_HIGH_CARDINALITY` or object labels, or raise the limit knowing the cost.

### OrkStackCensusStale

Severity info; fires after 15m. An engine census has not succeeded for 10 minutes.

- **What it means:** A census reads the cluster's index (open runs, schedules, pool backlogs, pending webhook deliveries, telemetry policies) on a budget. It fails while Temporal's visibility is unavailable. Its 10 minutes are twice the longest census interval the settings allow (`ORK_POOL_METRICS_INTERVAL` is at most 5 minutes).
- **Impact:** The gauges the census reports stop moving, and the alerts that read them (pool backlogs, schedules, webhook deliveries) are blind.
- **Confirm:** the engine log names the failing census; kit: `ork_census_last_success_seconds` by `ork_census`.
- **Fix:** Restore Temporal's visibility; see Temporal outages above. If it is skipped rather than failing, see CensusSkipped.
- **Related:** [OrkStackCensusSkipped](#orkstackcensusskipped), [temporal-outages](#temporal-outages)

### OrkStackCensusSkipped

Severity info; fires after 30m. An engine census is skipping passes.

- **What it means:** The censuses share a budget of requests per minute to the cluster (`ORK_TELEMETRY_CENSUS_REQUESTS_PER_MINUTE`); a large deployment can spend it.
- **Impact:** The census's gauges update less often.
- **Confirm:** kit: `ork_census_skipped_total` by `ork_census`.
- **Fix:** Raise the census budget if the cluster can take it, or accept slower gauges.
- **Related:** [OrkStackCensusStale](#orkstackcensusstale)

## Pools, Orkers and usage

### OrkStackPoolBacklogNoOrkers

Severity critical; fires after 10m. A pool has a backlog and no Orkers.

- **What it means:** Tasks are queued on a pool that no Orker serves: none was started for it, every one stopped, or every one lost the cluster.
- **Impact:** Its tasks wait, explicitly, bounded only by their runs' own timeouts. Nothing is lost.
- **Confirm:** `orkctl pool list` shows the pool with 0 live Orkers (not `unknown (unreachable)`, which is a Temporal outage).
- **Fix:** Start Orkers for the pool, with the release the waiting runs were started on.
- **Related:** [OrkStackPoolNotPolling](#orkstackpoolnotpolling), [OrkStackPoolLostAllOrkers](#orkstackpoollostallorkers), [OrkStackPoolBacklogGrowing](#orkstackpoolbackloggrowing)

### OrkStackPoolNotPolling

Severity warning; fires after 15m. A pool's Orkers are not taking its backlog.

- **What it means:** The pool's Orkers are all saturated (an Orker whose every slot is taken stops polling), all cordoned, or cannot reach Temporal's matching service.
- **Impact:** Its tasks wait. Nothing is lost.
- **Confirm:** `orkctl orker list` shows each Orker's pool, running tasks and cordon state.
- **Fix:** Saturated: add Orkers or capacity. Cordoned by an operator: `orkctl orker uncordon ID`. Neither: Temporal's matching service (Temporal outages, above).
- **Related:** [OrkStackPoolBacklogNoOrkers](#orkstackpoolbacklognoorkers), [OrkStackOrkerSaturated](#orkstackorkersaturated), [OrkStackOrkerCordonedLong](#orkstackorkercordonedlong)

### OrkStackPoolLostAllOrkers

Severity warning; fires after 5m. A pool lost every Orker.

- **What it means:** Every Orker of the pool stopped, or every one lost both the cluster and its telemetry.
- **Impact:** New tasks on the pool wait; a pinned task whose host was lost fails `PIN_HOST_LOST`.
- **Confirm:** `orkctl pool list` shows the pool with 0 live Orkers.
- **Fix:** Start Orkers for the pool again; find why they stopped in their own logs.
- **Related:** [OrkStackPoolBacklogNoOrkers](#orkstackpoolbacklognoorkers)

### OrkStackOrkersNotExporting

Severity warning; fires after 15m. A pool's Orkers poll but report no metrics.

- **What it means:** The pool's Orkers run without a metrics exporter, or cannot reach the Collector.
- **Impact:** The Orker-side alerts (saturation, store outages, attempt records) are blind for this pool. Its tasks run.
- **Confirm:** `orkctl orker list` lists the pool's Orkers.
- **Fix:** Give the Orkers the same `OTEL_*` settings as the engines.
- **Related:** [OrkStackTelemetryExportErrors](#orkstacktelemetryexporterrors)

### OrkStackPoolBacklogGrowing

Severity warning; fires after 30m. A pool's backlog keeps growing while it has pollers.

- **What it means:** The pool is too small for its load, or its waiting tasks belong to an older release whose Orkers were stopped before it drained. A run keeps its release, and its tasks wait for that release's Orkers; Orkers of another release never take them. Pollers are counted across releases, so this stall never raises PoolBacklogNoOrkers. It cannot happen with releases off (`ORK_BUILD_ID` unset).
- **Impact:** Tasks wait longer and longer.
- **Confirm:** `orkctl pool list` for the backlog; `orkctl release status` shows a release that is not drained.
- **Fix:** Too small: add Orkers. A release not drained: start its Orkers again with the same `ORK_BUILD_ID`, serving the same pool, and stop them only after `orkctl release wait` reports it drained.
- **Related:** [OrkStackPoolNotPolling](#orkstackpoolnotpolling), [OrkStackTasksWaiting](#orkstacktaskswaiting), [OrkStackMixedReleases](#orkstackmixedreleases)

### OrkStackPoolBacklogUnmeasured

Severity info; fires after 15m. A pool's backlog is not measured.

- **What it means:** The engines' pool-backlog census is failing or skipped for this pool.
- **Impact:** The backlog alerts are blind for this pool.
- **Confirm:** kit: `ork_pool_backlog_tasks` has no series for the pool; `orkctl pool list` still lists it.
- **Fix:** See CensusStale and CensusSkipped.
- **Related:** [OrkStackCensusStale](#orkstackcensusstale), [OrkStackCensusSkipped](#orkstackcensusskipped)

### OrkStackOrkerStoreOutage

Severity critical; fires after 2m. An Orker cannot reach object storage.

- **What it means:** The Orker's consecutive object-storage checks failed, so it takes no new task until consecutive good ones; running tasks carry on.
- **Impact:** The Orker takes no new work; tasks that need the store to finish wait for it.
- **Confirm:** `orkctl orker get ID` shows the store-outage cordon.
- **Fix:** Restore the Orker's route or credentials to the store; it uncordons itself after consecutive good checks.
- **Related:** [OrkStackObjectStoreErrors](#orkstackobjectstoreerrors)

### OrkStackOrkerCordonedLong

Severity info; fires after 6h. An Orker has been cordoned by an operator for 6 hours.

- **What it means:** An operator's cordon is still set, perhaps forgotten after maintenance.
- **Impact:** The Orker takes no new task; its pool has less capacity.
- **Confirm:** `orkctl orker list` shows the cordon and its cause.
- **Fix:** `orkctl orker uncordon ID` when the maintenance is over, or stop the Orker.
- **Related:** [OrkStackPoolNotPolling](#orkstackpoolnotpolling)

### OrkStackOrkerSaturated

Severity warning; fires after 30m. An Orker has run at its task limit for 30 minutes.

- **What it means:** The Orker is full; while full it stops polling for new tasks.
- **Impact:** Its pool's other tasks wait for a slot.
- **Confirm:** `orkctl orker list` shows its running tasks against its limit.
- **Fix:** Add Orkers to the pool, or raise `ORK_ORKER_MAX_CONCURRENT` where the host has room.
- **Related:** [OrkStackPoolNotPolling](#orkstackpoolnotpolling), [OrkStackTasksWaiting](#orkstacktaskswaiting)

### OrkStackTasksWaiting

Severity warning; fires after 15m. Tasks wait over 5 minutes for an Orker.

- **What it means:** Tasks are scheduled faster than the pool's Orkers take them.
- **Impact:** Runs take longer than their tasks do.
- **Confirm:** `orkctl pool list` shows the pool's backlog and live Orkers.
- **Fix:** Add Orkers or capacity to the pool.
- **Related:** [OrkStackPoolBacklogGrowing](#orkstackpoolbackloggrowing), [OrkStackOrkerSaturated](#orkstackorkersaturated)

### OrkStackHeartbeatLosses

Severity warning; fires at once. An Orker lost its heartbeat to Temporal.

- **What it means:** An Orker lost the cluster for longer than the heartbeat timeout and stopped its containers, and reported the attempts `ORKER_HEARTBEAT_LOST`. It sees only attempts the Orker reports: an attempt of a run whose pinned interruption class covers the heartbeat timeout reports nothing, because the cluster's own timeout ends it and the run counts it as an interruption, so this alert is blind to those.
- **Impact:** Work in those attempts is repeated; external side effects may repeat under the same idempotency key.
- **Confirm:** `orkctl stats failures` shows `ORKER_HEARTBEAT_LOST` by pool.
- **Fix:** Find what cut the Orker from Temporal: a Temporal outage (Temporal outages, above), the Orker's network, or a host under such load it could not heartbeat.
- **Related:** [OrkStackTemporalUnreachable](#orkstacktemporalunreachable), [temporal-outages](#temporal-outages)

### OrkStackOOMKills

Severity info; fires at once. A task's container was stopped at its memory limit.

- **What it means:** A task used all the memory it requested; the kernel stopped its container.
- **Impact:** The attempt failed and is retried under the task's policy, likely to fail the same way.
- **Confirm:** `orkctl run attempts RUN -t TASK` shows the out-of-memory attempt; `orkctl stats usage` shows memory peaks.
- **Fix:** Raise the task's `resources.memory`, or make it use less.

### OrkStackUsageMostlyUnmeasured

Severity info; fires after 6h. Most attempts' CPU and memory are not measured.

- **What it means:** The Orkers cannot read their containers' cgroups (rootless or cgroup v1 hosts, a runtime that hides them), or most tasks end before the first sample.
- **Impact:** Measured usage in `orkctl stats usage` and on the run page is mostly missing. Budgets are unaffected: they count reservations.
- **Confirm:** `orkctl stats usage` shows the unmeasured attempts and why.
- **Fix:** Run the Orkers where they can read cgroup v2, or accept unmeasured usage.

### OrkStackAttemptRecordsFailing

Severity warning; fires after 15m. Orkers fail to write attempt records.

- **What it means:** Each attempt's immutable record could not be written. The attempt's own result is unaffected.
- **Impact:** `orkctl run attempts` lacks those attempts' detail.
- **Confirm:** `orkctl run attempts RUN` for a recent run of the pool shows attempts with no record.
- **Fix:** Check the Orkers' object-storage access (ObjectStoreErrors).
- **Related:** [OrkStackObjectStoreErrors](#orkstackobjectstoreerrors), [OrkStackOrkerStoreOutage](#orkstackorkerstoreoutage)

### OrkStackMemoWritesFailing

Severity warning; fires after 15m. Cached tasks' memos are not being written.

- **What it means:** After a miss, the Orker could not write the memo; the task still succeeded.
- **Impact:** The next run of each such task misses again and runs its container.
- **Confirm:** `orkctl cache stats` shows hit rates falling.
- **Fix:** Check the Orkers' object-storage access (ObjectStoreErrors).
- **Related:** [OrkStackObjectStoreErrors](#orkstackobjectstoreerrors)

## Runs, admission and integrity

### OrkStackRunFailureRatioHigh

Severity warning; fires after 15m. Over a fifth of a namespace's runs fail.

- **What it means:** Many runs of the namespace end FAILED: a broken Orkflow or image, a dependency they call, or the platform under them.
- **Impact:** The namespace's work is not getting done.
- **Confirm:** `orkctl stats failures -n NS` shows the failing tasks and their reasons.
- **Fix:** Follow the top reason: a task's own failures go to its owner; infrastructure reasons to the alerts beside this one.
- **Related:** [OrkStackInfrastructureErrors](#orkstackinfrastructureerrors), [OrkStackIntegrityFailure](#orkstackintegrityfailure)

### OrkStackInfrastructureErrors

Severity warning; fires after 15m. Tasks fail for infrastructure reasons.

- **What it means:** Tasks fail for reasons that are not the workload's: image pulls, the object store, the container runtime, a lost Orker.
- **Impact:** Runs fail or retry for reasons their authors cannot fix.
- **Confirm:** `orkctl stats failures -n NS` shows the reasons.
- **Fix:** Follow the reason: `IMAGE_PULL_FAILED` is the registry, `ARTIFACT_UPLOAD_FAILED` and `COMMIT_*` the object store, `RUNTIME_*` the Orker's host.
- **Related:** [OrkStackObjectStoreErrors](#orkstackobjectstoreerrors), [OrkStackHeartbeatLosses](#orkstackheartbeatlosses)

### OrkStackIntegrityFailure

Severity critical; fires at once. Stored data is not what was written.

- **What it means:** An input artifact or stored value failed its hash, a commit is unreadable or not this task's, the object store does not honour conditional writes, or the Orker hit a bug.
- **Impact:** The task fails explicitly rather than use damaged data. Treat it as data damage until you know otherwise.
- **Confirm:** `orkctl stats failures` shows the reason and its runs; `orkctl run get RUN` the task.
- **Fix:** Export the run's evidence (`orkctl run evidence export RUN`) and check the store and its versioning. Never edit or delete records to get past it.
- **Related:** [OrkStackObjectStoreErrors](#orkstackobjectstoreerrors)

### OrkStackRunStateFailed

Severity critical; fires at once. A run could not load its saved state.

- **What it means:** The saved state a long run carries forward was missing, corrupt or of a format this release does not read.
- **Impact:** The run ended FAILED, explicitly, with its terminal event and archive.
- **Confirm:** `orkctl stats failures` shows the `RUN_STATE_*` failures and their runs.
- **Fix:** `RUN_STATE_UNAVAILABLE`: the object store; `RUN_STATE_CORRUPT`: data damage, as IntegrityFailure; `RUN_STATE_UNSUPPORTED`: a rollback below the release that wrote it.
- **Related:** [OrkStackContinuationWriteFailed](#orkstackcontinuationwritefailed), [OrkStackIntegrityFailure](#orkstackintegrityfailure)

### OrkStackContinuationWriteFailed

Severity warning; fires at once. A long run could not write its saved state.

- **What it means:** Object storage refused the run's saved state.
- **Impact:** The run keeps growing its history; if the store stays down it may fail `RUN_HISTORY_LIMIT`.
- **Confirm:** the engine log names the run; kit: `ork_deployment_run_continuations_total{outcome="write_failed"}`.
- **Fix:** Restore object storage (ObjectStoreErrors).
- **Related:** [OrkStackObjectStoreErrors](#orkstackobjectstoreerrors)

### OrkStackAdmissionRefusing

Severity info; fires after 15m. A namespace refuses submissions.

- **What it means:** The namespace's hold, its `max_running` or its enforced budget refuses new runs, as set.
- **Impact:** Callers get refusals; nothing already running is touched.
- **Confirm:** `orkctl namespace get NS` shows the hold, `max_running` and budget.
- **Fix:** Nothing, if intended. Otherwise release the hold, raise the bound or the budget.
- **Related:** [OrkStackBudgetUnmeasurable](#orkstackbudgetunmeasurable)

### OrkStackBudgetUnmeasurable

Severity warning; fires at once. A budget could not be measured, so submissions were refused.

- **What it means:** An enforced budget fails closed: when the engine cannot read the namespace's usage, it refuses.
- **Impact:** Submissions to namespaces with enforced budgets are refused until usage reads again.
- **Confirm:** `orkctl stats usage -n NS` fails or is incomplete.
- **Fix:** Restore what usage reads: Temporal's visibility and the object store.
- **Related:** [OrkStackAdmissionRefusing](#orkstackadmissionrefusing), [OrkStackCensusStale](#orkstackcensusstale)

## Archive, object store, API and identity

### OrkStackArchiveUnconfirmed

Severity warning; fires after 30m. Finished runs are not confirmed archived.

- **What it means:** Usually object storage is down or refuses the sweep, so finished runs' archives cannot be written or checked.
- **Impact:** Those runs are readable from Temporal for now; one still unarchived when the cluster deletes it is lost. See the archive-sweep risk, above.
- **Confirm:** the engine log line `finished runs are not confirmed archived`; kit: `ork_archive_sweep_unconfirmed`.
- **Fix:** Restore object storage; the sweep archives the runs on its next clean pass.
- **Related:** [OrkStackArchiveLossImminent](#orkstackarchivelossimminent), [OrkStackArchiveSweepBlind](#orkstackarchivesweepblind), [OrkStackRunLost](#orkstackrunlost), [the-archive-sweep-risk](#the-archive-sweep-risk)

### OrkStackArchiveLossImminent

Severity critical; fires at once. An unarchived run expires within a day.

- **What it means:** The first unarchived run expires soon: when the cluster deletes it before its archive is written, it is lost. An undated run counts as imminent.
- **Impact:** Runs may be lost: the one stated exception to an accepted run never disappearing.
- **Confirm:** the engine log names the runs at risk; `orkctl run get RUN` for each.
- **Fix:** Restore object storage now; export the evidence of runs that matter while the cluster still holds them (`orkctl run evidence export RUN`).
- **Related:** [OrkStackArchiveUnconfirmed](#orkstackarchiveunconfirmed), [OrkStackRunLost](#orkstackrunlost), [the-archive-sweep-risk](#the-archive-sweep-risk)

### OrkStackArchiveSweepBlind

Severity warning; fires after 10m. The archive sweep cannot see every finished run.

- **What it means:** The sweep's own counts are not trustworthy: a run at risk may not be counted.
- **Impact:** ArchiveUnconfirmed and ArchiveLossImminent may be quiet when they should not be.
- **Confirm:** the engine log of the sweep's passes; kit: `ork_archive_sweep_last_success_seconds`, `ork_archive_sweep_list_failed` and `ork_archive_sweep_interval_too_long`.
- **Fix:** List failed: Temporal's visibility. Interval too long: shorten `ORK_CLEANUP_INTERVAL`. Stale: restore object storage and Temporal.
- **Related:** [OrkStackArchiveUnconfirmed](#orkstackarchiveunconfirmed), [the-archive-sweep-risk](#the-archive-sweep-risk)

### OrkStackRunLost

Severity critical; fires at once. A finished run was lost before it was archived.

- **What it means:** Object storage was down until the cluster's delete time for a finished run: the run is gone.
- **Impact:** The run's record is lost; its committed objects may remain in the store.
- **Confirm:** the engine log line `run lost` names it.
- **Fix:** Restore object storage, then follow the archive-sweep risk, above, for the runs still at risk.
- **Related:** [OrkStackArchiveLossImminent](#orkstackarchivelossimminent), [the-archive-sweep-risk](#the-archive-sweep-risk)

### OrkStackAuditRecordsDropped

Severity critical; fires at once. Control-plane audit records were dropped.

- **What it means:** The `metadata/audit/` prefix of the bucket is unwritable; the audited RPCs themselves still answered.
- **Impact:** Changes to the deployment go unrecorded.
- **Confirm:** `orkctl admin audit` lacks the recent writes.
- **Fix:** Restore write access to the bucket's audit prefix.
- **Related:** [OrkStackObjectStoreErrors](#orkstackobjectstoreerrors)

### OrkStackObjectStoreErrors

Severity warning; fires after 10m. Object-store requests fail.

- **What it means:** The object store errors or is unreachable from the engines or Orkers.
- **Impact:** Tasks cannot read inputs or commit, archives wait, cleanup holds; Orkers cordon themselves after consecutive failed checks.
- **Confirm:** `orkctl orker list` shows Orkers cordoned for a store outage; kit for the error rate.
- **Fix:** Restore the store, its credentials or the route to it.
- **Related:** [OrkStackOrkerStoreOutage](#orkstackorkerstoreoutage), [OrkStackArchiveUnconfirmed](#orkstackarchiveunconfirmed), [OrkStackObjectStoreSlow](#orkstackobjectstoreslow)

### OrkStackObjectStoreSlow

Severity warning; fires after 15m. Object-store requests are slow.

- **What it means:** The store is overloaded or far away.
- **Impact:** Everything that reads or writes it slows: commits, archives, the run page.
- **Confirm:** kit: `ork_store_request_duration_seconds` by operation and family.
- **Fix:** Check the store's own load and the network to it.
- **Related:** [OrkStackObjectStoreErrors](#orkstackobjectstoreerrors)

### OrkStackStoreClockSkew

Severity warning; fires after 15m. An engine's clock differs from the object store's.

- **What it means:** The artifact GC ages objects by the store's clock and its own; past this skew it holds rather than risk an early delete.
- **Impact:** The artifact GC holds (`clock_skew`) and reclaims nothing.
- **Confirm:** `orkctl admin gc preview` shows the hold reason `clock_skew`; check the hosts' NTP.
- **Fix:** Fix time synchronisation on the engine hosts or the store.
- **Related:** [OrkStackArtifactGCHeld](#orkstackartifactgcheld)

### OrkStackApiErrors

Severity warning; fires after 10m. The API answers internal errors.

- **What it means:** The engine fails requests for reasons that are its own, not the caller's.
- **Impact:** Clients, the UI and `orkctl` see errors.
- **Confirm:** `orkctl admin info` answers; the engine log has the failing calls.
- **Fix:** Read the engine log for the failing RPC; its dependencies (Temporal, the store) are the usual cause.
- **Related:** [OrkStackApiUnavailable](#orkstackapiunavailable), [OrkStackTemporalUnreachable](#orkstacktemporalunreachable)

### OrkStackApiUnavailable

Severity warning; fires after 5m. An API method answers unavailable.

- **What it means:** A dependency of the method (usually Temporal) is unreachable or rate-limits the engine.
- **Impact:** Callers must retry; a submit retried with its idempotency key is safe.
- **Confirm:** `orkctl run list` fails `Unavailable`.
- **Fix:** See Temporal outages, above.
- **Related:** [OrkStackTemporalUnreachable](#orkstacktemporalunreachable), [OrkStackTemporalRateLimited](#orkstacktemporalratelimited), [temporal-outages](#temporal-outages)

### OrkStackIdentityKeysStale

Severity warning; fires at once. An engine's copy of the signing keys is stale.

- **What it means:** The engine could not refresh the published keys from object storage.
- **Impact:** Signed decisions, such as a force cancel, are refused by that engine.
- **Confirm:** the engine log of its key refreshes; kit: `ork_identity_keys_copy_stale_total`.
- **Fix:** Restore the engine's read access to the store.
- **Related:** [OrkStackObjectStoreErrors](#orkstackobjectstoreerrors)

## Temporal

### OrkStackTemporalUnreachable

Severity critical; fires after 2m. Most requests to Temporal fail.

- **What it means:** Temporal is down or unreachable from OrkStack.
- **Impact:** Nothing new starts; running containers carry on until the heartbeat timeout. See Temporal outages, above.
- **Confirm:** `orkctl pool list` shows `unknown (unreachable)`.
- **Fix:** Restore Temporal or the route to it; nothing in OrkStack needs repair. Follow the recovery checks in Temporal outages, above.
- **Related:** [OrkStackTemporalPollFailures](#orkstacktemporalpollfailures), [OrkStackHeartbeatLosses](#orkstackheartbeatlosses), [temporal-outages](#temporal-outages)

### OrkStackTemporalPollFailures

Severity warning; fires after 5m. Polls of Temporal fail.

- **What it means:** Engines and Orkers cannot poll for work: Temporal or its matching service is unreachable.
- **Impact:** Nothing is dispatched while it lasts.
- **Confirm:** `orkctl pool list`
- **Fix:** See Temporal outages, above.
- **Related:** [OrkStackTemporalUnreachable](#orkstacktemporalunreachable), [temporal-outages](#temporal-outages)

### OrkStackTemporalRateLimited

Severity warning; fires after 5m. Temporal rate-limits OrkStack.

- **What it means:** The cluster's namespace or frontend limits are lower than OrkStack's load.
- **Impact:** Submits and other calls answer `ResourceExhausted`; callers retry.
- **Confirm:** `orkctl run submit` answers `ResourceExhausted`.
- **Fix:** Raise the Temporal namespace's or frontend's rate limits, or lower the load.
- **Related:** [OrkStackApiUnavailable](#orkstackapiunavailable)

## Schedules, webhooks and triggers

### OrkStackScheduleFiresFailing

Severity warning; fires at once. A schedule's fires start no runs.

- **What it means:** Each fire was refused (the Orkflow is gone, its inputs no longer validate, the namespace is held) or found the engine unavailable.
- **Impact:** The schedule's work is not done.
- **Confirm:** `orkctl schedule get NAME` shows its recent fires and why each was refused.
- **Fix:** Fix the refusal reason: restore the Orkflow version, fix the schedule's input, or release the hold. `unavailable` is Temporal.
- **Related:** [OrkStackAdmissionRefusing](#orkstackadmissionrefusing), [OrkStackTemporalUnreachable](#orkstacktemporalunreachable)

### OrkStackScheduleFiresSkipped

Severity info; fires at once. A schedule skips fires while its last run is still running.

- **What it means:** Its runs take longer than its interval, and its overlap policy skips a fire while one runs.
- **Impact:** Fewer runs than the schedule declares.
- **Confirm:** `orkctl schedule get NAME` shows the skipped fires.
- **Fix:** Nothing, if intended. Otherwise lengthen the interval, speed the runs up, or change `overlap:`.

### OrkStackScheduleFiresMissed

Severity warning; fires at once. A schedule missed fires past its catch-up window.

- **What it means:** The cluster could not fire on time (an outage) for longer than the schedule's `catchup_window`.
- **Impact:** Those fires never run.
- **Confirm:** `orkctl schedule get NAME` shows the missed fires.
- **Fix:** Backfill the missed range if it matters (`orkctl schedule backfill`); widen `catchup_window` for the next outage.
- **Related:** [OrkStackTemporalUnreachable](#orkstacktemporalunreachable)

### OrkStackScheduleFireLate

Severity warning; fires after 15m. Schedules fire late.

- **What it means:** Fires reach the engine well after their time: the cluster or the engine is slow.
- **Impact:** Scheduled runs start late.
- **Confirm:** `orkctl schedule list` shows each schedule's last and next fire.
- **Fix:** Check Temporal's load and the engines' (TemporalRateLimited, ApiUnavailable).
- **Related:** [OrkStackTemporalRateLimited](#orkstacktemporalratelimited)

### OrkStackScheduleOverdue

Severity warning; fires after 10m. A schedule's next fire is overdue.

- **What it means:** The cluster is not firing the schedule.
- **Impact:** Its runs do not start.
- **Confirm:** `orkctl schedule get NAME` shows its next fire in the past.
- **Fix:** See Temporal outages, above; if the cluster is healthy, recreate the schedule with `orkctl automation apply`.
- **Related:** [OrkStackScheduleFiresMissed](#orkstackschedulefiresmissed), [temporal-outages](#temporal-outages)

### OrkStackScheduleCensusPartial

Severity info; fires after 15m. The schedule census reads only some schedules.

- **What it means:** The census ran out of its budget or could not read the cluster's schedules.
- **Impact:** ScheduleOverdue is blind to the schedules it did not read.
- **Confirm:** `orkctl schedule list` answers for every schedule.
- **Fix:** See CensusSkipped and CensusStale.
- **Related:** [OrkStackCensusSkipped](#orkstackcensusskipped), [OrkStackCensusStale](#orkstackcensusstale)

### OrkStackWebhookFailing

Severity warning; fires after 30m. A webhook's deliveries all fail.

- **What it means:** The receiver refuses or does not answer, past every retry.
- **Impact:** Its subscribers miss run events.
- **Confirm:** `orkctl webhook deliveries ID` shows each failed delivery's attempts and answers.
- **Fix:** Fix the receiver, then `orkctl webhook redeliver` what it missed.
- **Related:** [OrkStackWebhookDestinationBlocked](#orkstackwebhookdestinationblocked), [OrkStackWebhookDeliveriesStuck](#orkstackwebhookdeliveriesstuck)

### OrkStackWebhookDeliveriesPending

Severity warning; fires after 10m. Webhook deliveries have been pending over 5 minutes.

- **What it means:** Deliveries are retrying against slow or failing receivers, or the engine cannot run them.
- **Impact:** Subscribers hear of run events late.
- **Confirm:** `orkctl webhook deliveries ID` shows the pending deliveries.
- **Fix:** Check the receivers (WebhookFailing) and Temporal.
- **Related:** [OrkStackWebhookDeliveriesStuck](#orkstackwebhookdeliveriesstuck), [OrkStackWebhookFailing](#orkstackwebhookfailing)

### OrkStackWebhookDeliveriesStuck

Severity critical; fires at once. Webhook deliveries have been pending over an hour.

- **What it means:** Deliveries are stuck: their workflows are not progressing.
- **Impact:** Subscribers miss run events until they move.
- **Confirm:** `orkctl webhook deliveries ID` shows the oldest pending deliveries.
- **Fix:** Check Temporal and the engines; once they are healthy, the deliveries resume.
- **Related:** [OrkStackWebhookDeliveriesPending](#orkstackwebhookdeliveriespending), [OrkStackTemporalUnreachable](#orkstacktemporalunreachable)

### OrkStackWebhookDestinationBlocked

Severity info; fires at once. A webhook's destination is blocked by the network policy.

- **What it means:** The receiver's address is outside `webhooks.allowed_networks`, or is a private address the policy refuses.
- **Impact:** The webhook never delivers until its URL or the policy changes.
- **Confirm:** `orkctl webhook list` shows the webhook's URL; `orkctl webhook deliveries ID` the refusals.
- **Fix:** Point the webhook at an allowed address, or allow the network on purpose.
- **Related:** [OrkStackWebhookFailing](#orkstackwebhookfailing)

### OrkStackWebhookNotQueued

Severity warning; fires at once. Run events were not queued for webhook delivery.

- **What it means:** The engine could not encode or store a delivery's body, or could not start its delivery.
- **Impact:** Subscribers miss those events; nothing retries them.
- **Confirm:** `orkctl webhook deliveries ID` lacks them; kit: `ork_webhook_not_queued_total` by reason.
- **Fix:** `body_unstored`: the object store. `not_started`: Temporal. Then redeliver what was missed.
- **Related:** [OrkStackObjectStoreErrors](#orkstackobjectstoreerrors), [OrkStackTemporalUnreachable](#orkstacktemporalunreachable)

### OrkStackTriggerUnavailable

Severity warning; fires after 5m. HTTP triggers answer unavailable.

- **What it means:** The engine could not start the trigger's run: Temporal is unreachable.
- **Impact:** Callers' requests start nothing; a caller that retries is safe.
- **Confirm:** `orkctl trigger list` answers; a test call answers unavailable.
- **Fix:** See Temporal outages, above.
- **Related:** [OrkStackTemporalUnreachable](#orkstacktemporalunreachable), [temporal-outages](#temporal-outages)

## Retention cleanup and artifact GC

### OrkStackCleanupDisabled

Severity info; fires after 24h. Retention cleanup is off.

- **What it means:** `ORK_CLEANUP_ENABLED` is false: finished runs past retention are never retired, and the artifact GC never runs.
- **Impact:** The bucket only grows; the cluster's own backstop deletes executions at the retention buffer.
- **Confirm:** `orkctl admin gc preview` shows `cleanup_enabled` false.
- **Fix:** Nothing, if intended (during a restore it is). Otherwise turn it on.
- **Related:** [OrkStackCleanupNotReporting](#orkstackcleanupnotreporting)

### OrkStackCleanupNotReporting

Severity warning; fires after 10m. Retention cleanup has not completed a walk for three intervals.

- **What it means:** Cleanup's passes fail or never reach the end of the index: usually Temporal's visibility or the object store. It also fires while an engine's first walk since boot has not completed, as on a large index or after a restore, and while walks cannot read some run, since such a walk does not count.
- **Impact:** Runs past retention are not retired; past the retention buffer, the cluster's backstop deletes their executions, archived or not (see the archive-sweep risk, above).
- **Confirm:** the engine log line `artifact cleanup iteration completed` stops appearing; kit: `ork_cleanup_last_pass_seconds`.
- **Fix:** Restore what the engine log names. Read CleanupFailing beside it.
- **Related:** [OrkStackCleanupFailing](#orkstackcleanupfailing), [OrkStackCleanupBehindRetention](#orkstackcleanupbehindretention), [the-archive-sweep-risk](#the-archive-sweep-risk)

### OrkStackCleanupFailing

Severity warning; fires at once. Retention cleanup fails.

- **What it means:** A step of cleanup failed: reading the policy or the index, checking an archive, retiring or deleting.
- **Impact:** Runs due for retirement wait for a later pass.
- **Confirm:** the engine log names the failing step; kit: `ork_cleanup_errors_total` by `ork_cleanup_step`.
- **Fix:** Restore what the step needs: Temporal for the index, the object store for the rest.
- **Related:** [OrkStackCleanupNotReporting](#orkstackcleanupnotreporting)

### OrkStackCleanupBehindRetention

Severity warning; fires after 1h. Retention cleanup is far behind.

- **What it means:** Cleanup retires runs slower than they become due.
- **Impact:** Past the retention buffer, the cluster's backstop deletes executions, and an unarchived one is lost.
- **Confirm:** kit: `ork_deployment_cleanup_retention_lag_seconds` against `ork_cleanup_retention_buffer_seconds`.
- **Fix:** Shorten `ORK_CLEANUP_INTERVAL` or raise its batch; check CleanupFailing.
- **Related:** [OrkStackCleanupFailing](#orkstackcleanupfailing), [OrkStackArchiveUnconfirmed](#orkstackarchiveunconfirmed), [the-archive-sweep-risk](#the-archive-sweep-risk)

### OrkStackExecutionsMissing

Severity critical; fires at once. Runs are missing from Temporal before their retention ended.

- **What it means:** Temporal lost executions it should still hold: a restore to an older point, a namespace retention shorter than OrkStack's, or data loss. Cleanup holds those runs rather than retire them early.
- **Impact:** Those runs' live records are gone; their archives, if written, still answer. The artifact GC holds (`executions_missing`).
- **Confirm:** `orkctl admin info` shows the cluster; `orkctl admin gc preview` shows the hold `executions_missing`.
- **Fix:** If Temporal was restored, follow Backup and disaster recovery, above. Otherwise check the Temporal namespace's retention.
- **Related:** [OrkStackArtifactGCHeld](#orkstackartifactgcheld), [backup-and-disaster-recovery](#backup-and-disaster-recovery)

### OrkStackArtifactGCFailing

Severity warning; fires at once. The artifact GC fails.

- **What it means:** The GC could not list, check or delete what it reclaims.
- **Impact:** Space is not reclaimed; nothing is lost.
- **Confirm:** the engine log names the failure; kit: `ork_gc_passes_total{outcome="failed"}` and `ork_deployment_gc_objects_total{outcome="failed"}` by reason.
- **Fix:** Restore the object store or Temporal, whichever the engine log names.
- **Related:** [OrkStackArtifactGCNotReporting](#orkstackartifactgcnotreporting)

### OrkStackArtifactGCNotReporting

Severity warning; fires after 10m. The artifact GC has not made a pass for three intervals.

- **What it means:** The GC runs inside retention cleanup; cleanup is not reaching it, or no engine has made a pass since it booted.
- **Impact:** Space is not reclaimed.
- **Confirm:** `orkctl admin gc preview` shows the mode and that cleanup is on; kit: `ork_gc_last_pass_seconds`.
- **Fix:** See CleanupNotReporting.
- **Related:** [OrkStackCleanupNotReporting](#orkstackcleanupnotreporting)

### OrkStackArtifactGCNoProgress

Severity warning; fires after 1h. The artifact GC makes no progress.

- **What it means:** Passes run but stop before they finish a run: each is cut short by its budget, or fails on the same run.
- **Impact:** Space is not reclaimed.
- **Confirm:** kit: `ork_gc_due_runs` and `ork_gc_last_progress_seconds`; the engine log of each pass.
- **Fix:** Read the engine log for the run every pass stops at; raise the GC's batch if passes are only cut short.
- **Related:** [OrkStackArtifactGCFailing](#orkstackartifactgcfailing), [OrkStackArtifactGCBehind](#orkstackartifactgcbehind)

### OrkStackArtifactGCBehind

Severity warning; fires after 2h. The artifact GC is over 6 hours behind.

- **What it means:** Runs retire faster than the GC reclaims them.
- **Impact:** Space is reclaimed late.
- **Confirm:** kit: `ork_gc_lag_seconds` and `ork_gc_due_runs`.
- **Fix:** Raise the GC's batch (`ORK_CLEANUP_ARTIFACT_GC_BATCH`) or shorten the cleanup interval.
- **Related:** [OrkStackArtifactGCNoProgress](#orkstackartifactgcnoprogress)

### OrkStackArtifactGCHeld

Severity info; fires after 2h. The artifact GC is held.

- **What it means:** The GC refuses to delete while a condition it cannot verify stands. Waiting on the low-water mark is not a hold, and neither is a warming pass.
- **Impact:** Nothing is reclaimed; nothing is lost.
- **Confirm:** `orkctl admin gc preview` shows the hold reason; `orkctl release status` for a mixed fleet.
- **Fix:** `engines_lack_composed_artifacts`: finish the release rollout. `capabilities_unreadable`: Temporal. `lowwater_unknown`: a sweep that was not clean (the archive-sweep risk, above), Temporal's visibility, or a pass that clears itself. `clock_skew`: fix NTP (StoreClockSkew). `executions_missing`: ExecutionsMissing.
- **Related:** [OrkStackArtifactGCHeldLong](#orkstackartifactgcheldlong), [OrkStackStoreClockSkew](#orkstackstoreclockskew), [OrkStackExecutionsMissing](#orkstackexecutionsmissing)

### OrkStackArtifactGCHeldLong

Severity warning; fires after 24h. The artifact GC has been held for a day.

- **What it means:** A hold has stood for a day. Of the low-water causes, a pass mid-switch and a run with no creation time clear on a later pass; a hold that stands a day has a cause that needs an operator.
- **Impact:** Nothing is reclaimed for as long as it stands.
- **Confirm:** `orkctl admin gc preview` shows the hold reason.
- **Fix:** As ArtifactGCHeld.
- **Related:** [OrkStackArtifactGCHeld](#orkstackartifactgcheld)

### OrkStackArtifactGCLowWaterLag

Severity info; fires after 1h. A week-old run holds the artifact GC back.

- **What it means:** Not a fault: reclaim waits for every run created before a retirement to finish and be archived.
- **Impact:** Space from before that run's creation is reclaimed only when it ends.
- **Confirm:** `orkctl admin gc preview` names the oldest blocking run; then `orkctl run get RUN`.
- **Fix:** If the run is stuck, finish or cancel it; otherwise accept the lag, which ends when it does.

### OrkStackArtifactGCDryRun

Severity info; fires after 1h. The artifact GC has run in dry-run for a week with space to reclaim.

- **What it means:** Dry-run reports what delete mode would reclaim; it was left on.
- **Impact:** The space it reports is not reclaimed.
- **Confirm:** `orkctl admin gc preview` shows the mode and what it would delete.
- **Fix:** Review the preview, then set `ORK_CLEANUP_ARTIFACT_GC=delete`.

### OrkStackArtifactGCLateCommit

Severity info; fires at once. A commit appeared after a run retired; its objects are kept.

- **What it means:** An Orker committed for a run after the run retired. The GC keeps such objects for good: a counted leak, never a loss.
- **Impact:** A little space is never reclaimed.
- **Confirm:** `orkctl admin gc preview` shows objects kept for `late_commit`.
- **Fix:** Nothing is required. Frequent late commits mean Orkers that keep working long after their runs end.

<!-- END GENERATED ALERTS -->
