# Stats — where a dashboard's numbers come from

`StatsService.GetStats` answers one question: how is this namespace doing over
this window? It is the only read in OrkStack that aggregates, and it is built
so that aggregating cannot introduce a second version of the truth.

```bash
orkctl stats                       # the default namespace, the last 24 hours
orkctl -n payments stats           # one namespace
orkctl -n payments stats --since 6h
orkctl -n payments stats --since 2026-05-01T09:00:00Z
orkctl -n payments stats --json    # the same answer, for a machine
orkctl -n payments stats failures  # the window's failed runs, grouped (§6)
orkctl -n payments stats flakes    # the window's tasks that needed a retry (§7)
```

## 1. Two sources, and no third

| Field | Where it comes from | Over which runs |
|---|---|---|
| `runs_by_status["running"]`, `running[]` | the cluster's index of executions | created in the window and not finished |
| `runs_by_status["succeeded"/"failed"/"canceled"]` | the run archive's INDEX KEYS | created in the window and finished |
| `success_rate`, `success_rate_defined` | the same index keys | the same |
| `most_failing[]` | the same index keys | the same |
| `p50_duration`, `p95_duration`, `duration_sample_count` | the cluster's index of executions | the same |
| `duration_coverage_since` | the window, held against how long executions are kept | — |
| `pools[]` | the Orkers polling for each pool's work, live | — |

**There is no counter and no write path.** Every number is derived when it is
asked for. A stored counter would be a third record of facts two records
already hold, and the day it disagreed with them there would be no way to say
which was lying.

This is also why `StatsService` is not where a time-series dashboard's counters
come from: it answers "how is this namespace doing right now," derived on read
from the two sources above, and stores nothing between one answer and the
next. A counter you can graph over weeks — `ork.runs`, `ork.tasks`, and the
rest — comes from the telemetry pipeline instead (`docs/DEPLOY.md`,
"Lifecycle metrics"), which is the write path this service deliberately has
none of.

A submission refused by namespace admission (a hold, or `max_running`) is in
neither record — it never became a run — so it is reported nowhere here; it
is counted on `ork.runs.refused` in the telemetry pipeline instead.

**Nothing is read from an engine's memory.** Both sources are shared, so a
replica that started no runs answers exactly as the one that started them all
(AGENTS.md §7). `cmd/orkctl/stats_two_engine_integration_test.go` holds two
real engine processes to that.

**No aggregate reads a run body.** The archive index carries a run's namespace,
orkflow, status and time in the object KEY, so counting is a prefix scan that
transfers no bodies at all. That is a property of the type, not a habit:
the stats handler holds a `RunIndexTallier`, whose only method is a keys-only
tally, and the tests run it against a store that panics if a body is fetched
(`TestGetStats_ReadsIndexKeysAndNeverARunBody`).

## 2. The window

`since` is inclusive, `until` exclusive, so consecutive windows tile without
counting a run twice.

- Empty `until` means the moment the answer is computed.
- Empty `since` means `lookback` before that, and `lookback` defaults to 24h.
- A window that ends at or before it starts is refused. Answering it with zeros
  would hand back numbers that read exactly like a healthy, idle namespace.

**One window, one population.** Every number in an answer is about the same set
of runs: the ones this namespace CREATED inside the window, whenever they
happen to end. A run is counted in the window that submitted it, and the
percentiles are taken over those of its runs that have since finished — so
`p50` is the median of runs the success rate beside it is a rate over.

Creation is the coordinate because it is the only instant both records agree
on: the archive index stamps a run by `created_at`
(`internal/engine/archive.go`, `RunIndexTime`) and the cluster's execution
index carries its start. Selecting the percentiles by CLOSE time instead
would make one answer hold two different sets of runs with nothing saying so —
`orkctl stats --since 15m` over hour-long runs would report
`succeeded 0 / failed 0 / rate unavailable` beside `p50 58m over 12 finished
runs`. `TestExecutions_SamplesTheSameRunsTheArchiveCounts` puts one corpus
through both records and requires the two counts to match.

Two consequences follow, and both are the point:

- A run created inside the window and still going is counted as `running` and
  sampled by nothing. It has no duration yet.
- A window in the past keeps filling in as its runs end — its counts and its
  percentiles together. A pinned window is a question about a set of runs, not
  a photograph of an instant.

The response echoes what it answered: `namespace`, `window_since` and
`window_until`, all resolved. A client that asked for "the default namespace,
the last day" can then label its own numbers without knowing the default's
name — which is why `orkctl stats` prints the namespace the *server* named
rather than the one it was asked to act in.

`orkctl stats --since 6h` sends the **duration**, not an instant. One clock
then decides both ends of the window — the server's, which is the clock every
stored time is compared against. A client that resolved "six hours ago" itself
would be pairing its clock with the server's, and a client running a few
minutes fast would silently ask about a window of the wrong length.

`running` is the exception, deliberately: it is what is executing NOW, bounded
above by `until` but not below by `since`. A run that started last week and is
still going is the run most worth seeing, and a window that hid it would be
hiding it precisely when it mattered.

## 3. Numbers that cannot be answered say so

A dashboard that renders "no data" as `0` is worse than one that renders
nothing: 0% success and "nothing finished yet" look identical and mean opposite
things.

- `success_rate_defined` is false when the window finished no runs. `orkctl`
  prints `unavailable (no run finished in this window)`.
- `duration_sample_count` is 0 when no duration could be sampled, and the two
  percentiles are then unset rather than zero.
- `duration_coverage_since` is the instant the percentiles actually cover. How
  long a run took is known only while the cluster still holds its execution
  record — 72 hours by default (`internal/task/namespace.go`) — and the archive
  index carries no duration. A 30-day window over a 3-day record answers for 3
  days and says so here. It is the one place the two records may legitimately
  disagree about the one population they share: before this instant the
  window's counts are still complete and its percentiles are not.
- `runs_by_status` always carries all four keys, so a real zero is a zero.

There is no `pending` key. Telling an accepted run from a started one means
reading that run's history, one read per run, which is the cost this whole
service exists to avoid. `running` means "accepted and not finished", and the
`running` list is the same set.

`success_rate` counts canceled runs as finished. A deployment that cancels half
its runs has not succeeded at them, and excluding them would make the rate rise
when things go wrong.

Percentiles are **nearest rank**: p95 is the smallest sample at or above 95% of
them. Every value returned is a duration some run actually took, which an
interpolated percentile over a handful of runs would not be.

## 4. The cache

| Variable | Default | Meaning |
|---|---|---|
| `ORK_STATS_CACHE_TTL` | `5s` | how long one computed answer is reused. `0` (or any negative duration) switches the cache off. An unparsable value falls back to the default. |

A dashboard open in several tabs refreshes on a timer. Without a cache, ten
tabs at one refresh a second scan the archive index ten times a second for an
answer that cannot have moved much; at five seconds it is scanned once.

The cache **cannot change an answer**:

- It stores one whole `GetStatsResponse` and returns it verbatim or recomputes.
  It never merges a fresh half into a stale one, which is the only way a cache
  could produce numbers no computation ever produced.
- Its key is the namespace and the window **as requested** — including
  `lookback`, because "the last hour" and "the last day" are two questions. A
  request that pins both bounds gets the answer to exactly those instants.
- `computed_at` travels on the wire, so how old an answer is never has to be
  guessed.
- A namespace that does not exist is still `NotFound`, and a window that ends
  before it starts is still `InvalidArgument`, however warm the cache is: the
  request is resolved and validated before anything is looked up. A cache that
  answered first could turn a refused request into somebody else's numbers.

`TestGetStats_ACachedAnswerEqualsTheUncachedOne` recomputes the same question
at the same instant with the cache switched off and asserts the two responses
are equal field for field.

## 5. Cost

- One prefix scan of `metadata/index/runs/{namespace}/` per answer — keys only,
  one listing page per 1000 archived runs in the namespace.
- One page-walk of the cluster's execution index for the runs still going, and
  one bounded by START time for the window's own runs. If a walk runs out of
  pages the request FAILS rather than answering from a subset: a percentile
  over a truncated sample looks like an answer and is not one.
- One liveness probe per pool in scope — typically two or three.
- All of it divided by the cache TTL, per namespace and window.

Registering the namespace search attribute (the engine provisions it at boot)
turns the namespace into a predicate the cluster applies, instead of a filter
applied after reading every namespace's records. The boot log says which
happened: `run_namespace_index=true`.

## 6. Failure fingerprints

`StatsService.GetFailureBreakdown` answers the question `most_failing` stops
short of: *is this the same failure as the last eighteen, and is it mine or the
platform's?* It groups one namespace's failed runs over one window by the
dimensions a caller names, worst first.

```bash
orkctl -n payments stats failures                       # orkflow, task, code, category
orkctl -n payments stats failures --since 6h --group-by task,code,image,phase
orkctl -n payments stats failures --category infrastructure_error --json
orkctl -n payments stats failures --since 2026-09-22T04:00:00Z --until 2026-09-22T11:00:00Z --orkflow reconcile
```

The dashboard offers the same answer as a disclosure under **Recent failures**
(`ui/src/lib/components/FailureFingerprints.svelte`), fetched only when it is
opened, with one chip per dimension and a CLI equivalent that reproduces its
rows.

**The one read that opens run bodies.** §1's rule — no `GetStats` aggregate reads a run
body — holds for `GetStats` unchanged, with its keys-only type and its
panicking test. The facts that tell one failure from another are not in the
index key: the task that failed, its code and category, its pool and Orker are
in the archived snapshot, and the image the final attempt ran and the phase it
ended in are in the per-attempt record. Widening the key would be a migration
and an index that can disagree with the body it indexes. So this read crosses
the line, deliberately and in one place, and bounds the crossing three ways:
by the window, by the failed subset of it, and by a scan budget. What §1
protects is preserved — the bodies read are proportional to FAILED runs in the
window, never to how much output they wrote, because a run body holds
references and not bytes.

**One window, one population, again.** The window is exactly `GetStats`'
(§2), resolved by the same function (`windowOf`). The population is the index
keys under the namespace's `failed` status inside it, so
`failed_runs_in_window` is the number `GetStats` reports as
`runs_by_status["failed"]` for the same window, and `sum(run_count)` over
every group is `failed_runs_grouped`. The two answers about one namespace
cannot disagree about how many runs failed
(`TestGetFailureBreakdown_ReconcilesWithGetStatsFailedCount`).

**Dimensions** are exact equality on recorded fields; nothing is inferred,
clustered or read out of a message:

| Dimension | Read from |
|---|---|
| `orkflow` | `Run.orkflow_name` |
| `task` | the primary failed task's id (below) |
| `code`, `category` | that task's `error`, or the run's own `error` for a run-level failure |
| `pool`, `orker` | that task's `pool` and `orker_id` |
| `image`, `phase` | the task's final attempt record: `image.pinned`, `phase` |

The **primary failed task** is, among the run's `FAILED` tasks, the one that
finished first; a task with no `finished_at` sorts after every one that has
one, and ties go to declaration order. The tasks a failing run cancels around
its failure are consequences, never the cause. A lost pin member is `FAILED`
with `PIN_HOST_LOST`, so a lost pin fingerprints as exactly that. A failed run
with no `FAILED` task — a start that failed after its id was minted
(`RUN_START_FAILED`) — is a run-level failure with no task, pool, Orker, image
or phase. A failure handler (`docs/DSL.md`, "Joins and failure
handlers") that fails while a run is failing finishes after the run's original
failure, so it is never taken for the cause. In an Orkflow with a
failure handler, a `condition:` that cannot be evaluated records its task
`FAILED` with `CONDITION_FAILED`, so that failure is attributed to the task;
in every other Orkflow it stays a run-level failure with its task `CANCELED`,
and its grouping is unchanged. The **final attempt record** is the one whose number is the task
run's `attempt`, else the highest-numbered one (`attempt` is 0 for a task
Temporal timed out). A record is used whatever its outcome.

**`unavailable` is not `-`.** Attempt records are reclaimed on the run's
retention schedule (`docs/STORAGE.md`, "Reclaiming space"); the archive body
and its index key never are. A group whose image or phase could not be read
because the record is gone counts those runs in `attempt_evidence_missing`,
and both clients render the empty cell `unavailable`. Any other empty value —
a run-level failure's task, a task that never reached an Orker, a snapshot
written before a field existed — renders `-`. `attempt_evidence_read` says
whether the attempt store was consulted at all: it is only when `image` or
`phase` is asked for, so a zero beside `false` means nothing was looked for.
An engine with no attempt store refuses `image` and `phase` with
`FailedPrecondition` rather than answering them as "all unavailable".

**Fail rather than answer from a subset.** More than 5000 failed runs in the
window (`FailureBreakdownMaxScan`) is `ResourceExhausted` before a single body
is read, and the message names the population, the bound, and what to narrow.
An index row whose body is absent — the crash window between the archive's two
writes — is skipped exactly as `ListRuns` skips it, and shows as
`failed_runs_scanned < failed_runs_in_window`. Any other unreadable body, or an
attempt store that cannot be reached, fails the call with `Unavailable` naming
the run: a census with a hole in it is not a census, and "could not read" is
not "reclaimed". `limit` (default 50, at most 500) bounds the groups returned,
never the scan; `groups_total`, `failed_runs_grouped` and
`attempt_evidence_missing` are always over every group.

**Order** is total and clock-free: run count descending, then newest
`last_seen`, then each grouped dimension ascending in the enum's order. Each
group carries its `first_seen`/`last_seen` (creation times) and up to three
example runs, newest created first; ids are carried, never compared.

**The cache** is §4's, with a second key shape: the namespace and window as
sent, plus the orkflow, the resolved dimensions in request order, the
category and the limit as sent. Same TTL (`ORK_STATS_CACHE_TTL`), same bound,
same whole-answer rule, and the same order — namespace and window resolved,
every field validated, and only then the lookup.

**Cost**: one keys-only prefix scan of
`metadata/index/runs/{namespace}/{orkflow?}/failed/`, one body read per failed
run in the window (eight at a time), and — only for `image` or `phase` — one
attempt-record prefix listing per failed task-level run. Nothing is written.

## 7. Flake index

`StatsService.GetFlakeIndex` answers the question a success rate cannot: *which
tasks only succeed because they are tried again?* A task that failed twice and
passed on its third attempt makes a `SUCCEEDED` run, invisible by construction
to `runs_by_status`, `success_rate`, `most_failing` and the fingerprints. A
**flake** is a task that needed more than one Temporal attempt in one run,
whatever the run's final status; the index groups the window's dispatched tasks
by the dimensions a caller names and reports, per group, how often that
happened, flakiest first.

```bash
orkctl -n payments stats flakes                              # orkflow, task
orkctl -n payments stats flakes --since 168h --group-by task,orker
orkctl -n payments stats flakes --orkflow reconcile --group-by phase --json
```

The dashboard offers the same answer as a **Stability** disclosure
(`ui/src/lib/components/FlakeIndex.svelte`), fetched only when opened; the run
page's **Retry history (7 days)** button asks it for one orkflow and decorates
each task's retry badge with the task's historical rate (`flaky 50%` on a run
that did not retry a task that retries elsewhere).

**The second read that opens bodies, and the widest.** Like §6 it crosses §1's
line, and it crosses it further: it reads the body of **every** finished run
in the window — succeeded, failed and canceled — because the instability it
exposes is exactly the kind a `SUCCEEDED` status hides, and the body is the
only record of which tasks were dispatched. It then lists one task's attempt
records per dispatched task: the one aggregation that reads attempt records
across runs. The cost stays proportional to runs × dispatched tasks in the
window, never to output size — bodies hold references, attempt records are
bounded at 64 KiB — and it is bounded by the window, an optional orkflow, and
a scan budget.

**One window, one population.** The window is `GetStats`' (§2), resolved by
`windowOf`. The population is every index key under the namespace (and
orkflow, if named) inside it, whatever its finished status, so
`runs_in_window` equals `runs_by_status["succeeded"] + ["failed"] +
["canceled"]` for the same window
(`TestGetFlakeIndex_PopulationIsEveryFinishedRunTheWindowCreated`).

**Dispatched.** A task is in the index iff its snapshot's `pool` is set — it
reached an Orker. A skipped task, a task canceled before dispatch and a task of
a run whose start failed produce nothing. A run with no dispatched task is
counted in `runs_scanned` and nowhere else.

**Attempts** for one task of one run are the highest stored record's number or
the snapshot's `attempt`, whichever is higher; the snapshot alone is not
trusted (it is 0 for a task Temporal timed out and the committing attempt for
an adopted commit). A record is used whatever its outcome — a `CANCELED`
non-final record and an adopted commit on attempt 2 are both retries. The log
store is **not** consulted, so a task whose final attempts left neither a
record nor a snapshot attempt number is undercounted: the error is always
toward fewer retries, never more, and `orkctl run attempts` remains the
per-run truth. An attempt numbered below the highest with no record of its own
is `unreported_attempts` — a retry whose Orker died before filing its cause.

**The honest denominator.** Attempt records are reclaimed on the run's
retention schedule (`docs/STORAGE.md`, "Reclaiming space"), or a best-effort
write never landed. A dispatched task with **no** record is *evidence
missing*: it is **excluded** from every rate and counted
(`runs_evidence_missing` on the group and the response), never treated as a
task that ran once cleanly. Counting it as clean would make every rate fall
smoothly as the window ages — a graph that trends toward health for purely
mechanical reasons. A task no run in the window has a record of appears with
`retry_rate_defined = false` and renders `unavailable`, never `0%`; a window
older than retention answers with every task `unavailable` and says so with a
count. `runs_evidence_complete + runs_evidence_missing = runs_dispatched`; a
partially observed run is missing for the response, and its observed tasks
still count.

**Attribution.** `orkflow`, `task` and `pool` are read from the body;
`orker`, `phase` and `image` from each attempt record, because one task's
attempts can run on different Orkers. Keyed by a record-level dimension, a
retry is attributed to the key of the **non-final** attempt — the one that
failed and caused the next — so keyed by `orker` the index blames the host
whose attempt failed, never the one that then succeeded
(`TestGroupFlakes_ARecordKeyedGroupCountsARunRetriedOnlyWhereANonFinalAttemptCarriesItsKey`).
A record-keyed answer has `body_keyed = false`, and its per-group
evidence-missing and unreported counts are 0: a task with no record has no
Orker to be keyed by, so only the response totals are meaningful. `code` and
`category` are refused as dimensions — a group REPORTS them as its dominant
cause: the most common `(code, category, phase)` triple among its non-final
records, one triple, never three independent modes.

**Percentiles** of attempts per run are nearest rank over the group's observed
runs — the same `NearestRank` the duration percentiles use, so every value is
an attempt count some run actually reached.

**Fail rather than answer from a subset.** More than 5000 finished runs in the
window (`FlakeIndexMaxScan`) is `ResourceExhausted` before a single body or
listing is read; the message names the population, the bound, and what to
narrow. An index row with no body is skipped (`runs_scanned <
runs_in_window`); any other unreadable body, or an attempt listing that
errors, fails the call with `Unavailable` naming the run. An engine with no
attempt store refuses with `FailedPrecondition` rather than answer "everything
unavailable". `limit` (default 50, at most 500) bounds the groups returned,
never the scan.

**Order** is total and clock-free: defined rates first, then retry rate,
retried runs and observed runs, each descending, then each keyed dimension
ascending in enum order. Up to three example runs per group are its newest
retried runs.

**The cache** is §4's, with a third key shape: the namespace and window as
sent, plus the orkflow, the resolved dimensions in request order and the limit
as sent. Same TTL (`ORK_STATS_CACHE_TTL`), same whole-answer rule, and the
namespace, window and every field are validated before the lookup.

**Cost**: one keys-only prefix scan of
`metadata/index/runs/{namespace}/{orkflow?}/` (every status), one body read per
finished run in the window and one prefix listing of
`metadata/attempts/{run-id}/{task-id}/` per dispatched task, eight at a time.
Nothing is written; no counter, rollup or trend store exists. A flake rate
graphed over weeks belongs to the telemetry pipeline (`ork.task.attempts`).

## 8. Usage

`StatsService.GetUsage` answers *how much of the fleet did this namespace
RESERVE over a window?* — the read a namespace budget is held against, and the
one the admission gate itself makes for an enforced budget
(`docs/DEPLOY.md`, "Namespace budgets").

```bash
orkctl -n analytics stats usage                               # the namespace total, last 24h
orkctl -n analytics stats usage --since 720h --group-by pool  # the budget's own window, per pool
orkctl stats usage --since 720h --group-by namespace          # every namespace, one call each
```

The dashboard's **Usage** card and **Admin → Usage** render the same answers
(`ui/src/lib/usage.ts`).

**Three units, all reserved, none measured.**

| Unit | Definition |
|---|---|
| task-seconds | Σ held time — `finished_at − started_at` of every recorded attempt: from the Orker picking the activity up to it writing the record, image pull, staging and upload included, because the Orker's slot was held for all of it |
| cpu-seconds | Σ `TaskAttempt.resources.cpu × held time` over recorded attempts carrying a reservation |
| GiB-seconds | Σ `TaskAttempt.resources.memory_bytes ÷ 2³⁰ × held time` over the same attempts |

The **reservation** is what the Orker granted the container after its
defaults filled a missing request and its caps admitted it
(`ORK_ORKER_DEFAULT_*`, `ORK_ORKER_MAX_*`), recorded on every attempt record
(`TaskAttempt.resources`). It is a limit, not a measurement: every surface
prints "cpu-seconds and GiB-seconds are RESERVED cpu and memory × attempt
time, not measured utilization." OrkStack knows what it reserved, never what
it cost or what the kernel measured.

**The population** is the flake index's (§7): every finished run of the
namespace the window created, read from the archive index keys, so
`runs_in_window` equals `GetStats`' succeeded + failed + canceled for the same
window. A run still in flight is not in it; it is counted once it finishes.
Every recorded attempt counts whatever its outcome — succeeded, failed,
canceled, or an adopted commit (whose held time is the commit check).

**Honest gaps.** Each is counted and printed, never folded into a smaller
number that looks real:

- `runs_evidence_missing` — dispatched runs holding no record for at least one
  task (reclaimed with the run's logs, removed by a lifecycle rule, or a
  best-effort write that never landed). Their consumption is NOT in any sum.
- `coverage_since` — the creation time of the oldest run in the window that
  still has a record: where reclamation has reached.
- `attempts_without_resources` — recorded attempts with no reservation (written
  before this release, or refused before one was resolved): their time is in
  task-seconds, not in cpu- or GiB-seconds, which are then lower bounds.
- `unreported_attempts` — attempts below a task's highest that left no record.
- `attempts_unmeasured` — records with no usable interval; counted in nothing.

**Fail rather than answer from a subset.** More than 10000 finished runs in the
window (`UsageMaxScan`) is `ResourceExhausted` before a body is read. An index
row with no body is skipped (`runs_scanned < runs_in_window`); any other
unreadable body, attempt listing or record fails the call `Unavailable`,
naming the run. An engine with no archive, or no attempt store that can list a
run's records, is `FailedPrecondition`.

**The memo cannot change an answer.** A usage window is long and its runs are
finished, so each replica remembers, per run, its archived body's facts
(immutable: archive bodies are never deleted) and the sums its attempt records
fold to — valid ONLY for the exact set of record keys they were read from.
Every measurement re-lists each run's records; a listing that names other keys
(a record reclaimed since, or a late best-effort write) misses the memo and the
run is re-read, so a warm replica and a cold one answer identically for the
same bytes (`TestUsageMeter_WarmAnswerEqualsColdAnswer`,
`TestUsageMeter_AReclaimedRunIsMissingOnAWarmReplicaToo`). It is bounded at
20000 runs, least recently used first.

**The answer cache** holds whole measurements for `ORK_USAGE_CACHE_TTL`
(default 5m; `0` switches it off), keyed by the namespace and window as sent
and the grouping, and every answer carries `computed_at`. One measurement per
question runs at a time; a caller that gives up does not abort it, and the
next caller gets it. The budget is policy and is never cached: every answer
reads it fresh from the namespace document, and `budget_window` /
`budget_reached` are set only when the window asked is exactly the budget's
trailing period (`--since <period>`).

**Cost**, divided by `ORK_USAGE_CACHE_TTL` per (namespace, window as sent,
grouping):

| Read | When |
|---|---|
| index listing, 1 page per 1000 archived runs of the namespace | every measurement (GetStats pays the same) |
| 1 attempt-prefix listing per finished run in the window | every measurement |
| 1 body GET + 1 GET per attempt record, per run | the first measurement of that run on this replica, and again only when its record set changed |

Nothing is written; no counter, rollup or ledger exists. A consumption curve
over months belongs to the telemetry pipeline (`ork.orker.task.duration`
carries per-attempt durations labelled by namespace and pool).

## 9. Cache

`orkctl cache stats` and **Admin → Cache** (`CacheService.GetCacheStats`)
answer how one namespace's cached tasks (`docs/DSL.md` "Caching") fared over a
window, and what memos the store holds now. Both halves are derived on read;
no hit is counted anywhere.

- **Usage**: the window's FINISHED runs, by the run archive's index (of one
  Orkflow when one is named); each body is read, and every `TaskRun` — and
  every fan-out item — whose `cache.outcome` is set is folded into one row per
  (Orkflow, task): hits, misses, bypassed, `hit_rate = hits / (hits +
  misses)` (*unavailable* when that is zero — bypassed runs asked not to look),
  and `reused`, the producing attempts' duration the hits did not spend again.
- **Inventory**: a listing of `metadata/memo/` keys under the namespace (one
  Orkflow's orkflow-scoped memos when one is named); no memo body is read.

Two bounds, and the read **fails rather than answer from a subset**: more than
5,000 finished runs in the window (`CacheStatsMaxScan`, the flake index's
bound) is `ResourceExhausted` before any body is read; more than 100,000 memo
keys (`CacheStatsMaxMemos`) is `ResourceExhausted` naming `orkctl cache purge`.
An index row whose body is absent is skipped and counted (`runs_scanned` <
`runs_in_window`); any other body error fails the call `Unavailable`, naming
the run. Answers are cached per (namespace, window as sent, Orkflow) on the
same `ORK_STATS_CACHE_TTL` as the other stats reads, and a purge clears this
replica's cached answers for its namespace.

