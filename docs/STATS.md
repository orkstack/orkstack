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

**The one read that opens run bodies.** §1's rule — no aggregate reads a run
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
or phase. The **final attempt record** is the one whose number is the task
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
