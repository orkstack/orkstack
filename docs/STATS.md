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

**Nothing is read from an engine's memory.** Both sources are shared, so a
replica that started no runs answers exactly as the one that started them all
(AGENTS.md §1.7). This is proven, live, on two real engine processes:
`proof/M10.6/two-replica-stats.txt`.

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
index carries its start. The percentiles used to be selected by CLOSE time
instead, and one answer then held two different sets of runs with nothing
saying so — `orkctl stats --since 15m` over hour-long runs reported
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
