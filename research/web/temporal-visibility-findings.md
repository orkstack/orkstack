# Temporal Visibility & Persistence Limits — findings

Evidence base: docs.temporal.io (fetched 2026-08-14), `temporalio/temporal` @ commit
`83b35dc3bac4ea05296154e0b50055026d200150`, `temporalio/cli` @ `9112b4e0ac7a2b4321251f5922659363813feb7a`.

## Summary / verdict

Temporal's visibility store **can** serve list/filter queries over custom search attributes on
all three targets you named (dev-server SQLite, docker-compose Postgres, Elasticsearch) — standard
visibility no longer exists (removed in server v1.24). But it is **not** a substitute for a run
database for these concrete reasons:

1. Visibility is **eventually consistent** with no published bound ("usually fast, but it can be
   several seconds or longer"), and Temporal explicitly tells you not to use it for per-execution
   state.
2. **ORDER BY is unsupported on SQL visibility stores** (hard error in the store code) — so on
   SQLite/Postgres you cannot sort by StartTime or a custom attribute; you get the fixed default
   ordering only.
3. **Only 3–10 custom search attributes per type per namespace** on SQL stores (10 Keyword, 3 each
   of Bool/Datetime/Double/Int/KeywordList/Text). That is a hard ceiling on how much run metadata
   you can index.
4. **Data disappears at retention** (docker/dev-server default **24h**), taking history *and*
   visibility records with it.
5. Memo is **not queryable** at all.
6. History is capped (**50 MB / 51,200 events** error limits) and `GetWorkflowExecutionHistory`
   pages at 256 events — reconstructing task-level state from history on every read is a poor
   read path.

---

## 1. Standard vs advanced visibility, per deployment

Standard visibility is gone. From [Temporal Visibility](https://docs.temporal.io/visibility):

> "Standard Visibility supported only predefined filters such as Workflow Type, Workflow Id, Run Id,
> and Execution Status, without custom Search Attributes. Advanced Visibility required Elasticsearch.
> Starting with Temporal Server v1.20, advanced Visibility became available on SQL databases.
> Standard Visibility was deprecated in v1.21 and **removed in v1.24**. All current deployments use
> what was formerly called advanced Visibility, now simply called Visibility."

| Target | Custom SAs | ExecutionStatus | StartTime range | Combine AND/OR | ORDER BY |
|---|---|---|---|---|---|
| (a) `temporal server start-dev` (SQLite) | yes (SQLite ≥3.31.0) | yes | yes | yes | **no** |
| (b) docker-compose + Postgres 12+ | yes | yes | yes | yes | **no** |
| (c) Elasticsearch / OpenSearch | yes, unlimited count | yes | yes (1 ns resolution) | yes | supported by store, but `system.visibilityDisableOrderByClause` defaults to **true** |
| (c') legacy standard visibility (<v1.24) | no | single `=` only | no ranges | no | no |

- Custom SA support matrix and SQLite requirement: [Search Attributes → Custom Search Attributes](https://docs.temporal.io/search-attribute#custom-search-attribute) — "With Temporal Server v1.20 and later, custom Search Attribute capabilities are available on MySQL (v8.0.17 or later), PostgreSQL (v12 and later), and SQLite (v3.31.0 and later), in addition to Elasticsearch."
- The dev server uses SQLite for **both** the default store and the visibility store, so it is a full advanced-visibility store: [cli/internal/devserver/server.go](https://github.com/temporalio/cli/blob/9112b4e0ac7a2b4321251f5922659363813feb7a/internal/devserver/server.go#L283-L290)
  ```go
  conf.Persistence.DefaultStore = "sqlite-default"
  conf.Persistence.VisibilityStore = "sqlite-default"
  ```
- Combining filters is explicitly documented: `WorkflowType = 'main.YourWorkflowDefinition' and YourCustomSA = 'YourCustomSAValue' and (StartTime > '2022-06-07T16:46:34.236-08:00' or CloseTime < '...')` ([search-attribute](https://docs.temporal.io/search-attribute#custom-search-attribute)).
- **ORDER BY hard-fails on SQL visibility** — [common/persistence/visibility/store/sql/visibility_store.go#L962-L965](https://github.com/temporalio/temporal/blob/83b35dc3bac4ea05296154e0b50055026d200150/common/persistence/visibility/store/sql/visibility_store.go#L962-L965):
  ```go
  // ORDER BY is not support in SQL visibility store
  if len(queryParams.OrderBy) > 0 {
      return nil, query.NewConverterError("%s: 'ORDER BY' clause", query.NotSupportedErrMessage)
  }
  ```
  Same in the legacy converter: [query_converter_legacy.go#L198-L201](https://github.com/temporalio/temporal/blob/83b35dc3bac4ea05296154e0b50055026d200150/common/persistence/visibility/store/sql/query_converter_legacy.go#L198-L201). `LIMIT` is likewise rejected ([#L203-L205](https://github.com/temporalio/temporal/blob/83b35dc3bac4ea05296154e0b50055026d200150/common/persistence/visibility/store/sql/query_converter_legacy.go#L203-L205)).
  The fixed ordering is `ClosedTime DESC NULL FIRST, StartTime DESC` ([list-filter](https://docs.temporal.io/list-filter#supported-operators)); the SQL query template is `ORDER BY %s DESC, %s DESC, %s` in each dialect, e.g. [query_converter_legacy_postgresql.go#L250](https://github.com/temporalio/temporal/blob/83b35dc3bac4ea05296154e0b50055026d200150/common/persistence/visibility/store/sql/query_converter_legacy_postgresql.go#L245-L255).
- Time-range resolution differs: "1 ns on Elasticsearch and 1 µs for SQL databases" ([list-filter](https://docs.temporal.io/list-filter)).
- Operators supported: `=, !=, >, >=, <, <=`, `AND/OR/()`, `BETWEEN…AND`, `IN`, `STARTS_WITH`, `IS NULL/IS NOT NULL`. `GROUP BY` only in the Count API and only by `ExecutionStatus`; Count returns **approximate** counts ([visibility](https://docs.temporal.io/visibility#advanced-visibility)).
- SQL-store custom SAs are **per namespace**; Elasticsearch custom SAs are global across namespaces ([search-attribute](https://docs.temporal.io/search-attribute#custom-search-attribute)).

## 2. Default (predefined) search attributes

From [Default Search Attributes](https://docs.temporal.io/search-attribute#default-search-attribute):

`BatcherUser` (Keyword), `BinaryChecksums` (KeywordList, deprecated), `BuildIds` (KeywordList,
deprecated), `CloseTime` (Datetime), `ExecutionDuration` (Int, ns, closed only), `ExecutionStatus`
(Keyword), `ExecutionTime` (Datetime), `HistoryLength` (Int, closed only), `HistorySizeBytes`
(Long), `RunId` (Keyword), `StartTime` (Datetime), `StateTransitionCount` (Int, closed only),
`TaskQueue` (Keyword), `TemporalChangeVersion` (KeywordList), `TemporalReportedProblems`
(KeywordList), `TemporalScheduledStartTime` (Datetime), `TemporalScheduledById` (Keyword),
`TemporalSchedulePaused` (Boolean), `TemporalUsedWorkerDeploymentVersions` (KeywordList),
`TemporalWorkerDeployment` (Keyword), `TemporalWorkerDeploymentVersion` (Keyword),
`TemporalWorkflowVersioningBehavior` (Keyword), `WorkflowId` (Keyword), `WorkflowType` (Keyword).

Notes: all are reserved/read-only. `ExecutionStatus` ∈ {Running, Completed, Failed, Canceled,
Terminated, ContinuedAsNew, TimedOut}. `CloseTime`, `HistoryLength`, `StateTransitionCount`,
`ExecutionDuration` exist **only on closed executions** — so you cannot filter/sort open runs by
duration. Datetime values accept RFC3339Nano strings or epoch nanoseconds.
`BinaryChecksums`/`BuildIds` stop propagating in server 1.32 and lose query support in 1.34.

## 3. Custom search attribute limits

Per-namespace **count** limits by store ([search-attribute#custom-search-attribute-limits](https://docs.temporal.io/search-attribute#custom-search-attribute-limits)):

| Type | MySQL 8.0.17+ / PostgreSQL 12+ / SQLite 3.31+ | Temporal Cloud |
|---|---|---|
| Bool / Datetime / Double / Int | 3 each | 20 each |
| Keyword | 10 | 40 |
| KeywordList | 3 | 5 |
| Text | 3 | 5 |

Elasticsearch: no Temporal-imposed limit (Elasticsearch's own mapping limit applies). Cassandra:
custom SAs unsupported.

Docs size limits: single value 2 KB; total 40 KB; max 255 characters per value.

Server dynamic-config defaults ([common/dynamicconfig/constants.go#L894-L908](https://github.com/temporalio/temporal/blob/83b35dc3bac4ea05296154e0b50055026d200150/common/dynamicconfig/constants.go#L894-L908)):
```go
SearchAttributesNumberOfKeysLimit = NewNamespaceIntSetting(
    "frontend.searchAttributesNumberOfKeysLimit", 100, ...)
SearchAttributesSizeOfValueLimit = NewNamespaceIntSetting(
    "frontend.searchAttributesSizeOfValueLimit", 2*1024, ...)
SearchAttributesTotalSizeLimit = NewNamespaceIntSetting(
    "frontend.searchAttributesTotalSizeLimit", 40*1024, ...)
```
So: **100 keys per workflow's SA map, 2 KB per value, 40 KB total map** — these are validation
limits on what a workflow may set, distinct from the per-namespace *registration* counts above.
The registration counts (3/10) are the binding constraint on SQL stores.

## 4. Eventual consistency

[Visibility → "How current is Visibility data"](https://docs.temporal.io/visibility#how-current-is-visibility-data):

> "Visibility is a search index that Temporal updates asynchronously. After a change is recorded, it
> takes some time to propagate to the index, so a List or Count query can briefly return stale
> results. This propagation delay is variable. It is usually fast, but it can be **several seconds or
> longer**, and Temporal Cloud does not publish an SLA for how quickly changes become visible. Treat
> Visibility as eventually consistent and avoid depending on a fixed or guaranteed delay."

> "Visibility is built for finding and filtering across many executions, not for reading the current
> state of one execution. When you need the authoritative, up-to-date state of a specific Workflow
> Execution, use `DescribeWorkflowExecution` instead of a Visibility query."

Mechanism: the History service maintains an **Internal Visibility Task Queue** that "pushes data to
the Advanced Visibility index" ([temporal-server#history-shard](https://docs.temporal.io/temporal-service/temporal-server#history-shard)); the executor is
[service/history/visibility_queue_task_executor.go](https://github.com/temporalio/temporal/blob/83b35dc3bac4ea05296154e0b50055026d200150/service/history/visibility_queue_task_executor.go).
This async path applies to **both** SQL and Elasticsearch stores — there is no documented "sync
visibility" mode for SQL. Elasticsearch adds a second buffer: `worker.ESProcessorFlushInterval`
defaults to **1 s** with `worker.ESProcessorAckTimeout` 30 s ([constants.go#L3381-L3391](https://github.com/temporalio/temporal/blob/83b35dc3bac4ea05296154e0b50055026d200150/common/dynamicconfig/constants.go#L3376-L3391)), plus ES's own ~1 s refresh interval. In practice SQL
visibility converges faster (a DB upsert, no index refresh), but neither is read-your-writes.

Also relevant: `ListSchedules`, `CountWorkflowExecutions`, `ListBatchOperations`, and
`GetWorkerTaskReachability` all read the same index and share its staleness and rate limit.
Single-entity lookups (`DescribeWorkflowExecution`, `DescribeSchedule`) do **not** use visibility.

## 5. Memo

**Memo is not queryable.** It is not a search attribute and does not appear in the List Filter
grammar; only registered search attributes can be filtered on. Memo is returned in
`ListWorkflowExecutions`/`DescribeWorkflowExecution` responses but cannot appear in the query string.

Size defaults ([constants.go#L397-L406](https://github.com/temporalio/temporal/blob/83b35dc3bac4ea05296154e0b50055026d200150/common/dynamicconfig/constants.go#L397-L406)):
```go
MemoSizeLimitError = NewNamespaceIntSetting("limit.memoSize.error", 2*1024*1024, ...)  // 2 MiB
MemoSizeLimitWarn  = NewNamespaceIntSetting("limit.memoSize.warn",  2*1024, ...)       // 2 KiB
```
So memo is capped at **2 MiB (error)** with a **2 KiB warn** threshold — and it also counts against
the per-event blob and mutable-state budgets, so treating it as a document store is a bad idea.

## 6. Retention

- Minimum: **1 day for global namespaces, 1 hour for local namespaces**, enforced in
  [service/frontend/namespace_handler.go#L1115-L1131](https://github.com/temporalio/temporal/blob/83b35dc3bac4ea05296154e0b50055026d200150/service/frontend/namespace_handler.go#L1115-L1131) against
  [constants.go#L213-L222](https://github.com/temporalio/temporal/blob/83b35dc3bac4ea05296154e0b50055026d200150/common/dynamicconfig/constants.go#L213-L222):
  ```go
  NamespaceMinRetentionGlobal = NewGlobalDurationSetting("system.namespaceMinRetentionGlobal", 24*time.Hour, ...)
  NamespaceMinRetentionLocal  = NewGlobalDurationSetting("system.namespaceMinRetentionLocal",  time.Hour, ...)
  ```
  Also hardcoded as `MinRetentionGlobal = 1*24*time.Hour`, `MinRetentionLocal = 1*time.Hour` in
  [common/namespace/const.go#L6-L13](https://github.com/temporalio/temporal/blob/83b35dc3bac4ea05296154e0b50055026d200150/common/namespace/const.go#L1-L15).
- Maximum: **no server-side maximum** since v1.18 (I found no `MaxRetention` setting in the source).
  Docs: "On Temporal Service version 1.18 and later, the maximum Retention Period value for
  Namespaces can be set to anything over the minimum requirement of 1 day… On versions 1.17 and
  earlier, the maximum … is 30 days." ([temporal-server#retention-period](https://docs.temporal.io/temporal-service/temporal-server#retention-period))
- Defaults you will actually hit:
  - `temporal server start-dev` default namespace: **24h** —
    [schema/sqlite/setup.go#L151](https://github.com/temporalio/temporal/blob/83b35dc3bac4ea05296154e0b50055026d200150/schema/sqlite/setup.go#L145-L155) `Retention: timestamp.DurationFromHours(24)`
  - docker-compose (`temporalio/auto-setup`): **24h** — `: "${DEFAULT_NAMESPACE_RETENTION:=24h}"` in [docker/auto-setup.sh](https://github.com/temporalio/docker-builds/blob/main/docker/auto-setup.sh)
  - `temporal operator namespace create` CLI flag default: **72h** — [cli commands.gen.go](https://github.com/temporalio/cli/blob/9112b4e0ac7a2b4321251f5922659363813feb7a/internal/temporalcli/commands.gen.go#L2043) `s.Retention = cliext.MustParseFlagDuration("72h")` (docs say 3 days — same thing)
- What happens at expiry: "The Temporal Service triggers a Timer task at the end of the Retention
  Period that cleans up the data associated with the closed Workflow Execution" — this deletes both
  the event history and the visibility record. Timer firing is jittered by
  `history.retentionTimerJitterDuration`, default **30 min** ([constants.go#L2200-L2204](https://github.com/temporalio/temporal/blob/83b35dc3bac4ea05296154e0b50055026d200150/common/dynamicconfig/constants.go#L2196-L2205)).
- Changing retention only affects executions that close **after** the change; already-closed
  executions keep their original cleanup timer.

**Decision impact:** any run record you want to keep beyond retention must live outside Temporal.
Extending retention to keep run history means paying Temporal-persistence storage cost for full
event histories, which is far more expensive per run than a row in a run table.

## 7. History size / event count / blob limits

All from [common/dynamicconfig/constants.go @ 83b35dc](https://github.com/temporalio/temporal/blob/83b35dc3bac4ea05296154e0b50055026d200150/common/dynamicconfig/constants.go#L385-L475):

| Key | Default | Meaning |
|---|---|---|
| `limit.blobSize.warn` | 512 KiB | per-event blob warn |
| `limit.blobSize.error` | **2 MiB** | per-event blob error (matches the 2 MB gRPC payload ceiling) |
| `limit.memoSize.warn` / `.error` | 2 KiB / 2 MiB | memo |
| `limit.historySize.warn` | 10 MiB | |
| `limit.historySize.suggestContinueAsNew` | 4 MiB | sets the continue-as-new suggestion flag on WorkflowTaskStarted |
| `limit.historySize.error` | **50 MiB** | workflow terminated |
| `limit.historyCount.warn` | 10240 events (`10*1024`) | |
| `limit.historyCount.error` | **51200 events** (`50*1024`) | workflow terminated |
| `limit.mutableStateSize.warn` / `.error` | 1 MiB / **8 MiB** | |
| `limit.numPendingActivities.error` | 2000 | pending activities per workflow |
| `limit.numPendingChildExecutions.error` | 2000 | pending children |
| `limit.numPendingSignals.error` | 2000 | |

```go
BlobSizeLimitError = NewNamespaceIntSetting("limit.blobSize.error", 2*1024*1024, `BlobSizeLimitError is the per event blob size limit`)
HistorySizeLimitError = NewNamespaceIntSetting("limit.historySize.error", 50*1024*1024, ...)
HistoryCountLimitError = NewNamespaceIntSetting("limit.historyCount.error", 50*1024, ...)
```

## 8–9. Read path: GetWorkflowExecutionHistory and ListWorkflowExecutions

- **History page size default 256 events**: `FrontendHistoryMaxPageSize` = `primitives.GetHistoryMaxPageSize`
  ([constants.go#L719-L723](https://github.com/temporalio/temporal/blob/83b35dc3bac4ea05296154e0b50055026d200150/common/dynamicconfig/constants.go#L719-L723),
  [primitives/constants.go#L20-L21](https://github.com/temporalio/temporal/blob/83b35dc3bac4ea05296154e0b50055026d200150/common/primitives/constants.go#L18-L22) `GetHistoryMaxPageSize = 256`).
  A run with a few thousand events therefore costs multiple round trips plus payload
  deserialization per read. Combined with the 50 MiB / 51,200-event ceiling, **reconstructing
  task-level state from event history on every read is not a viable read path** for a UI or API —
  Temporal's own guidance is to use `DescribeWorkflowExecution` for single-execution state, or a
  Query, or "storing state in an external datastore through Activities and fetching it directly
  from the store" ([search-attribute](https://docs.temporal.io/search-attribute#search-attribute)).
  Temporal publishes no latency numbers for `GetWorkflowExecutionHistory`.
- **List page size max 1000**: `frontend.visibilityMaxPageSize` = 1000
  ([constants.go#L714-L718](https://github.com/temporalio/temporal/blob/83b35dc3bac4ea05296154e0b50055026d200150/common/dynamicconfig/constants.go#L714-L718)).
- **Pagination tokens: yes.** "To paginate the results using the `ListWorkflow` API, use the page
  token to retrieve the next page. Continue until the page token becomes `null`/`nil`."
  ([list-filter#efficient-api-usage](https://docs.temporal.io/list-filter#efficient-api-usage)).
  Go SDK: `ListWorkflowExecutionsRequest.NextPageToken` / response `NextPageToken`.
- **Latency:** no published numbers. The only documented signal is "If the Advanced List Filter API
  retrieves a substantial number of Workflow Executions (more than 10,000), the response time might
  be longer", plus the advice to use `CountWorkflow` (v1.20+) instead of counting by listing.
  Temporal Cloud publishes no visibility freshness SLA and applies a visibility API rate limit.
- **ORDER BY:** unsupported on SQL stores (see §1), and on Elasticsearch it is gated by
  `system.visibilityDisableOrderByClause`, default **true**
  ([constants.go#L75-L79](https://github.com/temporalio/temporal/blob/83b35dc3bac4ea05296154e0b50055026d200150/common/dynamicconfig/constants.go#L75-L84)):
  ```go
  VisibilityDisableOrderByClause = NewNamespaceBoolSetting(
      "system.visibilityDisableOrderByClause", true,
      `VisibilityDisableOrderByClause is the config to disable ORDERY BY clause for Elasticsearch`)
  ```
  Docs also note ORDER BY is not supported on Temporal Cloud, and `Text` attributes can never be
  ordered.

## Practical implications for replacing a run DB

Workable with Temporal alone: "list my runs, filtered by status/type/time/one or two custom
tags, newest-first, paginated" — that is exactly what visibility does, on dev SQLite and on
docker Postgres alike.

Not workable without a separate store:
- arbitrary sort orders / sort by a custom column (no ORDER BY on SQL visibility);
- more than ~10 Keyword + 3-per-other-type indexed fields per namespace;
- read-your-writes after starting or completing a run (create a run, immediately list → may miss it);
- retaining run records past retention (24h by default in both dev and docker);
- querying by memo, or storing anything sizable per run (2 KiB memo warn / 2 MiB error);
- cheap per-run detail reads driven off event history (256-event pages, no latency guarantees);
- exact counts (Count API is approximate).

A pragmatic middle ground: keep a thin run table (id, status, timestamps, sort keys, task-level
state) as the read model, write it from activities, and use Temporal search attributes only for
operator search in the Temporal UI.

## Conflicts, gaps, limitations

- The docs' "single Search Attribute value size limit is 2 KB / total 40 KB" matches
  `frontend.searchAttributesSizeOfValueLimit` (2048) and `frontend.searchAttributesTotalSizeLimit`
  (40960) exactly. But the docs also say "maximum total characters per Search Attribute value is
  255" — I did not locate that 255 constant in the server source; it may be Cloud-specific or
  enforced elsewhere. Treat 255 chars as the safe planning number for Keyword values.
- Temporal Cloud limits differ from self-hosted throughout (higher SA counts, no ORDER BY, its own
  rate limits). Everything above is self-hosted unless noted.
- I found no `MaxWorkflowRetentionPeriod` dynamic-config key in the current source, consistent with
  the docs' claim that the 30-day cap was removed in v1.18.
- Go SDK 1.47 specifically: nothing in the SDK changes these server-side limits; the SDK surfaces
  `ListWorkflowExecutions`/`CountWorkflowExecutions`/`GetWorkflowHistory` and
  `NextPageToken` pagination. I did not separately verify SDK 1.47 release notes for
  visibility-related changes.
- No published latency benchmarks for `ListWorkflowExecutions` or `GetWorkflowExecutionHistory`
  exist in official docs; the "10,000 results" note is the only quantitative hint.
