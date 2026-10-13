# Recorded run histories

Each `*.json` here is a real Temporal history of `RunWorkflow`, recorded once
against a real cluster by a **versioned** worker (every workflow task carries
`VERSIONING_BEHAVIOR_PINNED`) and then left to AGE: every later commit is
measured against a build older than itself. They are the repository's
old-history-meets-new-code guard (AGENTS.md §7.2), and `make test` holds the
tree to them in two halves per fixture:

- the **command half** — the SDK's `WorkflowReplayer` replays the history and
  rejects a reordered, added or deleted command;
- the **payload half** — the replayer never looks inside a payload, so a
  second test re-executes `RunWorkflow` from the fixture's own input, feeds
  back the recorded results, and compares what today's code puts into every
  activity call (and, where listed, every event and the finished run) with
  what the recorded build put there.
  The finished run is compared as current code returns it: a top-level run
  returns only its coordinates, status and timestamps (`compactRunResult`),
  so the recorded result — every task row included, from the builds that
  returned the whole run — is projected onto that shape first
  (`CompactRunResultForTest`). The projection is a strict subset of what was
  recorded, so no fixture is re-recorded for it.

Each fixture also has a **negative test**: a deliberately altered body or
stream that the guard must catch, so the fixture is known to see the kind of
change it exists for.

## Rules

- **Never hand-edit a fixture.** A fixture edited to agree with the code
  proves only that the code agrees with itself.
- Re-record a fixture only when what the engine commands or sends on that
  path genuinely changed — never to turn a red test green. The commit that
  re-records it says why.
- A new interpreter path is entered only by a definition that declares the
  feature, so the older fixtures keep replaying unchanged
  (`TestCommittedFixtures_DeclareNoJoin` checks this for `join:`). A new
  command path gets its own fixture, recorder, and command, payload and
  negative tests on this model.
- A missing fixture makes its tests skip, naming the command that records
  it; set the fixture's `ORK_REQUIRE_*` variable to make absence a failure.

Recording, against `make infra-up` and only when asked:

```bash
ORK_RECORD_GOLDEN_HISTORY=1 ORK_REQUIRE_INFRA=1 \
  ORK_TEMPORAL_NAMESPACE=ork-<your-agent> \
  go test -tags=integration -p 1 -count=1 ./internal/engine/ \
  -run <recorder>
```

## Fixtures

### `run-succeeded-v1.json` — the base path

A successful six-task run. It also pins the `ExecuteTaskInput` wire
encoding: the activity argument is a top-level proto message, so Temporal's
converter records it as protojson (lowerCamel keys). This fixture is never
re-recorded; a change that would need it is a change to the activity
contract (`docs/OCI_ACTIVITY_CONTRACT.md` §2.1), not a refactor.

- command: `TestRunWorkflow_ReplaysCommittedV1History`
- payload: `TestGoldenHistory_ActivityPayloadsMatchWhatCurrentCodeSends`
- negative: `TestCommittedHistory_CatchesAnEmitInsertedIntoTheStream`
- lean-path gate: `TestCommittedHistory_LeanEventPathWithoutItsGateIsCaught`
  replays it against the lean event path with its version check removed and
  must fail: this history predates `lean-event-path`, so it replays only
  because the gate sends it down the old path
- recorder: none any more. `TestIntegration_RecordGoldenRunHistory` now
  records `run-succeeded-v2.json`, because today's code records the
  `lean-event-path` marker

### `run-awaited-resolved-v1.json`, `run-awaited-timeout-v1.json` — awaits

The run `a → await b → c`, the first resolved through a Signal
(`TemporalStarter.SignalAwait`), the second left to time out. Pins the
deadline Timer, which starts before the `TASK_AWAITING` emit.

- command: `TestRunWorkflow_ReplaysCommittedAwaitedHistories`
- payload: `TestAwaitedHistory_ActivityPayloadsMatchWhatCurrentCodeSends`
  (with the recorded resolution fed back)
- negative: `TestAwaitedHistory_CatchesAMovedTimer`
- recorder: `TestIntegration_RecordAwaitedRunHistories`
  (`await_integration_test.go`); `ORK_REQUIRE_AWAITED_FIXTURES`

### `run-each-v1.json` — fan-out

`list → convert (each: over six parts, max_parallel: 2, item 3 retried once)
→ merge`. Pins the `CountEachItems` marker, item dispatch order, item events
without their own snapshot, and the `EachUpstream` reference.

- command: `TestRunWorkflow_ReplaysCommittedEachHistory`, which also checks
  the `ExecuteTask` schedule (task and item, in order) through an
  interceptor, because the replayer matches an activity by type and id, never
  by input
- payload: `TestEachHistory_ActivityPayloadsMatchWhatCurrentCodeSends`;
  `TestEachHistory_ReplayNeverRecountsTheArray` (the count comes from the
  marker, whatever a store would now answer)
- negative: `TestEachHistory_CatchesAReorderedDispatch`
- lean-path gate: `TestCommittedHistory_LeanEventPathWithoutItsGateIsCaught`,
  as for `run-succeeded-v1.json`
- recorder: none any more (`TestIntegration_RecordEachRunHistory` now records
  `run-each-v2.json`); `ORK_REQUIRE_EACH_FIXTURE`

### `run-composed-parent-v1.json`, `run-composed-child-v1.json` — composition

The shipped `composed-release` example composing `release-block@0.1.0`,
recorded together. Pins `ExecuteChildWorkflow`, the `RenderComposedValues`
and `StoreComposedValues` markers, and the declared-outputs render.

- command: `TestRunWorkflow_ReplaysCommittedComposedHistories`
- payload: `TestComposedHistory_PayloadsMatchWhatCurrentCodeSends` — every
  `ExecuteTask` input and the child start (id, task queue, parent-close
  policy, memo, search attributes, input payloads)
- negative: `TestComposedHistory_CatchesAChildStartWithoutItsRender`
- recorder: `TestIntegration_RecordComposedRunHistory`
  (`compose_integration_test.go`); `ORK_REQUIRE_COMPOSED_FIXTURES`

### `run-cached-v1.json` — caching

The second run of `examples/cached-pipeline.ork.yaml`: `featurize` a hit on
the first run's memo, `stamp` uncached, `train` a miss. Pins
`ExecuteTaskInput.cache`, `ExecuteTaskResult.cache` and the
`task succeeded (cached from run …)` event. A run with no cached task sends no
cache context, so `run-succeeded-v1.json` is unaffected.

- command: `TestRunWorkflow_ReplaysCommittedCachedHistory`
- payload: `TestCachedHistory_ActivityPayloadsMatchWhatCurrentCodeSends`
  (every input, event and the finished run)
- negative: `TestCachedHistory_PayloadCheckCatchesADroppedCacheContext`
- recorder: `TestIntegration_RecordCachedRunHistory`
  (`cache_integration_test.go`); `ORK_REQUIRE_CACHED_FIXTURE`

### `run-join-v1.json` — joins and failure handlers

`examples/branch-and-recover.ork.yaml` with `route=full`, `fail_deploy=true`,
`smoke_seconds=120`: `full` and `verify` run, `canary` skipped, `deploy` fails
after two attempts, `smoke` stopped, `announce` never started, `rollback` and
`notify` run, `escalate` skipped. Pins a `none_failed` decision, a failure
handler dispatched after fail-fast with `ExecuteTaskInput.join` and a failed
need's artifact reference, the failing note (a `TASK_LOG` with no task id),
and `RUN_FAILED` after the handlers end.

- command: `TestRunWorkflow_ReplaysCommittedJoinHistory`, with the
  `ExecuteTask` schedule checked as for fan-out
- payload: `TestJoinHistory_ActivityPayloadsMatchWhatCurrentCodeSends`
  (every input, event message and the finished run)
- negative: `TestJoinHistory_CatchesAHandlerOnTheRunContext` — handlers
  dispatched on the run context fail-fast cancels
- recorder: `TestIntegration_RecordJoinRunHistory`
  (`join_integration_test.go`); `ORK_REQUIRE_JOIN_FIXTURE`

### `run-succeeded-v2.json`, `run-each-v2.json` — the lean event path

The same two runs as `run-succeeded-v1.json` and `run-each-v1.json`, recorded
by a build that records the `lean-event-path` version marker
(`internal/engine/lean_events.go`). Each task's `TASK_STARTED` publishes with
the snapshot URI its `TASK_SCHEDULED` wrote, with no snapshot write of its own,
and the fan-out's item events without a snapshot are published without the
run waiting for each. This is a new command path, so it gets its own fixtures
rather than a re-recording of the v1 ones, which stay on `DefaultVersion`.

- command: `TestRunWorkflow_ReplaysCommittedLeanHistories`
- payload: `TestLeanHistory_TaskStartedCarriesItsScheduledURI` (the recorded
  marker, and every `TASK_STARTED` carrying its `TASK_SCHEDULED`'s URI); the
  v1 payload tests already re-execute both inputs on the lean path
- negative: `TestLeanHistory_CatchesASnapshotWrittenForTaskStarted`, a body
  that records the marker but writes `TASK_STARTED` its own snapshot again
- recorders: `TestIntegration_RecordGoldenRunHistory`
  (`golden_history_integration_test.go`) and
  `TestIntegration_RecordEachRunHistory` (`each_integration_test.go`);
  `ORK_REQUIRE_LEAN_FIXTURES`. Recorded once, against a real cluster, by the
  build that introduced `lean-event-path`; like the v1 fixtures they are meant
  to age, not to be refreshed.

### `run-history-limit-v1.json` — the history guard

A 30-item fan-out followed by a 40-task chain, under a plan whose history
limit is 600 events (`RunPlan.History`, the default reserve). The reserve the
pending chain keeps back brings the line forward, so the guard crosses inside
the fan-out's second dispatch batch, after a yield: the per-command count of a
workflow task decides the exact item. Pins the lazy `run-history-guard`
version marker at the crossing, the cancellation of the items in flight,
every never-started task `CANCELED`, and `RUN_FAILED` owned by no task, with
the workflow failure typed `RUN_HISTORY_LIMIT`. Every earlier fixture's plan
has no `history`, so none of them enters the guard
(`TestCommittedFixtures_DeclareNoNewPlanFields`).

- command: `TestRunWorkflow_ReplaysCommittedHistoryLimitHistory`. The replayer
  sets the history length and size from every recorded `WorkflowTaskStarted`,
  as live execution does, so this is the exact check of the crossing point
- payload: `TestHistoryLimitHistory_PayloadsMatchWhatCurrentCodeSends`. A test
  environment holds one static history length, so it checks the prefix
  (re-executed with the guard off, every input and event before the marker)
  and the terminal sentence's counts, not the crossing
- negatives: `TestHistoryLimitHistory_CatchesAGuardRemoved` (a body without the
  guard); `TestCommittedHistory_HistoryGuardWithoutItsGateIsCaught`
  (`run-succeeded-v2.json` handed a tiny limit replays clean behind the gate,
  and must fail with the version taken without asking)
- recorder: `TestIntegration_RecordHistoryLimitRunHistory`
  (`history_guard_integration_test.go`); `ORK_REQUIRE_HISTORY_LIMIT_FIXTURE`
