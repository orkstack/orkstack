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

`RunWorkflow` takes a fifth argument, the carry a run's earlier generation
hands the next when it continues as new. Every fixture's start, and every
child start it recorded, passes four, so the fifth decodes as nil and no
fixture enters the carried path; `TestCommittedFixtures_StartWithFourArgumentsAndNoCarry`
makes that a checked premise, and the command halves replay every fixture
against the five-parameter function.

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
- A fixture names no host. A real history stamps the recording machine into
  every SDK `identity` (`<pid>@<host>@`) and into the sticky task queue's name
  (`<host>:<uuid>`); neither is an input to either half of the guard, so a
  committed fixture carries them as `0@fixture@` and `fixture:<uuid>`, and
  nothing else about it changes. `TestFixtures_CarryNoHostIdentity` fails on
  a fixture that still names its host — a freshly recorded one — and
  `ORK_SCRUB_FIXTURES=1 go test ./internal/engine/ -run TestFixtures_CarryNoHostIdentity`
  scrubs it. This is the one mechanical rewrite a fixture may receive; it is
  not the hand-editing the first rule forbids, because it changes nothing the
  fixture says about the engine.

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
  (`history_guard_integration_test.go`); `ORK_REQUIRE_HISTORY_LIMIT_FIXTURE`.
  Unlike the other recorders it never skips: without
  `ORK_RECORD_GOLDEN_HISTORY` it records the same run into the proof
  directory (`history-limit/recorded-history.json`) and replays that fresh
  history against current code, leaving this fixture untouched; with it, it
  also rewrites this file

### `run-history-guard-lookahead-v1.json` — the guard's lookahead at a dispatch

A 30-task chain, every dispatch carrying a 4 KiB input, under a plan whose
history limit is 480 KiB (no continuation, no cap — the history-limit
fixture's shape). A few tasks in, the run's history plus what its failure path
keeps back still fits, but would not once the next dispatch's command —
counted at the plan's 256 KiB values bound — is recorded: it pins the lazy
`run-history-guard-lookahead` marker (`history_guard_lookahead.go`), asked
only where the lookahead decides otherwise than the check without it, the
guard's own marker, and the terminal `RUN_FAILED` owned by the run, typed
`RUN_HISTORY_LIMIT`. A build before the lookahead dispatched the task and
checked only after, which could leave the failure path less room than it
keeps back; the cluster then terminated the run (continuation chaos C24).

- command: `TestRunWorkflow_ReplaysCommittedHistoryGuardLookaheadHistory`
- payload: `TestHistoryGuardLookaheadHistory_PayloadsMatchWhatCurrentCodeSends`
  (the prefix before the marker re-executed with the guard off: every input
  and event; and the recorded terminal event)
- negative: `TestHistoryGuardLookaheadHistory_CatchesTheLookaheadRemoved`
- recorder: `TestIntegration_RecordHistoryGuardLookaheadHistory`
  (`history_guard_lookahead_integration_test.go`);
  `ORK_REQUIRE_LOOKAHEAD_FIXTURE`. It never skips.

### `run-continued-g0-v1.json`, `run-continued-g1-v1.json` and `run-continued-g0-v1.objects/` — continuing as new

One run recorded across a switch (`internal/engine/continuation.go`), under a
plan whose continuation budget is 70 events against a 480-event history
limit, read back in 64-byte chunks. Its definition is `approve` (an await
with no needs, open from the start), `a → b`, `list → convert` (a fan-out
over six parts, `max_parallel: 2`) and `merge` (needs `convert` and
`approve`). Generation 0 crosses its budget after `list` has finished, while
the fan-out refills — so no idle point precedes the crossing — holds new
items at its hold line, waits for the two running ones, and switches with
`approve` still open; the resolution is sent after the switch, and
generation 1 never reaches its own budget.

- `run-continued-g0-v1.json` ends with `CONTINUED_AS_NEW`. It pins the lazy
  `continue-as-new` marker, the held refill, the `WriteEventSegment`,
  `WriteSwitchSnapshot` and `WriteCheckpoint` markers, the `UpsertMemo` after
  them, and the switch itself, its run-id argument empty.
- `run-continued-g1-v1.json` starts with the carry (the fifth argument). It
  pins the `ReadContinuationChunk` markers in their batches of
  `chunks_per_task`, with the yield timers between them, the await's deadline
  timer recreated with its remaining duration, the fan-out resumed at its
  next index, and the resolution and completion.
- `run-continued-g0-v1.objects/` holds the three objects generation 0 wrote —
  `segment.pb.gz`, `switch-snapshot.pb.gz`, `checkpoint.pb.gz` — read back
  from MinIO byte for byte by the recording, so the payload half checks the
  carry against the bytes it names.

- command: `TestRunWorkflow_ReplaysCommittedContinuedHistories`, replaying
  each generation as the execution of the run's own workflow id (a carried
  generation reads its run id from there)
- payload: `TestContinuedHistory_PayloadsMatchWhatCurrentCodeSends` —
  generation 0's prefix re-executed with the history length held at 0 (every
  input and event before the marker); generation 0's carry against the
  recorded history and the three objects; generation 1 fully re-executed from
  its recorded arguments with its recorded chunks fed back (every input and
  every event, and no continuation path taken)
- negatives: `TestContinuedHistory_CatchesASwitchWithoutItsGate` (every
  earlier fixture, against a body that switches at every idle point with the
  version check removed); `TestContinuedHistory_CatchesATamperedCheckpoint`
  (one altered chunk is never adopted: `RUN_STATE_CORRUPT`);
  `TestContinuedHistory_CatchesAnOffByOneSequence`;
  `TestContinuedHistory_CatchesAChangedPinnedBatchSize` (generation 1's plan
  rewritten with `chunks_per_task: 4` fails to replay: the pinned value, not a
  code constant, decides the batches)
- recorder: `TestIntegration_RecordContinuedRunHistories`
  (`continuation_integration_test.go`); `ORK_REQUIRE_CONTINUED_FIXTURES`.
  Like the history-limit recorder it never skips: without
  `ORK_RECORD_GOLDEN_HISTORY` it records into the proof directory and replays
  what it recorded, leaving these fixtures untouched. It tries a short list
  of (history limit, budget) pairs and keeps the first whose run lands the
  shape above; the values committed are the ones that landed.

### `run-in-flight-cap-v1.json` — the in-flight cap

A 24-item fan-out without `max_parallel`, under a plan whose in-flight cap is
16 (`RunPlan.InFlight`). Pins the lazy `run-in-flight-cap` marker, asked the
first time the cap holds, and every refill the cap held back. Recorded by
`TestIntegration_RecordContinuedRunHistories` beside the continued run.

- command: `TestRunWorkflow_ReplaysCommittedInFlightCapHistory`
- payload: `TestContinuedHistory_PayloadsMatchWhatCurrentCodeSends`
  ("in-flight cap": fully re-executed; the cap reads no history length)

### `run-cancel-before-dispatch-v1.json` — a cancel seen at a dispatch

A twelve-task chain under the plan of the continuation chaos suite's C4
(a 120-event budget against a 1,000-event limit). Its first switch's
checkpoint write was held while the operator's cancel was recorded, so the
server refused the continue-as-new (`UNHANDLED_COMMAND`); the retried
workflow task saw the cancel where the run would have dispatched its next
task. It pins the lazy `cancel-before-dispatch` marker (`cancel.go`), asked
only when the run's context is already canceled at a dispatch point, and the
canceled ending that follows it: no task dispatched, every remaining task
`CANCELED` before it started, and the run's own `RUN_CANCELED`, the stream
contiguous. A build before the check dispatched the task instead, its emit on
the canceled context burned a sequence number, and the run ended owned by the
task.

- command: `TestRunWorkflow_ReplaysCommittedCancelBeforeDispatchHistory`
- payload: `TestCancelBeforeDispatchHistory_PayloadsMatchWhatCurrentCodeSends`
  — re-executed from its own arguments with its recorded results, switching
  where the recorded run did and canceled during that switch's checkpoint
  write: every input and every event equals the recorded one
- negative: `TestCancelBeforeDispatchHistory_CatchesTheCheckRemoved` (replayed
  with the check skipped, the dispatch it never made fails the replay)
- recorder: `TestIntegration_RecordCancelBeforeDispatchHistory`
  (`cancel_before_dispatch_integration_test.go`); `ORK_REQUIRE_CANCEL_FIXTURE`.
  It never skips: without `ORK_RECORD_GOLDEN_HISTORY` it records into the
  proof directory and replays what it recorded.

Like the continued-run fixtures it was recorded with the writer's plan fields, so the
premise and negative tests about the histories that predate the writer pass
it by (`recordedWithTheWriter`).

Every fixture recorded before the history guard carries no `history`,
`continuation` or `in_flight` in its plan, so none of them enters the writer,
a hold or the cap (`TestCommittedFixtures_DeclareNoNewPlanFields`).

### `run-definition-ref-v1.json` and `run-definition-ref-v1.objects/` — a definition passed by reference

Generation 0 of a five-task fan-in whose definition travels by reference
(`docs/DEPLOY.md` §1, `engine.definition_by_reference`): the start carries the
stub — name, version, namespace and digest, no tasks — and the plan names the
definition object (`RunPlan.Definition`, no URI). Its plan pins 1 KiB chunks,
two per workflow task, so the history holds the lazy
`definition-by-reference` marker as its first command, then the definition's
six `ReadContinuationChunk` markers in three batches with a 1 ms yield timer
between them (`definition_ref.go`, `loadDefinition`), and only then
`RUN_CREATED` and the run's own tasks. A build before the gate ignores the
plan field, finds a stub with no tasks and ends the run `INVALID_ORKFLOW`; a
history it recorded carries no marker and replays that branch.

`run-definition-ref-v1.objects/definition.pb.gz` is the definition object
the recording wrote and read back from MinIO, byte for byte: gzip members of
`Orkflow`, part 0 the definition with its tasks cleared, then task-only
parts. Its bytes are exactly the recorded chunks, concatenated.

- command: `TestRunWorkflow_ReplaysCommittedDefinitionRefHistory`
- payload: `TestDefinitionRefHistory_PayloadsMatchWhatCurrentCodeSends` — the
  object checked against the recorded chunks, the plan's reference and the
  stub, then the run re-executed from its own arguments with the recorded
  chunks and results fed back: every input and every event equals the
  recorded one
- negative: `TestDefinitionRefHistory_CatchesAChangedPinnedChunkSize` (the
  plan's `chunk_bytes` doubled, the reads it issues no longer match)
- recorder: `TestIntegration_RecordDefinitionRefHistory`
  (`definition_ref_integration_test.go`). It never skips: without
  `ORK_RECORD_GOLDEN_HISTORY` it records into the proof directory and replays
  what it recorded.

It was recorded with the writer's plan fields, so the premise and negative
tests about the histories that predate the writer pass it by
(`recordedWithTheWriter`).

### `run-snapshot-overlay-v1.json` and `run-snapshot-overlay-v1.objects/` — snapshot overlays

One run of a chain of plain tasks around a fan-out of 24 items, one at a
time (`prep` → `convert` → `a` → `merge`, 44 declared rows), whose plan
carries a snapshot-overlay policy (`docs/DEPLOY.md` §1,
`engine.snapshot_overlays`) with a floor of 16 rows, a chain of at most 6
overlays and at most 256 changed rows. The history holds the lazy
`snapshot-overlay` marker at the run's first snapshot, then a
`WriteSnapshotOrOverlay` marker for every non-terminal snapshot from both
write sites — an event's own (`emitWithSnapshot`) and the fan-out's fresh item
snapshots (`emitItem`) — each recording what it wrote (`SnapshotWritten`):
full `{seq}.pb` snapshots and, between them, overlays, ten for item-fresh
events and twelve for emits. The terminal `RUN_SUCCEEDED` snapshot is a
`WriteSnapshot` marker; with it the run wrote five full snapshots. A build before the gate writes every snapshot
through `WriteSnapshot`; a history it recorded carries no marker and replays
that branch. The run is sequential so that every event lands at the same
sequence when the payload half re-executes it.

`run-snapshot-overlay-v1.objects/` holds every full snapshot and overlay the
recording wrote, read back from MinIO byte for byte, each named by its key's
leaf (`{seq}.pb`, `{seq}.ovl`).

- command: `TestRunWorkflow_ReplaysCommittedSnapshotOverlayHistory`; and
  `TestSnapshotOverlay_DiffRunsInTheLocalActivityOnly` and
  `TestSnapshotOverlay_ItemFreshEmitReplays` — the replay runs no diff, and
  the base the workflow follows is exactly what the recorded results make
  it, item-fresh writes included
- payload: `TestSnapshotOverlayHistory_PayloadsMatchWhatCurrentCodeSends` —
  the run re-executed from its own arguments with its recorded results and
  snapshot writes fed back: every input and every event equals the recorded
  one, every overlay write is handed the recorded base and the plan's policy,
  and every companion object, read as a reader reads it (an overlay through
  its recorded base), is the run current code hands that write, times aside
- negative: `TestSnapshotOverlayHistory_CatchesAChangedPinnedMinRows` (the
  plan's `min_rows` raised over the run's rows: the snapshot activity no
  longer matches, `TMPRL1100`)
- recorder: `TestIntegration_RecordSnapshotOverlayHistory`
  (`snapshot_overlay_integration_test.go`). It never skips: without
  `ORK_RECORD_GOLDEN_HISTORY` it records into the proof directory, checks
  every overlay reads back, and replays what it recorded.

It was recorded with the writer's plan fields, so the premise and negative
tests about the histories that predate the writer pass it by
(`recordedWithTheWriter`).

### `run-refused-route-{plain,pinned,handler}-v1.json` and `run-refused-route-legacy-*-v1.json` — a task whose pool the plan cannot route

A hand-wired plan with no route for a task's pool (`INVALID_POOL`), reached
three ways: a plain dispatch, a pin's opening, and a definition with an
`any_failed` handler. The gated fixtures are recorded by today's code: they
carry the lazy `refused-route-task-failed` marker (`refuseRoute`,
`workflow.go`) and the `TASK_FAILED` event the refused task now emits. The
`legacy` fixtures are recorded with the gate switched off
(`DisableRefusedRouteGateForTest`), which is the command sequence of every
build before it, and must keep replaying through the `DefaultVersion` branch.

- command: `TestRunWorkflow_ReplaysCommittedRefusedRouteHistories`
- negative: `TestRefusedRouteHistories_CatchTheGateRemoved` (plain and pinned;
  with a handler the old path already emitted the same commands, only the
  marker differs)
- behaviour: `TestRunWorkflow_ARefusedRouteIsRecordedOnceInTheRowAndTheStream`
- recorder: `TestIntegration_RecordRefusedRouteHistories`
  (`refused_route_integration_test.go`). It never skips. The fixtures are
  committed, so the replay tests above fail, never skip, when one is missing;
  only this recorder is opt-in (`ORK_RECORD_GOLDEN_HISTORY=1` writes them).

### `run-pin-creation-timeout-v1.json` and `run-pin-creation-timeout-legacy-v1.json` — a pin opened under a plan's creation timeout

An Orkflow that opens two pins, one after the other with an unpinned task
between them (`twoPinsDocument`), run to completion under a plan that carries
`pin_creation_timeout_seconds: 30`. The gated fixture is recorded by today's
code: it carries the lazy `pin-creation-timeout` marker once
(`pinCreationTimeoutFor`, `workflow.go`, asked where the first pin opens), and
both pins' session creation activities were scheduled with a 30 second
`ScheduleToStartTimeout`, the plan's number. The `legacy` fixture is recorded
from the same plan with the gate switched off
(`DisablePinCreationTimeoutGateForTest`), which is the command sequence of
every build before the gate: no marker, and both creation activities scheduled
with the five minutes those builds hardcoded, whatever the plan says. It must
keep replaying through the `DefaultVersion` branch. The unpinned task between
the pins gives the run a moment with nothing in flight, so the continuation
tests that switch every fixture at each idle point carry its decision across
a generation.

Temporal's replayer compares no timeout, so the replay tests replay with an
interceptor and compare the `ScheduleToStartTimeout` current code sends on
replay with the one the history recorded, and the version it is answered with
the recorded marker.

- command: `TestRunWorkflow_ReplaysCommittedPinCreationTimeoutHistories`
- negative: `TestPinCreationTimeoutHistories_CatchTheGateRemoved` (the gated
  fixture replayed with the gate skipped is sent five minutes where it
  recorded thirty seconds)
- behaviour: `TestRunWorkflow_APinWaitsForThePlansCreationTimeout`,
  `TestRunWorkflow_AGateOffPinKeepsTheLegacyFiveMinutes`,
  `TestRunWorkflow_TheCreationTimeoutGateIsAskedOnlyWhereAPinOpens`
- recorder: `TestIntegration_RecordPinCreationTimeoutHistories`
  (`pin_creation_timeout_integration_test.go`). It never skips. The fixtures
  are committed, so the replay tests above fail, never skip, when one is
  missing; only this recorder is opt-in (`ORK_RECORD_GOLDEN_HISTORY=1` writes
  them).
