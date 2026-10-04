# The golden history fixture

`run-succeeded-v1.json` is a real Temporal history from a successful six-task
run, recorded against a real cluster by a **versioned** worker, so every
workflow task in it carries `VERSIONING_BEHAVIOR_PINNED`. It is the repo's
only old-history-meets-new-code guard: it is recorded once and then AGES, so
every commit after the recording is measured against a build genuinely older
than itself. Two tests hold the tree to it on every `make test`:

- `TestRunWorkflow_ReplaysCommittedV1History` — the COMMAND half. The SDK's
  `WorkflowReplayer` rejects a reordered, added or deleted activity.
- `TestGoldenHistory_ActivityPayloadsMatchWhatCurrentCodeSends` — the PAYLOAD
  half. The replayer never looks inside a payload, so this one re-executes
  `RunWorkflow` from the fixture's own input and compares what today's code
  puts INTO each activity call against what the recorded build put there.

`TestCommittedHistory_CatchesAnEmitInsertedIntoTheStream` (`replay_test.go`)
proves the command guard can see an inserted activity, so the fixture is worth
having from the day it is recorded and not only from the next change onward.

Nothing writes this file except `TestIntegration_RecordGoldenRunHistory`
(`internal/engine/golden_history_integration_test.go`), and only when asked:

```bash
ORK_RECORD_GOLDEN_HISTORY=1 ORK_REQUIRE_INFRA=1 \
  ORK_TEMPORAL_NAMESPACE=ork-<your-agent> \
  go test -tags=integration -p 1 -count=1 ./internal/engine/ \
  -run TestIntegration_RecordGoldenRunHistory
```

**Never hand-edit it.** A fixture edited to agree with the code proves that the
code agrees with itself, which is the one thing this guard exists not to be.

**The rule:** the fixture is re-recorded only when what the engine puts into an
activity — the command stream or an activity's input — genuinely changed, and
never to turn a red test green (AGENTS.md §7.2). The commit that re-records it
says why.

## The awaited-run fixtures

`run-awaited-resolved-v1.json` and `run-awaited-timeout-v1.json` hold the
await path still: the first Timer and the first Signal `RunWorkflow` ever
issued (`docs/DSL.md` "Awaited tasks"). Each is a real history of the run
`a → await b → c`, recorded once by a **versioned** worker against a real
cluster — the first completed through a resolution delivered with
`TemporalStarter.SignalAwait`, the second left to time out after two seconds.
Three tests hold the tree to them on every `make test`:

- `TestRunWorkflow_ReplaysCommittedAwaitedHistories` — the command half, for
  both files.
- `TestAwaitedHistory_ActivityPayloadsMatchWhatCurrentCodeSends` — the payload
  half: the resolved fixture re-executed from its own input, with its own
  activity results and its own recorded resolution fed back.
- `TestAwaitedHistory_CatchesAMovedTimer` — the replay of the resolved fixture
  against a body that starts the deadline timer after the `TASK_AWAITING`
  emit instead of before it, which must fail.

Until both files are committed those tests skip and name the command that
records them (set `ORK_REQUIRE_AWAITED_FIXTURES=1` to make their absence a
failure). Nothing writes them except `TestIntegration_RecordAwaitedRunHistories`
(`internal/engine/await_integration_test.go`), and only when asked:

```bash
ORK_RECORD_GOLDEN_HISTORY=1 ORK_REQUIRE_INFRA=1 \
  ORK_TEMPORAL_NAMESPACE=ork-<your-agent> \
  go test -tags=integration -p 1 -count=1 ./internal/engine/ \
  -run TestIntegration_RecordAwaitedRunHistories
```

The same rules apply as to `run-succeeded-v1.json`: never hand-edited, and
re-recorded only when the await path's command stream or payloads genuinely
change, with a commit that says so.

## The fan-out fixture

`run-each-v1.json` holds the fan-out path still (`docs/DSL.md` "Fan-out"): the
first `CountEachItems` local-activity marker, the first item activities, the
first item events published without a snapshot of their own, and the first
`EachUpstream` reference `RunWorkflow` ever issued. It is a real history of
`list → convert (each: over list's six parts, max_parallel: 2, item 3 retried
once) → merge`, recorded once by a **versioned** worker against a real
cluster. Four tests hold the tree to it on every `make test`:

- `TestRunWorkflow_ReplaysCommittedEachHistory` — the command half, plus the
  ExecuteTask schedule the replay produces (task and item, in order), read
  through an interceptor: the replayer itself matches an activity command by
  type and id and never by input, so a dispatch that picked different items
  would otherwise pass.
- `TestEachHistory_ActivityPayloadsMatchWhatCurrentCodeSends` — the payload
  half: the fixture re-executed from its own input, with each recorded
  ExecuteTask result fed back by task and item, and the recorded count.
- `TestEachHistory_ReplayNeverRecountsTheArray` — the replay reads the count
  from its marker and never calls `CountEachItems`, whatever a store would now
  answer: the replayed run has the recorded six items.
- `TestEachHistory_CatchesAReorderedDispatch` — the replay against a body that
  dispatches items in descending order, which must be caught.

Until the file is committed those tests skip and name the command that
records it (set `ORK_REQUIRE_EACH_FIXTURE=1` to make its absence a failure).
Nothing writes it except `TestIntegration_RecordEachRunHistory`
(`internal/engine/each_integration_test.go`), and only when asked:

```bash
ORK_RECORD_GOLDEN_HISTORY=1 ORK_REQUIRE_INFRA=1 \
  ORK_TEMPORAL_NAMESPACE=ork-<your-agent> \
  go test -tags=integration -p 1 -count=1 ./internal/engine/ \
  -run TestIntegration_RecordEachRunHistory
```

The same rules apply as to `run-succeeded-v1.json`: never hand-edited, and
re-recorded only when the fan-out path's command stream or payloads genuinely
change, with a commit that says so.

## The composed fixtures

`run-composed-parent-v1.json` and `run-composed-child-v1.json` hold the
composition path still (`docs/DSL.md` "Composition" and "Outputs"): the first
`ExecuteChildWorkflow` `RunWorkflow` ever issued, the first
`RenderComposedValues` and `StoreComposedValues` local-activity markers, and
the first declared-outputs render. They are real histories of the shipped
examples — `composed-release` composing `release-block@0.1.0`, whose
`outputs:` the parent's own `outputs:` projects — recorded once, together, by
a **versioned** worker against a real cluster. Three tests hold the tree to
them on every `make test`:

- `TestRunWorkflow_ReplaysCommittedComposedHistories` — the command half, for
  both files.
- `TestComposedHistory_PayloadsMatchWhatCurrentCodeSends` — the payload half:
  the parent re-executed from its own input, with its recorded task results,
  render and store markers and its child's recorded result fed back, and
  every ExecuteTask input and the child start — id, task queue, parent-close
  policy, memo, search attributes and its four input payloads — compared with
  the recorded `StartChildWorkflowExecutionInitiated`.
- `TestComposedHistory_CatchesAChildStartWithoutItsRender` — the replay of the
  parent against a body that starts the child without first recording its
  input render, which must fail.

Until both files are committed those tests skip and name the command that
records them (set `ORK_REQUIRE_COMPOSED_FIXTURES=1` to make their absence a
failure). Nothing writes them except `TestIntegration_RecordComposedRunHistory`
(`internal/engine/compose_integration_test.go`), and only when asked:

```bash
ORK_RECORD_GOLDEN_HISTORY=1 ORK_REQUIRE_INFRA=1 \
  ORK_TEMPORAL_NAMESPACE=ork-<your-agent> \
  go test -tags=integration -p 1 -count=1 ./internal/engine/ \
  -run TestIntegration_RecordComposedRunHistory
```

The same rules apply as to `run-succeeded-v1.json`: never hand-edited, and
re-recorded only when the composition path's command stream or payloads
genuinely change, with a commit that says so.
