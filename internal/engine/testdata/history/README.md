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
