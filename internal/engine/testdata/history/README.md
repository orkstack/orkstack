# The golden history fixture

`run-succeeded-v1.json` is a real Temporal history from a successful six-task
run, recorded against a real cluster by a **versioned** worker, so every
workflow task in it carries `VERSIONING_BEHAVIOR_PINNED`. It is the repo's
only old-history-meets-new-code guard, and two tests hold the tree to it on
every `make test`:

- `TestRunWorkflow_ReplaysCommittedV1History` — the COMMAND half. The SDK's
  `WorkflowReplayer` rejects a reordered, added or deleted activity.
- `TestGoldenHistory_ActivityPayloadsMatchWhatCurrentCodeSends` — the PAYLOAD
  half. The replayer never looks inside a payload, so this one re-executes
  `RunWorkflow` from the fixture's own input and compares what today's code
  puts INTO each activity call against what the recorded build put there.

Nothing writes this file except `TestIntegration_RecordGoldenRunHistory`
(`internal/engine/golden_history_integration_test.go`), and only when asked:

```bash
ORK_RECORD_GOLDEN_HISTORY=1 ORK_TEMPORAL_NAMESPACE=ork-<your-agent> \
  go test -tags=integration -p 1 -count=1 ./internal/engine/ \
  -run TestIntegration_RecordGoldenRunHistory
```

**Never hand-edit it.** A fixture edited to agree with the code proves that the
code agrees with itself, which is the one thing this guard exists not to be.

## The baseline was reset at M9

The fixture is meant to AGE: it is recorded once, and every commit after that
is measured against a build genuinely older than itself. That aging is the
whole value, so a re-record throws it away and starts the clock again. Do it
only when the contract legitimately changed, and say so.

It happened once, here, at **M9**. M9 renamed OrkStack's entity from *workflow*
to *orkflow* across the wire contract: `Run`'s two name/version fields became
`orkflow_name` and `orkflow_version`, and `task.ExecuteTaskInput`'s JSON tags
followed (`internal/task/contract.go`). The previous fixture's base64 activity
payloads carried the old spelling, so the payload guard failed — correctly.
Current code no longer sends what that fixture recorded.

Nothing was deployed, so there is no build anywhere that speaks the old
spelling and no migration to keep it alive. Re-recording is the honest move:
the pre-M9 baseline described a contract that no longer exists. The guard
itself was not weakened — both tests still run, unchanged, against this
recording — but the cross-version baseline now starts at M9, and every commit
after M9 is measured against M9.

Evidence for the reset: `proof/M9/`.
