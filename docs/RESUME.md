# Resuming a run

No build resumes runs yet: `ResumeRun` answers "this engine resumes no runs",
and no run offers to be resumed. What follows is how a resume works once it is
switched on.

A failed run can be resumed instead of started again. Its failed tasks, and
everything downstream of them, run again; every other task keeps its result.
The run keeps its id, its event stream continues after a `RUN_RESUMED` event,
and it ends again like any run.

```text
orkctl run resume <run-id> [--from TASK] [--reason TEXT] [--dry-run] [--yes] [--wait]
```

On the run page, **Resume** in the header does the same, and a task's menu
offers **Resume** (on a failed task) and **Retry from here**.

## Resume and Retry from here

- **Resume** reruns the failed tasks and every task downstream of them.
- **Retry from here** (`--from TASK`) also reruns the chosen task and every
  task downstream of it. Pick it when a task succeeded with a result you do
  not want to keep.

What else must rerun is added for you, and the preview says why:

| Section | Why the task is in it |
|---|---|
| You asked for these | It failed, it is the task you chose, or it is downstream of one of those. |
| Added because the resume must | Its saved result is gone, it never finished, another member of its pin reruns, or the task its fan-out is over reruns. |
| Waits again | An awaited task with no answer yet, or a sleep that had not woken: it waits again, in full, once what it needs is done. An answer sent before it is open again is ignored. |
| Answered and on record | An awaited task answered before the run ended, whose answer is used. |
| Kept | Its saved result is read back; it runs no container. |
| Already done | An awaited task or a composed task whose result is kept, or a sleep that woke. |
| Stays skipped | It was skipped, and nothing it depends on reruns. |
| Kept from an earlier part of the run, not run again | It finished before the run carried on, in an earlier part of its history: its result is kept as it is and is not read back again. |

A fan-out's items keep their own results: only the items that failed, or never
ran, run again, unless the task the fan-out is over reruns, in which case every
item does. A fan-out over an artifact that is kept whole shows no lines index,
because the resume does not count its lines again.

A cached task that succeeded before and runs again reads no memo, which would
hand back the result it runs again to replace; its new result refreshes the
memo. One that never succeeded looks its memo up as on any run.

A kept result that cannot be read back fails its task and the run, before
anything runs again. Resume the run again: the result is checked once more. The
preview shows it as "to be read back" when it is there, and its task among those
that run again when it is not.

A run with a `timeout:` gets a fresh one, in full, when its tasks start again:
the time before that, spent reading results back or waiting for a concurrency
slot, is not counted. So `timeout:` bounds each resume, not the run's life.

## What a resume asks before it goes on

The command shows the plan first. `--dry-run` stops there and changes
nothing.

- When a task that **succeeded** before would run again, the command asks you
  to type `yes`, because its external side effects repeat. `--yes` answers for
  a script. With no terminal and no `--yes` it stops, and `--json` never asks.
  When nothing that succeeded would run again, it goes on without asking; the
  notes it printed (a pause it set aside, a pin's host still holding its old
  slot) need no answer.
- The confirm carries the plan it showed. If the run or the plan changed in
  between, the resume is refused (`PLAN_CHANGED`, exit 76) and nothing is done:
  `--yes` never accepts a plan nobody saw.
- When an earlier attempt may still be running on an Orker that is gone, the
  resume needs `--acknowledge-unknown-attempts` as well. `--yes` does not
  imply it.
- What runs again is held to the rules a new run is held to now, not those of
  when the run was submitted: the namespace's admission, its image policy and
  pools, and, for a task that asks for an identity, a credential or a secret
  through a grant, the namespace's identity policy. A run that holds an
  identity also needs every Orker's storage credential confirmed scoped and
  its pools to serve task identity (`IDENTITY_POLICY_CHANGED`,
  `RERUN_STORE_UNSCOPED`, `RERUN_POOL_UNSERVED`).
- A run that holds an identity and that a schedule or trigger started is held to
  the binder its admission recorded when it was submitted, whatever version the
  schedule names: that binder must still be allowed to submit the Orkflow, and
  its consent must cover what the run's version asks for now. The consent
  judged is the trigger's own, while that binder still binds it, or the newest
  entry of the schedule for this Orkflow by that binder. A consent changed or
  revoked since the run was submitted refuses the resume
  (`IDENTITY_POLICY_CHANGED`); consent again, or run it again. A run an event
  source started records no binder, so it is run again.

## Accepting a gap

A run whose stored events go on far above its ending is refused
(`STATE_UNREADABLE`, cause `events_above_terminal`): some of its events were
never stored. An administrator (`run.resume.accept_gap`) can accept that. In
the run page's Resume dialog, a checkbox appears with its consequence when the
preview is refused for it. On the command line, pass the highest stored event
the refusal names: `--accept-sequence-gap 57`. The resume goes on above it, and
the events that were never stored stay missing from the run's history.

The acceptance is sent with the preview and the confirm, so the plan you are
shown is the plan it applies to. If the run's highest stored event moves in
between, the resume is refused (`PLAN_CHANGED`) and names the new one.

## Made once

A resume is made once even when its answer is lost, because the cluster applies
none after its deadline. A proxy between the engines and the cluster that drops
a request's deadline (gRPC's `grpc-timeout`) breaks that, and the engine cannot
detect it ([DEPLOY.md](DEPLOY.md#a-proxy-between-the-engines-and-the-cluster),
"A proxy between the engines and the cluster").

A run canceled before any of its resume was carried out ends canceled, with
its tasks as its earlier ending left them; nothing ran again, and a canceled
run cannot be resumed.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Resumed, or already resumed by the same plan; with `--wait`, the run succeeded |
| 1 | An error, or a refusal no resume can fix: run it again instead |
| 2 | With `--wait`: the run failed |
| 3 | With `--wait`: the run was canceled |
| 4 | The resume's outcome is not known: it was sent and it is not yet known whether the run was resumed, or the run was resumed and has not carried on yet, or the confirm failed or lost its answer and may not have been sent at all. The message says which; check the run, then try again |
| 5 | With `--wait`: the run ended again before any task ran again |
| 75 | Refused for now: try again after the time the message gives |
| 76 | Refused: preview again and decide |

A refusal names its reason, listed in [REASONS.md](REASONS.md).

## Limits

- Only a failed run can be resumed.
- A run is resumed at most **20** times, counting every resume that got going,
  including one that then ended waiting for a concurrency slot. After that,
  run it again.
- This release cannot resume every failed run. It refuses one held to a
  concurrency limit (`RELEASE_UNSUPPORTED`, whose cause says which). Run it
  again instead.
- A run whose tasks or child runs' tasks hold an identity keeps only what the
  engine recorded for it, as the record stood when the run ended: an answer
  recorded after that is not used, and is asked for again. The store's clock
  and the cluster's may differ by a second or two; an answer given in the last
  second before the run ended can be asked for again because of it.
- A long run carries on in a fresh history now and then. A resume reaches only
  what ended since it last did: a task that finished before that cannot be
  retried from (`RETRY_FROM_TOO_EARLY`), and its result is kept.
- A composed task that would have to run again refuses the resume
  (`COMPOSED_TASK`), and so does a run started by another run's composed task
  (`CHILD_RUN`). In a run whose results the engine records, a composed task
  that failed because its values could not be recorded
  (`CHILD_VALUES_UNSTORED`, `CHILD_VALUES_NOT_RECORDED`), or that a force cancel
  left running, is kept when its child succeeded and the engine recorded the
  child's outputs, and the tasks it needs are kept: its values are recorded
  from those outputs when the resume is confirmed. While that child is still
  open the resume waits (`STILL_STOPPING`, `child_running`), and the message
  says what the child waits on and how to end it, leading with ending it once
  the child has been open longer than `engine.resume.child_wait_hint`. A child
  that did not succeed, or whose outputs are not on record, refuses
  `COMPOSED_TASK` (`child_not_succeeded`, `child_outputs_unavailable`). Such a
  task is kept from its child only on its run's first resume: a run resumed
  before refuses `COMPOSED_TASK` (`resumed_before`), since a composed task runs
  only as submitted and its values are recorded under the run as submitted. What a
  kept composed task passed its child is held to the records of the tasks that
  produced it, or to what the run received from the run above it.
- A resume needs `run.resume` and `run.submit` on the run's Orkflow; a preview
  needs `run.read` ([ACCESS.md](ACCESS.md)).
