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
- This release cannot resume every failed run. It refuses one
  (`RELEASE_UNSUPPORTED`, whose cause says which) whose tasks or child runs'
  tasks hold an identity, or that is held to a concurrency limit. Run it again
  instead.
- A long run carries on in a fresh history now and then. A resume reaches only
  what ended since it last did: a task that finished before that cannot be
  retried from (`RETRY_FROM_TOO_EARLY`), and its result is kept.
- A composed task that would have to run again refuses the resume
  (`COMPOSED_TASK`), and so does a run started by another run's composed task
  (`CHILD_RUN`).
- A resume needs `run.resume` and `run.submit` on the run's Orkflow; a preview
  needs `run.read` ([ACCESS.md](ACCESS.md)).
