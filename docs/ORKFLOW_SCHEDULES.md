# Orkflow schedules

A **schedule** is an object bound to exactly one Orkflow — a namespace, an
Orkflow name, an optional version pin, an input — that starts a run of that
Orkflow whenever a Temporal Schedule fires. It is the third run starter beside
`orkctl run submit` and a trigger ([ORKFLOW_TRIGGERS.md](ORKFLOW_TRIGGERS.md)),
and it starts runs the way both of them do: through the ordinary submit
preparation. `AGENTS.md` §2.1 names the mechanism — *"Cron scheduling must
use Temporal Schedules"* — and this document is how OrkStack uses it.

```text
Temporal Schedule  ──fires──>  ScheduleFireWorkflow (engine, ork-tasks)
                                   │  local activity PrepareScheduledRun
                                   │  = the ordinary submit preparation,
                                   │    with origin {SCHEDULE, sch-…}
                                   └──child──> RunWorkflow
                                               (an ordinary run, ParentClosePolicy ABANDON)
```

## Vocabulary

- **Schedule** — the user-facing object: a name (a slug, unique within its
  namespace), a namespace, an Orkflow, an optional version pin, an input, a
  description and a schedule spec.
- **Schedule spec** — *when*: cron expressions and/or an interval, a time
  zone, jitter, a start and end, an overlap policy, a catch-up window and
  pause-on-failure.
- **Fire** — one occasion on which the schedule acted: on time, late within
  the catch-up window, by backfill, or by hand (`orkctl schedule fire`). The
  word *trigger* is never used for it: a trigger is the inbound HTTP starter.
- **Nominal fire time** — the instant the spec named (jitter included), as
  opposed to when the fire happened. A backfilled fire's nominal time is in
  the past. It is the started run's `origin.reference`.
- **Overlap policy** — what happens when a fire is due while the previous run
  of the schedule is still executing: `skip`, `buffer-one`, `buffer-all`,
  `cancel-other`, `allow-all`. Each is Temporal's `ScheduleOverlapPolicy` of
  the same name, applied to the **run**.
- **Refusal** — a fire that started no run, and why: the ordinary submit
  refusal (unknown Orkflow or version, held namespace, no live Orker, …).

No user-facing surface names a Temporal Schedule id, a workflow id, a task
queue, a memo or a search attribute. [DEPLOY.md](DEPLOY.md) ("Schedules: what
the cluster shows") is the operator's view of the cluster side.

## Temporal owns every piece of schedule state

**OrkStack stores nothing for a schedule.** The Temporal Schedule carries the
authored record (as its action's argument) and its identity (in its memo).
Paused state, note, next fire times, recent fires and counters are read from
the cluster on every call. There is no key family under `metadata/`, no second
copy of the cron expression and no "last fired at" column
([STORAGE.md](STORAGE.md)). A record in object storage beside the schedule was
rejected: two stores mean two writes per change, a crash window between them,
an orphan state to render, and a copy a `temporal schedule update` could
silently diverge from. One store cannot disagree with itself.

- A schedule's cluster id is derived from `(namespace, name)`, so a name is
  unique within a namespace atomically: the cluster refuses a second create.
  A schedule of another namespace is NotFound.
- A schedule's `id` (`sch-{16 hex}`) is opaque. It survives an update and
  changes on delete-then-create, so `orkctl run list --origin schedule:<id>`
  names one schedule's lifetime.
- Overlap, catch-up, jitter, pause, note, backfill and manual fire are the
  cluster's own. OrkStack never computes a next fire time, never decides
  whether a fire overlaps, never buffers or retries a fire, and never retries
  a failed run.
- `terminate-other` is not offered: termination skips workflow code, so a
  terminated fire would leave its run running while the policy claimed
  otherwise. Its number, 5, is reserved in `OverlapPolicy`, and the engine
  converts through an explicit switch that can never produce it.
- An unset overlap on a spec is recorded as `skip`, the cluster's default, so
  no read returns an unspecified policy. On a fire or a backfill an unset
  overlap means **no override** — the schedule's own policy applies.
- The input is a JSON object stored as given and submitted as given at every
  fire. Nothing is templated into it: the fire time is on the run's origin.

## A fire is an ordinary run

Every fire runs `ScheduleFireWorkflow`, which:

1. reads the nominal fire time the cluster stamped on it;
2. runs the ordinary submit preparation as a local activity — the namespace,
   the Orkflow and the version pointer resolved **at fire time** (a pinned
   version never falls through to current), admission, the pool plan with
   liveness, the replay-pinned plan — with origin
   `{SCHEDULE, <schedule id>, <nominal time, RFC 3339 UTC>, <schedule name>, ""}`
   (a schedule fires on its own behalf, so there is no principal; who changed
   the schedule is the audit trail's record);
3. records the run id (or the refusal) in its own memo;
4. starts the run as its child with an ordinary `ork-…` id,
   `ParentClosePolicy ABANDON` and `WaitForCancellation`, and waits for it.

**Why the fire outlives the run it starts.** Temporal applies an overlap
policy to the schedule's action workflow. An action that merely submitted a
run and exited would make every policy `allow-all` — it would be gone before
the run's first task started. An action that *was* `RunWorkflow` would freeze
its arguments at schedule creation: no version resolved at fire time, no
admission check, no `ork-…` run id. So the fire prepares, starts the run as a
child, and lives exactly as long as it. Consequences:

- a run that fails fails its fire, which is what `pause_on_failure` reads;
- cancelling a fire (by `cancel-other`) cancels its run through the path
  `orkctl run cancel` uses, and the fire waits until the run has actually
  ended, so a buffered fire starts after it is gone;
- terminating a fire by hand leaves its run executing to completion
  (`ABANDON`): an accepted execution cannot disappear;
- `orkctl run cancel` on a scheduled run ends the run and its fire.

The fire workflow is Pinned under versioning ([DEPLOY.md](DEPLOY.md)). The
run is read, listed, archived, streamed and delivered to webhooks exactly as a
submitted run; the fire is not a run and is never listed as one.

A fire takes no idempotency claim: the cluster starts exactly one fire per
nominal time, and the preparation's result is recorded once in that fire's
history. The cluster names a fire by its nominal time truncated to the
second, and a manual fire's nominal time is the second the cluster handles it,
so two `schedule fire`s of one schedule inside the same second, while the
first one's run still executes, are one fire: the second starts no run and is
counted nowhere. Every fire submits with `allow_empty_pool = false`: a pool
with no live Orker refuses the fire loudly rather than starting a run that
never leaves `PENDING`.

## A refused fire is a record, not a gap

A fire that could not start a run fails, non-retryably, with the submit
refusal's own message, recorded as the fire's `refusal` and shown by `orkctl
schedule get` and the Schedules tab. With `pause_on_failure` the cluster then
pauses the schedule and writes why into its note. "Did last night's reconcile
run, and if not, why?" is answered by `orkctl schedule get`; the run it did
start by `orkctl run list --origin schedule:<id>`.

A recent fire reads one of: a run id, `refused` with its reason, `preparing`
(it has not prepared yet), or `unavailable` (the fire retired past the
cluster's execution retention, or the cluster did not answer). It is never
blank.

## Surfaces

`ScheduleService` (`proto/orkstack/v1/schedule.proto`): `CreateSchedule`,
`ListSchedules`, `GetSchedule`, `UpdateSchedule`, `PauseSchedule`,
`ResumeSchedule`, `FireSchedule`, `BackfillSchedule`, `DeleteSchedule`.

```text
orkctl schedule create NAME --orkflow NAME [--version V]
    [--cron EXPR ...] [--interval DUR] [--timezone TZ] [--jitter DUR]
    [--start-at RFC3339] [--end-at RFC3339]
    [--overlap skip|buffer-one|buffer-all|cancel-other|allow-all]
    [--catchup-window DUR] [--pause-on-failure]
    [--input KEY=VALUE ...] [--input-json JSON] [--description TEXT] [--paused]
orkctl schedule list [--filter F ...]
orkctl schedule get NAME
orkctl schedule update NAME [--version V | --follow-current] [spec flags]
    [--input KEY=VALUE ...] [--input-json JSON] [--clear-input] [--description TEXT]
orkctl schedule pause NAME [--reason TEXT]
orkctl schedule resume NAME
orkctl schedule fire NAME [--overlap POLICY]
orkctl schedule backfill NAME --start RFC3339 --end RFC3339 [--overlap POLICY]
orkctl schedule delete NAME
```

Every subcommand takes `--json`. `create` checks the namespace and the
Orkflow (and pinned version) exist before the cluster is asked; a refused
create creates nothing. `--paused` creates it paused, so `schedule get` shows
the next fire times before it ever fires. `update` replaces every mutable
field (version pin, input, description, spec); the CLI reads the schedule and
writes back only the flags given (`--cron` replaces every expression). There
is no etag — the cluster updates a schedule without a conflict token — so last
writer wins. The Orkflow a schedule starts is immutable: rebinding is delete
then create. `fire` works on a paused schedule; its run is not known when the
command returns and appears in `schedule get` once prepared. `delete` leaves
runs already started running, with their origin; a repeat delete is NotFound.

The Orkflow page's **Schedules** tab is the UI: create (with the server's
refusal verbatim), list with a state pill (`active`, `paused` with its note,
`unavailable` with its reason), next fire and last run, pause (with a reason),
resume, fire now (which sends no overlap override), delete, the recreating
`schedule create` command, and each row's next ten fire times, recent fires
and counters. A run's origin `schedule` links back to the tab. **Backfill is
CLI-only**: it enqueues many fires at once and is an operator recovery tool,
an accepted parity exception ([UI_CLI_PARITY.md](UI_CLI_PARITY.md)).

Validation, each a refusal naming the field: at least one cron expression or
an interval; a cron expression of at most 200 bytes with no control character
and no `CRON_TZ=`/`TZ=` prefix (set the time zone instead); a positive
interval; a non-negative jitter; an IANA time zone; a catch-up window of at
least 10 s; `start_at` not after `end_at`; an `end_at` in the future (a
schedule with no fire left is retired by the cluster); a declared overlap
policy; a description or pause reason of at most 256 bytes, valid UTF-8, no
control character. Cron syntax is the cluster's to judge, and its message is
returned verbatim. A backfill window is at most 366 days.

`orkctl orkflow delete` is refused while a schedule names the Orkflow; delete
the schedules first. Every schedule write is a control-plane audit record
(`schedule.create`, `.update`, `.pause`, `.resume`, `.fire`, `.backfill`,
`.delete`); a record never carries the schedule's input.

## Time zones and daylight saving

Calendar matching is literal clock time in the schedule's zone with no DST
special-casing, as Temporal documents: a 02:30 fire does not happen on the day
that has no 02:30, and happens twice on the day that has two 01:30s. An
interval is aligned to the Unix epoch: every 15m fires at :00, :15, :30 and
:45.

## Rolling back

A release without schedules has no `ScheduleFireWorkflow` registered, so a
schedule that fires on it cannot start. Pause or delete every schedule before
rolling back to such a release ([DEPLOY.md](DEPLOY.md) §3, "Releases that add
Orkflow constructs", the `schedules` row).
