# Run an Orkflow on a schedule

Start runs of one Orkflow at set times. Each fire starts an ordinary run, with its own run id and logs.

## Example

Create a schedule that runs `poll-until-ready` every 15 minutes. It starts paused, so you can check the next fire times first:

```bash
orkctl schedule create poll-exports --orkflow poll-until-ready \
  --cron '*/15 * * * *' --timezone UTC --overlap skip --paused \
  --input status_url=https://exports.example.com/status \
  --input export_url=https://exports.example.com/latest.json

orkctl schedule get poll-exports     # the next fire times, before anything fires
orkctl schedule resume poll-exports  # let it fire
```

Apply `poll-until-ready` first. `schedule create` refuses an Orkflow that is not stored. A schedule with the same spec is declared as a document in [`examples/automation/examples.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/automation/examples.ork.yaml). See [Manage schedules, triggers and webhooks in a file](/docs/how-to/automation/).

## Set the times

Give at least one `--cron` or one `--interval`.

| Flag | Meaning |
|---|---|
| `--cron EXPR` | A cron expression, such as `0 2 * * *`, `@daily` or `@every 15m`. Repeat it to fire at each time. |
| `--interval DUR` | Fire every duration, such as `15m`. Intervals align to the Unix epoch, so `15m` fires at :00, :15, :30 and :45. |
| `--timezone TZ` | The IANA time zone the cron expressions use. Default `UTC`. |
| `--jitter DUR` | Delay each fire by a random amount up to this duration. |
| `--start-at`, `--end-at` | RFC 3339 instants. No fire before the first, and none after the second. |
| `--catchup-window DUR` | How late a fire missed during an outage may still start. At least `10s`. |

## Choose what happens on overlap

`--overlap` decides what a due fire does when the previous run is still running. The default is `skip`.

| Policy | When a fire is due and the previous run still runs |
|---|---|
| `skip` | Drop the fire. |
| `buffer-one` | Hold at most one fire, and start it when the run ends. |
| `buffer-all` | Hold every fire, and start them one after another when the run ends. |
| `cancel-other` | Cancel the running run the way `orkctl run cancel` does, and start this one without waiting. The two overlap for the cancelled run's grace period. |
| `allow-all` | Never wait. Start every fire, even while others run. |

Pass `--overlap` to `schedule fire` or `schedule backfill` to override the policy for one request.

## Pin or follow a version

Without `--version`, each fire starts the Orkflow's current version. `--version V` pins one version. `schedule update --follow-current` removes the pin.

## Pause, fire and backfill

```bash
orkctl schedule pause poll-exports --reason "export job is down"
orkctl schedule fire poll-exports
orkctl schedule backfill poll-exports --start 2026-10-01T00:00:00Z --end 2026-10-02T00:00:00Z
orkctl schedule resume poll-exports
```

- `pause` stops fires until `resume`. The reason shows on every read.
- `fire` starts one fire now, even while the schedule is paused.
- `backfill` fires once for each time in the window. The window can cover at most 366 days.

## Check what started

`orkctl schedule get <name>` shows the schedule's id, its next fire times and its recent fires. A recent fire shows the run it started, or `refused` with the reason.

To list every run the schedule started, filter the run list by origin:

```bash
orkctl run list --filter origin.kind=SCHEDULE --filter origin.id=<schedule-id>
```

## When a fire fails

A fire that starts no run is `refused`, and the refusal says why. With `--pause-on-failure`, a failed or refused fire pauses the schedule, and the pause note says why.

## Limits

- You cannot change the Orkflow a schedule starts. Delete the schedule and create a new one.
- An update has no conflict check. The last write wins.
- Deleting a schedule leaves the runs it started running.
- On a day with no 02:30, a 02:30 fire does not happen. On a day with two 01:30s, it happens twice.

## See also

- [Orkflow schedules](https://github.com/orkstack/orkstack/blob/main/docs/ORKFLOW_SCHEDULES.md#surfaces): the full rules and the command list
- [Time zones and daylight saving](https://github.com/orkstack/orkstack/blob/main/docs/ORKFLOW_SCHEDULES.md#time-zones-and-daylight-saving): the calendar rules
- [Declarative automation contract](https://github.com/orkstack/orkstack/blob/main/docs/AUTOMATION.md#schedules): the `schedules:` section of a document
- [Triggers](/docs/how-to/triggers/) and [Webhooks](/docs/how-to/webhooks/): the other ways to start runs and watch them
