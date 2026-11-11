# Schedules and triggers

Start runs without a person in the loop. A **schedule** starts runs of one Orkflow at set times. A **trigger** starts a run when an outside caller sends one HTTP request with its token. Each run they start is an ordinary run, with its own ID and logs.

## Run on a schedule

Create the Orkflow first. Then create a schedule that runs `poll-until-ready` once a day. It starts paused, so you can check the next fire times first:

```bash
orkctl schedule create poll-exports --orkflow poll-until-ready \
  --cron '0 6 * * *' --timezone UTC --overlap skip --paused

orkctl schedule get poll-exports     # the next fire times, before anything fires
orkctl schedule resume poll-exports  # let it fire
```

`schedule create` refuses an Orkflow that is not stored. A schedule can also be declared in an automation document: see [Automation documents](/docs/operate/automation/).

### Set the times

Give at least one `--cron` or one `--interval`.

| Flag | Meaning |
|---|---|
| `--cron EXPR` | A cron expression, such as `0 2 * * *`, `@daily` or `@every 15m`. Repeat it to fire at each time. |
| `--interval DUR` | Fire every duration. Intervals align to the Unix epoch, so `15m` fires at :00, :15, :30 and :45. |
| `--timezone TZ` | The IANA time zone the cron expressions use. Default `UTC`. |
| `--jitter DUR` | Delay each fire by a random amount up to this duration. |
| `--start-at`, `--end-at` | RFC 3339 instants. No fire before the first, and none after the second. |
| `--catchup-window DUR` | How late a fire missed during an outage may still start. At least `10s`. |

On a day with no 02:30, a 02:30 fire does not happen. On a day with two 01:30s, it happens twice.

### Choose what happens on overlap

`--overlap` decides what a due fire does while the previous run still runs. The default is `skip`.

| Policy | When a fire is due and the previous run still runs |
|---|---|
| `skip` | Drop the fire. |
| `buffer-one` | Hold at most one fire, and start it when the run ends. |
| `buffer-all` | Hold every fire, and start them one after another when the run ends. |
| `cancel-other` | Cancel the running run, as `orkctl run cancel` does, and start this one without waiting. |
| `allow-all` | Never wait. Start every fire, even while others run. |

### Pin a version, pause and resume

Without `--version`, each fire starts the Orkflow's current version. `--version V` pins one version. `schedule update --follow-current` removes the pin.

```bash
orkctl schedule pause poll-exports --reason "export job is down"
orkctl schedule resume poll-exports
```

`pause` stops fires until `resume`. The reason shows on every read.

### Check what started

`orkctl schedule get <name>` shows the next fire times and the recent fires. A recent fire shows the run it started, or `refused` with the reason.

To list every run a schedule started, filter by origin:

```bash
orkctl run list -F origin.kind=SCHEDULE -F origin.id=<schedule-id>
```

<!-- site-media: cli-schedules-triggers -->

### Limits

- A fire that starts no run is `refused`. A fire on a pool with no live Orker is refused too. With `--pause-on-failure`, a failed or refused fire pauses the schedule, and the pause note says why.
- You cannot change the Orkflow a schedule starts. Delete the schedule and create a new one.
- An update has no conflict check. The last write wins.
- Deleting a schedule leaves the runs it started running.

## Backfill and fire by hand

`schedule fire` starts one fire now, even while the schedule is paused. `schedule backfill` fires once for each time in a past window:

```bash
orkctl schedule fire poll-exports
orkctl schedule backfill poll-exports --start 2026-10-01T00:00:00Z --end 2026-10-02T00:00:00Z
```

- `fire` and `backfill` take `--overlap` to override the schedule's policy for one request.
- `backfill` takes RFC 3339 `--start` and `--end`. The window can cover at most 366 days.
- The run from a fire appears in `schedule get` once it is prepared.
- Backfill is in the CLI only. The Schedules tab on an Orkflow's page does the other schedule actions. A schedule's name there opens its edit.

## Start runs over HTTP

A trigger is an inbound starter. Create one for an Orkflow, then send one `POST` with its token. A CI job, a cron host or `curl` can do it, and no client library is needed.

```bash
orkctl trigger create --orkflow each-fan-out --name ci-kickoff --description 'CI kickoff'
```

The command prints the token once, with a ready-to-run `curl` command. Replace `trg-…` and `<token>` with the printed values:

```bash
curl -X POST http://localhost:8080/v1/triggers/trg-… \
  -H 'Authorization: Bearer <token>' \
  -H 'Content-Type: application/json' \
  -d '{}'
```

The body is the run's input, as one JSON object. `{}` starts a run with no input, so the Orkflow's defaults apply, and `each-fan-out` declares no inputs. For an Orkflow that declares `count`, `{"count": 3}` sets it to `3`. The body can be at most 1 MiB. A `201` response holds the new run's `id`.

### Create and manage a trigger

`--orkflow` is required. `--version` pins one version, and without it each call starts the current version. `--name` is the handle an automation document uses to find the trigger, unique in the namespace. `--description` labels every run the trigger starts.

The token is shown once, and a listing never shows it. To rotate a token, delete the trigger and create a new one: the new trigger has a new ID and URL. `orkctl trigger update` renames a trigger, changes its pin or changes its label, and keeps the token.

### Send the request

| Header | Required | Meaning |
|---|---|---|
| `Authorization: Bearer <token>` | yes | The trigger's token. |
| `Idempotency-Key` | no | A repeated key returns the run it started, not a new one. The key is scoped to the namespace. |
| `Ork-Origin-Reference` | no | A reference recorded on the run, such as a CI job's URL. Up to 256 bytes. |
| `Ork-Principal` | no | A name recorded on the run. OrkStack records it as asserted, and does not check it. |

### When a request fails

A failed request starts no run.

| Status | `error.code` | What to do |
|---|---|---|
| `401` | `unauthenticated` | Send the `Authorization` header with the right token. |
| `404` | `not_found` | The trigger was deleted, or its Orkflow or pinned version no longer exists. |
| `400` | `invalid_argument` | The body is not one JSON object, or it is over 1 MiB. Or `Ork-Origin-Reference` is invalid. |
| `412` | `failed_precondition` | The namespace is on hold, at its run limit or over an enforced budget, or no live Orker serves the pool. Nothing started. |
| `503` or `500` | `unavailable`, `internal` or `storage_error` | Storage or the engine is unavailable, retry the request. Or an unexpected error, or the trigger store could not be read. |

### Trigger limits

- No rate limit, and no retry: one request gets one result. Put your gateway in front of the trigger for rate limits.
- A trigger starts only the Orkflow it was created for. A request cannot change the Orkflow or the version.
- `Idempotency-Key` is scoped to the namespace, not the trigger. Two triggers in one namespace that send the same key start one run.

## See also

- [docs/ORKFLOW_SCHEDULES.md, "Surfaces"](https://github.com/orkstack/orkstack/blob/main/docs/ORKFLOW_SCHEDULES.md#surfaces): the full command list and validation rules
- [docs/ORKFLOW_SCHEDULES.md, "Time zones and daylight saving"](https://github.com/orkstack/orkstack/blob/main/docs/ORKFLOW_SCHEDULES.md#time-zones-and-daylight-saving): the calendar rules
- [docs/ORKFLOW_TRIGGERS.md, "Invoking a trigger"](https://github.com/orkstack/orkstack/blob/main/docs/ORKFLOW_TRIGGERS.md#invoking-a-trigger): the full request and response rules
- [docs/ORKFLOW_TRIGGERS.md, "What triggers do not do"](https://github.com/orkstack/orkstack/blob/main/docs/ORKFLOW_TRIGGERS.md#what-triggers-do-not-do): the limits in full
- [docs/AUTOMATION.md, "schedules"](https://github.com/orkstack/orkstack/blob/main/docs/AUTOMATION.md#schedules) and [docs/AUTOMATION.md, "triggers"](https://github.com/orkstack/orkstack/blob/main/docs/AUTOMATION.md#triggers): declaring them in a document
