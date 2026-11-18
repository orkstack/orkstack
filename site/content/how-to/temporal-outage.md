# Ride out a Temporal outage

Tell a Temporal outage from an OrkStack fault, know what stops and what carries on, and check the recovery.

## Tell it apart

- `orkctl admin info` answers: the engine is up.
- `orkctl pool list` shows `unknown (unreachable)` where a pool's count would be.
- `orkctl orker list` still lists the Orkers.
- The alerts `OrkStackTemporalUnreachable` and `OrkStackTemporalPollFailures` fire.

## While it lasts

| What | What happens |
|---|---|
| Submits | Answered `Unavailable`. Retry with the same `--idempotency-key`: it is safe, and answers the run that landed if one did. |
| Running containers | Left alone until the heartbeat timeout; then stopped and retried, never reported cancelled. |
| Schedules | No fires; afterwards, fires within each schedule's `catchup_window` run. |
| Webhooks | Wait, then deliver. |
| Cleanup and artifact GC | Hold, and carry on after. |
| Finished runs past retention | Still readable from the archive. |

Nothing needs repair afterwards: no record edits, no terminated runs, no Orker restarts.

## Check the recovery

1. `orkctl pool list` shows counts again.
2. `orkctl run list` answers.
3. `orkctl run watch RUN` on a running run carries on without a gap.
4. `orkctl schedule get NAME` shows the catch-up fires.
5. `orkctl webhook deliveries ID` drains.

Do not take a resolved alert as the check: some alerts read gauges that stop when their process stops.

## Limits

- A submit made without an idempotency key may still have started a run. Look for it in `orkctl run list` before you submit it again.
- A schedule without a `catchup_window` gets the cluster's default catch-up window.

## See also

- [RUNBOOKS.md, "Temporal outages"](https://github.com/orkstack/orkstack/blob/main/docs/RUNBOOKS.md#temporal-outages): every area, and partial failures of Temporal's services
- [DEPLOY.md, "5. Ops notes"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#5-ops-notes): idempotency keys
