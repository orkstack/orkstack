# Send run events to a URL with webhooks

Create a webhook for a URL. OrkStack then sends a signed `POST` to it when a run starts or ends, and when a task changes state if you ask for those events.

## Example

Create a webhook that calls a URL, for a run's success and failure events:

```bash
orkctl webhook create https://example.test/hooks --event RUN_SUCCEEDED --event RUN_FAILED
```

The command prints a signing secret once. Store it with your receiver. Without `--event`, a webhook gets every run transition.

Each delivery is a `POST` with a JSON body. A run event looks like this, abridged:

```text
POST /hooks
X-Ork-Event: RUN_FAILED
X-Ork-Sequence: 7
X-Ork-Signature: sha256=…

{"runId":"ork-…","orkflowName":"…","status":"FAILED","timestamp":"…","sequence":7,"run":{…}}
```

`sequence` is the event's number in the run's event stream, counted from 1. `X-Ork-Sequence` carries the same number.

## Choose the events

| Event | Sent when |
|---|---|
| `RUN_STARTED` | A run starts running. |
| `RUN_SUCCEEDED` | A run succeeds. |
| `RUN_FAILED` | A run fails. |
| `RUN_CANCELED` | A run is canceled. |
| `TASK_SCHEDULED` | A task is handed to the scheduler. |
| `TASK_STARTED` | A task is dispatched to an Orker. |
| `TASK_AWAITING` | An awaited task starts waiting for a resolution. |
| `TASK_RESOLVED` | An awaited task is completed or rejected from outside the run. |
| `TASK_SLEEPING` | A sleeping task starts its fixed wait. |
| `TASK_SUCCEEDED` | A task's result is committed. |
| `TASK_FAILED` | A task fails for good, after its retries. |

The four run events are the default. Task events are opt-in: name each one with `--event`. A task event adds `taskId`, and `attempt` when the engine has numbered the attempt. Task events are hints with a short retry budget. A receiver that must not miss one reads the run again through the API (`GetRun`).

## Limit the scope

| Flag | Narrows the webhook to |
|---|---|
| `-n NS` | One namespace. Without it, every namespace. |
| `--orkflow NAME` | Runs of one Orkflow. |
| `--task ID` | One task's events. It needs at least one `TASK_*` event. |

Only an explicit `-n` narrows the namespace. The namespace in `ORK_NAMESPACE`, or the one you saved, does not apply.

## Verify the signature

Check every delivery before you act on it:

1. Read the raw request body, before any JSON parsing.
2. Compute HMAC-SHA256 of those bytes, using the secret as the key.
3. Write the digest as lowercase hex, and add the prefix `sha256=`.
4. Compare the result with `X-Ork-Signature` in constant time. Accept only an exact match.

The signature covers the body only. Read the event name from `X-Ork-Event`, because the body carries `status`, not the event name. A repeated event can arrive twice, under two different `X-Ork-Delivery` ids, with the same `sequence`. Record the run id and the sequence of each one you have handled.

## Put deliveries in order

Deliveries of one run can arrive in any order, because a retry lands late and tasks running side by side report at the same time. Order them by `sequence`, not by when they arrive or by `timestamp`:

- Keep the highest `sequence` you have applied for each run. A delivery with a lower one is an earlier event, so its `status` must not overwrite a later one.
- The sequence orders events, not the `run` in the body, which is read when the delivery is queued and can be newer. Read the run's current state with `GetRun`.
- Expect gaps. You get only the events you subscribed to, and the stream can skip numbers. Never wait for a missing one.
- Compare sequences within one run only.
- A retry and a redelivery carry the original event's sequence.

## Retries

- A `2xx` response ends the delivery.
- A transport error or a `5xx` response is retried. A delivery makes at most four requests: the first, then retries after waits of 100 ms, 200 ms and 400 ms. Each request times out after 10 seconds.
- The waits are at least those, and the cluster can make them longer: about a second each on a stock Temporal server.
- The retries survive a restart: when the engine replica that queued a delivery stops, another one makes the next attempt.
- Any other response, including every `4xx`, ends the delivery without a retry.

## Find and resend a delivery

```bash
orkctl webhook deliveries -F 'delivered=false' --limit 20
orkctl webhook delivery dlv-…
orkctl webhook redeliver dlv-…
```

`deliveries` lists what each delivery did. `delivery` shows one record, with every attempt and the signature sent. `redeliver` sends the event once, now. It signs the event with the current secret and carries the run as it reads today. It exits `1` when the receiver does not accept it.

A delivery record keeps the SHA-256 of the body. It does not keep the body.

## Limits

- A delivery can arrive twice, and deliveries can arrive out of order. Order them by `sequence`.
- Webhooks never delay or fail a run. A receiver that is down does not stop a run.
- The receiver should answer without a redirect. A redirect copies the signature header to the new host.
- Scope narrows by namespace, Orkflow and task only. You cannot filter on a field of the run.
- Any reachable `http` or `https` URL is accepted. OrkStack does not check the destination.

## See also

- [Run-event webhooks](https://github.com/orkstack/orkstack/blob/main/docs/WEBHOOKS.md): the full delivery rules
- [Verifying the signature](https://github.com/orkstack/orkstack/blob/main/docs/WEBHOOKS.md#verifying-the-signature): the verification steps
- [Attempts and retries](https://github.com/orkstack/orkstack/blob/main/docs/WEBHOOKS.md#attempts-and-retries): the retry ladder
- [Duplicates and ordering](https://github.com/orkstack/orkstack/blob/main/docs/WEBHOOKS.md#duplicates-and-ordering): ordering by sequence
- [Triggers](/docs/run/schedules-and-triggers/): the inbound counterpart, which starts runs
- [Inspect a run](/docs/run/watch-a-run/): reading a run after its event arrives
