# Run-event webhooks

A **webhook** is a subscription that makes OrkStack call an external system when
a run, or one of its tasks, changes state. OrkStack sends a `POST` with a JSON
body, signed with the subscription's secret, to the subscriber's URL. A webhook
is the outbound half of run-event delivery. It is the mirror image of a
**trigger**, which is inbound and starts a run
([ORKFLOW_TRIGGERS.md](ORKFLOW_TRIGGERS.md), "Trigger versus webhook").

A webhook is an object of its own, not a key of the Orkflow
([DSL.md](DSL.md#what-an-orkflow-does-not-declare)). It is deployment-wide unless
it names a scope. Delivery is never on a run's path: a subscriber that is slow,
down or wrong cannot delay or fail a run
([UI_CLI_PARITY.md](UI_CLI_PARITY.md), "Webhook administration"). A subscriber
that must not miss a transition reconciles with `GetRun`.

## Webhook versus trigger

| | Webhook | Trigger |
| --- | --- | --- |
| Direction | Outbound: OrkStack calls the subscriber | Inbound: the caller calls OrkStack |
| Scope | Deployment-wide, or narrowed by namespace, Orkflow and task | One Orkflow in one namespace, optionally one pinned version |
| Authentication | OrkStack signs each delivery with the subscription's secret (HMAC-SHA256) | The caller presents the trigger's bearer token |
| Secret at rest | Plaintext: delivery replays it into a signature | Salted SHA-256 digest only |
| Failure semantics | Best-effort; a subscriber outage never fails a run | Synchronous; the HTTP response is the outcome of one `SubmitRun` |

The table follows [ORKFLOW_TRIGGERS.md](ORKFLOW_TRIGGERS.md), "Trigger versus
webhook". The code is in `pkg/webhooks`, and the engine's announcement seam is
`internal/engine/activities.go`.

## What a subscription selects

### Events

A subscription names the transitions it wants in `events`. The engine announces
only the events below, and a subscription receives only the ones it names
(`pkg/webhooks/webhooks.go`, `Subscribes`; `internal/engine/activities.go`,
`IsSubscribableEvent`).

| Event | Delivered when |
| --- | --- |
| `RUN_QUEUED` | the run waits for a concurrency slot before it starts, or, after a resume or an unpause, before it continues |
| `RUN_STARTED` | the run moves from pending to running |
| `RUN_SUCCEEDED` | the run ends successfully |
| `RUN_FAILED` | the run ends failed |
| `RUN_CANCELED` | the run ends cancelled, by its own cancel or by a force cancel (`RUN_FORCE_CANCELED`), whose tasks are as of its last event and before which no failure-handler event comes |
| `TASK_QUEUED` | a task waits for a task-level concurrency slot |
| `TASK_SCHEDULED` | a task is handed to the scheduler |
| `TASK_STARTED` | a task is dispatched to an Orker; the activity is scheduled, which does not mean a container is running |
| `TASK_AWAITING` | an awaited task starts waiting to be resolved |
| `TASK_RESOLVED` | an awaited task is resolved from outside its run: `SUCCEEDED` for a completion, `FAILED` for a rejection |
| `TASK_SLEEPING` | a sleeping task starts its fixed wait; its message names the wake-up time, and `TASK_SUCCEEDED` follows when it wakes |
| `TASK_SUCCEEDED` | a task's success is committed |
| `TASK_FAILED` | a task fails for good, after its retries stop; a task cancelled because its run ended is never delivered as this event |
| `RUN_PAUSED` | the run is paused, by a person (`orkctl run pause`) or by its parent run's pause, or stays paused with its causes changed; status `PAUSED`, and `pause_transition` says which ([API.md](API.md#pauserunrequest)) |
| `RUN_UNPAUSED` | the run continues after a pause (status `RUNNING`), or an unpause found it not paused (`pause_transition` `NOT_PAUSED`); a pause its run's end closed has none: the terminal delivery's `pause_transition` and the run's `last_pause` say so |
| `RUN_RESUMED` | a run that had ended runs again under the same id, from failed to running (status `RUNNING`); `resumeCount` says which resume of the run it is |
| `RUN_RESULTS_KEPT` | a resumed run has read back the results it keeps (status `RUNNING`); `resultsKept` says how many |

- An empty `events` selects the run's lifecycle: `RUN_STARTED`, the three
  endings and `RUN_RESUMED`, and no other event. Task events, `RUN_QUEUED`,
  `RUN_PAUSED`, `RUN_UNPAUSED` and `RUN_RESULTS_KEPT` are opt-in, by name.
- No build resumes runs yet, so `RUN_RESUMED` and `RUN_RESULTS_KEPT` are never
  sent. Once one does, a run that failed and is resumed delivers `RUN_FAILED`, then `RUN_RESUMED`,
  then its next ending, all with rising sequences. The highest sequence is the
  run's current state.
- `TASK_LOG` is refused: a log line is not a transition (`ParseEvent`).
- Duplicates are removed and the list is kept in lifecycle order on registration
  (`normalizeEvents`).
- A `TASK_FAILED` delivery always reads `"status":"FAILED"`, and a task that was
  skipped, or still pending or running when its run ended, is never delivered as
  a task outcome ([UI_CLI_PARITY.md](UI_CLI_PARITY.md), "Scoped subscriptions and
  task outcome events"), with one exception: a run's own `timeout:` stops each
  task still running, waiting or sleeping as `FAILED`
  `TASK_STOPPED_BY_RUN_TIMEOUT` ([DSL.md](DSL.md), "Run timeout"), so a
  `TASK_FAILED` subscriber gets one delivery per task the timeout stopped, then
  the run's `RUN_FAILED` with `RUN_TIMED_OUT`. Tasks that had not started stay
  `CANCELED` and are not delivered.

### Scope

A subscription has three scope fields. Each is an exact match on one coordinate,
and an empty field matches every value.

| Field | Matches |
| --- | --- |
| `namespace` | runs of one namespace. A run that names none is in `default`, so a subscription scoped to `default` matches it |
| `orkflow_name` | runs of the namespace's own Orkflow of that name. A child run of another namespace's shared Orkflow is never one, whatever its name |
| `task_id` | one task's outcome events. It is consulted only for `TASK_*` events, so a `RUN_*` event passes it |

- An Orkflow name is read in the namespace a run ran in: a deployment-wide
  subscription that names one hears each namespace's own Orkflow of that name. A
  scope with no Orkflow name hears a shared Orkflow's child runs with the
  namespace's other runs, and the body's `orkflowNamespace` says whose they are.
- A subscription with no scope is deployment-wide. It receives the transitions of
  every namespace.
- A `task_id` needs at least one `TASK_*` event in `events`. Otherwise the task
  filter could never be consulted, and registration refuses it
  (`ValidateScope`).
- Each scope field is a slug (`^[a-z][a-z0-9-]*$`, at most 200 bytes). It is
  validated and never sanitised.
- Nothing checks that the namespace, Orkflow or task exists, so a subscription
  may be created before the Orkflow it names exists
  ([UI_CLI_PARITY.md](UI_CLI_PARITY.md), "Scoped subscriptions and task outcome
  events").

### The webhook object

| Field | Meaning |
| --- | --- |
| `id` | `wh-` and 16 hex characters: 8 bytes from `crypto/rand`. Opaque; it survives an update |
| `url` | an absolute `http` or `https` URL with a host (`ValidateURL`) |
| `events` | the transitions, as above; empty means the run's lifecycle: its start, endings and resumption |
| `secret` | the signing secret. It is returned once, by the response that creates the subscription, and never by a listing or a read |
| `namespace`, `orkflowName`, `taskId` | the scope |
| `name` | an optional handle, unique among the named subscriptions of its namespace scope. It requires a namespace, and it plays no part in delivery |
| `createdAt` | the registration time, in UTC |

A generated secret is 32 bytes from `crypto/rand`, hex encoded
(`pkg/webhooks/webhooks.go`, `Register`). A caller may supply its own secret
instead. OrkStack sets no minimum length for a supplied secret.

## Management

`WebhookService` (`proto/orkstack/v1/webhook_service.proto`) manages the
subscriptions and reads the delivery records. Its RPCs are:

- `RegisterWebhook`, `ListWebhooks`, `UpdateWebhook` and `DeleteWebhook`;
- `ListDeliveries`, `GetDelivery` and `RedeliverWebhook`.

With authentication on ([ACCESS.md](ACCESS.md)), each is decided at the
subscription's scope: its namespace, or the deployment for a subscription that
names none. Reading needs `webhook.read` and a redelivery `webhook.redeliver`.
Registering or changing a subscription needs `webhook.write` **and** `run.read`
there, at the old scope and the new: a subscription is a standing read of every
run in its scope, sent to an address its author chooses. The legacy JSON routes
below are decided at the deployment.

Plain JSON routes remain as compatibility adapters over the same registry
(`internal/api/webhooks.go`): `POST /v1/webhooks` answers `201` with the
subscription and its secret, `GET /v1/webhooks` lists, and
`GET /v1/webhooks/{id}` and `DELETE /v1/webhooks/{id}` read and delete. The
adapter has no update. Its writes are audited by the adapter itself. Clients
use the generated Connect types.

A `POST /v1/webhooks` body must be declared `Content-Type: application/json`.
Parameters such as `; charset=utf-8` are allowed, and the type is read in any
case. A body declared as anything else, not declared, or declared with a
malformed `Content-Type` (such as `application/json; charset`) is `415` with the
error envelope (`invalid_argument`): it is refused before it is read, so it
creates nothing and is not recorded in the audit trail. A browser sends a
body declared `application/json` to another site only after a CORS preflight,
and the engine grants none, so a page on another site cannot create a
subscription ([SECURITY_MODEL.md](SECURITY_MODEL.md), "Transport, listeners and
configuration").

- **Create** validates the events, the URL, the scope and the name, in that
  order, before any write. A refused registration writes nothing. A name another
  subscription of the same namespace scope holds is `AlreadyExists` (`409` on the
  adapter).
- **Update** replaces the name, URL, events and scope in place. The id and the
  secret never change, so a receiver keeps verifying and the delivery history
  stays continuous. On S3 the update is a conditional write on the ETag it read,
  retried a bounded number of times. A subscription deleted in the meantime is
  never recreated; it reads as `NotFound`.
- **Delete** removes the record. Matching reads the registry on every event, so
  a deleted subscription matches no later event. A delivery already in flight is
  not recalled.

Records live in the shared object store, one per subscription:

```text
metadata/webhooks/{wh-id}.json
```

The record holds the signing secret in plaintext. Delivery replays the secret
into a signature, so the secret must be stored where delivery can read it
([ORKFLOW_TRIGGERS.md](ORKFLOW_TRIGGERS.md), "Storage and secret handling";
[STORAGE.md](STORAGE.md), "Key layout"). Every replica reads the registry on
each operation, so a subscription created through one replica is delivered
by all of them.

## The delivery

### The request

Each matching event produces one `POST` to the subscription's URL:

```text
Content-Type: application/json
User-Agent: orkstack-webhooks/1
X-Ork-Signature: sha256=<lowercase hex>
X-Ork-Event: RUN_FAILED
X-Ork-Delivery: dlv-<queued stamp>-<16 hex>
X-Ork-Webhook-Id: wh-<16 hex>
X-Ork-Run-Id: ork-…
X-Ork-Task-Id: <task id>                  (task events only)
X-Ork-Timestamp: <RFC 3339 instant, UTC>
X-Ork-Sequence: 7
X-Ork-Attempt: 1
X-Ork-Redelivery-Of: dlv-…               (redelivery only)
```

The body is one JSON object, in this field order:

```json
{"runId":"ork-…","orkflowName":"…","orkflowNamespace":"…","status":"FAILED","timestamp":"<RFC 3339 instant>","sequence":7,"pauseTransition":"…","taskId":"…","attempt":2,"run":{…}}
```

- `orkflowNamespace` is present only for a child run of another namespace's
  shared Orkflow: the namespace that owns it. `orkflowName` is the name there,
  and the run is in the namespace it runs in (`run.namespace`).
- `sequence` is the run event's sequence: the number the run's event stream
  gives the event, counted per run from 1 in the order the run emitted its
  events (`orkctl run watch --from-sequence` resumes by the same number). It
  is what a receiver orders deliveries by; see "Duplicates and ordering". A
  redelivery of a record written before deliveries carried it has none, and
  then the body and `X-Ork-Sequence` omit it rather than send 0.
- `pauseTransition` is present only on `RUN_PAUSED`, `RUN_UNPAUSED` and a
  terminal event that ended a pause: what the event did to the run's pause
  (`PAUSED`, `CAUSES_CHANGED`, `UNPAUSED`, `NOT_PAUSED`, `ENDED_TIMED_OUT`,
  `ENDED_RUN_ENDED`). The embedded run's `pause`, `pausedByParent` and
  `lastPause` say who paused it, when and until when.
- `resumeCount` is present only on `RUN_RESUMED`: how many resumes of the run
  got going, that one included. `resultsKept` is present only on
  `RUN_RESULTS_KEPT`: `keptCount` results read back from their commits,
  `settledCount` awaited and composed tasks kept from their records, and
  `readBackSeconds`. A kept task has no task event of its own.
- `taskId` and `attempt` are present only for a task event. `attempt` is the
  task's own attempt number, and it is omitted when the engine has not numbered
  it. It is never invented.
- `status` is the status the event carries.
- `run` is the run in the JSON dialect of [JSON.md](JSON.md), with its input,
  its output and its origin, and on a `RUN_CANCELED` delivery who canceled it
  (`canceledBy`) when the engine could read it; a delivery whose read failed
  goes out without it. A principal in the body (`origin.principal`,
  `canceledBy`, an awaited task's `resolvedBy`) is as recorded: verified only
  when its flag is true (`origin.principalVerified`, `canceledByVerified`,
  `resolvedByVerified`). An absent or false flag means it was asserted in the
  `Ork-Principal` header and never authenticated, so a receiver that does not
  read the flag must not treat the name as verified.
  Fan-out items are removed (`engine.WithoutItems`).
  It is the run as the engine's projection held it when the transition was
  published. A redelivery carries the run as it reads now.

The headers are listed below. Only the body is signed.

| Header | Carries | In the signature |
| --- | --- | --- |
| `X-Ork-Signature` | `sha256=` and the lowercase hex HMAC-SHA256 of the body | is the signature |
| `X-Ork-Event` | the event name | no |
| `X-Ork-Delivery` | `dlv-` and the queued instant, then 16 hex characters. The same on every retry of one delivery | no |
| `X-Ork-Webhook-Id` | the subscription's id | no |
| `X-Ork-Run-Id` | the run's id | no |
| `X-Ork-Task-Id` | the task's id; task events only | no |
| `X-Ork-Timestamp` | the transition's time, RFC 3339 with nanoseconds, UTC | no |
| `X-Ork-Sequence` | the run event's sequence, the body's `sequence`. The same on every retry and on a redelivery | no |
| `X-Ork-Attempt` | the HTTP attempt, counted from 1. It is not the task's attempt | no |
| `X-Ork-Redelivery-Of` | the delivery an operator repeated; redelivery only | no |

The headers vary between attempts, so they are deliberately outside the
signature. A receiver that dispatches on the event name therefore reads it from
`X-Ork-Event`, which the signature does not cover. The body carries `status`,
not the event name.

### Verifying the signature

1. Read the request body as raw bytes, before any JSON parsing or re-encoding.
2. Compute HMAC-SHA256 over those bytes, with the subscription's secret as the key.
3. Encode the digest as lowercase hex, with the `sha256=` prefix.
4. Compare the result with `X-Ork-Signature` in constant time. Accept only an
   exact match. A missing or empty header never verifies.

The Go package `pkg/webhooks` provides this as `Sign` and `Verify`
(`pkg/webhooks/sign.go`). `Verify` accepts the header with or without its
prefix, compares in constant time, and never verifies an empty secret or an
empty signature.

A matching signature proves that the body was signed with that subscription's
secret, which is to say that it came from this deployment
([ORKFLOW_TRIGGERS.md](ORKFLOW_TRIGGERS.md), "Trigger versus webhook"). It does
not prove that the event is new. See "Duplicates" below.

### Where a delivery may go

A delivery connects to public addresses only. A loopback, link-local (including
the cloud metadata address 169.254.169.254), private (RFC 1918 and ULA), shared
(100.64.0.0/10), multicast or otherwise reserved address is refused with the
reason `WEBHOOK_DESTINATION_BLOCKED` ([REASONS.md](REASONS.md), "Webhooks").

- Registration (and update) refuses a URL whose host is such an address, in any
  spelling, or `localhost`, with that reason as an `invalid_argument`. A name is
  not resolved at registration.
- Every connection a delivery makes is checked at the moment it is dialled, on the
  address the name resolved to. A name that is rebound to an internal address after
  registration, and a redirect to one, fail the attempt. The attempt records the
  code `destination_blocked`, its error leads with the reason, and a refused
  attempt is not retried.
- The operator admits private receivers, such as one inside the cluster or a
  local stub, by listing their networks in `webhooks.allowed_networks`
  (`ORK_WEBHOOK_ALLOWED_NETWORKS`, a comma-separated list of CIDRs; [DEPLOY.md](DEPLOY.md), § 1).
  Empty admits public addresses only. A subscription already registered for a
  private address is refused at its next delivery until its network is listed.

### Attempts and retries

- A delivery is one `POST` to one subscription for one event. Every queued
  delivery has its own `X-Ork-Delivery` id.
- A `2xx` response ends the attempt series as delivered. A transport error and
  any `5xx` are retried. Any other response ends the series as not delivered, so
  a `4xx` is never retried (`pkg/webhooks/deliver.go`, `Attempt.Retryable`).
- At most four requests are made: the first, then retries after 100 ms, 200 ms
  and 400 ms. Each request is bounded at 10 s. The engine pins these numbers
  into each delivery when it queues it, so a release that changes them moves
  only deliveries queued after it (`engine.DefaultWebhookDeliveryPolicy`).
- The retries are Temporal's, not a loop in the engine (AGENTS.md §2.1). Each
  delivery is a short workflow of its own: one activity makes one `POST`, and
  its retry policy is the ladder above; a second records the delivery and
  deletes its stored body (`internal/engine/webhook_delivery.go`). Both run on
  the engine's own channel, which no Orker polls. A pause between attempts
  is the cluster's timer, so it is at least the ladder's and can be longer: a
  stock Temporal server spaced the 100 ms and 200 ms pauses about 1 s each
  (`TestIntegration_WebhookRetriesUntilTheReceiverAccepts`). Each attempt's
  `backoff` records the pause it actually took.
- A delivery outlives the replica that queued it. When the replica that made
  one attempt stops, another engine replica makes the next, and the record
  holds every attempt, whichever replica made it. An attempt cut short by a
  replica dying records the attempts before it and one row saying it did not
  finish within its timeout.
- Before the first attempt, the engine stores the signed body under
  `metadata/delivery-bodies/` ([STORAGE.md](STORAGE.md), "Key layout"), because
  the body carries the whole run and is too large to travel inside the
  delivery. Every attempt sends exactly those bytes. A delivery whose body or
  subscription is gone before an attempt is not retried, and its record says
  which.
- The body is rendered once. Every attempt sends the same bytes with the same
  signature.
- Each attempt reads the subscription, its URL and its signing secret, from
  the registry by the webhook's id when it runs. A subscription whose URL is
  updated while a delivery is being retried gets that delivery's next attempt
  at its new URL.
- The receiver's response body is read for at most 2 KiB, then discarded.
- The dispatcher sets no redirect policy, so Go's HTTP client applies its
  default: it follows up to ten redirects. A `307` or `308` re-sends the body.
  When the host changes, the client strips the sensitive headers, such as
  `Authorization` and `Cookie`, but it copies the other headers, `X-Ork-Signature`
  among them. A subscriber URL should therefore answer without redirecting.
- Delivery runs off the run's path. Each event's subscription lookup is bounded
  at 5 s, and queuing each delivery (storing its body, starting it) at 10 s,
  each off the run's goroutine. A failed lookup is logged and never fails the
  run. A delivery that could not be queued is never sent; its record has one
  attempt row saying so, and `orkctl webhook redeliver` sends it.
- At shutdown the engine waits up to 15 s for deliveries it is still matching
  or queuing (`webhookDrainTimeout`, `cmd/ork/main.go`). A delivery already
  queued needs no wait: another replica, or this one after its restart, carries
  it on. Only a transition whose deliveries were not yet queued when the process
  stopped is lost.

### Duplicates and ordering

- Delivery is best-effort, and deliveries can arrive in any order: a retried
  delivery lands after events the run emitted later, and a run publishes some
  events — those of tasks running side by side, a fan-out's items — from
  separate activities, so even first attempts can cross
  ([UI_CLI_PARITY.md](UI_CLI_PARITY.md), "Webhook administration").
- A receiver orders a run's deliveries by `sequence`, never by arrival or by
  `timestamp`. A delivery with a lower sequence than one already applied for
  that run is an earlier event, so a receiver that keeps the run's latest
  status keeps the highest sequence it has applied per run and does not let a
  lower one overwrite it.
- The sequence orders the events, not the `run` each body embeds. That is the
  run as the engine read it when it queued the delivery, which can be newer
  than its event, and a redelivery embeds the run as it reads now. A receiver
  that needs the run's current state reads it with `GetRun`.
- Sequences have gaps. A subscription receives only the events it names, and
  the run's stream itself can skip numbers: a resumed run's stream skips to its
  resume's starting number (`RUN_RESUMED`). Opt-in events (`RUN_QUEUED`,
  `RUN_PAUSED`, `RUN_UNPAUSED`, `RUN_RESULTS_KEPT` and the task events) leave
  gaps in a subscription that does not name them. A receiver never waits for a missing number, keeps the
  highest sequence it has applied per run, and deduplicates on (run id,
  sequence), the `X-Ork-Sequence` header. Sequences of different runs are not
  comparable.
- The sequence is the event's own: every retry, a repeated announcement of the
  same event by another replica (under another `X-Ork-Delivery` id) and an
  operator's redelivery carry the original's.
- Whether an event is announced is decided by each replica's own event bus. An
  activity retry that lands on another replica announces the event again
  (`internal/engine/activities.go`, `EventObserver`). The same transition can
  therefore arrive twice, under two `X-Ork-Delivery` ids.
- A receiver that cannot tolerate a repeat should record what it has processed.
  It should reconcile with `GetRun` when it must not miss a transition.

### What the cluster holds

A webhook URL often carries a credential, so none of a delivery's state in
the execution cluster names it. The delivery's workflow input, its
activities' arguments and results, each attempt's heartbeat and each failure
hold the webhook's id, the delivery's id, the body's digest and length, and,
for each attempt, a code and OrkStack's own sentence for it — never the URL
and never the HTTP client's error, which quotes the URL
(`internal/engine/webhook_delivery.go`;
`TestIntegration_WebhookDeliveryHistoryHoldsNoURL`). The record in object
storage names the URL ("Delivery records", below), and the signature sent:
the activities' results carry none, since with the stored body it would let
a reader of the cluster guess the secret. The HTTP client's error goes to
the engine's log only, with the URL taken off it; the error for a URL that
cannot be parsed can still quote the part it could not parse.

A delivery carries its URL in the cluster only while an engine process that
could make its attempts runs a release that sends to the URL in the
delivery's input: the engine checks the fleet for the `webhook-by-id`
capability ([DEPLOY.md](DEPLOY.md), §5, "Engine capabilities and the fleet
check") and logs, at most once a minute, that deliveries carry their URL.
Once every engine process runs this release, deliveries leave it out.
Deliveries queued by an earlier release carry their URL until they settle.
A URL that holds a credential and was subscribed under an earlier release
was written into the cluster with each of its deliveries, and should be
rotated.

| Code | The attempt | Retried |
|---|---|---|
| `destination_blocked` | was refused: the destination is not a public address the operator admits | no |
| `dns` | found no address for the receiver's host name | yes |
| `connect` | could not connect to the receiver | yes |
| `tls` | failed the TLS handshake with the receiver | yes |
| `timeout` | did not finish in time | yes |
| `request` | could not build a request from the subscription's URL | yes |
| `transport` | failed before the receiver answered, for another reason | yes |
| `body_lost` | made no request: the stored body is gone or altered | no |
| `store_unavailable` | made no request: the stored body could not be read | yes |
| `subscription_unavailable` | made no request: the subscription could not be read | yes |
| `subscription_deleted` | made no request: the subscription was deleted | no |
| `unreported` | ended without reporting its outcome (its engine replica died, or it outran its timeout) | — |

An attempt the receiver answered has no code: its status code says what
happened. A record written by an earlier release has no codes, and its
errors are the HTTP client's own words.

## Delivery records

Every delivery that settles, whether it landed or was given up, is written once
as an immutable record ([STORAGE.md](STORAGE.md), "Key layout"):

```text
metadata/deliveries/{yyyy}/{mm}/{dd}/{delivery-id}/{namespace}/{run-id}/{webhook-id}/{event}/{delivered|failed}.json
```

A record holds:

- every attempt: its number, status code, code, error, backoff, duration and
  start time;
- the subscription, run, namespace, Orkflow (and its owner, for another
  namespace's shared one), task, event, the event's
  sequence, status and URL. The URL is the subscription's when the delivery
  settled, or, for a subscription deleted before then, the one the delivery
  was queued with, when it carried one;
- the signature sent, the SHA-256 and the length of the body, and the instants
  the event happened, the delivery was queued and the delivery settled;
- the id of the delivery it repeats, for a redelivery.

A record never holds the body or the secret. The run is read with `GetRun`, and
a second copy of it would be a second version of the truth
(`proto/orkstack/v1/webhook_service.proto`, `Delivery`). The bounds are 2 KiB for
the URL, 1 KiB for each attempt's error and 16 KiB for the whole record
(`pkg/artifacts/deliveries.go`).

- The record is create-only. Nothing reads it to decide what to send. Nothing
  in OrkStack reclaims it, so a bucket lifecycle rule over the prefix is the
  operator's to set ([DEPLOY.md](DEPLOY.md), "Retention cleanup and bucket
  lifecycle rules").
- If a record cannot be written, the failure is logged with the delivery and
  webhook ids, and the delivery's outcome does not change.

## Listing and reading deliveries

`ListDeliveries` is a deployment-wide listing. It is narrowed by filters on
`created_at`, `namespace`, `run_id`, `webhook_id`, `event` and `delivered`. Each
filter is decided from the record's key, so a body is read only for a record
that matches (`internal/api/deliveries.go`).

- The default window is the seven days before now. With exactly one `run_id`
  filter that names a run the engine can read, the window starts at the run's
  start instead.
- A page holds 50 records by default, and at most 200.
- A scan that examines more than 100,000 keys before it fills a page is refused
  with `ResourceExhausted`. The remedy is to narrow the window or add filters.
- `GetDelivery` reads one record by id.

Both read the shared store, so any replica answers
([UI_CLI_PARITY.md](UI_CLI_PARITY.md), "Webhook deliveries").

## Redelivery

`RedeliverWebhook` posts one delivery again. It does so once, now, and
synchronously:

- to the subscription as it is now. If the subscription has been deleted,
  nothing is sent, and the call is refused with `FailedPrecondition`;
- signed with its current secret;
- with the run as it reads now, and with the original's event, transition
  timestamp and sequence. `pauseTransition`, `resumeCount` and `resultsKept`
  are omitted: a delivery's record does not hold them;
- with `X-Ork-Redelivery-Of` naming the original.

It makes no retry, since the operator is the retry policy. Its outcome is
recorded as a new delivery whose `redeliveryOf` names the original. The call is
audited as `webhook.redeliver`.

## CLI

Every `orkctl webhook` subcommand takes `--json`, `-n`, `--server` and
`--timeout` ([CLI.md](CLI.md), "orkctl webhook"). `deliveries` refuses `-n`.

| Command | Flags |
| --- | --- |
| `orkctl webhook create URL` | `--event E` (repeatable), `--secret S`, `-n NS`, `--orkflow NAME`, `--task ID`, `--name NAME` |
| `orkctl webhook list` | `--filter F` (repeatable) |
| `orkctl webhook update ID` | `--name NAME`, `--url URL`, `--event E` (repeatable) or `--clear-events`, `-n NS` or `--deployment-wide`, `--orkflow NAME`, `--task ID` |
| `orkctl webhook delete ID` | none beyond the inherited flags |
| `orkctl webhook deliveries` | `--filter F` (repeatable), `--limit N`, `--page-token T` |
| `orkctl webhook delivery ID` | none beyond the inherited flags |
| `orkctl webhook redeliver ID` | none beyond the inherited flags; exits `1` when the receiver did not accept the delivery |

- `create` prints the signing secret once. The default scope is every
  namespace. Only an explicit `-n` narrows it, even when `ORK_NAMESPACE` or the
  saved namespace is set. `--name` needs an explicit `-n`.
- `update` is read-modify-write: it changes only the flags given. `--clear-events`
  empties the list, so the run's lifecycle is selected. `--deployment-wide`
  clears the namespace scope.

```bash
orkctl webhook create https://example.test/hooks --event RUN_SUCCEEDED --event RUN_FAILED
orkctl webhook create https://example.test/load -n analytics --orkflow nightly-etl \
  --task extract --event TASK_SUCCEEDED
orkctl webhook deliveries -F 'delivered=false' --limit 20
orkctl webhook redeliver dlv-…
```

## The automation document

A namespace's [automation document](AUTOMATION.md) declares its webhooks by
name, under `webhooks:`:

```yaml
webhooks:
  etl-loader:
    url: https://hooks.acme.dev/etl
    orkflow: nightly-etl
    events:
      - TASK_SUCCEEDED
      - RUN_FAILED
    secret_env: ETL_WEBHOOK_SECRET
```

The keys are `url` (required), `events`, `orkflow`, `task` and `secret_env`. An
absent `events` selects the run's lifecycle (above), an absent `orkflow` every Orkflow
(a present one is the namespace's own, never another's shared one),
and an absent `task` every task. An absent `secret_env` generates the signing
secret, which is disclosed once ([AUTOMATION.md](AUTOMATION.md), "webhooks").

- The entry's key is the subscription's `name`. A document-declared webhook is
  always scoped to its namespace, and a `namespace:` key in an entry is refused
  (`internal/automation/parse.go`).
- `secret_env` names a variable that the **client** reads when it creates the
  subscription. The pre-shared secret never enters the document, the plan or the
  engine, except as the one `RegisterWebhook` that stores it. An automation apply never
  changes the secret of an existing subscription, so `secret_env` has no effect
  on one that already exists ([AUTOMATION.md](AUTOMATION.md), "webhooks").
- `orkctl automation apply --prune` deletes only named subscriptions that a section declares.
  It never deletes an unnamed subscription or a deployment-wide one.

## The UI

Admin → Webhooks ([UI_CLI_PARITY.md](UI_CLI_PARITY.md), "Webhook administration"
and "Webhook deliveries"; `ui/src/routes/admin/webhooks/+page.svelte`):

- lists every subscription, deployment-wide, with its scope;
- creates one, with an optional signing secret. The secret is shown once, with
  the note that it will not be shown again;
- edits a subscription's name, URL, events and scope. The id and the secret are kept;
- deletes a subscription;
- shows the **Deliveries** panel, with filters, each record's signature and body
  digest, its attempts, and a link per subscription;
- redelivers a record, behind a confirmation that describes one signed `POST` of
  the run as it reads today.

## Audit

Every webhook write is an audited control-plane write: `webhook.register`,
`webhook.update`, `webhook.delete` and `webhook.redeliver`. An audit record is
deployment-wide. It carries the URL with any password redacted, the event names
and the scope. It never carries the secret or the body (`internal/api/audit.go`,
`webhookAuditDetail`). Its principal is the verified caller with authentication
on, and otherwise the `Ork-Principal` the gateway asserted, which OrkStack does
not authenticate ([SECURITY_MODEL.md](SECURITY_MODEL.md), "The principal is
verified or asserted, and says which"). Delivery itself is not audited, because the delivery
record is its record ([UI_CLI_PARITY.md](UI_CLI_PARITY.md), "Control-plane audit
trail").

## What webhooks do not do

- **No queue and no pacing.** A matching event is sent as it happens. Nothing
  buffers or spaces deliveries beyond the retry ladder, and nothing reads the
  stored bodies to decide what to send.
- **No retry beyond the ladder.** Nothing re-sends a delivery that gave up,
  except an operator's redelivery.
- **No content filter.** Scope narrows by namespace, Orkflow and task. It does
  not narrow by a field of the run.
- **No exactly-once and no ordered sending.** Deliveries can arrive in any
  order; a receiver orders them by `sequence`. See "Duplicates and ordering".
- **No setting that turns webhooks off.** The notifier runs in every engine
  (`cmd/ork/main.go`). A deployment with no subscriptions queues nothing.
- **No reconciliation.** A webhook is not a state source. Reconcile with `GetRun`.
- **No call to a non-public address.** See "Where a delivery may go".
  [SECURITY_MODEL.md](SECURITY_MODEL.md), "Webhook secrets and destinations", has the detail.

## Related

- [ORKFLOW_TRIGGERS.md](ORKFLOW_TRIGGERS.md): the inbound counterpart.
- [ORKFLOW_SCHEDULES.md](ORKFLOW_SCHEDULES.md): the third run starter.
- [AUTOMATION.md](AUTOMATION.md): declaring webhooks by name in a document.
- [UI_CLI_PARITY.md](UI_CLI_PARITY.md): the webhook surfaces and their parity.
- [STORAGE.md](STORAGE.md): the registry and delivery-record keys.
- [DEPLOY.md](DEPLOY.md): operating the shared store and its lifecycle rules.
- [CLI.md](CLI.md): the full `orkctl webhook` reference.
- [SECURITY_MODEL.md](SECURITY_MODEL.md): where webhook secrets sit in the model.
