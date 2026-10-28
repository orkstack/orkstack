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
| `RUN_STARTED` | the run moves from pending to running |
| `RUN_SUCCEEDED` | the run ends successfully |
| `RUN_FAILED` | the run ends failed |
| `RUN_CANCELED` | the run ends cancelled |
| `TASK_SCHEDULED` | a task is handed to the scheduler |
| `TASK_STARTED` | a task is dispatched to an Orker; the activity is scheduled, which does not mean a container is running |
| `TASK_AWAITING` | an awaited task starts waiting to be resolved |
| `TASK_RESOLVED` | an awaited task is resolved from outside its run: `SUCCEEDED` for a completion, `FAILED` for a rejection |
| `TASK_SUCCEEDED` | a task's success is committed |
| `TASK_FAILED` | a task fails for good, after its retries stop; a task cancelled because its run ended is never delivered as this event |

- An empty `events` selects the four run transitions and no task event. Task
  events are opt-in, by name.
- `TASK_LOG` is refused: a log line is not a transition (`ParseEvent`).
- Duplicates are removed and the list is kept in lifecycle order on registration
  (`normalizeEvents`).
- A `TASK_FAILED` delivery always reads `"status":"FAILED"`, and a task that was
  skipped, or still pending or running when its run ended, is never delivered as
  a task outcome ([UI_CLI_PARITY.md](UI_CLI_PARITY.md), "Scoped subscriptions and
  task outcome events").

### Scope

A subscription has three scope fields. Each is an exact match on one coordinate,
and an empty field matches every value.

| Field | Matches |
| --- | --- |
| `namespace` | runs of one namespace. A run that names none is in `default`, so a subscription scoped to `default` matches it |
| `orkflow_name` | runs of one Orkflow, by name |
| `task_id` | one task's outcome events. It is consulted only for `TASK_*` events, so a `RUN_*` event passes it |

- A subscription with no scope is deployment-wide. It receives the transitions of
  every namespace.
- A `task_id` needs at least one `TASK_*` event in `events`. Otherwise the task
  filter could never be consulted, and registration refuses it
  (`ValidateScope`).
- Each scope field is a slug (`^[a-z][a-z0-9-]*$`, at most 200 bytes). It is
  validated and never sanitised.
- Nothing checks that the namespace, Orkflow or task exists, so a subscription
  may be registered before the Orkflow it names is applied
  ([UI_CLI_PARITY.md](UI_CLI_PARITY.md), "Scoped subscriptions and task outcome
  events").

### The webhook object

| Field | Meaning |
| --- | --- |
| `id` | `wh-` and 16 hex characters: 8 bytes from `crypto/rand`. Opaque; it survives an update |
| `url` | an absolute `http` or `https` URL with a host (`ValidateURL`) |
| `events` | the transitions, as above; empty means the four run transitions |
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

Plain JSON routes remain as compatibility adapters over the same registry
(`internal/api/webhooks.go`): `POST /v1/webhooks` answers `201` with the
subscription and its secret, `GET /v1/webhooks` lists, and
`GET /v1/webhooks/{id}` and `DELETE /v1/webhooks/{id}` read and delete. The
adapter has no update. Its writes are audited by the adapter itself. Clients
use the generated Connect types.

- **Register** validates the events, the URL, the scope and the name, in that
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
each operation, so a subscription registered through one replica is delivered
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
X-Ork-Attempt: 1
X-Ork-Redelivery-Of: dlv-…               (redelivery only)
```

The body is one JSON object, in this field order:

```json
{"runId":"ork-…","orkflowName":"…","status":"FAILED","timestamp":"<RFC 3339 instant>","taskId":"…","attempt":2,"run":{…}}
```

- `taskId` and `attempt` are present only for a task event. `attempt` is the
  task's own attempt number, and it is omitted when the engine has not numbered
  it. It is never invented.
- `status` is the status the event carries.
- `run` is the run in the JSON dialect of [JSON.md](JSON.md), with its input,
  its output and its origin. Fan-out items are removed (`engine.WithoutItems`).
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

### Attempts and retries

- A delivery is one `POST` to one subscription for one event. Every queued
  delivery has its own `X-Ork-Delivery` id.
- A `2xx` response ends the attempt series as delivered. A transport error and
  any `5xx` are retried. Any other response ends the series as not delivered, so
  a `4xx` is never retried (`pkg/webhooks/deliver.go`, `retryable`).
- At most four requests are made: the first, then retries after 100 ms, 200 ms
  and 400 ms. Each request is bounded at 10 s. These are the dispatcher's
  defaults, which the engine uses unchanged (`cmd/ork/main.go`).
- The body is rendered once. Every attempt sends the same bytes with the same
  signature.
- The receiver's response body is read for at most 2 KiB, then discarded.
- The dispatcher sets no redirect policy, so Go's HTTP client applies its
  default: it follows up to ten redirects. A `307` or `308` re-sends the body.
  When the host changes, the client strips the sensitive headers, such as
  `Authorization` and `Cookie`, but it copies the other headers, `X-Ork-Signature`
  among them. A subscriber URL should therefore answer without redirecting.
- Delivery runs off the run's path. Each event's subscription lookup is bounded
  at 5 s, and each delivery runs in its own goroutine. A failed lookup is logged
  and never fails the run.
- At shutdown the engine waits up to 15 s for deliveries in flight
  (`webhookDrainTimeout`, `cmd/ork/main.go`). Nothing persists a delivery before
  it is sent, so a delivery not yet settled when the process stops is lost.

### Duplicates and ordering

- Delivery is best-effort, and there is no ordering across deliveries
  ([UI_CLI_PARITY.md](UI_CLI_PARITY.md), "Webhook administration").
- Whether an event is announced is decided by each replica's own event bus. An
  activity retry that lands on another replica announces the event again
  (`internal/engine/activities.go`, `EventObserver`). The same transition can
  therefore arrive twice, under two `X-Ork-Delivery` ids.
- A receiver that cannot tolerate a repeat should record what it has processed.
  It should reconcile with `GetRun` when it must not miss a transition.

## Delivery records

Every delivery that settles, whether it landed or was given up, is written once
as an immutable record ([STORAGE.md](STORAGE.md), "Key layout"):

```text
metadata/deliveries/{yyyy}/{mm}/{dd}/{delivery-id}/{namespace}/{run-id}/{webhook-id}/{event}/{delivered|failed}.json
```

A record holds:

- every attempt: its number, status code, error, backoff, duration and start time;
- the subscription, run, namespace, Orkflow, task, event, status and URL;
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
- with the run as it reads now, and with the original's event and transition
  timestamp;
- with `X-Ork-Redelivery-Of` naming the original.

It makes no retry, since the operator is the retry policy. Its outcome is
recorded as a new delivery whose `redeliveryOf` names the original. The call is
audited as `webhook.redeliver`.

## CLI

Every `orkctl webhook` subcommand takes `--json`, `-n`, `--server` and
`--timeout` ([CLI.md](CLI.md), "orkctl webhook"). `deliveries` refuses `-n`.

| Command | Flags |
| --- | --- |
| `orkctl webhook register URL` | `--event E` (repeatable), `--secret S`, `-n NS`, `--orkflow NAME`, `--task ID`, `--name NAME` |
| `orkctl webhook list` | `--filter F` (repeatable) |
| `orkctl webhook update ID` | `--name NAME`, `--url URL`, `--event E` (repeatable) or `--clear-events`, `-n NS` or `--deployment-wide`, `--orkflow NAME`, `--task ID` |
| `orkctl webhook delete ID` | none beyond the inherited flags |
| `orkctl webhook deliveries` | `--filter F` (repeatable), `--limit N`, `--page-token T` |
| `orkctl webhook delivery ID` | none beyond the inherited flags |
| `orkctl webhook redeliver ID` | none beyond the inherited flags; exits `1` when the receiver did not accept the delivery |

- `register` prints the signing secret once. The default scope is every
  namespace. Only an explicit `-n` narrows it, even when `ORK_NAMESPACE` or the
  saved namespace is set. `--name` needs an explicit `-n`.
- `update` is read-modify-write: it changes only the flags given. `--clear-events`
  empties the list, so every run transition is selected. `--deployment-wide`
  clears the namespace scope.

```bash
orkctl webhook register https://example.test/hooks --event RUN_SUCCEEDED --event RUN_FAILED
orkctl webhook register https://example.test/load -n analytics --orkflow nightly-etl \
  --task extract --event TASK_SUCCEEDED
orkctl webhook deliveries --filter 'delivered=false' --limit 20
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
absent `events` selects every run transition, an absent `orkflow` every Orkflow,
and an absent `task` every task. An absent `secret_env` generates the signing
secret, which is disclosed once ([AUTOMATION.md](AUTOMATION.md), "webhooks").

- The entry's key is the subscription's `name`. A document-declared webhook is
  always scoped to its namespace, and a `namespace:` key in an entry is refused
  (`internal/automation/parse.go`).
- `secret_env` names a variable that the **client** reads when it creates the
  subscription. The pre-shared secret never enters the document, the plan or the
  engine, except as the one `RegisterWebhook` that stores it. An apply never
  changes the secret of an existing subscription, so `secret_env` has no effect
  on one that already exists ([AUTOMATION.md](AUTOMATION.md), "webhooks").
- `apply --prune` deletes only named subscriptions that a section declares.
  It never deletes an unnamed subscription or a deployment-wide one.

## The UI

Admin → Webhooks ([UI_CLI_PARITY.md](UI_CLI_PARITY.md), "Webhook administration"
and "Webhook deliveries"; `ui/src/routes/admin/webhooks/+page.svelte`):

- lists every subscription, deployment-wide, with its scope;
- registers one, with an optional signing secret. The secret is shown once, with
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
`webhookAuditDetail`). Its principal is the `Ork-Principal` the gateway asserted,
which OrkStack does not authenticate ([SECURITY_MODEL.md](SECURITY_MODEL.md), "The
principal is asserted, not authenticated"). Delivery itself is not audited, because the delivery
record is its record ([UI_CLI_PARITY.md](UI_CLI_PARITY.md), "Control-plane audit
trail").

## What webhooks do not do

- **No queue and no pacing.** A matching event is sent as it happens. Nothing
  buffers or spaces deliveries beyond the retry ladder.
- **No retry beyond the ladder.** Nothing re-sends a delivery that gave up,
  except an operator's redelivery.
- **No content filter.** Scope narrows by namespace, Orkflow and task. It does
  not narrow by a field of the run.
- **No exactly-once and no ordering.** See "Duplicates and ordering".
- **No setting that turns webhooks off.** The notifier runs in every engine
  (`cmd/ork/main.go`). A deployment with no subscriptions queues nothing.
- **No reconciliation.** A webhook is not a state source. Reconcile with `GetRun`.
- **No destination check.** Any reachable `http` or `https` URL can be named.
  See [SECURITY_MODEL.md](SECURITY_MODEL.md), "Webhook secrets and destinations".

## Related

- [ORKFLOW_TRIGGERS.md](ORKFLOW_TRIGGERS.md): the inbound counterpart.
- [ORKFLOW_SCHEDULES.md](ORKFLOW_SCHEDULES.md): the third run starter.
- [AUTOMATION.md](AUTOMATION.md): declaring webhooks by name in a document.
- [UI_CLI_PARITY.md](UI_CLI_PARITY.md): the webhook surfaces and their parity.
- [STORAGE.md](STORAGE.md): the registry and delivery-record keys.
- [DEPLOY.md](DEPLOY.md): operating the shared store and its lifecycle rules.
- [CLI.md](CLI.md): the full `orkctl webhook` reference.
- [SECURITY_MODEL.md](SECURITY_MODEL.md): where webhook secrets sit in the model.
