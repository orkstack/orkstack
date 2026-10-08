# Inbound Orkflow triggers

A **trigger** is an object bound to exactly one Orkflow — a namespace, an
Orkflow name, and an optional version pin — that an external caller invokes
over plain HTTP with a bearer token to start one run of that Orkflow. An empty
version pin means the Orkflow's current pointer, resolved at invoke time,
exactly as `SubmitRunRequest.orkflow_version` defaults. A trigger is not a
webhook, a schedule ([ORKFLOW_SCHEDULES.md](ORKFLOW_SCHEDULES.md)), a
subscription, or a new execution authority: an invocation is one `SubmitRun`
call, and Temporal owns everything after it.

`TriggerService` (`proto/orkstack/v1/service.proto`) manages triggers;
`orkctl trigger` and the Orkflow page's **Triggers** tab are its two clients,
with parity as [UI_CLI_PARITY.md](UI_CLI_PARITY.md) defines it.

## Trigger versus webhook

The two constructs are mirror images, not variants of one another.

| | Webhook | Trigger |
| --- | --- | --- |
| Direction | Outbound: OrkStack calls the subscriber | Inbound: the caller calls OrkStack |
| Scope | Deployment-wide unless it names an exact-match namespace/orkflow/task scope ([UI_CLI_PARITY.md](UI_CLI_PARITY.md), "Scoped subscriptions and task outcome events") | One Orkflow in one namespace, optionally one pinned version |
| Authentication | OrkStack signs each delivery with the stored secret (HMAC-SHA256) | The caller presents the trigger's bearer token; OrkStack verifies it |
| Secret at rest | Plaintext, by necessity: delivery replays it into a signature | Salted SHA-256 digest and salt only; the plaintext is never stored |
| What the secret proves | That a delivery came from this deployment | That the caller may start this one Orkflow |
| Failure semantics | Best-effort delivery; a subscriber outage never fails a run | Synchronous; the HTTP response is the outcome of one `SubmitRun` |

A per-trigger bearer token was chosen over request signing as the boring API
(`AGENTS.md` §14).

## The trigger object

| Field | Meaning |
| --- | --- |
| `id` | Opaque, `trg-{16 hex}` (8 bytes from `crypto/rand`). Nothing parses it or derives a namespace or Orkflow from it. Trigger ids are not confidential; only tokens are. |
| `namespace` | A coordinate, never a filter. Empty is `default` on the wire. |
| `orkflow_name` | Fixed at creation. Rebinding is delete then create. |
| `orkflow_version` | Empty follows the current pointer at each invoke (rendered `current`); a pin never falls through to current. |
| `description` | Optional operator label. It becomes the started run's `origin.description`. |
| `name` | Optional handle: a slug (`^[a-z][a-z0-9-]*$`, at most 200 bytes), unique among the named triggers of its namespace. It is how an automation document addresses the trigger ([AUTOMATION.md](AUTOMATION.md)); it is not part of the invoke URL and never selects what a run starts. |

`CreateTriggerResponse.secret` is a 32-byte `crypto/rand` token, hex-encoded,
disclosed exactly once. No other message carries it.

### Management RPCs

- **`CreateTrigger`** resolves the namespace, then confirms the Orkflow — and
  the pinned version, when one is given — exists, with `SubmitRun`'s own
  errors in that order. A refused create writes nothing: a trigger never
  silently points at nothing.
- **`ListTriggers`** returns one namespace's triggers; `orkflow_name` narrows
  to one Orkflow. It reads the shared store on every call, so any replica
  answers for a trigger another created. A listing never carries a token or
  its digest.
- **`UpdateTrigger`** replaces every mutable field — name, pin, description —
  in place, after `CreateTrigger`'s checks in the same order (an absent
  trigger, or one of another namespace, is NotFound; a pinned version must
  exist). The id, URL, Orkflow and token digest are carried over untouched:
  **an update never re-issues a token**. On S3 it is a conditional put on the
  ETag it read, so it can never recreate a trigger deleted meanwhile; last
  writer wins otherwise. A name another trigger of the namespace holds is
  AlreadyExists. Name uniqueness is checked by the store immediately before
  the write and is best-effort across replicas; the state a race can leave
  (two triggers sharing a name) is refused, with the fix, by every automation
  plan and export.
- **`DeleteTrigger`** removes one record addressed by id and namespace (another
  namespace's trigger is NotFound and nothing is deleted). Afterwards an
  invoke of that id is `404` on every replica. A storage failure is an error,
  never a successful delete.

### Storage and secret handling

Every record is one flat object, beside the webhook registry and on the same
`ORK_ARTIFACT_BACKEND` ([STORAGE.md](STORAGE.md)):

```text
metadata/triggers/{id}.json
```

The key is flat, not scoped by namespace and Orkflow, because the invoke URL
carries no namespace: `POST /v1/triggers/{id}` is one opaque thing to paste
into a caller, so the bare id is the only lookup the invoke path can make.
`namespace` and `orkflow_name` are fields inside the record, and a listing
filters the collection in application code — triggers are few and
hand-made, so no secondary index earns its keep. Creates are conditional
(`IfNoneMatch: "*"` on S3, an atomic rename locally), and no process keeps a
snapshot.

**The token is never stored.** A webhook secret must sit in plaintext because
delivery replays it into a signature; a trigger token is only ever compared.
The record holds a per-trigger random salt and `sha256(salt || token)`, and
the invoke path compares with `crypto/subtle.ConstantTimeCompare` — a property
tests hold the source to, since a timing measurement is not evidence. Await
callback tokens ([DSL.md](DSL.md), "Awaited tasks") are stored and compared
the same way. A record, a listing, a log line or
a support bundle can therefore never disclose a token. A fast hash is correct
here, not bcrypt or Argon2: those slow a brute force against a low-entropy
human secret, and a 32-byte random token has no low-entropy space to search —
a slow function would only cost every caller latency.

## Invoking a trigger

```text
POST /v1/triggers/{id}
Authorization: Bearer <token>
Content-Type: application/json
Idempotency-Key: <key>          (optional)
Ork-Origin-Reference: <text>    (optional; e.g. the CI job URL, recorded on the run)
Ork-Principal: <name>           (optional; recorded, never authenticated)

<the run's input JSON object>
```

The route is plain JSON on the API port (not Connect), so a CI job, a cron
host or a `curl` one-liner can start a run without a client library. In
order:

1. A missing `Authorization` header, or one that is not `Bearer <token>`, is
   `401` before any lookup.
2. The id is resolved from the shared store on every call; an unknown or
   deleted id is `404`.
3. The token is hashed with the record's salt and compared in constant time;
   a mismatch is `401`. A wrong token on a real id (401) and an unknown id
   (404) are deliberately distinguishable.
4. The body **is** the run's input object, not an envelope: `{"count": 3}`
   arrives as `data.count == 3`. It is bounded at 1 MiB (the bound every
   plain-HTTP route on the port shares), must be exactly one JSON object, and
   an empty body means no input — the declared defaults apply.
5. `Ork-Origin-Reference` is validated (at most 256 bytes, no control
   character) before anything is minted; an invalid one is `400`.
6. The run is started through the ordinary submit path with the namespace,
   Orkflow and version **copied from the stored record**,
   `allow_empty_pool = false`, the `Idempotency-Key` header as the
   idempotency key, and origin `{TRIGGER, <trigger id>, <reference>,
   <trigger description>, <principal>}`.

**The load-bearing invariant:** the namespace, Orkflow and version a trigger
starts come only from the stored record. No path, query, header or body
field selects them — a body key named `orkflow_name` is input data like any
other. A token can start exactly the Orkflow its trigger was created for, and
a trigger cannot be redirected.

Success is `201 Created` with the `SubmitRunResponse` envelope in the
short-enum dialect of [JSON.md](JSON.md), exactly as `orkctl run submit
--json` prints it:

```json
{"run":{"id":"ork-…","orkflowName":"hello","orkflowVersion":"3","status":"PENDING","namespace":"team-a","origin":{"kind":"TRIGGER","id":"trg-…"}}}
```

`orkflowVersion` is the version actually resolved, never empty. When the
Orkflow declares an awaited task with `callback: true`, the envelope also
carries `awaitCallbacks` — each callback path and one-time token, disclosed
by this response and nothing else ([DSL.md](DSL.md), "Awaited tasks").

Every failure is `{"error":{"code":"…","message":"…"}}`:

| Condition | HTTP status | `error.code` |
| --- | --- | --- |
| `Authorization` missing or not `Bearer <token>` | 401 | `unauthenticated` |
| Token does not match the record | 401 | `unauthenticated` |
| Unknown or deleted id | 404 | `not_found` |
| Trigger store unreadable | 500 | `storage_error` |
| Body not one JSON object, or over 1 MiB; malformed or over-long `Ork-Origin-Reference`; the stored definition names a secret source its namespace does not allow | 400 | `invalid_argument` |
| Orkflow or pinned version no longer exists | 404 | `not_found` |
| A resolved pool has no live Orker; the namespace is held, at `max_running`, or over (or unable to measure) an enforced budget; the image policy refuses the definition's image; a composed child can no longer be planned | 412 | `failed_precondition` |
| Object storage or Temporal unavailable at submit | 503 | `unavailable` |
| Anything else | 500 | `internal` |

Rows from `400` down are `SubmitRun`'s own Connect codes carried through; the
adapter adds no classification of its own and never turns an error into a
`201`. The `400`, `404` and `412` refusals happen in `SubmitRun`'s preparation
before a run id is minted, so a refused invoke leaves nothing behind: no id
burnt, no claim taken, no run recorded.

`Idempotency-Key` is `SubmitRun`'s contract unchanged: the claim is scoped to
the namespace, so a repeated key on any replica returns the run it names
(same id, `201`) rather than starting another. It is per namespace, not per
trigger — two triggers of one namespace presenting one key name one run — so
a caller that needs one run per external event supplies that event's own
identifier.

Invocation is synchronous. One request is one `SubmitRun`: no queue, no
retry, no acknowledge-then-start. A `201` means the run exists and is durably
accepted (`AGENTS.md` §15); any other status means this request started no
run, and nothing is left to deliver later.

## CLI

```text
orkctl trigger create --orkflow NAME [--version V] [--description TEXT] [--name NAME] [-n NS]
orkctl trigger list   [--orkflow NAME] [-n NS]
orkctl trigger update ID [--name NAME] [--version V | --follow-current] [--description TEXT] [-n NS]
orkctl trigger delete ID [-n NS]
```

`create` prints the one-time token and a ready-to-run `curl` against the
`--server` address the command used, every value shell-escaped:

```text
curl -X POST http://localhost:8080/v1/triggers/trg-… \
  -H 'Authorization: Bearer <token>' \
  -H 'Content-Type: application/json' \
  -d '{}'
# optional: -H 'Ork-Origin-Reference: <your CI job URL>'
```

`update` is read-modify-write: it reads the trigger, replaces only the fields
whose flag was given (`--name ""` unnames, `--follow-current` clears the pin,
`--description ""` clears the label), and says the token is unchanged.
`list` has a `NAME` column (`-` when unnamed). `--json` is the raw response
(`{"trigger":{…},"secret":"…"}` for create, `{"triggers":[…]}` for list).
An unpinned trigger follows current, so a promotion moves what it starts;
`orkctl orkflow callers <name>` names it ([DSL.md](DSL.md), "Promoting
compares declared interfaces").

## UI

The Orkflow page's **Triggers** tab, namespace-scoped by construction:

- a scope statement first — each trigger is labelled
  `Runs {name}@{version or current} in {namespace}` — so a reader never
  assumes a trigger reaches another Orkflow or namespace;
- a create form (optional name, version `Select` of `Current` and each
  stored version, optional description), with a server error in an
  `ErrorPanel`;
- the one-time panel after a create: a runnable `curl` embedding the real
  token, stating it will not be shown again, never rebuilt from a listing;
- the list: name (*unnamed* when empty), id, pin or `current`, description,
  created; a per-row `curl` with the placeholder `$ORKSTACK_TRIGGER_TOKEN`;
  **Edit** (name, version, description — *"Editing keeps this trigger's URL
  and token. Its orkflow cannot change."*) and **Delete** behind a
  confirmation.

## What triggers do not do

- **No rate limiting.** The route is reachable by whoever can reach the API
  port; abuse protection is the operator's gateway (`AGENTS.md` §13). What
  bounds a legitimate upstream that has started looping is the namespace's
  admission control — `orkctl namespace update NS --hold REASON` refuses every
  new run (an invoke is `412` carrying the reason) and `--max-running N` caps
  runs in flight ([DEPLOY.md](DEPLOY.md), "Namespace admission").
- **No `allow_empty_pool`.** A pool with no live Orker fails the invoke `412`
  rather than starting a run that never leaves `PENDING`.
- **No delivery retries or delivery state.** Success or failure is the HTTP
  response itself.
- **No configuration, expiry or toggle.** A deployment that never creates a
  trigger runs exactly as if triggers did not exist.
- **No rotation or re-disclosure.** Rotating a token is delete then create,
  which yields a new id and URL.
- **No per-invocation record on the trigger.** Provenance is the run's own
  origin (`kind TRIGGER`, the trigger id, the caller's reference); a
  trigger-started run notifies webhooks like any other run. Trigger
  invocations are not audited: the run's own record is the durable fact.
