# Inbound Orkflow trigger contract

This document specifies the inbound trigger capability accepted on 2026-09-08.
It is an implementation and acceptance checklist, not a claim that its gates
have passed. `AGENTS.md` remains authoritative. Proto is the source of truth
for shared types; the `TriggerService` additions have landed in
`proto/orkstack/v1/service.proto` and are quoted below as landed. Run
`make proto` before any Go or TypeScript code uses them.

A **Trigger** means an object bound to exactly one Orkflow — a namespace, an
Orkflow name, and an optional version pin — that an external caller invokes
over plain HTTP with a bearer token to start one run of that Orkflow. An empty
version pin means the Orkflow's current pointer, resolved at invoke time,
exactly as `SubmitRunRequest.orkflow_version` already defaults. A Trigger is
not a webhook, a schedule, a subscription, or a new execution authority: an
invocation is one `SubmitRun` call, and Temporal remains the sole owner of
what happens next.

The capability is additive. The outbound `WebhookService`, the Admin Webhooks
page, `orkctl webhook`, and their delivery semantics are kept exactly as
`docs/UI_CLI_PARITY.md` specifies them. Parity applies as that document
defines it: the Triggers tab and `orkctl trigger` accomplish the same
operations with the same selectors, defaults, results, and errors.

## Trigger versus webhook

The two constructs are mirror images, not variants of one another.

| | Webhook (existing, unchanged) | Trigger (this contract) |
| --- | --- | --- |
| Direction | Outbound: OrkStack calls the subscriber | Inbound: the caller calls OrkStack |
| Scope | Deployment-wide event subscription unless it names an exact-match namespace/orkflow/task scope (`docs/UI_CLI_PARITY.md`, "Scoped subscriptions"); the namespace picker does not filter the listing | One Orkflow in one namespace, optionally one pinned version |
| Authentication | OrkStack signs each delivery with the stored secret (HMAC-SHA256) | The caller presents the trigger's bearer token; OrkStack verifies it |
| Secret at rest | Plaintext, by necessity: delivery replays it into a signature | Salted SHA-256 digest and salt only; the plaintext is never stored |
| What the secret proves | That a delivery came from this deployment | That the caller may start this one Orkflow |
| Failure semantics | Best-effort delivery; a subscriber outage never fails a run | Synchronous; the HTTP response is the outcome of one `SubmitRun` |

The bearer token is deliberate. OrkStack had no inbound caller-authentication
mechanism before this contract; a per-trigger bearer token was accepted over
request signing as the boring API (`AGENTS.md` §14).

## Shared API changes

### Proto

`proto/orkstack/v1/service.proto`, beside `WebhookService`, as landed:

```proto
// TriggerService manages inbound run starters. A trigger is bound to ONE
// orkflow in one namespace, and an external caller presents that trigger's
// bearer token over plain HTTP to start a run of it.
//
// It is the mirror image of WebhookService, not a variant of it. A webhook is
// OUTBOUND and deployment-wide: OrkStack calls the subscriber when any run
// changes state. A trigger is INBOUND and orkflow-scoped: the caller calls
// OrkStack, authenticating as the trigger. The orkflow coordinates come from
// the stored record and never from the request, so whoever holds a token can
// only start the orkflow the trigger was created for.
//
// The secret is handed back exactly once, at creation, on the same terms as
// RegisterWebhookResponse.secret: a listing never carries it again.
service TriggerService {
  rpc CreateTrigger(CreateTriggerRequest) returns (CreateTriggerResponse);
  rpc ListTriggers(ListTriggersRequest) returns (ListTriggersResponse);
  rpc DeleteTrigger(DeleteTriggerRequest) returns (DeleteTriggerResponse);
}

message Trigger {
  string id = 1;
  string namespace = 2;
  string orkflow_name = 3;
  string orkflow_version = 4;              // empty = current pointer, resolved at invoke time
  string description = 5;                  // optional operator label
  google.protobuf.Timestamp created_at = 6;
}
message CreateTriggerRequest {
  string namespace = 1;
  string orkflow_name = 2;
  string orkflow_version = 3;
  string description = 4;
}
message CreateTriggerResponse {
  Trigger trigger = 1;
  string secret = 2;                       // shown exactly once, like RegisterWebhookResponse.secret
}
message ListTriggersRequest {
  string namespace = 1;
  string orkflow_name = 2;                 // empty = every trigger in the namespace
}
message ListTriggersResponse { repeated Trigger triggers = 1; }
message DeleteTriggerRequest { string id = 1; string namespace = 2; }
message DeleteTriggerResponse {}
```

Field semantics beyond the comments above:

- `Trigger.id` is opaque. It is shaped `trg-{16 hex}`, generated the same way
  as `wh-{16 hex}` (8 bytes from `crypto/rand`, hex-encoded). Nothing parses
  it or derives a namespace or Orkflow from it.
- `Trigger.namespace` is a coordinate, never a filter. Empty is `default` on
  the wire, on the same rule every other namespace field follows.
- `CreateTriggerResponse.secret` is a 32-byte `crypto/rand` token, hex-encoded,
  disclosed exactly once. No other message carries it; a listing never
  contains it.
- `DeleteTriggerRequest.namespace` addresses the trigger inside its namespace.
  A trigger of another namespace is NotFound, not forbidden and not deleted.

### Management RPC behavior

`internal/api/trigger_service.go` holds the Connect handlers, in the shape of
`internal/api/webhook_service.go`.

`CreateTrigger` resolves the namespace first and then confirms the named
Orkflow exists — and the pinned version exists, when one is given — before
it writes anything. These are the checks `SubmitRun`'s own preparation
already performs, and they return the same errors: a namespace error first,
then NotFound for an Orkflow or version that does not exist. A refused create
writes nothing. A trigger must never silently point at nothing. `description`
is an optional operator label, returned as given.

`ListTriggers` returns the triggers of one namespace; `orkflow_name` narrows
the result to one Orkflow and empty means every trigger in the namespace.
The list is read from the shared store on every call and never from a
startup snapshot, so a replica answers for a trigger another replica created.

`DeleteTrigger` removes exactly one record, addressed by id and namespace.
Success means the object is gone from the shared store; a later invoke of
that id is `404` on every replica. Storage failure is an error, never a
successful delete or create.

### Storage and secret handling

`pkg/artifacts/trigger_store.go` mirrors `pkg/artifacts/webhook_store.go`:
a Local store that writes a temporary sibling and publishes it atomically, and
an S3 store that creates with a conditional PUT (`IfNoneMatch: "*"`) so two
replicas cannot both create the same object. Selection follows
`ORK_ARTIFACT_BACKEND`, the same backend that holds artifacts, run history,
and webhooks. It is a registry, not an artifact: callers read it directly
and no process keeps a snapshot.

Two differences from the webhook store are deliberate.

**The key layout is FLAT, matching webhooks exactly.** Every trigger record is
one object at

```text
metadata/triggers/{id}.json
```

An earlier draft of this contract scoped the key by namespace and Orkflow, on
the reasoning that triggers are listed by (namespace, Orkflow) far more often
than deployment-wide. That draft did not survive contact with the invoke
endpoint: `POST /v1/triggers/{id}` carries no namespace in the path (a trigger
URL is one opaque thing to paste into a caller, mirroring
`/v1/webhooks/{id}`), so resolving a trigger from its bare id is the *only*
lookup the hot path can do — a namespace-prefixed key cannot be built without
already knowing the namespace, which is exactly what the lookup exists to
answer. `namespace` and `orkflow_name` live as fields *inside* the record
instead. `ListTriggers(namespace, orkflow_name)` reads the flat collection and
filters in application code — triggers are a low-cardinality, manually-created
resource (an operator makes a handful per Orkflow), so this is the same
"list everything, filter in front of the caller" shape `ListWebhooks` already
uses deployment-wide; no secondary index is needed at this scale.

**The secret is never stored in plaintext.** A webhook secret must sit in
plaintext at rest because outbound delivery replays it into an HMAC
signature on every delivery. A trigger secret is only ever compared. The
record therefore holds

```text
salt     random bytes, generated per trigger, not secret
digest   sha256(salt || secret)
```

and never the secret itself. On invoke, the presented bearer token is hashed
with the stored salt and the result is compared with
`crypto/subtle.ConstantTimeCompare` (`hmac.Equal` has the same effect). A
record read back from the store, a listing, a log line, and a support bundle
can therefore never disclose a token; the only copy of the plaintext is the
one handed to the creator, once.

A fast hash is the correct choice here, not bcrypt, scrypt, or Argon2. Those
functions exist to slow a brute force against a low-entropy secret a human
chose. A trigger secret is 32 bytes from `crypto/rand`: there is no
low-entropy space to search, so slowing each guess buys nothing, and a
deliberately slow function on the invoke path would only cost every caller
latency on every call.

### HTTP invoke contract

`internal/api/triggers.go` is a plain-JSON HTTP adapter on the API port,
following `internal/api/webhooks.go` exactly: a small dedicated
`http.ServeMux` with method-prefixed patterns and `r.PathValue("id")`,
registered in `internal/api/server.go` beside `WebhookCollectionPath` and
`WebhookItemPath`. It serves one route:

```text
POST /v1/triggers/{id}
Authorization: Bearer <token>
Content-Type: application/json
Idempotency-Key: <key>          (optional)

<the run's input JSON object>
```

The steps are, in order:

1. Read `Authorization`. A missing header, or one that is not
   `Bearer <token>`, is `401` before any lookup.
2. Resolve `{id}` against the shared store. An unknown or deleted id is
   `404`. The lookup is by id alone — the URL carries no namespace. How the
   store finds one id beneath the scoped prefix is `trigger_store.go`'s
   concern; what the contract requires is that the answer comes from the
   shared backend on every call, so a create or delete on another replica is
   visible on the next invoke, and that nothing derives the namespace or
   Orkflow from the id.
3. Hash the token with the record's salt and compare in constant time. A
   mismatch is `401`.
4. Read the body as the run's input object. The body is the input, not an
   envelope around it: `{"count": 3}` arrives as `SubmitRunRequest.input`
   with `data.count == 3`. The read is bounded, as the webhook adapter bounds
   its own; a body that is not a JSON object is `400`. An empty body means no
   input, and the Orkflow's declared defaults apply as they do for any other
   submission.
5. Read `Idempotency-Key`, if present, into `SubmitRunRequest.idempotency_key`.
6. Call `(*api.Service).SubmitRun` with `namespace`, `orkflow_name`, and
   `orkflow_version` copied from the stored record and `allow_empty_pool`
   set to `false`.
7. Answer with the result.

**The load-bearing invariant:** the namespace, Orkflow name, and version a
trigger invokes come only from the stored Trigger record, never from the
request. There is no path, query, header, or body field that selects them.
A body key named `orkflow_name` is input data like any other key. A caller
who holds a token can start exactly the Orkflow the trigger was created for
and nothing else; a trigger cannot be redirected.

Success is `201 Created` with the envelope `SubmitRun` returns, encoded by
`api.MarshalJSON` in the short-enum dialect of `docs/JSON.md`, so it reads
exactly as `orkctl run submit --json` does:

```json
{"run":{"id":"ork-…","orkflowName":"hello","orkflowVersion":"3","status":"PENDING","namespace":"team-a"}}
```

`id`, `status`, `namespace`, `orkflowName`, and `orkflowVersion` are always
present, with whatever else the pending `Run` carries. `orkflowVersion` is
the version that was resolved, never empty, so a caller of an unpinned
trigger learns which version actually started. `status` is `PENDING`, not
`STATUS_PENDING`.

Every failure is the error envelope `webhookErrorEnvelope` already uses:

```json
{"error":{"code":"unauthenticated","message":"…"}}
```

| Condition | HTTP status | `error.code` |
| --- | --- | --- |
| `Authorization` missing or not `Bearer <token>` | 401 | `unauthenticated` |
| Token does not match the record | 401 | `unauthenticated` |
| Unknown or deleted id | 404 | `not_found` |
| Trigger store unreadable | 500 | `storage_error` |
| Body is not a JSON object, or exceeds the bound, or the stored definition names a secret source its namespace does not allow | 400 | `invalid_argument` |
| Orkflow or pinned version no longer exists | 404 | `not_found` |
| A resolved pool has no live Orker, or no run starter is configured | 412 | `failed_precondition` |
| The namespace is on hold, or at its `max_running` (namespace admission) | 412 | `failed_precondition` |
| The stored definition's image is refused by the image policy now in force | 412 | `failed_precondition` |
| Object storage or Temporal unavailable at submit | 503 | `unavailable` |
| Anything else | 500 | `internal` |

The rows from `400` down are `SubmitRun`'s own Connect codes carried through
the Connect protocol's status mapping, with `error.code` the code's own name;
the adapter adds no failure classification of its own. A `401` or `404`
never reaches `SubmitRun`. The `400`, `404`, and `412` rows are refused by
`SubmitRun`'s preparation before a run id is minted, so a refused invoke
leaves nothing behind: no id burnt, no claim taken, no run recorded. A
pinned version that has been deleted fails with its coordinates; it never
falls through to current. The adapter never turns a `SubmitRun` error into a
`201`.

`Idempotency-Key` carries the existing `SubmitRun` contract unchanged: the
claim is scoped to the namespace (`{namespace}/{key}`), the cluster decides
which run the key names, and a repeated key on any replica returns that run
— same `id`, `201` — rather than starting a second one. The claim is per
namespace, not per trigger: two triggers of one namespace presenting one key
name one run. A caller that needs one run per external event supplies that
event's own identifier.

Invocation is synchronous. One HTTP request is one `SubmitRun` call. The
adapter does not queue, retry, or acknowledge-then-start: a `201` means the
run exists and is durably accepted (`AGENTS.md` §15: an accepted execution
cannot disappear), and any other status means this request started no run.
Nothing is left to deliver later.

## CLI

`cmd/orkctl/trigger.go` mirrors `cmd/orkctl/webhook.go`: one parent command,
`--json` on every subcommand, table and JSON rendering through
`g.unaryContext`, registered in `cmd/orkctl/root.go` beside
`newWebhookCommand(g)`.

```text
orkctl trigger create --orkflow NAME [--version V] [--namespace NS] [--description TEXT]
orkctl trigger list   [--orkflow NAME] [--namespace NS]
orkctl trigger delete ID [--namespace NS]
```

`--namespace` is `orkctl`'s existing global selector (`-n`); empty is
`default` on the wire. `--version` omitted means the current pointer at
invoke time and is rendered as `current`.

`create` prints the one-time secret and a ready-to-run `curl` command, the
way `orkctl webhook register` prints the registered webhook and its secret.
The command's host is the `--server` address the command itself used:

```text
curl -X POST http://localhost:8080/v1/triggers/trg-… \
  -H 'Authorization: Bearer <secret>' \
  -H 'Content-Type: application/json' \
  -d '{}'
```

`--json` output is the proto response encoded by the shared encoder
(`cmd/orkctl/output.go`): `{"trigger":{…},"secret":"…"}` for create and
`{"triggers":[…]}` for list, with no second envelope. `list` never contains a
secret. `delete` prints `Deleted trigger ID` and exits nonzero with the
server's error on NotFound. A value in the printed command is
shell-escaped; a quote in a description cannot become shell syntax.

## UI

The Orkflow detail page gains a fourth tab: `Overview | Versions | Triggers |
Edit`. `ui/src/lib/client.ts`'s `orkstack()` factory gains
`triggers: createClient(TriggerService, transport)`, beside `webhooks`. The
tab shows, in this order:

- **A scope statement**, first, in the same place and voice as the Admin
  Webhooks page's `webhooks-scope` line. The Admin Webhooks page states its
  scope is deployment-wide because a reader would otherwise assume the
  namespace picker filters it; the Triggers tab states its scope for the
  opposite reason — a reader must not assume a trigger can reach any other
  Orkflow or namespace. Each trigger is labeled
  `Runs {name}@{version or current} in {namespace}`.
- **A create form**: a version `Select` offering `Current` and each version
  `OrkflowVersions` already fetches (the same data `RunSubmitPanel` uses),
  and an optional description. Submit calls `CreateTrigger` and re-reads the
  list; a server error renders in an `ErrorPanel`, never as a silently empty
  result.
- **The one-time-secret panel**, shown immediately after a successful create
  and mirroring the Admin Webhooks page's `shownSecret` block, except that
  what it presents is a runnable command, not a bare string: a full
  `curl -X POST … -H "Authorization: Bearer <token>" -d '{…}'` embedding the
  real token, in a `CliEquivalent` popover with a Copy action. The panel says
  the secret will not be shown again. It is never rebuilt from a listing.
- **The list**: id, `{version}` or `current`, description, created. Each row
  has a `CliEquivalent` popover whose `curl` uses the placeholder
  `$ORKSTACK_TRIGGER_TOKEN` in place of the token — the secret is gone after
  creation and this cannot show it again, exactly as `Public()` strips a
  webhook's — and a Delete action behind `ConfirmDialog`. The empty state
  says no triggers exist for this Orkflow, distinct from a load error.

The tab is namespace-scoped by construction: it lives on the Orkflow page,
which is already addressed by the namespace the picker selects. It never
merges triggers across namespaces.

## What this does not do

- **No rate limiting.** A trigger route on the API port is reachable by
  whoever can reach the port. Abuse protection is the operator's reverse
  proxy or gateway, the same posture `docs/DEPLOY.md` §7 takes for
  `ORK_ADMIN_ADDR`: what to expose, to whom, and how fast is a deployment
  decision, not an OrkStack subsystem. This contract adds no rate limiter and
  no rate-limit configuration (`AGENTS.md` §13). What does bound a
  legitimate, authenticated upstream that has started looping is the
  namespace's own admission control: `orkctl namespace update NS --hold
  REASON` refuses every new run of the namespace (a trigger invoke is `412`
  carrying the reason), and `--max-running N` caps its runs in flight. Both
  are the same `SubmitRun` gate every other door goes through; neither is a
  rate limiter.
- **No `allow_empty_pool` override.** Every trigger-started run submits with
  `allow_empty_pool = false`. A pool with no live Orker fails the invoke with
  `412` rather than starting a run that would never leave PENDING. The field
  is not exposed to the caller and not stored on the trigger.
- **No delivery retries and no delivery state.** Invocation is synchronous:
  one HTTP call maps to exactly one `SubmitRun` call, and success or failure
  is the HTTP response itself. There is no queue, no later redelivery, and no
  delivery-state database.
- **No new `internal/config` setting.** No TTL, no expiry, no rate limit, no
  toggle. A deployment that never creates a trigger runs exactly as before.
- **No secret rotation or re-disclosure.** The plaintext exists only in the
  create response. Rotating a token is delete then create, yielding a new id.
- **No provenance on the run.** `Run` gains no field naming the trigger that
  started it; a trigger-started run is indistinguishable from `run submit`.
- **No change to webhooks.** `WebhookService`, its storage, its delivery, and
  its UI are untouched; a trigger-started run notifies webhook subscribers
  exactly as any other run does.

## Capability and acceptance matrix

| Capability | UI behavior | CLI/shared behavior | Required observable acceptance |
| --- | --- | --- | --- |
| Trigger creation | Triggers tab create form: version `Select` (current or listed), optional description; one-time runnable `curl` with the real token shown once | `trigger create --orkflow NAME [--version V] [--description TEXT]`; prints secret and `curl` once; `--json` returns `trigger` and `secret` | Record exists at `metadata/triggers/{id}.json` on shared storage, namespace/Orkflow as fields on the record, with salt and digest, never plaintext; a nonexistent Orkflow or version is NotFound and writes nothing; a second replica lists the trigger |
| Listing | Rows with id, pin or `current`, description, created; `curl` template with `$ORKSTACK_TRIGGER_TOKEN` | `trigger list [--orkflow NAME]`; `--json` returns `triggers` | No secret and no digest in any listing; `--orkflow` narrows to one Orkflow; empty means every trigger in the namespace |
| Deletion | Delete behind `ConfirmDialog`; row disappears after re-read | `trigger delete ID` | Object gone from shared storage; the next invoke of that id is `404` on every replica; storage failure is an error, not a silent success |
| Invocation, valid token | — | `POST /v1/triggers/{id}` with the bearer token | `201` with a `run` envelope; `GetRun` on the returned `id` in the trigger's namespace shows the run; its `orkflowName` and `namespace` equal the record's and `orkflowVersion` is the pin or the current pointer at invoke time |
| Invocation, invalid or missing token | — | Same route, wrong or absent `Authorization` | `401` with `unauthenticated`; no run started, proven by observation (no new run in `ListRuns`), not by absence of error |
| Invocation, unknown id | — | Same route, an id that never existed | `404` with `not_found`; no run started |
| Invocation, deleted trigger | — | Delete, then invoke again with the original token | `404`; no run started; the original token authenticates nothing |
| Version resolution | Pin shown as chosen; `current` shown for an unpinned trigger | `--version` pins; omitted resolves current at invoke | A pinned trigger keeps starting its version after current moves; an unpinned trigger follows current; a deleted pinned version fails with its coordinates and never falls to current |
| Cross-namespace isolation | Tab shows only this namespace's Orkflow's triggers | Every management request carries its namespace | `ListTriggers` for another namespace never returns this one's records and `DeleteTrigger` from another namespace is NotFound and deletes nothing; an invoke starts a run only in the record's namespace, and `GetRun` from another namespace is NotFound |

## Evidence and completion

Behavior tests precede implementation. What must be proven for triggers
specifically:

- **Unit** (`pkg/artifacts`, `internal/api`, `cmd/orkctl`): id and secret
  generation shape (`trg-{16 hex}`, 32 random bytes); the stored record
  round-trips through both the Local store and the S3-shaped test double and
  contains a salt and digest but never the plaintext; bearer verification
  accepts the right token and rejects a wrong one and a missing header, with
  the comparison being `subtle.ConstantTimeCompare` or `hmac.Equal` (confirmed
  in review; a timing measurement is not evidence); `CreateTrigger` refuses an
  unknown namespace, Orkflow, or version and writes nothing; the adapter's
  status and `error.code` table above holds row by row; CLI golden output for
  `create`, `list`, and `delete`, human and `--json`.
- **Integration** (`-tags=integration`, real Temporal and MinIO via
  `make infra-up`, `ORK_REQUIRE_INFRA=1`): create, then invoke over real
  HTTP, then observe the run through `GetRun` in the trigger's namespace;
  delete, then invoke again and receive `404`; invoke with a wrong token,
  receive `401`, and confirm through `ListRuns` that no run was started; a
  trigger created on one replica is listed and invocable on a second replica
  sharing the same bucket, mirroring the existing webhook replica-visibility
  test; a pinned trigger keeps its version after the current pointer moves.
- **Playwright** (`ui/e2e`, against the real engine, Orker, Temporal, and
  MinIO): open an Orkflow's Triggers tab, read the scope statement, create a
  trigger, see the one-time secret and runnable `curl`, confirm the list row
  shows the placeholder rather than the token, delete it, and confirm it
  disappears. Screenshots and transcripts land under the fresh
  `proof/<name>/` directory.

Run `make lint test`, the new integration tests, `make proof`, and
`make proof-ui`; report each gate's outcome and the fresh proof directory
name. A skip or absent prerequisite is not a pass. Commit as scoped
Conventional Commits — `chore(proto): …` for the service,
`feat(api,cli): …` for storage, RPCs, the adapter, and the CLI,
`feat(ui): …` for the tab, `docs: …` for this document — quoting the
evidence; never commit `proof/`.
