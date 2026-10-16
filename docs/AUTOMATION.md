# Declarative automation contract

An **automation document** is one reviewable YAML file per namespace that
declares, by name, everything that surrounds that namespace's Orkflows — what
an Orkflow itself does not declare
([DSL.md](DSL.md#what-an-orkflow-does-not-declare)): its settings, its
schedules, its triggers and its webhooks. It lives in Git beside
the Orkflow documents; `orkctl apply` applies both.

```yaml
# environments/prod/automation.ork.yaml
apiVersion: orkstack/v1
kind: Automation
namespace: payments

settings:
  default_pool: main
  allowed_pools: [main, gpu]
  run_retention: 168h

schedules:
  nightly-reconcile:
    orkflow: reconcile
    cron: "0 2 * * *"
    timezone: Europe/Lisbon
    overlap: skip
    input:
      mode: full

triggers:
  github-deploy:
    orkflow: deploy
    description: GitHub Actions deploy hook

webhooks:
  etl-loader:
    url: https://hooks.acme.dev/etl
    orkflow: nightly-etl
    events: [TASK_SUCCEEDED, RUN_FAILED]
    secret_env: ETL_WEBHOOK_SECRET
```

```bash
orkctl apply environments/prod/ --dry-run     # plans every document, writes nothing
orkctl apply environments/prod/               # applies: Orkflows first, then each namespace's plan
orkctl apply automation.ork.yaml --prune      # also deletes named objects the document no longer declares
orkctl export -n payments > automation.ork.yaml   # a hand-managed namespace, as a document
```

**The engine plans; the client executes.** `AutomationService.PlanAutomation`
parses the document, reads what the namespace holds, and answers with an
ordered list of steps. Each step that changes something carries the exact
request of **one existing write RPC** — `CreateTrigger`, `UpdateSchedule`,
`DeleteWebhook`, `UpdateNamespace`, … — and the client sends those requests in
order. There is no apply RPC, no stored desired state, no loop and nothing
that watches: an apply is a one-shot diff-and-write started by a person or a
CI job, every write is the same validated, audited call a person could make by
hand, and re-running it against an unchanged namespace plans nothing.

"Plan" here is unrelated to `orkctl run plan`, which simulates one run's DAG
(`PlanRun`); the automation plan is `PlanAutomation`, shown by `apply
--dry-run`.

What this is not (`AGENTS.md` §13): not a reconciliation loop, a controller or
a drift watcher — drift is found by `orkctl apply --check`, run by a person or
a CI job; not a desired-state store — no document, plan, digest or
"last-applied" record is stored anywhere, and no object carries a
"managed-by" marker; not a templating language — no variables, includes or
interpolation, and per-environment differences are separate documents.

## The document

| Key | Required | Meaning |
| --- | --- | --- |
| `apiVersion` | yes | Exactly `orkstack/v1`. |
| `kind` | yes | Exactly `Automation`; an Orkflow says `kind: Orkflow`, and `orkctl apply` dispatches on it. |
| `namespace` | yes | The one namespace the document describes, a slug. Required and authoritative: never inferred from `-n`, `ORK_NAMESPACE` or a saved default, so a document that prunes can never land in the wrong namespace by accident of a shell. |
| `settings` | no | The namespace's settings, as an overlay. |
| `schedules`, `triggers`, `webhooks` | no | `name: entry` mappings. |

Its JSON Schema is [`docs/schemas/automation.v1.json`](schemas/automation.v1.json),
generated from this parser (`docs/DSL.md`, "Editor support").

A document **manages** a section when it declares the key, even as `{}`; a
section it omits is not managed and a plan says nothing about it. A section
written as null (`triggers:` and nothing) is an error, because "manage none"
and "leave alone" are opposites: write `triggers: {}` or remove the key.

Parsing is strict: an unknown key at any level is an error naming the keys
allowed there; a second YAML document (`---`), a duplicate key, and anchors or
aliases are errors. Entry names are slugs (`^[a-z][a-z0-9-]*$`, at most 200
bytes). Every problem is a `ValidationIssue` with a dot-separated field path —
`settings.run_retention`, `schedules.nightly-reconcile.cron[1]`,
`webhooks.etl-loader.events[0]` — and a message beginning `line N:` when the
line is known. A document with any parse error is planned no further: a plan
built from a partial document would show deletions for what it could not read.

### `settings` — an overlay, key by key

`UpdateNamespace` replaces the whole namespace document. A document must not,
by omitting a field, clear it — above all not a hold. So each key present sets
that one field; each key absent leaves the stored value exactly as it is.

| Key | Value | Clearing value | Compared as |
| --- | --- | --- | --- |
| `description` | string | `""` | exact |
| `labels` | mapping of strings | `{}` | map |
| `default_pool` | string | `""` (= `main`) | through the pool-resolution rule (`main` equals unset) |
| `allowed_pools` | list of strings | `[]` | set |
| `run_retention` | Go duration, or `inherit` | `inherit` | duration / unset |
| `telemetry` | mapping: `sink`, `task_logs` (`inherit`\|`enabled`\|`disabled`), `attributes` | `{}` | per sub-field; written as a unit, an all-zero policy is unset |
| `max_running` | integer ≥ 0 | `0` | exact |
| `allowed_secrets` | list of strings | `[]` | set |
| `require_signed_by` | string | `""` | exact |
| `budget` | mapping: `cpu_seconds`, `gib_seconds` (integers ≥ 0), `period` (Go duration), `enforce` (boolean) — the `--budget-*` flags | `{}` | per sub-field; written as a unit, an all-zero budget is unset (`--clear-budget`) |

The comparisons are exactly `CreateNamespace`'s idempotency comparison. A
document never manages `name` (it is `namespace:`), the registry-owned
`created_at`, `updated_at` and `etag`, or `hold` — operational state:
`settings.hold` is refused, and a live hold is shown as a note and never
changed. A namespace field a later release adds must be classified as managed
or unmanaged before it ships; a test walks the descriptor and fails otherwise.

### `schedules`

| Key | Value | Absent means |
| --- | --- | --- |
| `orkflow` | slug, **required**, fixed at creation | — |
| `version` | string | the current pointer at each fire |
| `description` | string, ≤ 256 bytes, no control characters | `""` |
| `cron` | string, or list of strings | none |
| `interval` | Go duration | unset |
| `timezone` | IANA name | UTC |
| `jitter` | Go duration | none |
| `start_at`, `end_at` | RFC 3339 | unbounded |
| `overlap` | `skip`\|`buffer-one`\|`buffer-all`\|`cancel-other`\|`allow-all` | `skip` |
| `catchup_window` | Go duration, ≥ 10s | the cluster's default |
| `pause_on_failure` | boolean | `false` |
| `input` | a YAML mapping, converted to JSON by the strict mapping (`docs/JSON.md`) | none |

There is no `paused` key — pause is operational, shown as a note, never
changed — and no magic `version` word: `version: current` pins a version
named `current`. Run input is JSON on the wire; in this document it is written
as a YAML mapping and converted by the strict mapping (`AGENTS.md` §3.1,
`docs/JSON.md`), so `apply --dry-run` shows exactly the JSON each fire submits.
A string is refused (a JSON object written as a string was the shape before
this release), and `{}` is recorded as unset. Cron syntax is the
cluster's alone: an expression it cannot parse fails that step of the apply.

### `triggers`

| Key | Value | Absent means |
| --- | --- | --- |
| `orkflow` | slug, **required**, fixed at creation | — |
| `version` | string | the current pointer at each invoke |
| `description` | string | `""` |

The entry's name is `Trigger.name`. The token is never in the document.

### `webhooks`

| Key | Value | Absent means |
| --- | --- | --- |
| `url` | absolute `http(s)` URL, **required** | — |
| `events` | list of event names | every run transition |
| `orkflow` | slug | every Orkflow |
| `task` | slug; needs a `TASK_*` event | every task |
| `secret_env` | environment-variable name | a generated signing secret, disclosed once |

A webhook in a namespace's document is scoped to that namespace, always;
`namespace:` in an entry is refused. Events are normalised to lifecycle order,
so order and duplicates make no difference. `secret_env` names a variable the
**client** reads when it executes the create step: the pre-shared secret never
enters the document, the plan or the engine except as the one
`RegisterWebhook` that stores it. It has no effect on an existing
subscription — an update never changes a secret — and the plan says so.

## Names and in-place updates

A document addresses objects by name. Schedules always have one; triggers and
webhooks carry an optional `name` (`Trigger.name`, `Webhook.name`). A trigger
or webhook without one — including every one created before names existed —
is **unnamed**, and an unnamed object is outside every document: no apply, not even `--prune`, ever
touches it. A webhook name requires a namespace scope; a deployment-wide
subscription belongs to no namespace document at all.

`UpdateTrigger` and `UpdateWebhook` change a trigger's name, pin and
description, and a subscription's name, URL, events and scope, **in place**:
the trigger's id, URL and bearer token, and the subscription's id and signing
secret, never change. That is what lets a hand-made object be brought under a
document without re-issuing its credential:

```bash
orkctl export -n payments                      # unnamed objects are listed in a trailing comment
orkctl trigger update trg-4a1c… -n payments --name github-deploy
orkctl webhook update wh-9f2e… --name etl-loader
orkctl export -n payments > automation.ork.yaml    # now they are entries
orkctl apply automation.ork.yaml --check          # exits 0
```

Name uniqueness is checked by the store immediately before the write. It is
best-effort across replicas — a lock across replicas would be the distributed
lock `AGENTS.md` §2.1 forbids — and the one state a race can leave, two
objects sharing a name, is refused loudly by every plan and every export,
naming both ids and the command that removes one.

## The plan

A plan is a list of steps, one per object the document declares or the
namespace holds:

| Action | Symbol | Meaning |
| --- | --- | --- |
| CREATE | `+` | declared, absent |
| UPDATE | `~` | declared, present, differs — in place, never delete-and-recreate |
| DELETE | `-` | named, in a managed section, not declared, and `--prune` |
| UNCHANGED | ` ` | declared, present, identical |
| UNDECLARED | `?` | named, in a managed section, not declared, no `--prune`: kept |
| UNMANAGED | `!` | unnamed: outside every document, never deleted |

CREATE, UPDATE and DELETE are **actionable** and carry the one request that
performs them. A namespace that does not exist is created first; `default` is
never created (it always resolves). A namespace UPDATE carries the live etag,
so a namespace changed between plan and apply fails its compare-and-swap
instead of being overwritten.

A document that binds an existing name to a different Orkflow is an error
naming the two ways out — a new entry name, or deleting the old object first.
Apply never deletes and recreates to make a field true, because recreating a
trigger re-issues its credential. A CREATE that matches an UNDECLARED object
field for field is a **warning**: renaming an entry declares a different
object, and for a trigger or webhook the warning names the `update … --name`
that keeps the credential. A schedule has no in-place rename, so its warning
offers no command; it says what the new key costs instead — the old schedule
keeps firing until it is deleted (or `--prune` deletes it, its run history
staying under the old name), and a paused one's hold is not carried over.

Every schedule and trigger entry's `(orkflow, version)` must be stored — or be
an Orkflow version the same invocation applies first. The plan also runs the
target RPCs' own validators over the carried requests — the retention
ceiling, a declared telemetry sink, the namespace's pool and policy shape, a
schedule spec's rules — so an apply fails at plan time, before any write,
wherever the engine can know it.

### Step order

Steps sort by group — the namespace's own CREATE or UPDATE first, then
creates, updates, deletes, then UNCHANGED, UNDECLARED, UNMANAGED — then by
kind (namespace, schedule, trigger, webhook), name and id. Actionable steps
are therefore a prefix of the list and run in list order: everything else
lives in the namespace, and additive steps run before destructive ones, so an
apply that stops part-way never leaves a namespace with fewer front doors
than it had.

### Renderings

A step's summary, its changes and an export are rendered by one set of
functions in the engine and shown verbatim by every client: strings as `%q`;
an unpinned version as `current` (unquoted); lists as `[a, b]`; maps as
`{k: "v"}` sorted by key; durations in their short form (`168h`, `1h30m`);
`run_retention` unset as `inherit`; timestamps RFC 3339 UTC, `unset` when
absent; input as compact JSON with sorted keys in a plan (an export writes
input as a YAML mapping, keys sorted, quoting any string a YAML reader could
read as something else); events in lifecycle order,
`[]` meaning every run transition.

### The digest

`plan_digest` is `sha256:` over the namespace and, for each actionable step in
order, its action, kind, name, id and changes. It covers what a reviewer read
and nothing they did not: not the carried requests (a new etag does not move
it), not UNCHANGED, UNDECLARED or UNMANAGED steps, not notes or issues. It is
defined even when nothing is actionable — "no changes" is pinnable — and
empty exactly when nothing was planned; an empty digest never matches.

## `orkctl apply`

```text
orkctl apply <file|dir>... [--dry-run | --check] [--prune]
             [--expect-plan sha256:HEX] [--secrets-out PATH] [-n NS] [--json]
```

One Orkflow file with none of the automation flags behaves as a plain Orkflow
apply (`--json` prints `ApplyOrkflowResponse`). Otherwise each argument is a file, or a directory read non-recursively for
`*.ork.yaml` and `*.ork.yml` in lexical order. A file's `kind:` decides:
`kind: Orkflow` is an Orkflow, `kind: Automation` an automation document, and
a file with no `kind`, or any other, is refused before anything is sent. Two automation documents naming one namespace are
refused. Only an `-n` typed on the command line is checked against a
document's namespace; `ORK_NAMESPACE` and the saved default never apply to
it.

Nothing is written until every document has been read, validated and planned:

1. Each Orkflow document is validated, and its compiled name and version
   looked up: **new**, **identical** (stored with
   the same source, not counting the two header lines written first — so a
   version stored before the header existed is identical to its file with them
   added; `docs/DSL.md`, "The document header") or
   **conflicting** (stored with different source — an error).
2. Each automation document is planned, told which Orkflow versions this
   invocation stores first.
3. The invocation is **refused** — everything printed, exit 1, nothing
   written — on any Orkflow error or conflict, any plan error, an
   `--expect-plan` with other than one automation document or a different
   digest (the fresh plan is printed), a `secret_env` variable that is unset
   (a printed note on `--dry-run`), or a `--secrets-out` file that exists
   or cannot be created. The `--secrets-out` file is created (exclusively,
   mode 0600) here, before the first write, so a path apply cannot write
   refuses the apply instead of losing a secret after it.
4. `--dry-run` prints and exits 0; `--check` is a dry run that exits **4**
   when anything would change.
5. New Orkflow versions are applied, then each document's actionable steps
   run in order. A DELETE whose object is already gone is `already gone`; any
   other failure stops everything after it. A partial apply is reported, not
   rolled back: running apply again plans from what now exists and finishes.

A trigger token, or a webhook secret the engine generated, is printed once
with the trigger's runnable `curl` — or, with `--secrets-out PATH`, written to
a new mode-0600 file as a `DisclosedSecrets` document and named in place of
each value. Should that final write still fail, the file is removed and the
secrets are printed (and carried in the `--json` report) instead, and apply
exits 1. A re-apply prints no secret: nothing is created.

`--json` prints one `ApplyReport` (`proto/orkstack/v1/automation.proto`): the
Orkflow reports, each automation document's plan, and the results of an
apply.

An Orkflow is validated inside an existing namespace, so a namespace that
only this invocation would create must be created first: apply the
automation document that creates it, then the Orkflows. `apply` says so
rather than failing obscurely.

## `orkctl export`

`orkctl export -n NS` prints the document a namespace's current automation
would be applied from — every setting at its live value, every section (even
empty), entries sorted by name — so that `export` then `apply --check` exits
0. It is deterministic. It refuses rather than omits: a schedule the cluster
could not describe, or two objects sharing a name, fails the export, because
an export missing an object would delete it when applied with `--prune`. A
trailing comment lists each unnamed object with the command that names it,
repeated on stderr.

## CI recipes

```bash
# Pull request: post the plan as a comment.
orkctl apply environments/prod/ --dry-run

# Merge: apply exactly the plan that was reviewed.
orkctl apply environments/prod/automation.ork.yaml --prune \
  --expect-plan sha256:3f9a… --secrets-out "$RUNNER_TEMP/secrets.json"

# Nightly: find drift; a non-zero exit is a person's decision to act on.
orkctl apply environments/prod/ --check
```

`--expect-plan` is what keeps a `--prune` from deleting something created
between review and merge that nobody reviewed.

## Admin → Automation

The page respects the namespace switcher. It exports the namespace's document
(Download, "Edit as a plan", and a callout naming each unnamed object), plans
a pasted or imported document — issues, one row per step with its symbol,
changes and notes, a line per unmanaged section, the counts and the digest —
and applies it behind a confirmation that names every deletion. On accept it
plans again and refuses a plan that moved since it was shown, exactly
`--expect-plan`; then it runs each step through the same RPC the CLI would,
marking each row applied, already gone, failed or not run, and shows any
one-time secret once. A `secret_env` entry asks for its secret in a password
field, and Apply stays disabled until it is filled. The Triggers tab and the
Webhooks page name and edit objects in place.

## Audit

Each step an apply executes is audited as what it is — one record under that
write's own action (`namespace.update`, `schedule.create`, `trigger.update`,
`webhook.register`, …). `PlanAutomation` and `ExportAutomation` are reads and
are never recorded. `trigger.update` and `webhook.update` record the new
name, pin, URL, events and scope, never a secret.

## Guarantees

- `--dry-run`, `--check`, `PlanAutomation` and `ExportAutomation` write
  nothing: `metadata/` and the cluster's schedules are byte-identical before
  and after.
- Re-applying an unchanged document against an unchanged namespace makes zero
  write calls.
- Only `--prune` deletes, only named objects of sections the document
  declares, and never an unnamed object, a deployment-wide subscription, an
  Orkflow or an Orkflow version.
- A trigger token and a webhook secret are disclosed once, on the apply that
  creates them, and survive every later in-place update; a later delivery
  still verifies with the original secret.
- A plan that moved between review and apply (`--expect-plan`, or the page's
  re-plan) writes nothing and prints the new plan.
- A hold, a paused schedule and an Orker cordon are operational state: an
  apply never sets, releases or changes them.
- A plan computed on one replica can be applied through another; two replicas
  export the same document.

## Why it is shaped this way

| Alternative | Why not |
| --- | --- |
| A server-side `ApplyAutomation` RPC that performs the writes | A second write path for every object, bypassing each RPC's own validation and audit row, one request doing unbounded work — the shape of a controller endpoint. |
| A reconciliation loop or drift watcher | `AGENTS.md` §13 (no Kubernetes-operator shape). Drift is `--check`, acted on by a person. |
| Ids in the document (`id: trg-…`) | Not stable across environments, and a created object's id would have to be written back into Git before the next apply could find it. |
| A name-to-id map stored beside the objects, or a "managed-by" marker | Two writes and a crash window per create, an orphan state, and a last-applied record — a second state machine (`AGENTS.md` §2.2). The name is a field on the object. |
| Deriving an object's id from its name | An existing object could never be adopted without re-issuing its URL, and a deleted trigger's URL would come back to life under a new token. |
| Delete-and-recreate to make a field true | Re-issues the trigger token or webhook secret the integration depends on. |
| Planning in each client | The plan is served by one planner so `apply --dry-run` and Admin → Automation can never disagree about what an apply would do; only the writes are client-side. |
| A JSON object written as a string for schedule `input` (the shape before this release) | The document is YAML; a JSON string inside it is a second syntax with its own quoting, and a reader cannot see the input's shape. The mapping is converted by the strict mapping — never a lenient decode — so `yes`, `0755` or a date is refused rather than coerced, and run input stays JSON on the wire (`AGENTS.md` §3.1). |
