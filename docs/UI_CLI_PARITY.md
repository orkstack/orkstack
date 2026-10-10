# UI and CLI parity

`orkctl` and the browser UI are two clients of one Connect API. **Parity**
means both can accomplish the same deployment operation with the same
selectors, defaults, results and errors; their presentation may differ. This
document is the contract for what each operator surface reads and writes, and
the rules both clients follow. Proto (`proto/orkstack/v1/`) is the source of
truth for every type named here; `AGENTS.md` remains authoritative.

Semantics owned elsewhere are linked, not repeated: the DSL
([DSL.md](DSL.md)), operating a deployment ([DEPLOY.md](DEPLOY.md)), stored
keys ([STORAGE.md](STORAGE.md)), dashboard numbers ([STATS.md](STATS.md)),
triggers ([ORKFLOW_TRIGGERS.md](ORKFLOW_TRIGGERS.md)), schedules
([ORKFLOW_SCHEDULES.md](ORKFLOW_SCHEDULES.md)), automation documents
([AUTOMATION.md](AUTOMATION.md)) and evidence bundles
([RUN_EVIDENCE.md](RUN_EVIDENCE.md)).

## Rules both clients follow

- **Namespace.** Every run and Orkflow operation carries its namespace; empty
  means `default`, and a resource of another namespace is NotFound — holding
  an id is not authority over another tenant. The audit trail and the webhook
  delivery records are **deployment-wide listings**: a `namespace` filter
  narrows them like any other property ([Filtering lists](#filtering-lists)),
  and an explicit `-n` on `admin audit` or `webhook deliveries` is refused
  before any request, so a flag cannot silently narrow or widen them;
  `ORK_NAMESPACE` and `namespace use` are ignored there. A webhook's namespace
  scope is optional, and only an explicit `-n` sets it. Webhook subscriptions,
  Orkers, releases, the image policy and the retention maximum are listed
  deployment-wide and ignore the namespace.
- **Any replica answers.** Every read comes from Temporal or the shared store,
  never from process memory, so a replica that started nothing answers
  identically for runs, attempts, deliveries and audit records others made.
- **Rendered once, server-side.** Where the engine renders a value (a diff
  cell, a plan step, a reason sentence), both clients print it verbatim. An
  unknown reason token from a newer build prints as itself; its row is never
  dropped.
- **Unavailable is a word, never a zero.** A quantity that does not apply
  prints `-`; one that happened but cannot be known prints `unavailable` with
  its reason. The two never look alike, and neither is ever `0`, `0s` or
  blank.
- **No partial answers.** A read whose scan would exceed its budget is refused
  (`ResourceExhausted`, a non-zero CLI exit, an error panel in the UI) and is
  never answered from the subset it read. A store or cluster that cannot
  answer is `Unavailable`, never an empty list.
- **`--json` is the raw response envelope** in the short-enum dialect of
  [JSON.md](JSON.md), unless a command says otherwise. Two responses cannot
  share one document, so a flag that would append a second read to `--json`
  output is refused and names the command that prints it.
- **Every action has its CLI equivalent.** The UI shows a shell-escaped,
  runnable `orkctl` (or `curl`) command beside each action, carrying the
  namespace, version, task, attempt and item where they apply; a quote in a
  value cannot become shell syntax.
- **Deep links and copies keep their coordinates.** A URL or download retains
  the namespace, pinned version, task, attempt, item and inspector tab, and a
  reload restores the selection. Run inputs may contain secrets and never
  enter a shareable URL: a run is shared by its id alone.
- **One-time secrets are shown once.** A trigger token, a generated webhook
  secret and an await callback token appear in the creating response only;
  no listing, URL or later read carries them.
- **`orkctl` exit codes.** `0` the command succeeded; `1` it failed —
  including a mistyped subcommand (`orkctl run lsit` fails with
  `unknown command "lsit" for "orkctl run"` and the nearest suggestion; a
  bare group such as `orkctl run` prints its help and exits 0); `2` a watched
  run failed; `3` a watched run was canceled; `4` the command's own negative
  verdict (`orker drain` timed out, `run plan` contradicts an
  `--assert-*`, `apply --check` found changes).
- **The browser invents no validation the CLI bypasses**, and typed input
  controls are a presentation of declared inputs, not a schema language (see
  "Baseline operator flows").

## Filtering lists

Every list whose collection grows with use takes one structured filter,
`repeated FieldFilter filters` (`proto/orkstack/v1/filter.proto`). A filter
is a field, one operator from a closed set and its values; **every filter of
a request must hold**. There is no OR, no grouping, no negation of a group and
no expression, by design (`AGENTS.md` §13). Which fields of which object may
be filtered, with which operators, is declared **once**, as the
`(orkstack.v1.filterable)` option on the list request; the engine validates
and matches with it, `orkctl --filter help` prints it, and the browser's
filter form renders it. The matcher is written once, in
`internal/listfilter`.

Lists that take filters: `ListNamespaces`, `ListOrkflows`, `ListWebhooks`,
`ListTriggers`, `ListSchedules`, `ListPools`, `ListActivities`, `ListOrkers`,
`ListReleases`, `ListDeliveries`, `ListAuditRecords` and `ListRuns`. Child listings of one parent
(`ListOrkflowVersions`, `ListRunArtifacts`, `ListTaskAttempts`), the
`ListOrkflowCallers` report and `ListTelemetrySinks` take no filters.

### Scoping versus filtering

A request field stays a typed field only when it names the **tenant** a read
is confined to (`namespace` on a namespaced resource: empty means `default`,
another namespace's object is NotFound), the one **parent** of a bounded
child listing, or **paging** (`page_size`, `page_token`). Everything else that
narrows a listing is a filter — any property of the listed object, including
an id, a time, or the namespace *recorded on* a deployment-wide object (a
webhook's scope, an audit record's namespace, a delivery's run namespace).

So webhook deliveries and the audit trail are deployment-wide listings with
no `namespace` request field: `--filter namespace=payments` narrows them like
any property, `--filter 'namespace='` selects the audit trail's
deployment-wide writes (retention, image policy, webhooks, releases, Orker
cordons), and an explicit `-n` on either command is refused before any
request (`admin audit lists every namespace; narrow it with --filter
namespace=<ns>`). A delivery's `webhook_id` and `run_id` are filters too: a
delivery outlives a deleted subscription and is no child of a run.

### Operators and the CLI grammar

| Operator | CLI | Values | Holds when |
| --- | --- | --- | --- |
| equals | `f=v` | exactly 1 | the field equals `v` |
| not equals | `f!=v` | exactly 1 | the field differs from `v` |
| in | `f=v1,v2` | 1–64, none empty | the field equals any value (one value is equals) |
| contains | `f~v` | exactly 1, non-empty | the text contains `v` |
| prefix | `f^v` | exactly 1, non-empty | the text starts with `v` |
| at or after | `f>=t` | exactly 1 instant | the instant is `t` or later |
| before | `f<t` | exactly 1 instant | the instant is earlier than `t` |

To exclude several values, repeat `!=`: AND is the only combinator. The
display names above, with the CLI spelling in parentheses (`contains (~)`),
are the ones both clients use.

`orkctl` takes one repeatable `--filter FIELD OP VALUE` flag on every list
command (`--filter 'status=FAILED,CANCELED' --filter 'labels.team=payments'`).
The grammar is exact and nothing is trimmed:

```text
FILTER   = FIELD OPERATOR [VALUES]
FIELD    = 1*( ESCAPED / any character except \ = ! ~ ^ < > )
           -- "labels.<key>": everything after the first "labels." is the key
OPERATOR = ">=" / "!=" / "=" / "~" / "^" / "<"
VALUES   = ITEM *( "," ITEM )      -- more than one ITEM only after "="
ITEM     = 1*( ESCAPED / any character except \ and , )
ESCAPED  = "\" any-character      -- "\," is a comma, "\\" a backslash, "\=" an equals sign
```

A field ends at its first unescaped operator character; `!` must be followed
by `=` and `>` by `=`; a value beginning with an unescaped `=` is refused
(`no operator "<=", "==" or ">=="; escape a value's leading = as \=`), so
`<=` and `==` never parse as something else; after `=`, unescaped commas make
an `in` and an empty item is refused, while a lone empty value is allowed
(`namespace=` is equals ""); after any other operator an unescaped comma is
refused; a trailing lone `\` and an empty `--filter ''` are refused.
`orkctl` checks this syntax only, before any request; every other refusal is
the server's, printed verbatim with a non-zero exit — one validator, so the
browser and the CLI cannot disagree.

**Field paths are proto field names; values are `--json` spellings.** A path
is `orkflow_name`, `created_at`, `hold.reason` — the names in the `.proto` and
in `--filter help`, never the camelCase keys `--json` prints for the same
fields, and no alias is accepted. A value is exactly what `--json` prints
(`FAILED`, `2026-10-09T00:00:00Z`, `true`), so a value copied from `--json`
output filters for it.

`--filter help` prints the command's allowlist and exits 0 without a request;
the same table ends the command's `--help`:

```text
Filters (--filter FIELD OP VALUE, repeatable; every filter must hold):
  FIELD      OPERATORS       VALUES
  orker_id   = != =a,b ~ ^   an Orker id
  pool       = != =a,b ~ ^   a name (a-z, 0-9, -; at most 200 bytes) or empty; an unreachable row is kept
  build_id   = != =a,b ~ ^   text; an unreachable row is kept
  cordoned   =               true false; an unreachable row is kept
Operators: = equals, != not equals, =a,b in, ~ contains, ^ prefix, >= at or after, < before.
A label is labels.KEY. \ escapes the next character; a comma inside a value is \,.
```

A filtered listing that matches nothing prints `No <things> match --filter
'…'.` instead of an empty table; `--json` keeps each command's own envelope,
and filters only narrow the rows inside it.

### Matching rules

A filter reads the listed object exactly as the response carries it, in
[JSON.md](JSON.md)'s dialect:

| Field type | Operators | Value spelling | An unset field reads as |
| --- | --- | --- | --- |
| text | equals, not equals, in, contains, prefix; an opaque id or digest only the first three | verbatim UTF-8, case-sensitive, no trimming | `""` |
| enum | equals, not equals, in | the short name (`FAILED`); input is case-insensitive, the type prefix is optional and `-` reads as `_` | `UNSPECIFIED`, which no filter names: equals and in never match it, not equals always does |
| bool | equals | `true` or `false`, exactly | `false` |
| timestamp | at or after, before | RFC 3339 **with a zone** (`Z` or `±hh:mm`); compared as instants at microsecond precision | never matches |
| `labels.<key>` | as text | as text | an absent key reads `""` |

A path through a nested message reads each unset message as its zero value:
`hold.reason=` lists the namespaces with no hold and `hold.reason!=` the held
ones. Repeated and numeric fields are never filterable. **Opaque ids are
matched whole** — a webhook, trigger or schedule id, a delivery's `run_id`
and `webhook_id`, and the digests take equals, not equals and in only, and a
run's `origin.id` equals only — because nothing derives meaning from an
id's parts (`AGENTS.md` §7); operator-chosen names (an Orker id, a release,
every slug) take all five text operators.

**Rows the deployment could not ask.** A list whose rows can say "this could
not be measured" (an unreachable Orker, a schedule the cluster could not
describe) keeps such a row under every filter on a field it did not measure,
and lists it with its reason: a filter never hides a row the deployment failed
to ask. Identity fields (an Orker's id; a schedule's id, name and Orkflow) are
always measured and filter normally.

### Values, limits and refusals

A request is refused with `InvalidArgument`, before any read, with exactly
these messages (`i` is the filter's index in the request):

| Condition | Message |
| --- | --- |
| more than 16 filters | `at most 16 filters; got 17` |
| a field not in the allowlist (a camelCase path included) | `filters[i]: "statuss" is not a filterable field of Run; filterable: …` — the declared paths in order, a labels map written `labels.KEY` |
| `labels` with no key | `filters[i]: name a key: labels.<key>` |
| no operator | `filters[i]: orkflow_name: no operator` |
| an operator the field does not take | `filters[i]: status does not take contains (~); it takes equals (=), not equals (!=), in (=a,b)` |
| value count | `filters[i]: equals (=) takes exactly one value`, `filters[i]: in (=a,b) takes 1 to 64 values`, `filters[i]: in (=a,b) cannot hold an empty value` |
| a value over 1024 bytes, or not UTF-8 | `filters[i]: a value is at most 1024 bytes of UTF-8` |
| an empty contains or prefix | `filters[i]: contains (~) needs a non-empty value` |
| an enum value the list cannot select | `filters[i]: "AWAITING" is not a status this list can select; status is one of …` — the article follows the field's name: `"TASK_LOG" is not an event this list can select; event is one of …` |
| a name, in equals, not equals or in | `filters[i]: namespace "Payments" is not a slug (^[a-z][a-z0-9-]*$)` |
| an Orker id, in equals, not equals or in | `filters[i]: orker_id: invalid orker id: empty` |
| bool | `filters[i]: drained is true or false, not "yes"` |
| time | `filters[i]: "2026-10-09" is not an RFC 3339 instant with a zone (2026-10-09T00:00:00Z)` |
| an unmet requirement | `filters[i]: origin.id needs an equals (=) filter on origin.kind` |
| an empty interval | `created_at >= 2026-10-09T00:00:00Z and < 2026-10-08T00:00:00Z select no instant` |
| a page token of another filter set | `page_token was issued for other filters; drop it and list from the first page` |

Values are checked only where the field declares what they must spell. A
whole-value comparison on a field that holds an OrkStack name — a namespace,
Orkflow, task id or pool, or a trigger, schedule or webhook name — is held to
the same name rule as every other boundary (`AGENTS.md` §7: validated, never
sanitized), and the empty value is accepted, because it is what an unset name
reads as (`namespace=` selects deployment-wide webhooks). An Orker id is held
to the Orker id rule. A contains or prefix value is a fragment, not a name,
and is held only to the size and UTF-8 limits; an opaque id that names
nothing matches nothing — an empty answer, never an error.

### Canonical form and page tokens

A filter set is canonicalized before it is used: an `in` of one value becomes
equals and its values are deduplicated and sorted; enum values become upper
short names; instants become UTC at microsecond precision; several lower
bounds on one field collapse to the latest and several upper bounds to the
earliest; then each filter is written in the CLI grammar, the texts sorted
bytewise and duplicates dropped. A page token binds the SHA-256 of those
texts, so `status=failed` then `status=FAILED`, or the same filters in
another order, continue one listing, and a changed set never does — the
token is refused with the message above, and clients clear it whenever the
filters change. A token of an older shape is `invalid page_token`.

### What a filter costs

A filter only ever narrows what is read; the whole set is still applied to
every row read, so a narrower read can never change an answer.

| List | Decided from | Note |
| --- | --- | --- |
| Namespaces | `name`: the namespace registry's name listing, before any document is read; every other field: the document | name filters alone read only the page's documents; any other filter reads documents in name order, eight at a time, until the page is full |
| Orkflows | the current-version documents the list already reads | filters apply before the page is cut |
| Webhooks, triggers | the subscription and trigger documents the list already reads | a single `orkflow_name=` narrows what the trigger store reads |
| Schedules | `id`, `name`, `orkflow_name`: the cluster's list of schedules, before any is described; the rest: each schedule as described | a narrowed listing describes only what it may return |
| Pools | the computed pool rows | |
| Activities | the catalog | |
| Orkers | `orker_id=`: that one Orker, asked on its own; everything else: the cluster's list of Orkers and each one's answer | a malformed id is refused before anything is asked |
| Releases | `build_id=`: that release alone is counted; the release rows | filters select release rows only; every other field is the deployment's |
| Deliveries | every field from the record's key (`created_at`, `namespace`, `run_id`, `webhook_id`, `event`, `delivered`) | a body is read only for a record whose key matches; the `created_at` bounds are the window; 100,000 keys per page at most |
| Audit trail | each record's body (the key holds only the day and the id) | the `at` bounds are the window; 5,000 records per page at most |
| Runs | live runs: what the cluster lists for each run (its Orkflow, status, origin and start); finished runs: the run index's keys, never a run body | exactly the fields both halves answer without reading a run: `orkflow_name`, `status`, `created_at`, `origin.kind` and `origin.id`. One `orkflow_name=` or `status=` narrows what each half reads, one `origin.kind=` (and `origin.id=` beside it) selects the origin index, and the `created_at` bounds bound the scan; any other filter is decided per row without narrowing the read. A finished run's body is read only when its key matches |

**Runs.** `created_at` is the instant a run is ordered and paged by: its
`created_at`, else its `started_at`, else its `finished_at`, at microsecond
precision — so a `created_at` filter selects a run on exactly the instant the
list sorts it by, and a run with no time at all matches no `created_at`
filter. `origin.kind` takes only equals, because a run started before origins
were recorded is in no origin index and matches no `origin.kind` filter, and
`origin.id` needs an equals filter on `origin.kind` (an id is meaningful only
within a kind). `status` never names `AWAITING`, which is a task status: a run
whose task is awaiting is `RUNNING`.

### Filters in the browser

Every filtered list renders one component, `ListFilter.svelte`, from the same
declaration (`ui/src/lib/listFilter.ts`): a form holding a **draft** of the
applied filters — refilled from the URL after every navigation, applied only
with **Apply filters** — with one row per filter: the field (`Label (path)`,
a labels map as `Label (labels.KEY)` with a key input), the operator (only
the field's own, by their display names), and a value control for the field's
kind (text; for in, one input where `\,` is a literal comma; an enum select,
or a checkbox per value for in; `true`/`false`; a `datetime-local` shown with
the UTC instant it sends). **Add filter**, **Clear filters** and the CLI
equivalent of the applied filters (`orkctl … --filter '…'`, every value
shell-quoted) sit beside it. The browser blocks nothing the CLI would send —
an empty label key goes out as `labels.`, an empty time as `""` — and shows
the server's refusal verbatim in the page's error panel, with Clear filters
still there to recover.

The filters live in the URL as one **repeated** key per list — `filter`, one
value per filter in the CLI grammar, in canonical order, after `ns` — so a
link is the listing. Admin → Webhooks holds two lists: the subscriptions
table's filters are `filter`, the Deliveries panel's `deliveryFilter`. Values are percent-encoded; in a hand-typed link write a
`+` as `%2B` (or use `Z`), since a literal `+` decodes as a space and is then
refused as not an instant. A parameter that does not parse is shown as an
error without a request.

## Orkflows

### Validation without applying

`OrkflowService.ValidateOrkflow` runs the same preparation as apply — compile,
resolve the namespace, check allowed pools and secret sources, resolve images
and the image policy, read declared activity interfaces, derive the effective
version and definition digest, compare the declared interface against the
current version — and writes nothing: no definition, pointer, execution or
subscription, and it never claims an apply's concurrency check succeeded.
Each problem is a `ValidationIssue` (`field`, `message`, `severity` ERROR or
WARNING) with an actionable field path when one is known and an empty field
otherwise, never a guessed location. A dependency outage stays an RPC error.
Apply still checks edit tokens and immutable-version conflicts at its own
write boundary. Warnings stay visible after validation and after a successful
apply.

CLI: `orkctl orkflow validate FILE [--json]` exits non-zero only for an ERROR.
UI: **Validate** on the Apply page and in the editor; validation alone never
clears the editor's dirty state.

### Interface compatibility

An Orkflow's declared interface is its `inputs:` and `outputs:`
([DSL.md](DSL.md), "Promoting compares declared interfaces"). Two reads serve
it, computed per request and stored nowhere:

- `OrkflowService.CompareInterfaces` classifies how one stored version's
  interface changes in another: every finding has its own `COMPATIBLE`,
  `BREAKING` or `UNKNOWN` verdict and the one sentence both clients print.
- `OrkflowService.ListOrkflowCallers` lists every composing task of another
  Orkflow's scanned version, every trigger and every schedule of the
  namespace that references it, with the version each pins (empty: it follows
  current, so a promotion moves it). A schedule the cluster could not
  describe carries `unavailable_reason`. Nothing of another namespace is
  listed.

`OrkflowVersion.interface_verdict` marks each version against the current one.
`SetCurrentOrkflowVersion` refuses a BREAKING promotion with
`FailedPrecondition` naming each breaking change until
`acknowledge_breaking` is set, never refuses COMPATIBLE or UNKNOWN, and swaps
the pointer only while it still names the version it compared against — a
promotion that raced another is refused rather than swapped across an
unchecked break. A rollback is gated alike. Apply and validate report a
breaking change against the running version as field-anchored WARNINGs and
never refuse.

CLI: `orkflow compat NAME [FROM] TO`, `orkflow callers NAME
[--all-versions]`, `orkflow promote NAME V [--acknowledge-breaking]` (prints
`Interface against <v>: …`), the `COMPAT` column of `orkflow versions`, and
the warnings of `apply`/`orkflow validate`. `compat` exits 0 whatever the
verdict. UI: a verdict chip on every row of the Versions tab, **Compare
interfaces** beside **Diff**, a promote dialog listing breaking and unknown
findings, what follows current and what pins it, with an acknowledgement
checkbox for a breaking promotion, and a **Callers** section under the version
table.

### Run plan

`OrkflowService.PlanRun` answers, for one definition and one input, which
tasks would run, which would be skipped and why, and in which dispatch wave
each would start. What a plan decides, its flags, exit codes and how each
kind of task reads are in [DSL.md](DSL.md), "Planning a run"; this section is
the parity contract.

The server resolves a stored version as `GetOrkflow` does (empty = current)
or prepares an unapplied document as `ValidateOrkflow` does, applies declared
input defaults, and walks the DAG with the interpreter's own condition
evaluator and skip sentences, so both clients show the same outcome and
detail strings. An engine that predates assumed failures ignores
`assume_failed`; both clients then say
`The server did not apply --assume-failed: it predates assumed failures.`
rather than presenting the plan as the failure path.

**A plan never submits.** No flag, button or field turns a plan into a run:
`PlanRun` mints no run id, takes no idempotency claim, writes no definition or
event, and is not an admission check — pools, holds, budgets and limits stay
`SubmitRun`'s questions, so Plan works while a namespace is held.

CLI: `run plan <orkflow> [--version V]` or `run plan --file FILE`, with
`--input`/`--input-json`, `--output TASK=JSON`, `--outputs-file PATH`,
`--assume-failed TASK` and `--assert-runs`/`--assert-skipped`. UI: **Plan**
beside **Submit run** in the submit dialog (also behind **Run again**), with
assumed-outputs and *Assume these tasks fail* fields, a
wave/task/outcome/detail table, the summary sentence, and the plan's CLI
equivalent; a stale plan clears when the draft changes.

## Submitting runs

### Run origin

Every run carries one immutable `Run.origin`: what started it. It is decided
once, at submit, by the process that accepted the submission, travels on the
replay-pinned `RunPlan`, and is present on every snapshot, in the archive and
in every webhook body (inside `run`). Nothing in OrkStack reads it to decide
anything — not admission, routing or retries.

| Kind (slug) | Produced by | `id` | `reference` | `description` | `principal` |
| --- | --- | --- | --- | --- | --- |
| `SUBMIT` (`submit`) | `SubmitRun` without `source_run_id` | — | caller's | — | `Ork-Principal` |
| `RUN_AGAIN` (`run-again`) | `SubmitRun` with a `source_run_id` the server finds in the namespace (NotFound otherwise) | source run id | caller's | — | `Ork-Principal` |
| `TRIGGER` (`trigger`) | the trigger door | trigger id | `Ork-Origin-Reference` header | the trigger's description | `Ork-Principal` |
| `SCHEDULE` (`schedule`) | a schedule fire | schedule id | the nominal fire time | the schedule's name | — |
| `PARENT_RUN` (`parent-run`) | a composed task | parent run id | — | — | — |

The kind is never taken from a client. A caller may add only a `reference`
(`--origin-ref`, the submit dialog's **Origin reference**, or the trigger
door's header): at most 256 bytes, no control characters, validated and never
sanitized — an invalid one is `InvalidArgument` and starts nothing. `principal`
is the `Ork-Principal` assertion, recorded and never authenticated. A run
recorded before origins existed has kind `UNSPECIFIED`, renders `unrecorded`
(never `submit`) and matches no origin filter.

`ListRuns` selects by origin with an `origin.kind=` filter, or `origin.kind=`
and `origin.id=` ("Filtering lists"), live and finished runs alike — finished
runs through the archive's origin index ([STORAGE.md](STORAGE.md)) — merged
under the same fail-rather-than-subset rule as every list.

CLI: `run submit --origin-ref`, `run again --origin-ref`, `run list --filter
origin.kind=KIND [--filter origin.id=ID]`, an `ORIGIN` column (`-` when
unrecorded), and `Origin:`, `Reference:` and `Principal:` lines on `run get`.
UI: the run header's origin line — kind chip, the id linked to the source run
or the Triggers/Schedules tab, the description, the reference linked only when
it is an http(s) URL, and `asserted by …` with a not-authenticated title; the
runs list's **Started by** (`origin.kind`) and **Origin id** (`origin.id`)
filters, kept in the URL as `filter` keys, and its **Origin** column.

### Run again

UI **Run again** loads the selected run into a reviewed submission draft,
showing the source and its pinned version. CLI `run again RUN` reads that run
and submits through `SubmitRun` with `source_run_id`, so the new run's origin
is `run-again <source>`. Both default to the source's pinned version and
inputs, support reviewed overrides (`--input`, `--input-json`, `--version`,
`--no-cache`, `--origin-ref`), and `--dry-run` prints the exact new submission.
The original idempotency key is never reused; a fresh one may be supplied. A
new run gets a new id. Both explain before submitting that this starts a new
execution and may repeat external side effects. An unavailable pinned
definition fails with its coordinates and never silently switches to current.
The seeded draft exists only in the page that already holds the run; the
dialog's copied command is `orkctl run again <source>` with the same flags.

### Awaited tasks

A task may declare `await:` instead of `uses:` ([DSL.md](DSL.md), "Awaited
tasks"): it starts no container and waits, `AWAITING`, until something outside
the run resolves it or its deadline passes. `RunService.ResolveAwait`
completes (`values_json`, the exact bytes that become the task's
`values.json`) or rejects (`reject`, `reason`) one await of a running run,
namespace-scoped like `CancelRun`. It answers once the values are stored and
the resolution delivered — never once applied; a repeat of the resolution that
already won answers `repeat` and delivers nothing. An await declaring
`callback: true` also accepts `POST /v1/runs/{id}/awaits/{task}[/reject]` with
the one-time token in `SubmitRunResponse.await_callbacks`; only the token's
digest is kept. `TaskRun.await` carries the deadline and who resolved it,
through which door, as asserted. `TASK_AWAITING` and `TASK_RESOLVED` are
subscribable webhook events. A Connect resolution is audited
(`run.await.resolve`, values recorded only by digest); the callback door, like
the trigger door, is not.

CLI: `run resolve RUN --task TASK [--values JSON | --values-file PATH |
--reject --reason TEXT]` — `--values-file` is sent verbatim (`-` reads stdin),
neither flag means `{}`, and every refusable flag combination is refused
before a request. `run submit`/`run again` print each callback once as a
shell-safe `curl`; `run get` adds an await table (deadline, remaining,
resolved via, by, callback); `run plan` lists awaits; `run timing` reads an
await as `awaited`. UI: the run page's await panel in place of the log pane —
state, deadline, live remaining time, **Resolve** (typed fields over the
await's schema, or JSON sent verbatim) and **Reject** dialogs with their CLI
equivalents, *Delivered* until the run applies it, and a placeholder `curl`
for a callback await; the submit dialog shows the one-time callback panel
before it continues to the run.

## Reading runs

### Run browsing

`ListRuns` takes filters ("Filtering lists"): `orkflow_name`, `status`,
`created_at`, `origin.kind` and `origin.id`. `created_at>=` is inclusive and
`created_at<` exclusive; an inverted or empty interval is `InvalidArgument`.
Rows sort newest `created_at` first, then id descending at equal times;
nothing parses an id. Live and finished runs merge by id, the archive
preferred for terminal records; both halves get the same filters, and each
applies them and the cursor before its own limit, fetching enough to know
whether another matching row exists.

The page token is opaque and versioned and binds the namespace, the canonical
filter set and the last returned `created_at`/id. A token presented with other
filters is `InvalidArgument` (`page_token was issued for other filters; drop
it and list from the first page`); clients clear the token when a filter
changes, and a page-size change may keep it. A cursor is a position, not a
frozen snapshot: newly visible records before it appear on refresh and
statuses may change between pages — a run whose status changes out of the
filters between pages simply stops matching. A run that has just finished can
still be listed as live and `RUNNING` for a moment after its finished record
exists; under a status filter that excludes its final status (`status!=SUCCEEDED`,
`status=RUNNING,PENDING`) it is then listed once, as `RUNNING`, until the
listing catches up — never twice and never with a wrong final status, and a
refresh corrects it. A scan cut short by its budget is never labelled
complete; an undeterminable continuation is an actionable failure.

CLI: `run list [--filter F…] [--limit N] [--page-token TOKEN]`; human output
prints a usable next-page command that repeats every `--filter`, `--json`
carries `next_page_token`. UI: the Runs page's filter form (`ListFilter`),
whose header reads `Runs for <name>` — and whose **Submit a run** opens that
Orkflow — when one `orkflow_name=` filter is applied; the dashboard's failure,
flake and fingerprint rows, an Orkflow's **View runs** and a run's **Child
runs** open it already filtered.

#### Paging in the browser

The Runs, Orkflows and Admin → Namespaces lists share one URL rule and one
footer (`ui/src/lib/browsing.ts`, `Pagination.svelte`):

- **The URL holds what the page shows, and nothing else**: `ns`, the filters,
  `size`, the engine's `cursor`, and `page` (1-based, omitted at 1). A pasted
  link opens the exact page it was copied from. One function writes the query
  string, `listHref`: it sets every scalar key once and omits empty values, so
  a scalar never appears twice and a cleared filter leaves the bar. On all
  three the filters are repeated `filter` keys in canonical order ("Filtering
  lists") — the one key that repeats.
- **The path back lives in `sessionStorage`**, keyed by the list's canonical
  filter query, written on Next before navigating. Not the URL (it grew by one
  escaped cursor per page), not `localStorage` (a cursor is a position, so a
  stack means something only while its reader is paging), not in-memory page
  state (F5 must keep Previous), and not a reverse engine cursor (reverse
  paging over the live/archive union is its own design).
- **When the path back is unknown** — a pasted link, a new tab, another filter
  set, storage that throws — Previous is disabled and **First page** is
  offered. Previous is enabled only when the stack describes this exact page.
- **No totals.** Responses carry no count, so the footer never shows "of N":
  `Page 3 · 10 runs`, `Page 5 · 7 runs · last page`, or `7 runs`.
- **Page size** is a field of the filter form on every paged list — Runs,
  Orkflows and Admin → Namespaces — and applies with **Apply filters**, never
  on change, starting a new page 1 and clearing the stack of the filter set it
  leaves. One form never applies some fields on change and others on submit.

The dashboard's window control has four segments — `15m`, `6h`, `24h` and
**Custom** (a `datetime-local` capped at now, labelled `Since <instant>`). The
URL keeps `since=` as an ISO instant because that is what `orkctl stats
--since` takes. There is no "until": the window is `since` to now.
`?window=custom` without a valid `since` shows the error panel rather than
snapping back to `24h`, which would disagree with the address bar.

### Task attempts

Temporal keeps only a task's final outcome, so `TaskRun.attempt` and
`TaskRun.error` describe the last attempt. The Orker files one immutable
`TaskAttempt` record per attempt to the shared store ([STORAGE.md](STORAGE.md)),
and `RunService.ListTaskAttempts` reads them back — no stream: clients refetch
on a task's terminal event, the run's terminal event, or a higher attempt seen
on the log stream.

- **Order is the response's contract**: tasks in `Run.task_runs` order, then
  ascending attempt.
- **Gaps are reported, never hidden.** The highest attempt is the maximum of
  `TaskRun.attempt`, the highest stored record and the log store's latest
  attempt; every number below it with no record is returned as `UNREPORTED`
  carrying only its coordinates. Clients present it as missing evidence ("no
  record from the orker — it crashed or lost its heartbeat"), never as a
  failure, and invent no timing, error or retry schedule for it.
- `TaskRun.attempt` alone is not the attempt count: it is 0 for a task
  Temporal timed out and is the committing attempt for an adopted commit.
- An unattempted task returns an empty list, which is not an error. Records
  share the logs' lifecycle, so a run past cleanup returns empty. An unknown
  task is NotFound, a missing run id `InvalidArgument`, and an engine with no
  attempt store answers `FailedPrecondition` rather than a timeline of
  UNREPORTED rows.

CLI: `run attempts RUN [--task TASK] [--item N]` and `run get RUN --attempts`
(`run get --attempts --json` is refused, pointing at `run attempts --json`).
UI: a per-task retry badge and an attempt timeline with each failed attempt's
own error and a link that pins the log pane to that attempt.

### Task timing

*Was this task slow, or was it waiting?* is read off one snapshot and the
attempt records; there is no timing RPC. `TaskRun.scheduled_at` is when the
engine scheduled the task (the `TASK_SCHEDULED` instant); `TaskRun.started_at`
is when the Orker began the attempt that decided it (unset while running and
for a task no Orker reported on). Each `TaskAttempt` lists `phases`: every
phase it passed through ([OCI_ACTIVITY_CONTRACT.md](OCI_ACTIVITY_CONTRACT.md)
§7.1), contiguous, the last ending at `finished_at`.

Both clients derive, with one rule (`cmd/orkctl/timing.go`,
`ui/src/lib/timing.ts`): WAIT = `started_at − scheduled_at`, RUN =
`finished_at − started_at`, TOTAL = `finished_at − scheduled_at`, and for a
task decided on a retry the queue wait `attempts[1].started_at −
scheduled_at` on its pool. A quantity that does not apply is `-`; one that
cannot be known is `unavailable` with exactly one of: `no orker reported a
start`, `clock skew`, `recorded before scheduled time existed`, `attempt
records reclaimed`, `no attempt record`, `record predates phase spans`, `no
task attempt store`. A snapshot with `started_at` and no `scheduled_at` was
written before the field existed; its TOTAL is dispatch to finish and is
labelled so.

CLI: `run timing RUN [--task TASK] [--item N]` prints the table, the run's
summed wait and each task's attempts with phases; `run get --timing` appends
it and `run get` has a `SCHEDULED` column. `--json` is a CLI-derived document,
not a proto envelope, so `run get --timing --json` is refused. UI: a running
card reads `Since scheduled …`; a finished one draws a wait/run bar (an
unavailable segment hatched and naming its reason); the attempt timeline lists
each attempt's phases and a retried task's queue wait.

### Live attempt introspection

*What is this running task doing right now* is one unary read,
`RunService.InspectRun`. It reads the heartbeat detail the Orker already sends
and the cluster already holds for each pending attempt, and answers one
`LiveAttempt` per in-flight task: its state (`SCHEDULED`, `STARTED`,
`CANCEL_REQUESTED`), attempt, scheduled/started instants, the task's timeout
and grace, Temporal's backstop, and — only when a heartbeat from **this**
attempt exists — its phase, container, bytes logged, Orker and last beat, with
the instant Temporal times the attempt out without another
(`heartbeat_deadline_at`). Nothing is stored, projected or streamed; a
terminal run answers its status and no attempts without touching the cluster;
a cluster error is `Unavailable`.

`heartbeat_available=false` carries one of three reasons: `NOT_STARTED` (no
Orker has the attempt), `NO_HEARTBEAT_YET` (an Orker has it and no beat from
this attempt has arrived — a previous attempt's detail Temporal still holds is
never shown as the new attempt's), `UNDECODABLE`. A last beat older than
`task.HeartbeatStaleAfter` (20 s, two ceiling ticks) is `heartbeat_stale`,
computed server-side against the response's `computed_at`, which every age
both clients print is measured against.

CLI: `run inspect RUN [--task TASK] [--item N] [--watch] [--interval D]`
prints `STATE`, `PHASE`, `ORKER`, `CONTAINER`, `LOGGED`, `RUNNING`,
`HEARTBEAT` and footers (the reason, the stale deadline, the timeout as a
lower bound, the pool an attempt waits on, the next retry, a requested
cancellation). `--watch` polls (default 5 s, at least 1 s) until the run is
terminal and exits with the run's code (0 succeeded, 2 failed, 3 canceled);
under `--watch`, `--json` prints one compact response per line. UI: a running
card polls the same read every 5 s while the page is visible and shows
`Running for …`, a live line (`Waiting for an orker · …`, `Phase run · 412.0
MiB logged · heartbeat 3s ago`, `… — STALE`, `Attempt 3 scheduled for …`,
`Cancelling · phase …`, `Phase unavailable · <reason>`) and the Orker.

### Task pins

A **pin** is a named group of tasks of one Orkflow that execute on one Orker
and share `/orkstack/shared` ([DSL.md](DSL.md), "Pins"). Two `TaskRun` fields
carry it, with no new RPC, stream or event:

- `pin` — the pin the task belongs to, as authored; empty when unpinned.
- `orker_id` — the Orker that produced the outcome, for **every** task that
  reached an Orker. Empty means "never reached an Orker" (skipped, canceled
  before dispatch, or its host died before reporting), never "not pinned". A
  member lost with its pin (`PIN_HOST_LOST`, `PIN_TIMEOUT`) is backfilled from
  a sibling of the same pin — a pin never moves, so a sibling's value is that
  task's by construction. When no member ever reported one, the failure's
  `error.details` carries the session's raw OS host name as an explicitly
  labelled best-effort lead, which is not an `orker_id`.

Read together they prove a pin held: one pin's members show one `orker_id`.
Neither names a queue, session or session id. `run get` renders `POOL`, `PIN`
and `ORKER` columns (`-` when absent); the UI states `Pin <name>` and `Orker
<id>` beside `Pool <pool>` and tints each pin's member cards with one colour
derived from the pin name. `run watch` needs nothing new: `TASK_SCHEDULED`
reads `task scheduled on pool <pool>, pin <name>` for a pinned task.

### Run diff

*"This run failed and last night's succeeded — what is different?"*
`RunService.DiffRuns` is one read over what `GetRun`, `GetOrkflow` and
`ListTaskAttempts` serve, computed on every call and stored nowhere. Both runs
resolve in the request's namespace (another namespace's run is NotFound, never
compared); equal or empty ids and an unknown field name are `InvalidArgument`
(the last lists the valid fields). Runs of different Orkflows are compared and
flagged (`same_orkflow=false`), never refused. A store error fails the call;
it never degrades to "unavailable" evidence.

**Fields, in canonical order:** `definition`, `input` (run-level); `presence`,
`status`, `attempt`, `pool`, `pin`, `orker`, `exit_code`, `error`, `duration`,
`image`, `platform_digest`, `cached` (task-level). `differences` is ordered:
the `definition` row, then the from-run's tasks in its `task_runs` order, then
tasks only the to-run has; within a task, fields in canonical order. The input
difference is not a row but `input_unified_diff`: a canonical-JSON unified
diff (keys sorted, integers without fraction, labelled `<run id>/input.json`)
printed verbatim.

**`only` and `presence`.** `only` narrows the fields and the reads — naming
neither `image` nor `platform_digest` never opens the Orkflow or the attempt
store. `presence` is compared under every `only`: a task one run lacks yields
exactly one `presence` row (`present`/`absent`), so narrowing never makes an
added or removed task count identical. `fields_compared` echoes the resolved
list.

| `field` | Rendered value | `-` when | `unavailable` when |
| --- | --- | --- | --- |
| `presence` | `present` / `absent` | never | never |
| `status` | `SUCCEEDED`, `SKIPPED`, … | never | never |
| `attempt` | decimal, `0` included | never | never |
| `pool` | verbatim | never reached an Orker | never |
| `pin` | verbatim | not pinned | never |
| `orker` | `TaskRun.orker_id` | no Orker reported | never |
| `exit_code` | decimal | no Orker reported | never |
| `error` | `{code} ({category})` or `{code}`; message and details never compared | no error | never |
| `duration` | `finished − started`, Go spelling (`1m3s`) | not terminal; SKIPPED; never reached an Orker | `timestamps_missing`, `timestamps_inverted` |
| `image` | the pinned definition's `Task.image.pinned`, else the outcome attempt record's | never | definition unavailable and no record answers; `task_not_in_definition` |
| `platform_digest` | the outcome attempt record's `image.platform_digest` | never reached an Orker; the container never ran | `no_attempt_store`, `no_outcome_attempt`, `attempt_record_missing` |
| `cached` | `hit <source run>`, `miss`, `bypassed` | no cache | never |

**The outcome attempt record** — for `image` (fallback) and
`platform_digest` — is the record whose attempt is `TaskRun.attempt` (the
committing attempt, for an adopted commit). When that record is gone, the
newest record with `commit_adopted` and outcome SUCCEEDED answers instead: an
adopting retry files its record under its own number, carrying the committing
attempt's image from `commit.json`. A failed or canceled record is never used.
A definition whose digest differs from the run's pinned digest cannot answer.

| token | sentence |
| --- | --- |
| `definition_unavailable` | the pinned definition is no longer stored |
| `definition_digest_mismatch` | the stored definition does not hash to the digest this run pinned |
| `task_not_in_definition` | the pinned definition has no task of this id |
| `no_attempt_store` | this engine has no task attempt store configured |
| `no_outcome_attempt` | the snapshot names no outcome attempt |
| `attempt_record_missing` | no record of the outcome attempt or of a retry that adopted its commit: reclaimed, or the Orker never filed one |
| `timestamps_missing` | the snapshot lacks a start or finish time |
| `timestamps_inverted` | the finish time precedes the start time |

CLI: `run diff FROM TO [--only FIELD,...]` prints a header (`Diff`,
`Namespace`, `Compared` when narrowed, `Orkflow`, `Status`, `Finished`,
`Definition`, `Input`), the input diff, a TASK/FIELD/FROM/TO table (or `No task
differences.`), the summary `{differ} of {compared} tasks differ; {identical}
identical[; {n} field(s) unavailable].` and one footnote per unavailable side.
Digests are never abbreviated; exit status is 0 whenever the RPC succeeded.
UI: **Compare** on the run page pre-selects the Orkflow's newest other
SUCCEEDED run and opens `/runs/{from}/compare?to={to}`, a deep-linkable page
with a swap link, the same table (only differing cells tinted), summary and
footnotes.

### Artifact retrieval

`RunService.ListRunArtifacts` lists a run's (or one task's) recorded
references — output artifacts, `values_ref`, and the stdout/stderr system
artifacts — each with its task, kind (`OUTPUT`, `VALUES`, `STDOUT`,
`STDERR`), attempt and `ArtifactRef`. `RunService.ReadArtifact` streams one,
addressed by task, kind and the exact listed name, in chunks of at most
64 KiB, the first carrying the reference and the last `last=true`.

Every read resolves against the authoritative, namespace-checked run and its
recorded references; an arbitrary URI, object key or local path is never
accepted. Only attempts with a recorded reference are listed (older logs stay
readable through the attempt-aware log stream). Bytes stream from the artifact
store, outside Temporal and events. Missing or retired bytes are explicit
errors, and size and checksum are verified before `last` is sent — a mismatch
ends the stream `DataLoss` — so a download is never called complete on bytes
that do not match.

CLI: `run artifacts RUN [--task] [--item]` and `run artifact download RUN
--task --kind --name --output PATH [--overwrite]`, which writes a temporary
sibling and renames on success, refuses an existing file without
`--overwrite`, and never turns an artifact name into a path. UI: the same
task/kind/name/size/checksum metadata and a download with safe attachment
naming.

### Artifact provenance

`RunService.GetArtifactProvenance` reads the object index the run archive
keeps for cleanup ([STORAGE.md](STORAGE.md), "Artifact provenance"),
addressed exactly as a download is (optionally with `expect_sha256`; a
mismatch with the recorded digest is `FailedPrecondition` naming both). It
answers the recorded reference, `bytes_present`, this run's own reference
state (`HELD`, `RELEASED` with `released_at`, or `ABSENT`), every archived
run of the namespace that recorded the same object, `references_unreadable`
and `computed_at`.

Three absences are never conflated: bytes gone, reference released by
cleanup, and no evidence either way. References are ordered by
`run_created_at` descending, then run id descending, then `task_runs` order
and kind. A store or listing failure is `Unavailable`; more than 1000
references is `ResourceExhausted`; no archive or no artifact store is
`FailedPrecondition`. There is no stream and no cache.

CLI: `run artifact provenance RUN --task --kind --name [--item]
[--expect-sha256 HEX]` prints the artifact, a BYTES line, a REFERENCE line in
one of three fixed wordings and a REFERENCED BY table; missing bytes exit 0 —
a finding, not an error. UI: a **Provenance** toggle beside each artifact's
Download, fetched on every open, with the same wordings, each reference
linking to its run (this one marked `this run`) and the unreadable footer.

### Run evidence

`RunService.ExportRunEvidence` streams one finished run's evidence bundle — a
`tar.gz` led by `MANIFEST.json` — assembled per request and kept nowhere; the
layout, manifest, states, reason tokens and verifier are
[RUN_EVIDENCE.md](RUN_EVIDENCE.md). `manifest_only` streams the manifest
alone. A live run, or a terminal run whose archive write has not landed, is
`FailedPrecondition`; a member whose bytes do not match its recorded digest
ends the stream `DataLoss` without `last`.

CLI: `run evidence export RUN --output PATH [--task [--item]] [--no-logs]
[--no-artifacts] [--overwrite]` (or `--json`: the manifest itself, no file) and
`run evidence verify FILE [--against-store]`. UI: **Evidence** on a finished
run's header opens a dialog that shows the manifest summary before any bundle
byte is fetched (`N present · … · size`), the definition line, absent members
with their sentences, **View manifest**, **Include logs**/**Include
artifacts** toggles that re-fetch the manifest, and a browser download —
refused above 512 MiB, where the CLI is the tool — that completes only on
`last`.

### Fan-out

A task that declares `each:` ([DSL.md](DSL.md), "Fan-out") is one node whose
items are each a durable activity. `TaskRun.each` (`expanded`, `count`) and
`TaskRun.items` (one `TaskRun` per item, `item` set) carry them on `GetRun`;
an item's `TASK_LOG` event carries `Event.item`. Every per-task read takes one
optional `item` — `StreamTaskLogs` and the log socket (`item=`),
`ListRunArtifacts`, `ReadArtifact`, `GetArtifactProvenance`,
`ListTaskAttempts`, `InspectRun`, `ExportRunEvidence` — under one set of
rules: `item` without `task_id`, a negative item, or an item of a task that is
not a fan-out is `InvalidArgument`; an item of a fan-out not yet expanded, or
past its count, is NotFound; a read of ONE stream or object of a fan-out
without `item` is `InvalidArgument` naming `--item N`, while a list without
`item` returns every item's rows. `ListRuns`, the webhook body and the
workflow result carry a fan-out's node row without `items`, so a list stays
bounded whatever a run expanded into. A human rendering spells an item
`convert[97]` (0-based); that spelling is never sent or parsed — the wire
field is the integer.

CLI: `run get` shows a fan-out as one row (`RUNNING 97/120`) with a summary
line and `--expand <task>`; `--item N` on `run logs`, `run artifacts`, `run
artifact download|provenance`, `run attempts`, `run timing`, `run inspect`,
`run evidence export` and `task run`; `run watch` spells item events
`convert[97]`. UI: one card per fan-out with a summary and an item grid moved
by item events, an item panel (status, attempt, Orker, exit, times, failure,
attempts, logs, artifacts), an `N items retried` badge and
`?task=&item=&attempt=` deep links; the DAG labels the node `each ≤ N`.

### Composed Orkflows

A task whose `uses:` is `orkflow:<name>@<version>` ([DSL.md](DSL.md),
"Composition") starts a **child run** and reaches no Orker. `TaskRun.child`
carries the pinned name, version and definition digest, and `run_id` once the
child's start is confirmed — never before. The child's own origin is
`{PARENT_RUN, <parent id>}`, so a parent's children are `ListRuns` with
`--filter origin.kind=PARENT_RUN --filter origin.id=<id>` — no new RPC; a child is read, watched, cancelled,
diffed and exported by its own id. `StreamTaskLogs` refuses a composed task
with `FailedPrecondition` naming the child and `orkctl run logs <child>`;
`ListTaskAttempts` answers an empty timeline; `DiffRuns` compares a composed
row on presence, status, error and duration only. Apply reports composition
errors as field-anchored issues, and submit refuses a tree the deployment can
no longer plan with `FailedPrecondition` naming the child.

CLI: `run get` names each child run (or why none started) and `--tree
[--depth N]` appends every child's task table depth first, reading in the
parent's namespace and printing `unavailable (<code>: <message>)` for a child
it cannot read (`--tree --json` is refused, naming `taskRuns[].child.runId`);
`orkflow get` adds `COMPOSED` and `OUTPUT` tables; `task run` refuses a
composed task by name. UI: a composed task's card links the pinned child
Orkflow and, once `TASK_STARTED` arrives, the child run, with its status and a
live roll-up of the child's tasks from the child's own stream; the header's
**Child runs** link opens the runs list filtered to the children; the Orkflow
page links each child at its version with the pinned digest and lists the
declared outputs.

### Caching

A task that declares `cache:` ([DSL.md](DSL.md), "Caching") may reuse an
identical earlier run's commit. `TaskRun.cache` (`HIT` with its source run,
commit, digest and producing times; `MISS` with a reason token and sentence;
or `BYPASSED`; and whether the memo was written) marks it on `GetRun` and
`ListRuns`; each `TaskAttempt` carries its own decision and the key's
components; a hit's `TASK_SUCCEEDED` reads `task succeeded (cached from run
<id>)`. `SubmitRunRequest.no_cache` runs every cached task's container and
refreshes its memo, for the whole tree. `CacheService` serves `GetCacheStats`
and `PurgeCache` (exactly one of an Orkflow, the namespace-scoped memos, or
all; optionally one task and an age; at most 10,000 deletions per call, with
`more`) — see [STATS.md](STATS.md) §9. Submit refuses a cached task on a pool
whose Orker predates caching with `FailedPrecondition`.

CLI: `run submit|again --no-cache`; `run get` labels a hit `SUCCEEDED
(cached)` with one cache line per cached task; `run attempts` adds a `CACHE`
column; `orkflow get` a `CACHED` table; `cache stats [--orkflow] [--since]
[--until]`; `cache purge (--orkflow NAME | --namespace-scoped | --all)
[--task] [--older-than]`, refused before any request unless exactly one
selector is given. UI: **Cached** (linking the producing run), **Cache miss**
with its reason, or **Cache bypassed** on the task card; Inspect shows the
key, source commit and memo write; the submit panel offers *Ignore cached
results*; **Admin → Cache** shows hit rate, reused time and memos and purges
through a confirmed dialog.

### Joins and failure handlers

A task's `join:` ([DSL.md](DSL.md), "Joins and failure handlers") says when it
may start from how its needs ended; `Task.join` and `TaskRun.join` carry it. A
**failing run** — its outcome already `FAILED` while its failure handlers run
— is `Run.status = RUNNING` with `Run.error` set, and its stream carries one
`TASK_LOG` with no task id (`run failing after task "<cause>" failed; waiting
for its failure handlers: <handlers>`); it ends `FAILED` with the original
reason whatever its handlers did. Submit refuses a container failure handler
on a pool whose Orker predates joins (`FailedPrecondition`;
`allow_empty_pool` does not override it). No new RPC, write or audit row.

CLI: `run get` reads `Status: RUNNING (failing)` and prints a `JOIN POLICY`
table; `run watch` prints the failing note with `-` for its task; `orkflow
get` and `apply` print a `JOIN POLICY NEEDS` table; `run cancel` on a failing
run stops its handlers and the run still ends `FAILED`; `task run --need
TASK=STATUS` overrides a handler's need outcome locally. UI: the DAG marks a
joined task and draws failure handlers and their edges dashed; the run page
shows a `join` chip per joined task, keeps a skipped or cancelled task's
reason after the run ends, and — live, from the failing note — a *Failing*
banner naming the outstanding handlers beside a Running pill; a pending
handler reads `Waiting for <needs>`.

## Webhooks

### Webhook administration

`WebhookService` manages subscriptions: `RegisterWebhook`, `ListWebhooks`,
`UpdateWebhook`, `DeleteWebhook`. Subscriptions are stored in the shared store
at `metadata/webhooks/{id}.json` (`wh-{16 hex}`), with atomic create and
delete, so management and delivery matching both see another replica's
changes on the next request; there is no startup snapshot and no process-only
registry. A storage failure never produces a successful registration or
deletion, and a notification lookup failure never fails a run. The legacy
plain-JSON `/v1/webhooks` routes are compatibility adapters over the same
service; clients use the generated types.

- **Events.** The four run transitions (`RUN_STARTED`, `RUN_SUCCEEDED`,
  `RUN_FAILED`, `RUN_CANCELED`) and six task events (`TASK_SCHEDULED`,
  `TASK_STARTED`, `TASK_AWAITING`, `TASK_RESOLVED`, `TASK_SUCCEEDED`,
  `TASK_FAILED`); `TASK_LOG` is refused. Empty selects every run transition
  and no task event: task events are opt-in by name.
- **Secrets.** URLs must be HTTP(S). The signing secret (supplied, or
  generated) is disclosed once, in the register response; `List` and `Get`
  never carry it. Delivery reads the registry through the one read that does
  (`ListForDelivery`). A matched subscription without a secret is delivered
  unsigned and logged at Warn (`webhook delivery unsigned`).
- **`name`** is an optional handle by which an automation document addresses
  the subscription ([AUTOMATION.md](AUTOMATION.md)): a slug, unique among the
  named subscriptions of its namespace scope, and it **requires** a namespace
  scope, because a deployment-wide subscription belongs to no namespace. It
  plays no part in delivery.
- **`UpdateWebhook`** replaces name, URL, events and scope in place on the
  register path's checks (a conditional put on S3, so an update never brings
  back a deleted subscription). The id and secret never change, so a receiver
  keeps verifying and the delivery history stays continuous. Rotating a secret
  is delete then register.

**What a receiver gets.** One `POST` per matched event, `Content-Type:
application/json`, body:

```json
{"runId":"ork-…","orkflowName":"…","status":"FAILED","timestamp":"<RFC 3339>","taskId":"…","attempt":2,"run":{…}}
```

`taskId` and `attempt` appear only on a task event (`attempt` is omitted when
Temporal has not numbered one — `TASK_SCHEDULED`, `TASK_STARTED`, a timed-out
task); `run` is the full run in [JSON.md](JSON.md)'s dialect, `origin`
included. Headers: `X-Ork-Signature: sha256=<hex HMAC-SHA256 of the exact body
bytes>`, `X-Ork-Event`, `X-Ork-Delivery` (stable across one delivery's retries:
the receiver's deduplication handle), `X-Ork-Webhook-Id`, `X-Ork-Run-Id`,
`X-Ork-Task-Id` (task events), `X-Ork-Timestamp`, `X-Ork-Attempt`, and
`X-Ork-Redelivery-Of` on an operator's redelivery. Only the body is signed, so
a receiver verifies by hashing the bytes it read; headers vary per attempt and
are outside the signature.

**Delivery is best-effort.** A transport error or a 5xx is retried, up to four
requests within about 700 ms of backoff (100/200/400 ms), each bounded at
10 s; a 4xx ends the attempt series. Delivery runs off the run's path and can
never fail a run. Durable registration does not make in-flight delivery
durable or exactly-once, and there is no ordering across deliveries; across
replicas an event may be delivered twice. A subscriber that must not miss an
event reconciles with `GetRun`. What each delivery did is recorded once it
settles ("Webhook deliveries"); the record is a log, never a queue.

CLI: `webhook register URL [--event E…] [--secret S] [-n NS] [--orkflow]
[--task] [--name]`, `webhook list [--filter F…]` (a `NAME` and a `SCOPE`
column; `--filter namespace=` keeps the deployment-wide ones), `webhook
update ID [--name] [--url] [--event…|--clear-events] [-n|--deployment-wide]
[--orkflow] [--task]` (read-modify-write), `webhook delete ID`. UI: **Admin →
Webhooks** — list (with the filter form), register (name disabled until a namespace scope is given,
run and task event checkboxes), the one-time secret, an Edit dialog stating
the id and secret are kept, and delete. The listing is deployment-wide and
says so rather than implying the namespace switcher filters it.

#### Scoped subscriptions and task outcome events

A subscription may name `namespace`, `orkflow_name` and `task_id`: three
exact-match strings, each empty meaning every, each validated as a slug and
never sanitized. A run with no namespace is in `default`, so a subscription
scoped to `default` matches it. `task_id` is consulted only for task events (a
`RUN_*` event passes it), and registration refuses a `task_id` when no
`TASK_*` event is named. A scope names coordinates, not objects: nothing checks
that the namespace, Orkflow or task exists, so a subscription may precede the
Orkflow it names. A record written before scopes existed reads as
deployment-wide; a record whose scope is invalid is refused on read rather
than widened.

A task that was skipped, or that was running or pending when its run ended, is
never delivered as a task outcome, so a `TASK_FAILED` delivery always reads
`"status":"FAILED"`; a cancellation is read from the run's terminal delivery.
Every delivery lookup lists the whole registry (no cache), up to four more
times per task with task events subscribed.

| Surface | Register | Read the scope | Refusal |
| --- | --- | --- | --- |
| CLI | `webhook register URL -n NS --orkflow NAME --task TASK --event E`; only an explicit `-n` narrows | `SCOPE`: `ns/orkflow/task`, `*` per unset component, `ALL` when unscoped; `--json` carries `namespace`/`orkflowName`/`taskId` when set | Local, before any request, with the server's message |
| UI | Admin → Webhooks: three optional inputs and the event checkboxes, with the best-effort caveat under the task events | `Scope` column, `Deployment-wide` when unscoped | Server `InvalidArgument` in the register form |

#### Webhook deliveries

Every delivery that **settles** — landed, or gave up — is recorded as one
immutable, create-only object
(`metadata/deliveries/{yyyy}/{mm}/{dd}/{delivery-id}/{ns}/{run-id}/{webhook-id}/{event}/{outcome}.json`,
[STORAGE.md](STORAGE.md)): every attempt with its status, error, backoff and
timing, the signature that was sent, and the SHA-256 and byte count of the
exact body — **never the body**, which would be a second copy of the run. The
record proves what OrkStack sent and what the receiver answered, never what
the receiver did with it. Recording is best-effort: a record that cannot be
stored is logged at warn and the delivery's outcome is unchanged. Nothing
reads a record to decide what to send — no pending state, lease or scan — and
nothing reclaims them; a bucket lifecycle rule is the operator's
([DEPLOY.md](DEPLOY.md)).

- **`ListDeliveries`** is a deployment-wide listing narrowed by filters
  ([Filtering lists](#filtering-lists)) on `created_at`, `namespace`,
  `run_id`, `webhook_id`, `event` (a subscribable event only) and
  `delivered` — every one decided from the record's key, so a body is read
  only for a record that matches. The window is the `created_at` interval the
  filters bound: `created_at<T` ends it (default now) and `created_at>=T`
  starts it — by default at the run's start when exactly one `run_id=` filter
  names a run the engine can read, else seven days before the end; a window
  with no instant in it is `InvalidArgument`. Page size defaults to 50, at
  most 200; the page token binds the filter set and carries the resolved
  window, so "now" does not move under a paged listing. A scan of more than
  100,000 keys before a page fills is `ResourceExhausted` ("narrow the
  created_at window or add filters"). The response echoes the resolved
  window, and `since_is_run_start` says whether its start is the run's
  `created_at` — the engine says so, and no client infers it from the
  window's width or a filter's spelling; the page token carries it.
- **`GetDelivery`** returns one record.
- **`RedeliverWebhook`** is one operator-initiated, synchronous POST — no
  retry ladder — to the subscription as it is now (a deleted subscription is
  `FailedPrecondition` and nothing is sent), signed with its **current**
  secret, carrying the original's event fields and timestamp but the run as
  it reads today, with `X-Ork-Redelivery-Of` naming the original. Its outcome
  is recorded as a new delivery; a POST that was sent but could not be
  recorded is `Unavailable` saying both. It is audited (`webhook.redeliver`).
  Nothing is queued: the operator is the retry policy.

With no store configured, all three answer `FailedPrecondition`. CLI:
`webhook deliveries [--filter F…] [--limit N] [--page-token T]` (it prints
`Window: <since> to <until>, newest first.`, with `(the run's start)` when
the response's `since_is_run_start` is true, and its next-page line repeats every
filter and `--limit`, never a window), `webhook delivery ID`, `webhook
redeliver ID` (exit 1 when the receiver did not accept it). UI: the
**Deliveries** panel on Admin → Webhooks — a caption with the resolved window
and scope, the filter form (`deliveryFilter` in the URL) with its CLI
equivalent, a delivered/failed pill, attempts and last
status, an expanded row with the signature, body digest and byte count ("body
not stored") and the attempt timeline, a per-webhook **Deliveries** link
(`deliveryFilter=webhook_id=<id>`), and
**Redeliver** behind a confirmation stating one signed POST, now, of today's
run, with the result inline.

## Control plane

### Automation documents

`AutomationService` plans and exports one namespace's automation document —
settings, schedules, triggers and webhooks, declared by name — and writes
nothing ([AUTOMATION.md](AUTOMATION.md)). An apply is the **client** sending
each actionable step's carried request to the existing write RPC it names, so
both clients apply through exactly the calls a person makes by hand, each
audited under its own action. Applying a directory, `--check` and
`--secrets-out` are CLI affordances over operations both clients have: the
Apply page applies Orkflows, and **Admin → Automation** (which respects the
namespace switcher) exports, plans and applies one document, re-planning on
accept and refusing a plan that moved since it was shown. Triggers and webhooks
are named and edited in place on the Triggers tab and the Webhooks page.

### Control-plane audit trail

Every call of a control-plane write RPC — succeeded or refused — writes one
immutable record to `metadata/audit/` before the response leaves the engine
([DEPLOY.md](DEPLOY.md), "Attributing control-plane writes", for the
operator's view). `AuditService.ListAuditRecords` reads them back from any
replica. The recorded actions are:

| Area | Actions |
| --- | --- |
| Orkflows | `orkflow.apply`, `orkflow.promote`, `orkflow.delete` |
| Runs | `run.cancel`, `run.await.resolve` (values by digest only) |
| Namespaces | `namespace.create`, `namespace.update` (hold, cap, budget, secret list and signer in its detail), `namespace.delete` |
| Triggers | `trigger.create`, `trigger.update`, `trigger.delete` |
| Schedules | `schedule.create`, `.update`, `.pause`, `.resume`, `.fire`, `.backfill`, `.delete` (never the input) |
| Webhooks | `webhook.register`, `webhook.update`, `webhook.delete`, `webhook.redeliver` (never a secret) |
| Deployment | `retention.update`, `image_policy.update` (keys by fingerprint), `orker.cordon`, `orker.uncordon`, `release.promote`, `release.ramp`, `cache.purge` |

Reads, `SubmitRun` (the run's own record and origin are the durable fact) and
the trigger and await-callback doors are never recorded. A `webhook.*` record
is deployment-wide (empty namespace) even for a scoped subscription; its
detail is `url=… events=[…]`, plus `scope=ns/orkflow/task` when scoped.

- It is a deployment-wide listing narrowed by filters ([Filtering
  lists](#filtering-lists)) on `at`, `namespace`, `action`, `principal`,
  `target`, `succeeded` and `error_code`, decided on each record's body.
  `namespace=payments` selects that namespace's writes, `namespace=default`
  the default namespace's, and `namespace=` (the empty value) the
  deployment-wide writes, which carry no namespace. Both clients print the
  scope in force.
- The window is the `at` interval the filters bound: `at<T` ends it (default
  the engine's `now`) and `at>=T` starts it (default seven days before the
  end); the response echoes the resolved `since`/`until`. Page size defaults
  to 50, at most 200; a page that would examine more than 5000 records is
  `ResourceExhausted` ("narrow the at window or add filters").
- The page token binds the filter set and carries the resolved window; a
  token presented with other filters is `InvalidArgument` ("page_token was
  issued for other filters; drop it and list from the first page").
- `principal` is what a gateway asserted in `Ork-Principal`, never
  authenticated; an empty one renders `unattributed`, and both clients print
  the "asserted, not authenticated" caption under every rendering, empty
  page included. `ORK_PRINCIPAL` makes `orkctl` assert one.

CLI: `admin audit [--filter F…] [--limit N] [--page-token TOKEN]`, e.g.
`admin audit --filter 'action^namespace.' --filter 'at>=2026-10-01T00:00:00Z'`;
it prints `Window: <since> to <until> (exclusive), newest first.` and, with no
namespace filter, `Every namespace unless a namespace filter narrows it.`. UI:
**Admin → Audit** — every namespace by default, the filter form (a
`namespace` filter narrows it) with its CLI equivalent,
`refused {code}` for a refusal, pagination, the caption.

### Orker inventory, cordon and drain

`ActivityService.ListOrkers` lists every live Orker from the cluster's poller
information plus each Orker's own answer, so any replica answers the same; an
Orker that could not be asked is `unreachable` with its reason and no numbers,
never idle. `CordonOrker` sets or clears a cordon, which lives only in the
Orker process. Semantics, exit codes and the `ORK_ORKER_ID` uniqueness rule
are [DEPLOY.md](DEPLOY.md), "Removing an Orker safely: cordon and drain".

CLI: `orker list [--filter F…]` (`--filter pool=build`), `orker get ID`
(an `orker_id=ID` filter: that Orker alone is asked), `orker cordon ID --reason TEXT`
(a blank reason is refused before any request), `orker uncordon ID`, `orker
drain ID [--reason] [--timeout] [--watch]` — a client-side loop: exit 0
drained, 4 timed out naming what remains, 1 unreachable, uncordoned or
restarted. Orker commands ignore `--namespace`. UI: **Admin → Pools**, the
filter form over the pool cards (`pool list [--filter F…]`), a
per-pool **Show Orkers** disclosure with a state pill (`ready`, `cordoned`,
`unreachable`), tasks, pins, last seen and open pins linking to their runs;
**Cordon** through a dialog that requires a reason and shows a server error in
place; one-click **Uncordon**.

### Releases

`SystemService.ListReleases`, `RampRelease` and `PromoteRelease` read and
change which release (one `ORK_BUILD_ID` of `ork` and `orker`) takes new runs
([DEPLOY.md](DEPLOY.md) §3–4). Any replica, of either release, answers the
same releases, roles, counts and blockers. A ramp or promotion is refused with
nothing written when the release could not carry the runs it would take — no
engine of it running, a pool in use with no uncordoned Orker of it, a pinning
pool with no pin host of it, or runs started before any release was current
still executing — naming every blocker; a routing that changed since the read
is refused. There is no override: an operator who must do what this refuses
uses the cluster's own commands ([DEPLOY.md](DEPLOY.md), "Underneath: the
cluster commands"). OrkStack starts and stops no process and stores no
rollout: the cluster's routing is the only record of which release is
current, runs always finish on the release that started them, and `status
--watch` and `wait` are loops in `orkctl`. Every write is a `release.promote`
or `release.ramp` audit record.

CLI: `release status [--watch] [--filter F…]` (`--watch --json` is
refused; the filters select release rows and are sent on every poll),
`release ramp
BUILD --percent N` (1–100; 0 stops the ramp), `release promote BUILD`,
`release rollback BUILD` (refused client-side unless the target is a previous
release), `release wait BUILD [--timeout]` — a client-side loop: exit 0
drained, 4 timed out naming every run and pin left, 1 otherwise. A count that
could not be read is `unknown`, never 0. Release commands ignore
`--namespace`. UI: **Admin → Releases** — the filter form over the release
rows, one row per release with its role
(current, ramping N%, previous, candidate), engines, Orkers, pools served,
runs, and state (`serving`, `ready`, `not ready (N blockers)`, `draining`,
`drained`), every blocker verbatim, a holds disclosure listing the runs still
executing and each open pin with its Orker; **Promote**, **Roll back**,
**Ramp…** and **Stop ramp** through a dialog that re-reads the release on
open, keeps accept disabled while blockers remain, and sends that read's
revision. Refresh is manual; nothing polls.

### Namespace policy

Each is a field of the namespace document, set with `namespace create|update`
or the **Admin → Namespaces** form, enforced at the one admission gate every
door shares (`SubmitRun` preparation: `run submit`, `run again`, the submit
dialog, a trigger invoke, a schedule fire), and audited through
`namespace.update`. A refusal mints no run id and takes no claim, and both
clients show it verbatim. Operator semantics are in [DEPLOY.md](DEPLOY.md).

| Policy | CLI | UI | Refusal |
| --- | --- | --- | --- |
| Hold and cap | `--hold REASON`, `--release-hold`, `--max-running N` | Hold and Max running columns; Hold… / Release hold; a hold banner on Orkflows, the dashboard and the submit dialog, with submit disabled | `failed_precondition` `NAMESPACE_HELD` / `MAX_RUNNING_REACHED`, with the reason and the fix |
| Budget | `--budget-cpu-seconds`, `--budget-gib-seconds`, `--budget-period`, `--budget-enforce[=false]`, `--clear-budget`; `stats usage` | Budget column and fields; dashboard Usage card; Admin → Usage ([STATS.md](STATS.md) §8) | `BUDGET_EXCEEDED` / `BUDGET_UNMEASURABLE`, with usage, budget, period and fix |
| Secret sources | `--allow-secret PATTERN…`, `--clear-allowed-secrets` | Allowed secrets column, one per line, `unrestricted` caption | `invalid_argument` at apply/validate/submit naming the task, secret and list; the Orker's own floor refuses with `SECRET_NOT_PERMITTED` before pulling |
| Required signer | `--require-signed-by cosign:KEY`, `--clear-require-signed-by` | Signed by column and a signer select | see image policy below |

`orkflow validate`, the Apply page and the editor show a policy refusal as a
field-anchored issue on the offending task.

**Listing.** `ListNamespaces` takes filters ("Filtering lists") on the name,
description, labels, default pool, hold reason, required signer, telemetry
sink and the created/updated times; `default` is matched like any other name.
With `page_size` 0 it answers every match in one response — what `namespace
list`, the namespace switcher and Admin → Usage ask for. A positive
`page_size` pages by name (`next_page_token`, bound to the filter set), which
is how Admin → Namespaces lists a registry of thousands; its pages are
presentation, as on Runs and Orkflows, and `namespace list [--filter F…]`
still prints every match. What a page costs depends on its filters: the names
come from the registry's name listing, so with no filter or only name filters
a page reads just the documents it shows (a page of 25 out of thousands is
one listing and 25 reads); a filter on any other field reads documents in
name order, eight at a time, until the page is full — at worst every
document after the cursor. Every document, `default`'s included, is read the
same way, so a `default` that was updated is listed as stored. Admin →
Namespaces renders the filter form with page size inside it, and a create in
the browser lands on `filter=name=<created>` on a first page, so its row is on
screen whichever page it sorts onto.

### Image policy

`SystemService.GetImagePolicy`/`UpdateImagePolicy` hold the deployment floor —
require-digest, allowed registry prefixes, and named cosign public keys a
namespace may require — in the shared store, so any replica enforces the same
policy ([DEPLOY.md](DEPLOY.md), "Image supply-chain policy"). Apply and
validate refuse a non-conforming `uses:` as a field-anchored
`invalid_argument`; `run submit`, `run again` and a trigger invoke refuse a
stored definition the policy now forbids with `failed_precondition`, naming
the task, image, rule and the command that reads the policy. The Orker
verifies a required signature itself and fails the task before pulling with
`IMAGE_NOT_PERMITTED`, `IMAGE_NOT_SIGNED` or `IMAGE_SIGNATURE_INVALID`
(non-retryable) or `IMAGE_SIGNATURE_UNAVAILABLE` (retried; never a pass). An
accepted run finishes on the policy it pinned. Removing a key a namespace
names is refused naming it.

CLI: `admin image-policy get`, `admin image-policy set [--require-digest]
[--allowed-registry PREFIX…|--clear-allowed-registries] [--signing-key
NAME=PATH] [--remove-signing-key NAME]`. UI: **Admin → Image policy**, one
form, deployment-wide caption, keys with fingerprints, refusals verbatim.

### Activity interfaces

An activity image may declare its `with:` interface in an OCI label
([ACTIVITY_IMAGES.md](ACTIVITY_IMAGES.md)). CLI: `activity list [--filter F…]` (an
`INTERFACE` column: `-`, `none`, `malformed`, `N keys`, `N ops`) and
`activity inspect <name-or-reference>`; `orkflow validate` prints its ERRORs
and WARNINGs, and `apply` prints the warnings. UI: **Admin → Activities**, the
filter form over the catalog, the same column with a disclosure of the declared table and an **Inspect a
reference** form; the Apply page and the editor show the `tasks[i].with.<key>`
issues, warnings kept after apply.

### Dashboard reads

Failure fingerprints (`stats failures`, the dashboard's **Fingerprints**
disclosure), the flake index (`stats flakes`, the **Stability** disclosure
and the run page's retry history), usage (`stats usage`, the **Usage** card
and Admin → Usage) and cache statistics are specified, numbers and parity
both, in [STATS.md](STATS.md).

## Baseline operator flows

| Capability | Contract both clients honour |
| --- | --- |
| Cancellation | `run cancel --reason`; the UI's cancel action takes an optional reason, disables duplicate requests and shows "cancellation requested" until the authoritative status changes; the reason survives and the terminal state comes from the server. |
| Version-safe submission | The version picker marks current; links from a definition or a run keep the pinned version, so an old version is submitted even after current moves. Idempotency key and empty-pool override (`--allow-empty-pool`) are explicit, advanced inputs; a repeated key deduplicates per the server contract. |
| Inputs and results | Run inputs/outputs, task values and metadata with copy and JSON export (`run get --json`); empty values are distinct from unavailable values. |
| Typed inputs | String/number/boolean controls with required/default hints; object and array use JSON editors, with a JSON alternative for the whole input. Switching modes preserves values; false/zero/empty string not dropped; invalid numeric/JSON data cannot silently coerce; unknown user-supplied keys are kept; unknown or complex declarations keep the JSON editor. The submitted JSON equals `run submit --input`'s. |
| Retained logs | `run logs` (task, attempt, item, `--from-seq` cursor with its attempt) and a full retained-log download that bypasses the pane's scrollback cap; older chunks can be browsed; search says it covers loaded lines; truncation notices stay visible; a failed download never looks complete. |
| Editor protection | Warn on navigation with unsaved YAML; import a local file; export the draft or source; conflict handling on apply; a successful save clears dirty state. |
| Run timing basics | Elapsed time from authoritative start/end, updating while running; missing timestamps render unknown; no invented retry schedule. |

## Parity exceptions

These are accepted exceptions, not gaps.

### Local activity build/test parity decision

- **`orkctl activity build`, `orkctl activity test` and `orkctl task run`**
  are local developer operations on the caller's filesystem and OCI runtime,
  with no deployment API ([OCI_ACTIVITY_CONTRACT.md](OCI_ACTIVITY_CONTRACT.md)
  §10.3–10.4). The UI presents them as local tooling with accurate copyable
  commands and prerequisites — for `task run`, a CLI-equivalent popover on a
  FAILED task card carrying exactly `orkctl --namespace '<ns>' 'task' 'run'
  '--from-run' '<run>' '--task' '<task>'`, with no input, secret or path.
  Copying a command is assistance, not browser execution. Do not mount a
  Docker socket into the UI or engine, accept arbitrary server host paths, or
  invent a privileged activity runner to manufacture parity; a browser
  execution equivalent needs its own accepted contract for build-context
  transfer, isolation, registry credentials, artifact custody and
  cancellation.

### CLI-only operations

- **`orkctl schedule backfill`** is CLI-only: it enqueues many fires at once
  and is an operator recovery tool. The UI shows backfilled runs like any
  other fire ([ORKFLOW_SCHEDULES.md](ORKFLOW_SCHEDULES.md)).
- **Applying a directory, `apply --check` and `--secrets-out`** are CLI
  affordances over operations the UI also performs one document at a time.
