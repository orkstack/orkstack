# OrkStack Orkflow DSL (v0)

An orkflow is a YAML document compiled into the `orkstack.v1.Orkflow` proto and
applied to the `ork` engine with `orkctl apply`.

## Top-level fields

| field         | required | description                                            |
|---------------|----------|--------------------------------------------------------|
| `namespace`   | no       | Slug scope; absent means the built-in `default`.        |
| `name`        | yes      | Slug identifier, unique within its namespace.           |
| `version`     | no       | Author-pinned version; defaults to the source's hash.  |
| `description` | no       | Human description.                                     |
| `labels`      | no       | `map[string]string` for organization/filtering, free-form — no key shape, entry count, or value size is enforced here. When telemetry is on, at most 32 of them (sorted by key) are exported as `ork.label.<key>` telemetry attributes, each key and value sanitized for that boundary (`docs/DEPLOY.md`, "Per-namespace policy"). |
| `inputs`      | no       | Declared input parameters (see below).                 |
| `outputs`     | no       | Declared return contract: name to template (see below). |
| `pins`        | no       | Declared task pins (see below).                        |
| `tasks`       | yes      | List of task nodes forming a DAG.                      |

`orkctl apply` also accepts **automation documents**, recognised by
`kind: Automation` (with `apiVersion: orkstack/v1` and a `namespace:`): one
file per namespace declaring its settings, schedules, triggers and webhooks by
name. No orkflow document can carry `kind` or `apiVersion` — the compiler
refuses an unknown key — so the two never collide. See
[`docs/AUTOMATION.md`](AUTOMATION.md).

### `namespace`

A namespace is a scope: an orkflow name is unique *within* one, so two
namespaces may each hold a `deploy` and never see each other's. Definitions and
runs are stored and indexed under it, and every read is scoped by it —
`orkctl orkflow list`, `orkctl orkflow get`, `orkctl run list` and run
submission all answer for one namespace and never for another's documents.

A run id is scoped the same way, and this is worth stating because an id looks
like an address that stands on its own. It does not: `orkctl run get`,
`orkctl run watch` and `orkctl run cancel` all address a run *inside* the
namespace the command acts in, and a run of another one is not found even when
its id is known. An id is not a secret — submit prints it, tickets quote it,
CI transcripts echo it — and cancel is a write, so holding one is not
authority over somebody else's tenant.

There are two ways to say which, and they must agree:

```bash
orkctl apply payments.ork.yaml              # the document's own `namespace:`
orkctl apply portable.ork.yaml -n payments  # a document that names none
```

A document that names `payments` applied with `-n staging` is refused: one of
the two is a mistake, and guessing which would store the definition where its
author will not look for it.

The namespace a command acts in is, most specific first: `--namespace`/`-n`,
then `ORK_NAMESPACE`, then the one saved by `orkctl namespace use`, then
`default`. Namespaces themselves are managed with
`orkctl namespace create|list|get|update|delete|use`; a namespace also carries
a `default_pool` and an `allowed_pools` list, which decide and bound where its
tasks run (see `pool` below).

### `version`

A version is a name for one document, and it is **immutable**: applying an edit
adds a version and never rewrites one, because a run pinned to a version reads
its definition long after it was applied.

```bash
orkctl apply deploy.ork.yaml     # unchanged file: no new version, nothing written
orkctl apply deploy.ork.yaml     # edited file:    a new version
```

`version:` is optional. When a document pins none, the compiler derives one
from the source itself: the sha256 of the very bytes that version is stored
with. So editing and applying produces a new version without anybody inventing
a number, and re-applying an unchanged file produces the same version and
writes nothing. A version names the document a reader opens, comments
included — a comment-only edit is a new version, because what is stored
changed.

Pinning one takes that decision back: the number is yours, and applying a
**changed** document under a version that is already stored is refused
(`already_exists`), because a run pinned to it is still reading those bytes.
The fix is a new version rather than a force — bump `version:`, or drop it and
let the source name one.

### Which version runs

Each orkflow has one movable pointer, its **current** version. A run that names
no version executes it:

```bash
orkctl orkflow versions deploy          # every version, newest first, CURRENT marked
orkctl orkflow promote deploy 1.4.0     # move the pointer
orkctl orkflow promote deploy 1.3.0     # roll back: the same command
orkctl orkflow diff deploy 1.3.0 1.4.0  # what changed between two versions
orkctl orkflow compat deploy 1.4.0      # what promoting 1.4.0 breaks (inputs: and outputs:)
orkctl orkflow callers deploy           # what starts or composes deploy, and what it pins
orkctl orkflow edit deploy              # $EDITOR round trip, applied as a new version
orkctl orkflow edit deploy --version 1.3.0   # start from an older version
```

Until somebody promotes, an orkflow runs its **most recently applied** version,
which is what it did before pointers existed — promoting nothing changes
nothing. Promoting to a version that was never applied is refused, so a pointer
never names a version that is not there.

A promotion decides what STARTS, never what is already running: a run in flight
carries its own resolved definition and finishes on the version it began with,
whichever way the pointer moves under it. `--version` on `orkctl run submit`
pins one run explicitly and ignores the pointer.

Two questions are reported separately, because they are two questions: the
**current** version is what runs, and the **newest applied** version — the
head — is where the edit lineage tips. `orkctl orkflow edit` starts from the
head by default; `--version` starts from any stored version instead.

Editing an old version is not a mistake to be prevented: it is how you fork
forward from it. The result is applied as a NEW version and becomes the newest
applied one — a stored version is never rewritten — and the **current** pointer
does not move, so what runs is unchanged until you promote. That is the whole
of it: applying is authoring, promoting is releasing.

Whichever version you start from, the apply carries the head your editor saw.
If somebody applies while your editor is open, the apply is refused
(`failed_precondition`, naming the head that was fetched and the head that
replaced it) instead of landing beside a version nobody looked at, and your
edit is kept in a file the refusal names. Re-read, diff, re-apply.

#### Promoting compares declared interfaces

`diff` answers "what text changed"; promoting asks a narrower question first:
**what will this break?** An orkflow's *declared interface* is its `inputs:`
(with `type`, `required` and `default`) and its `outputs:` (by name) — nothing
else. Tasks, images, `with:`, pools, pins, retries, timeouts, conditions and
output *templates* are implementation: an orkflow that rewrites every task but
keeps its inputs and outputs is **compatible**.

`orkctl orkflow promote` compares the target with the version that runs now
and gives every declared input and output a finding:

| Change | Verdict |
|---|---|
| input added, required with no default | breaking |
| input added, optional (or defaulted) | compatible |
| input removed | breaking |
| input retyped, or typed where it was untyped | breaking |
| input's type dropped | compatible |
| input now required (with no default) | breaking |
| input no longer required | compatible |
| input gains a default, or its default changes | compatible |
| input loses its default (and is not now required) | breaking |
| output removed (a rename is a removal and an addition) | breaking |
| output added, or unchanged | compatible |
| neither version declares `outputs:`, or only the newer one does | unknown |

An orkflow with no `outputs:` declares no return contract — a run's output is
then every leaf task's values, an accident of graph shape — so its output side
is **unknown**, never compatible. A version string is a name: `3.0.0` gets no
pass for being a major bump; the declarations decide.

A **breaking** promotion is refused (`failed_precondition`, naming each
breaking change) until you pass `--acknowledge-breaking`; **compatible** and
**unknown** promotions are never refused. Rolling back is the same command and
is gated alike: rolling back from a version that added an input removes it, so
an incident rollback across such a change needs the flag too. The pointer moves
only while it still names the version the comparison was made against: a
promotion that raced another one is refused rather than landed across a
comparison it never made.

**Applying is never refused** for a consumer's sake. A breaking version is
stored, with warnings at the exact fields (`inputs.region`, `outputs.url`) and
one at `version` saying what applying does; `orkctl orkflow validate` reports
the same warnings and still exits `0`. For an orkflow **nobody ever promoted**
applying *is* a release — it runs its newest applied version — and the warning
says so: promote the version you want to keep running before applying.

`orkctl orkflow compat NAME [FROM] TO` prints the findings (two arguments
compare from the current version; it exits `0` whatever it finds — `--json`'s
`verdict` is what a CI job reads). `orkctl orkflow callers NAME
[--all-versions]` lists every composing orkflow version, trigger and schedule
in the namespace that references it, with the version each pins — or
`current`, for the triggers and schedules a promotion moves. A composing
orkflow always pins a version (`uses: orkflow:<name>@<version>`), so it meets a
breaking change only when its author bumps the pin, and rule 32 then refuses
that apply on the exact field.

## Inputs

```yaml
inputs:
  url:
    type: string          # string | number | boolean | object | array
    default: "https://…"  # optional
    required: false        # default false when a default is present
```

Provided at submit time and referenced as `{{ .inputs.<name> }}`.

The schema above is YAML, like the rest of this document — but the VALUES a
run is submitted with are JSON, never YAML, and stay JSON in the recorded
output too. The DSL declares shape; the submission carries data. See
`docs/JSON.md` for why.

## Outputs

```yaml
outputs:
  digest: '{{ .tasks.sign.output.value }}'
  url: 'https://records.example/{{ .tasks.publish.output.value }}'
  channel: stable            # a value with no template action is a constant
```

`outputs:` is an Orkflow's declared **return contract**: a flat map from an
output name to a template. When a run's tasks have all finished, the engine
renders every template **once** and the rendered map **is** the run's output
(`Run.output`) — and it is exactly what an Orkflow composing this one receives
as its composed task's values (see "Composition"). Every value renders to a
string.

- A name is a template field name: `^[a-z][a-z0-9_]*$`, at most 128 bytes, so
  `.tasks.<task>.output.<name>` parses in the composing Orkflow.
- A template may read `.inputs.<declared>` and `.tasks.<id>.output…` of **any**
  task of the Orkflow — the outputs are rendered after every task is terminal,
  so no `needs:` closure applies — except a fan-out, whose items are files.
- The engine renders it, not an Orker, so the Orker's helpers (`artifact`,
  `env`, `sh`) are refused, `.tasks.<id>` may be followed only by `output`, and
  a `.tasks` index names its task with a string literal.
- A task that did not succeed contributes nothing, so a template that reads it
  fails the render. For a task that may be skipped, guard the read:
  `{{ with index .tasks "sign" }}{{ .output.value }}{{ end }}`.
- **A run whose outputs cannot be rendered fails** (`OUTPUTS_RENDER_FAILED`,
  `OUTPUTS_TOO_LARGE` over the deployment's values bound, or
  `OUTPUTS_UNAVAILABLE` when an upstream value cannot be read): every task
  keeps the status it earned, and the run is `FAILED`, never `SUCCEEDED`
  without the interface it declares.

An Orkflow that declares no `outputs:` (absent and `{}` are one value) keeps
the output it always had: every leaf task's inline values, keyed by task id —
and an Orkflow composing it receives `{}`.

## Tasks

```yaml
tasks:
  - id: fetch             # unique within the Orkflow
    uses: http            # catalog activity name, full OCI image reference,
                          # or orkflow:<name>@<version> (see "Composition")
    needs: []             # DAG edges list existing task ids; no cycles
    condition: ""         # optional templated gate
    timeout_seconds: 300  # optional container wall-clock timeout; default 300
    retry:                # optional Temporal retry policy
      maximum_attempts: 3
      initial_interval_seconds: 1
      backoff_coefficient: 2.0
      maximum_interval_seconds: 100
    pool: ""              # optional Orker pool; absent means the default pool
    pin: ""               # optional declared pin; absent means unpinned
    command: ["curl", "--version"] # optional image ENTRYPOINT/CMD override; argv
    env:                  # workload environment only; no platform state
      MODE: probe
    secrets:              # resolved to read-only files, never embedded values
      - name: api-token
        source: env:API_TOKEN
    resources:            # request; Orker policy supplies defaults and caps
      cpu: "2"
      memory: 4Gi
    network: none         # optional: `default` or `none`
    cancellation_grace_period_seconds: 30 # SIGTERM-to-SIGKILL window
    run_as_orker_user: false # opt in to overriding the image's USER
    with:                 # image-specific config, rendered into config.json
      op: get
      url: https://example.test/health
```

`uses:` is the only image selector. A catalog name is resolved at apply time;
a registry tag or digest may be written directly. In every case the applied
definition records an immutable digest. `uses:` cannot be templated. A
deployment may require every registry reference to be written with a digest
and bound the registries images may come from (`orkctl admin image-policy
get`), and a namespace may require a cosign signature (`orkctl namespace
get`); an image outside either is refused at validate and apply with the
field path `tasks[i].uses`, and again at submit.

`command` is a top-level argv override and never a shell string. `env` contains
only variables supplied to the workload; OrkStack's required context travels
as files. Each `secrets` item names the file exposed under
`/orkstack/input/secrets` and the Orker-specific source used to resolve it.
Secret values never enter this YAML or Temporal history. A namespace may bound
which `source:` values its tasks may name (`allowed_secrets`, shown by
`orkctl namespace get`), and a source outside that bound is refused at
validate, apply and submit.

`resources` uses `cpu` and `memory` request strings. Orker policy may refuse a
request above its cap but never silently raises or clamps it. `network` accepts
only `default` and `none`. Cancellation sends SIGTERM and waits
`cancellation_grace_period_seconds` before SIGKILL. `run_as_orker_user` is an
opt-in readability escape hatch: it overrides the image's own USER so output
files are owned by the Orker, and may therefore break images that require their
baked-in identity.

A task with no `pool:` runs on the deployment's default pool, which answers to
the name `main`. Writing `pool: main` is the same thing, always: `main` names
the default pool, not one fixed set of Orkers, so wherever an operator has
pointed the default, both spellings go there and a run reports both on the
same pool. Within a run, one pool name means one set of Orkers and one set of
Orkers has one name — so the pool a run reports a task on is a pool you can
write to reach those same Orkers. A task that names a pool with no live
Orkers is refused at submit rather than left waiting for one.

## Pins

A **pin** is a named group of tasks that execute on one Orker process and share
one read-write directory, `/orkstack/shared`, for the life of the group. Where
`pool:` says *which Orkers* a task may run on, `pin:` says *which one member of
that set*, for these tasks, together.

```yaml
name: build-and-test
version: "1.0.0"

pins:
  build:                       # a pin is declared once, by name
    timeout_seconds: 3600      # optional: the pin's whole lifetime

tasks:
  - id: clone
    uses: git
    pin: build                 # a task joins a pin by name, like `pool:`
    run_as_orker_user: true
    with: { op: clone, url: "{{ .inputs.repo }}", dest: /orkstack/shared/src }

  - id: compile
    uses: golang
    pin: build
    needs: [clone]             # `needs:` is unchanged and still required for order
    run_as_orker_user: true
    command: [sh, -c, "cd /orkstack/shared/src && go build -o /orkstack/shared/bin/app ./..."]

  - id: test
    uses: golang
    pin: build
    needs: [compile]
    run_as_orker_user: true
    command: [sh, -c, "cd /orkstack/shared/src && go test ./... > /orkstack/output/artifacts/report.txt"]

  - id: publish                # unpinned: runs wherever the pool sends it and
    uses: file                 # sees `test` only through its committed artifacts
    needs: [test]
    with: { op: checksum, src: '{{ artifact "test/report.txt" }}' }
```

The `uses:` names above are illustrative — a deployment's catalog decides what
they resolve to. For a pin written against the first-party activity images and
compiled by the test suite, see `examples/pinned-build.ork.yaml`.

`pins:` is a top-level map from pin name to a (possibly empty) options object;
`pins: { build: {} }` is legal. `pin:` on a task is a bare name and must name a
declared pin — a name nothing declared is refused at compile, exactly as a
`needs` entry that names no task is, so a typo cannot silently create a second,
one-task group. A pin's only option today is `timeout_seconds`, the whole
group's lifetime from the moment its first task is dispatched until its last
task ends; `0` or absent means derived, as the sum of every member task's own
wall-clock budget, which is the sequential worst case. The lifetime is declared
on the pin rather than on a task because it is a property of the group, and
asking which task's value wins is the shape of question the compiler exists to
refuse.

Every task of one pin must name the same pool and the same
`run_as_orker_user`. A pin cannot span two pools: it is one process out of one
pool's set. `/orkstack/shared` is `1777` with the sticky bit, like `output/`, so
two members running as different UIDs can each create files the other cannot
delete or overwrite; agreeing on `run_as_orker_user` removes the most common way
for a pin to break on its second task, and `run_as_orker_user: true` across a
pin is the recommendation.

A pin needs a host to open on, and it never moves once opened. A task pinned
into a pool whose Orkers host no pins is refused at submit, the way a pool with
no live Orkers is (`docs/DEPLOY.md` §1). If a pin's host is lost mid-run, its
unfinished tasks fail with an infrastructure error naming the pin and the run
fails: the alternative would be restarting them somewhere with an empty
`/orkstack/shared`, which is a wrong answer rather than a slow one.

**A pin imposes no ordering.** The `needs` graph is the one scheduling model,
and membership in a pin adds no edge to it: two tasks in a pin with no `needs:`
edge between them run concurrently, on the same Orker, sharing
`/orkstack/shared` concurrently — the author's own concurrency contract with
their filesystem, exactly as it would be for two processes on one machine. This
is the one place the familiar `workingDir` analogy misleads: Kestra serializes
the tasks that share one working directory, and OrkStack's pin does not.

`/orkstack/shared` is an additive convenience, never a replacement for the
commit protocol. It is visible only within one pin: an unpinned downstream
task — and a task in a *different* pin — sees a pinned task's work only through
its committed `values.json` and artifacts, which is unchanged and remains the
only cross-task channel the engine knows about. A pinned task is not required
to touch `/orkstack/shared` at all. The platform does not keep the two channels
consistent with each other, so a task that wants something under
`/orkstack/shared` to outlive the pin also writes it to
`/orkstack/output/artifacts/`, where it becomes an artifact like any other. A
retried attempt gets a fresh per-attempt workspace but the **same**
`/orkstack/shared`, as the previous attempt left it, so a step that mutates
shared state should be idempotent or attempt-aware (`task.json` carries
`task.attempt` and `idempotencyKey`) — the same at-least-once caveat that
already applies to a task's external side effects, now applied to the host's
own disk. Do not copy a secret into `/orkstack/shared`: that puts it on disk
for the pin's lifetime, the same warning that applies to `output/`.

## Awaited tasks

An **awaited task** declares `await:` instead of `uses:`. It starts no
container, holds no pool slot and reaches no Orker: when the run reaches it, the
task becomes **`AWAITING`** and the run waits — inside Temporal, with one timer
for its deadline — until something outside the run **resolves** it. A
resolution either **completes** the task, and its JSON body becomes the task's
`values.json`, or **rejects** it with a reason. Downstream tasks read a
completion exactly as they read any upstream output.

```yaml
tasks:
  - id: submit-training
    uses: ghcr.io/acme/databricks-submit:v2
  - id: training
    needs: [submit-training]
    await:
      timeout_seconds: 21600          # required, 1 s … 30 days
      callback: true                  # optional: mint a one-time callback token at submit
      schema:                         # optional: the `inputs:` vocabulary, checked on resolve
        model_uri: { type: string, required: true }
        auc:       { type: number }
  - id: evaluate
    needs: [training]
    uses: ghcr.io/acme/evaluate:v5
    with: { model: "{{ .tasks.training.output.model_uri }}" }
```

`await:` takes three keys:

- `timeout_seconds` — **required**, from 1 second to 30 days. The deadline
  counts from the moment the task starts awaiting. A task nobody resolves by
  then fails `AWAIT_TIMED_OUT` (category `TimedOut`) and the run fails with it.
  The cap is 30 days because an awaiting run is a live execution pinned to the
  release that started it (`docs/DEPLOY.md` §2): its deadline is also the
  longest it can keep that release's workers required. A longer business
  process is two runs.
- `callback` — optional. When `true`, submitting the run mints a one-time
  bearer token for this await so a caller outside the deployment's API can
  resolve it over plain HTTP (below).
- `schema` — optional: what a completion must carry, in the `inputs:`
  vocabulary — a map from key to `{type, required}`, `type` one of `string`,
  `number`, `boolean`, `object`, `array`. It declares **no `default`**: a
  completion's `values.json` is stored as the caller's exact bytes, and a
  default would make the stored object differ from what was sent. Keys the
  schema does not declare are accepted.

An awaited task takes only `id`, `needs`, `condition` and `await`; every key
that shapes a container (`uses`, `with`, `command`, `env`, `secrets`,
`resources`, `network`, `pool`, `pin`, `retry`, `timeout_seconds`,
`cancellation_grace_period_seconds`, `run_as_orker_user`) is refused by name.
An `artifact "<task>/<name>"` helper may not name an await — it produces no
artifacts — and `{{ .tasks.<await>.output.<field> }}` is how its values are
read. A `condition:` on an await is evaluated before it opens, and an upstream
skip propagates through it like any task. There are no retries: a rejection
fails the task with `AWAIT_REJECTED` (category `ActivityFailed`) and the run
fails fast, as for any task failure. A business "no" that should branch is a
*completion* with a value (`{"approved": false}`) read by a downstream
`condition:`.

**Resolving.** There are two doors, sharing every check after
authentication. Both store a completion's values before they deliver the
resolution, answer an identical repeat of the resolution that already won as
success without sending anything again, and store and send nothing when they
refuse. A resolution is accepted only while the task is `AWAITING`: one sent
while it is still `PENDING` is refused, and the caller retries once it is
awaiting.

- `orkctl run resolve <run> --task <await> [--values JSON | --values-file
  PATH | --reject --reason TEXT]`, the run page's Resolve and Reject actions,
  or any Connect client calling `RunService.ResolveAwait`. It is scoped to a
  namespace exactly as `orkctl run cancel` is, and works for every await,
  `callback: true` or not.
- `POST /v1/runs/{run_id}/awaits/{task_id}` with
  `Authorization: Bearer <callback token>` and the values as the body (an
  empty body means `{}`), or `POST …/{task_id}/reject` with
  `{"reason": "…"}`. It works only for an await that declares
  `callback: true`, with that await's own token, and answers `202` once the
  resolution is recorded in the run's history. The token is the capability:
  it resolves exactly the await it was minted for, in the run's own
  namespace, and nothing else.

**Who receives a callback token.** The token is disclosed once, to whoever
submitted the run — `orkctl run submit`/`run again` print it, the UI's submit
dialog shows it before it continues to the run, and a trigger's `201` carries
it — and only a salted digest of it is kept. A **scheduled** run's tokens are
disclosed to nobody; resolve its awaits with `orkctl run resolve`. A container
never receives a callback token. Do not set `callback: true` on an approval
whose approver must differ from whoever submits the run: the submitter
receives the token.

See `examples/awaited-approval.ork.yaml`.

## Fan-out (`each:`)

A **fan-out** is one task that runs once per element of an array: one
durable **item** per element, each its own activity with its own commit,
attempts, retries, timeout, logs and artifacts. It is one node of the DAG —
its dependents wait for every item — and the run's history carries one entry
per item, so the number of items is bounded by the author, up front.

```yaml
tasks:
  - id: list
    uses: ghcr.io/acme/list-parts:v1      # writes {"parts": [...]} to values.json
  - id: convert
    uses: ghcr.io/acme/convert:v3
    needs: [list]
    each:
      over: '{{ .tasks.list.output.parts }}'   # required: one reference to an array
      as: part                                 # optional, default "item"
      max_items: 500                           # required: the most items this task may expand
      max_parallel: 8                          # optional: at most 8 items at a time
    with:
      src: "{{ .each.part.uri }}"
      label: "part {{ .each.index }} of {{ .each.count }}"
  - id: merge
    uses: ghcr.io/acme/merge:v1
    needs: [convert]       # stages every item of convert as files
```

`each:` takes four keys:

- `over` — **required**: exactly one template action naming an array, in one
  of four shapes and nothing else:

  | Shape | Reads |
  | --- | --- |
  | `{{ .inputs.NAME }}` | a declared input of `type: array` |
  | `{{ .inputs.NAME.KEY… }}` | an array under keys of a declared `type: object` input |
  | `{{ .tasks.ID.output.KEY… }}` | an array in an upstream task's `values.json` |
  | `{{ index .inputs "NAME" "KEY" … }}` / `{{ index .tasks "ID" "output" "KEY" … }}` | either, for an id or key a Go template field cannot spell |

  The `index` form is needed for a hyphenated task id or key
  (`{{ index .tasks "list-parts" "output" "parts" }}`): that is
  `text/template`'s rule. Text around the action, a pipeline, a function
  other than `index`, a variable and a non-literal `index` argument are
  refused. A `.tasks` source must be in the task's `needs:` closure.

  The array is **counted once**, when the run reaches the task, and the count
  is recorded in the run's history: a retry, a replay or a later reader never
  re-counts it. An **input** array travels inside every item's activity input
  (the run input is part of it), so it counts against every item's input
  bound below; a large array belongs in an upstream task's `values.json`,
  where it costs each item one reference.
- `as` — the name an item is bound to under `.each`: `^[a-z][a-z0-9_]*$`,
  at most 64 bytes, never `index` or `count`. Absent means `item`.
- `max_items` — **required**, from 1 to 1000. More elements than this fail
  the task `EACH_TOO_MANY_ITEMS` before any item runs. The sum of `max_items`
  over every fan-out of one Orkflow is at most 1000 too (a deployment may
  lower that ceiling with `ORK_EACH_MAX_ITEMS`, `docs/DEPLOY.md` §1): the
  items of one run share one history budget.
- `max_parallel` — at most this many items run at once; absent or `0` means
  no bound of the task's own (the pool's Orkers are the bound).

**Inside an item.** A fan-out's `with:` may read `.each.<as>` (the element,
and keys under it), `.each.index` (0-based) and `.each.count`; the element is
also in `task.json` as `each.item`, with its exact JSON text, beside
`each.index`, `each.count` and `each.as` (`docs/OCI_ACTIVITY_CONTRACT.md`
§3.2). Every item has its own `idempotencyKey`,
`{namespace}/{run}/{task}/{index}`, unchanged across that item's retries.
`.each` is legal nowhere else: not in `condition:` (evaluated once, before the
items exist) and not in a task without `each:`.

**Downstream, items are files.** A task that names a fan-out in its **own**
`needs:` finds every item's values at
`/orkstack/input/upstream/<task>/<item>/values.json` and its artifacts at
`/orkstack/input/artifacts/<task>/<item>/<name>`, `<item>` zero-padded to five
digits so a sorted listing is item order; `task.json`'s
`inputs.eachUpstream` says how many there are, including zero. A transitive
dependent gets nothing staged — name the fan-out in `needs:` to read it. No
template may read a fan-out (`.tasks.<fan-out>` or `artifact "<fan-out>/…"`):
N items have no single value to substitute.

**Per-item bounds.** With `S` the Orkflow's declared total (the sum of its
`max_items`), every item is held to:

| Bound | Value | At S = 1000 | At S ≤ 64 |
| --- | --- | --- | --- |
| activity input (`with:`, run input, upstream refs) | `min(values_max_bytes, 16 MiB / S)` | 16 KiB | 256 KiB |
| output artifacts per item | `min(artifact_max_count, 16384 / S)` | 16 | 256 |

Apply refuses a fan-out whose own `with:`, `env`, `secrets` and resources
already exceed the input bound; the run input and upstream references are
checked per item when the run reaches it (`INPUT_TOO_LARGE`, naming the item).

**Failure is fail-fast.** The first item that fails — after its own retries —
fails the task with that item's error, prefixed `item 97 of 120 failed:`, and
the run with it; items in flight are cancelled, and items never dispatched
end `CANCELED` without running. An item retries alone: its siblings' commits
stand.

**Not supported in this version:** a fan-out may not be pinned (`pin:`), may
not fan out over another fan-out or name one in `needs:` (no nesting), and an
awaited task may not declare `each:`.

`orkctl run get` shows a fan-out as one row with a progress count;
`--expand <task>` lists its items, and `run logs`, `run artifacts`,
`run attempts`, `run timing`, `run inspect`, `run evidence export`,
`run artifact provenance` and `task run` take `--item N`. The run page draws
one card with a cell per item. See `examples/each-fan-out.ork.yaml`.

## Composition (`uses: orkflow:`)

A task may name another Orkflow instead of an image. Such a **composed task**
starts a **child run** of that Orkflow — its own run id, history, logs,
commits, events and archive record, linked to its parent by its origin
(`parent-run <parent id>`) — and waits for it. It reaches no Orker.

```yaml
tasks:
  - id: build
    uses: ghcr.io/acme/build:4
  - id: release
    uses: orkflow:release-block@2.1.0     # version REQUIRED, never floating
    needs: [build]
    with:                                  # the child run's input
      image: '{{ .tasks.build.output.image_digest }}'
  - id: announce
    uses: http
    needs: [release]
    with:
      op: post
      url: https://hooks.example/notify
      body: '{"shipped":"{{ .tasks.release.output.digest }}"}'
```

- **One pinned version.** `orkflow:<name>@<version>` — the version is
  required (`orkflow:<name>` alone is refused, naming the versions that exist);
  it is resolved at apply **in this Orkflow's own namespace**, and its
  definition digest is stamped into this Orkflow's definition, so this
  version pins the child's exact bytes and, through them, every descendant's.
  A version pointer never moves a running parent; `uses:` cannot be a
  template.
- **What it may carry:** `id`, `needs`, `condition`, `uses` and `with`.
  `command`, `env`, `secrets`, `resources`, `network`, `pool`, `pin`,
  `retry`, `timeout_seconds`, `cancellation_grace_period_seconds`,
  `run_as_orker_user`, `each` and `await` are refused by name: the child's own
  tasks declare where and how they run.
- **`with:` is the child's input**, rendered by the **engine**: values, and
  strings from templates. It reads `.inputs.<declared>` and its `needs:`
  closure like any `with:`, but the Orker's helpers (`artifact`, `env`, `sh`)
  are refused — a child run receives values, never files; pass a reference it
  can fetch (a registry digest, a URL). Apply checks it against the child's
  declared `inputs:`: a required input missing is an error, an undeclared key
  or a literal of the wrong type a warning.
- **The task's values are the child's declared outputs** (`outputs:`), every
  value a string, stored as this task's `values.json` — so a downstream
  container reads them exactly as it reads any upstream's, at
  `/orkstack/input/upstream/<id>/values.json`. A child that declares none
  gives `{}`. Apply refuses a read of an output the child does not declare.
- **Failure and cancellation.** A failed child fails the composed task with
  `CHILD_RUN_FAILED` and the child's own failure category, naming the child
  run; a child cancelled on its own cancels the parent; a cancelled parent
  cancels its running children and waits for them to end; a parent
  terminated from outside asks its children to cancel, so no child is left
  running. A composed task is never retried: its child's tasks carry their
  own retries and timeouts.
- **Bounds.** Composition is acyclic and at most `ORK_COMPOSE_MAX_DEPTH`
  levels deep (default and ceiling 4; a run is level 0). The whole tree is
  planned at the parent's submit — its pools, secret policy, image policy and
  fan-out cap — and a submission the deployment can no longer plan (a child
  deleted, a cap lowered, a pool forbidden) is refused before anything starts.
  Every definition of the tree travels in the run's plan, which is bounded by
  256 KiB.
- A child's callback awaits get tokens that are disclosed to no one: resolve
  them with `orkctl run resolve <child-run> --task <task>` or on the child
  run's page.

`orkctl run get <run> --tree` prints every child run under its parent, and
`orkctl run list --origin parent-run:<run>` lists one parent's children; a
child is read, watched and cancelled on its own id. `orkctl run logs` points a
composed task at its child's logs, and `orkctl task run` refuses one by name.
See `examples/release-block.ork.yaml` and `examples/composed-release.ork.yaml`.

## Templating

Task `with` values and `condition` are rendered with Go `text/template` against
this context:

- `.inputs.<name>` — submitted inputs.
- `.tasks.<id>.output.<field>` — a completed upstream task's output payload.
  `orkctl run plan` evaluates a `condition` for a given input without running
  anything.
- Helper `artifact "<taskId>/<artifactName>"` — resolves to a **local path** the
  Orker has materialized before the task runs.
- Helper `env "<NAME>"` — reads an allowlisted environment variable on the Orker.
- Helper `sh <value>` — quotes a rendered value for an explicitly requested
  shell boundary.

Only tasks listed (transitively) in `needs` are guaranteed to be resolvable;
referencing an unrelated task's output is a compile-time error.

Two kinds of template are rendered by the **engine** rather than an Orker: a
composed task's `with:` and an Orkflow's `outputs:`. They get **no helpers**
(`artifact`, `env` and `sh` are Orker-side), read `.tasks.<id>.output` only,
and name every task they read literally (rule 28).

## Validation rules (enforced by `internal/dsl`)

1. `namespace`, when present, is slug-formatted (`^[a-z][a-z0-9-]*$`); absent
   means `default`. The engine additionally refuses a namespace nothing
   registered, naming the ones that exist — the compiler has no registry to
   consult.
2. `name` present and slug-formatted (`^[a-z][a-z0-9-]*$`) within its namespace.
3. Task ids unique and slug-formatted.
4. Every `needs` entry references an existing task id.
5. The `needs` graph is acyclic (DAG).
6. A task has exactly one of `uses:` and `await:`. `uses` is a non-empty
   catalog activity name or OCI registry reference; an awaited task runs no
   image. A `uses:` beginning with `orkflow:` is a composed task (rule 26) and
   skips the catalog check — it would otherwise be taken for a registry
   reference because it contains `:` and `@`.
7. A task `pool`, when present, is slug-formatted.
8. Templates parse and only reference declared inputs and upstream tasks, and
   `.each` under rule 25.
9. Every `pin:` names a key of `pins:`; each pin name is slug-formatted.
10. Every task of one pin names the same pool, treating an absent pool and
    `main` as equal.
11. Every task of one pin has the same `run_as_orker_user`.
12. Every declared pin is used by at least one task. A declaration nothing
    references is a typo.
13. `pins.<name>.timeout_seconds` is not negative. `0` and absent are one
    value — both mean derived — so a literal `timeout_seconds: 0` is accepted
    and means the same thing as omitting the key; the compiled field is a plain
    integer in which the two cannot be told apart, and every rule the compiler
    applies must also hold for a request that never met this YAML.
14. When a task's resolved image declares an interface, `with:` supplies
    every key the image requires and names an op it declares; an undeclared
    key or a type that disagrees is reported as a warning, never refused.
    Enforced by `internal/api` at validate and apply, after resolution — the
    compiler has no registry to ask (`docs/ACTIVITY_IMAGES.md`, "Declaring the
    interface"). An image without the label is not checked.
15. An awaited task carries nothing but `id`, `needs`, `condition` and
    `await`: every field that shapes a container is refused by name, on the
    compiled message as well as in YAML, so a hand-built request meets the
    same rule.
16. `await.timeout_seconds` is present and between 1 and 2592000 (30 days).
17. Every `await.schema` entry is a mapping of only `type` (required; one of
    `string`, `number`, `boolean`, `object`, `array`) and `required`
    (optional, boolean). `default` is refused: an await's values are stored
    as the caller's exact bytes.
18. An `artifact "<task>/<name>"` helper may not name an awaited task, which
    produces no artifacts; read its values with `.tasks.<id>.output`.
19. `each.over` is one reference to an array, in one of the four shapes of
    "Fan-out". A `.tasks` source names a task in the fan-out's `needs:`
    closure; an `.inputs` source names a declared input of `type: array`
    (or `type: object` when keys follow it). A compiled `each.source` must
    equal what `over` parses to.
20. `each.as`, when present, matches `^[a-z][a-z0-9_]*$`, is at most 64
    bytes, and is neither `index` nor `count`.
21. `each.max_items` is present and between 1 and 1000, and the sum of
    `max_items` over every fan-out of the Orkflow is at most 1000; the error
    names every contributor. A deployment's `ORK_EACH_MAX_ITEMS` may lower
    the ceiling, enforced at validate, apply and submit by `internal/api`.
22. `each.max_parallel` is not negative and not above `max_items`; `0` and
    absent are one value (no bound of the task's own).
23. A fan-out is not pinned.
24. A fan-out's items are files, never template values: no `with:` or
    `condition:` references `.tasks.<fan-out>` or `artifact "<fan-out>/…"`,
    an `each.over` may not name a fan-out, and a fan-out may not name another
    fan-out in `needs:`.
25. `.each` (`.each.<as>…`, `.each.index`, `.each.count`, field or `index`
    form) is legal only in the `with:` of a task that declares `each:`;
    never in a `condition:`, and no other key of `.each` exists.
26. A `uses:` beginning with `orkflow:` is a composed task: it parses as
    `orkflow:<name>@<version>` (name a slug, version
    `^[A-Za-z0-9][A-Za-z0-9._-]*$` of at most 200 bytes, no template), and
    the compiled `Task.orkflow` agrees with it; such a task carries no image.
    It skips rule 6's catalog check.
27. A composed task carries nothing but `id`, `needs`, `condition`, `uses`,
    `with` (and its compiled `orkflow`): every execution field — `command`,
    `env`, `secrets`, `resources`, `network`, `pool`, `pin`, `retry`,
    `timeout_seconds`, `cancellation_grace_period_seconds`,
    `run_as_orker_user`, `each`, `await` — is refused by name.
28. Engine-rendered templates — a composed task's `with:` and every
    `outputs:` value — stay values: the `artifact`, `env` and `sh` helpers
    are refused, `.tasks.<id>` is followed only by `output`, `.each` is
    refused, and a `.tasks` index names its task with a string literal.
29. `outputs:` is a flat map: each name matches `^[a-z][a-z0-9_]*$` and is at
    most 128 bytes; each value is a template under rule 28 that may read
    declared inputs and the output of any task but a fan-out.
30. A fan-out's `each.over` may not name a composed task: its values are the
    child's declared outputs, which are strings and never an array.
31. A composed task's child resolves in this namespace — it exists, its
    pinned digest still holds, its tree is acyclic and at most
    `ORK_COMPOSE_MAX_DEPTH` levels deep, the namespace's pool and secret
    policy allow every descendant, and every definition of the tree fits a
    run's 256 KiB plan. Enforced by `internal/api` at validate and apply, and
    again at submit; a descendant that declares callback awaits is a warning.
32. A composed task honours its child's interface: every input the child
    requires (with no default) is supplied, and every
    `.tasks.<composed>.output.<key>` read anywhere in the document names an
    output the child declares (errors); an undeclared `with:` key or a
    literal of the wrong type is a warning. Enforced by `internal/api`.

Decoding is strict: a key the schema above does not define is refused, never
ignored, so a misspelled or unsupported key cannot surface later as a run that
behaves differently from its document.

`labels:` is free-form: no key shape, entry count, or value size is enforced
by the compiler. A namespace's own telemetry attributes remain bounded and
shaped (`ValidateAttributeMap`, `internal/dsl`: at most 16 entries, each key
`^[a-z][a-z0-9_.-]{0,63}$`, each value at most 256 bytes and free of control
characters) because they are operator/admin input rendered verbatim in
terminals and the UI; a label is the tenant's own document and an ordinary,
widely-used key like `Team` or `app.kubernetes.io/name` should not be refused
for not looking like an attribute.

When telemetry is on, a namespace's operator-set attributes and an orkflow's
own labels merge onto the same run's signals (`ork.ns.<key>` and
`ork.label.<key>`, respectively; `docs/DEPLOY.md`, "Per-namespace policy").
The merge is where labels are made safe as telemetry rather than where they
are refused as a document: at most 32 labels are exported, taken in
sorted-key order; each key is folded into the attribute alphabet
(`dsl.SanitizeAttributeKey` — lowercased, every byte outside
`[a-z0-9_.-]` replaced with `_`, a non-letter first character prefixed
`l_`, capped at 64 bytes) and each value is truncated to 256 bytes with
control characters stripped (`dsl.SanitizeAttributeValue`). Two label keys
that fold to the same sanitized key keep only the one that sorts first.

A slug is at most **200 bytes**, in every position above. The bound is half the
rule and not a detail of it: a namespace, an orkflow name and a version are
path components of a stored object key, a pool becomes an execution channel,
and a pin name becomes a directory component on the Orker, so a longer name is
one the compiler could accept and nothing could store. It is the same 200 the orkflow store and the namespace registry enforce
(`pkg/artifacts`), and the two are held together by measurement rather than by
a shared constant: the artifact store is a public package and the compiler is
not, so the store does not depend on the compiler and does not drag it into
the build of every program that stores an artifact. A test inside
`pkg/artifacts` imports the compiler and fails on any disagreement, in either
direction — a test dependency costs an importer nothing.

## Example

See `examples/hello.ork.yaml` for a fetch → transform → checksum pipeline that
uses the first-party `http`, `shell`, and `file` OCI activity images with
artifact hand-off. Every shipped example is compiled by
`TestCompile_AcceptsEveryShippedExample`.
