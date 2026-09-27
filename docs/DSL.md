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
| `pins`        | no       | Declared task pins (see below).                        |
| `tasks`       | yes      | List of task nodes forming a DAG.                      |

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

## Tasks

```yaml
tasks:
  - id: fetch             # unique within the Orkflow
    uses: http            # catalog activity name or full OCI image reference
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
definition records an immutable digest. `uses:` cannot be templated.

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

## Templating

Task `with` values and `condition` are rendered with Go `text/template` against
this context:

- `.inputs.<name>` — submitted inputs.
- `.tasks.<id>.output.<field>` — a completed upstream task's output payload.
- Helper `artifact "<taskId>/<artifactName>"` — resolves to a **local path** the
  Orker has materialized before the task runs.
- Helper `env "<NAME>"` — reads an allowlisted environment variable on the Orker.
- Helper `sh <value>` — quotes a rendered value for an explicitly requested
  shell boundary.

Only tasks listed (transitively) in `needs` are guaranteed to be resolvable;
referencing an unrelated task's output is a compile-time error.

## Validation rules (enforced by `internal/dsl`)

1. `namespace`, when present, is slug-formatted (`^[a-z][a-z0-9-]*$`); absent
   means `default`. The engine additionally refuses a namespace nothing
   registered, naming the ones that exist — the compiler has no registry to
   consult.
2. `name` present and slug-formatted (`^[a-z][a-z0-9-]*$`) within its namespace.
3. Task ids unique and slug-formatted.
4. Every `needs` entry references an existing task id.
5. The `needs` graph is acyclic (DAG).
6. `uses` is a non-empty catalog activity name or OCI registry reference.
7. A task `pool`, when present, is slug-formatted.
8. Templates parse and only reference declared inputs and upstream tasks.
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
