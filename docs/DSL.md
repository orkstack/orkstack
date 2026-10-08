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
| `labels`      | no       | Free-form `map[string]string` for organization and filtering (see "Labels"). |
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

Until somebody promotes, an orkflow runs its **most recently applied** version.
Promoting to a version that was never applied is refused, so a pointer
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
    required: false       # optional, default false
```

Provided at submit time and referenced as `{{ .inputs.<name> }}`. An input is
**required** exactly when it says `required: true` and has no `default`; a
declaration with a default is never required. A composing task's `with:` check
(rule 32) and the promote gate ("Promoting compares declared interfaces") both
read it that way.

Submit fills every omitted input that has a `default` and checks nothing
else: it does not refuse a missing required input or a value of another type.
A task whose template reads an input nobody supplied fails `RENDER_FAILED`
(`ContractViolation`) rather than running with an empty string.

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

An Orkflow that declares no `outputs:` (absent and `{}` are one value) has no
return contract: its run's output is every leaf task's inline values, keyed
by task id — an accident of graph shape that adding a task changes — and an
Orkflow composing it receives `{}`.

## Tasks

```yaml
tasks:
  - id: fetch             # unique within the Orkflow
    uses: http            # catalog activity name, full OCI image reference,
                          # or orkflow:<name>@<version> (see "Composition")
    needs: []             # DAG edges list existing task ids; no cycles
    join: all_succeeded   # optional: none_failed, any_failed or all_done (see "Joins and failure handlers")
    condition: ""         # optional templated gate
    timeout_seconds: 300  # optional container wall-clock timeout; default 300
    retry:                # optional; absent means exactly one attempt
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
    cache: { enabled: true } # optional: reuse an identical earlier result (see "Caching")
    with:                 # image-specific config, rendered into config.json
      op: get
      url: https://example.test/health
    # each:, await: — see "Fan-out" and "Awaited tasks"
```

`retry:` absent (or `maximum_attempts` of 0 or 1) means one attempt — never
Temporal's unbounded default. Within `retry:`, an unset interval is 1 s, an
unset coefficient 2.0 and an unset maximum interval 100 s.

`uses:` is the only image selector. A catalog name is resolved at apply time;
a registry tag or digest may be written directly. In every case the applied
definition records an immutable digest, so a moved tag never changes a stored
version. `uses:` cannot be templated: an image chosen at run time could not be
pinned at apply.

An image is also subject to the deployment's image policy (`orkctl admin
image-policy get`): `require_digest` refuses a registry reference written as a
tag (a catalog name is exempt — the catalog is the operator's own
configuration), `allowed_registries` bounds where images may come from, and a
namespace may require a cosign signature (`require_signed_by`, shown by
`orkctl namespace get`). An image outside the policy is refused at validate
and apply on `tasks[i].uses`, again at submit, and by the Orker before it
pulls (`IMAGE_NOT_PERMITTED`, `IMAGE_NOT_SIGNED`, `IMAGE_SIGNATURE_INVALID`).
See `docs/DEPLOY.md`, "Image supply-chain policy".

When the resolved image declares its `with:` interface (the
`org.orkstack.activity.interface` label), validate and apply check `with:`
against it (rule 14): a missing required key or an undeclared `op` is an
error, an undeclared key or a literal of the wrong type a warning, and a
templated value is never typed. An image without the label is not checked.

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
Orkers is refused at submit rather than left waiting for one
(`orkctl run submit --allow-empty-pool` overrides that), and a namespace's
`allowed_pools` bounds which pools its tasks may name.

## Joins and failure handlers (`join:`)

A task's optional `join:` says when it may start, decided from how its
`needs:` ended. It takes exactly one of four words, on every task kind —
container, awaited and composed:

| `join:` | The task starts when its needs are all terminal and… | Otherwise it is `SKIPPED`, with |
|---|---|---|
| `all_succeeded` *(default)* | …every need `SUCCEEDED` | `upstream task "<need>" was skipped` |
| `none_failed` | …none `FAILED` or `CANCELED`, and at least one `SUCCEEDED` | `join none_failed: every need was skipped (<needs>)` |
| `any_failed` | …at least one `FAILED` (after its retries) | `join any_failed: none of its needs failed (<needs>); it runs only when one of them fails` |
| `all_done` | …whatever they did | — |

`join: all_succeeded` compiles to no join at all: the same definition and
digest as a task that writes none. A task's `condition:` still applies, after
the join test, unchanged.

The set is closed on purpose. How a need ended is a fact only the interpreter
holds, so the decision belongs to it — but as a lookup, not a language: an
expression in `join:`, or a need's status exposed to `condition:`, would be a
second language over the run's record and would change what `.tasks.<id>`
means to every existing condition (it holds only succeeded ancestors). The
words are predicates over **this task's own needs**, not hooks: a handler is
not "on run failure", and `all_done` does not mean "even when cancelled".

**Re-joining branches.** `none_failed` closes an if/else: "if A then B else C,
then D" is `D: {needs: [B, C], join: none_failed}` — one of B and C is always
skipped, and D runs after whichever ran. Skips propagate along every
`all_succeeded` edge; a `none_failed` task stops the propagation unless every
one of its needs was skipped.

```yaml
- id: decide                      # the decision is a task's value
  uses: shell
  with: {op: eval, command: ["printf", "%s", "{{ .inputs.route }}"]}
- id: canary
  uses: shell
  needs: [decide]
  condition: '{{ eq .tasks.decide.output.value "canary" }}'
  with: {op: exec, command: ["echo", "canary rollout"]}
- id: full
  uses: shell
  needs: [decide]
  condition: '{{ eq .tasks.decide.output.value "full" }}'
  with: {op: exec, command: ["echo", "full rollout"]}
- id: verify
  uses: shell
  needs: [canary, full]
  join: none_failed               # one branch was skipped; re-join after the other
  with: {op: exec, command: ["echo", "verified"]}
```

**Failure handlers.** A task whose join is `any_failed` or `all_done` is a
**failure handler**: the only kind of task that starts in a run that is
already failing. A handler cleans up, notifies or compensates; it never rescues
the run. It covers only the needs it lists — `rollback: {needs: [deploy], join:
any_failed}` does not run when `build` fails, because then `deploy` never ran.
An `any_failed` handler on another handler is an escalation ("page someone if
the rollback failed"); an `all_done` handler on one is a step after it,
whatever it did.

**How a run ends.**

- It *succeeds* when every task is decided and none failed. An `any_failed`
  handler of needs that all succeeded is skipped; an `all_done` handler runs.
- It is *canceled* when an operator cancels it before anything failed:
  nothing new starts — not even a failure handler — and every task still
  running, handlers included, is stopped with SIGTERM. A pending handler is
  recorded `CANCELED` with `the run was canceled; a canceled run starts no
  failure handler`. Cleanup at cancellation is the container's own SIGTERM
  handling: a "cancel, then run the cleanup tasks" mode would leave nothing
  but an external terminate to stop a slow or stuck handler.
- It *fails* otherwise. Fail-fast stops every task that is not a failure
  handler — in an Orkflow that declares a handler, a never-started one is
  `CANCELED` with `the run is failing (task "<cause>" failed) and this task is
  not a failure handler` — then the failure handlers run to their own
  outcomes, and the run ends **`FAILED` with the original failure's reason**.
  A handler's success never changes that; a handler's failure is recorded on
  the handler, stops nothing else and never replaces the run's reason. A
  handler already running when the failure happened (an `all_done` handler
  admitted before it) keeps running.
  While the handlers run the run is **failing**: its status is still
  `RUNNING`, its error is already set, and its event stream carries one
  `TASK_LOG` with no task id: `run failing after task "<cause>" failed;
  waiting for its failure handlers: <handlers>`.
- An operator who cancels a failing run stops its handlers (pending ones are
  recorded `the run was canceled while its failure handlers ran`); the run
  still ends `FAILED` with the original reason. A run terminated outside
  OrkStack runs no workflow code: it reads `CANCELED`, with the failing error
  it carried kept.

**A cancelled need is not a failed one.** A task fail-fast stopped while it
ran beside the one that failed is `CANCELED`, never `FAILED` — so `any_failed`
does not fire for it, although it may have changed something outside OrkStack
before it stopped (activities are at-least-once). A task fail-fast stopped
before it started is `CANCELED` too.

**What a failure handler receives.** A handler is an ordinary task in every
other respect — its own image, `with:`, pool, secrets, commit, attempts and
logs — and, like any task, **a handler with no `retry:` has exactly one
attempt**: an Orker lost under it records it `FAILED`
(`ORKER_HEARTBEAT_TIMEOUT`). A compensation that must survive a lost Orker
declares `retry:` and is written to be safe to run twice, keyed on its stable
`idempotencyKey`. What it is handed differs in three ways, set only for a
failure handler so every other task's input is unchanged:

1. **How its needs ended, as a file.** `/orkstack/input/task.json` carries a
   `join` object: the policy and, for each need in written order, its
   `taskId`, `status` (`SUCCEEDED`, `FAILED`, `SKIPPED`, `CANCELED`) and, when
   the need recorded one, its `error` (`code` ≤ 128 bytes, `category`,
   `message` ≤ 1024 bytes, `details` ≤ 256 bytes; a cut text ends in `…`).
   `CANCELED` carries an error (`TASK_CANCELED`) when the need was stopped in
   flight, and none when it never started.
2. **The failed needs' files.** A direct need that `FAILED` after writing
   output artifacts has them staged like any upstream's, so `task.json`'s
   `inputs.artifacts` lists them with `producer` set to the need. A need that
   failed before writing a file — an image that could not be pulled, a
   container killed at its timeout — has none. **Read a failed need's files
   from `task.json`'s `inputs.artifacts`**, where a missing file is a missing
   entry; the `artifact` helper fails the render for a name that is not staged.
3. **Succeeded ancestors' values, as always.** A skipped, cancelled or failed
   ancestor contributes no values.

**The saga idiom.** A condition — on any task — still sees only succeeded
ancestors, so `{{ if index .tasks "reserve-flight" }}true{{ end }}` is the
deterministic "did that succeed?" test. Choose the form by whether the step a
compensation undoes can be running when another forward step fails:

- *Forward steps run in sequence:* when one fails, every earlier one has
  `SUCCEEDED` and every later one never started. The compensation is
  `join: any_failed` over the forward steps plus a condition that the step it
  undoes succeeded:

  ```yaml
  - id: cancel-hotel
    uses: ghcr.io/acme/booking:3
    needs: [reserve-hotel, reserve-flight, charge-card]
    join: any_failed
    condition: '{{ if index .tasks "reserve-hotel" }}true{{ end }}'
    retry: {maximum_attempts: 3}
    with: {op: cancel-hotel}
  ```
- *Forward steps run in parallel:* fail-fast stops a step still running and
  records it `CANCELED`, so the condition above is false for it and
  `any_failed` does not fire on its account. Its compensation is `join:
  all_done` over the forward steps and decides in the container from
  `task.json`'s `join.needs[].status`: it undoes its step when that step may
  have run (`SUCCEEDED`, `FAILED`, or `CANCELED` with an `error`) **and** the
  run is failing (some need `FAILED` or `CANCELED`), and otherwise exits 0
  having done nothing — which is what it does in a run where every forward
  step succeeded, since an `all_done` handler runs there too:

  ```yaml
  - id: release-capacity
    uses: shell
    needs: [reserve-capacity, provision-dns]
    join: all_done
    retry: {maximum_attempts: 3}
    with:
      op: script
      interpreter: sh
      script: |
        set -eu
        j=/orkstack/input/task.json
        failing=$(jq '[.join.needs[] | select(.status == "FAILED" or .status == "CANCELED")] | length' "$j")
        mine=$(jq -r '.join.needs[] | select(.taskId == "reserve-capacity") | if .status == "SKIPPED" or (.status == "CANCELED" and .error == null) then "no" else "yes" end' "$j")
        if [ "$failing" -gt 0 ] && [ "$mine" = yes ]; then echo "releasing capacity"; fi
  ```

In both forms the compensation's `needs:` lists every forward step whose
failure should undo its step: a handler decides only on its own needs. A third
form — for a request that may have taken effect although it failed — needs the
step it undoes and every step after it, with no condition and an idempotent
undo: see `docs/ORKFLOW_PATTERNS.md`, "Undo what was done: failure handlers and
sagas", and `examples/order-saga.ork.yaml`.

**Reading an ancestor a join can leave absent.** Without a join, every task of
a needs closure has succeeded whenever a task runs. A join breaks that, and not
only for the joined task's direct needs: an ordinary `deploy` after a
`none_failed` `verify` runs whether `canary` or `full` ran, and the one that
was skipped is absent from `.tasks`. Reading it — `.tasks.canary.output.x`,
`index .tasks "canary" "output" "x"`, or `artifact "canary/…"` — fails the
task. Guard the read: `{{ with index .tasks "canary" }}{{ .output.x }}{{ end }}`
(or `{{ if index .tasks "canary" }}…{{ end }}`; the body runs only when the
task is present, never the `else`). Apply warns, never refuses, about every
unguarded read of an ancestor a join can leave absent (`tasks[i].condition`,
`tasks[i].with…`). Only those two exact pipelines are recognised as a guard:
`{{ if and (index .tasks "a") … }}` still warns — a known false positive. For
a failure handler an `artifact` read always warns, guarded or not: read the
file from `task.json`'s `inputs.artifacts` instead.

**A broken condition.** In an Orkflow that declares a failure handler, a task
whose `condition:` cannot be evaluated is recorded `FAILED` with
`CONDITION_FAILED` (category `ContractViolation`) — so its own `any_failed`
handlers fire — and the run fails with the evaluation error. In every other
Orkflow the task is recorded `CANCELED`.

**Planning both paths.** `orkctl run plan --assume-failed TASK` plans the
failure path (see "Planning a run").

**Not supported:** a failure handler may not be pinned, fan out or be cached
(rule 39); no ordinary task may need an `any_failed` task (rule 40). There is
no loop, no `continue_on_error` and no Orkflow-level `on_failure:` hook: the
graph stays a DAG that `run plan` sees whole, a handler lists the needs it
handles (the DAG is the one place edges are declared), and a failure that
should not fail the run is the workload's to report as data, which a
`condition:` reads. See `examples/branch-and-recover.ork.yaml`.

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

`git` is the first-party `activity-git` image (`images/activity-git/README.md`,
"`dest` — a working tree for a pin"); `golang` is illustrative — a
deployment's catalog decides what such a name resolves to. For a pin written
against the first-party activity images and compiled by the test suite, see
`examples/pinned-build.ork.yaml`. `examples/pinned-checkout.ork.yaml` clones
with the `git` activity into a pin.

`pins:` is a top-level map from pin name to a (possibly empty) options object;
`pins: { build: {} }` is legal. `pin:` on a task is a bare name and must name a
declared pin — a name nothing declared is refused at compile, exactly as a
`needs` entry that names no task is, so a typo cannot silently create a second,
one-task group. A pin's only option is `timeout_seconds`, the whole
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

Validate and apply warn (field `pins.<name>`) about a pin whose every task
leaves `run_as_orker_user` false: each member then keeps its image's own
`USER`, and rule 11 cannot compare UIDs that come from the images rather than
the document.

**Lifecycle.** A pin opens lazily, when the first of its tasks is ready and
classified to run — never at run start — so a pin whose every task is skipped
by `condition:` or an upstream skip never occupies a host. While it opens, its
ready tasks stay `PENDING`; unpinned branches of the run keep advancing. The
pin closes when every member is terminal: the engine asks the pin host to
remove the pin's directory, then frees the host's slot, and the run does not
report its own outcome until every pin it opened has closed. A run is
fail-fast, pinned or not: a member that fails fails the run, which cancels its
running siblings and marks every not-yet-started task `CANCELED`. A member's
own `retry:` retries on the same host, so a retry is not a pin failure and the
pin stays open.

**What only a pin can fail with.** Each is non-retryable, names the pin, and
fails the run:

| code | category | when |
|---|---|---|
| `PIN_UNAVAILABLE` | `InfrastructureError` | every pin host of the pool stayed full (or cordoned) for the whole creation timeout, a fixed **5 minutes**. Only the task whose readiness asked for the pin fails; it never reached an Orker, so its `scheduled_at` and `started_at` stay unset. |
| `PIN_HOST_LOST` | `InfrastructureError` | the host stopped heartbeating the pin (noticed within the 30-second heartbeat timeout) before the pin's lifetime ran out. Every member running at that moment is `FAILED` with this code — never `CANCELED`, because the platform lost it — and members not yet started are `CANCELED`. A member that already committed stays `SUCCEEDED`. |
| `PIN_TIMEOUT` | `TimedOut` | the pin was open for at least its lifetime (`timeout_seconds`, or the derived sum). Members still running are stopped with it, as above. |

A pool with **no** pin host at all is refused at submit instead (above), so
`PIN_UNAVAILABLE` only ever means saturation. The creation timeout is a
constant, not a setting: workflow code may not read process configuration,
and an author has no better information about the number than the platform
does. `PIN_HOST_LOST` and `PIN_TIMEOUT` reach the engine as the same event —
the pin's hold on its host ended — and are told apart by the elapsed time
against the pin's lifetime, which is deterministic on replay.
A member that fails on its own (a non-zero exit) leaves the pin open and
takes the ordinary failure path. `INVALID_PIN` is reserved for a hand-built
definition naming a pin it does not declare, which the compiler refuses.

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

An awaited task takes only `id`, `needs`, `condition`, `join` and `await`; every key
that shapes a container (`uses`, `with`, `command`, `env`, `secrets`,
`resources`, `network`, `pool`, `pin`, `retry`, `timeout_seconds`,
`cancellation_grace_period_seconds`, `run_as_orker_user`, and also `each` and
`cache`) is refused by name. While a task awaits, its run is `RUNNING`;
`AWAITING` is a task status only. An `artifact "<task>/<name>"` helper may not
name an await — it produces no
artifacts — and `{{ .tasks.<await>.output.<field> }}` is how its values are
read. A `condition:` on an await is evaluated before it opens, and an upstream
skip propagates through it like any task. There are no retries: a rejection
fails the task with `AWAIT_REJECTED` (category `ActivityFailed`) and the run
fails fast, as for any task failure — after its failure handlers (see "Joins
and failure handlers"), which an await may itself be (`join: any_failed`:
"approve the rollback?"). A business "no" that should branch is a
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
  namespace, and nothing else, and it grants no read.

A completion's values are bounded by the deployment's values limit and stored
as an immutable object; they reach a downstream container only as its
upstream `values.json`. Neither door queues, retries or rate-limits: one
request is one delivery, and what to expose, to whom and how fast is the
operator's gateway. A resolution records the `Ork-Principal` the gateway
asserted (`resolved_by`), as asserted, not authenticated — `ResolveAwait`
has `CancelRun`'s trust model.

**Who receives a callback token.** The token is disclosed once, to whoever
submitted the run — `orkctl run submit`/`run again` print it, the UI's submit
dialog shows it before it continues to the run, and a trigger's `201` carries
it — and only a salted digest of it, in the run's own start record, is kept.
A **scheduled** run's tokens are disclosed to nobody; resolve its awaits with
`orkctl run resolve`. A container never receives a callback token: getting one
there would mean storing the plaintext or sharing a key with every Orker. Do
not set `callback: true` on an approval whose approver must differ from
whoever submits the run: the submitter receives the token.

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
  items of one run share one history budget. A run's history is bounded by the
  cluster, and a run that crossed that bound would be terminated or stuck, so
  the bound is declared up front and enforced before anything runs.
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
`max_items`), every item is held to bounds derived from `S` alone, so they are
known at apply and identical on replay:

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
stand. The run fails after its failure handlers; an `any_failed` handler of the
fan-out is handed the node's error (`item 97 of 120 failed: …`) and no item
files. A `none_failed` fan-out whose `each.over` task did not succeed fails
`EACH_OVER_MISSING`: such a fan-out reads its array from an input or from a
task that always runs.

A fan-out's own row fails, before any item runs, with one of:
`EACH_OVER_MISSING` (the array's source is absent), `EACH_OVER_NOT_ARRAY` (it
is not a JSON array, or the committed `values.json` is not JSON) and
`EACH_TOO_MANY_ITEMS` (more elements than `max_items`) — all
`ContractViolation`, never retried — or `EACH_EXPANSION_FAILED`
(`InfrastructureError`: the committed array could not be read, was larger
than the values bound, or did not match its recorded digest). `INVALID_EACH`
is reserved for a hand-built definition the compiler would have refused.

**Not supported:** a fan-out may not be pinned (`pin:`) — N concurrent items
writing one `/orkstack/shared` multiplies the hazard of "A pin imposes no
ordering" — may not fan out over another fan-out or name one in `needs:` (no
nesting, no matrix: a second dimension is a task that writes the flattened
array), and an awaited or composed task may not declare `each:`. There is no
per-item condition and no continue-on-error: `condition:` gates the whole node
once, and an array that must be filtered is filtered by the task that writes
it.

`orkctl run get` shows a fan-out as one row with a progress count;
`--expand <task>` lists its items, and `run logs`, `run artifacts`,
`run artifact download`, `run artifact provenance`, `run attempts`,
`run timing`, `run inspect`, `run evidence export` and `task run` take
`--item N`. The run page draws
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
    uses: shell
    needs: [release]
    with:                                  # the child's outputs, read as values
      op: exec
      command: ["echo", "shipped {{ .tasks.release.output.digest }}"]
```

- **One pinned version.** `orkflow:<name>@<version>` — the version is
  required (`orkflow:<name>` alone is refused, naming the versions that exist);
  it is resolved at apply **in this Orkflow's own namespace**, and its
  definition digest is stamped into this Orkflow's definition, so this
  version pins the child's exact bytes and, through them, every descendant's.
  A version pointer never moves a running parent; `uses:` cannot be a
  template — a child is never chosen by input. Composition never crosses
  namespaces: one team's apply must not be able to break another team's run.
- **What it may carry:** `id`, `needs`, `condition`, `join`, `uses` and `with`.
  `command`, `env`, `secrets`, `resources`, `network`, `pool`, `pin`,
  `retry`, `timeout_seconds`, `cancellation_grace_period_seconds`,
  `run_as_orker_user`, `each`, `await` and `cache` are refused by name: the
  child's own tasks declare where and how they run (and may be cached). There
  is no retry or timeout of the child as a whole: a run id is single-use, so a
  retried child would need a new identity, and a timeout would stop the child
  without running its workflow code.
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
  own retries and timeouts. A failed child fires the parent's `any_failed`
  handlers after the child's own handlers ran, and the parent fails after
  them; a composed failure handler starts its child in a failing parent
  (compensation as a reusable Orkflow). The composed task's other codes:
  `CHILD_INPUT_RENDER_FAILED` and `CHILD_INPUT_TOO_LARGE` (`ContractViolation`:
  the child's input could not be rendered, or exceeds the values bound);
  `CHILD_INPUT_UNAVAILABLE` (an upstream value could not be read),
  `CHILD_START_FAILED`, `CHILD_RUN_TERMINATED` and `CHILD_VALUES_UNSTORED`
  (all `InfrastructureError`); and `CHILD_RUN_CANCELED` (`Cancelled`, status
  `CANCELED`). `INVALID_COMPOSITION` is reserved for a hand-built definition
  the compiler would have refused.
- **Bounds.** Composition is acyclic and at most `ORK_COMPOSE_MAX_DEPTH`
  levels deep (default and ceiling 4; a run is level 0). The whole tree is
  planned at the parent's submit — its pools, secret policy, image policy and
  fan-out cap — and a submission the deployment can no longer plan (a child
  deleted, a cap lowered, a pool forbidden) is refused before anything starts.
  Deleting an Orkflow is never refused for its callers' sake: a parent whose
  child is gone is refused at its next submit, naming the child, and
  `orkctl orkflow callers` is the read that answers "who uses this". Every
  definition of the tree travels in the run's plan, which is bounded by
  256 KiB. Admission (`hold`, `max_running`) and budgets are decided once, at
  the parent's submit: a child is never refused by them, because refusing it
  would fail a run already accepted. Children do count toward `max_running`,
  and their attempts are usage of the namespace.
- A child's callback awaits get tokens that are disclosed to no one: resolve
  them with `orkctl run resolve <child-run> --task <task>` or on the child
  run's page.

`orkctl run get <run> --tree` prints every child run under its parent, and
`orkctl run list --origin parent-run:<run>` lists one parent's children; a
child is read, watched and cancelled on its own id. `orkctl run logs` points a
composed task at its child's logs, and `orkctl task run` refuses one by name.
See `examples/release-block.ork.yaml` and `examples/composed-release.ork.yaml`.

## Caching (`cache:`)

A task may declare `cache:` to reuse the result of an identical earlier run:

```yaml
  - id: featurize
    uses: ghcr.io/acme/featurize:3.1
    needs: [fetch-data]
    cache:
      enabled: true          # required: the opt-in, and the author's assertion of purity
      ttl_seconds: 86400     # optional: 0/absent = the deployment's cap (ORK_CACHE_MAX_TTL)
      scope: orkflow         # optional: orkflow (default) | namespace
```

**Marking a task cached asserts that it is a pure function of what it is
given; OrkStack cannot verify that, and a task with external side effects must
not be cached — a hit skips them.** Caching makes execution rarer, never
exactly-once.

When an attempt of a cached task starts, the Orker — after the commit check
and the image policy, and after staging the upstream `values.json` files the
render needs, but before any artifact download, secret, image pull or
container — computes a **memo key**: the SHA-256 of exactly what the task is
and is given. It looks for a **memo** under that key: a small pointer to a
`commit.json` another run already durably wrote.

- **Hit.** The memo is readable, unexpired, and points at a commit that still
  exists with the recorded digest, whose value and artifact objects still
  exist and fit this run's limits. The Orker writes **this run's own**
  `commit.json`, whose value and artifact *references* are the source's
  content-addressed objects, and reports success. No image is pulled, no
  secret is read, no container runs. `orkctl run get` labels the task
  `SUCCEEDED (cached)` and names the run that produced the result.
- **Anything else is a miss, never a failure** — the task runs, and the reason
  is recorded on the attempt (table below). After a fresh commit the Orker
  writes (best-effort) the memo for the next run.

### What the key covers

| Component | Value |
|---|---|
| `image` | the pinned index digest |
| `command` | SHA-256 of the command (`[]` when empty) |
| `env` | SHA-256 of the environment (`{}` when empty) |
| `secrets` | SHA-256 of every secret's name and source — **never a value** |
| `network` | `default` or `none` |
| `run_as_orker_user` | `true` or `false` |
| `config` | SHA-256 of the exact `config.json` bytes (every rendered `with:` value) |
| `run_input` | SHA-256 of the exact `run.json` bytes |
| `inputs` | SHA-256 of every staged input file's container path, name and content digest, and every fan-out upstream's item count |
| `item`, `item_as`, `item_index`, `item_count` | for one item of a fan-out: its value, the name it is bound to, its position and the array's length |

The key document also names the namespace, the scope, the task id and — under
`orkflow` scope — the Orkflow's name.

| Not in the key | Why |
|---|---|
| run id, idempotency key, attempt, pool, trace | coordinates of *this* execution |
| `orkflow_version`, definition digest | they cover every task: editing another task must not invalidate this one's memo |
| the Orkflow name, under `namespace` scope | that is what namespace scope means |
| `timeout_seconds`, `cancellation_grace_period_seconds`, `retry`, `needs`, `condition`, `pool`, `cache:` itself | they decide whether, where, how long and how often a task runs, not what it computes |
| `limits` | deployment configuration; a source that no longer fits is refused at hit time (`SOURCE_OVER_LIMITS`) |
| resources, platform, delivery | facts about the host that ran it |
| secret **values** | a value never enters a key; rotating a secret does not invalidate a memo (apply warns, and says how to purge) |
| upstream artifact **URIs** | a URI names the producing run; the digest is what the workload reads |

Three consequences, stated rather than discovered:

- **Editing another task does not touch this task's key** — the key is built
  from the task's own definition and inputs, never the whole Orkflow's digest.
- **The run input is in the key.** `run.json` is a file the workload may read,
  so a run whose input changes misses every cached task, even one whose
  `with:` ignores that input. `orkctl run again` keeps the input and hits.
- **A fan-out item's position is in the key.** Items of an unchanged array
  hit; an array that grows by one element misses every item.

Render an upstream artifact with `artifact "<task>/<name>"` (a path), never
its `uri`: a URI names the producing run, so a `with:` that renders it misses
whenever that run changes.

### Miss reasons

| Token | Meaning |
|---|---|
| `MEMO_ABSENT` | no memo for this key |
| `MEMO_UNREADABLE` | the memo could not be read (store error, corrupt, unknown schema, or the 30 s lookup bound passed) |
| `MEMO_MISMATCH` | the memo stored at this key describes another key, namespace, scope, task or Orkflow |
| `MEMO_EXPIRED` | the memoized result is older than the task's effective TTL |
| `SOURCE_COMMIT_GONE` | the run that produced the result was retired; its commit is gone |
| `SOURCE_COMMIT_CHANGED` | the commit's stored bytes no longer hash to the digest the memo recorded |
| `SOURCE_COMMIT_UNREADABLE` | the commit could not be read or decoded |
| `SOURCE_COMMIT_MISMATCH` | the commit is not this task's result (namespace, task, run, time, image digest, a non-zero exit, Orkflow) |
| `SOURCE_OVER_LIMITS` | the result does not fit this run's limits (values, artifact count or size, commit size) |
| `SOURCE_OBJECT_UNAVAILABLE` | the committed `values.json` is gone or corrupt, or an artifact object is gone |
| `INPUT_UNHASHED` | an input reference carries no digest, so no key could be computed (no memo is written) |
| `MEMO_STORE_ABSENT` | the Orker has no memo store |

### TTL, scope and bypass

- **TTL.** A memo is reused only while its result is younger than the task's
  `ttl_seconds`, measured from when the result was committed, and never older
  than the deployment's `ORK_CACHE_MAX_TTL` (default 168h): `0` or absent means
  the cap. Changing a TTL invalidates nothing.
- **Scope.** `orkflow` (the default) — reused by runs of the same Orkflow;
  `namespace` — reused by any Orkflow of the namespace whose task of the same
  id computes the same key. A memo never crosses namespaces.
- **`--no-cache`.** `orkctl run submit --no-cache` (and `run again`, and the
  submit panel's *Ignore cached results*) runs every cached task's container
  and refreshes its memo. It applies to the whole tree, child runs included.
- **A memo is a pointer, deletable at any time.** `orkctl cache purge` — or
  deleting the whole `metadata/memo/` prefix — changes no result, only whether
  the next run's containers run. `orkctl cache stats` reports the hit rate and
  the memos stored; both are derived on read.
- **Where it does not apply.** A pinned task, an awaited task and a composed
  task cannot be cached (rules 36, 15, 27); a child Orkflow's own tasks may be.
  `orkctl task run` and `orkctl activity test` never read or write a memo.
  Nothing a container sees changes.
- **No selective keys.** A task cannot exclude a run input, an item's position
  or a variable from its key: each such knob would let a wrong result be
  served silently.
- **No automatic reclamation.** Nothing deletes memos but `orkctl cache purge`
  and a bucket lifecycle rule on `metadata/memo/` (`docs/STORAGE.md`); a memo
  whose source run was retired simply misses.
- **No per-trigger or per-schedule bypass.** An Orkflow whose runs must never
  reuse a result declares no `cache:`.

See `examples/cached-pipeline.ork.yaml`.

## Templating

A task's `with:` values and `condition:` are Go `text/template` templates, as
are `outputs:` values and `each.over` (one reference, see "Fan-out"). Every
other key — `command`, `env`, `uses` included — is literal. A literal `{{` in
`with:` is written `{{ "{{" }}`. The context:

- `.inputs.<name>` — the submitted input, after defaults.
- `.tasks.<id>.output.<field>` — a succeeded ancestor's `values.json`.
- `.tasks.<id>.artifacts` — (`with:` only) the ancestor's artifact references;
  prefer the `artifact` helper.
- `.each.<as>`, `.each.index`, `.each.count` — one fan-out item (see
  "Fan-out").
- Helper `artifact "<taskId>/<artifactName>"` — resolves to a **local path** the
  Orker has materialized before the task runs.
- Helper `env "<NAME>"` — reads an environment variable of the **Orker**, only
  when it is listed in `orker.env_allowlist` (`ORK_ORKER_ENV_ALLOWLIST`).
- Helper `sh <value>` — quotes a rendered value for an explicitly requested
  shell boundary.

There are exactly these three helpers, plus Go's built-in functions (`eq`,
`ne`, `lt`, `and`, `or`, `not`, `index`, `len`, `printf`, …). A template may
read only the task's `needs:` closure (`outputs:` excepted); referencing any
other task is a compile-time error. Past a join, an ancestor may be absent
(see "Joins and failure handlers"). A template that reads a key nobody
supplied fails the task `RENDER_FAILED`; every rendered value is a string.

A `condition:` is evaluated by the **engine**, once, before the task starts:
it sees `.inputs` and `.tasks.<id>.output` only, and a condition that calls a
helper fails when evaluated.

**Condition truthiness.** A `condition:` is false exactly when its rendered
text, trimmed and lower-cased, is one of eleven spellings: the empty string,
`false`, `0`, `no`, `off`, `null`, `<no value>`, `[]`, `map[]`, `<nil>`,
`unspecified`. Anything else is true. So a decide task emits an **enum string
compared with `eq`** — `{{ eq .tasks.decide.output.route "canary" }}` — or a
JSON boolean rendered bare; never a free-form string rendered bare, which is
true for `"No thanks"` and false for `"no"`.

Two more kinds of template are rendered by the **engine** rather than an
Orker: a composed task's `with:` and an Orkflow's `outputs:`. They get **no
helpers** (`artifact`, `env` and `sh` are Orker-side), read
`.tasks.<id>.output` only, and name every task they read literally (rule 28).

Computing — formatting, arithmetic, a decision — is a task's job, never a
template's: see `docs/ORKFLOW_PATTERNS.md`, "The rule: the platform moves data,
tasks compute".

## Planning a run (`orkctl run plan`)

`orkctl run plan NAME` (or `--file DOC` for an unapplied document), and the
submit dialog's **Plan**, answer for one input which tasks would run, which
would be skipped and exactly why, and in which dispatch wave each would start.
The engine decides every condition and join with the interpreter's own
evaluator, predicate and skip sentences, and starts, stores and records
nothing. A plan proves wiring, never behaviour, and is not an admission check:
pools, holds and limits are asked at submit. This is possible only because a
condition computes nothing.

- A condition that reads an upstream output sees `{}` unless the output is
  supplied with `--output TASK=JSON` or `--outputs-file PATH`; the plan names
  every condition that read an unsupplied output.
- `--assume-failed TASK` (repeatable) plans the run as if TASK, when it would
  run, failed after its retries: its failure handlers are planned, every task
  that depends on it and is not a failure handler is `unreached`, and
  independent work is planned as if nothing had failed.
- `--assert-runs TASK` and `--assert-skipped TASK`, one task per flag, make
  a plan a CI check: exit `4` when the plan contradicts one, `1` when no plan
  could be computed. `orkctl run plan deploy --assume-failed deploy
  --assert-runs rollback` checks that a handler is wired.

How each node reads:

| Node | Outcome | Detail |
|---|---|---|
| a task that runs or is skipped | `runs`, `skipped` | the skip sentence a run would record; `pin <name>` for a pinned task |
| a task with a `join:` | as above | prefixed `join <word>` |
| awaited | `awaits`: takes a wave, counted apart from `runs` | `await ≤ <deadline>`, plus `callback`; an `--output` for it is its resolution |
| fan-out | one row, `runs` | `each over <ref>: N items (≤ max_items, P at a time)` when the array is countable — an input, or a stubbed source — otherwise `counted when the run reaches it`; a fan-out whose source does not run, or whose array is too long, `fails` with its `EACH_*` code |
| composed | one row, `runs` | `orkflow <name>@<version> as a child run`: the plan never recurses — plan the child with `orkctl run plan <name> --version <version>` |
| cached | `runs` | the TTL and scope; a plan never predicts a hit, which the Orker decides |
| a condition that cannot be evaluated | `fails` | the run's own error; every task decided after it is `unreached` |
| `--assume-failed` | `assumed_failed` | its non-handler dependents are `unreached` |

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
   reference because it contains `:` and `@`. `network:`, when present, is
   `default` or `none` (6b). Every secret has a `name` matching
   `^[A-Za-z0-9._-]{1,128}$` — validated, never sanitized, so it cannot
   escape the secrets directory — and a non-empty `source:` (6c).
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
    value — both mean derived — because the compiled field cannot tell them
    apart, and every rule must also hold for a hand-built request that never
    met this YAML.
14. When a task's resolved image declares an interface, `with:` supplies
    every key the image requires and names an op it declares (or the image
    declares a default op); an undeclared key or a literal whose type
    disagrees is reported as a warning, never refused, and a templated value
    is never typed.
    Enforced by `internal/api` at validate and apply, after resolution — the
    compiler has no registry to ask (`docs/ACTIVITY_IMAGES.md`, "Declaring the
    interface"). An image without the label is not checked.
15. An awaited task carries nothing but `id`, `needs`, `condition`, `join`
    and `await`: every field that shapes a container — and `each` and `cache` —
    is refused by name, on the compiled message as well as in YAML, so a
    hand-built request meets the same rule.
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
27. A composed task carries nothing but `id`, `needs`, `condition`, `join`,
    `uses`, `with` (and its compiled `orkflow`): every execution field — `command`,
    `env`, `secrets`, `resources`, `network`, `pool`, `pin`, `retry`,
    `timeout_seconds`, `cancellation_grace_period_seconds`,
    `run_as_orker_user`, `each`, `await`, `cache` — is refused by name.
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
33. `cache:` states `enabled`. `enabled: false` is legal and compiles to no
    cache at all — the same definition and digest as a task with no `cache:`.
    Rules 34 and 35 are checked whatever `enabled` says.
34. `cache.ttl_seconds` is not negative. `0` and absent are one value: no
    bound of the task's own; the deployment's `ORK_CACHE_MAX_TTL` bounds
    every memo either way.
35. `cache.scope` is `orkflow` or `namespace`; the compiler writes `orkflow`
    when it is absent, so a stored definition never carries an unspecified
    scope.
36. A cached task is not pinned: its pin's `/orkstack/shared` is an input no
    memo key can cover. An awaited task (rule 15) and a composed task (rule
    27) may not carry `cache:` either.
37. `join:`, when present, is one of `all_succeeded`, `none_failed`,
    `any_failed`, `all_done` (field `tasks[i].join`). `all_succeeded` is the
    default and compiles to no join at all. A compiled `Task.join` is one of
    the defined values.
38. A task whose `join:` is not `all_succeeded` names at least one need: a
    join decides on its needs' outcomes.
39. A failure handler (`any_failed`, `all_done`) is not pinned
    (`tasks[i].pin`), is not a fan-out (`tasks[i].each`) and is not cached
    (`tasks[i].cache`): fail-fast ends every pin with the run's work, a
    fan-out's contract is that its first failed item fails it and stops its
    siblings, and a handler's input includes how its needs ended, which no
    memo key covers.
40. A task whose `join:` is `all_succeeded` or `none_failed` may not name, in
    `needs:`, a task whose `join:` is `any_failed` (`tasks[i].needs[j]`): it
    could never run. Use `join: all_done` (run after it, whatever it did) or
    `join: any_failed` (run if it fails).
41. `outputs:` may not read a task whose `join:` is `any_failed`
    (`outputs.<name>`): outputs are rendered only when a run succeeds, and
    then such a task is always skipped.
42. A failure handler that runs a container names at most 32 needs
    (`tasks[i].needs`): its input records how every need ended, and that
    record is bounded. An awaited or composed failure handler is not limited.

Decoding is strict: a key the schema above does not define is refused, never
ignored, so a misspelled or unsupported key cannot surface later as a run that
behaves differently from its document.

A slug is at most **200 bytes**, in every position above. The bound is half the
rule and not a detail of it: a namespace, an orkflow name and a version are
path components of a stored object key, a pool becomes an execution channel,
and a pin name becomes a directory component on the Orker, so a longer name is
one the compiler could accept and nothing could store. The orkflow store and
the namespace registry (`pkg/artifacts`) enforce the same 200; they share no
constant, so the store does not import the compiler, and a test in
`pkg/artifacts` fails on any disagreement.

## Labels

`labels:` is free-form: no key shape, entry count, or value size is enforced,
because a label is the tenant's own document and an ordinary key like `Team`
or `app.kubernetes.io/name` should not be refused for not looking like an
attribute. (A namespace's operator-set telemetry attributes, by contrast, are
bounded and shaped at write — `ValidateAttributeMap`: at most 16 entries, each
key `^[a-z][a-z0-9_.-]{0,63}$`, each value at most 256 bytes with no control
characters — because they are rendered verbatim in terminals and the UI.)

When telemetry is on, labels are made safe where they are exported, not
refused as a document: at most 32, in sorted-key order, become
`ork.label.<key>` attributes beside the namespace's `ork.ns.<key>`
(`docs/DEPLOY.md`, "Per-namespace policy"). Each key is lowercased, every byte
outside `[a-z0-9_.-]` replaced with `_`, a non-letter first character
prefixed `l_`, and capped at 64 bytes (`dsl.SanitizeAttributeKey`); each value
is truncated to 256 bytes with control characters stripped
(`dsl.SanitizeAttributeValue`). Two keys that fold to the same attribute keep
only the one that sorts first.

## Example

See `examples/hello.ork.yaml` for a fetch → transform → checksum pipeline that
uses the first-party `http`, `shell`, and `file` OCI activity images with
artifact hand-off. Every shipped example is compiled by
`TestCompile_AcceptsEveryShippedExample`. `examples/README.md` lists every
shipped example and what it needs to run, and `docs/ORKFLOW_PATTERNS.md` is the
best-practice guide they illustrate.
