# Orkflow patterns

This guide is the set of rules the shipped examples follow, and why. It is
not a reference: [the DSL reference](DSL.md) defines every key and rule, and
[`examples/README.md`](../examples/README.md) indexes every example and what
it needs to run. Two rules run through all of it: environment variables are
an ordinary way to configure a task, and caching is the author's assertion,
not a property OrkStack expects of tasks.

Every example cites the sections below by title in its header
(`# Pattern: docs/ORKFLOW_PATTERNS.md, "<title>"`), and every example is
compiled, planned and run end to end by the test suite.

## The rule: the platform moves data, tasks compute

**The platform moves and quotes data; a task computes.** The engine and the
Orker move values and files between tasks, and quote a value where it lands —
an argv element, a shell word, a file. Every computation — formatting,
arithmetic, parsing, validation, a decision — runs in a task.

The template layer is Go `text/template` with exactly three helpers,
`artifact`, `env` and `sh`, plus Go's built-ins (`eq`, `ne`, `lt`, `le`,
`gt`, `ge`, `and`, `or`, `not`, `len`, `index`, `printf`). A `condition:`
reads values, compares them and combines the comparisons, nothing more.

Why:

- the engine's own workflow code must stay deterministic, so it cannot run
  arbitrary computation on a run's data;
- `orkctl run plan` can evaluate every condition ahead of a run only because
  conditions compute nothing;
- OrkStack deliberately has no expression language or scripting language.

A request for a template function — `toJSON`, `now`, arithmetic, string
splitting — is answered by a task, usually a `json` one: jq is a complete
language for exactly this, and it runs in a container like any other task.

Examples: [decide-and-route](../examples/decide-and-route.ork.yaml),
[each-fan-out](../examples/each-fan-out.ork.yaml),
[config-from-git](../examples/config-from-git.ork.yaml).

## jq, a template, or your own image

| You need to… | Use |
|---|---|
| reshape JSON, compute values, make a decision, build the array a fan-out reads, build a request body | `json` (one jq filter, or a deep merge) |
| render a text file — a config, a manifest — from a template file and data | `template` (its template is a file, so its `{{ }}` never meets the DSL's) |
| exact decimal arithmetic (money), time zones and calendars as business rules, a library, heavy computation, anything you would unit-test as code | your own image ("When to write your own image") |
| run a command | `shell` |
| make one HTTP request | `http` |
| copy, compress, archive, untar | `file` |
| fetch source | `git` |

Numbers deserve a caveat (the `json` README's *Limits*,
[`images/activity-json/README.md`](../images/activity-json/README.md)):
arithmetic is IEEE double; a number that reaches a task through a template
arrives as Go's spelling (`1000000` arrives as `1e+06`), so `tonumber | floor`
recovers an integer; ids and integers beyond 2^53 travel as files, never as
template values.

Examples: [config-from-git](../examples/config-from-git.ork.yaml),
[notify-block](../examples/notify-block.ork.yaml),
[custom-image](../examples/custom-image.ork.yaml).

## Decide in a task, branch on its value

A decide task's values **are** the decision: an enum string or a JSON
boolean — for a `json` task, under `result`. Downstream conditions only read
it:

- compare an enum with `eq`: `{{ eq .tasks.decide.output.result.route "canary" }}`;
- render a JSON boolean bare: `{{ .tasks.assess.output.result.risky }}`;
- combine with `and`, `or`, `not`;
- compare numbers inside the task, not in the condition — a template sees
  every JSON number as a float, which is why older examples write `200.0`.

A condition is false exactly when its rendered text, trimmed and lower-cased,
is one of eleven spellings (the empty string, `false`, `0`, `no`, `off`,
`null`, `<no value>`, `[]`, `map[]`, `<nil>`, `unspecified`; the DSL
reference's "Templating"). Anything else is true. That is why a free-form
string rendered bare is unsafe: `"no"` is false, and `"No thanks"` is true.

A real decision often has several outputs, so it is an object. Planning such
a document needs the decision supplied with `--output` ("Plan before you
run").

Examples: [decide-and-route](../examples/decide-and-route.ork.yaml),
[approval-gate](../examples/approval-gate.ork.yaml).

## Branch and re-join

`join: none_failed` re-joins after whichever branch ran: the task starts when
none of its needs failed or was cancelled and at least one succeeded. The
default join, `all_succeeded`, would skip it, because the branches that did
not run are `SKIPPED`.

A skipped task is absent from `.tasks`, and its files are not staged. So,
downstream of a join, any ancestor may be absent, and reading it fails the
task. Guard every such read with one of the two forms apply recognises:

```text
{{ if index .tasks "canary" }}…{{ end }}
{{ with index .tasks "canary" }}{{ .output.x }}{{ end }}
```

Only those two exact pipelines are recognised; `{{ if and (index .tasks "a")
… }}` still warns. Choose a path with a guard rather than name a skipped
task — `decide-and-route`'s `report` picks the staged file of the branch that
ran with an `if … else if … else` chain. Apply warns about every unguarded
read of an ancestor a join can leave absent.

Examples: [decide-and-route](../examples/decide-and-route.ork.yaml),
[approval-gate](../examples/approval-gate.ork.yaml),
[branch-and-recover](../examples/branch-and-recover.ork.yaml).

## Batch transforms into one prepare task

One task per run validates the inputs and computes everything later tasks
need — request bodies, formatted strings, the array a fan-out reads — once,
not once per item. An item's `with:` only places precomputed fields.

- Fan out with `each:` over the prepare task's array; fan in with `json`'s
  `gather`. A fan-out's items are files, never template values.
- A failure handler of a failed fan-out has nothing to gather: it reads the
  node's error from `task.json`'s `join` object instead.
- A prepare over repeated inputs is the natural thing to cache ("Cache only
  what you assert is reusable").

Examples: [each-fan-out](../examples/each-fan-out.ork.yaml),
[config-from-git](../examples/config-from-git.ork.yaml),
[order-saga](../examples/order-saga.ork.yaml).

## Pass values as data, never as code

A rendered value is data. Where it lands decides whether it stays data. In
order of preference:

1. **An argv element.** `command` is an array: a rendered value is one
   argument, whatever it contains.
2. **A positional argument to a script.** The script text is constant; the
   value is `"$1"`, passed through `command`.
3. **`{{ sh … }}`** when the value must sit inside shell text: it quotes the
   value for the shell.
4. **jq's `vars`** for values into a filter — never spliced into the filter
   text — and files for structures and large integers.
5. **JSON built by jq** (`tojson`), never by splicing a value into a JSON
   string in `with:`: a quote or a newline in the value would break the
   document, or change its meaning.
6. **A value used in a URL or a path is validated first**, by the prepare
   task, so a malformed one fails before any request is made.

Also:

- every templated value renders to a string;
- a literal `{{` in a `with:` string is written `{{ "{{" }}`;
- a task's top-level `command:` is passed to the image verbatim and never
  rendered — only `with:` and `condition:` are.

Examples: [inputs-and-escaping](../examples/inputs-and-escaping.ork.yaml),
[notify-block](../examples/notify-block.ork.yaml),
[order-saga](../examples/order-saga.ork.yaml),
[poll-until-ready](../examples/poll-until-ready.ork.yaml).

## Environment variables are configuration

**`env:` is a valid, expected way to configure any activity.** Set a time
zone (`TZ`), a locale, a proxy (`HTTPS_PROXY`, `NO_PROXY`), a tool's own
settings or tracing (`GIT_TRACE`, `GOMPLATE_*`), or the configuration of your
own image — exactly as you would configure the same program on any machine.

Every first-party image honours a task's `env:` like any Unix tool: nothing
is cleared or silently overridden. The `json`, `git` and `template` READMEs'
*Environment* sections list each of those images' defaults, and that a
`with:` key beats the variable that configures the same thing; `shell`,
`http` and `file` read no variable of their own and hand a task's `env:` to
the program they run unchanged ([the activity images
page](ACTIVITY_IMAGES.md#first-party-reference-images)).

What `env:` is not:

- **per-run data.** `env:` values are literal text in the document, not
  templates; per-run values go through inputs and `with:`.
- **a secret.** The value is stored in the definition for anyone who can read
  it. Secrets are files under `secrets:`.
- **OrkStack's own context.** That is `task.json`.
- **the `{{ env "NAME" }}` helper**, which reads an allowlisted variable of
  the **Orker** (`orker.env_allowlist`) when `with:` is rendered.

No `env:` value grants a credential, a privilege or host access: the Orker
remains the boundary.

For caching: a variable set by `env:` is part of a cached task's key; a
variable the container runtime sets per container (`HOSTNAME`) is not.

Examples: [poll-until-ready](../examples/poll-until-ready.ork.yaml),
[config-from-git](../examples/config-from-git.ork.yaml).

## Cache only what you assert is reusable

**Nothing in OrkStack expects a task to produce the same output from the same
input.** Tasks are at-least-once and may read the clock, the network and
their environment; determinism is required of the engine's own workflow code
only, never of a task.

`cache:` is the author's assertion that reusing an earlier result for the
same key is acceptable. OrkStack cannot verify it, and a hit skips the
container and everything it would have done.

What makes the assertion false:

- the clock, and randomness;
- the network;
- a variable the container runtime sets, such as `HOSTNAME`;
- the run's coordinates read from `task.json` (run id, idempotency key,
  attempt);
- a git ref that moves;
- state outside the run;
- a secret's value — the key covers a secret's name and source, never its
  value.

The rules:

1. `network: none` on every cached compute task, so an accidental network
   dependency fails rather than being memoized.
2. **A task that mounts a secret is never cached.**
3. Clone by commit id ("Pin moving inputs: resolve, then use the commit").
4. Cache the tasks *after* an uncached step. A cached task's key covers every
   succeeded ancestor's values and files, so it hits exactly when they
   reproduced. An ancestor whose values vary — an HTTP response's timing and
   headers — makes every task after it miss, so do not cache those:
   `poll-until-ready`'s `summarize` reads nothing but its input and is still
   not cached, for exactly this reason.
5. Never cache an `http` task, or any side effect.
6. Always set `ttl_seconds`.
7. When a result must be refreshed: `orkctl run submit --no-cache`, or
   `orkctl cache purge`.

The per-image rules are in [the activity images page, "Network and caching of
the first-party
images"](ACTIVITY_IMAGES.md#network-and-caching-of-the-first-party-images),
and are not restated here.

Examples: [config-from-git](../examples/config-from-git.ork.yaml),
[poll-until-ready](../examples/poll-until-ready.ork.yaml),
[cached-pipeline](../examples/cached-pipeline.ork.yaml).

## Pin moving inputs: resolve, then use the commit

A branch, a tag and `HEAD` move. Resolve one once — `git` `op: resolve` — and
hand the commit to every later task, as apply resolves an image tag to a
digest. A clone by commit id gives every task of the run one tree, even when
someone pushes mid-run; it makes `orkctl run diff` name the commit; and it
lets the tasks after it be cached.

Examples: [config-from-git](../examples/config-from-git.ork.yaml),
[pinned-checkout](../examples/pinned-checkout.ork.yaml).

## Side effects are at-least-once

**Activities are at-least-once.** A retry, a lost Orker or a pinned retry can
repeat a side effect. Make it idempotent:

- with `task.json`'s `idempotencyKey`, which is stable across a task's
  retries (and per item for a fan-out);
- or with a key you choose — `PUT /orders/{id}/payment` is the same request
  however often it is sent.

Never cache a task with a side effect. Give a compensation `retry:` and make
it safe to run twice.

Examples: [order-saga](../examples/order-saga.ork.yaml),
[notify-block](../examples/notify-block.ork.yaml).

## A non-2xx answer is data

The `http` activity exits `0` for any response it received, and reports
`status`. Branch on it with a condition, or fail explicitly with a `json`
check (`if .status == 201 then … else error(…) end`, which exits non-zero).
A request that could not be performed — DNS, refused, TLS, a timeout — is a
failure, and `retry:` retries it.

Examples: [order-saga](../examples/order-saga.ork.yaml),
[notify-block](../examples/notify-block.ork.yaml),
[http-post](../examples/http-post.ork.yaml).

## Undo what was done: failure handlers and sagas

**A failure handler never rescues the run.** A task whose join is
`any_failed` or `all_done` is a failure handler: the only kind of task that
starts in a run that is already failing. When a task fails:

- the run still ends `FAILED` with the original failure's reason;
- a handler's own failure is recorded on the handler and replaces nothing;
- a handler has one attempt unless it declares `retry:`;
- a task fail-fast stopped is `CANCELED`, never `FAILED`, so `any_failed`
  does not fire on its account.

Compensation is not guaranteed on a cancel or a terminate: an operator's
`orkctl run cancel` starts no failure handler, and a run terminated from
outside OrkStack runs no workflow code at all. Cleanup at cancellation is the
container's own SIGTERM handling.

Three compensation forms, each for a different situation:

| Form | Shape | Use it when |
|---|---|---|
| *succeeded step* | `needs:` every forward step, `join: any_failed`, and a condition that the step it undoes succeeded (`{{ if index .tasks "reserve" }}true{{ end }}`) — the DSL reference's sequential saga idiom | "failed" means "did nothing": undo only what certainly happened |
| *step k and every later step* | `needs:` step *k* and every step after it, `join: any_failed`, no condition, an idempotent undo (a `DELETE`) | a request that failed after its retries — a timeout, a reset connection — may still have taken effect |
| *parallel* | `join: all_done` over the forward steps, deciding in the container from `task.json`'s `join.needs[].status` (the DSL reference's parallel form) | forward steps run in parallel, so a stopped one is `CANCELED` |

The second form selects exactly the right compensations: a later step can
only start after step *k* succeeded, and a step that never started is
`CANCELED`, not `FAILED`.

Report either way with an `all_done` task that reads `task.json`'s `join`
object, and notify through a composed block. What does not exist: loops,
`continue_on_error`, an Orkflow-level `on_failure:`.

Examples: [order-saga](../examples/order-saga.ork.yaml),
[branch-and-recover](../examples/branch-and-recover.ork.yaml).

## Repeat without loops

There is no loop, back-edge or `while`: the graph stays a DAG, and
`orkctl run plan` sees all of it before it runs.

- **Polling** is one check per attempt plus a retry policy — bounded, with
  backoff. Each attempt does work (a request and a test); between attempts
  the retry policy's timer waits, and no container runs.
- **A longer wait** than a retry policy should hold is an awaited task with a
  callback.
- **Periodic work** is a schedule — an automation document, or `orkctl
  schedule create` — with `overlap: skip`.
- **Iteration** is `each:`.
- **Waiting for the outside world** is `await:`. Never start a container
  merely to sleep or wait.

Examples: [poll-until-ready](../examples/poll-until-ready.ork.yaml),
[the automation document](../examples/automation/examples.ork.yaml),
[each-fan-out](../examples/each-fan-out.ork.yaml).

## Wait for a person or a system

`await:` with a `schema`, opened only when needed by a condition. It starts
no container and holds no Orker while it waits.

- A business "no" is a **completion** carrying a value
  (`{"approved": false}`), which a condition reads.
- `--reject` and an expired deadline are **failures**, which failure handlers
  see.
- A callback token is disclosed to whoever submits the run, so never set
  `callback: true` when the approver must be someone else.
- `orkctl run resolve <run> --task <id> --values '<json>'` and the run page
  resolve it.

Examples: [approval-gate](../examples/approval-gate.ork.yaml),
[awaited-approval](../examples/awaited-approval.ork.yaml).

## Share a working tree: pins

Use a pin when several steps need one working tree, or a large intermediate,
in place; otherwise hand files on as artifacts.

- Every member names the same pool (or none) and sets
  `run_as_orker_user: true`.
- A pin adds no ordering: the `needs:` edges do.
- A pinned task is never cached, and a failure handler is never pinned.
- A retry sees the shared directory its failed predecessor left, so mutating
  steps must be idempotent (`git` `dest` stages and renames into place).
- A lost host fails the pin `PIN_HOST_LOST` rather than moving it.
- Commit what must outlive the pin as an artifact: nothing under
  `/orkstack/shared` does.
- The pool needs a pin host, an Orker with `ORK_ORKER_MAX_PINS` above zero.

Examples: [pinned-checkout](../examples/pinned-checkout.ork.yaml),
[pinned-build](../examples/pinned-build.ork.yaml).

## Reuse a block with declared outputs

- `uses: orkflow:<name>@<version>` always pins a version.
- The child receives values, never files.
- Its `outputs:` are its interface, and they are strings.
- `orkctl orkflow compat` and the promote gate compare interfaces.
- A composed failure handler runs its child in a failing parent.
- Apply a child before the Orkflows that compose it.

Examples: [notify-block](../examples/notify-block.ork.yaml),
[order-saga](../examples/order-saga.ork.yaml),
[approval-gate](../examples/approval-gate.ork.yaml),
[composed-release](../examples/composed-release.ork.yaml).

## When to write your own image

Write one when "jq, a template, or your own image" says so — or use an
existing tool's image when one already does the job. The contract is files:

- read `/orkstack/input/` (`task.json`, `config.json`, `run.json`);
- write `/orkstack/output/values.json` and regular-file artifacts under
  `/orkstack/output/artifacts/`;
- exit `0`, or non-zero for a failure.

No SDK, and no `op` unless you want one. Declare the interface label
(`org.orkstack.activity.interface`), and handle SIGTERM if the work is long.

- Build, push and pin it with `orkctl activity build <dir> --tag <ref>`.
- The image policy applies to it: `require_digest`, `allowed_registries`,
  `require_signed_by`.
- Try one task with `orkctl task run --file <doc> --task <id>`.
- `orkctl activity test` drives only images that implement the `op: exec`
  convention.

Examples: [custom-image](../examples/custom-image.ork.yaml),
[the Go image](../examples/custom-image/),
[the shell image](../examples/custom-image-shell/).

## Declare what surrounds your Orkflows

One automation document per namespace declares its settings, schedules,
triggers and webhooks ([the automation contract](AUTOMATION.md)). `orkctl apply` plans it with
`--dry-run`, checks for drift with `--check`, pins a reviewed plan with
`--expect-plan`, and deletes what the document no longer names with
`--prune`. A trigger's token is printed once and never stored in the
document. It is a one-shot diff-and-write, not a controller.

Examples: [the automation document](../examples/automation/examples.ork.yaml).

## Plan before you run

`orkctl run plan` and the submit panel's Plan decide conditions, joins and
skips without starting anything.

- Supply a decide task's output with `--output TASK=JSON`. A condition that
  reads a key under an output that was not supplied cannot be evaluated, and
  the plan says so.
- `--assume-failed TASK` plans a failure path.
- `--assert-runs TASK` and `--assert-skipped TASK` make a plan a CI check: it
  exits `4` when the plan contradicts one.
- Every plan line in an example's header is run by the test suite.
- `orkctl task run` runs one task's filter or template locally.

Examples: [decide-and-route](../examples/decide-and-route.ork.yaml),
[order-saga](../examples/order-saga.ork.yaml),
[approval-gate](../examples/approval-gate.ork.yaml).

## Examples index

The eleven older examples (marked *older*) predate this guide and are kept
byte-identical, because a stored version cannot change. Five of them splice a
rendered value into shell text or a JSON string — `http-post`, `shell-eval`,
`file-read`, `pinned-build` and `advanced-demo` — which "Pass values as data,
never as code" says not to do; follow the guide, not them.

| Example | Shows | Sections |
|---|---|---|
| [`examples/advanced-demo.ork.yaml`](../examples/advanced-demo.ork.yaml) | *older* — retries, conditions, a secret, streamed logs | "Decide in a task, branch on its value" |
| [`examples/approval-gate.ork.yaml`](../examples/approval-gate.ork.yaml) | an approval only when a change is risky | "Wait for a person or a system", "Decide in a task, branch on its value", "Reuse a block with declared outputs" |
| [`examples/awaited-approval.ork.yaml`](../examples/awaited-approval.ork.yaml) | *older* — an awaited task | "Wait for a person or a system" |
| [`examples/branch-and-recover.ork.yaml`](../examples/branch-and-recover.ork.yaml) | *older* — branch, re-join, roll back | "Branch and re-join", "Undo what was done: failure handlers and sagas" |
| [`examples/cached-pipeline.ork.yaml`](../examples/cached-pipeline.ork.yaml) | *older* — a cached task | "Cache only what you assert is reusable" |
| [`examples/composed-release.ork.yaml`](../examples/composed-release.ork.yaml) | *older* — composing a block | "Reuse a block with declared outputs" |
| [`examples/config-from-git.ork.yaml`](../examples/config-from-git.ork.yaml) | one commit rendered per service, cached | "Pin moving inputs: resolve, then use the commit", "Batch transforms into one prepare task", "Cache only what you assert is reusable", "jq, a template, or your own image" |
| [`examples/custom-image.ork.yaml`](../examples/custom-image.ork.yaml) | two images of your own as activities | "When to write your own image" |
| [`examples/decide-and-route.ork.yaml`](../examples/decide-and-route.ork.yaml) | decide, branch three ways, re-join | "Decide in a task, branch on its value", "Branch and re-join", "Plan before you run" |
| [`examples/each-fan-out.ork.yaml`](../examples/each-fan-out.ork.yaml) | a fan-out over a computed list | "Batch transforms into one prepare task" |
| [`examples/file-read.ork.yaml`](../examples/file-read.ork.yaml) | *older* — the file activity | "jq, a template, or your own image" |
| [`examples/hello.ork.yaml`](../examples/hello.ork.yaml) | *older* — fetch, transform, checksum | "The rule: the platform moves data, tasks compute" |
| [`examples/http-post.ork.yaml`](../examples/http-post.ork.yaml) | *older* — a POST and its status | "A non-2xx answer is data" |
| [`examples/inputs-and-escaping.ork.yaml`](../examples/inputs-and-escaping.ork.yaml) | typed inputs and escaping | "Pass values as data, never as code" |
| [`examples/notify-block.ork.yaml`](../examples/notify-block.ork.yaml) | a reusable notification block | "Reuse a block with declared outputs", "Pass values as data, never as code" |
| [`examples/order-saga.ork.yaml`](../examples/order-saga.ork.yaml) | a saga over an HTTP API | "Undo what was done: failure handlers and sagas", "Side effects are at-least-once", "A non-2xx answer is data", "Reuse a block with declared outputs" |
| [`examples/pinned-build.ork.yaml`](../examples/pinned-build.ork.yaml) | *older* — a pin | "Share a working tree: pins" |
| [`examples/pinned-checkout.ork.yaml`](../examples/pinned-checkout.ork.yaml) | a git checkout in a pin | "Share a working tree: pins", "Pin moving inputs: resolve, then use the commit" |
| [`examples/poll-until-ready.ork.yaml`](../examples/poll-until-ready.ork.yaml) | polling through a retry policy | "Repeat without loops", "Environment variables are configuration" |
| [`examples/release-block.ork.yaml`](../examples/release-block.ork.yaml) | *older* — the block composed-release composes | "Reuse a block with declared outputs" |
| [`examples/shell-eval.ork.yaml`](../examples/shell-eval.ork.yaml) | *older* — the shell activity's ops | "jq, a template, or your own image" |
| [`examples/automation/examples.ork.yaml`](../examples/automation/examples.ork.yaml) | settings, a schedule and a trigger | "Declare what surrounds your Orkflows", "Repeat without loops" |
| [`examples/custom-image/`](../examples/custom-image/) | an activity image in Go, `FROM scratch` | "When to write your own image" |
| [`examples/custom-image-shell/`](../examples/custom-image-shell/) | the same activity in POSIX shell | "When to write your own image" |
