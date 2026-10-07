# activity-json

Two JSON operations, and deliberately no third: `query` runs one
[jq](https://jqlang.org) filter and reports exactly one result, and `merge`
deep-merges several JSON objects into one. It is the default **compute** step
of an Orkflow: the platform only moves and quotes data, and anything that
computes — reshaping an API response, turning a run's input into a fan-out
list, deciding a route — is a task. This one.

```yaml
- id: pick
  uses: json
  needs: [fetch]
  network: none
  with:
    op: query
    src: '{{ artifact "fetch/body" }}'
    filter: '{slideshow: .slideshow.title, slides: (.slideshow.slides | length)}'
```

`network: none` costs nothing here and is worth writing: jq reads files and
writes files, and nothing in jq reads the network.

The computing tool is jq 1.7 (Alpine's `jq~1.7`, patch releases only); the
first line of every task's stdout names the exact version, e.g.
`activity-json: jq-1.7.1`.

## Paths

Every path resolves inside the workspace by activity-file's rules, character
for character. A relative path resolves under `/orkstack/input/`, so
`upstream/build/values.json` means `/orkstack/input/upstream/build/values.json`.
An absolute path must already be under `/orkstack/`. A `..` component is
**refused, not normalized**, and a path must name a regular file.

The files the contract already stages are therefore readable by relative path:
`run.json` (the run's input), `task.json` (this attempt's context, including a
fan-out item's `each.item` as exact JSON) and `upstream/<task-id>/values.json`
(an upstream task's committed values). A pinned task may also read
`/orkstack/shared/…`.

## Operations

| `op` | Reads | Reports |
|---|---|---|
| `query` | exactly one of `src`, `srcs`, `gather`, `nullInput: true` | the filter's one result |
| `merge` | `srcs`, at least two files, each one JSON object | the deep left-to-right merge |

| Key | Op | Type | Meaning |
|---|---|---|---|
| `op` | — | string | **Required.** `query` or `merge`. There is no default op. |
| `filter` | `query` | string | **Required.** One jq program, 1–65536 bytes. |
| `src` | `query` | string | One JSON file. With `slurp: true`, every JSON value in it as one array (JSON Lines). |
| `slurp` | `query` | boolean | Only with `src`. Default `false`. `slurp: true` with any other source form is refused; `slurp: false` changes nothing. |
| `srcs` | `query`, `merge` | array of strings | `query`: at least one file; the filter sees **one array** — every JSON value of every file, in order. `merge`: at least two files, each holding exactly one JSON object. |
| `gather` | `query` | string | A task id: the filter sees one array of that **fan-out**'s item `values.json` objects, in item order. |
| `nullInput` | `query` | boolean | `true`: the filter runs once with `null` as its input. Default `false`. |
| `vars` | `query` | object | Bound as the jq variable `$vars`, always (`{}` when absent). At most 65536 bytes as compact JSON. |
| `as` | both | string | Write the result as this output artifact instead of `values.json`'s `result`. |

`query` takes **exactly one** source form. None, or several, is refused naming
the four; `srcs: []` is refused too (`with.srcs names no file; use nullInput:
true to run the filter on null`). There is no implicit null input: a forgotten
`src` would otherwise run `.items | length` over `null` and report `0`, a
silently wrong answer.

Every key is read by **presence, then type**. A key that is present with the
wrong JSON type — `null` and `false` included — fails
`activity-json: with.<key> must be <a string | a boolean | an object | an
array>`, before any work. `vars: false` is refused, never read as `{}`.

`merge` uses jq's `*`, which recurses into objects: a nested key of a later
file overrides only the leaf it names. Any other value — arrays included — is
replaced by the later file's, and a later `null` replaces too.

### `gather` — the fan-in

A fan-out's items each commit their own `values.json`, staged for a direct
dependent under `upstream/<fan-out>/<item>/values.json`. `gather: <id>` reads
them as one array, in item order, using `task.json`'s authoritative list — so
the filter usually starts `map(.result)` when each item was itself a `json`
task:

```yaml
- id: line
  needs: [prepare]
  each: {over: '{{ .tasks.prepare.output.result }}', max_items: 100}
  uses: json
  network: none
  with: {op: query, src: task.json, filter: '.each.item | "\(.name)=\(.replicas)"'}
- id: collect
  needs: [line]
  uses: json
  network: none
  with: {op: query, gather: line, filter: 'map(.result) | join(",")'}
```

A fan-out with zero items gathers `[]`. A `gather` id that `task.json` lists no
fan-out items for fails — gather reads a fan-out this task names in its
`needs:` and that succeeded; an ordinary upstream is read with
`src: upstream/<id>/values.json`. A count that does not match the staged files
fails naming both numbers, never guessing.

## Output

Without `as`, the result is `values.json`'s one key:

```json
{"result": {"slideshow": "Sample Slide Show", "slides": 2}}
```

It is **always** under `result`, never spliced into the top level. A
downstream path is then predictable — `.tasks.pick.output.result.slides` —
whatever the filter returns, and a `result` array is exactly what a fan-out's
`each.over` reads (`'{{ .tasks.<id>.output.result }}'`).

A filter must produce **exactly one** output. None fails (`the filter produced
no output`), several fail naming the count — collect them, e.g. `[ .[] | … ]`.

With `as`, the result becomes one output artifact and `values.json` is
activity-file's three keys:

```json
{
  "path": "/orkstack/output/artifacts/config.json",
  "sizeBytes": 142,
  "sha256": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
}
```

A JSON **string** result is written as the string's own bytes plus a newline;
anything else as compact JSON plus a newline.

**Large results.** When the `values.json` this image writes is larger than the
task's values limit, the image prints a hint on stderr — `the result is <n>
bytes, over this task's values limit of <m>; write it as an artifact with as:
<name>` — and still exits 0. The platform then refuses the result as a
contract violation (`VALUES_TOO_LARGE`), which is never retried: the same
filter over the same files would fail the same way.

## Limits

- **Numbers** are IEEE doubles under arithmetic: an integer beyond 2^53 loses
  precision when computed with (`12345678901234567890 + 0` →
  `12345678901234567000`), and there is no decimal type — money belongs in your
  own image. jq 1.7 preserves a number's literal text when it passes through
  **unchanged**, so reshaping a **file** never corrupts an id:
  `{"n":12345678901234567890}` filtered by `.n` reports
  `12345678901234567890`.
- **That guarantee stops at the file.** A number that reaches a task through a
  template — `{{ .inputs.<n> }}`, `{{ .tasks.<id>.output.<n> }}` — is a double
  before this image sees it, and is printed in Go's spelling: `42` stays `42`,
  but `1000000` arrives as `1e+06`, `123456789` as `1.23456789e+08` and
  `12345678901234567890` as `1.2345678901234567e+19`. So:
  - `$vars.n | tonumber` recovers every integer up to 2^53 exactly and computes
    correctly; emitted unchanged it keeps the parsed spelling (`"1e+06" |
    tonumber` prints `1E+6`), so `tonumber | floor` is the idiom when the
    integer is itself the result (`1000000`).
  - An id used as text, and any integer beyond 2^53, survives **only as a
    file**: `src: upstream/<id>/values.json` (the committed bytes themselves),
    an artifact, or — inside a fan-out item — `src: task.json` and its
    `.each.item`, which carries the element as exact JSON. A number typed into
    the document's own `with:` or into a run's input is a double from the
    moment it is compiled or submitted, whatever image reads it.
- **Dates.** `now`, `gmtime`, `mktime`, `strptime`, `todate`, `fromdate` and
  `strftime` of a `gmtime` value are UTC whatever `TZ` says; `localtime` and
  `strflocaltime` follow `TZ` — `UTC` by default, or any zone a task's
  `env: {TZ: Europe/Berlin}` names (the image ships zone data, so
  `0 | strflocaltime("%H")` prints `01` there). `%Z` prints an empty string
  under the image's C library, so a filter prints the zone it set rather than
  reading it back. Time-zone business rules belong in your own image.
- **Input** is JSON only — no YAML, CSV or raw text mode. A multi-line text
  document is the `template` activity's job.
- **Values into a filter** go through `vars`, never into the filter text: a
  templated value spliced into a jq program is an injection, and there is no
  jq-quoting helper (and there will not be one). A templated `with:` value
  renders to a **string**, an object renders as Go's `map[k:v]` rather than
  JSON, and a number renders as above — so strings, booleans and small numbers
  travel in `vars` (`$vars.limit | tonumber`), while structures, ids and large
  integers travel as files.
- **Literal braces.** `filter` and every `vars` leaf are `with:` strings, and a
  `with:` string containing `{{` is a DSL template: `orkctl orkflow validate`
  parses it and the Orker renders it before the container starts. A filter that
  needs the two characters `{{` literally — a string literal such as
  `"{{name}}"` — writes them as `{{ "{{" }}`: `'"{{ "{{" }}name}}"'` reaches jq
  as `"{{name}}"`. Unescaped, the document is refused at validate and apply
  with a template parse error. A lone `}}` needs no escape.
- **Encoders** available: `@csv`, `@tsv`, `@json`, `@text`, `@base64`,
  `@base64d`, `@uri`, `@sh`, `@html`.
- **Trying a filter without a run:** `orkctl task run --file <doc> --task <id>
  --output <upstream>=<json> [--artifact <task>/<name>=<path>]` runs one task
  locally against supplied outputs; it never reads or writes a cached result.

## Joins and failure handlers

Two idioms for tasks that use `join:` (`docs/DSL.md`, "Joins and failure
handlers"):

1. **Re-joining after an if/else reads whichever branch ran — by choosing the
   path, never by naming both.** Under `join: none_failed` a skipped need is
   absent from `.tasks` and from `task.json`'s `inputs.upstream`, so its
   `upstream/<id>/values.json` is never staged. A joiner whose `srcs` names
   both branches therefore fails `activity-json: not a regular file:
   /orkstack/input/upstream/<skipped>/values.json` — by design: this image
   never substitutes a missing input. Pick the path with the `index` guard,
   which never fails a render:

   ```yaml
   - id: report
     needs: [canary, direct]
     join: none_failed
     uses: json
     network: none
     with:
       op: query
       src: '{{ if index .tasks "canary" }}upstream/canary/values.json{{ else }}upstream/direct/values.json{{ end }}'
       filter: .result
   ```

   (`.tasks.canary` in the field spelling would fail the render when `canary`
   was skipped.) The joiner may be cached: its key names the staged path it
   read.
2. **A failure handler reads how its needs ended from `task.json`.** A handler
   (`join: any_failed` or `all_done`) is given a top-level `join` object —
   `policy`, and per need `taskId`, `status` and an optional
   `error.{code,category,message,details}`. With `src: task.json`,
   `filter: '[(.join.needs // [])[] | select(.status == "FAILED") |
   "\(.taskId): \(.error.code // "unknown")"] | join("; ")'` builds a
   notification line for a following `http` task. A handler of a failed
   fan-out has nothing to `gather` — the items of a fan-out that failed are not
   staged — so `gather` fails naming the id; read the node's error from `.join`
   instead.

A **decide task** emits a JSON enum or boolean, compared with `eq` in a
`condition:` — `'{{ eq .tasks.decide.output.result "canary" }}'`.

## Caching

Nothing in OrkStack expects a task to produce the same output from the same
input: tasks run at least once and may read the clock, the network and their
environment. Exactly one opt-in feature depends on it — `cache:` — and writing
`cache:` is **your assertion** that reusing an earlier result for the same
memo key is as good as running the filter again. OrkStack cannot verify it.
This section tells you when that assertion is false for this image. The
inventory below was derived from jq 1.7's own list of builtins (218 entries,
read in the image), not from memory, and is re-derived whenever the `jq~1.7`
constraint moves.

A task that mounts a secret is never cached. This image refuses a task that
mounts one at all, so that rule never applies here.

**Unsafe to cache when** the filter

- reads the **clock** — `now`, jq's only clock read;
- reads the **environment** through `$ENV` or `env` for a variable the
  container runtime sets per container — `HOSTNAME`, each container's own id.
  A variable your task's `env:` sets is part of the memo key, and the image
  defaults `HOME` and `TZ` are fixed by the image digest, which is in the key
  too — so `localtime` and `strflocaltime` under your own `TZ` are covered;
- reads **run coordinates** from `task.json` — `run.id`, `idempotencyKey`,
  `task.attempt` — which the key does not cover (`each.item`, `each.index` and
  `each.count` are covered);
- **randomness** and the **network** do not apply: jq has no random source and
  nothing in jq reads the network.

Everything else is a function of what the key covers: `input`, `inputs`,
`input_filename` and `input_line_number` read or name the staged input files;
`get_jq_origin`, `get_prog_origin`, `get_search_list`, `modulemeta` and the
`import`/`include` directives name or read files of the image (fixed by its
digest) or of the workspace (staged, so covered); `debug` and `stderr` write to
stderr, never to the result.

**Recommended practice**

- `network: none` on every `json` task, so an accidental network dependency
  fails instead of being memoized;
- the values a cached filter depends on passed through `vars` and files, where
  the key sees them;
- `now` only in a task that is not cached;
- cache the tasks **after** an uncached step rather than the step itself: a
  cached task's key covers the content digest of every staged input, so it
  hits exactly when every ancestor reproduced its outputs byte for byte.

## Environment

The environment is yours, and this image honours it like any Unix tool: the
entry script never clears, filters or re-executes it, and jq sees exactly what
the container was started with — the image defaults below, overridden by your
task's `env:`, plus what the container runtime adds (under Docker, `HOSTNAME`,
the container's own id).

| Image default | What it does |
|---|---|
| `HOME=/orkstack/tmp` | a writable, empty, per-attempt home instead of the runtime's read-only `/`. jq sources `$HOME/.jq` into every program when it is a file; the default home holds none. |
| `TZ=UTC` | local time is UTC unless your task says otherwise; the image ships zone data, so `env: {TZ: Europe/Berlin}` really means Berlin. |

- Every variable is visible to a filter as `$ENV.<NAME>` — honoured, not
  hidden. For data a filter computes with, `vars` is usually the better
  channel (typed JSON, visible in the document beside the filter), but reading
  `$ENV` is legitimate.
- A variable your task's `env:` sets is in the memo key; `HOSTNAME` is not (see
  *Caching*).
- This image sets no other variable from `with:`, and refuses none.

## Errors

| Condition | Exit | Message names |
|---|---|---|
| missing / unknown `op` | 2 | the op |
| the task mounts a secret | 1 | why a secret is refused |
| no / several source forms; `slurp: true` without `src`; `srcs: []` | 1 | the four forms; `srcs` |
| a present key of the wrong type, `null` and `false` included | 1 | the key and the type it must have |
| a path empty, outside `/orkstack`, containing `..`, not a regular file | 1 | the path |
| `gather` id malformed / no fan-out items for it / count mismatch | 1 | the id, both counts |
| `vars` over 64 KiB; `filter` missing, empty or over 64 KiB | 1 | the key, the size |
| a jq compile or runtime error | 1 | jq's exit code (jq's own text above it) |
| zero / several outputs | 1 | the count |
| `merge`: fewer than two `srcs`, a file not exactly one object | 1 | the file |
| an unsafe `as` | 1 | the name |

Every failure is an ordinary non-zero exit with the reason on stderr. A
malformed `with:` fails the same way on every attempt, so no retry policy is
needed for one.

## Interface

The image declares its `with:` contract in the
`org.orkstack.activity.interface` label (`docs/ACTIVITY_IMAGES.md`,
"Declaring the interface"), so `orkctl orkflow validate` and apply check a
task's `with:` before anything runs: a missing `filter` or an unknown `op` is
an error, an undeclared key a warning. Two ops, no default op (an unknown or
missing `op` exits `2`). The label, as the Dockerfile carries it:

```dockerfile
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"ops":{"query":{"fields":{"filter":{"type":"string","required":true},"src":{"type":"string"},"slurp":{"type":"boolean","default":false},"srcs":{"type":"array"},"gather":{"type":"string"},"nullInput":{"type":"boolean","default":false},"vars":{"type":"object"},"as":{"type":"string"}}},"merge":{"fields":{"srcs":{"type":"array","required":true},"as":{"type":"string"}}}},"values":{"path":{"type":"string"},"sizeBytes":{"type":"number"},"sha256":{"type":"string"}}}'
```

`result` is deliberately **not** declared under `values`: its JSON type is
whatever the filter returns, and the interface vocabulary has no "any" type.
The declared values are the three keys an `as:` run reports.

## Cancellation

jq runs in the background under a TERM trap (§10.2 rule 2a), so cancelling a
run interrupts a long filter within the grace period rather than being
escalated to SIGKILL; the task then fails with the filter's exit status named.

## What this image does not do

- **no format conversion.** No YAML, CSV, TOML or XML: jq reads JSON.
- **no JSON Schema validation.** A validator is a different tool with its own
  vocabulary of errors.
- **no jq module paths.** No `-L`; one task, one program.
- **no chained filters.** One filter per task. The DAG is the pipeline, and a
  second filter is a second task whose inputs and result are visible in the
  document.
- **no secrets.** A task that mounts one is refused.

## Building

```bash
make -C images image-json
```
