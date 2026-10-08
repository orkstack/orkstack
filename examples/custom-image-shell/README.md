# custom-image-shell

## What this is

A user's own activity image, written in POSIX `sh` (`entrypoint.sh`) on a
pinned alpine base, with `jq` as its JSON library — the role `encoding/json`
plays for the Go image. It is **not** a first-party activity: it has no catalog
name, names no `op`, and links nothing of OrkStack's. It is an activity because
it reads the files the platform writes under `/orkstack/input/`, writes the
files the platform reads under `/orkstack/output/`, and exits with an ordinary
status — the whole contract (`docs/OCI_ACTIVITY_CONTRACT.md`).

[`../custom-image`](../custom-image/) is the same activity in Go. Given the
same configuration and the same ASCII source file, the two write
byte-identical reports: a custom activity is an image, not a language.

## Configuration

`/orkstack/input/config.json` is the task's rendered `with:` block. Every key
is optional, and an unknown key is ignored.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `label` | string | — | the label to report |
| `src` | string | — | a file to report on; without it, no report is written |
| `as` | string | `report.txt` | the report artifact's name |

The **label** is `with.label` when it is set and not empty; otherwise, for one
item of a fan-out, the item itself when it is a JSON string (`task.json`'s
`each.item`); otherwise there is none. So a fan-out over a list of strings
needs no `.each` in its `with:` at all.

From `task.json` it reads `schemaVersion` (it must be `1`), `idempotencyKey`
and `task.id` (both required), `task.attempt`, and `each` when present.

## Output

`/orkstack/output/values.json`:

```json
{"idempotencyKey":"examples/ork-…/report","attempt":1,"taskId":"report",
 "argv":["{{ .inputs.label }}","is-not-rendered"],"label":"no sdk required",
 "source":"/orkstack/input/upstream/probe/values.json","artifact":"report.txt",
 "stats":{"lines":1,"words":1,"bytes":62}}
```

- `idempotencyKey`, `attempt`, `taskId` and `argv` are always present; `argv`
  is the container's arguments — the task's top-level `command:`, which the
  Orker passes verbatim and **never renders** — and `[]` when there are none.
- `label` only when there is one.
- `source`, `artifact` and `stats` (`lines`, `words`, `bytes`) only with `src`.
- `each` (`index`, `count`, and `item` as the exact JSON `task.json` carries)
  only for one item of a fan-out.

With `src`, the report artifact is written first, mode `0644`, and is exactly
seven lines:

```text
activity report
label:   no sdk required
source:  /orkstack/input/upstream/probe/values.json
argv:    ["{{ .inputs.label }}","is-not-rendered"]
lines:   1
words:   1
bytes:   62
```

`lines` counts every newline, plus one for a last line without one; `words`
counts runs of bytes that are not ASCII whitespace; `bytes` is the file's
size. `argv` is compact JSON.

## Paths

`src` follows the `file` activity's rules: a relative path resolves under
`/orkstack/input/` (`upstream/<task>/values.json` is a succeeded ancestor's
values); an absolute path must be inside `/orkstack/`; a `..` component is
refused, never normalized; and the result must be a regular file — a
directory, a device or a symbolic link is refused. `as` must be non-empty, at
most 128 bytes, contain no `/`, and not be `.` or `..`.

## Errors

Any error exits `1` with one line on stderr, `custom-image-shell: <reason>`:

| Reason | When |
|---|---|
| `task.json has schemaVersion <v>; this image understands 1` | a `task.json` written for another major version |
| `task.json has no idempotencyKey or task.id` | either is missing |
| `source path may not contain '..': <src>` | a `..` component |
| `source path must be inside /orkstack: <src>` | an absolute path elsewhere |
| `not a regular file: <src>` | a directory, a device, a symbolic link |
| `artifact name may not …`, `artifact name is <n> bytes, limit is 128` | a bad `as` |

There are no other exit codes: retrying is the task's `retry:` policy.

## Interface

The image declares its `with:` contract in one label, which validate and apply
read; the container never does:

```dockerfile
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"fields":{"label":{"type":"string"},"src":{"type":"string"},"as":{"type":"string","default":"report.txt"}},"values":{"idempotencyKey":{"type":"string"},"attempt":{"type":"number"},"taskId":{"type":"string"},"label":{"type":"string"},"argv":{"type":"array"},"source":{"type":"string"},"artifact":{"type":"string"},"stats":{"type":"object"},"each":{"type":"object"}}}'
```

There are no `ops`, so the whole `with:` block is its `fields`, and
`orkctl activity inspect <reference>` heads it `schema 1 · 3 keys`.

## Build, push and pin

The image is published nowhere. Build and push it to a registry your Orkers
can pull from, then pin it:

```sh
orkctl activity build examples/custom-image-shell --tag localhost:5001/orkstack-examples/custom-image-shell:dev
```

The command prints the pinned reference (`…@sha256:…`); paste it over the tag
in `examples/custom-image.ork.yaml`. A tag is resolved to a digest at apply
anyway, but writing the digest makes the document say the same thing
tomorrow. A locally built image that was never pushed has no registry digest,
and apply refuses it.

## Image policy

The image is subject to the deployment's and the namespace's image policy like
any other (`docs/DSL.md`, "Tasks"): a policy with `require_digest` refuses the
tag, so paste the digest; `allowed_registries` must admit the registry you
pushed to; and a namespace with `require_signed_by` needs the image signed by
one of its keys.

## Try one task

Run one task locally, without a run, against a supplied upstream:

```sh
orkctl task run --file examples/custom-image.ork.yaml --task report --output probe='{"idempotencyKey":"x","attempt":1,"taskId":"probe","argv":[]}'
```

`orkctl activity test` drives an image through the first-party `op: exec`
convention — it asks the image to run a command of the suite's choosing — and
this image runs no commands: it ignores those keys. Its assertions do not
describe this image; test it with `orkctl task run` and a run instead.

## Signals

None are handled, on purpose: the work takes milliseconds, and the Orker
starts every container with an init process, so the script is not PID 1 and
SIGTERM's default action ends it. The Dockerfile's exec-form `ENTRYPOINT` makes
the script itself the process that receives the signal. An image whose work is
long handles SIGTERM to clean up within the task's cancellation grace period
(30 seconds by default, then SIGKILL). A shell does that by running the work in
the background under a `TERM` trap that forwards the signal and waits —
`images/activity-shell/entry.sh` shows the shape
(`docs/OCI_ACTIVITY_CONTRACT.md` §10.1 rule 2a, and §10.2 for the pattern).

## Environment

The image reads no environment variable. A task's `env:` reaches the process
like it reaches any Unix program, and an image of your own may well read it
for its configuration — `env:` is a valid, expected way to configure an
activity (`docs/ORKFLOW_PATTERNS.md`, "Environment variables are
configuration"). Per-run data still belongs in `with:`, and a secret under
`secrets:`.
