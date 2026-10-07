# Activity images and the `op` convention

An activity is an OCI image named by a task's `uses:` — a catalog name such
as `http`, `shell`, or `file`, or a full registry reference. Every image
implements the public filesystem ABI of the
[OCI Activity Contract](OCI_ACTIVITY_CONTRACT.md); there is no extension
interface inside the Orker, no OrkStack SDK, and no plugin registry. To add
an activity, build an image that reads `/orkstack/input`, writes
`/orkstack/output`, logs to stdout/stderr, and exits normally.

This page documents one convention the first-party reference images share.
It is not part of the platform contract.

An activity image may expose one operation selector, spelled `op`, inside its
`with:` configuration. `op` names what that image does; the other keys are the
arguments for that operation. This is an image-level convention, not part of
the OCI Activity Contract: third-party images may choose any configuration
shape and need no OrkStack SDK.

```yaml
name: notify
inputs:
  url:
    type: string
    required: true
tasks:
  - id: publish
    uses: http
    with:
      op: post
      url: "{{ .inputs.url }}"
      headers:
        content-type: application/json
      body: '{"status":"ok"}'
```

The catalog name `http` resolves to the `activity-http` OCI image at apply time.
A task may instead put a registry reference directly in `uses:`. There is no
`container` activity: every activity already is a container, so a custom
workload names its own image directly.

## Rules for reference activity authors

1. Use `op` only when one image genuinely offers several related operations.
2. Use lowercase verbs and reject unknown values before side effects.
3. Document whether an op is the default; do not silently guess when no choice
   is clearly safest.
4. Keep arguments minimal and document output values and artifact files.
5. Use ordinary exit status: zero succeeds and non-zero fails. An image such as
   `activity-shell` may explicitly define an operation whose child exit status
   is returned as data, but that is the image's contract, not a platform magic
   exit code.

## Declaring the interface

An image MAY write down its `with:` contract in one OCI image label,
`org.orkstack.activity.interface`. The engine reads it from the image's config
during the apply-time resolution that already pins the digest
([OCI Activity Contract](OCI_ACTIVITY_CONTRACT.md) §4.3 step 7) and checks every
task's `with:` against it at `orkctl orkflow validate` and at apply (DSL
validation rule 14). The container never reads the label, and nothing about it
is stored: it is recomputed from the digest-addressed image on every read.

The label is **optional**. An image without it — every image built before it
existed — is an ordinary activity, resolved, applied and run exactly as
before; its `with:` is checked by the image itself at run time (rule 2 above).

Its value is the protojson encoding (lowerCamel names) of the
`orkstack.v1.ActivityInterface` message, verbatim, on one line:

```dockerfile
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"ops":{"copy":{"fields":{"src":{"type":"string","required":true},"as":{"type":"string"}}}},"defaultOp":"copy","values":{"path":{"type":"string"}}}'
```

- `schemaVersion` is `1`.
- `fields` are the top-level `with:` keys every op reads (for an image with no
  `op`, the whole `with:` block).
- `ops`, when present, says the image follows the `op` convention: each op's
  `fields` are the keys it reads in addition to `fields`. `op` is then
  implied and must not appear in `fields`; a key is declared once, shared or
  under an op, never both.
- `defaultOp` is the op the image runs when `op` is absent; with `ops` and no
  `defaultOp`, `op` is required.
- `values` is documentary: the keys the image writes to `values.json`.

Each key is declared with exactly the vocabulary an Orkflow's `inputs:` has —
`type` (`string`, `number`, `boolean`, `object`, `array`), `required`, and a
documentary `default` the engine never writes into `with:` — and not one word
more: no pattern, range, enum or nesting, and a word outside the vocabulary
makes the label unreadable rather than being ignored.

What a document is told:

- a required key that is not supplied, an `op` the image does not declare, or
  a missing `op` with no default is an **error** — the image would refuse it
  at run time, so apply refuses it first and nothing is stored;
- a key the interface does not declare is a **warning**, never an error — the
  image may accept more than it declares;
- a literal whose JSON type disagrees with the declared type is a
  **warning**; a templated value (`{{ … }}`) is never typed, because the
  Orker renders every template to a string;
- a label the engine cannot read is one warning on the task's `uses:`, and the
  image is still pinned and runnable.

`orkctl activity inspect <name-or-reference>` and Admin → Activities show what
an image declares. Every first-party image carries the label; each README's
"Interface" section reproduces it.

## First-party reference images

The current configuration and output contract for each image lives beside its
Dockerfile:

- [`activity-http`](../images/activity-http/README.md) — HTTP requests;
- [`activity-shell`](../images/activity-shell/README.md) — argv, scripts, and
  evaluations;
- [`activity-file`](../images/activity-file/README.md) — small Unix-like file
  operations;
- [`activity-json`](../images/activity-json/README.md) — one jq filter over JSON
  files, the run input, a fan-out's items or nothing at all, or a deep merge:
  the default compute step;
- [`activity-git`](../images/activity-git/README.md) — a read-only HTTPS
  checkout (a source tarball, or a working tree in a pin) and ref → commit
  resolution, with the token kept out of every argument, URL and log;
- [`activity-template`](../images/activity-template/README.md) — one text
  artifact rendered with gomplate from a template file and JSON/YAML
  datasources.

Every one of them reads rendered configuration from
`/orkstack/input/config.json`, writes compact values to
`/orkstack/output/values.json`, and writes regular-file artifacts beneath
`/orkstack/output/artifacts/`. They use the same ABI as any third-party image.
Every first-party image honours a task's `env:` like any Unix tool — nothing is
cleared or silently overridden — and the `json`, `git` and `template` READMEs'
*Environment* sections list each image's defaults and what overrides them.
See the [OCI Activity Contract](OCI_ACTIVITY_CONTRACT.md) for the authoritative
platform contract and [the DSL reference](DSL.md) for task syntax.

## Network and caching of the first-party images

Nothing in OrkStack expects a task to produce the same output from the same
input. Tasks run at least once and may read the clock, the network and their
environment; determinism is required of the engine's own workflow code only,
never of a task. Exactly one opt-in feature depends on it — `cache:` — and
writing `cache:` is the author's assertion that reusing an earlier result for
the same memo key is as good as running the task again. The rules and the
table below tell you when that assertion is false for each first-party image;
each image's README has the detail.

1. **`cache:` is your assertion**, which OrkStack cannot verify ([DSL
   reference](DSL.md), "Caching"). Write it only when a reused result is as
   good as a fresh one — when the result depends on nothing outside what the
   memo key covers ("What the key covers": the image, `command`, `env`, the
   secrets' names and sources, `network`, the rendered `with:`, the run input,
   and the content of every staged input). What the key does not cover makes a
   task unsafe to cache: the clock, randomness, the network, a variable the
   container runtime sets per container (`HOSTNAME`), the run's coordinates in
   `task.json` (run id, idempotency key, attempt), and a git ref that moves.
2. **A task that mounts a secret is never cached** — `git` included. A cache
   hit reads no secret, so it never exercises the credential: after a token is
   revoked or a repository's access is removed, a cached private clone would
   keep handing out the tree until its memo expired (up to `ORK_CACHE_MAX_TTL`)
   or someone purged it, and the key covers a secret's name and source, never
   its value, so rotating the token changes nothing. Apply already warns on
   every cached task that mounts a secret; this rule makes "don't" the
   practice rather than "purge on revoke".
3. **Cache the tasks after an uncached step instead.** A cached task's key
   covers the content digest of every staged input — and a task is staged
   every succeeded ancestor's values and artifacts, not only its direct
   needs' — so it hits exactly when every ancestor reproduced its outputs byte
   for byte. A private `git` clone by commit id does: the `untar`, `render`
   and `query` tasks after it hit on every run of an unchanged commit while the
   clone itself re-fetches and re-checks the token. An ancestor whose output
   varies is not an error; its successors simply miss.
4. **`network: none` on every task that should not need the network**, so an
   accidental network dependency fails instead of being memoized; values
   travel through `with:` and files, where the key sees them.
5. **Side effects belong to uncached tasks**, and use the task's
   `idempotencyKey`, because execution is at-least-once.

| Image / op | Needs network | `cache:` is safe when | Unsafe to cache when |
|---|---|---|---|
| `json` (both ops) | no — write `network: none` | the filter does not call `now`, reads no runtime-set variable (`HOSTNAME`) through `$ENV`/`env`, and reads no run coordinate from `task.json` (rule 2 never applies: the image refuses secrets) | it calls `now`, reads `$ENV.HOSTNAME`, or reads `run.id`, `idempotencyKey` or `task.attempt` |
| `template` `render` | no — write `network: none` | the template calls none of the functions its README's *Caching* section lists (rule 2 never applies: the image refuses secrets) | it reads the clock, randomness, a runtime-set variable such as `HOSTNAME`, the network or undeclared files |
| `git` `clone` (`as`) | yes | `ref` is a **full commit id** (resolve first, then clone `{{ .tasks.<resolve>.output.commit }}`) **and the task mounts no secret** — a public repository; a `caFile` read from an upstream artifact is an input, not a secret | `ref` names a branch, tag or `HEAD` (a hit would keep returning the old tree until the TTL); **or** it mounts a secret — `tokenSecret`, or `caFile: secrets/…` (rule 2): cache the tasks after it instead (rule 3) |
| `git` `resolve` | yes | never | always — its job is to observe a moving ref |
| `git` `clone` with `dest` | yes | never (a pinned task is never cached) | — |
| `file` (all ops) | no — write `network: none` | it mounts no secret | it mounts a secret (rule 2). Caching a `tar` task is safe, but `tar`'s output is not byte-reproducible — its members carry the time each file was staged — so tasks downstream of a `tar` never hit (rule 3); pass a directory's files on, or archive at the end of a pipeline |
| `http` | yes | never — a request observes or changes something outside | always |
| `shell` | depends | only for a script whose result depends on nothing the key does not cover, and that mounts no secret | it reads the clock, the network, randomness or `HOSTNAME`, has side effects, or mounts a secret |

A `git` tarball is reproducible per commit and **platform image** under the
image's default environment: a pool that mixes architectures produces one
digest per architecture, so the cached tasks after the clone miss when a clone
lands on the other kind of Orker (`activity-git`'s README, "`as` — a source
tarball").
