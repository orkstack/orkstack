# Activity images and the `op` convention

An activity is an OCI image named by a task's `uses:` — a catalog name such
as `http`, `shell`, or `file`, or a full registry reference. Every image
implements the public filesystem ABI of the
[OCI Activity Contract](OCI_ACTIVITY_CONTRACT.md); there is no extension
interface inside the Orker, no OrkStack SDK, and no plugin registry. To add
an activity, build an image that reads `/orkstack/input`, writes
`/orkstack/output`, logs to stdout/stderr, and exits normally — following the
Dockerfile and entry-script rules of the contract's §10.1.
`examples/custom-image` and `examples/custom-image-shell` are two such images,
in Go and in POSIX shell, and
[`examples/custom-image/custom-image.ork.yaml`](../examples/custom-image/custom-image.ork.yaml)
is the recipe that builds and runs them; `docs/ORKFLOW_PATTERNS.md`, "When to
write your own image", says when to write one.

This page documents the conventions the first-party reference images share:
the `op` selector, the interface label, `env:` handling, and when caching
them is safe. None of it is part of the platform contract; a third-party
image may choose any configuration shape.

## The `op` convention

An activity image may expose one operation selector, spelled `op`, inside its
`with:` configuration. `op` names what that image does; the other keys are the
arguments for that operation.

```yaml
apiVersion: orkstack/v1
kind: Orkflow
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
      body:
        status: ok
```

The catalog name `http` resolves to the `activity-http` OCI image at apply time.
A task may instead put a registry reference directly in `uses:`. There is no
`container` activity: every activity already is a container, so a custom
workload names its own image directly.

Rules for reference activity authors:

1. Use `op` only when one image genuinely offers several related operations.
2. Use lowercase verbs and reject unknown values before side effects (the
   first-party images exit `2` on an unknown `op`, and on a missing one unless
   the image documents a default — `http` defaults to `get`, `shell` to `exec`).
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
during the apply-time resolution that already pins the digest (OCI Activity
Contract §4.3) and checks every task's `with:` against it at `orkctl orkflow
validate` and at apply (`docs/DSL.md`, validation rule 14). The container never
reads the label, and nothing about it is stored: it is recomputed from the
digest-addressed image on every read (an `InspectActivity` that asks with
`allow_cached` about a pinned digest may get an earlier read of it), so it
can never disagree with what runs.
Beside it, an image MAY draw itself an icon (`org.orkstack.activity.icon`,
"The icon") and describe itself, its ops and its keys in words
(`org.orkstack.activity.docs`, "Describing the activity"), which the API
returns for clients to show; nothing checks a run against them. For a
multi-platform image all three labels are read from the first platform
manifest in index order, so every platform variant should carry the same
labels.

The label is **optional**. An image without it is an ordinary activity,
resolved, applied and run exactly as before; its `with:` is checked by the
image itself at run time (rule 2 above).

### Writing it: `interface.ork.yaml`

The label's value is one line of JSON, which is hard to read and review once
an image has more than a few keys. Write the declaration as an activity
interface document instead, and let `orkctl` pack it into the label. The
document is YAML with the header every authored document has, the keys the
label has (below), and the `description:` and `icon:` keys the other two
labels hold:

```yaml
# interface.ork.yaml, beside the Dockerfile.
apiVersion: orkstack/v1
kind: ActivityInterface
ops:
  copy:
    fields:
      src:
        type: string
        required: true
      as:
        type: string
defaultOp: copy
values:
  path:
    type: string
```

`orkctl activity build <context> --tag <ref> --interface interface.ork.yaml`
packs it into the image it builds, as one `--label` per label — interface,
icon and docs — each replacing a `LABEL` of the same key in the Dockerfile; an
icon or descriptions the document does not declare are passed empty, which
clears a label a base image set. A document it cannot pack builds nothing. Any
other toolchain takes the values `orkctl activity interface pack` prints, one
label per call (`--label interface`, the default, `icon` or `docs`; an
undeclared one prints an empty line):

| Toolchain | Where the value goes |
|---|---|
| `docker build`, `docker buildx build`, `podman build`, `buildah build`, kaniko | `--label "org.orkstack.activity.interface=$(orkctl activity interface pack interface.ork.yaml)"` |
| ko | `--image-label` with the same `key=value` |
| rules_oci | `oci_image`'s `labels` |
| nix `dockerTools` | `config.Labels` |
| a Dockerfile alone | paste each as `LABEL org.orkstack.activity.interface='<value>'` (and `….icon`, `….docs`); a value never holds a single quote, which pack writes as a JSON unicode escape, so it fits a single-quoted line as it is — up to BuildKit's 65,535-byte line, past which only a `--label` carries it |

The value is canonical: one declaration always packs to the same bytes (map
keys sorted, no whitespace), so the label contributes the same bytes to every
build of the image. Pack holds the document to every rule the engine reads a label by, so
a value it prints is never one the engine reports as unreadable, and it writes
`schemaVersion` itself — the lowest version that expresses the document, `1`
today — which the document never names.

The document is held to the authored-document rules: the two header lines;
the keys exactly as the label spells them; `required:` and every `default:`
converted by the strict mapping (`docs/JSON.md`, "Data written in an authored
document"), so `yes`, `0755` or a date is refused naming its path; no anchor,
alias, merge key or tag anywhere; one document per file, at most 1 MiB. An op
that reads no key beyond `fields` is written `name: {}` or `name:` alone.
Editors complete it from [`activityinterface.v1.json`](schemas/activityinterface.v1.json),
which the dispatcher `orkstack.v1.json` selects by `kind`, so the
`*.ork.yaml` mapping of `docs/DSL.md`, "Editor support", covers
`interface.ork.yaml`. It is never applied: `orkctl apply` refuses one named on
its command line and skips one it finds in a directory, naming it on
stderr.

Labels are inherited from a base image: an image built `FROM` a labelled image
declares the base's interface, icon and descriptions until it declares its
own, with `--interface` (which sets all three) or a `LABEL` of each key; an
empty icon or docs label declares none. An image that overrides only the
interface label of a first-party base keeps the base's descriptions, which
then describe keys its interface does not declare: they are reported
unreadable (below), never shown against the wrong keys.

### The label

Its value is the protojson encoding (lowerCamel names) of the
`orkstack.v1.ActivityInterface` message, on one line — what pack prints for the
document above:

```dockerfile
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"ops":{"copy":{"fields":{"as":{"type":"string"},"src":{"type":"string","required":true}}}},"defaultOp":"copy","values":{"path":{"type":"string"}}}'
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
`type` (`string`, `number`, `boolean`, `object`, `array`, or several joined by
`|` for a key that accepts any of them — `activity-http`'s `body` is
`string|object|array`), `required`, and a
documentary `default` the engine never writes into `with:` — and not one word
more: no pattern, range, enum or nesting. A word outside the vocabulary makes
the label unreadable rather than being ignored, and so do `required` together
with a `default`, a `null` default, or a default of the wrong type. A label is
content an untrusted image supplies, so it is bounded: at most 64 KiB of text,
64 ops, and 256 keys in any one `fields`/`values` map; a key matches
`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$` and an op `^[a-z][a-z0-9_-]{0,63}$`.

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
  image is still pinned and runnable. An engine older than the `|` union
  reads a label that uses one as unreadable, so in a fleet that mixes releases
  it warns once and still runs the image; first-party images and engines are
  released together.

`orkctl activity inspect <name-or-reference>` shows what an image declares —
its interface, icon and descriptions — and Admin → Activities shows its
interface. `orkctl activity test <image>` reports the interface beside the
contract's assertions (declared, none or malformed), read as apply reads it;
the label is optional, so the test never fails over it. Every first-party
image carries the three labels, and so does each example image: each one's
`interface.ork.yaml`, beside its Dockerfile, is the source, and
`go run ./internal/schemas/cmd/genschemas` writes the Dockerfile's three
activity `LABEL` lines from it (an undeclared icon or docs as `''`), so a
plain `docker build` of the directory still declares them; a stale line fails
the unit tests. Each README's "Interface"
section links the document.

### Describing the activity

A document may say what the activity does, what each op does, and what each
`with:` key and `values.json` key means, each with a `description:` beside
its declaration:

```yaml
apiVersion: orkstack/v1
kind: ActivityInterface
description: >-
  Copies one file into an output artifact.
ops:
  copy:
    description: Copies one file.
    fields:
      src:
        type: string
        required: true
        description: >-
          The file to copy, under `/orkstack/input`.
values:
  path:
    type: string
    description: The artifact the copy wrote.
```

Pack writes the descriptions into their own label, `org.orkstack.activity.docs`,
never into the interface label, whose vocabulary stays exactly `inputs:`'s.
Its value is the protojson of `orkstack.v1.ActivityDocs` — `schemaVersion`
(`1`), the activity's `description`, `fields` (shared key → text), `ops` (op →
its `description` and its `fields`) and `values` (key → text):

```dockerfile
LABEL org.orkstack.activity.docs='{"schemaVersion":1,"description":"Copies one file into an output artifact.","ops":{"copy":{"description":"Copies one file.","fields":{"src":"The file to copy, under `/orkstack/input`."}}},"values":{"path":"The artifact the copy wrote."}}'
```

A description is **one paragraph of plain text**. Text between a pair of
backticks on it, non-empty and with no backtick inside, is code — a key, an
op, a path — which a client may show in a code face; nothing else is
interpreted, so markup and links are text. Write a long one as a folded block
scalar (`>-`), whose lines join into one paragraph; pack trims each. A
description must be valid UTF-8 and not empty, and hold no control character
— a line break, a tab or a carriage return included — no format character
(bidirectional marks and overrides, zero-width and invisible characters, tag
characters), which would show text other than it reads, and no noncharacter.
Bounds, in characters: the activity's description at most 1,000, an op's,
a key's or a value's at most 500; the label at most 64 KiB, with the
interface's own bounds on ops, keys and their names. The implied `op` key is
not described: the activity's and each op's descriptions cover it. Every
first-party image describes itself, each op and every key it declares.

The engine checks the descriptions against the interface the same image
declares: an op, key or value it describes must be one that interface
declares, in the same place. When the image carries no interface label, or
one this engine cannot read, the descriptions are read unchecked — the labels
fail independently.

### The icon

A document may draw the activity an icon — one small SVG, drawn in the
current colour on a 16 by 16 grid, as `activity-file`'s page is:

```yaml
icon: |-
  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><path d="M4 1.8h5L12.5 5.3v8.9H4z" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linejoin="round"/><path d="M9 1.8v3.5h3.5" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linejoin="round"/></svg>
```

An image without an icon omits the key: an empty `icon:` is refused, as an
empty `description:` is.

Draw it at 16 px with a 1.4 stroke, round caps and joins, and 1 px of
padding on every side, as the first-party icons are; a client is expected to
draw it at 15 to 24 px, in its own text colour. Pack checks it and writes it into
`org.orkstack.activity.icon` as one canonical line — double quotes,
attributes in their order, comments, the XML declaration, `title` and `desc`
dropped:

```dockerfile
LABEL org.orkstack.activity.icon='<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><path d="M4 1.8h5L12.5 5.3v8.9H4z" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linejoin="round"/><path d="M9 1.8v3.5h3.5" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linejoin="round"/></svg>'
```

An icon may hold only this, and anything else refuses the whole icon — a
client then draws the default one, never part of an icon:

| Where | Allowed |
|---|---|
| the root `<svg>` | `xmlns` (the SVG namespace only), `viewBox` exactly `0 0 16 16`, `width` and `height` (dropped), presentation attributes |
| elements below it | `path`, `circle`, `ellipse`, `rect`, `line`, `polyline`, `polygon`, `g` |
| presentation attributes, on any of them | `fill` and `stroke` (`none` or `currentColor`), `stroke-width` (0 to 4), `stroke-linecap` (`butt`, `round`, `square`), `stroke-linejoin` (`miter`, `round`, `bevel`), `fill-rule` and `clip-rule` (`nonzero`, `evenodd`) |
| geometry, only where it belongs | `path`: `d` (SVG path data); `circle`: `cx`, `cy`, `r`; `ellipse`: `cx`, `cy`, `rx`, `ry`; `rect`: `x`, `y`, `width`, `height`, `rx`, `ry`; `line`: `x1`, `y1`, `x2`, `y2`; `polyline` and `polygon`: `points` (number pairs); `g`: `transform` (`matrix`, `translate`, `scale`, `rotate`, `skewX`, `skewY`) |

Numbers are plain — a sign, digits, a fraction, an exponent; no unit, no
`%` — and lie between -10,000 and 10,000; a radius, width or height is never
negative. Comments, the XML declaration, space between elements, and `title`
and `desc` with their text are dropped. Everything else is refused: text,
`use` and every `href`, `image`, `text`, `style` and `class`, `id`, scripts
and event attributes, gradients, patterns, filters, masks, clip paths,
markers, animation, `opacity`, any colour but `currentColor`, a DOCTYPE,
other processing instructions, CDATA, namespaced attributes, and an attribute
given twice. Bounds: at most 2,048 bytes, 32 elements below the root, and `g`
nested 4 deep.

The API returns the icon as a tree of those elements and attributes, never
as markup; a client builds the drawing from it, drawing the root itself as
`<svg viewBox="0 0 16 16" fill="currentColor">` plus the icon's own root
attributes. An element with no `fill` of its own is therefore filled in the
text colour, never in SVG's default black: give a line or an outline
`fill="none"`.

### The default icon

For an image without an icon — no label, an empty one, one the engine cannot
read, or a reference that did not resolve — a client draws the default
activity icon, a container. It is never put in a label:

```yaml
icon: |-
  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><path d="M8 1.8 13.5 5v6L8 14.2 2.5 11V5zM2.5 5 8 8.2 13.5 5M8 8.2v6" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linejoin="round"/></svg>
```

### Unreadable labels and older engines

Each of the three labels is read on its own: an icon or descriptions the
engine cannot read never cost the interface, nor each other. Such a label is
reported, with the reason, as the activity's `icon_error` or `docs_error` and
in `orkctl activity inspect` (`malformed — …`); it never stops a document
from validating or a run from starting, and it is never shown raw. An engine
older than the icon and docs labels reads the interface label alone, which
they leave byte-for-byte unchanged, so an image that carries them declares the
same interface to every release. An `orkctl` older than them refuses a
document that uses `description:` or `icon:`.

## First-party reference images

The configuration and output contract for each image lives beside its
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
`/orkstack/output/artifacts/`, through the same ABI as any third-party image.
`json` and `template` refuse a task that mounts a secret at all: a filter or
template can read any file it can name, and its result becomes a value or an
artifact in object storage.

### The environment

Every first-party image honours a task's `env:` like any Unix tool (OCI
Activity Contract §3.7): the entry script never clears, filters or re-executes
the environment, so the tool sees the image's `ENV` defaults, overridden by
the task's `env:`, plus what the container runtime adds (under Docker,
`HOSTNAME`). Images whose tool wants a home set `HOME=/orkstack/tmp` (the root
filesystem is read-only), and images that read the clock set `TZ=UTC`. The
`json`, `git` and `template` READMEs' *Environment* sections list each image's
defaults, what overrides them, and the one variable an image refuses
(`activity-git`: `GIT_TRACE_REDACT` set false while a token is in use).

## Network and caching of the first-party images

Nothing in OrkStack expects a task to produce the same output from the same
input. Tasks run at least once and may read the clock, the network and their
environment; determinism is required of the engine's own workflow code only,
never of a task. Exactly one opt-in feature depends on it — `cache:` — and
writing `cache:` is the author's assertion, which OrkStack cannot verify,
that reusing an earlier result for the same memo key is as good as running the
task again. The rules and the table below tell you when that assertion is
false for each first-party image; each image's README has the detail.

1. **Write `cache:` only when the result depends on nothing outside the memo
   key** ([DSL reference](DSL.md), "What the key covers": the image,
   `command`, `env`, the secrets' names and sources, `network`, the rendered
   `with:`, the run input, and the content of every staged input). The key
   does not cover the clock, randomness, the network, a variable the container
   runtime sets per container (`HOSTNAME`), the run's coordinates in
   `task.json` (run id, idempotency key, attempt), or a git ref that moves.
2. **A task that mounts a secret is never cached** — `git` included. A cache
   hit reads no secret, so it never exercises the credential: after a token is
   revoked or a repository's access removed, a cached private clone would keep
   handing out the tree until its memo expired (up to `ORK_CACHE_MAX_TTL`) or
   someone purged it, and the key covers a secret's name and source, never its
   value, so rotating the token changes nothing. Apply warns on every cached
   task that mounts a secret.
3. **Cache the tasks after an uncached step instead.** A cached task's key
   covers the content digest of every staged input — every succeeded
   ancestor's values and artifacts, not only its direct needs' — so it hits
   exactly when every ancestor reproduced its outputs byte for byte. A private
   `git` clone by commit id does: the `untar`, `render` and `query` tasks after
   it hit on every run of an unchanged commit while the clone itself
   re-fetches and re-checks the token. An ancestor whose output varies is not
   an error; its successors simply miss.
4. **`network: none` on every task that should not need the network**, so an
   accidental network dependency fails instead of being memoized; values
   travel through `with:` and files, where the key sees them.
5. **Side effects belong to uncached tasks**, and use the task's
   `idempotencyKey`, because execution is at-least-once.

| Image / op | Needs network | `cache:` is safe when | Unsafe to cache when |
|---|---|---|---|
| `json` (both ops) | no — write `network: none` | the filter does not call `now`, reads no runtime-set variable (`HOSTNAME`) through `$ENV`/`env`, and reads no run coordinate from `task.json` | it calls `now`, reads `$ENV.HOSTNAME`, or reads `run.id`, `idempotencyKey` or `task.attempt` |
| `template` `render` | no — write `network: none` | the template calls none of the functions its README's *Caching* section lists | it reads the clock, randomness, a runtime-set variable such as `HOSTNAME`, the network or undeclared files |
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
