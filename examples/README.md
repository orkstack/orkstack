# Examples

## What these are

Every Orkflow here is a runnable reference document: the test suite compiles
each one, runs every `orkctl run plan --file` line in its comment header as a CI check,
and runs each end to end against a real engine, Orker, Temporal and object
store (all but `advanced-demo`, which needs the internet).
[`docs/ORKFLOW_PATTERNS.md`](../docs/ORKFLOW_PATTERNS.md) is the guide they
illustrate; each example names the sections it shows in a `# Pattern:` line
of its comment header, and the comment header lists everything the example
needs before it runs. Every example begins with its comment header, then the
document header `apiVersion: orkstack/v1` and `kind: Orkflow`.

The eleven examples marked *older* below do not follow every rule of the
guide, and are not edited except for their document header: each pins a
`version:` a deployment may already store, and applying changed bytes under
a stored version is refused — the two lines `apiVersion: orkstack/v1` and
`kind: Orkflow` are the one addition a stored version takes
([`docs/DSL.md`](../docs/DSL.md), "The document header").
Five of them splice a rendered value into shell text or a JSON string —
`http-post`, `shell-eval`, `file-read`, `pinned-build` and `advanced-demo` —
which the guide's "Pass values as data, never as code" says not to do, and
`cached-pipeline` caches a task with no `ttl_seconds` (its one validation
warning). Follow the guide, not them.

## Running them

```sh
make up                                          # the whole local stack
orkctl apply examples/decide-and-route.ork.yaml  # store one example
orkctl run plan decide-and-route --output decide='{"result":{"route":"canary","notify":true}}'
orkctl run submit decide-and-route --watch       # run it, with --input K=V as its comment header says
```

- `orkctl run plan <name> …` plans a stored Orkflow; the plan lines in a
  comment header use `--file examples/<file>` and plan the file without storing it.
- `orkctl task run --file examples/<file> --task <id> …` tries one task
  locally, without a run.
- The UI's Apply page lists the same files as cards; a card's text is the
  example's `description:`.

## The table

| Example | Shows | Activities | Needs |
|---|---|---|---|
| [advanced-demo](advanced-demo.ork.yaml) | *older* — retries, a condition on an exit code, a secret, streamed logs | `shell`, `file` | internet access to `httpbin.org` |
| [approval-gate](approval-gate.ork.yaml) | an approval only when a change is risky; apply, decline, or report a rejection | `json`, `shell`, an awaited task, `notify-block` | the input `notify_url`; `notify-block` applied first; an HTTP endpoint you provide; someone to resolve the approval |
| [awaited-approval](awaited-approval.ork.yaml) | *older* — an awaited task between two steps | `shell`, an awaited task | someone to resolve the approval |
| [branch-and-recover](branch-and-recover.ork.yaml) | *older* — branch, re-join, roll back and escalate | `shell` | nothing |
| [cached-pipeline](cached-pipeline.ork.yaml) | *older* — a cached task, and one that always misses | `shell` | nothing |
| [composed-release](composed-release.ork.yaml) | *older* — composing a block and reading its outputs | `shell`, `release-block` | `release-block` applied first |
| [config-from-git](config-from-git.ork.yaml) | resolve a branch to a commit, render one config per service, cached while the commit holds | `git`, `file`, `json`, `template` | the input `repo`; a git repository over HTTPS and `GIT_TOKEN` exported where the Orker runs |
| [custom-image](custom-image.ork.yaml) | two images of your own, Go and POSIX shell, as durable activities | your own images, `json`, `file` | the images built and pushed |
| [decide-and-route](decide-and-route.ork.yaml) | decide in a json task, branch three ways, re-join | `json`, `shell` | nothing |
| [each-fan-out](each-fan-out.ork.yaml) | a fan-out over a list a json task computes | `json`, `shell` | nothing |
| [file-read](file-read.ork.yaml) | *older* — compress, decompress, checksum | `shell`, `file` | nothing |
| [hello](hello.ork.yaml) | *older* — fetch, transform, checksum | `http`, `shell`, `file` | internet access to `httpbin.org` |
| [http-post](http-post.ork.yaml) | *older* — a POST, and a condition on its status | `http`, `file`, `shell` | internet access to `httpbin.org` |
| [inputs-and-escaping](inputs-and-escaping.ork.yaml) | typed inputs; argv, positional arguments, `sh` and jq `vars` against a hostile default | `shell`, `json`, `file` | the input `release` |
| [notify-block](notify-block.ork.yaml) | a reusable block: one JSON notification, its status as the declared output | `json`, `http` | the inputs `url` and `text`; an HTTP endpoint you provide |
| [order-saga](order-saga.ork.yaml) | a saga over an HTTP API: compensate, report either way | `json`, `http`, `notify-block` | the inputs `api`, `notify_url` and `order`; `notify-block` applied first; an HTTP endpoint you provide |
| [pinned-build](pinned-build.ork.yaml) | *older* — two pinned tasks hand a file over on disk | `shell`, `file` | a pin host, `ORK_ORKER_MAX_PINS` above zero |
| [pinned-checkout](pinned-checkout.ork.yaml) | a git checkout into a pin, validated and packaged in place | `git`, `shell`, `json`, `file` | the input `repo`; a git repository over HTTPS and `GIT_TOKEN` exported where the Orker runs; a pin host, `ORK_ORKER_MAX_PINS` above zero |
| [poll-until-ready](poll-until-ready.ork.yaml) | polling through a retry policy, with no loop | `shell`, `http`, `json` | the inputs `status_url` and `export_url`; an HTTP endpoint you provide |
| [release-block](release-block.ork.yaml) | *older* — the block `composed-release` composes | `shell` | the input `image` |
| [shell-eval](shell-eval.ork.yaml) | *older* — the shell activity's three ops | `shell`, `file` | nothing |
| [automation/examples.ork.yaml](automation/examples.ork.yaml) | the namespace's settings, a schedule and a trigger, declared | an automation document | the recipe below |
| [custom-image/](custom-image/) | an activity image in Go, `FROM scratch` | — | Docker, to build it |
| [custom-image-shell/](custom-image-shell/) | the same activity in POSIX shell | — | Docker, to build it |

## Composing examples

`order-saga` and `approval-gate` compose `notify-block`, and
`composed-release` composes `release-block`. Apply the block first, into the
same namespace:

```sh
orkctl apply examples/notify-block.ork.yaml
orkctl apply examples/order-saga.ork.yaml examples/approval-gate.ork.yaml
```

Apply the block in its own command. Applying several documents at once
(`orkctl apply examples/`) validates every one before writing any, against
the Orkflows already stored — so a block in the same command does not count,
a composing example is refused, and nothing is written.

## The automation document

[`examples/automation/examples.ork.yaml`](automation/examples.ork.yaml)
declares the `examples` namespace's settings, a schedule that runs
`poll-until-ready` every 15 minutes, and a trigger for `approval-gate`. An
Orkflow is validated inside an existing namespace, and a schedule must name a
stored Orkflow version, so:

```sh
orkctl namespace create examples
orkctl apply -n examples examples/notify-block.ork.yaml
orkctl apply -n examples examples/poll-until-ready.ork.yaml examples/approval-gate.ork.yaml examples/automation/examples.ork.yaml --dry-run
orkctl apply -n examples examples/poll-until-ready.ork.yaml examples/approval-gate.ork.yaml examples/automation/examples.ork.yaml
orkctl apply examples/automation/examples.ork.yaml --check     # exits 0: nothing drifted
orkctl schedule fire poll-exports -n examples                  # one fire now
```

The trigger's token is printed once, with a runnable `curl`, and never stored
in the document. Replace the `example.com` URLs with yours.

## The custom images

They are published nowhere: build, push and pin them first, then paste each
printed `…@sha256:…` reference over the tag in `custom-image.ork.yaml`.

```sh
orkctl activity build examples/custom-image --tag localhost:5001/orkstack-examples/custom-image:dev
orkctl activity build examples/custom-image-shell --tag localhost:5001/orkstack-examples/custom-image-shell:dev
```

Each image's README ([Go](custom-image/README.md),
[shell](custom-image-shell/README.md)) states its configuration, output and
interface.
