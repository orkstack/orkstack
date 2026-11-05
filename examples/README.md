# Examples

## What these are

Every Orkflow here runs as it stands: apply it, submit it with no inputs, and it
reaches the result its comment header states. There is no input to find out, no
secret to supply and no service to stand up. The table's *Needs* column lists
what each one does need. Two ask one thing more: `order-saga` and `approval-gate`
compose a block you store first. The one Orkflow that runs images you build is a
recipe beside their build, [`custom-image/custom-image.ork.yaml`](custom-image/custom-image.ork.yaml),
and not one of these.

Each is a runnable reference document: the test suite compiles each one, runs
every `orkctl run plan --file` line in its comment header as a CI check, and runs
each end to end against a real engine, Orker, Temporal and object store.
[`docs/ORKFLOW_PATTERNS.md`](../docs/ORKFLOW_PATTERNS.md) is the guide they
illustrate; each example names the sections it shows in a `# Pattern:` line
of its comment header, and the comment header lists everything the example
needs before it runs. Every example begins with its comment header, then the
document header `apiVersion: orkstack/v1` and `kind: Orkflow`.

Every example pins its `version:`, and a store refuses different bytes under
a version it already holds ([`docs/DSL.md`](../docs/DSL.md), "The document
header"). On 2026-10-27 every example's YAML was rewritten in block style,
one `- item` or `key: value` per line, and no definition changed: each
compiles to the digest it had. A store that applied an example before then
refuses the new file under the same version, so apply it to a fresh store
or under another version. An example changed since then has a new version, and
applies beside the one a store already holds.

## Running them

Start a stack first, from the repository's root: the quickstart,
`docker compose up --build -d --wait` with `orkctl` from its image
([Getting started](../docs/GETTING_STARTED.md)), or `make up`
([CONTRIBUTING.md](../CONTRIBUTING.md), "The local development stack"). Then:

```sh
orkctl apply examples/decide-and-route.ork.yaml  # store one example
orkctl run plan decide-and-route --output decide='{"result":{"route":"canary","notify":true}}'
orkctl run submit decide-and-route --watch       # run it: it needs no inputs
```

- `orkctl run plan <name> …` plans a stored Orkflow; the plan lines in a
  comment header use `--file examples/<file>` and plan the file without storing it.
- `orkctl task run --file examples/<file> --task <id> …` tries one task
  locally, without a run.
- The UI's Apply page lists the same files as cards; a card's text is the
  example's `description:`.

Task containers reach the internet, not this stack, so an example that talks to
a service talks to a public one. The HTTP examples default to `httpbin.org`, a
public test service, and the git examples to `github.com/octocat/Hello-World`, a
small public repository. Each is an input, so `--input` points it at your own. An
answer of any status is data, so an outage of `httpbin.org` is not a platform
failure: `order-saga` takes its failure path and compensates, and `poll-until-ready`
keeps polling until its retries run out.

## The table

| Example | Shows | Activities | Needs |
|---|---|---|---|
| [approval-gate](approval-gate.ork.yaml) | an approval only when a change is risky; apply, decline, or report a rejection | `json`, `shell`, an awaited task, `notify-block` | `notify-block` applied first; internet access to `httpbin.org` |
| [config-from-git](config-from-git.ork.yaml) | resolve a branch to a commit, render one config per service, cached while the commit holds | `git`, `file`, `shell`, `json`, `template` | internet access to `github.com` |
| [decide-and-route](decide-and-route.ork.yaml) | decide in a json task, branch three ways, re-join | `json`, `shell` | nothing |
| [each-fan-out](each-fan-out.ork.yaml) | a fan-out over a list a json task computes | `json`, `shell` | nothing |
| [http-post](http-post.ork.yaml) | a POST whose JSON body is a YAML mapping, and a condition on its status | `http`, `file`, `shell` | internet access to `httpbin.org` |
| [inputs-and-escaping](inputs-and-escaping.ork.yaml) | typed inputs; argv, positional arguments, `sh` and jq `vars` against a hostile default | `shell`, `json`, `file` | nothing |
| [notify-block](notify-block.ork.yaml) | a reusable block: one JSON notification, its status as the declared output | `json`, `http` | internet access to `httpbin.org` |
| [order-saga](order-saga.ork.yaml) | a saga over an HTTP API: compensate, report either way | `json`, `http`, `notify-block` | `notify-block` applied first; internet access to `httpbin.org` |
| [pinned-checkout](pinned-checkout.ork.yaml) | a git checkout into a pin, validated and packaged in place | `git`, `shell`, `json`, `file` | internet access to `github.com` |
| [poll-until-ready](poll-until-ready.ork.yaml) | polling through a retry policy, with no loop | `shell`, `http`, `json` | internet access to `httpbin.org` |
| [automation/examples.ork.yaml](automation/examples.ork.yaml) | the namespace's settings, a schedule and a trigger, declared | an automation document | the recipe below |
| [custom-image/](custom-image/) | an activity image in Go, `FROM scratch`, and the recipe that runs it with its shell twin | the recipe, your own images, `json`, `file` | Docker, to build it; the recipe below |
| [custom-image-shell/](custom-image-shell/) | the same activity in POSIX shell | — | Docker, to build it |

## Composing examples

`order-saga` and `approval-gate` compose `notify-block`, so each asks one thing of
you: store the block first, in the same namespace, in a command of its own.

```sh
orkctl apply examples/notify-block.ork.yaml
orkctl apply examples/order-saga.ork.yaml
```

```sh
orkctl apply examples/notify-block.ork.yaml
orkctl apply examples/approval-gate.ork.yaml
```

Apply the block in its own command. Applying several documents at once
(`orkctl apply examples/`) validates every one before writing any, against
the Orkflows already stored — so a block in the same command does not count,
a composing example is refused, and nothing is written.

Their default paths succeed with nobody there. To see the others:

```sh
# The shipping service refuses: the shipment fails, and the reservation and the payment are undone.
orkctl run submit order-saga --input shipping_api=https://httpbin.org/absent --watch
# A risky change: the run waits for an approval. Resolve it with the run id this prints.
orkctl run submit approval-gate --input-json '{"changes":[{"service":"payments","setting":"limit","value":"10"}]}'
orkctl run resolve <run> --task approval --values '{"approved":true,"approver":"dana"}'
```

## The automation document

[`examples/automation/examples.ork.yaml`](automation/examples.ork.yaml)
declares a description for the `default` namespace, a schedule that runs
`poll-until-ready` once a day, and a trigger for `approval-gate`. A schedule or
trigger must name an Orkflow that is stored or applied by the same command, and
`approval-gate` composes `notify-block`, so:

```sh
orkctl apply examples/notify-block.ork.yaml
orkctl apply examples/poll-until-ready.ork.yaml examples/approval-gate.ork.yaml examples/automation/examples.ork.yaml --dry-run
orkctl apply examples/poll-until-ready.ork.yaml examples/approval-gate.ork.yaml examples/automation/examples.ork.yaml
orkctl apply examples/automation/examples.ork.yaml --check     # exits 0: nothing drifted
orkctl schedule fire poll-exports                              # one fire now
orkctl schedule delete poll-exports                            # when you are done with it
```

The trigger's token is printed once, with a runnable `curl`, and never stored
in the document.

## The custom images

A recipe, not an example of the gallery:
[`custom-image/custom-image.ork.yaml`](custom-image/custom-image.ork.yaml) runs two
images of your own, in Go and in POSIX shell, as durable activities, so it asks
these steps of you. Nothing publishes the images, so build, push and pin them
first. The registry is the quickstart's own, at `127.0.0.1:5002`; `make up`'s is at
`localhost:5001`. `orkctl activity build` runs Docker, which the quickstart's
`orkctl` image does not have, so use an `orkctl` on your own machine.

```sh
orkctl activity build examples/custom-image --tag 127.0.0.1:5002/orkstack-examples/custom-image:dev
orkctl activity build examples/custom-image-shell --tag 127.0.0.1:5002/orkstack-examples/custom-image-shell:dev
```

Each prints its pinned `…@sha256:…` reference. Paste each one over its tag in the
recipe, then:

```sh
orkctl apply examples/custom-image/custom-image.ork.yaml
orkctl run submit custom-image --watch
```

Each image's README ([Go](custom-image/README.md),
[shell](custom-image-shell/README.md)) states its configuration, output and
interface.
