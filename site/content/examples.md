# Examples

The repository's example Orkflows, what each one shows, and what it needs before it runs. Start with [`each-fan-out`](/docs/examples/each-fan-out/): it needs nothing beyond a running stack.

Each one runs as it stands. Apply its file, then submit the Orkflow by its `name:`, with no inputs. The *Needs* column lists what it does need:

```bash
orkctl apply examples/each-fan-out.ork.yaml
orkctl run submit each-fan-out --watch
```

Each example has its own page, with the file as it is in the repository.

Tasks reach the internet, not your stack, so an example that talks to a service talks to a public one: `httpbin.org`, a public test service, or `github.com/octocat/Hello-World`, a small public repository. Each is an input, so `--input` points it at your own.

## The examples

| Example | What it shows | Needs |
|---|---|---|
| [approval-gate](/docs/examples/approval-gate/) | an approval only when a change is risky; apply, decline, or report a rejection | `notify-block` applied first; internet access to `httpbin.org` |
| [config-from-git](/docs/examples/config-from-git/) | resolve a branch to a commit, render one config per service, cached while the commit holds | internet access to `github.com` |
| [custom-image](/docs/examples/custom-image/) | two images of your own, Go and POSIX shell, as durable activities | your own images, built and pushed |
| [decide-and-route](/docs/examples/decide-and-route/) | decide in a json task, branch three ways, re-join | nothing |
| [each-fan-out](/docs/examples/each-fan-out/) | a fan-out over a list a json task computes | nothing |
| [http-post](/docs/examples/http-post/) | a POST whose JSON body is a YAML mapping, and a condition on its status | internet access to `httpbin.org` |
| [inputs-and-escaping](/docs/examples/inputs-and-escaping/) | typed inputs; argv, positional arguments, `sh` and jq `vars` against a hostile default | nothing |
| [notify-block](/docs/examples/notify-block/) | a reusable block: one JSON notification, its status as the declared output | internet access to `httpbin.org` |
| [order-saga](/docs/examples/order-saga/) | a saga over an HTTP API: compensate, report either way | `notify-block` applied first; internet access to `httpbin.org` |
| [pinned-checkout](/docs/examples/pinned-checkout/) | a git checkout into a pin, validated and packaged in place | internet access to `github.com` |
| [poll-until-ready](/docs/examples/poll-until-ready/) | polling through a retry policy, with no loop | internet access to `httpbin.org` |
| [automation/examples](/docs/examples/automation-examples/) | a namespace's settings, a schedule and a trigger, declared in one automation document | the Orkflows it names, applied first |

## Composed examples

`order-saga` and `approval-gate` compose `notify-block`, so each asks one thing of you: store the block first, in the same namespace, in a command of its own.

```bash
orkctl apply examples/notify-block.ork.yaml
orkctl apply examples/order-saga.ork.yaml
```

```bash
orkctl apply examples/notify-block.ork.yaml
orkctl apply examples/approval-gate.ork.yaml
```

Applying several documents in one command checks every one against the Orkflows already stored. A block in the same command does not count yet, so the composing examples are refused.

Their default paths succeed with nobody there. To see the others:

```bash
# The shipping service refuses: the shipment fails, and the reservation and the payment are undone.
orkctl run submit order-saga --input shipping_api=https://httpbin.org/absent --watch
# A risky change: the run waits for an approval.
orkctl run submit approval-gate --input-json '{"changes":[{"service":"payments","setting":"limit","value":"10"}]}'
```

## The automation document

The automation document in `examples/automation/` declares a description for the `default` namespace, a schedule that runs `poll-until-ready` once a day, and a trigger for `approval-gate`. Store the Orkflows it names first, or apply them in the same command. `approval-gate` composes `notify-block`, so store that first of all:

```bash
orkctl apply examples/notify-block.ork.yaml
orkctl apply examples/poll-until-ready.ork.yaml examples/approval-gate.ork.yaml examples/automation/examples.ork.yaml
orkctl schedule fire poll-exports
```

See [Automation documents](/docs/operate/automation/) for how to apply one.

## The custom images

[`custom-image`](/docs/examples/custom-image/) uses two activity images built from this repository: one in Go, and one in POSIX shell. Nothing publishes them. Build and push them first, as [Build an activity image](/docs/activities/build-an-image/) shows.

The shell image's folder is [`examples/custom-image-shell/`](https://github.com/orkstack/orkstack/tree/main/examples/custom-image-shell). The Go image is in [`examples/custom-image/`](https://github.com/orkstack/orkstack/tree/main/examples/custom-image).

## See also

- [examples/README.md](https://github.com/orkstack/orkstack/blob/main/examples/README.md#the-table): the full table and the commands for each example.
- [docs/ORKFLOW_PATTERNS.md](https://github.com/orkstack/orkstack/blob/main/docs/ORKFLOW_PATTERNS.md): the patterns these examples follow.
