# Examples

The repository's example Orkflows, what each one shows, and what it needs before it runs. Start with [`each-fan-out`](/docs/examples/each-fan-out/): it needs nothing beyond a running stack.

Each one runs as it stands. Create its Orkflow from its file, then submit the Orkflow by its `name:`, with no inputs. The *Needs* column lists what it does need:

```bash
orkctl orkflow create examples/each-fan-out.ork.yaml
orkctl run submit each-fan-out -w
```

Each example has its own page, with the file as it is in the repository.

Tasks reach the internet, not your stack, so an example that talks to a service talks to a public one: `httpbin.org`, a public test service, or `github.com/octocat/Hello-World`, a small public repository. Each is an input, so `--input` points it at your own.

## The examples

| Example | What it shows | Needs |
|---|---|---|
| [approval-gate](/docs/examples/approval-gate/) | an approval only when a change is risky; apply, decline, or report a rejection | `notify-block` created first; internet access to `httpbin.org` |
| [bake-then-verify](/docs/examples/bake-then-verify/) | a change, a five-second bake that runs no container, then a check | nothing |
| [config-from-git](/docs/examples/config-from-git/) | resolve a branch to a commit, render one config per service, cached while the commit holds | internet access to `github.com` |
| [decide-and-route](/docs/examples/decide-and-route/) | decide in a json task, branch three ways, re-join | nothing |
| [each-fan-out](/docs/examples/each-fan-out/) | a fan-out over a list a json task computes | nothing |
| [http-post](/docs/examples/http-post/) | a POST whose JSON body is a YAML mapping, and a condition on its status | internet access to `httpbin.org` |
| [inputs-and-escaping](/docs/examples/inputs-and-escaping/) | typed inputs; argv, positional arguments, `sh` and jq `vars` against a hostile default | nothing |
| [notify-block](/docs/examples/notify-block/) | a reusable block: one JSON notification, its status as the declared output | internet access to `httpbin.org` |
| [order-saga](/docs/examples/order-saga/) | a saga over an HTTP API: compensate, report either way | `notify-block` created first; internet access to `httpbin.org` |
| [pinned-checkout](/docs/examples/pinned-checkout/) | a git checkout into a pin, validated and packaged in place | internet access to `github.com` |
| [poll-until-ready](/docs/examples/poll-until-ready/) | polling through a retry policy, with no loop | internet access to `httpbin.org` |
| [automation/examples](/docs/examples/automation-examples/) | a namespace's settings, a schedule and a trigger, declared in one automation document | the Orkflows it names, created first |

## Composed examples

`order-saga` and `approval-gate` compose `notify-block`, so each asks one thing of you: create the block first, in the same namespace, in a command of its own.

```bash
orkctl orkflow create examples/notify-block.ork.yaml
orkctl orkflow create examples/order-saga.ork.yaml
```

```bash
orkctl orkflow create examples/notify-block.ork.yaml
orkctl orkflow create examples/approval-gate.ork.yaml
```

Creating several Orkflows in one command checks every one against the Orkflows already stored. A block in the same command does not count yet, so the composing examples are refused.

Their default paths succeed with nobody there. To see the others:

```bash
# The shipping service refuses: the shipment fails, and the reservation and the payment are undone.
orkctl run submit order-saga -i shipping_api=https://httpbin.org/absent -w
# A risky change: the run waits for an approval.
orkctl run submit approval-gate --input-json '{"changes":[{"service":"payments","setting":"limit","value":"10"}]}'
```

## The automation document

The automation document in `examples/automation/` declares a description for the `default` namespace, a schedule that runs `poll-until-ready` once a day, and a trigger for `approval-gate`. Create the Orkflows it names first. `approval-gate` composes `notify-block`, so create that first of all:

```bash
orkctl orkflow create examples/notify-block.ork.yaml
orkctl orkflow create examples/poll-until-ready.ork.yaml examples/approval-gate.ork.yaml
orkctl automation apply examples/automation/examples.ork.yaml
orkctl schedule fire poll-exports
```

See [Automation documents](/docs/operate/automation/) for how to apply one.

## Your own images

An Orkflow that runs images you build asks more of you than the examples here, so it is a recipe and not one of them. [`examples/custom-image/custom-image.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/custom-image/custom-image.ork.yaml) runs two activity images built from this repository: one in Go, and one in POSIX shell. Nothing publishes them. [Build an activity image](/docs/activities/build-an-image/) gives the steps.

The shell image's folder is [`examples/custom-image-shell/`](https://github.com/orkstack/orkstack/tree/main/examples/custom-image-shell). The Go image is in [`examples/custom-image/`](https://github.com/orkstack/orkstack/tree/main/examples/custom-image).

## See also

- [examples/README.md](https://github.com/orkstack/orkstack/blob/main/examples/README.md#the-table): the full table and the commands for each example.
- [docs/ORKFLOW_PATTERNS.md](https://github.com/orkstack/orkstack/blob/main/docs/ORKFLOW_PATTERNS.md): the patterns these examples follow.
