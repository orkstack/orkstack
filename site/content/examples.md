# Examples

Every example Orkflow in the repository, what each one shows, and what it needs before it runs. Start with [`each-fan-out`](/docs/examples/each-fan-out/): it needs nothing beyond a running stack.

To run an example, apply its file, then submit the Orkflow by its `name:`. Check its *Needs* column first:

```bash
orkctl apply examples/each-fan-out.ork.yaml
orkctl run submit each-fan-out --watch
```

Each example has its own page, with the file as it is in the repository.

## The examples

| Example | What it shows | Needs |
|---|---|---|
| [advanced-demo](/docs/examples/advanced-demo/) *(older)* | retries, a condition on an exit code, a secret, streamed logs | internet access to `httpbin.org` |
| [approval-gate](/docs/examples/approval-gate/) | an approval only when a change is risky; apply, decline, or report a rejection | the input `notify_url`; `notify-block` applied first; an HTTP endpoint you provide; someone to resolve the approval |
| [awaited-approval](/docs/examples/awaited-approval/) *(older)* | an awaited task between two steps | someone to resolve the approval |
| [branch-and-recover](/docs/examples/branch-and-recover/) *(older)* | branch, re-join, roll back and escalate | nothing |
| [cached-pipeline](/docs/examples/cached-pipeline/) *(older)* | a cached task, and one that always misses | nothing |
| [composed-release](/docs/examples/composed-release/) *(older)* | composing a block and reading its outputs | `release-block` applied first |
| [config-from-git](/docs/examples/config-from-git/) | resolve a branch to a commit, render one config per service, cached while the commit holds | the input `repo`; a git repository over HTTPS and `GIT_TOKEN` exported where the Orker runs |
| [custom-image](/docs/examples/custom-image/) | two images of your own, Go and POSIX shell, as durable activities | your own images, built and pushed |
| [decide-and-route](/docs/examples/decide-and-route/) | decide in a json task, branch three ways, re-join | nothing |
| [each-fan-out](/docs/examples/each-fan-out/) | a fan-out over a list a json task computes | nothing |
| [file-read](/docs/examples/file-read/) *(older)* | compress, decompress, checksum | nothing |
| [hello](/docs/examples/hello/) *(older)* | fetch, transform, checksum | internet access to `httpbin.org` |
| [http-post](/docs/examples/http-post/) | a POST whose JSON body is a YAML mapping, and a condition on its status | internet access to `httpbin.org` |
| [inputs-and-escaping](/docs/examples/inputs-and-escaping/) | typed inputs; argv, positional arguments, `sh` and jq `vars` against a hostile default | the input `release` |
| [notify-block](/docs/examples/notify-block/) | a reusable block: one JSON notification, its status as the declared output | the inputs `url` and `text`; an HTTP endpoint you provide |
| [order-saga](/docs/examples/order-saga/) | a saga over an HTTP API: compensate, report either way | the inputs `api`, `notify_url` and `order`; `notify-block` applied first; an HTTP endpoint you provide |
| [pinned-build](/docs/examples/pinned-build/) *(older)* | two pinned tasks hand a file over on disk | a pin host, `ORK_ORKER_MAX_PINS` above zero |
| [pinned-checkout](/docs/examples/pinned-checkout/) | a git checkout into a pin, validated and packaged in place | the input `repo`; a git repository over HTTPS and `GIT_TOKEN` exported where the Orker runs; a pin host |
| [poll-until-ready](/docs/examples/poll-until-ready/) | polling through a retry policy, with no loop | the inputs `status_url` and `export_url`; an HTTP endpoint you provide |
| [release-block](/docs/examples/release-block/) *(older)* | the block `composed-release` composes | the input `image` |
| [shell-eval](/docs/examples/shell-eval/) *(older)* | the shell activity's three operations | nothing |
| [automation/examples](/docs/examples/automation-examples/) | a namespace's settings, a schedule and a trigger, declared in one automation document | the `examples` namespace, created first; the Orkflows it names, applied first |

## Older examples

Examples marked *older* do not follow every rule of the patterns guide. Six of them splice a rendered value into shell text, which the guide advises against. Read the newer examples first, and copy from the patterns rather than from these.

## Composed examples

`order-saga` and `approval-gate` compose `notify-block`, and `composed-release` composes `release-block`. Apply the block first, in its own command, into the same namespace:

```bash
orkctl apply examples/notify-block.ork.yaml
orkctl apply examples/order-saga.ork.yaml examples/approval-gate.ork.yaml
```

Applying several documents in one command checks every one against the Orkflows already stored. A block in the same command does not count yet, so the composing examples are refused.

## The automation document

The automation document in `examples/automation/` declares the `examples` namespace's settings, a schedule and a trigger. Create that namespace first, and apply the Orkflows the document names before it. See [Automation documents](/docs/how-to/automation/) for how to apply one.

## The custom images

[`custom-image`](/docs/examples/custom-image/) uses two activity images built from this repository: one in Go, and one in POSIX shell. Nothing publishes them. Build and push them first, as [Your first activity image](/docs/tutorials/first-activity/) shows.

The shell image's folder is [`examples/custom-image-shell/`](https://github.com/orkstack/orkstack/tree/main/examples/custom-image-shell). The Go image is in [`examples/custom-image/`](https://github.com/orkstack/orkstack/tree/main/examples/custom-image).

## See also

- [examples/README.md](https://github.com/orkstack/orkstack/blob/main/examples/README.md#the-table): the full table, the commands for each example, and the notes on older examples.
- [docs/ORKFLOW_PATTERNS.md](https://github.com/orkstack/orkstack/blob/main/docs/ORKFLOW_PATTERNS.md): the patterns the newer examples follow.
