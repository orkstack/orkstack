# Limit concurrent runs

Let only so many runs of an Orkflow hold one key at once. The rest wait in line, in the order they arrived, and each starts when a run holding the key ends.

## Example

One migration per environment at a time:

```yaml
apiVersion: orkstack/v1
kind: Orkflow
name: db-migrate
inputs:
  env:
    type: string
concurrency:
  key: "db-migrate:{{ .inputs.env }}"
  limit: 1
  queue_timeout: 2h
tasks:
  - id: apply
    uses: ghcr.io/acme/migrate:1
```

Without `key:`, every run of the Orkflow in its namespace shares one key. The key is rendered once, when the run is submitted, from its inputs and a few run fields.

## See who waits

- The run page's **Queued** card says why the run waits, its place in line, the runs it waits behind, and when it times out. `orkctl run get <run>` prints the same as its `Queued:` line.
- The Runs page's **Queued** tile lists the runs waiting now: `orkctl run list -F queued=true`. `queued_on=KEY` and `holding=KEY` narrow to one key.
- The Orkflow page, and `orkctl orkflow get`, name the limit. The dashboard, and `orkctl stats`, count the queued runs among the running.

## Limits

- A waiting run is Pending. It fails `RUN_QUEUE_TIMEOUT` when its `queue_timeout` passes first.
- A waiting run cannot be paused. A paused run gives its slot back once nothing of it runs, and waits again when unpaused.
- `queue` is the only `on_limit` this release runs.

## See also

- [DSL.md, "Concurrency limits"](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#concurrency-limits-concurrency): every key, the rules for a key, and what a waiting run does
- [UI_CLI_PARITY.md](https://github.com/orkstack/orkstack/blob/main/docs/UI_CLI_PARITY.md): the same states in the CLI and the UI
