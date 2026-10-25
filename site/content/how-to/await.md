# Wait for a person or a system

Add `await:` to a task, and it waits with no container until someone resolves it from outside the run. Resolve it with values to complete it, or reject it.

## Example

In [`examples/approval-gate.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/approval-gate.ork.yaml), the `approval` task opens only when the change is risky:

```yaml
  - id: approval
    needs: [assess]
    condition: '{{ .tasks.assess.output.result.risky }}'
    await:
      timeout_seconds: 86400
      schema:
        approved: { type: boolean, required: true }
        approver: { type: string, required: true }
```

Submit a risky change. Do not use `--watch`, because the run waits for the approval:

```bash
orkctl apply examples/notify-block.ork.yaml
orkctl apply examples/approval-gate.ork.yaml
orkctl run submit approval-gate --input notify_url=https://hooks.example.com/changes --input 'changes=[{"service":"payments","setting":"replicas","value":"3"}]'
orkctl run get <run-id>
```

A change to `payments` or `auth`, or more than five changes, is risky. The `approval` task shows `AWAITING` until someone resolves it. Then resolve it:

```bash
orkctl run resolve <run-id> --task approval --values '{"approved":true,"approver":"dana"}'
```

`notify-block` is applied first because `approval-gate` composes it.

## Resolve the task

- Complete it with `--values '{...}'`, or with `--values-file PATH`. Use `--values-file -` to read standard input. The completion becomes the task's `values.json`.
- Reject it with `--reject --reason "…"`. The task fails with `AWAIT_REJECTED`.
- Answer "no" as a completion, such as `{"approved": false}`. A condition in a later task reads the answer and branches. A rejection fails the run. A completion does not.
- Resolve only a task that shows `AWAITING`. A resolution sent before that is refused, so send it again once the task is awaiting.
- The run page has Resolve and Reject buttons too.

<!-- site-media: ui-await -->

## Set the deadline

`timeout_seconds` is required. It is 1 second to 30 days, and it starts when the task starts waiting. A task that nobody resolves by then fails with `AWAIT_TIMED_OUT`, and the run fails. For a longer wait, use two runs.

## Let a caller resolve it over HTTP

Set `callback: true` to let a system outside OrkStack resolve the task. Submitting the run issues a one-time token for that task.

- The token is shown once, to whoever submitted the run. A task never receives it.
- Send `POST /v1/runs/<run-id>/awaits/<task-id>` with `Authorization: Bearer <token>`. The body is the JSON values. The answer is `202` once the resolution is recorded.
- To reject, send `POST` to the same path with `/reject` added, and a body of `{"reason": "…"}`.
- A scheduled run's token goes to nobody. Resolve its awaits with `orkctl run resolve`.
- Do not set `callback: true` when the approver must differ from the person who submits the run. The submitter receives the token.

## Describe a completion

`schema:` lists the fields a completion must include. Each field has a `type`: `string`, `number`, `boolean`, `object` or `array`. Add `required: true` to make it mandatory. A schema field has no default. Fields the schema does not list are accepted.

## When an await fails

| Code | Category | Result |
|---|---|---|
| `AWAIT_REJECTED` | ActivityFailed | The run fails. |
| `AWAIT_TIMED_OUT` | TimedOut | The run fails. |

An await has no retries. Its failure handlers run as they do for any task. An approval can have a handler with `join: any_failed` that tells the requester. See [Handle a failed run](/docs/how-to/handle-failure/).

## Limits

- An await takes only `id`, `needs`, `condition`, `join` and `await`. A `uses:`, `with:`, `pool:` or `retry:` key fails validation.
- A completion's values follow the values limit, 256 KiB by default.

## See also

- [DSL reference, “Awaited tasks”](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#awaited-tasks): every rule and the resolution doors
- [ORKFLOW_PATTERNS, “Wait for a person or a system”](https://github.com/orkstack/orkstack/blob/main/docs/ORKFLOW_PATTERNS.md#wait-for-a-person-or-a-system): the pattern this example shows
- [`examples/approval-gate.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/approval-gate.ork.yaml): the whole example, with its plan checks
