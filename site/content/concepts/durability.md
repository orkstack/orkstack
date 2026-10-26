# Runs and durability

Learn what OrkStack keeps when a worker or a container fails, which failures retry, and what you still handle yourself.

<!-- site-diagram: durable-commit -->

## Tasks run at least once

A task can run more than once. A retry can start its container again, and so can a lost Orker. Anything the container does outside OrkStack, such as sending an email or charging a card, can happen twice.

Every attempt of one task gets the same `idempotencyKey`, in its `task.json`. A system you call can use that key to recognize a repeat.

## A committed result is kept

When a container exits with `0`, the Orker checks its outputs and uploads the artifacts and `values.json` to object storage. Then it writes a commit record, and only then reports success.

If the Orker is lost after the commit and before it reports, the retry finds the commit. It returns the stored result, and the container does not run again.

## Which failures retry

A non-zero exit, a timeout, a contract violation and a cancellation write no commit, so a later attempt runs the container again. Whether it does depends on the category:

| Category | Retried |
|---|---|
| `ActivityFailed`: the container exited non-zero | As the task's `retry:` allows |
| `InfrastructureError`: storage, the registry or the runtime failed | As `retry:` allows, except for some host misconfigurations |
| `ContractViolation`: the image or the Orkflow broke the contract | Never |
| `TimedOut`: the container passed its `timeout_seconds` | Never by default |
| `Cancelled`: you cancelled the run | Never automatically |

A timeout caused by a lost Orker is different. It retries under the task's policy, so the task moves to a live Orker.

A task with no `retry:` makes one attempt, whatever its category.

## Every failure has a reason

Each failure ends with a reason code and a plain-language message. The [reason codes](/docs/reference/reasons/) page lists every code, and what to do about it.

## Caching is your call

A cached task reuses a stored result for an identical run of it. You turn that on with `cache:`, and you assert that reuse is safe. OrkStack cannot check that assertion. [Cache a task](/docs/how-to/cache/) shows how to decide.

## See also

- [AGENTS.md, Durability, retries, and completion commits](https://github.com/orkstack/orkstack/blob/main/AGENTS.md#4-durability-retries-and-completion-commits): the guarantees this page describes.
- [docs/OCI_ACTIVITY_CONTRACT.md, Durable commit protocol](https://github.com/orkstack/orkstack/blob/main/docs/OCI_ACTIVITY_CONTRACT.md#5-durable-commit-protocol): the commit key and the order of its steps.
- [docs/OCI_ACTIVITY_CONTRACT.md, Failure categories](https://github.com/orkstack/orkstack/blob/main/docs/OCI_ACTIVITY_CONTRACT.md#8-failure-categories-and-error-handling): each category, and its reason codes.
