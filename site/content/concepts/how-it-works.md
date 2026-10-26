# How OrkStack works

Learn which part of OrkStack keeps which state, and what happens to a run from the moment you submit it.

<!-- site-diagram: architecture -->

OrkStack has four parts. Each does one job, and no OrkStack process holds the only copy of a run's state.

**Temporal** keeps the state of every run. It schedules tasks, applies their retries and timeouts, holds deadlines and approvals, and cancels work. A run's progress lives in Temporal, not in any OrkStack process.

**The engine** (`ork`) takes your Orkflows and your run requests. It checks them, starts runs in Temporal, and answers reads. Any engine replica answers for any run.

**Orkers** run each task's container. An Orker holds no durable state, so a task from a lost Orker can go to another Orker of its pool. Pinned tasks are the exception: they stay on the Orker that started them.

**Object storage** holds artifacts, logs, stored Orkflow versions and the records of finished tasks. It is S3-compatible.

## What happens in a run

When you apply an Orkflow, OrkStack checks it, resolves each image to a digest, and stores the result as an immutable version. When you submit a run, the engine decides where each task runs and which policies apply. Then it starts the run in Temporal.

Temporal starts each task once the tasks in its `needs:` have ended. The task goes to an Orker of its pool. The Orker runs the container, checks its outputs, saves them to object storage, and only then reports success to Temporal.

The UI and `orkctl` read a live run from Temporal, and a finished run from the archive. When run cleanup is on (`ORK_CLEANUP_ENABLED`, off by default), OrkStack removes a run's logs, attempt records and commits once its retention ends. The archived run record stays.

## What this means for you

A lost Orker does not lose a run. Its task is retried on another Orker, unless the task is pinned. A pinned task never moves: if its host is lost, the run fails with `PIN_HOST_LOST`, and the reason says why.

A result that is committed is not run again. A retry finds the commit and returns the stored result. See [Retries and durability](/docs/concepts/durability/).

You run no database for OrkStack. Its state is in Temporal and in object storage. You can read any run from any engine replica, so a replica that started the run does not have to be the one you ask.

## See also

- [ARCHITECTURE.md, Components](https://github.com/orkstack/orkstack/blob/main/ARCHITECTURE.md#components): what each component owns, in detail.
- [ARCHITECTURE.md, Data flow of a run](https://github.com/orkstack/orkstack/blob/main/ARCHITECTURE.md#data-flow-of-a-run): the six steps of a run.
- [docs/DEPLOY.md, Topology](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#0-topology--what-you-run-and-where-state-lives): where state lives, and how to run more than one engine or Orker.
