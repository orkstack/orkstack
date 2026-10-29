# Troubleshooting and FAQ

This page covers the failures and refusals users meet most often, phrased the way they appear on screen. Each entry gives the cause, a command that confirms it, and what to do about it. Reason codes are spelled exactly as the code spells them. Each entry names the code location that produces the reason, so a maintainer can find it, and links the reference document that states the rule.

Every terminal failure carries a machine-readable code and a human-readable message, and its category says whether a retry may help ([AGENTS.md](../AGENTS.md), "15. Product invariants"; [AGENTS.md](../AGENTS.md), "4. Durability, retries, and completion commits"). Three commands answer most questions about a run:

```bash
orkctl run get <run-id> --attempts            # every task's final outcome, and every try
orkctl run attempts <run-id> --task <task>    # each try's error, exit status and Orker
orkctl run logs <run-id> --task <task>        # what the task wrote
```

Every command takes `--json` for scripts and `-n, --namespace` to choose the namespace ([CLI.md](./CLI.md), "`orkctl run get`").

The page is grouped into five parts. **Submitting** covers refusals, which happen before a run exists. **Running** covers a run that exists but is not progressing as expected, or ended in a way nobody asked for. **Failures** covers a task or run that failed, by reason code. **The quickstart** covers the stack `docker compose` builds and runs from a clone ([GETTING_STARTED.md](./GETTING_STARTED.md)). **The local stack** covers `make up` and the dev stack in `scripts/stack.sh`.

## Submitting

A submission refused in this section leaves nothing behind: no run id is minted and no idempotency key is claimed ([AGENTS.md](../AGENTS.md), "7. Orkflow execution model"). Fix the cause and submit again.

### "has no live Orkers, so nothing would pick these tasks up"

**Cause.** The pool a task resolves to has no Orker that would take it: nothing polls the pool's channel, and no uncordoned Orker of the pool answers on its own channel. The second check is what keeps a busy pool live, because an Orker whose every task slot is taken stops polling the pool's channel but still answers on its own. An Orker that does not answer within five seconds is not counted, and the engine logs `a pool reads empty while some live Orkers did not answer when asked`. The engine refuses the submit with `failed_precondition`, before a run id exists (`internal/engine/pools.go`, `PoolEmptyError`; `internal/api/service.go`, `poolPlanError`). The rule is in ([DEPLOY.md](./DEPLOY.md), "Running M Orker pools").

**Confirm.**

```bash
orkctl pool list --json     # liveOrkerCount is 0 for the pool named in the error
orkctl orker list           # the Orkers that are polling, with their pool and cordon state
```

**Fix.** Start an Orker for that pool. The message says so: `ORK_ORKER_POOL=<pool>`. If the run should wait for an Orker to arrive, submit with `--allow-empty-pool`. The CLI flag is `--allow-empty-pool`; the message and the API spell the field `allow_empty_pool`. The run is then accepted, its tasks wait in the pool, and the engine logs `submit into empty pool` ([CLI.md](./CLI.md), "`orkctl run submit`"). A pool whose Orkers have all been cordoned reports their count for about 90 seconds, then zero ([DEPLOY.md](./DEPLOY.md), "Removing an Orker safely: cordon and drain").

### "cannot tell whether pool … has live Orkers"

**Cause.** The cluster could not be asked: about the pool's channel, or, for a pool whose channel shows no poller, about the Orker inventory. Nothing was measured, so this is not an empty pool. The engine answers `unavailable`, and `--allow-empty-pool` does not change that answer when the pool's channel could not be asked (`internal/engine/pools.go`, `PoolUnreachableError`; `internal/api/service.go`, `poolPlanError`). When only the inventory could not, the override accepts the run, because it would accept the pool busy or empty, and the engine logs `whether a pool with no poller is busy is unknown`. The distinction is deliberate: an empty pool needs an Orker or the override, and an unreachable cluster needs a retry ([DEPLOY.md](./DEPLOY.md), "Running M Orker pools").

**Confirm.** `orkctl pool list` prints `unknown (unreachable)` for the pool, with the reason under the table ([CLI.md](./CLI.md), "`orkctl pool list`").

**Fix.** Retry. If the error persists, check the engine and the Temporal server (`make status` on the local stack). Do not use `--allow-empty-pool` for this error.

### "RESOURCES_UNSATISFIABLE: task … requests …, which no Orker of pool … runs"

**Cause.** The task declares a `resources:` request above the per-task cap of every Orker of its pool that answered (`orker.resources.max_cpu`, `orker.resources.max_memory`). An Orker refuses such a request after dispatch, terminally (`RESOURCE_REQUEST_REFUSED`), so the run is refused at submit instead, before a run id exists, and `--allow-empty-pool` does not change that (`internal/engine/pools.go`, `PoolResourcesUnsatisfiableError`). The message names the largest cap the pool reported ([DEPLOY.md](./DEPLOY.md), "Declaring an Orker's capacity").

**Confirm.** `orkctl orker list --filter pool=<pool>` lists the pool's Orkers; each one's cap is in `orkctl orker get <orker-id> --json` (`maxRequest`).

**Fix.** Lower the task's request, or move the task to a pool whose Orkers set larger caps. The cap is a pool-wide setting: set the same one on every Orker of a pool.

### "has live Orkers but none of them hosts pins"

**Cause.** The pool has live Orkers, but none of them has `ORK_ORKER_MAX_PINS` above zero, so no pin can open. The engine asks every uncordoned Orker of the pool, each on its own channel, and refuses with `failed_precondition` (`internal/engine/pools.go`, `PoolNoPinHostError`). Pin hosting is a pool-wide setting ([DEPLOY.md](./DEPLOY.md), "Pin hosting is a pool-wide setting").

**Confirm.** `orkctl orker list` shows `open/max` in the PINS column for a pin host and `-` for any other Orker ([CLI.md](./CLI.md), "`orkctl orker list`").

**Fix.** Set `ORK_ORKER_MAX_PINS=1` on every Orker of that pool, then restart them. With `--allow-empty-pool` the submit is accepted, but if no host can open the pin, the task that asked for it fails `PIN_UNAVAILABLE` after the pin creation timeout, five minutes by default ([DSL.md](./DSL.md), "Pins").

### The pool's Orkers are too old for `each:`, `cache:` or `join:`

**Cause.** An Orker built before fan-out, task caching or join policies cannot read the input those tasks carry. The engine refuses the submit with `failed_precondition`, naming the Orker and its build. `--allow-empty-pool` does not override this refusal, because the Orker is present and wrong (`internal/engine/pools.go`, `PoolNoEachSupportError`, `PoolNoCacheSupportError`, `PoolNoJoinSupportError`).

**Confirm.** The error names the Orker id and its build. The build is also in `orkctl orker list --json`.

**Fix.** Upgrade every Orker of the pool to the release the engine runs. Every Orker of one pool must run one release ([AGENTS.md](../AGENTS.md), "7.2 Versioning"; [DEPLOY.md](./DEPLOY.md), "Fan-out, caching and failure handlers need every Orker of a pool on a release that supports them").

### `namespace "…" does not allow pool "…"`

**Cause.** The namespace's allow-list excludes the pool the task resolves to. The submit is refused with `invalid_argument` (`internal/engine/pools.go`, `PoolNotAllowedError`).

**Fix.** Either change the task's `pool:`, or add the pool to the namespace: `orkctl namespace update <namespace> --allow-pool <pool>`. The flag is repeatable, and an empty list means unrestricted ([CLI.md](./CLI.md), "`orkctl namespace update`").

### `namespace "…" may not resolve "…"` (a secret source)

**Cause.** A secret's `source:` is outside the namespace's allow-list. The submit is refused with `invalid_argument`, naming the task, the secret and the allowed list (`internal/engine/secrets.go`, `SecretNotAllowedError`). Each Orker has its own floor, `ORK_ORKER_SECRET_ALLOW`, and a source must pass both ([DEPLOY.md](./DEPLOY.md), "Secrets on a shared pool").

**Fix.** Use an allowed source, or widen the namespace list: `orkctl namespace update <namespace> --allow-secret 'env:NAME_*'`. On update this replaces the whole set ([CLI.md](./CLI.md), "`orkctl namespace update`").

### "… is on hold" (`NAMESPACE_HELD`)

**Cause.** An operator closed the namespace to new runs with `--hold`. The submit is refused with `failed_precondition`, and `--allow-empty-pool` does not bypass it. Runs already accepted are not touched ([DEPLOY.md](./DEPLOY.md), "Namespace admission: `hold` and `max_running`"; `internal/engine/admission.go`, `HoldRefusal`).

**Confirm.** `orkctl namespace get <namespace>` shows the hold and its reason. On the local stack the refusal is logged with its reason: `grep 'run refused' .run/ork.log` prints a `reason` of `NAMESPACE_HELD` (`internal/api/admission.go`, `refuse`).

**Fix.** Find out why the namespace was held, then reopen it with `orkctl namespace update <namespace> --release-hold`. Releasing a hold starts nothing that was refused while it was set.

### "already has N runs in flight, which is its max_running" (`MAX_RUNNING_REACHED`)

**Cause.** The namespace's `max_running` is reached. The count is read from the cluster's execution index on each submit, and the check is best-effort: concurrent submits can overshoot by a small bounded amount ([AGENTS.md](../AGENTS.md), "7. Orkflow execution model"; `internal/engine/admission.go`, `MaxRunningRefusal`).

**Confirm.** `orkctl run list --filter status=RUNNING` lists the runs in flight, and `orkctl namespace get <namespace>` shows the cap.

**Fix.** Wait for a run to finish, or raise the cap: `orkctl namespace update <namespace> --max-running N`. `0` means unlimited ([CLI.md](./CLI.md), "`orkctl namespace update`").

### "… has reserved … at or above its enforced budget" (`BUDGET_EXCEEDED`) or "… could not be measured" (`BUDGET_UNMEASURABLE`)

**Cause.** The namespace has an enforced budget and the reserved cpu or memory over its rolling period has reached it. The unmeasurable case refuses too: an enforced budget that cannot be measured refuses every submit (`internal/engine/admission.go`, `BudgetRefusal` and `BudgetUnmeasurableRefusal`). Usage is a reservation, the cpu and memory each attempt was granted multiplied by its time, not measured use ([DEPLOY.md](./DEPLOY.md), "Namespace budgets: reserved consumption, soft or enforced").

**Confirm.**

```bash
orkctl stats usage --since 720h                   # what the namespace reserved over the window
orkctl -n <namespace> stats usage --since 720h    # answers only if the budget can be measured
```

**Fix.** For `BUDGET_EXCEEDED`, raise `--budget-cpu-seconds` or `--budget-gib-seconds`. For `BUDGET_UNMEASURABLE`, shorten `--budget-period`, as the message says. To report without refusing, use `--budget-enforce=false` ([CLI.md](./CLI.md), "`orkctl namespace update`").

### "a submit or an apply is refused by the image policy"

**Cause.** The deployment's image floor, or the namespace's `--require-signed-by`, forbids an image the document names. At apply and validate the issue is on `tasks[i].uses`. At submit the refusal is `failed_precondition`, because a stored definition can be refused after the policy tightens (`internal/api/image_policy.go`, `submitPolicyError`). The reason codes are `IMAGE_NOT_PERMITTED`, `IMAGE_NOT_SIGNED`, `IMAGE_SIGNATURE_INVALID`, and `IMAGE_SIGNATURE_UNAVAILABLE` when the registry cannot be asked ([DEPLOY.md](./DEPLOY.md), "Image supply-chain policy").

**Confirm.** `orkctl admin image-policy get` shows the deployment floor, and `orkctl namespace get <namespace>` shows the namespace's settings.

**Fix.** Use an image under an allowed registry, written with a digest (`--require-digest` refuses a tag, except a catalog name such as `uses: shell`), and signed by a key the policy names. A namespace can tighten the deployment policy but cannot loosen it ([DEPLOY.md](./DEPLOY.md), "Image supply-chain policy").

### My run's tasks stay `PENDING`

**Cause.** `PENDING` is the engine's own hold: a ready task it has not dispatched yet. That happens in three cases ([DSL.md](./DSL.md), "How many tasks an Orkflow may declare"; [DEPLOY.md](./DEPLOY.md), "A run that pauses new tasks"):

1. The run is at its in-flight cap, `ORK_RUN_MAX_IN_FLIGHT` (1000 by default). It starts more as running work finishes (`internal/engine/hold.go`; [DSL.md](./DSL.md), "How many tasks an Orkflow may declare").
2. The run's history is near its hold line. It holds new tasks until the running ones finish, so it can carry on into a fresh history ([DEPLOY.md](./DEPLOY.md), "A run that pauses new tasks").
3. A pin is opening. Its ready tasks stay `PENDING` while the pin opens ([DSL.md](./DSL.md), "Pins").

A task waiting for a free slot on a busy pool is not one of them: the engine has dispatched it, so it is `RUNNING`. See "My run stays `RUNNING` and nothing happens".

**Confirm.** `orkctl run inspect <run-id>` shows a held task with no attempt. When the in-flight cap applies, the engine logs `run reached its in-flight cap` ([DEPLOY.md](./DEPLOY.md), "A run that pauses new tasks").

**Fix.** A `PENDING` task is not a failure, and the run resumes as its running work finishes.

## Running

### My run stays `RUNNING` and nothing happens

**Cause.** One of four things is usually true:

- Every Orker of the pool is busy. Each runs at most `ORK_ORKER_MAX_CONCURRENT` tasks at once (16 by default), and, when it declares a capacity, polls only while a whole per-task cap is free beside what it has reserved; a task beyond that waits in the pool. `orkctl orker list` shows each Orker's `TASKS`, `CAPACITY` and `RESERVED`. The task is `RUNNING` from the moment it is dispatched: `orkctl run inspect` says `waiting for an orker on pool <pool> for <time>`, and the run page says `Waiting for an orker` (`internal/engine/workflow.go`; `internal/orker/cordon.go`, `DefaultMaxConcurrent`; [DEPLOY.md](./DEPLOY.md), "Sizing an Orker's task ceiling").
- No Orker serves the pool, or the ones that do are cordoned. A cordoned Orker takes no new task and opens no new pin, and a restart clears the cordon ([CLI.md](./CLI.md), "`orkctl orker cordon`").
- An Orker died while it held an attempt. Its heartbeat stops, `orkctl run inspect` reports the heartbeat as `STALE`, and the attempt times out as `ORKER_HEARTBEAT_TIMEOUT` (`internal/engine/workflow.go`). A heartbeat timeout is retryable under the task's `retry:` policy ([OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "8.1 The five categories").
- A pinned task's host was lost. See `PIN_HOST_LOST` below.

**Confirm.**

```bash
orkctl orker list
orkctl run inspect <run-id>
orkctl run attempts <run-id>     # an attempt its Orker never filed shows as UNREPORTED
```

**Fix.** If the pool is busy, the task starts when a slot frees. To shorten the wait, add Orkers to the pool, or raise `ORK_ORKER_MAX_CONCURRENT` on hosts that can take the load. Restart a dead Orker so it polls again. If it is cordoned and should take work, run `orkctl orker uncordon <orker-id>`. If the attempt was lost and the task has no `retry:` policy, the task fails. Start a new run with `orkctl run again <run-id>`, which submits a new run from the prior run's pinned definition and inputs ([CLI.md](./CLI.md), "`orkctl run again`").

### I stopped an Orker and its tasks failed

**Cause.** `SIGTERM` stops an Orker from polling and cancels its in-flight work. The Orker does not cordon itself first, so an operator who wants in-flight work to finish cordons and drains first ([DEPLOY.md](./DEPLOY.md), "Removing an Orker safely: cordon and drain"). Stopping an Orker that hosts an open pin fails that pin's run with `PIN_HOST_LOST`, and the stop command says so (`cmd/orkctl/orker.go`).

**Fix.** Drain the Orker before stopping it. `orkctl orker drain <orker-id> --reason "…"` cordons it and waits until it runs no task and hosts no pin. Exit `0` means it is safe to stop. Exit `4` means the timeout passed first, and exit `1` means the Orker stopped answering, or was uncordoned or restarted ([CLI.md](./CLI.md), "`orkctl orker drain`").

### A run shows `CANCELED`, not `FAILED`

**Cause.** A run that was canceled ends with a run-level code. `RUN_CANCELED` means the run was canceled, for example with `orkctl run cancel <run-id>`. `RUN_TERMINATED` means the run was terminated from outside the engine: its status is `CANCELED` and its message is the termination reason. The engine reads both from the event that closed the run (`internal/engine/reader.go`; [OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "8.3 Engine side").

**Confirm.** `orkctl run get <run-id>` shows the run's error code and message. Control-plane writes, including run cancels, are recorded in the audit trail. `orkctl admin audit` lists them. The principal in an audit record is the one a gateway asserted. It is not a verified identity ([AGENTS.md](../AGENTS.md), "7. Orkflow execution model", "Principal"; [DEPLOY.md](./DEPLOY.md), "Attributing control-plane writes").

### A task ran again

**Cause.** Activities are at-least-once. A retry, a crash, or a lost completion can run a container more than once. OrkStack does not make external side effects exactly once. When a valid commit already exists for the attempt, a retry adopts that commit and does not run the container again ([AGENTS.md](../AGENTS.md), "4.1 Durable output commit"). The `idempotencyKey` stays the same across the retries of one logical activity, so an external system can deduplicate ([AGENTS.md](../AGENTS.md), "3. OCI Activity Contract").

**Confirm.** `orkctl run attempts <run-id> --task <task>` prints one row per try. An adopted commit is the committing attempt ([UI_CLI_PARITY.md](./UI_CLI_PARITY.md), "Task attempts").

**Fix.** Make the external effect idempotent, keyed on the stable idempotency key. Do not add `retry:` to a task that is not safe to run twice.

### A failure handler ran after my run failed

**Cause.** A task with `join: any_failed` or `join: all_done` is a failure handler. It is the only kind of task that starts in a failing run. It never rescues the run, which still ends `FAILED` with its original reason ([DSL.md](./DSL.md), "Joins and failure handlers (`join:`)"; [AGENTS.md](../AGENTS.md), "7. Orkflow execution model").

**Fix.** If the handler should not run after a failure, give it the default `join: all_succeeded`.

### A cached task ran its container again

**Cause.** The memo missed, or the run was submitted with `--no-cache`, which runs every cached task and refreshes its memo. A miss has a token, such as `MEMO_ABSENT`, `MEMO_EXPIRED` or `MEMO_MISMATCH`. The token is recorded on the cache result (`internal/orker/memo.go`, `MemoMissAbsent` and the others). A memo is advisory: every doubt about one is a miss, and deleting a memo changes no result ([DSL.md](./DSL.md), "Miss reasons"; [AGENTS.md](../AGENTS.md), "4.1 Durable output commit").

**Confirm.** `orkctl run get <run-id>` shows the miss reason beside each cached task. `orkctl cache stats --since 24h` gives the hit rate per cached task ([CLI.md](./CLI.md), "`orkctl cache stats`").

### An awaited task ended `AWAIT_TIMED_OUT` or `AWAIT_REJECTED`

**Cause.** An `await:` task waits for a resolution from outside the run and runs no container. When its deadline passes, it fails with `AWAIT_TIMED_OUT` (category `TimedOut`). When it is rejected, it fails with `AWAIT_REJECTED` (category `ActivityFailed`) (`internal/engine/await.go`; [DSL.md](./DSL.md), "Awaited tasks").

**Fix.** Resolve the await before its deadline, with `orkctl run resolve <run-id> --task <await-id> --values '{…}'`, or `--reject --reason TEXT`. A resolution is accepted only while the task is `AWAITING`. One sent while the task is still `PENDING` is refused, so retry once it is awaiting ([DSL.md](./DSL.md), "Awaited tasks"; [UI_CLI_PARITY.md](./UI_CLI_PARITY.md), "Awaited tasks").

## Failures

Every failed task attempt has a category and a reason code. The category decides whether a retry may help, and the task's `retry:` policy decides how many retries it gets. A task with no `retry:` makes exactly one attempt ([DSL.md](./DSL.md), "Tasks").

| Category | Retry by default | Examples |
|---|---|---|
| `ActivityFailed` | per the task's `retry:` | `CONTAINER_EXIT_NONZERO` |
| `InfrastructureError` | per policy; a few host misconfigurations are never retried | `IMAGE_PULL_FAILED`, `WORKSPACE_NOT_BINDABLE` |
| `ContractViolation` | never | `VALUES_MISSING`, `RENDER_FAILED` |
| `TimedOut` | not retried by default | `CONTAINER_TIMEOUT`, `PIN_TIMEOUT` |
| `Cancelled` | never automatically | `TASK_CANCELED` |

The categories are defined in ([OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "8.1 The five categories"), and the retry decision is in `internal/orker/failure.go` (`RetryableReason`). The CLI prints an attempt's error as `CATEGORY/CODE: message`, for example `ACTIVITY_FAILED/CONTAINER_EXIT_NONZERO: …` (`cmd/orkctl/attempts.go`, `attemptErrorLine`).

### A task failed with `CONTAINER_EXIT_NONZERO` ("the container exited N")

**Cause.** The container exited with a non-zero status. Every non-zero status is this reason, whatever the number. When the kernel killed the container for exceeding its memory limit, the message says so (`internal/orker/oci_dispatcher.go`, the exit-capture step). Category `ActivityFailed`, retried per the task's `retry:` ([OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "8.2 Reason codes").

**Confirm.**

```bash
orkctl run attempts <run-id> --task <task>                 # the exit status and the error
orkctl run logs <run-id> --task <task> --attempt <n>       # what the task wrote on that try
orkctl run artifacts <run-id> --task <task>                # the recorded stdout and stderr artifacts
```

**Fix.** Fix the workload. For a memory kill, raise `resources.memory` on the task, within the Orker's cap. A request above the cap fails `RESOURCE_REQUEST_REFUSED` ([DSL.md](./DSL.md), "Tasks"; [OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "8.2 Reason codes").

### A task failed with `CONTAINER_TIMEOUT` ("exceeded its timeout of N seconds")

**Cause.** The container ran past its `timeout_seconds` (300 by default). The Orker sent `SIGTERM`, waited for the grace period, then sent `SIGKILL` (`internal/orker/oci_dispatcher.go`). Category `TimedOut`, and not retried, because the same wall clock would time it out again ([AGENTS.md](../AGENTS.md), "4. Durability, retries, and completion commits"; [OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "8.1 The five categories").

**Fix.** Raise the task's `timeout_seconds` if the work is legitimately slow. Otherwise find the hang. `cancellation_grace_period_seconds` sets the window between `SIGTERM` and `SIGKILL` ([DSL.md](./DSL.md), "Tasks").

### A task failed with `VALUES_MISSING` ("exited 0 but wrote no values.json")

**Cause.** Exit status `0` means the computation succeeded. The Orker then validates the outputs, and a missing `/orkstack/output/values.json` is a contract violation (`internal/orker/workspace.go`, `ReadValues`). Category `ContractViolation`, never retried ([OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "3.4 `output/values.json`").

**Fix.** On success, write a JSON object to `/orkstack/output/values.json`. A workload with no result writes `{}`.

### A task failed with `VALUES_MALFORMED` or `VALUES_TOO_LARGE`

**Cause.** `values.json` is not valid UTF-8 JSON, or its top-level value is not an object, such as an array. A symlink, a directory or another non-regular file at that path is refused the same way, whatever the link points at: the Orker follows no link the workload wrote. A file over `limits.valuesMaxBytes` is `VALUES_TOO_LARGE`. Both are contract violations and never retried (`internal/orker/workspace.go`; [OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "8.2 Reason codes").

**Fix.** Write exactly one JSON object. Move large data into an artifact, which is a regular file, or archive a directory such as `workspace.tar.gz` ([AGENTS.md](../AGENTS.md), "3.1 Files, values, and artifacts").

### A task failed with `OUTPUT_NOT_READABLE` or `ARTIFACT_NOT_A_FILE`

**Cause.** `OUTPUT_NOT_READABLE`: the Orker cannot open a file the workload wrote, and the message names the path, mode and owner. `ARTIFACT_NOT_A_FILE`: a directory, symlink, device, socket or FIFO was written under `output/artifacts/`, or `output/artifacts` itself was replaced by a symlink or a file. Both are contract violations (`internal/orker/workspace.go`).

**Fix.** For `OUTPUT_NOT_READABLE`, the contract's fix is `umask 022` in the workload, so the files are readable by the Orker's user. For `ARTIFACT_NOT_A_FILE`, write regular files only, and archive any directory ([OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "8.2 Reason codes").

### A task failed with `RENDER_FAILED`

**Cause.** A templated value could not be rendered. The usual reasons are a key that nobody supplied, a missing key, `<no value>`, or an `env` name that is not on the Orker's allow-list. Category `ContractViolation`. A template reads only its task's `needs:` closure, and a missing input fails the task rather than rendering an empty string ([DSL.md](./DSL.md), "Inputs" and "Templating"; `internal/orker/oci_dispatcher.go`).

**Confirm.** Render the task on this machine without starting a container:

```bash
orkctl task run --from-run <run-id> --task <task> --render-only
```

It prints the rendered `config.json` and starts no container ([CLI.md](./CLI.md), "`orkctl task run`").

**Fix.** Supply the missing input with `--input key=value`, or correct the key in the template. An `env` name must be on the Orker's allow-list, `ORK_ORKER_ENV_ALLOWLIST`.

### A task failed with `IMAGE_PULL_FAILED`

**Cause.** The Orker could not pull the pinned image within `ORK_DOCKER_PULL_TIMEOUT`, and no verified embedded copy exists. Category `InfrastructureError`, retried per policy (`internal/orker/oci_dispatcher.go`; [OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "8.2 Reason codes").

**Confirm.** `orkctl run attempts <run-id> --json` carries each attempt's `image`, the pinned digest reference the Orker used ([DEPLOY.md](./DEPLOY.md), "Image supply-chain policy"). Then try a pull of that reference from the Orker's host.

**Fix.** Check the registry and the Orker host's network. A running execution never switches images because a tag moved ([AGENTS.md](../AGENTS.md), "5. Image resolution and Orker runtime boundary").

### A running task failed with `IMAGE_NOT_SIGNED`, `IMAGE_SIGNATURE_INVALID` or `IMAGE_NOT_PERMITTED`

**Cause.** The Orker judges every attempt against the policy the run pinned at submit, and it asks the registry for the signature itself. A missing signature is `IMAGE_NOT_SIGNED`. A signature by another key, or over another digest, is `IMAGE_SIGNATURE_INVALID`. An image outside the floor is `IMAGE_NOT_PERMITTED`. All three are contract violations and are never retried. `IMAGE_SIGNATURE_UNAVAILABLE` means the registry could not be asked, and it is retried (`internal/task/image_policy.go`; [DEPLOY.md](./DEPLOY.md), "Image supply-chain policy").

**Fix.** A run keeps the policy it was submitted under, so a later tightening does not change it. Restore the signature for that exact digest, or move the image under an allowed registry, then start a new run with `orkctl run again <run-id>`.

### A task failed with `SECRET_UNRESOLVED`, `SECRET_NOT_PERMITTED` or `SECRET_SCHEME_FORBIDDEN`

**Cause.** `SECRET_UNRESOLVED`: the Orker could not read the source. For `env:NAME`, the variable is not set on that Orker. For `file:PATH`, the file cannot be read. It is an `InfrastructureError` and is retried under the task's policy. `SECRET_NOT_PERMITTED`: the source is outside the Orker's `ORK_ORKER_SECRET_ALLOW` floor. It is refused before the image is pulled and is never retried. `SECRET_SCHEME_FORBIDDEN`: the Orker does not resolve that scheme (`ORK_ORKER_SECRET_SCHEMES`), or the name is malformed (`internal/orker/oci_dispatcher.go`; [DEPLOY.md](./DEPLOY.md), "Secrets on a shared pool").

**Confirm.** The message names the secret and the variable or path. It never contains the value.

**Fix.** For `SECRET_UNRESOLVED`, set the variable, or make the file readable, on the Orker host that ran the attempt. For `SECRET_NOT_PERMITTED`, ask the operator to add the source to `ORK_ORKER_SECRET_ALLOW`, or use an allowed source.

### A task failed with `INPUT_ARTIFACT_UNAVAILABLE` or `INPUT_ARTIFACT_CORRUPT`

**Cause.** `INPUT_ARTIFACT_UNAVAILABLE`: an upstream artifact could not be fetched from object storage. It is an `InfrastructureError` and is retried. `INPUT_ARTIFACT_CORRUPT`: the fetched bytes do not match their recorded sha256. It is a contract violation and is never retried (`internal/orker/staging_cache.go` for the unavailable case and `internal/orker/oci_dispatcher.go` for the corrupt one; [OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "8.2 Reason codes").

**Confirm.** On the local stack, `make status` and `make infra-logs` show whether the object store is up.

**Fix.** Restore object storage for an unavailable input. A corrupt input is not fixed by a retry, so start a new run once the upstream output is rewritten.

### A task failed with `WORKDIR_NOT_BINDABLE` or `WORKSPACE_NOT_BINDABLE` ("bind source path does not exist")

**Cause.** The container daemon cannot bind-mount the Orker's work directory. This happens when the daemon runs in a virtual machine that does not share that path with the host. The daemon then says "bind source path does not exist" for a path that exists, which reads like a missing file. There are two codes:

- `WORKDIR_NOT_BINDABLE`: the Orker refuses to start (`internal/orker/activity.go`, `Boot`) because its work root cannot be bind-mounted. Either the path holds a comma or a control character, which a container mount cannot carry (a mount is a comma-separated option list), and the message names `ORK_ORKER_WORKDIR`; or the Orker's own boot probe found the work root unbindable: the daemon refused the bind, or its container and the Orker did not read back each other's nonce, which means the daemon bound a directory of its own (`internal/orker/boot_probe.go`). Retrying does not fix it.
- `WORKSPACE_NOT_BINDABLE`: one attempt's container could not be created because the daemon refused the workspace's bind (`internal/orker/oci_dispatcher.go`, `runtimeFailure`). It is an `InfrastructureError` and is never retried.

([OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "8.2 Reason codes"; [stack.sh](../scripts/stack.sh), "check_workdir_mountable".)

**Confirm.** `make up` prints a warning when the probe fails. The Orker log, `.run/orker.log`, shows `orker boot checks failed:` followed by `WORKDIR_NOT_BINDABLE`. The script's comment describes the virtual-machine case, and says how to list the paths the machine shares. The script's warning describes the per-task symptom. The Orker's own probe is stricter, and it refuses to start.

**Fix.** Point `ORK_ORKER_WORKDIR` at a directory the virtual machine shares. On a macOS setup that usually means a path under the user's home directory. If the message says the path holds a comma or a control character, the cause is the path itself and not the virtual machine: choose a path without either. Then run `make down` and `make up`. The script's default, `.data/work` under the repository, is chosen because those machines share the home directory.

### A task failed with `PIN_UNAVAILABLE`

**Cause.** Every pin host of the pool stayed full, or cordoned, for the whole pin creation timeout, five minutes unless `ORK_PIN_CREATION_TIMEOUT` says otherwise. The task's message says which: "within" the timeout when no pin host picked the pin up in time, or that every pin host refused it because it is cordoned. Only the task that asked for the pin fails. It never reached an Orker, so its `scheduled_at` and `started_at` stay unset. Category `InfrastructureError`, not retried (`internal/engine/workflow.go`; [DSL.md](./DSL.md), "Pins").

**Confirm.** `orkctl orker list` shows each pin host's `open/max` and whether it is cordoned.

**Fix.** Add a pin host, uncordon one with `orkctl orker uncordon <orker-id>`, reduce the number of pins that are open at once, or raise `ORK_PIN_CREATION_TIMEOUT` if slots free up slowly (a new value applies to runs submitted after the engine restarts). A pool whose hosts are all cordoned fails after about five minutes whatever the setting says. A pool with no pin host at all is refused at submit instead ([DEPLOY.md](./DEPLOY.md), "Pin hosting is a pool-wide setting").

### A task failed with `PIN_HOST_LOST` or `PIN_TIMEOUT`

**Cause.** `PIN_HOST_LOST`: the host stopped heartbeating the pin, or its Orker process was stopped while the pin was open. Members still running at that moment fail with this code, and members not yet started are canceled. A member that already committed stays `SUCCEEDED`. `PIN_TIMEOUT`: the pin was open for its whole lifetime, `timeout_seconds` (`internal/engine/workflow.go`, `pinFailureError`; [DSL.md](./DSL.md), "Pins").

**Confirm.** `orkctl run attempts <run-id> --task <task>` shows the failed members. `orkctl orker list` shows the host.

**Fix.** A pin never moves to another Orker, because its tasks share one host's `/orkstack/shared`, so the run is not retried elsewhere ([AGENTS.md](../AGENTS.md), "5. Image resolution and Orker runtime boundary"). Start a new run with `orkctl run again <run-id>`. Drain a host before you stop it, and give a long-lived pin a long enough `timeout_seconds`.

### A task failed with `ORKER_HEARTBEAT_TIMEOUT` or `ACTIVITY_TIMEOUT`

**Cause.** `ORKER_HEARTBEAT_TIMEOUT` means the Orker stopped heartbeating its attempt, so it died or lost contact. It is an `InfrastructureError` and stays retryable under the task's policy. `ACTIVITY_TIMEOUT` means the engine's start-to-close deadline ended the attempt, and its category is `TimedOut` (`internal/engine/workflow.go`; [OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "8.3 Engine side").

**Confirm.** `orkctl run inspect <run-id>` reports a `STALE` heartbeat for a lost Orker, and names the instant the attempt will time out ([CLI.md](./CLI.md), "`orkctl run inspect`").

**Fix.** Restart the Orker. For `ACTIVITY_TIMEOUT`, compare the task's `timeout_seconds` with the work it does.

### A run failed with `RUN_HISTORY_LIMIT`

**Cause.** The run would have exceeded the cluster's history limit for one execution, so it stopped explicitly rather than being terminated. The message says how much history was recorded and what was still running, the pins open and the awaits open. Category `InfrastructureError`, and not retried ([OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "8.3 Engine side"; `internal/engine/history_guard.go`). A stage of tasks that all start together is the binding shape ([DSL.md](./DSL.md), "How many tasks an Orkflow may declare").

**Confirm.** `orkctl run get <run-id>` shows the run's error and its details.

**Fix.** Split the Orkflow into composed Orkflows, so each run is smaller, and keep task outputs small. The deployment's bounds are in ([DEPLOY.md](./DEPLOY.md), "Task ceiling and the flat-stage bound").

### A run failed with `RUN_START_FAILED`

**Cause.** The run got an id, but the engine could not start it. The definition could not be written, or the start was refused. The run is recorded as `FAILED` with `RUN_START_FAILED`, and the cause is its message. The submit call also returned an error. A run in the list with this code is therefore not a run that is still going (`internal/api/service.go`, `recordFailedStart`).

**Confirm.** `orkctl run get <run-id>` shows the cause in the run's error message.

**Fix.** Check the cluster (`make status` on the local stack), then submit again.

### A fan-out failed with `EACH_TOO_MANY_ITEMS`, `EACH_OVER_NOT_ARRAY` or `EACH_OVER_MISSING`

**Cause.** A fan-out (`each:`) cannot start its items. Its array is missing, is not an array, or has more elements than `max_items`. All three are contract violations and are not retried. `EACH_TOO_MANY_ITEMS` fails before any item runs (`internal/engine/each.go`; [DSL.md](./DSL.md), "Fan-out (`each:`)").

**Fix.** Make the array exist, make it an array, and keep it within `max_items`. The deployment's total item ceiling is `engine.each_max_items` (`ORK_EACH_MAX_ITEMS`) ([DEPLOY.md](./DEPLOY.md), "1. Configuration").

### Reading a failed run

Reading a failed run takes four steps. Most of them need only the commands from the top of this page.

1. **The run.** `orkctl run get <run-id>` shows the run's own error, `code: message`, and each task's final outcome. The run record keeps only each task's final outcome, so a try that failed and was retried away does not appear here ([CLI.md](./CLI.md), "`orkctl run get`").
2. **Every try.** `orkctl run attempts <run-id> --task <task>` lists every try, oldest first, with its category and code, its exit status and the Orker that ran it. A try whose Orker died before filing a record shows as `UNREPORTED`. That is missing evidence, not a failure ([UI_CLI_PARITY.md](./UI_CLI_PARITY.md), "Task attempts").
3. **The output.** `orkctl run logs <run-id> --task <task> --attempt <n>` prints what that try wrote ([CLI.md](./CLI.md), "`orkctl run logs`").
4. **The timing.** `orkctl run timing <run-id> --task <task>` splits each task's time into wait, run and total. A value that cannot be known prints `unavailable` with its reason, never zero ([UI_CLI_PARITY.md](./UI_CLI_PARITY.md), "Task timing").

The UI run page shows the same attempt timeline. Each failed attempt has its own error and a link that pins the log pane to that attempt ([UI_CLI_PARITY.md](./UI_CLI_PARITY.md), "Task attempts"). For scripts, `orkctl run watch` exits `0` when the run succeeded, `2` when it failed and `3` when it was canceled ([CLI.md](./CLI.md), "`orkctl run watch`").

Every terminal failure has a machine-readable code and a human-readable message ([AGENTS.md](../AGENTS.md), "15. Product invariants"). If a failure has neither, that is a defect to report, not a state to work around.

### Reproducing a failed task on this machine

Two commands run a task's pieces locally, through the same code the Orker uses:

- `orkctl task run --from-run <run-id> --task <task> --render-only` renders the task with the run's recorded upstream bytes, and starts no container.
- `orkctl task run --from-run <run-id> --task <task>` runs the task here. It writes nothing to the deployment, but any external side effect it performs is real ([CLI.md](./CLI.md), "`orkctl task run`").
- `orkctl activity test <image>` checks an image against the activity contract, with no Orkflow and no Temporal server involved ([CLI.md](./CLI.md), "`orkctl activity test`").

## The quickstart

These entries apply to the quickstart: the repository's root `compose.yaml`,
built and started from a clone with `docker compose up --build -d --wait`
([compose.yaml](../compose.yaml)). A setting it names goes in a `.env` file
beside it.

### `docker compose up` fails while building

**Cause.** The first build downloads the toolchain images (Go, Bun, Node), the
Go modules, the UI's packages and the activities' Alpine packages, so it needs
the network, and a few GB free for Docker. A build also fails when Docker's
build cache holds a broken layer, or when the clone was edited into a state
that does not compile.

**Confirm.** The output names the failing service and step, for example
`ork` and its `go build`, or `activity-template` and its download.

**Fix.** Run the same command again once the network is back. For a broken
cache, `docker builder prune` and run it again. For a clone that does not
build, `git status` shows what changed; `git stash` or a fresh clone gets back
to the committed code.

### `docker compose up` stops with "port is already allocated"

**Cause.** The quickstart publishes the UI on 5173, the engine on 8080, the
Temporal UI on 8233 and its activity registry on 5002, on `127.0.0.1`. The
development stack (`make up`) and many other tools use the same ports.

**Fix.** Stop what holds the port, or set `ORKSTACK_UI_PORT`,
`ORKSTACK_API_PORT`, `ORKSTACK_TEMPORAL_UI_PORT` or `ORKSTACK_REGISTRY_PORT`
in `.env` and run `docker compose up --build -d --wait` again.

### `orkctl apply` cannot resolve `127.0.0.1:5002/orkstack/activity-…`

**Cause.** The engine pins each first-party activity by asking the
quickstart's registry for its digest, at `127.0.0.1:5002` inside the engine's
own network, where a forwarder (`registry-forward`) serves it. The forwarder
restarts whenever compose restarts the engine; an engine started some other
way, for example by `docker start`, comes back without it.

**Confirm.** `docker compose ps registry-forward` shows it missing or exited,
or older than `ork`.

**Fix.** `docker compose restart registry-forward`, or
`docker compose up --build -d --wait` again.

### A task fails with `WORKDIR_NOT_BINDABLE` or "bind source path does not exist"

**Cause.** The Orker runs each task through this machine's Docker daemon,
which mounts the task's workspace from `ORKSTACK_WORKDIR` (default
`~/.cache/orkstack-quickstart`) as a path on the daemon's own machine. A daemon
in a virtual machine (Docker Desktop, colima, Lima) sees only the directories
that machine shares. The Orker also refuses to start when that path holds a
comma or a control character, which a container mount cannot carry; its message
names `ORK_ORKER_WORKDIR`, which is `ORKSTACK_WORKDIR` here.

**Fix.** Set `ORKSTACK_WORKDIR` in `.env` to a directory the virtual machine
shares and that only you can write, usually one under your home directory, not
a shared `/tmp`, and whose path holds no comma and no control character, then
`docker compose up --build -d --wait` again ([DEPLOY.md](./DEPLOY.md), "Running
the published images").

### The Orker cannot reach Docker

**Cause.** The compose file mounts `ORKSTACK_DOCKER_SOCKET` (default
`/var/run/docker.sock`) into the Orker, and into the step that pushes the
activities. Rootless Docker, colima and some Docker Desktop setups put the
socket elsewhere; the push step then fails and the engine never starts.

**Confirm.** `docker context inspect --format '{{.Endpoints.docker.Host}}'`
prints the socket the `docker` command uses.

**Fix.** Set `ORKSTACK_DOCKER_SOCKET` in `.env` to that path without its
`unix://` prefix.

### Runs are gone after `docker compose down`

**Cause.** The quickstart's Temporal keeps its history in memory, as a
development server, so a stopped stack forgets its runs. The object store's
volume keeps the stored Orkflows and the artifacts, unless you ran
`docker compose down -v`.

**Fix.** None needed for a local trial: apply and submit again. A deployment
runs a persistent Temporal ([DEPLOY.md](./DEPLOY.md), "0. Topology — what you
run, and where state lives").

## The local stack

These entries apply to `make up`, which runs `scripts/stack.sh up` ([Makefile](../Makefile), "up"). The script starts the infrastructure in Docker Compose, then the engine (`ork`), an Orker and the UI as host processes, and waits for each one to answer ([stack.sh](../scripts/stack.sh), `cmd_up`).

### `make up` times out or stops with "timed out waiting for …"

**Cause.** The script waits for each stage in turn, and gives up when one does not answer in time. The waits are: the Temporal server (60 seconds), the object store (60 seconds), the engine API (60 seconds), the Orker's own answer to the engine (60 seconds), the UI (90 seconds), and finally one check of the browser's socket. A process that exits within two seconds of starting is restarted up to three times, and its last log line is printed each time. The script's comment notes that an Orker losing a race to reach Temporal is the failure it sees most in practice ([stack.sh](../scripts/stack.sh), `start`).

**Confirm.** `make status` shows which processes are running and the state of the containers. Infrastructure has no `.run` log, so use `make infra-logs`. Process logs are `.run/ork.log`, `.run/orker.log` and `.run/ui.log`, and `make logs` follows them.

**Fix.** Start Docker, then run `make up` again. The script skips processes that are already running. If `ork` exits during startup, read the last line of `.run/ork.log`. Set `ORK_HTTP_ADDR` to another address if the port is taken. The UI's dev server runs with `--strictPort`, so a taken `ORK_UI_PORT` (5173 by default) stops it rather than letting it move ([stack.sh](../scripts/stack.sh), `cmd_up`).

### The Orker refuses to start, or a task fails, on Podman

**Cause.** Podman needs host settings Docker does not, and the Orker's boot probe checks them rather than assuming them ([DEPLOY.md](./DEPLOY.md), "Running on Podman"). The usual ones:

- `RUNTIME_POSTURE_REFUSED` naming `cpu.max`, `memory.max` or `pids.max` as unreadable: a rootless Podman whose user has not been delegated the `cpu`, `memory` and `pids` cgroup controllers, or that is not using the systemd cgroup manager. The limit would not be enforced.
- `RUNTIME_POSTURE_REFUSED` quoting `catatonit` or an init that cannot be found: `--init` needs `init_path` in `containers.conf`.
- `RUNTIME_POSTURE_REFUSED` quoting `open builtin: no such file or directory`: the engine was not identified as Podman, so `seccomp=builtin` reached it. The boot log's `container runtime engine=` line says which engine the Orker found.
- `RUNTIME_POSTURE_REFUSED` saying `identify the container engine` and `can't evaluate field Components`: the CLI is the podman CLI under the name `docker` (the `podman-docker` shim), which speaks the podman grammar. Set `ORK_DOCKER_CLI=podman`.
- `IMAGE_PULL_FAILED` with "server gave HTTP response to HTTPS client": a plain-HTTP registry, the local `localhost:5001` included, that `registries.conf` does not mark `insecure = true`.
- `RUN_AS_ORKER_UNAVAILABLE`: the docker CLI against a rootless Podman socket, run by a non-root Orker, which cannot keep the Orker's uid in the container. Run the Orker as root inside the runtime, or use the podman CLI.

**Confirm.** `.run/orker.log` (or the Orker's log) has a `container runtime` line with `engine=podman`, then the refusal.

**Fix.** Make the change the refusal names in the host's Podman configuration, then start the Orker again.

### `make up` stops with "orker boot checks failed"

**Cause.** The Orker refuses to boot when one of its checks fails (`cmd/orker/main.go`; `internal/orker/activity.go`, `Boot`). The codes are:

- `WORKDIR_NOT_BINDABLE`: see the entry above.
- `RUNTIME_POSTURE_REFUSED`: the probe ran one container built exactly as a task's is, and the runtime did not give it a task's posture. The message names each weaker fact: privileges it could gain, no seccomp filter, a capability, a writable root, `/run`, `/var/tmp` or input, a `/tmp` without `noexec`, a proxy variable the runtime added (by name only), or cgroup v2 limits other than it asked for. It also covers a runtime that refused the container for a reason that is not a bind or a deadline ([OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "3.1 Layout").
- `RUN_AS_ORKER_UNAVAILABLE`, only when `ORK_ORKER_FORCE_USER` is set: running a container as the Orker's own user does not land as the Orker on this host, as on a rootless or user-namespaced daemon. Without `ORK_ORKER_FORCE_USER` the Orker starts, logs `running a task as the Orker's user does not work on this host`, and fails only the tasks that set `run_as_orker_user: true`, with the same code.
- `BIND_PROBE_IMAGE_UNAVAILABLE`: the probe image (`orker.BindProbeImage`, alpine 3.20 by digest) is not present, and it could not be pulled within `ORK_DOCKER_PULL_TIMEOUT`. The message names the image, not the work root ([OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "8.2 Reason codes").
- `BIND_PROBE_TIMED_OUT`: the container daemon was too slow for the probe. It did not say within `ORK_DOCKER_BIND_PROBE_TIMEOUT` (two minutes) whether the probe image is local, or on each of two attempts it did not start the probe container within that time or ended it with a deadline error of its own. The daemon is overloaded or unresponsive. The work root is not the finding, so do not change `ORK_ORKER_WORKDIR`. The probe can take up to nine minutes and ten seconds to give up, longer than the 60 seconds `make up` waits for the Orker, so the refusal may appear in `.run/orker.log` after `make up` has stopped with a timeout ([OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "8.2 Reason codes").
- `RESOURCE_REQUEST_REFUSED`: a resource cap that cannot be read, such as an invalid `ORK_ORKER_MAX_CPU` or `ORK_ORKER_MAX_MEMORY`.

**Confirm.** `.run/orker.log` contains `orker boot checks failed:` followed by the code.

**Fix.** For the probe image, run `docker pull docker.io/library/alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc`. For a weaker posture, fix the runtime: for a proxy variable, remove `proxies` from the docker CLI's `config.json` for the Orker's user, and give a task that needs a proxy its own `env:`; for limits on a rootless daemon, delegate the `cpu`, `memory` and `pids` controllers to its user. For `RUN_AS_ORKER_UNAVAILABLE`, run the Orker as root inside the runtime, or unset `ORK_ORKER_FORCE_USER`. For a timed-out probe, check that `docker run --rm docker.io/library/alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc true` finishes, stop whatever is loading the daemon, and raise `ORK_DOCKER_BIND_PROBE_TIMEOUT` if the daemon is only slow. Correct the cap in `ORK_ORKER_MAX_CPU` or `ORK_ORKER_MAX_MEMORY`, or fix the work root. Then run `make down` and `make up`. Configuration keys are listed in ([DEPLOY.md](./DEPLOY.md), "1. Configuration").

### `uses: shell` fails to apply on a fresh clone

**Cause.** `uses: shell` names a catalog entry. The local stack points the catalog at the local registry only when all six first-party images are there. Otherwise the built-in catalog resolves the name to `ghcr.io/orkstack/activity-shell:0.1`, which may not exist or may not be reachable from this machine ([DEPLOY.md](./DEPLOY.md), "1. Configuration", the `activities:` row; [OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "4.6 Delivery model"). The resolver returns `IMAGE_NOT_FOUND` when the registry has no manifest, which is `invalid_argument`. It returns `IMAGE_RESOLVE_UNAVAILABLE` when the registry cannot be reached or answers with a server error, which is `unavailable` (`internal/dsl/registry_resolver.go`; `internal/api/activity_catalog.go`, `applyResolveError`). The stack's own message says the same thing ([stack.sh](../scripts/stack.sh), `cmd_up`).

**Confirm.** `orkctl activity list` shows the catalog this engine resolves names against. `.run/orkstack.yaml` has an `activities:` block only when the local catalog was written.

**Fix.** Push the images, then restart the stack so the catalog is rewritten:

```bash
make -C images push
make down && make up
```

### A pushed image is not used after `make up`

**Cause.** `make up` rewrites `.run/orkstack.yaml` on every run, but it does not restart a process that is already running, and both processes read that file when they start ([stack.sh](../scripts/stack.sh), `start` and `write_run_config`). The local catalog is also all or nothing. If one of the six images is missing from the registry, the script writes no local catalog at all (`local_activity_catalog`).

**Confirm.** `cat .run/orkstack.yaml` shows whether an `activities:` block was written. `orkctl activity list` shows what the engine resolves.

**Fix.** Push all six images, then `make down && make up`.

### The run page never goes live (no events, the log stays empty)

**Cause.** The browser's socket is refused. The UI is served from a different port than the engine, so every WebSocket is cross-origin. The engine accepts only the origins listed in `ORK_WS_ALLOWED_ORIGINS`, and its default is same-origin only. The local stack allows `localhost` and `127.0.0.1` on the UI's port. The page still renders and a refresh still works, because a refresh reads through the same-origin Connect proxy. The refused upgrade is logged as `websocket upgrade refused` (`internal/api/ws.go`, `NewWebSocketBridge`; [stack.sh](../scripts/stack.sh), `cmd_up`; [DEPLOY.md](./DEPLOY.md), "Running N `ork` replicas").

**Confirm.** `make up` prints `websocket ready` when the origin is accepted, and stops with "the engine refuses websockets from …" when it is not. To run the same probe by hand, the stack's request is:

```bash
curl -s -o /dev/null -w '%{http_code}\n' --max-time 5 \
  -H 'Connection: Upgrade' -H 'Upgrade: websocket' \
  -H 'Sec-WebSocket-Version: 13' -H 'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==' \
  -H 'Origin: http://localhost:5173' \
  'http://127.0.0.1:8080/ws/runs/ork-probe-nonexistent?from_sequence=0'
```

A `404` means the origin is accepted, because the probe run does not exist. A `403` means it is refused.

**Fix.** Add the UI's origin to `ORK_WS_ALLOWED_ORIGINS`, as a comma-separated list of `host:port` or `scheme://host:port` values. Then run `make down && make up`, because the running engine keeps the setting it started with. A deployment sets `engine.ws_allowed_origins` on every replica ([DEPLOY.md](./DEPLOY.md), "Running N `ork` replicas").

### `host.docker.internal` does not resolve inside a container (Linux)

**Cause.** On Linux the name `host.docker.internal` does not resolve inside containers by default. Some integration and browser test suites, and the observability stack, rely on it. The Orker never adds a host mapping to an activity container, by design ([README.md](../README.md), "Linux hosts"; [AGENTS.md](../AGENTS.md), "11.1 Development workflow").

**Confirm.** The check in [CONTRIBUTING.md](../CONTRIBUTING.md), "Linux hosts", prints nothing when the name is not mapped:

```bash
docker run --rm alpine:3.20 getent hosts host.docker.internal
```

**Fix.** Apply the one-time host setup in [CONTRIBUTING.md](../CONTRIBUTING.md), "Linux hosts": map the name to the default bridge gateway, `172.17.0.1`, in `/etc/hosts`, and allow containers on the bridge through the host firewall if that is active. A host with a customised bridge uses its own gateway address.

### `make down` reports a process it did not start, or a process survived

**Cause.** A `bin/ork` or `bin/orker` started by hand from this worktree outlives the pidfiles, and it takes activities from later runs. `make down` finds these processes by the identity of their executable, never by name, and stops them ([stack.sh](../scripts/stack.sh), `worktree_binary_pids` and `cmd_down`).

**Confirm.** The script prints `stopping pid N: it runs this worktree's bin/ork or bin/orker, and make up did not start it`. If a process survives the wait, it prints `processes running this worktree's bin/ork or bin/orker survived`, with their pids.

**Fix.** Nothing is needed when the script stops the process. If it reports a survivor, stop that pid yourself and run `make down` again.

### What `make down` and `make reset` keep

**Cause.** `make down` stops what `make up` started, and keeps the data ([stack.sh](../scripts/stack.sh), `cmd_down`). `make reset` wipes Temporal history, object-store data and build output ([Makefile](../Makefile), "reset"). `make reset-force` skips the confirmation prompt.

**Fix.** Use `make down` to stop the stack and keep its data. Use `make reset` only when every run, artifact and build output may go, because the wipe cannot be undone. The script's comment on the artifact backend explains one trap: `ork` and `orker` must share one `ORK_ARTIFACT_BACKEND`. If only one of them is set differently, archived runs and artifacts split between stores, and a reset then leaves runs that the engine still lists ([stack.sh](../scripts/stack.sh), `cmd_up`).

---

Related references: [DEPLOY.md](./DEPLOY.md) for the deployment's configuration and operator procedures, [DSL.md](./DSL.md) for the Orkflow rules behind these reasons, [CLI.md](./CLI.md) for every command and flag, [UI_CLI_PARITY.md](./UI_CLI_PARITY.md) for what the UI shows, and [OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md) for the full reason-code table.
