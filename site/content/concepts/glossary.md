# Glossary

Look up a word that OrkStack uses in its interface and its docs. The words are in alphabetical order.

| Term | Meaning |
|---|---|
| **Activity** | An OCI image that a task runs. It reads files, writes files and exits. |
| **Artifact** | A regular file a task writes to `/orkstack/output/artifacts/`. It is stored in object storage, and other tasks receive it by reference. |
| **Automation document** | A YAML file, `kind: Automation`, that declares a namespace's settings, schedules, triggers and webhooks by name. |
| **Await** (awaited task) | A task that runs no container. It waits until someone completes or rejects it, or until its deadline passes. |
| **Budget** | A namespace's bound on reserved cpu-seconds and GiB-seconds over a rolling period. A soft budget only reports. An enforced budget refuses new runs. |
| **Cache** (cached task) | A task that reuses an earlier result for an identical run of it. You turn it on with `cache:`. |
| **Callback token** | A one-time credential issued when an await sets `callback: true`. It lets one caller resolve that await. |
| **Carries on** | A long run continues under the same run ID, from its saved state. It stays one run in every list. |
| **Commit** | The record that a task succeeded and its outputs are stored. A retry that finds it returns the stored result. |
| **Composed task** | A task whose `uses:` is `orkflow:<name>@<version>`. It starts a child run of that Orkflow, and waits for it. |
| **Cordon** | Stops an Orker from taking new tasks or opening new pins. Work already running continues. A restarted Orker is ready again. |
| **Current version** | The version of an Orkflow that new runs use, unless a run names another version. |
| **Drain** | Cordons an Orker and waits until it runs no task and hosts no pin. |
| **Failure handler** | A task whose `join:` is `any_failed` or `all_done`. It runs in a failing run, and the run still ends `FAILED` with its original reason. |
| **Fan-out** | A task with `each:`. It runs one item for each element of an array. |
| **Fire** | One occasion on which a schedule acts: on time, by backfill, or by hand. |
| **Hold** | Closes a namespace to new runs, with a reason. Runs already accepted finish. |
| **Idempotency key** (activity) | The key in a container's `task.json`, in the form `{namespace}/{run ID}/{task ID}`. It stays the same across the retries of one task. |
| **Idempotency key** (submit) | The `--idempotency-key` you pass to `orkctl run submit`. It de-duplicates repeated submissions in a namespace. |
| **Item** | One element of a fan-out, identified by its position in the array, counting from 0. |
| **Join policy** (`join:`) | When a task may start, from how its needs ended: `all_succeeded` (the default), `none_failed`, `any_failed` or `all_done`. |
| **Maximum run retention** | The deployment's ceiling on run retention. A namespace that sets none inherits it. |
| **Memo** | The stored pointer that a cached task reuses. It points to a commit. |
| **Namespace** | A scope for Orkflows, runs, pools and settings. An Orkflow's name is unique within its namespace. |
| **Origin** | The record of what started a run: a kind, the object it came from, a reference you supply, and the principal. |
| **Orker** | An execution process. It runs each task's container. |
| **Orkflow** | A YAML document, with `apiVersion: orkstack/v1` and `kind: Orkflow`, that lists tasks and the order they run in. |
| **Outputs** | The values an Orkflow returns, declared under `outputs:`. |
| **Pin** | A named group of tasks that run on one Orker and share `/orkstack/shared`. |
| **Pin host** | An Orker that accepts pins. It is an Orker with `ORK_ORKER_MAX_PINS` above zero. |
| **Pool** | The set of Orkers a task may run on. `main` is the default pool. |
| **Principal** | The name a gateway asserts in the `Ork-Principal` header. OrkStack records it as asserted, and never as verified. |
| **Release** | One build of OrkStack: `ork` and `orker` deployed together. A release is current, ramping, previous or candidate. It is drained when no run on it remains and none of its Orkers hosts an open pin. |
| **Run** | One execution of an Orkflow version. It has its own ID and its own event stream. |
| **Run ID** | The ID of a run. Address a run with it inside its namespace. Treat it as opaque. |
| **Run retention** | How long a finished run stays readable, counted from when it finished. |
| **Saved state** | The state a run keeps when it carries on. If it cannot be read, the run stops with a `RUN_STATE_*` reason. |
| **Schedule** | An object that starts runs of one Orkflow at times set by cron expressions or an interval. |
| **Task** | One step of an Orkflow: an activity, an awaited task or a composed task. Its `needs:` set what must finish first. |
| **Telemetry policy** | A namespace's choice of telemetry sink, whether its task logs export, and the attributes it stamps. |
| **Telemetry sink** | A named OpenTelemetry destination that the deployment declares. |
| **Trigger** | An inbound plain-HTTP starter. A caller that holds its token starts one run. |
| **Usage** | The cpu and memory each attempt was granted, multiplied by how long it held them. It is computed on read. It is not measured utilization. |
| **Values** | A task's structured result: a JSON object in `values.json`. |
| **Webhook** | A signed callback that OrkStack sends to a URL you register, for run events. |

## See also

- [AGENTS.md, Orkflow execution model](https://github.com/orkstack/orkstack/blob/main/AGENTS.md#7-orkflow-execution-model): the repository's vocabulary, in section 7.
- [docs/DSL.md](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md): the keys that Orkflow terms map to.
