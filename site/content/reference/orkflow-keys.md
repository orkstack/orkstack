# Orkflow keys

Every key of an Orkflow document. Templated values are marked; everything
else is taken literally.

```yaml
apiVersion: orkstack/v1
kind: Orkflow
name: hello
tasks:
  - id: greet
    uses: shell
    with:
      op: script
      script: echo hello
```

## Document

| Key | Required | Meaning |
|---|---|---|
| `apiVersion` | yes | Always `orkstack/v1`. |
| `kind` | yes | Always `Orkflow`. |
| `name` | yes | Unique within its namespace: lowercase letters, digits and `-`, starting with a letter. |
| `version` | no | Your version label. Absent: derived from a hash of the document. |
| `namespace` | no | Absent: `default`. |
| `description` | no | Shown in the UI and by `orkctl`. |
| `labels` | no | A map of strings, for filtering. |
| `inputs` | no | The run's parameters: [Input](#input). |
| `outputs` | no | What a run returns: a map from a name to a template. |
| `pins` | no | Named groups of tasks that run on one Orker: [Pin](#pin). |
| `tasks` | yes | The tasks: [Task](#task). |

## Input

`inputs.<name>:`

| Key | Default | Meaning |
|---|---|---|
| `type` | any JSON value | `string`, `number`, `boolean`, `object` or `array`. |
| `default` | none | Used when a run does not pass the input. |
| `required` | `false` | Required only when `true` and there is no `default`. |

Read an input as `{{ .inputs.<name> }}`.

## Task

`tasks[]:`. A task needs `id`, and either `uses` or `await`.

| Key | Default | Meaning |
|---|---|---|
| `id` | required | Unique in the Orkflow: lowercase letters, digits and `-`. |
| `uses` | required | A catalog name (`shell`, `http`, `file`, `json`, `git`, `template`), an image reference, or `orkflow:<name>@<version>`. |
| `needs` | `[]` | The tasks this one waits for. The only thing that orders tasks. |
| `with` | none | The activity's settings. **Templated.** |
| `condition` | none | Runs the task only when this is true. **Templated.** |
| `join` | `all_succeeded` | When the task may start, from how its needs ended: `all_succeeded`, `none_failed`, `any_failed`, `all_done`. |
| `timeout_seconds` | `300` | The container's time limit. |
| `retry` | one attempt | [Retry](#retry). |
| `each` | none | Run once per element of an array: [Fan out over a list](/docs/how-to/fan-out/). |
| `await` | none | Wait for an outside resolution instead of running a container: [Await](#await). |
| `cache` | none | Reuse an identical earlier result: [Cache](#cache). |
| `pool` | the default pool | Which Orkers run the task. |
| `pin` | none | Run on one Orker with the other tasks of this pin. |
| `command` | the image's `CMD` | Replaces the image's default arguments (its `CMD`), as an argument list. The image's entrypoint still runs and receives them. |
| `env` | none | Environment variables. Stored in the Orkflow, so never secrets. |
| `secrets` | none | `[{name, source}]`: files under `/orkstack/input/secrets/`, resolved by the Orker. |
| `resources` | the Orker's default | `cpu` and `memory`, for example `cpu: "2"`, `memory: 4Gi`. |
| `network` | `default` | `default`, or `none` for no network. |
| `cancellation_grace_period_seconds` | `30` | Time between SIGTERM and SIGKILL when a task is cancelled. |
| `run_as_orker_user` | `false` | Run as the Orker's user instead of the image's `USER`. |

## Retry

`retry:`. Without it a task makes one attempt.

| Key | Default | Meaning |
|---|---|---|
| `maximum_attempts` | `1` | Attempts in total. |
| `initial_interval_seconds` | `1` | Wait before the first retry. |
| `backoff_coefficient` | `2.0` | Each wait is this many times the last. |
| `maximum_interval_seconds` | `100` | The longest wait. |

## Fan-out

`each:` — [Fan out over a list](/docs/how-to/fan-out/).

| Key | Required | Meaning |
|---|---|---|
| `over` | yes | The array, as one template. |
| `as` | no | The element's name under `.each`. Default `item`. |
| `max_items` | yes | 1 to 10,000. |
| `max_parallel` | no | Items at once. Default: no limit of the task's own. |

## Await

`await:`. The task runs no container and waits until it is completed or
rejected from outside the run.

| Key | Default | Meaning |
|---|---|---|
| `timeout_seconds` | required | 1 second to 30 days. |
| `callback` | `false` | `true` issues a one-time token at submit, for a caller to resolve the task. |
| `schema` | none | The fields a completion must carry, in the `inputs:` form (`type`, `required`). |

## Cache

`cache:`. The task reuses the result of an identical earlier run of it.

| Key | Default | Meaning |
|---|---|---|
| `enabled` | required | `true` to cache: your statement that the task is safe to reuse. |
| `ttl_seconds` | the deployment's maximum | How long a cached result may be reused. |
| `scope` | `orkflow` | `orkflow`: reused by runs of this Orkflow. `namespace`: by any Orkflow of the namespace whose task has the same `id`, image, settings and inputs. |

## Pin

`pins.<name>:`

| Key | Default | Meaning |
|---|---|---|
| `timeout_seconds` | the sum of its tasks' timeouts | The pin's whole lifetime, from its first task's start to its last task's end. |

## Templates

| Template | Value |
|---|---|
| `{{ .inputs.<name> }}` | a run input |
| `{{ .tasks.<id>.output.<key> }}` | a key of an earlier task's `values.json` |
| `{{ .each.<as> }}`, `.each.index`, `.each.count` | inside a fan-out item |
| `{{ artifact "<id>/<name>" }}` | the local path of an earlier task's artifact (`with:` only) |
| `{{ sh <value> }}` | the value quoted for a shell |

In `with:` and `condition:`, a template reads only the tasks in `needs:`, directly or through them. `outputs:` may read any task except a fan-out.

## Check a document as you write it

The UI's YAML editor checks an Orkflow as you type. It is on the Apply page and
on an Orkflow's Edit tab; Admin → Automation uses it for automation documents.

- **Keys and types, at once.** An unknown key, a wrong type or a missing key is
  underlined on its line, against the same JSON Schema as below. An unknown key
  comes with the key it was probably meant to be.
- **Everything else, from the engine, when you pause.** The engine checks the
  [strict YAML-to-JSON rules](https://github.com/orkstack/orkstack/blob/main/docs/JSON.md#data-written-in-an-authored-document)
  for data such as `with:` (quote `yes` for the string), `needs:` that name no
  task, pools, secrets and image policy.
- **Only the engine says a document has no problems.** The line under the
  editor says so only after the engine has checked exactly the text on screen.

Ctrl-Space suggests the keys a position takes. Hovering a key, or
Ctrl-Shift-Space on its line, shows what it does. F8 goes to the next problem.
Tab indents; to leave the editor with the keyboard, press Escape, then Tab.

## See also

- [DSL reference](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md): every rule, default and validation
- [JSON Schema](https://github.com/orkstack/orkstack/blob/main/docs/schemas/orkflow.v1.json): for editor completion
- [Editor support](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#editor-support): what the schema checks, and what only the engine can
