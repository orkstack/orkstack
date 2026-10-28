# Templates

Write `{{ }}` in a task's `with:` and `condition:` to read run inputs, earlier outputs and artifacts. This page lists the names, the helpers, and where each one is rendered.

## Example

An excerpt from [`examples/http-post.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/http-post.ork.yaml). The `condition:` runs the task only when the status is `200`. The `artifact` helper gives the local path of an upstream file.

```yaml
  - id: summarize
    uses: shell
    needs:
      - publish
      - archive
    condition: '{{ eq .tasks.publish.output.status 200.0 }}'
    with:
      op: exec
      command:
        - sh
        - -c
        - 'printf "status=%s bytes=%s\n" "$1" "$(wc -c < "$2" | tr -d " ")"'
        - sh
        - '{{ .tasks.publish.output.status }}'
        - '{{ artifact "archive/response.json" }}'
```

## Where templates render

| Field | Rendered by | Names available |
|---|---|---|
| `with:` of a task | the Orker, when the task starts | `.inputs`, `.tasks.<id>.output`, `.tasks.<id>.artifacts`, `.each`, and the three helpers |
| `condition:` | the engine, once, before the task starts | `.inputs` and `.tasks.<id>.output` only. No helpers |
| `with:` of a composed task | the engine | `.inputs` and `.tasks.<id>.output` only. No helpers |
| `outputs:` of an Orkflow | the engine | `.inputs` and `.tasks.<id>.output` of any task but a fan-out. No helpers |
| `each.over` | the engine, when the run reaches the task | one reference to an array: `.inputs` or `.tasks.<id>.output` |

Every other field, such as `command`, `env`, `uses` and `needs`, is literal text.

## Names

| Name | Value |
|---|---|
| `.inputs.<name>` | A run input, after its default is applied. |
| `.tasks.<id>.output.<key>` | A key from an upstream task's `values.json`. |
| `.tasks.<id>.artifacts` | The upstream task's artifact references. Prefer the `artifact` helper. |
| `.each.<as>`, `.each.index`, `.each.count` | The current item of a fan-out, its position from `0`, and the number of items. `as` defaults to `item`. |

A template in a task's `with:` or `condition:` reads only the tasks in `needs:`, directly or through them; naming another task is refused at apply. `outputs:` may read any task except a fan-out.

## Helpers

| Helper | Returns |
|---|---|
| `artifact "<task>/<name>"` | The local path of an upstream artifact, placed before the task starts. In `with:` only. |
| `env "<NAME>"` | The value of a variable on the Orker, if `ORK_ORKER_ENV_ALLOWLIST` names it. Otherwise the task fails with `RENDER_FAILED`. |
| `sh <value>` | The value in single quotes, for a POSIX shell. Embedded single quotes are escaped. |

Go's built-in functions (`eq`, `ne`, `lt`, `and`, `or`, `not`, `index`, `len`, `printf`) also work; no other helpers exist.

## Rules

| Case | Result |
|---|---|
| A key that nobody supplied | The task fails with `RENDER_FAILED`. |
| A rendered value | Always a string. |
| A literal `{{` in `with:` | Write `{{ "{{" }}`. |
| A value that begins with `{{` | Quote the YAML scalar: `'{{ .inputs.x }}'`. Unquoted, YAML reads it as a mapping. |
| `artifact` or `env` in a `condition:` | The condition fails when it is evaluated. |

## Condition truthiness

A `condition:` is false when its rendered text, trimmed and lower-cased, is one of `""`, `false`, `0`, `no`, `off`, `null`, `<no value>`, `[]`, `map[]`, `<nil>` or `unspecified`. Anything else is true.

Compare an enum with `eq` rather than rendering a free-form answer bare. A rendered `no` is false, and a rendered `No thanks` is true. Use `{{ eq .tasks.decide.output.route "canary" }}` instead.

## Limits

- Templates compute nothing. There is no expression language. Do arithmetic and decisions in a task.
- A template reads no file and makes no network call. `artifact` and `env` resolve on the Orker only.

## See also

- [DSL.md, “Templating”](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#templating): the full rules
- [ORKFLOW_PATTERNS.md, “The rule: the platform moves data, tasks compute”](https://github.com/orkstack/orkstack/blob/main/docs/ORKFLOW_PATTERNS.md#the-rule-the-platform-moves-data-tasks-compute): why computation belongs in a task
- Keys: [Orkflow keys](/docs/reference/orkflow-keys/)
