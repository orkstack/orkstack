# OrkStack Orkflow DSL (v0)

An orkflow is a YAML document compiled into the `orkstack.v1.Orkflow` proto and
applied to the `ork` engine with `orkctl apply`.

## Top-level fields

| field         | required | description                                            |
|---------------|----------|--------------------------------------------------------|
| `namespace`   | no       | Slug scope; absent means the built-in `default`.       |
| `name`        | yes      | Slug identifier, unique within its namespace.           |
| `version`     | no       | Author-pinned version; defaults to a content hash.     |
| `description` | no       | Human description.                                     |
| `labels`      | no       | `map[string]string` for organization/filtering.        |
| `inputs`      | no       | Declared input parameters (see below).                 |
| `tasks`       | yes      | List of task nodes forming a DAG.                      |

## Inputs

```yaml
inputs:
  url:
    type: string          # string | number | boolean | object | array
    default: "https://…"  # optional
    required: false        # default false when a default is present
```

Provided at submit time and referenced as `{{ .inputs.<name> }}`.

## Tasks

```yaml
tasks:
  - id: fetch            # unique within the orkflow
    plugin: http         # registered plugin name
    needs: [other_id]    # DAG edges; must reference existing task ids; no cycles
    condition: ""        # optional gate expression (v0: truthy string / template)
    timeout_seconds: 300 # optional, default 300
    retry:               # optional Temporal retry policy
      maximum_attempts: 3
      initial_interval_seconds: 1
      backoff_coefficient: 2.0
      maximum_interval_seconds: 100
    pool: ""             # optional Orker pool override
    with:                # plugin-specific config (templated)
      key: value
```

## Templating

Task `with` values and `condition` are rendered with Go `text/template` against
this context:

- `.inputs.<name>` — submitted inputs.
- `.tasks.<id>.output.<field>` — a completed upstream task's output payload.
- Helper `artifact "<taskId>/<artifactName>"` — resolves to a **local path** the
  Orker has materialized before the task runs (used by `shell`/`file`).
- Helper `env "<NAME>"` — reads an allowlisted environment variable on the Orker.

Only tasks listed (transitively) in `needs` are guaranteed to be resolvable;
referencing an unrelated task's output is a compile-time error.

## Validation rules (enforced by `internal/dsl`)

1. `namespace`, when present, is slug-formatted (`^[a-z][a-z0-9-]*$`); absent means `default`.
2. `name` present and slug-formatted (`^[a-z][a-z0-9-]*$`) within its namespace.
3. Task ids unique and slug-formatted.
4. Every `needs` entry references an existing task id.
5. The `needs` graph is acyclic (DAG).
6. `plugin` is a non-empty known plugin name.
7. A task `pool`, when present, is slug-formatted.
8. Templates parse and only reference declared inputs and upstream tasks.

## Example

See `examples/hello.ork.yaml` for a fetch → transform → persist pipeline that
exercises the `http`, `shell`, and `file` plugins with artifact hand-off.
