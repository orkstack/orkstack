# OrkStack Orkflow DSL (v0)

An orkflow is a YAML document compiled into the `orkstack.v1.Orkflow` proto and
applied to the `ork` engine with `orkctl apply`.

## Top-level fields

| field         | required | description                                            |
|---------------|----------|--------------------------------------------------------|
| `namespace`   | no       | Slug scope; absent means the built-in `default`.        |
| `name`        | yes      | Slug identifier, unique within its namespace.           |
| `version`     | no       | Author-pinned version; defaults to a content hash.     |
| `description` | no       | Human description.                                     |
| `labels`      | no       | `map[string]string` for organization/filtering.        |
| `inputs`      | no       | Declared input parameters (see below).                 |
| `tasks`       | yes      | List of task nodes forming a DAG.                      |

### `namespace`

A namespace is a scope: an orkflow name is unique *within* one, so two
namespaces may each hold a `deploy` and never see each other's. Definitions and
runs are stored and indexed under it, and every read is scoped by it —
`orkctl orkflow list`, `orkctl orkflow get`, `orkctl run list` and run
submission all answer for one namespace and never for another's documents.

A run id is scoped the same way, and this is worth stating because an id looks
like an address that stands on its own. It does not: `orkctl run get`,
`orkctl run watch` and `orkctl run cancel` all address a run *inside* the
namespace the command acts in, and a run of another one is not found even when
its id is known. An id is not a secret — submit prints it, tickets quote it,
CI transcripts echo it — and cancel is a write, so holding one is not
authority over somebody else's tenant.

There are two ways to say which, and they must agree:

```bash
orkctl apply payments.ork.yaml              # the document's own `namespace:`
orkctl apply portable.ork.yaml -n payments  # a document that names none
```

A document that names `payments` applied with `-n staging` is refused: one of
the two is a mistake, and guessing which would store the definition where its
author will not look for it.

The namespace a command acts in is, most specific first: `--namespace`/`-n`,
then `ORK_NAMESPACE`, then the one saved by `orkctl namespace use`, then
`default`. Namespaces themselves are managed with
`orkctl namespace create|list|get|update|delete|use`; a namespace also carries
a `default_pool` and an `allowed_pools` list, which decide and bound where its
tasks run (see `pool` below).

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
    pool: ""             # optional: the set of Orkers this task runs on
    with:                # plugin-specific config (templated)
      key: value
```

A task with no `pool:` runs on the deployment's default pool, which answers to
the name `main`. Writing `pool: main` is the same thing, always: `main` names
the default pool, not one fixed set of Orkers, so wherever an operator has
pointed the default, both spellings go there and a run reports both on the
same pool. Within a run, one pool name means one set of Orkers and one set of
Orkers has one name — so the pool a run reports a task on is a pool you can
write to reach those same Orkers. A task that names a pool nothing serves
waits for one; M10.2 refuses the submission up front instead.

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

1. `namespace`, when present, is slug-formatted (`^[a-z][a-z0-9-]*$`); absent
   means `default`. The engine additionally refuses a namespace nothing
   registered, naming the ones that exist — the compiler has no registry to
   consult.
2. `name` present and slug-formatted (`^[a-z][a-z0-9-]*$`) within its namespace.
3. Task ids unique and slug-formatted.
4. Every `needs` entry references an existing task id.
5. The `needs` graph is acyclic (DAG).
6. `plugin` is a non-empty known plugin name.
7. A task `pool`, when present, is slug-formatted.
8. Templates parse and only reference declared inputs and upstream tasks.

A slug is at most **200 bytes**, in every position above. The bound is half the
rule and not a detail of it: a namespace, an orkflow name and a version are
path components of a stored object key, and a pool becomes an execution
channel, so a longer name is one the compiler could accept and nothing could
store. It is the same 200 the orkflow store and the namespace registry enforce
(`pkg/artifacts`), and the two are held together by measurement rather than by
a shared constant — `pkg/` may not import `internal/`.

## Example

See `examples/hello.ork.yaml` for a fetch → transform → persist pipeline that
exercises the `http`, `shell`, and `file` plugins with artifact hand-off.
