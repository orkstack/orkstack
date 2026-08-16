# `file` plugin

Write, read, copy or delete a file in the task work directory.

## Ops

`op` is the generic operation selector every OrkStack plugin shares
(see [docs/PLUGIN_DESIGN.md](../../../docs/PLUGIN_DESIGN.md)). For `file` it
names the filesystem operation.

| `op`     | arguments         | produces an artifact | notes                                       |
|----------|-------------------|----------------------|---------------------------------------------|
| `write`  | `path`, `content` | yes                  | creates parent directories                  |
| `read`   | `path`            | yes                  | re-exports a materialized upstream file     |
| `copy`   | `from`, `to`      | yes                  | both ends must stay inside the workdir      |
| `delete` | `path`            | no                   | idempotent: a missing path still succeeds   |

- **`op` is required.** None of the four is a safe guess, so omitting it fails
  with ``file: `op` is required (write, read, copy or delete)``.
- An unknown `op` fails the task **permanently**:
  `file: unknown op "chmod" (want write, read, copy or delete)`.
- `delete` being idempotent matters because Temporal may retry the activity: a
  second attempt behaves like the first.

## `with`

| field        | applies to              | description                                                     |
|--------------|-------------------------|-----------------------------------------------------------------|
| `op`         | all                     | operation selector (**required**)                               |
| `path`       | `write`, `read`, `delete` | target path                                                   |
| `content`    | `write`                 | file contents                                                   |
| `from`, `to` | `copy`                  | source and destination                                          |
| `capture_as` | `write`, `read`, `copy` | artifact name override (default: the workdir-relative path)     |

Every path resolves inside the task work directory — which includes files
materialized by the `artifact` template helper. A path that escapes it fails the
task permanently.

## `output`

| field        | description                                   |
|--------------|-----------------------------------------------|
| `path`       | the resulting path, relative to the workdir   |
| `size_bytes` | size of the resulting file (`0` for a missing `delete` target) |

## Example

```yaml
- id: seed
  plugin: file
  with:
    op: write
    path: notes/greeting.txt
    content: "{{ .inputs.greeting }}"
    capture_as: greeting

- id: reread
  plugin: file
  needs: [seed]
  with:
    op: read
    path: '{{ artifact "seed/greeting" }}'
```

Runnable: [`examples/file-read.ork.yaml`](../../../examples/file-read.ork.yaml).
