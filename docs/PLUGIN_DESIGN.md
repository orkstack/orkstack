# Plugin design: the `op` pattern

Every OrkStack plugin exposes **one generic operation selector, spelled `op`**.
`op` names *what the task does*; the rest of `with:` supplies the arguments that
operation needs. A reader who knows one plugin can therefore guess the shape of
the next one.

```yaml
tasks:
  - id: fetch
    plugin: http
    with:
      op: get                 # what to do
      url: "{{ .inputs.url }}"  # what to do it to
```

`docs/PLUGINS.md` describes the *contract* a plugin implements (`Name`,
`Describe`, `Execute`). This document describes the *convention* every plugin
follows on top of it.

## Why one field

- **One thing to learn.** `op` is the first line of every task, in every plugin.
- **Discoverable.** `op` is a property of the plugin's `Describe().InputSchema`
  with an `enum` of its values, so `orkctl plugin list`, the UI, and any editor
  reading the schema can offer the operations without special-casing plugins.
- **Closed set, open plugin.** Adding an operation is an additive change to one
  plugin. It never touches the proto, the engine, or the DSL — `with:` is an
  opaque struct to everything except the plugin that receives it.
- **Errors point at the author.** A misspelled operation is caught before any
  side effect, and reported as the author's mistake rather than as a runtime
  surprise 200 lines into a shell script.

## Rules for plugin authors

1. **Name the field `op`.** Not `action`, `mode`, `method`, or `command`.
2. **Values are lowercase verbs**: `get`, `write`, `copy`, `exec`. Compare
   case-insensitively; report the value the author actually wrote.
3. **Declare the enum in `Describe().InputSchema`** under `properties.op`, so
   discovery advertises the real set.
4. **Validate `op` first**, before touching the network, the filesystem, or any
   other argument that could fail for an unrelated reason.
5. **Reject an unknown `op` permanently** (`plugin.Permanent`), with a message
   that quotes the rejected value *and* lists the supported ops. An unknown op
   is never retryable: retrying cannot make a typo correct.
6. **Never silently ignore an unknown `op`.** A dropped field is a config bug
   that produces a plausible-looking run doing the wrong thing.
7. **Pick a default only when one operation is obviously the common case**, and
   document it. `http` defaults to `get`; `shell` defaults to `exec`; `file` has
   no sensible default and requires `op`.
8. **Keep an op's arguments minimal.** Arguments that only apply to one op are
   documented with that op and ignored by the others.

The rules are enforced by
`internal/orker/activity_test.go:TestBuiltinPlugins_AllExposeAndValidateTheGenericOpSelector`,
which walks the builtin registry, asserts every plugin advertises an `op` enum,
and asserts every plugin rejects `op: frobnicate` with a permanent error.

## The builtin ops

| plugin  | ops                                          | default | required |
|---------|----------------------------------------------|---------|----------|
| `http`  | `get`, `post`, `put`, `patch`, `delete`, `head` | `get`   | no       |
| `shell` | `exec`, `eval`, `script`                     | `exec`  | no       |
| `file`  | `write`, `read`, `copy`, `delete`            | —       | yes      |

Per-plugin reference: [`pkg/plugins/http/README.md`](../pkg/plugins/http/README.md),
[`pkg/plugins/shell/README.md`](../pkg/plugins/shell/README.md),
[`pkg/plugins/file/README.md`](../pkg/plugins/file/README.md).

### `http` — the op is the HTTP verb

```yaml
- id: publish
  plugin: http
  with:
    op: post              # get | post | put | patch | delete | head
    url: "https://httpbin.org/post"
    headers:
      Content-Type: application/json
    body: '{"message": "hello"}'
    capture_body_as: response
```

`method` is the pre-`op` spelling and still works, so orkflows written before
this convention keep running. Unlike `op` it accepts any verb (`OPTIONS`,
`TRACE`, a custom one). Setting both to *different* verbs is a permanent error;
setting both to the same verb is fine.

Full example: [`examples/http-post.ork.yaml`](../examples/http-post.ork.yaml).

### `shell` — the op is how the body runs and what a non-zero exit means

```yaml
- id: report
  plugin: shell
  with:
    op: exec              # exec | eval | script
    command: ./build.sh
    capture_stdout_as: build-log
```

| op       | body      | meaning                                                                |
|----------|-----------|------------------------------------------------------------------------|
| `exec`   | `command` | The command **is the work**: `sh -c command`. A non-zero exit fails the task. |
| `eval`   | `command` | The command **is a question**: `sh -c command`. Any exit succeeds; the answer is `output.value` (stdout, trimmed) and the caller branches on `output.exit_code`. |
| `script` | `script`  | A **program** is the work. The body is written to `.ork-shell/script` in the workdir and executed by `interpreter` (default `sh`) or by its own `#!` line. A non-zero exit fails the task. |

`eval` is for probes and measurements whose failure is information, not an
outage. `script` is for work that is a program rather than a one-liner: it
keeps its own `$0`, needs no `&&` chaining, and can choose its interpreter.

```yaml
- id: package
  plugin: shell
  with:
    op: script
    interpreter: sh       # optional; a `#!` first line wins if this is unset
    script: |
      set -eu
      mkdir -p dist
      date -u +%Y-%m-%dT%H:%M:%SZ > dist/built-at.txt
    capture_paths: ["dist/*"]
```

All three ops take the same capture arguments and produce the same artifacts;
`eval` produces them even after a non-zero exit.

Full example: [`examples/shell-eval.ork.yaml`](../examples/shell-eval.ork.yaml).

### `file` — the op is the filesystem operation

```yaml
- id: save
  plugin: file
  with:
    op: write             # write | read | copy | delete
    path: result.txt
    content: "{{ .tasks.report.output.stdout }}"
```

| op       | arguments        | produces an artifact | notes                                    |
|----------|------------------|----------------------|------------------------------------------|
| `write`  | `path`, `content`| yes                  | creates parent directories               |
| `read`   | `path`           | yes                  | re-exports a materialized upstream file  |
| `copy`   | `from`, `to`     | yes                  | both ends stay inside the workdir        |
| `delete` | `path`           | no                   | idempotent: a missing path still succeeds|

`file` has no default op because none of the four is safer to guess than the
others; omitting `op` is an error.

Full example: [`examples/file-read.ork.yaml`](../examples/file-read.ork.yaml).

### Container plugins

There is **no container plugin in the tree yet** — the builtin registry holds
exactly `http`, `shell` and `file`
(`internal/orker/registry.go`). When one lands it is bound by the rules above
like any other plugin; nothing in the engine, DSL or proto needs to change for
it, because `op` lives entirely inside `with:`.

## Unknown ops in practice

```console
$ orkctl run submit http-post --input op=frobnicate
task publish failed: http: unknown op "frobnicate" (want get, post, put, patch, delete, head)
```

The failure is permanent, so Temporal does not retry it, and it happens before
the request is built — an unknown op never reaches the network, the filesystem,
or a shell.

## Adding an op to an existing plugin

1. Add the value to the plugin's `switch` and to the `enum` in `Describe()`.
2. Add a behavior test for the new op **and** confirm the unknown-op test still
   passes.
3. Document it in the plugin's `README.md` and in the table above.
4. Add or extend an example under `examples/` — every task in every shipped
   example sets an explicit `op`, and
   `internal/dsl/compile_test.go:TestCompile_AcceptsEveryShippedExample`
   enforces it.
