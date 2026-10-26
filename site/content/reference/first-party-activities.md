# First-party activities

Every `with:` key, default and `values.json` key of the six images that ship with OrkStack. Each section matches the image's interface label, and `orkctl activity inspect <name>` shows the same keys from a deployment.

```yaml
- id: notify
  uses: http
  with:
    op: post
    url: https://example.test/hooks/deploy
    body:
      status: ok
```

| `uses:` | Image | Default `op` |
|---|---|---|
| `http` | `activity-http` | `get` |
| `shell` | `activity-shell` | `exec` |
| `file` | `activity-file` | none: `op` is required |
| `json` | `activity-json` | none: `op` is required |
| `git` | `activity-git` | none: `op` is required |
| `template` | `activity-template` | none: `op` is required |

## http

Makes one HTTP request. The ops are the HTTP methods.

| Key | Ops | Type | Default | Meaning |
|---|---|---|---|---|
| `op` | all | string | `get` | `get`, `post`, `put`, `patch`, `delete` or `head`. |
| `url` | all | string | required | The absolute request URL. |
| `headers` | all | object | none | Request headers. |
| `body` | all | string, object or array | none | A string is sent as is. A mapping or a list is sent as JSON, with `Content-Type: application/json` unless `headers` sets one. |
| `timeoutSeconds` | all | number | none | The longest time the request may take. |
| `captureBodyAs` | all | string | none | Save the response body as this artifact. |

| `values.json` key | Meaning |
|---|---|
| `status` | The HTTP status code. Any status is a success, so check it. |
| `headers` | The response headers. A repeated header keeps its last value. |
| `durationMs` | How long the request took, in milliseconds. |
| `sizeBytes` | The size of the response body. |
| `json` | The parsed body. Present only when the body is JSON of at most 1 MiB. |

## shell

Runs a command, a script or an evaluation.

| Key | Ops | Type | Default | Meaning |
|---|---|---|---|---|
| `op` | — | string | `exec` | `exec`, `script` or `eval`. `exec` and `script` fail on a non-zero exit. `eval` reports it in `exitCode`. |
| `command` | `exec`, `eval`; optional in `script` | array of strings | required in `exec` and `eval` | The argument list. Each element is one argument. In `script`, the elements are the script's arguments. |
| `script` | `script` | string | required in `script` | The program text. |
| `interpreter` | `script` | string | the script's `#!` line, else `sh` | The program that runs the script. |

| `values.json` key | Meaning |
|---|---|
| `exitCode` | The exit status of the command. Every op writes it. |
| `scriptPath` | Where the script was written. `script` only. |
| `value` | The command's stdout. `eval` only. |

## file

Copies, compresses, archives, checksums and joins files. A relative `src` or `srcs` path starts under `/orkstack/input/`.

| Key | Ops | Type | Default | Meaning |
|---|---|---|---|---|
| `op` | — | string | required | `copy`, `checksum`, `gzip`, `gunzip`, `tar`, `untar` or `concat`. |
| `src` | `copy`, `checksum`, `gzip`, `gunzip`, `untar` | string | required | The source file. |
| `srcs` | `tar`, `concat` | array of strings | required | The source files. `concat` joins them in this order. |
| `member` | `untar` | string | required | The one file to extract from the archive. |
| `as` | all but `checksum` | string | see meaning | The artifact name. Default: the source's name for `copy`; the name plus `.gz` for `gzip`; the name without `.gz` for `gunzip`; `archive.tar.gz` for `tar`; the member's name for `untar`. `concat` requires it. |

| `values.json` key | Meaning |
|---|---|
| `path` | The file the op produced. For `checksum`, the file it read. |
| `sizeBytes` | The file's size. |
| `sha256` | The file's SHA-256 digest. |

## json

Runs one jq filter (`query`), or deep-merges JSON objects (`merge`).

| Key | Ops | Type | Default | Meaning |
|---|---|---|---|---|
| `op` | — | string | required | `query` or `merge`. |
| `filter` | `query` | string | required | One jq program, from 1 to 65536 bytes. |
| `src` | `query` | string | none | One JSON file. `query` takes exactly one of `src`, `srcs`, `gather` or `nullInput: true`. |
| `srcs` | `query`, `merge` | array of strings | none; required in `merge` | `query` reads every JSON value in the files as one array. `merge` takes at least two files, each holding one object. |
| `slurp` | `query` | boolean | `false` | With `src`, read every JSON value in the file as one array (JSON Lines). |
| `gather` | `query` | string | none | A fan-out's task id. The filter gets one array of its items' results, in item order. |
| `nullInput` | `query` | boolean | `false` | `true` runs the filter once, with `null` as its input. |
| `vars` | `query` | object | `{}` | Bound to the jq variable `$vars`. At most 64 KiB as compact JSON. |
| `as` | `query`, `merge` | string | none | Write the result as this artifact instead of `result`. |

| `values.json` key | Meaning |
|---|---|
| `result` | The result, when `as` is not set. |
| `path`, `sizeBytes`, `sha256` | The artifact's path, size and SHA-256 digest, when `as` is set. |

## git

Reads a repository over HTTPS. `clone` writes one commit as a tarball or as a working tree. `resolve` returns the commit a ref names.

| Key | Ops | Type | Default | Meaning |
|---|---|---|---|---|
| `op` | — | string | required | `clone` or `resolve`. |
| `url` | both | string | required | An `https://` URL with no credentials, query or fragment. |
| `ref` | both | string | `HEAD` | A branch, a tag, a full ref name or a 40-character commit id. |
| `tokenSecret` | both | string | none | The name of a `secrets:` entry that holds the token. |
| `tokenUser` | both | string | `x-access-token` | The user name sent with the token. |
| `caFile` | both | string | none | A PEM file that replaces the trust store for this task. |
| `depth` | `clone` | number | `1` | Commits of history. `0` is the full history. |
| `as` | `clone` | string | `src.tar.gz` | The tarball artifact's name. |
| `dest` | `clone` | string | none | A path under `/orkstack/shared/` in a pinned task. Use it instead of `as`. |

| `values.json` key | Meaning |
|---|---|
| `commit` | The commit that was fetched or resolved. Both ops. |
| `ref` | The ref that matched. Both ops. |
| `path` | The tarball's path, or the working tree's path with `dest`. `clone` only. |
| `sizeBytes`, `sha256` | The tarball's size and SHA-256 digest. `clone` with `as` only. |

## template

Renders one template file into one text artifact, with gomplate 4.3.3.

| Key | Ops | Type | Default | Meaning |
|---|---|---|---|---|
| `op` | — | string | required | `render`, the only op. |
| `src` | `render` | string | required | The template file. |
| `data` | `render` | string | none | A JSON file that holds one object. It is the base of the root context. |
| `values` | `render` | object | none | Merged over `data` for the root context. |
| `datasources` | `render` | object | none | Names mapped to files. A name is letters, digits, `_` or `-`, up to 64 characters. `data` and `values` are reserved. `.yaml` and `.yml` files are read as YAML, others as JSON. |
| `leftDelim`, `rightDelim` | `render` | string | none | Set both or neither, for a template that contains `{{` itself. 1 to 16 bytes, no whitespace. |
| `as` | `render` | string | `src` without `.tmpl`, `.tpl` or `.gotmpl` | The artifact name. |

| `values.json` key | Meaning |
|---|---|
| `path` | The rendered artifact's path. |
| `sizeBytes` | The artifact's size. |
| `sha256` | The artifact's SHA-256 digest. |

## See also

- [Activity images and the `op` convention](https://github.com/orkstack/orkstack/blob/main/docs/ACTIVITY_IMAGES.md): the shared conventions for the six images
- [activity-http README](https://github.com/orkstack/orkstack/blob/main/images/activity-http/README.md): the full `http` contract
- [activity-shell README](https://github.com/orkstack/orkstack/blob/main/images/activity-shell/README.md): the full `shell` contract
- [activity-file README](https://github.com/orkstack/orkstack/blob/main/images/activity-file/README.md): the full `file` contract
- [activity-json README](https://github.com/orkstack/orkstack/blob/main/images/activity-json/README.md): the full `json` contract
- [activity-git README](https://github.com/orkstack/orkstack/blob/main/images/activity-git/README.md): the full `git` contract
- [activity-template README](https://github.com/orkstack/orkstack/blob/main/images/activity-template/README.md): the full `template` contract
- [Use the first-party activities](/docs/how-to/first-party-activities/): how to use them in a task
