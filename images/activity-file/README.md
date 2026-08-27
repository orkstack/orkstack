# activity-file

Seven file operations, and deliberately no eighth: `copy`, `checksum`, `gzip`,
`gunzip`, `tar`, `untar`, `concat`. AGENTS.md §9 says to keep this activity
small and Unix-like rather than growing it into a general file-processing
framework, so a workload that needs more builds its own image — which under the
OCI Activity Contract costs nothing.

```yaml
- id: package
  uses: file
  needs: [build]
  with:
    op: tar
    srcs: ["artifacts/build/app", "artifacts/build/config.yaml"]
    as: release.tar.gz
```

## Paths

Sources resolve inside the workspace. A relative path resolves under
`/orkstack/input/`, so `artifacts/build/app` means
`/orkstack/input/artifacts/build/app`. An absolute path must already be under
`/orkstack`. A `..` component is **refused, not normalized** — a path can never
be argued out of the workspace.

Every op that produces something writes exactly one file to
`/orkstack/output/artifacts/`, which is what makes it one output artifact
(§3.5). The output name is `as`, or a sensible default derived from the source.
It must be a plain file name: no `/`, no `..`, at most 128 bytes.

## Operations

| `op` | Reads | Writes | `as` default |
|---|---|---|---|
| `copy` | `src` | a copy | `basename(src)` |
| `checksum` | `src` | *nothing* | — |
| `gzip` | `src` | the compressed file | `basename(src).gz` |
| `gunzip` | `src` | the decompressed file | `basename(src)` minus `.gz` |
| `tar` | `srcs[]` | a gzipped tar of those files | `archive.tar.gz` |
| `untar` | `src`, `member` | that one member, extracted | `basename(member)` |
| `concat` | `srcs[]` | the files joined in order | **required** |

`tar` stores each source under its base name, so the archive carries no
workspace path inside it.

`untar` extracts **one named member**, because an artifact is a regular file
and §3.5 has no directory semantics — a whole hierarchy is not an artifact.
`member` is required, and is a path inside the archive.

`checksum` produces no artifact. It reads a file and reports its digest; there
is nothing new to store, and `values.json` already carries the answer.

## Output

`/orkstack/output/values.json` is the same three keys for every op:

```json
{
  "path": "/orkstack/output/artifacts/release.tar.gz",
  "sizeBytes": 4096,
  "sha256": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
}
```

`path` names the file the op produced — or, for `checksum`, the file it read.

## Errors

A missing or unreadable source, a `..` in a path, an over-long or unsafe
artifact name, or a failed compression is a non-zero exit with a message on
stderr. An unknown or missing `op` exits `2`.

## Cancellation

The real work runs in the background under a TERM trap (§10.2 rule 2a), so a
long `tar` is interrupted within the cancellation grace period.

## Building

```bash
make -C images image-file
```
