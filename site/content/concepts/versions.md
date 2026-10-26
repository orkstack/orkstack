# Orkflows and versions

Learn how an Orkflow's versions work, and why a run keeps the version it started with.

```yaml
# An Orkflow may open with comments; the header follows them.
apiVersion: orkstack/v1
kind: Orkflow
name: deploy
version: "1.4.0"
```

`name` and `version` are the Orkflow's own identity. `apiVersion` is the format of the document, and `orkstack/v1` is the only one.

## Each apply makes a version

Applying a file stores it as a version. Applying an unchanged file stores nothing new. Applying an edited file stores a new version, and a stored version is never rewritten.

If you leave `version:` out, OrkStack derives one from the file's bytes. If you pin one, applying a changed file under that same version is refused, because a run pinned to it still reads those bytes. Bump the version, or drop it.

## A run keeps its version

Every run records the version it started with, and the digest of that version's definition. A run keeps its version even when the current version changes.

Each Orkflow has one **current** version. A run that names no version uses it. Until someone promotes a version, the current one is the newest applied version.

## Move the current version

These commands change which version new runs use:

| Command | What it does |
|---|---|
| `orkctl orkflow versions deploy` | Lists the versions, newest first, and marks the current one. |
| `orkctl orkflow promote deploy 1.4.0` | Makes `1.4.0` the current version. |
| `orkctl orkflow promote deploy 1.3.0` | Rolls back: the same command, with an older version. |
| `orkctl orkflow diff deploy 1.3.0 1.4.0` | Shows what changed between two versions. |
| `orkctl orkflow compat deploy 1.4.0` | Shows what promoting would break. |

Promoting changes what starts next. It never changes a run that is already running.

## Breaking changes need an acknowledgement

An Orkflow's declared interface is its `inputs:` and `outputs:`. Promoting a version is refused when it breaks that interface, for example by removing an input or adding a required one. The refusal names each change. Pass `--acknowledge-breaking` to promote anyway.

Changes to tasks, images, pools and retries never block a promotion. Only the interface does. Run `orkctl orkflow compat` first to see what a promotion would break.

## See also

- [docs/DSL.md, version](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#version): the rules for pinned and derived versions.
- [docs/DSL.md, Which version runs](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#which-version-runs): promotion, rollback and the commands that compare versions.
- [docs/DSL.md, Promoting compares declared interfaces](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#promoting-compares-declared-interfaces): the table of what counts as breaking.
