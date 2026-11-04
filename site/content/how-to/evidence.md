# Evidence bundles

Export one finished run as a single `tar.gz` file. It holds the run's record, its Orkflow source, each task's values, artifacts, attempt records and logs, and a manifest of hashes. Anyone can check it offline, without a server.

## Export a run

```bash
orkctl run evidence export <run-id> --output run.tar.gz
orkctl run evidence export <run-id> --output run.tar.gz --no-logs --no-artifacts
orkctl run evidence export <run-id> --task convert --item 2 --output item-2.tar.gz
orkctl run evidence export <run-id> --json        # prints MANIFEST.json, writes no file
```

- Only a finished run can be exported.
- The file is written only once the engine confirms a complete bundle. An interrupted export leaves no file behind.
- `--no-logs` and `--no-artifacts` leave those members out. `--task` and `--item` narrow the bundle to one task, or to one item of a fan-out.
- `--overwrite` replaces an existing file.
- The `orkctl` alias cannot write `--output` files. Use the installed binary, as [Run it locally](/docs/get-started/run-locally/#install-orkctl-on-your-machine) explains.

In the UI, choose **Evidence** on a finished run. The dialog shows the manifest first, so you can see what is in the bundle before the download starts. A bundle larger than 256 MiB is not downloaded by the browser. The dialog shows the `orkctl` command to use instead.

## What is in a bundle

| Member | Contents |
|---|---|
| `MANIFEST.json` | The first member. Each member's path, size, SHA-256 and state. |
| `run.jsonl` | The run's archived record and events. |
| `orkflow/source.yaml` | The Orkflow source, exactly as it was applied. |
| `tasks/<task>/commit.json` | The task's commit. Present for a task that succeeded. |
| `tasks/<task>/values.json` | The task's committed values. |
| `tasks/<task>/artifacts/` | Each output artifact. |
| `tasks/<task>/attempts/<n>.json` | The record of one try. |
| `tasks/<task>/attempts/<n>/stdout.log` and `stderr.log` | What that try printed. |

A fan-out's items sit under `tasks/<task>/items/`, one folder per item.

A bundle does not hold the Orker's per-attempt `task.json`, `config.json` or `run.json`. Those files live in a workspace that is removed after each try.

No member carries a secret value, because no store holds one. The bundle does hold the run's input, the `with:` and `env` values of the Orkflow, and the logs and artifacts as they were. Treat the file as sensitive as the run itself.

Each member has a state:

| State | Meaning |
|---|---|
| `present` | The bytes were read and match the recorded hash. |
| `reclaimed` | The store no longer has the object, because cleanup or a lifecycle rule removed it. The recorded hash is kept. |
| `unrecorded` | A try with no record. Its Orker died before it filed one, or the record was reclaimed. |
| `rejected` | The stored definition does not match the digest the run pinned. |

## Verify it offline

```bash
orkctl run evidence verify run.tar.gz
orkctl run evidence verify run.tar.gz --against-store   # also compare with the live run
```

`verify` needs no server and no credentials. It writes nothing to disk. It hashes every member and compares it with the manifest.

- A member whose bytes do not match is named, and the command exits `1`.
- A `reclaimed` or `unrecorded` member is a finding. The command still exits `0` and lists it.
- `--against-store` asks the engine for the run's live manifest, and reports any member that differs.

## What a gap means

A gap is an explicit entry in the bundle. It is never a silent omission.

- **Reclaimed:** the bytes were removed after the run finished. The bundle keeps the hash, so you can see what was there.
- **Unrecorded:** one try has no record. The bundle still lists the try number.
- **Event gap:** part of the run's event history was lost from object storage before the run was archived. The bundle names the missing event range, and `verify` prints it as a notice.

A bundle proves that the bytes match their hashes. It does not prove who made it. If you need that, sign the file with a tool you already use, such as `gpg --detach-sign`.

## See also

- [docs/RUN_EVIDENCE.md, "Layout"](https://github.com/orkstack/orkstack/blob/main/docs/RUN_EVIDENCE.md#layout): the members, their order and the fan-out layout
- [docs/RUN_EVIDENCE.md, "States"](https://github.com/orkstack/orkstack/blob/main/docs/RUN_EVIDENCE.md#states): the four member states
- [docs/RUN_EVIDENCE.md, "verify"](https://github.com/orkstack/orkstack/blob/main/docs/RUN_EVIDENCE.md#verify): every rule `verify` checks
- [docs/RUN_EVIDENCE.md, "verify --against-store"](https://github.com/orkstack/orkstack/blob/main/docs/RUN_EVIDENCE.md#verify---against-store): the live comparison
- [docs/RUN_EVIDENCE.md, "What a bundle is not"](https://github.com/orkstack/orkstack/blob/main/docs/RUN_EVIDENCE.md#what-a-bundle-is-not): the limits of the format
- [docs/CLI.md, "orkctl run evidence export"](https://github.com/orkstack/orkstack/blob/main/docs/CLI.md#orkctl-run-evidence-export): every export flag
