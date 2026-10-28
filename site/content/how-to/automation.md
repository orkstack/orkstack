# Manage schedules, triggers and webhooks in a file

Write one automation document for each namespace. It names the namespace's schedules, triggers and webhooks, and its settings. `orkctl apply` makes the namespace match the file.

## Example

This is [`examples/automation/examples.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/automation/examples.ork.yaml), with its comment header removed:

```yaml
apiVersion: orkstack/v1
kind: Automation
namespace: examples

settings:
  description: "The shipped example Orkflows (examples/README.md)"
  run_retention: 48h
  allowed_secrets:
    - "env:GIT_TOKEN"

schedules:
  poll-exports:
    orkflow: poll-until-ready
    description: "Fetch and summarize the export every 15 minutes, once it is ready"
    cron: "*/15 * * * *"
    timezone: UTC
    overlap: skip
    input:
      status_url: https://exports.example.com/status
      export_url: https://exports.example.com/latest.json

triggers:
  change-from-ci:
    orkflow: approval-gate
    description: "CI submits a change for assessment and, when risky, approval"
```

The schedule and the trigger name Orkflows. Each one must be stored, or applied in the same command. The [examples guide](https://github.com/orkstack/orkstack/blob/main/examples/README.md#the-automation-document) lists the steps in order.

Preview the plan, apply it, then check for drift:

```bash
orkctl apply examples/automation/examples.ork.yaml --dry-run
orkctl apply examples/automation/examples.ork.yaml
orkctl apply examples/automation/examples.ork.yaml --check
```

## Document keys

| Key | Required | Meaning |
|---|---|---|
| `apiVersion` | yes | `orkstack/v1`. |
| `kind` | yes | `Automation`. |
| `namespace` | yes | The namespace the document describes. An `-n` on the command line must match it. |
| `settings` | no | The namespace's settings. Each key you write changes that setting. |
| `schedules`, `triggers`, `webhooks` | no | Named entries, one for each object. |

A section you write, even as `{}`, is managed: the apply creates and changes its objects, and `--prune` deletes the ones the document no longer lists. A section you leave out is not touched.

Entry keys:

| Section | Keys |
|---|---|
| `schedules` | `orkflow` (required), `version`, `description`, `cron`, `interval`, `timezone`, `jitter`, `start_at`, `end_at`, `overlap`, `catchup_window`, `pause_on_failure`, `input` |
| `triggers` | `orkflow` (required), `version`, `description` |
| `webhooks` | `url` (required), `events`, `orkflow`, `task`, `secret_env` |

## Read the plan

`--dry-run` prints the plan and writes nothing. Each line starts with one symbol:

| Symbol | Meaning |
|---|---|
| `+` | The object will be created. |
| `~` | The object will change in place. |
| `-` | The object will be deleted. This happens only with `--prune`. |
| ` ` | The object already matches the document. |
| `?` | The object is not in the document. It is kept unless you pass `--prune`. |
| `!` | The object has no name, so no document manages it. It is never deleted. |

The plan has a digest, `sha256:…`. Pass it to `--expect-plan` to apply only the plan you reviewed.

## Apply

```bash
orkctl apply environments/prod/ --dry-run
orkctl apply environments/prod/
orkctl apply automation.ork.yaml --prune --expect-plan sha256:…
```

A directory is read for `*.ork.yaml` and `*.ork.yml` files. Orkflows are applied before automation. Only `--prune` deletes, and only named objects in the sections the document declares.

`orkctl apply --check` runs a dry run and exits `4` when anything would change. Run it on a schedule to find drift.

## Secrets are shown once

A trigger token and a generated webhook secret print once, with the trigger's `curl` command. To keep them out of the terminal, add `--secrets-out FILE`. It writes them to a new file with mode `0600`, and it refuses a file that already exists.

For a webhook, `secret_env` names an environment variable that holds the secret. The document never holds the value. An apply never changes the secret of an existing webhook.

## Bring an existing object under a document

Objects created by hand have no name, so no document manages them. Name them in place. Their tokens and secrets stay the same:

```bash
orkctl export -n payments > automation.ork.yaml
orkctl trigger update trg-… -n payments --name github-deploy
orkctl webhook update wh-… --name etl-loader
orkctl apply automation.ork.yaml --check
```

`export` prints the namespace as a document. It lists each unnamed object in a comment at the end, with the command that names it. A webhook must already be scoped to a namespace before it can take a name. After you name them, `apply --check` exits `0`.

## When an apply fails

- A document with a parse error is not planned. The error names the field, such as `schedules.poll-exports.cron`.
- A plan that changed since review is refused under `--expect-plan`. Nothing is written. Review the new plan, which the error prints.
- An entry name that points at a different Orkflow is refused. Use a new entry name, or delete the old object first.
- An apply that stops part way is not rolled back. Run the same apply again. It plans from what exists now.
- A `secret_env` variable that is not set stops the apply.

## Limits

- Apply does not set, release or change a namespace hold, a paused schedule or an Orker cordon.
- Apply never deletes an unnamed object or a deployment-wide webhook.
- There is no watcher and no rollback. Drift shows only when you run `--check`.

## See also

- [Declarative automation contract](https://github.com/orkstack/orkstack/blob/main/docs/AUTOMATION.md): the full document rules and the plan
- [The plan](https://github.com/orkstack/orkstack/blob/main/docs/AUTOMATION.md#the-plan): the symbols and the digest
- [`orkctl apply`](https://github.com/orkstack/orkstack/blob/main/docs/AUTOMATION.md#orkctl-apply): the flags and the order of an apply
- [Schedules](/docs/how-to/schedules/), [Triggers](/docs/how-to/triggers/) and [Webhooks](/docs/how-to/webhooks/): the objects a document declares
