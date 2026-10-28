# Pass secrets and environment variables

Give a task a secret as a file under `/orkstack/input/secrets/`, and plain settings with `env:`. A secret never goes into the Orkflow, and a value in `env:` is configuration, not a secret.

## Example

In [`examples/config-from-git.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/config-from-git.ork.yaml), `resolve` reads a Git token from the Orker's environment:

```yaml
  - id: resolve
    uses: git
    retry:
      maximum_attempts: 3
      initial_interval_seconds: 2
    secrets:
      - name: git-token
        source: env:GIT_TOKEN
    with:
      op: resolve
      url: '{{ .inputs.repo }}'
      ref: '{{ .inputs.ref }}'
      tokenSecret: git-token
```

Set `GIT_TOKEN` in the environment of every Orker that runs `resolve`. Do not put it in the Orkflow. Then apply and run:

```bash
orkctl apply examples/config-from-git.ork.yaml
orkctl run submit config-from-git --input repo=https://git.example.com/acme/app.git --input environment=prod --watch
```

`tokenSecret: git-token` names the secret whose file holds the token. The Orker reads `GIT_TOKEN` and writes the file, and the `git` activity reads that file.

## Name a secret

- `name` matches `^[A-Za-z0-9._-]{1,128}$`. It cannot be `.` or `..`.
- `source` is `env:NAME` to read a variable on the Orker, or `file:/path` to read a file on the Orker's host.
- The task reads the secret at `/orkstack/input/secrets/<name>`. The file is read-only, and it holds the exact bytes.
- The Orker resolves each secret when it starts the attempt. The value never appears in the Orkflow, the run's history, logs or `task.json`.

## Allow a source

A source must pass two lists. Each list matches `scheme:pattern`, and `*` matches within one path segment.

| List | Set by | Checked |
|---|---|---|
| The Orker's floor | `ORK_ORKER_SECRET_ALLOW` on each Orker. An empty value allows every `env` and `file` source. | When the Orker starts the attempt |
| The namespace's list | `orkctl namespace update <name> --allow-secret 'env:GIT_*'`. An empty list is unrestricted. | At apply and at every submit |

## Set environment variables

`env:` on a task sets variables for its container. The container gets the image's own variables plus these. OrkStack adds none.

```yaml
    env:
      TZ: Europe/Berlin
```

In [`examples/poll-until-ready.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/poll-until-ready.ork.yaml), this sets `TZ`, so that jq formats a local time.

- A value in `env:` is stored in the Orkflow. Never put a secret there.
- `env:` grants no credential, no privilege and no host access.
- A `with:` value can read a variable of the Orker with `{{ env "NAME" }}`. That works only when the Orker lists `NAME` in `ORK_ORKER_ENV_ALLOWLIST`.

## Test it locally

`orkctl task run --file <path> --task <id>` runs one task on your machine. Supply each declared secret with `--secret NAME=env:VAR` or `--secret NAME=file:PATH`. Or skip one with `--omit-secret NAME`.

## When a secret fails

| Code | Category | Meaning | Retried under `retry:` |
|---|---|---|---|
| `SECRET_NOT_PERMITTED` | ContractViolation | The source is outside the Orker's floor. | no |
| `SECRET_SCHEME_FORBIDDEN` | ContractViolation | The Orker does not read that kind of source. | no |
| `SECRET_UNRESOLVED` | InfrastructureError | The variable is unset, or the file cannot be read. | yes |

## Limits

- Do not cache a task that mounts a secret. A cache hit reads no secret. See [Reuse an earlier result](/docs/how-to/cache/).
- Do not copy a secret into `/orkstack/shared` or `/orkstack/output`. Those files stay on disk for the life of the attempt or the pin.

## See also

- [Security model, “Secrets”](https://github.com/orkstack/orkstack/blob/main/docs/SECURITY_MODEL.md#secrets): how values are resolved and kept out of records
- [Security model, “Which sources may be named”](https://github.com/orkstack/orkstack/blob/main/docs/SECURITY_MODEL.md#which-sources-may-be-named): the two lists in full
- [Security model, “Environment values are configuration”](https://github.com/orkstack/orkstack/blob/main/docs/SECURITY_MODEL.md#environment-values-are-configuration): what `env:` may and may not hold
- [DEPLOY, “Secrets on a shared pool”](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#secrets-on-a-shared-pool): operator settings
