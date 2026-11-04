# Object storage

Every `ork` and every `orker` in a deployment shares one S3-compatible bucket. It holds artifacts, Orkflow definitions, task logs, finished runs and the audit trail.

## Example

Set the same storage values on both binaries. Read the credentials from files, not from the document:

```yaml
storage:
  backend: s3
  s3:
    endpoint: https://s3.internal.example
    region: us-east-1
    bucket: orkstack-artifacts
    access_key:
      file: /run/secrets/s3_access_key
    secret_key:
      file: /run/secrets/s3_secret_key
    path_style: false
```

Then check the file on each binary:

```bash
ork --config orkstack.yaml --check
orker --config orkstack.yaml --check
```

## Why it must be shared

The default backend is `local`, a directory on each host. With `local`, each replica archives runs to its own disk, and an artifact written by one Orker cannot be read by another. Set `storage.backend` to `s3` on both binaries for any replicated deployment.

The engine reads Orkflow definitions from the bucket whatever the backend is, so `ork` always needs the `storage.s3` values.

## What the store must support

- **Conditional writes.** Commits, Orkflow pointers and registry documents are arbitrated by `If-None-Match` and `If-Match`. Every Orker probes the store at boot and refuses to start on a store that lacks them.
- **Bulk delete.** Cleanup empties each run's prefixes with `DeleteObjects`, in batches of up to 1000 keys.
- **Path style.** `storage.s3.path_style` defaults to `true`, which the development store needs. Set it to `false` for AWS S3.

The local development stack uses SeaweedFS, with path style turned on.

## What is kept where

| Prefix | Contents |
|---|---|
| `orkflows/`, `objects/` | Artifact bytes. The run archive keeps references to them. |
| `metadata/logs/` | Task output of runs |
| `metadata/attempts/` | One record per task attempt |
| `metadata/runs/`, `metadata/index/` | The archive of finished runs |
| `metadata/audit/` | The control-plane audit trail |
| `metadata/memo/` | Cached-task memos |
| `metadata/settings/`, `metadata/namespaces/`, `metadata/orkflows/` | Deployment settings, namespaces and definitions |

Expiry rules for the prefixes that are safe to shorten are on the [Retention](/docs/operate/retention/) page. Do not expire the settings, namespace, definition, snapshot or commit prefixes.

## Credentials

The configuration's defaults are the development store's credentials, which the repository contains. Supply your own access key and secret key. Keep them in files, or in the environment, which overrides the file.

## See also

- [DEPLOY.md, "The bucket stops being optional at the second replica"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#the-bucket-stops-being-optional-at-the-second-replica): the shared bucket, and the settings on both binaries
- [DEPLOY.md, "Retention cleanup and bucket lifecycle rules"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#retention-cleanup-and-bucket-lifecycle-rules): lifecycle rules, and the prefixes not to expire
- [STORAGE.md](https://github.com/orkstack/orkstack/blob/main/docs/STORAGE.md): what OrkStack persists, and the key layout
- [STORAGE.md, "Backends"](https://github.com/orkstack/orkstack/blob/main/docs/STORAGE.md#backends): the two backends
- [PRODUCTION_CHECKLIST.md, "Object storage"](https://github.com/orkstack/orkstack/blob/main/docs/PRODUCTION_CHECKLIST.md#object-storage): the items to check before a deployment
- Settings: [Storage settings](/docs/configuration/storage/)
