# Storage settings

These settings choose where artifacts, the run archive and task logs are kept, and how `ork` and `orker` reach the S3-compatible bucket. Set them the same on every `ork` and every `orker`.

## Decide these first

| Setting | Why a deployment must look at it |
|---|---|
| [`storage.backend`](/docs/configuration/#storage-backend) | Set `s3` for any replicated deployment. The default `local` is one directory per process. |
| [`storage.s3.endpoint`](/docs/configuration/#storage-s3-endpoint) | The S3-compatible endpoint. `ork` needs it whatever the backend is, because it reads definitions from the bucket. |
| [`storage.s3.bucket`](/docs/configuration/#storage-s3-bucket) | The one bucket that holds definitions, artifacts, archived runs and task logs. |
| [`storage.s3.secret_key`](/docs/configuration/#storage-s3-secret-key) | Set with `access_key`, or leave both unset to use an instance or pod role. A `file:` mapping keeps it out of the document. |
| [`storage.s3.path_style`](/docs/configuration/#storage-s3-path-style) | Keep `true` for the development store. Set `false` for AWS S3. |

## Every storage setting

<!-- site-generated: settings-table-storage -->

## See also

- [DEPLOY.md, "1. Configuration"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#1-configuration): the full table, each key's variable and default
- [DEPLOY.md, "The bucket stops being optional at the second replica"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#the-bucket-stops-being-optional-at-the-second-replica): why the backend must be `s3` on a replicated deployment
- [DEPLOY.md, "Retention cleanup and bucket lifecycle rules"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#retention-cleanup-and-bucket-lifecycle-rules): lifecycle rules for the bucket
- [STORAGE.md, "Backends"](https://github.com/orkstack/orkstack/blob/main/docs/STORAGE.md#backends): the two backends
- Object storage guide: [Object storage](/docs/deploy/object-storage/)
- Setting details: [Setting details](/docs/configuration/details/)
- All settings: [All settings](/docs/configuration/)
