# Audit

Read the record of every control-plane write: what changed, who the caller says they were, and whether the change succeeded. The record is a trail for people to read, and OrkStack never reads it to decide anything.

## Example

```bash
orkctl admin audit                                              # the last seven days, newest first
orkctl admin audit --filter 'action^orker.'                     # Orker cordons and uncordons
orkctl admin audit --filter 'namespace=payments' --json         # one namespace, as JSON
```

## What is recorded

Each write call leaves one record, whether it succeeded or was refused. The records cover:

- namespace, Orkflow, trigger, webhook and schedule changes;
- retention and image policy changes;
- releases (ramp, promote, rollback);
- cache purges, run cancels and await resolutions;
- Orker cordons and uncordons.

Reads are not recorded. Run submissions, trigger invocations and await callbacks are not recorded either, because the run itself is the record of those. A deployment-wide write, such as a retention change or an Orker cordon, has no namespace. To list only those, filter with `namespace=`.

## Read it

Each row shows when, who, what, which namespace and target, and the result.

| Filter | Example |
|---|---|
| `at>=` and `at<` | `--filter 'at>=2026-10-01T00:00:00Z'` |
| `action` | `--filter 'action^retention.'` |
| `principal` | `--filter 'principal=svc-platform-ci'` |
| `namespace` | `--filter 'namespace=payments'` |
| `succeeded` | `--filter 'succeeded=false'` |
| `error_code` | The error code of a refused write |

- The window defaults to the seven days before now. Set `at>=` and `at<` to choose another.
- A page holds 50 records by default, and 200 at most. Use `--page-token` for the next page.
- A window that would scan more than 5000 records is refused. Narrow the window and page through it.

## The principal is asserted

The principal is the `Ork-Principal` header, copied as the caller sent it. OrkStack does not authenticate it, so it is an assertion, not a verified identity. A request with no header shows as `unattributed`.

- A gateway that authenticates callers must **overwrite** `Ork-Principal` with the identity it established. A gateway that passes a client's value through lets any caller claim any name.
- `ORK_PRINCIPAL` sets the header from `orkctl`. It is useful on a deployment with no gateway. It proves nothing about who typed the command.
- A run canceled through the API records the same assertion as `canceled_by`.
- A record never includes a secret. A webhook signing secret and a trigger token are never written.

## Keep it as long as you need

Nothing reclaims the audit trail, and OrkStack has no setting for it. Keep it with a bucket lifecycle rule on `metadata/audit/`, or with the bucket's own object lock or versioning.

The write is best-effort. If the audit store refuses a record, the write still succeeds, and the engine logs `audit record dropped` at warn level. Check that log if you rely on the trail.

## See also

- [DEPLOY.md, "Attributing control-plane writes"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#attributing-control-plane-writes): what is recorded, the principal, and the listing bounds
- [SECURITY_MODEL.md, "The audit trail"](https://github.com/orkstack/orkstack/blob/main/docs/SECURITY_MODEL.md#the-audit-trail): where the trail is kept
- [SECURITY_MODEL.md, "The principal is asserted, not authenticated"](https://github.com/orkstack/orkstack/blob/main/docs/SECURITY_MODEL.md#the-principal-is-asserted-not-authenticated): what the principal means
- [UI_CLI_PARITY.md, "Control-plane audit trail"](https://github.com/orkstack/orkstack/blob/main/docs/UI_CLI_PARITY.md#control-plane-audit-trail): the recorded actions, and the same trail in the web UI
- [CLI.md, "orkctl admin audit"](https://github.com/orkstack/orkstack/blob/main/docs/CLI.md#orkctl-admin-audit): every filter and flag
