# Turn on authentication

Make OrkStack check who calls its API, and decide what each caller may do with roles. It is off by default.

## Example

```yaml
auth:
  mode: observe
  issuers:
    corp:
      issuer: https://login.example.com/realms/acme
      kind: user
      audiences:
        - orkstack
      algorithms:
        - RS256
      groups_claim: groups
      max_lifetime: 1h
  admins:
    - group:corp:platform-admins
```

Every caller then sends `Authorization: Bearer <token>`, a token your identity provider issued for the `orkstack` audience. The group in `admins` holds `admin` across the deployment.

## Go through observe

1. Set `auth.mode: observe` on every engine. Requests with a token are verified, requests without one are still served, and nothing is refused.
2. Bind roles to your teams in the access document.
3. Read the audit trail. Every request a role would refuse is recorded as `would-deny`.
4. When those are the refusals you mean, set `auth.mode: enforce` on every engine, together.

## The roles

| Role | For |
|---|---|
| `viewer` | reading Orkflows, runs, schedules and stats |
| `operator` | running, cancelling, approving, reading logs |
| `developer` | writing Orkflows, schedules and triggers |
| `namespace-admin` | running a tenant's namespace |
| `auditor` | reading everything but logs and artifact bytes, and exporting evidence |
| `admin` | the deployment |

A role is bound to subjects (`user:`, `group:`, `claims:`) at a scope: the deployment, a namespace, one Orkflow, or a pool.

## Lower it again

```bash
orkctl admin auth status
orkctl admin auth lower observe
```

Then restart the engines with the lower mode. An engine refuses to start below the mode the deployment last ran, so a mistyped configuration cannot turn authentication off.

## When the identity provider is down

Set `auth.break_glass_key_file` to one public key and keep its private half offline. A request signed with it for that one request, with an `Ork-Reason` header, is `admin`, and every use is recorded and logged at error level.

## See also

- [AUTHENTICATION.md, "Modes"](https://github.com/orkstack/orkstack/blob/main/docs/AUTHENTICATION.md#modes): what each mode verifies and records
- [AUTHENTICATION.md, "The mode record"](https://github.com/orkstack/orkstack/blob/main/docs/AUTHENTICATION.md#the-mode-record): why an engine refuses to start below the deployment's mode, and the way back
- [AUTHENTICATION.md, "The first admin, and break-glass"](https://github.com/orkstack/orkstack/blob/main/docs/AUTHENTICATION.md#the-first-admin-and-break-glass): `auth.admins` and break-glass
- [ACCESS.md, "Built-in roles"](https://github.com/orkstack/orkstack/blob/main/docs/ACCESS.md#built-in-roles): every role's exact permissions
- [ACCESS.md, "The access document"](https://github.com/orkstack/orkstack/blob/main/docs/ACCESS.md#the-access-document): subjects, scopes and bounds
- [ACCESS.md, "How a request is decided"](https://github.com/orkstack/orkstack/blob/main/docs/ACCESS.md#how-a-request-is-decided): what every RPC asks for
