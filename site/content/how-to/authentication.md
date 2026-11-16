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
identity:
  signing:
    key_files:
      - /etc/orkstack/engine-signing.pem
```

Every caller then sends `Authorization: Bearer <token>`, a token your identity provider issued for the `orkstack` audience. The group in `admins` holds `admin` across the deployment. In `observe` and `enforce` each engine also needs a signing key in `identity.signing.key_files`, or it does not start.

## Let people sign in

Give the issuer a `login:` block, and the engine its public URL and a sign-in key:

```yaml
auth:
  public_url: https://orkstack.example.com
  signin:
    key_files:
      - /etc/orkstack/signin.key
  issuers:
    corp:
      # as above, and:
      login:
        client_id: orkstack-web
        client_secret:
          source: file:/etc/orkstack/web-client.secret
        cli_client_id: orkctl
        scopes:
          - openid
          - email
          - groups
```

Make the key once with `head -c 32 /dev/urandom > /etc/orkstack/signin.key`, and register `https://orkstack.example.com/v1/auth/callback/corp` as the web client's redirect URI. People then sign in to the web UI with the identity provider.

In a terminal, run `orkctl login`. It opens a browser, or, with `--device`, prints a code to enter on any device. `orkctl logout` signs you out again.

## Check who you are

Run `orkctl whoami`. It shows the principal the engine verified you as, the bindings that matched you, and what you may do at each scope.

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

A binding at a namespace or an Orkflow belongs to the one that existed when you saved it. If that namespace or Orkflow is deleted and created again under the same name, the binding no longer matches, and `orkctl admin binding list` shows it as stale. To bind the new one, run:

```bash
orkctl admin binding update NAME --repin
```

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
- [AUTHENTICATION.md, "Sign-in for the web UI"](https://github.com/orkstack/orkstack/blob/main/docs/AUTHENTICATION.md#sign-in-for-the-web-ui): the flow, the record, its limits and the sign-in key
- [AUTHENTICATION.md, "From orkctl"](https://github.com/orkstack/orkstack/blob/main/docs/AUTHENTICATION.md#from-orkctl): `orkctl login`, service accounts and the order credentials are tried in
- [AUTHENTICATION.md, "The first admin, and break-glass"](https://github.com/orkstack/orkstack/blob/main/docs/AUTHENTICATION.md#the-first-admin-and-break-glass): `auth.admins` and break-glass
- [ACCESS.md, "Built-in roles"](https://github.com/orkstack/orkstack/blob/main/docs/ACCESS.md#built-in-roles): every role's exact permissions
- [ACCESS.md, "The access document"](https://github.com/orkstack/orkstack/blob/main/docs/ACCESS.md#the-access-document): subjects, scopes and bounds
- [ACCESS.md, "Pinned bindings"](https://github.com/orkstack/orkstack/blob/main/docs/ACCESS.md#pinned-bindings): what a binding is pinned to, and pinning it again
- [ACCESS.md, "How a request is decided"](https://github.com/orkstack/orkstack/blob/main/docs/ACCESS.md#how-a-request-is-decided): what every RPC asks for
