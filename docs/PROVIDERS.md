# Credential and secret providers

A **provider** is where a task's credentials and secrets come from when they
are not in the Orker's environment or on its disk: a cloud's token service, a
secret store. A provider logs in **as the attempt**, with the attempt's own
identity token ([TASK_IDENTITY.md](./TASK_IDENTITY.md)), reads a secret or
issues a credential, and the Orker writes what it got as files. An activity
reads files; it needs no SDK and no network path to the provider.

**Status.** This release has the `providers` setting, its rules, the refresh
rules below and two provider types, `aws` ("The `aws` type") and `vault`
("The `vault` type"): a deployment can declare either, and its declaration is
checked at boot. A document that declares another type refuses boot, naming
it. A task's `credentials:` and the provider sources of `secrets:` come in
later releases; until then nothing here changes what a task sees.

## The rule that makes it safe

A provider is never called with the Orker's own credentials. Its only way to
authenticate is the attempt's identity token for the provider's audience. An
Orker host therefore needs no role that can read anybody's secrets, and
cannot be made to read one namespace's secret for another's task.

## Declaring providers

`providers` (`ORK_PROVIDERS`) is one YAML document, set **identically on
`ork` and `orker`**, as `telemetry.sinks` is: the engine checks references
against it, the Orker does the work.

```yaml
providers:
  aws:
    type: aws
    region: eu-west-1
    audience: sts.amazonaws.com
  vault:
    type: vault
    address: https://vault.internal:8200
    audience: vault.internal
```

Each type's own keys are in its section below. Rules every
declaration follows, checked at boot on both binaries:

| Rule | |
|---|---|
| Name | the key under `providers:`; it is the scheme of the provider's sources (`vault:secret/data/…`), so it matches `^[a-z][a-z0-9]*$`, at most 16 bytes. `env` and `file` are taken |
| Count | at most 16 providers |
| `type` | required; one of the types this build has |
| Keys | only the keys the type reads; any other, a misspelt one included, refuses boot |
| Values | read strictly: a string is a string (`123`, `true` and `null` are refused where a string is meant), and anchors, aliases, merge keys and tags are refused |
| No secret | the document holds none. A key whose name holds `secret`, `password`, `token`, `private`, `credential` or `api_key` refuses boot |
| Files | a file a provider names (a CA bundle) is an absolute, clean path, and joins the Orker's own paths, which no secret floor admits |

The document is at most 64 KiB.

## References

A task names a provider through a source `<provider>:<locator>` or a
credential's `path`. A **locator is validated, never normalized**: it holds
only letters, digits and `. _ / - : @ + =`, at most one `#` before a field,
at most 1024 bytes, and no empty, `.` or `..` segment. There is no `%`,
`?` or space. A pattern in a namespace's `allowed_secrets` can therefore not
be escaped by a locator that says one thing and reads another.

Every reference has one **policy string**, which OrkStack computes from the
reference and the grant; a provider has no say in it:

| Reference | Policy string |
|---|---|
| a secret, or a credential with a `path` | `<provider>:<locator>`, without its `#<field>` |
| a credential with no `path`, which is the login itself | `<provider>:<scheme>:<principal>`, where the scheme is the type's one word for a login |

A locator that spells a login (it begins with the type's scheme and `:`) is
refused, so the two never share a string. `allowed_secrets` and the Orker's
`ORK_ORKER_SECRET_ALLOW` judge it, with the grammar they use for every source
([DEPLOY.md](./DEPLOY.md#secrets-on-a-shared-pool), "Secrets on a shared
pool"). Three things follow:

- `*` stops only at `/`, and `:` is an ordinary character, so `vault:*` also
  admits a login as a principal that holds no `/`. A grant still bounds which
  principal a task may use; write `vault:secret/…` to admit secrets alone.
- A list entry is at most 256 bytes, so a longer policy string is admitted
  only by an entry with `*`.
- A principal follows the locator's rule. An IAM role whose name holds `,`
  cannot be used.

A credential's `params` (for a path a provider writes rather than reads) are
literal strings, never templated: at most 16 keys of
`^[a-z][a-z0-9_]{0,62}$`, each value at most 1024 bytes of UTF-8 with no
control character, 4 KiB in all, and only the keys that path's kind
accepts.

## Grants

A task reaches a provider only through a grant of the namespace its run
executes in: a name for one principal at one provider, with the Orkflows that
may use it, the kinds of start it serves and whether only a current version
may ([TASK_IDENTITY.md](./TASK_IDENTITY.md#a-namespaces-identity-policy),
"A namespace's identity policy"). Every credential, and every secret read
from a provider, is judged when the Orkflow is created and at every submit,
for every Orkflow the run composes too:

| Asked | Refused |
|---|---|
| the namespace has the grant | `GRANT_NOT_DEFINED` |
| the deployment declares the grant's provider, and the source's | `PROVIDER_UNKNOWN` |
| a secret's source names the grant's own provider | `GRANT_NOT_PERMITTED` |
| the grant lists the Orkflow: its name or `*`; `<owner>/<name>` alone for an Orkflow another namespace shared | `GRANT_NOT_PERMITTED` |
| the grant serves the kind of start of the run tree's root; asked at submit | `GRANT_NOT_PERMITTED` |
| with `current_only`, the Orkflow's owner names this version current; asked at submit | `GRANT_NOT_PERMITTED` |
| the provider accepts the reference | `PROVIDER_REQUEST_INVALID` |
| `allowed_secrets` admits the reference's policy string | `SECRET_SOURCE_NOT_ALLOWED` |
| every uncordoned Orker of the pool the task runs on declares the provider, as the same type; asked at submit | `POOL_NO_PROVIDER` |

A resumed run keeps the plan and the admission it has, and no Orker is asked
again: an Orker that no longer declares the provider refuses the task at
dispatch, `PROVIDER_NOT_CONFIGURED`.

A run's admission records, for each run of its tree, the grants its tasks use
with the provider, principal and audience each selects, and which task uses
which.

## What a credential looks like in the container

```text
/orkstack/credentials/<name>/      read-only
├── <file> -> current/<file>       one link per file of the credential
├── expiration -> current/expiration   RFC 3339; absent when it does not expire
├── current -> .v3                 replaced by one rename(2)
└── .v3/                           the files, 0444
```

A refresh writes every file into a new directory and moves `current` with
one rename, so the set on disk is always whole: the old one or the new one.
The previous directory stays until the refresh after it, for a process that
opened a file of it. A process that reads two files at two moments can still
straddle a swap: read the one file of a credential that holds everything
(`process.json`, `secret.json`) where its type writes one.

## Refresh

A credential with a lifetime is refreshed at a random point between a third
of its life and a quarter later, never sooner, so the items of a fan-out do
not refresh together. A refresh starts from the attempt's identity, never
from the credential it replaces, with one exception: where a provider can
extend a login or a lease in place, it is extended until the provider will
not, and only then replaced. An extension asks OrkStack nothing, so a login
never outlives 24 hours, however often it was extended, and an event
source's never outlives `identity.token.instance_max_ttl`: it is replaced
from a fresh token, its credentials are issued again, and a grant taken away
is seen. **The task runs on**: nothing stops or restarts its container. It
reads its files again when it needs them, as at any refresh: each refresh
swaps the whole set with one rename, and a program that keeps a value it read
once holds it only until that value expires. What the replaced login issued
is not revoked early from a task; it ends at its own expiry, at its login's,
or with the attempt.

What a refresh obtained is masked in the task's logs **before** its files are
put in place, and so is each value inside a JSON, INI or `.env` file, up to
32 of a file. A value it replaced stays masked until it expires plus a
five-minute grace, because a process may hold and print it until then.

A credential's `refresh:` decides what happens when a refresh fails:

| What happened | `required` (the default) | `best_effort` |
|---|---|---|
| The provider did not answer, threw an error or throttled | retried, 5 to 60 s apart, until expiry; at expiry the container is stopped and the attempt fails `CREDENTIAL_REFRESH_FAILED`, retried | at expiry one `[ork]` line; the files stay and the loop keeps trying |
| The provider refused, three times over 30 s | the container is stopped now, with the refusal's reason (`CREDENTIAL_REFUSED`), not retried | one `[ork]` line; no further refresh |
| The answer cannot be put in place, or the masker cannot take its values | no further refresh; the task keeps what it holds; at expiry `CREDENTIAL_REFRESH_FAILED`, naming the cause | one `[ork]` line; the files stay |
| A file could not be written | the answer is kept and its write retried for up to 60 s, without asking the provider again; after that a fresh one is obtained | the same |

A refusal is believed only after three identical answers over 30 seconds,
each with a fresh token. That absorbs a clock difference and most of the
time a changed trust policy takes to apply, but not all of it: create a role
before the runs that use it.

## Failure reasons

| Reason | Category | When |
|---|---|---|
| `PROVIDER_UNAVAILABLE` | InfrastructureError, retried | the provider did not answer within the setup budget |
| `CREDENTIAL_REFRESH_FAILED` | InfrastructureError, retried | a credential expired because it could not be refreshed |
| `CREDENTIAL_REFUSED` | ContractViolation | the provider refused the login or the credential |
| `SECRET_ACCESS_DENIED` | ContractViolation | the provider refused a secret |
| `SECRET_NOT_FOUND` | ContractViolation | a secret or a path is not there |
| `PROVIDER_REQUEST_INVALID` | ContractViolation | the provider called the request malformed |
| `SECRET_TOO_LARGE` | ContractViolation | a value over 1 MiB |
| `MASK_CAPACITY_EXCEEDED` | ContractViolation | the task's values do not fit the log masker's bound |
| `PROVIDER_NOT_CONFIGURED` | InfrastructureError, not retried | this Orker does not declare the provider |

A message names the secret or credential, the provider and the provider's
error code; never the provider's message, a value, a token or a principal.
[REASONS.md](./REASONS.md) lists each with what to do.

## The `aws` type

| Key | Default | |
|---|---|---|
| `region` | required | the region of STS and of every read |
| `audience` | `sts.amazonaws.com` | the audience the identity token carries; register it as a client id of the account's OIDC identity provider |
| `session_seconds` | `3600` | how long a role's session lasts, 900 to 43200; a role whose maximum is lower refuses it |
| `sts_endpoint`, `endpoint` | AWS's own | the address of STS, and of Parameter Store, Secrets Manager and KMS; `https`, or `http` at a loopback address |
| `session_tags` | `true` | whether identity tokens for this provider's audience carry session tags ([TASK_IDENTITY.md](./TASK_IDENTITY.md#aws-session-tags), "AWS session tags"). Two providers that share an audience must agree, or the engine refuses to boot |
| `transitive_tags` | none | which of those tags follow the session through role chaining; a key outside the set refuses boot |

A grant's `principal` is a role's ARN. A login is one
`AssumeRoleWithWebIdentity` with the attempt's identity token, unsigned; every
other call is signed with the session it returned. Of the Orker's
own environment only the proxy settings (`HTTPS_PROXY`, `NO_PROXY`) are
followed: no `AWS_*` variable, profile, instance metadata or container
endpoint is read. A proxy that inspects TLS, with a CA the host trusts, reads
the attempt's token and what AWS answers: name AWS's hosts in `NO_PROXY`. A
redirect is never followed.
The session is named `<run id>-<attempt>`, which is what CloudTrail shows, or
`ork-` and 40 hexadecimal digits where that is not a name STS takes. To work
in several accounts, register the issuer in each and grant a role there.

### A credential

A `credentials:` entry with an `aws` grant takes no `path` and no `params`. It
is the role's session:

| File | |
|---|---|
| `config` | an AWS config file: the region, and a `credential_process` that prints `process.json` |
| `process.json` | the session's keys and expiry, as `credential_process` answers them |
| `credentials` | an AWS shared credentials file, profile `[default]` |
| `expiration` | when the session ends |

OrkStack sets no variable; the task's own `env:` names a file:

| The task sets | Fits |
|---|---|
| `AWS_CONFIG_FILE=/orkstack/credentials/<name>/config` (recommended) | any process, a long-lived one included: an SDK runs `cat` again before the keys expire. The image needs `cat` |
| `AWS_SHARED_CREDENTIALS_FILE=/orkstack/credentials/<name>/credentials` | the AWS CLI and short processes. Most SDKs read it once, so a long-lived client keeps expired keys |

AWS has no way to end a session early: what a task copied out works until it
expires.

The secret key and the session token may not be in a task's `values.json`; the
access key id, which is an identifier, may. A refresh is a new exchange with a
fresh identity token, one a cycle.

### A secret

| Source | Read with | Value |
|---|---|---|
| `aws:ssm:<name>` | `GetParameter`, decrypted | the parameter. The name may end in `:<version or label>`, or be an ARN |
| `aws:secretsmanager:<id>` | `GetSecretValue` | the secret's string or bytes |
| `aws:secretsmanager:<id>#<key>` | the same | that key of a JSON object: a string's own text, or a number or boolean as written. A key written twice, a nested value or a secret that is no object is `PROVIDER_REQUEST_INVALID` |
| `aws:kms:<key>` with `ciphertext:` | `Decrypt` with that key and the encryption context `ork:namespace=<namespace>` | the plaintext |

One secret has several spellings (a name and its ARN, a key and its alias, a
version and no version), and `allowed_secrets` judges the one the task
writes: list the spellings you mean to admit, and name the account where a
pattern admits ARNs. An ARN is of the provider's own region. A `ciphertext` is at most 6144
bytes, and KMS refuses one encrypted under another key or for another
namespace:

```bash
aws kms encrypt --key-id alias/payments --encryption-context ork:namespace=payments --plaintext fileb://value --output text --query CiphertextBlob
```

### How AWS's answers are read

| Answer | Read as |
|---|---|
| STS: `AccessDenied`, `IDPRejectedClaim`, `ValidationError`, `MalformedPolicyDocument`, `PackedPolicyTooLarge`, `RegionDisabledException` | refused |
| a read: `AccessDeniedException`, `IncorrectKeyException`, `InvalidCiphertextException`, `DisabledException`, `KMSInvalidStateException` | refused |
| a read: `ParameterNotFound`, `ParameterVersionNotFound`, `ResourceNotFoundException`, `NotFoundException`, `InvalidRequestException` (a secret marked for deletion, or otherwise not in a state to be read), a JSON key that is not there | not found |
| a read: `ValidationException`, `InvalidParameterException`, `InvalidKeyUsageException` | malformed: `PROVIDER_REQUEST_INVALID` |
| any 5xx, no answer, an answer that is not AWS's own (`UnreadableAnswer`: look for a proxy), throttling, and every other code | unavailable: retried, and loud |

`AccessDenied` at a login is first of all a trust policy that lacks
`sts:TagSession`, which a role needs as soon as tokens carry session tags: add
the action, or set `session_tags: false`.

`InvalidIdentityToken` is unavailable on purpose: STS answers it while it
holds an old copy of the issuer's keys. It also answers it for an audience the
account's OIDC provider does not list, which waiting does not mend: look there
when `PROVIDER_UNAVAILABLE` repeats with that code.

## The `vault` type

HashiCorp Vault, over its HTTP API. The Orker logs in at a JWT auth mount as
the role the grant's principal names, with the attempt's identity token.

```yaml
providers:
  vault:
    type: vault
    address: https://vault.internal:8200
    audience: vault.internal
    ca_file: /etc/orkstack/vault-ca.pem
    auth_mount: jwt-orkstack
    kv2_mounts:
      - secret
```

| Key | Default | |
|---|---|---|
| `address` | required | an `https` origin, with no path. Certificates are always verified |
| `audience` | required | the identity token's audience, which the role's `bound_audiences` must hold |
| `ca_file` | the system's roots | a PEM file on every Orker of the pool, read at the first login |
| `auth_mount` | `jwt` | the JWT auth mount |
| `vault_namespace` | *(none)* | the Vault Enterprise namespace of every call |
| `max_login_ttl` | `1h` | the longest login accepted, from 1m to 24h |
| `kv2_mounts` | *(none)* | the mounts that are KV version 2. It is configuration, never guessed from an answer |

Of the Orker's own environment only the proxy settings (`HTTPS_PROXY` and
`NO_PROXY`, in either case) are followed; no `VAULT_*` variable is read. Name
an internal Vault in `NO_PROXY`. A proxy that inspects TLS, with a CA the host
trusts, reads the attempt's token and what Vault answers: set `ca_file`, which
such a proxy's certificate fails, or `NO_PROXY`. Without `ca_file` the host's
own roots are used, as Go finds them (`SSL_CERT_FILE` and `SSL_CERT_DIR`
included). It follows no redirect, which would send a login
token where the answer says: give it the active node's address, or a load
balancer's.

**What a role must be.** A role is a Vault role of type `jwt` on that mount
that trusts OrkStack's issuer and binds its audience and the subject
([TASK_IDENTITY.md](./TASK_IDENTITY.md)). Its tokens need Vault's `default`
policy, or the same paths of it: `sys/capabilities-self`,
`auth/token/lookup-self`, `auth/token/renew-self`, `auth/token/revoke-self`
and `sys/leases/renew`. A login is refused (`CREDENTIAL_REFUSED`; for an event
source's instance it is retried instead and never a pause) as follows. The
token it was given is revoked, where it can be; one that cannot is logged by
its accessor and lives to its TTL, one for each try:

| The code in the message | What to change on the role |
|---|---|
| `login-ttl-768h0m0s-over-1h0m0s` | set `token_ttl`, or `token_period`, to at most `max_login_ttl`: a role with neither gets Vault's own default, and an Orker that is killed leaves the login to live that long |
| `login-never-expires`, `login-num-uses` | give its tokens a TTL, and no number of uses |
| `batch-token` | `token_type` service: a batch token cannot be revoked, now or when the attempt ends |
| `login-cannot-be-revoked` | let its tokens read `auth/token/lookup-self` and update `auth/token/revoke-self`, as Vault's `default` policy does |
| `vault-token-unbounded` | for an event source's instance: no `token_period`, and a `token_explicit_max_ttl` of at most `identity.token.instance_max_ttl`; `max_login_ttl` holds beside it |
| `instance-login-unbounded` | for an instance: `identity.token.instance_max_ttl` is not set |
| `400`, `403` | Vault itself refused the role, the token's claims or the request |

A login is renewed at a third of its life for as long as Vault extends it, and
then made again. When the attempt ends its token is revoked, and with it every
lease it made. If the Orker dies instead, the token ends by itself within
`max_login_ttl`. A token that could not be revoked is logged at error by its
accessor, never by itself, so an operator can revoke it.

**Secrets.** `vault:<API path>` reads the path, and `#<field>` takes one field
of it: `vault:secret/data/payments/db#password` under a KV version 2 mount,
`vault:kv/payments/db#password` under another. A string is its text; a number,
a boolean or an object is its JSON. With no `#<field>` the value is the whole
object as JSON, and each value in it is masked too. A path under a
`kv2_mounts` mount is its data path, `<mount>/data/<name>`; any other is
refused when the Orkflow is created.

**Credentials.**

| `path` | Files |
|---|---|
| none: the login itself | `token`, and `address`, which is not masked |
| a secrets engine's path that answers with a lease, such as `database/creds/app` | `secret.json`, the whole of it, and one file for each value whose name can be a file's (`username`, `password`) |

A task may not return the login's token, or a field named `password`,
`token`, `secret_key`, `security_token`, `private_key`, `secret_id` or
`client_secret`, in its `values.json`. Any other value of a credential is
masked in logs and may be returned.

A lease is renewed at a third of its life. When Vault will not extend it (its
role's `max_ttl`), a new one is read: **a new username and password**. A
process that opened a connection with the old one keeps it until Vault revokes
it, and must read the files again when it reconnects. A path that answers with
no lease (`no-lease`), and one with more than 15 values (`too-many-fields`),
fail `PROVIDER_REQUEST_INVALID`. A login handed over as a credential lasts at
least 45 s: the 30 s a credential must have left, and a margin of 15 s, one
call's time, for the calls that make it. A role whose logins are shorter is
refused, and no login is made again for it
(`login-ttl-32s-under-45s`).

| Vault answers | Classified |
|---|---|
| 429, 412, 472, 473, any 5xx, a redirect, a code not listed here | unavailable: retried. A 404 at login, which is a wrong `auth_mount`, is one, and is logged at error |
| 400 or 403 to a login or a credential, 403 to a read | refused |
| 404 to a secret or a credential, or a field that is not there | `SECRET_NOT_FOUND` |

## Adding a provider type

A type is compiled in, selected by `type:`, from a set fixed when the binary
is built: there is no plugin, no command run on the host and no discovery.
To add one: implement it under `internal/provider/<type>/`, add one line to
`internal/provider/types/types.go`, give it a `LoginScheme`, name in each bundle's `Secrets` what is certainly secret, give its `Parse` a fuzz target, and pass
`providertest.Conformance` against the real system it talks to (for
example LocalStack for AWS, a Vault dev server for Vault).
