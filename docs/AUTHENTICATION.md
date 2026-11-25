# Authentication

OrkStack can authenticate the callers of its API itself, and decide what
each may do ([ACCESS.md](./ACCESS.md)). It is off by default: an engine in
`asserted` mode behaves exactly as v0.3.0 did, and a deployment's gateway
names the caller in the `Ork-Principal` header.

## Modes

`auth.mode` (`ORK_AUTH_MODE`), on `ork`:

| Mode | Authentication | Authorization | The principal on records |
|---|---|---|---|
| `asserted` (default) | none | none | the `Ork-Principal` header, recorded as sent and labelled asserted |
| `observe` | a credential that is presented is verified; a request with none is still served | computed and recorded as `would-deny`, never enforced | verified when a credential was presented, else none |
| `enforce` | required | enforced | verified |

In `asserted` mode no middleware is installed and no new route is mounted.
In `observe` and `enforce` mode the `Ork-Principal` header is read nowhere:
every record takes the principal from the verified credential, or records
none. A deployment that moves from a gateway's header to `observe` therefore
loses the gateway's name for every caller that sends no token yet. The trigger invoke and the await callback keep their own bearer
tokens in every mode; outside `asserted` mode they record no caller-chosen
name. A client learns the mode from `GetSystemInfo`.

The access document and the mode record are the exception: their service
(`AccessService`) is enforced in `observe` mode too, so nobody reads who
holds what, or lowers the mode, without `access.read` or `access.write`.

Every engine of a deployment runs the same mode. `observe` is the step
between: turn it on everywhere, read the audit trail's `would-deny`
records (`orkctl admin audit -j`) until they are the refusals you mean,
then set `enforce` everywhere.

An engine in `observe` or `enforce` must hold a signing key
(`identity.signing.key_files`), also with no `identity.issuer`, or it does
not start: in those modes a record can say "verified", and the engines sign
what they hand each other so that it can be checked later
([TASK_IDENTITY.md](./TASK_IDENTITY.md#signing-keys-with-no-issuer)).

## What the API accepts

One middleware wraps the API listener. It classifies each request by the
route pattern that serves it, never by its path, and authenticates every
route except:

| Route | Why |
|---|---|
| the web UI's static files (`/`) | the page must load to sign in; it holds no data |
| `POST /v1/triggers/{id}` | the trigger's own bearer token is the capability |
| `POST /v1/runs/{run_id}/awaits/{task_id}[/reject]` | the await callback's own token is the capability |
| `AuthService.GetAuthConfig` | public: the issuers a person can sign in with |
| `GET /v1/auth/login/{issuer}`, `GET /v1/auth/callback/{issuer}` | the browser's sign-in redirects: the caller has no credential yet |

A request carries one credential: `Authorization: Bearer`, or, with no
bearer, the web UI's sign-in cookie. A credential that is presented and
refused is refused (`401`), never passed on to another method.

- **A token from a trusted issuer.** `auth.issuers` (`ORK_AUTH_ISSUERS`) is
  one YAML document of the issuers whose JWTs the engine accepts: a person's
  identity provider, or a CI or cluster issuer for services. Every claim rule
  is equality; a service issuer must name `required_claims` or say
  `dedicated: true`. The engine checks the signature against the issuer's
  published keys, the audience, the expiry and the issuer's maximum
  lifetime, with `auth.clock_skew` (1 minute) of leeway.
- **A service account's assertion.** A service account of the access
  document (docs/ACCESS.md) holds up to four public keys and no secret. Its
  caller signs, for each request, a JWT whose `iss` and `sub` are
  `serviceaccount:<name>`, `aud` is `auth.public_url`, `exp` at most 60
  seconds ahead, `typ` `ork-request+jwt`, and which is bound to that one
  request: its method (`htm`), its URL without query (`htu`, whose host the
  listener must accept and whose scheme and host are `auth.public_url`'s),
  the SHA-256 of its raw query (`ork_query_sha256`, of the empty string for
  none), the SHA-256 of its body (`ork_body_sha256`, the body read whole, at
  most 1 MiB, within 10 seconds) and a fresh `jti`. A replica refuses a `jti` it has seen, for the
  assertion's life. An assertion that is not bound is refused. The
  identical request can be played once more at each other replica within
  its minute; it cannot be turned into another call. A caller for whom that
  repeat matters uses an idempotency key. An engine with no
  `auth.public_url` accepts no service account.
- **A sign-in** to the web UI (below).
- **Break-glass**, for an identity provider that is down
  (below).

A person is `user:<issuer>:<username>`, a service of a trusted issuer
`service:<issuer>:<sub>`. Groups come from the issuer's `groups_claim` (at
most 200), and only the claims the issuer's `claims` list names are kept.

## From orkctl

`--token-file FILE` (`ORK_TOKEN_FILE`) presents the file's one token as
`Authorization: Bearer`, read again at every request, so a token something
else rotates (a CI job's GitHub or Kubernetes token) is sent as it is now. It
is presented only to an `https://` server or a loopback `http://` one
(`localhost`, or a loopback address; a name under `localhost` is not one),
and refused for any other: it would cross the network in the clear.
`--ca-file` sets the roots an `https://` server is verified against, in
place of the system's, and `--client-cert` with `--client-key` presents a
client certificate; a client certificate takes precedence over a token
file. This release's engine authenticates bearer tokens, sign-ins, and
service-account and break-glass assertions; it does not yet take a
principal from a client certificate.

`--service-account NAME` with `--service-account-key FILE`
(`ORK_SERVICE_ACCOUNT`, `ORK_SERVICE_ACCOUNT_KEY`; the private key in PEM,
PKCS #8, SEC 1 or PKCS #1) signs one assertion per request, bound to it;
with `--service-account break-glass` it needs `--break-glass-reason`
(`ORK_BREAK_GLASS_REASON`), sent as `Ork-Reason`. `orkctl auth assertion
--method M --url U [--body-file F]` prints one assertion for a request
another tool will make: the same method, URL and body, sent within a minute.

`orkctl login [--issuer NAME] [--device]` signs a person in with one of the
server's identity providers, as the issuer's terminal client
(`cli_client_id`, a public client): by default in a browser, with PKCE and a
redirect to `http://127.0.0.1:<free port>/callback` (`--no-browser` prints
the URL instead of opening it); with `--device`, the device grant, for a
terminal with no browser. `--issuer-ca-file` trusts the provider's roots in
place of the system's. The ID and refresh tokens are kept in
`orkctl/credentials.json` under the user's config directory (mode `0600`,
one login per server; `ORK_CREDENTIALS` names another file), and a file
another user can read is refused. Every command given no other credential
sends the ID token as its bearer, refreshed first when it is within a
minute of expiry. `orkctl logout` forgets the login and revokes its refresh
token where the provider has a revocation endpoint.

Credentials are tried in this order, and the first one given is the only
one sent: a client certificate, a token file, a service account, the
stored login.

## Sign-in for the web UI

An issuer whose entry has a `login:` block offers sign-in: `client_id`, the
web client's `client_secret` (a source, `env:NAME` or `file:/path`, read at
each use), `cli_client_id` for `orkctl login`, `scopes` (with `openid`) and
`device_flow`. Such an issuer must be `https`; the engine reads its
discovery document for its endpoints. Register
`<auth.public_url>/v1/auth/callback/<issuer>` as the web client's redirect
URI. Many providers (Dex, Keycloak) issue a refresh token only when the
login asks for `offline_access`: add it to `scopes`, or a sign-in is renewed
by signing in again once it is past `auth.signin.revalidate_interval`.
Sign-in is proven against Dex, a real provider
(`TestIntegration_SigninWithDex`): Dex has neither a revocation nor an
end-session endpoint, so signing out ends the OrkStack sign-in alone.

1. The page reads `GetSystemInfo.auth_mode`, then `AuthService.GetAuthConfig`,
   and sends a visitor with no sign-in to `/signin`.
2. `GET /v1/auth/login/{issuer}?return=/path` makes a fresh `state`, `nonce`
   and PKCE verifier, seals them with the issuer's name and the return path
   in the `__Host-ork-login` cookie (ten minutes), and redirects to the
   issuer with `code_challenge_method=S256`.
3. `GET /v1/auth/callback/{issuer}` checks `state` against the cookie, the
   issuer in its path against the cookie's, and the response's `iss` against
   the issuer (RFC 9207; required when the issuer says it sends one), so a
   code from one issuer is never redeemed at another. It redeems the code
   with the verifier, verifies the ID token (its audience is exactly the
   `client_id`, or holds it with `azp` equal to it, and its `nonce` is the
   cookie's), refuses a disabled principal, writes the sign-in record, sets
   `__Host-ork-signin` (`Secure; HttpOnly; SameSite=Lax; Path=/`) and
   returns to the return path. A return path that is not a plain path of
   this origin returns to `/`. A refusal returns to `/signin?error=<reason>`
   and its detail goes to the engine's log.

**The record.** `metadata/identity/signins/{sha256(cookie)}.json`, created
only if absent, sealed whole (AES-256-GCM) under the sign-in key with its
storage key as associated data: the principal as of its last validation,
when it was made and when it ends, the issuer's refresh token and a CSRF
token. The cookie holds 32 random bytes, so a reader of the bucket cannot
use a record as a cookie. A replica answers from a record it read for 15
seconds. A record is deleted at sign-out, when it is revoked, or when it is
read expired; set a lifecycle rule on `metadata/identity/signins/` to
remove the rest.

**Limits.** A sign-in ends `auth.signin.max_age` (12 h) after it was made,
never extended, and after `auth.signin.idle_timeout` (1 h) without a
refresh. Every `auth.signin.revalidate_interval` (10 min) it must be
revalidated with its issuer: a request under it past that is refused
`SIGNIN_STALE`, except `GetSignin` (which hands a page loaded afresh its
CSRF token), `RefreshSignin` and `SignOut`, and the page calls `AuthService.RefreshSignin`, one call at
a time across its tabs, which redeems the refresh token, verifies the new
ID token when the issuer sends one (its `sub` the sign-in's) and takes its
groups as they are now, and writes the record under its etag. An issuer that refuses ends the
sign-in, so a person removed at the issuer loses access within the
interval; an issuer that grants no refresh token gives a sign-in that is
renewed by signing in again. A sign-in that has ended is refused
`SIGNIN_ENDED`.

**CSRF.** A request authenticated by the cookie whose method is not `GET`
or `HEAD` carries `Ork-CSRF` equal to the sign-in's token, which
`AuthService.GetSignin` hands out (it is the one call exempt) and the page
keeps in memory; any such request that carries an `Origin` carries
`auth.public_url`'s. A bearer request needs neither. Refused, it is
`403`, `SIGNIN_CSRF`.

**Sign-out.** `AuthService.SignOut` deletes the record, clears the cookie,
revokes the refresh token where the issuer has a revocation endpoint, and
answers the issuer's `end_session_endpoint`, when it has one, for the page
to visit. `AccessService.RevokeSignins` (`orkctl` and the Access page;
`access.write`) ends a principal's sign-ins at once on the replica that
answers, and on every other within the 15 seconds it may answer from a
record it read; a stream opened under a sign-in is closed within 15 seconds
of its end. A token `orkctl login` holds is a bearer until its `exp`, which
only disabling the principal stops sooner; `ListSignins`
(`access.read`) lists them, never a cookie, refresh token or CSRF token.
Each sign-in made, refreshed, ended or refused writes an audit record
(`auth.signin.created`, `.refreshed`, `.ended`, `.refused`).

**The sign-in key.** `auth.signin.key_files` lists files of exactly 32
random bytes (`head -c 32 /dev/urandom`). The first seals; every one opens.
Rotate in two steps: add the new file last on every engine and restart,
then move it first. A sign-in sealed under a key an engine lacks is ended,
and its person signs in again. Lost or leaked: remove it everywhere; every
sign-in ends. Nothing else depends on this key.

An issuer that offers sign-in needs `auth.public_url`, `https` except on a
loopback host, and a sign-in key, or the engine refuses to start.

## Passwords, through Dex

OrkStack stores no users and checks no password. A person is whoever a
trusted issuer's token says (above). A password store would be a durable
system of its own beside Temporal and object storage, with hashing, resets,
lockouts and second factors to get right, and OrkStack adds no such system
(AGENTS.md §13). A deployment with no identity provider runs one instead:
[Dex](https://dexidp.io), an OpenID Connect provider whose local password
database (the `local` connector, turned on by `enablePasswordDB`) signs in
the users its configuration lists under `staticPasswords`. OrkStack trusts
Dex as it would any issuer. The development stack runs the same set-up, with
in-memory storage and development users (`deploy/dex/config.yaml`);
`TestIntegration_SigninWithDex_DocumentedSetup` serves the two
configurations below with the pinned Dex and signs their user in.

Run the image `deploy/dex-image.txt` pins:
`ghcr.io/dexidp/dex:v2.45.1@sha256:8499afd690c437f52301efd2b05b2455da5bd2dfc20332cd697dc9937f808462`.
v2.45.1 is the first release that gives a static user groups and a verified
email, which OrkStack reads; v2.46.0 and v2.46.1 deadlock the refresh grant
on in-memory storage (dexidp/dex#5017).

**Dex's configuration.** One user, `alice@example.com`, in the group
`platform-admins`; two clients, the engine's web sign-in and `orkctl login`:

<!-- dex-example: dex -->
```yaml
issuer: https://dex.example.com/dex
storage:
  type: sqlite3
  config:
    file: /var/dex/dex.db
web:
  https: 0.0.0.0:5554
  tlsCert: /etc/dex/tls/tls.crt
  tlsKey: /etc/dex/tls/tls.key
expiry:
  idTokens: 1h
  signingKeys: 6h
oauth2:
  responseTypes:
    - code
  skipApprovalScreen: true
enablePasswordDB: true
staticPasswords:
  - email: alice@example.com
    # bcrypt of the example password `example-only`: replace it.
    hash: $2a$10$ooVYj3p6Jn2AMW.w6vwwwe3KSKNo48Off2uiMlIFsYhJoRl00EZ.y
    username: alice
    userID: 0b9d8f4e-3c1a-4f8e-9a52-6d1f2e7c4a10
    emailVerified: true
    groups:
      - platform-admins
staticClients:
  - id: orkstack-web
    name: OrkStack
    secretEnv: ORKSTACK_WEB_CLIENT_SECRET
    redirectURIs:
      - https://orkstack.example.com/v1/auth/callback/people
  - id: orkctl
    name: orkctl
    public: true
```

- `hash` is a bcrypt hash, never the password: make one with
  `htpasswd -nbBC 10 '' 'THE-PASSWORD' | tr -d ':\n'` (Apache's
  `htpasswd`, in `apache2-utils` or `httpd-tools`), and keep the file
  readable by Dex alone. The hash above is of a published password: an
  operator who copies it as it is makes an admin anyone can sign in as.
  `userID` is any stable, unique id; Dex derives the token's `sub` from it.
- `emailVerified: true` is required, because the issuer below names people
  by their email, which OrkStack accepts only verified.
- `orkstack-web` is a confidential client. Make its secret once, for
  example `(umask 077; head -c 32 /dev/urandom | base64 | tr -d '=+/' > web-client.secret)`, and hand the same
  value to Dex (`ORKSTACK_WEB_CLIENT_SECRET`) and to every engine (the file
  `client_secret` names below). Its one redirect URI is
  `<auth.public_url>/v1/auth/callback/<issuer name>`.
- `orkctl` is a public client, so `orkctl login` may use any loopback
  redirect, and the device flow (`orkctl login --device`), which Dex offers
  by default, for a terminal with no browser.
- Dex reads `staticPasswords` at start: adding a user, changing a password
  or removing one is an edit and a restart of Dex.
- Dex listens on port 5554 (`web.https`). Publish it at the issuer URL, on
  443 behind a load balancer or a port mapping, or write the port into
  `issuer` here and in OrkStack's `issuer` alike. The volume at `/var/dex`
  must be writable by the user Dex runs as.

**OrkStack's configuration.** Every engine trusts Dex as the issuer
`people`, and names the group of its first admins:

<!-- dex-example: orkstack -->
```yaml
auth:
  mode: enforce
  public_url: https://orkstack.example.com
  admins:
    - group:people:platform-admins
  signin:
    key_files:
      - /etc/orkstack/signin.key
  issuers:
    people:
      issuer: https://dex.example.com/dex
      kind: user
      audiences:
        - orkstack-web
        - orkctl
      algorithms:
        - RS256
      username_claim: email
      require_verified_email: true
      groups_claim: groups
      max_lifetime: 2h
      login:
        client_id: orkstack-web
        client_secret:
          source: file:/etc/orkstack/web-client.secret
        cli_client_id: orkctl
        device_flow: true
        scopes:
          - openid
          - email
          - groups
          - offline_access
identity:
  signing:
    key_files:
      - /etc/orkstack/engine-signing.pem
```

- `issuer` is Dex's `issuer`, exactly. Dex serves it over `https`, which
  sign-in requires; when its certificate is from a private CA, give the
  issuer `ca_file`.
- `audiences` holds both client ids: a web sign-in's ID token is issued to
  `orkstack-web`, and `orkctl login`'s to `orkctl`.
- `algorithms` is `RS256`, the only one Dex v2.45.1 signs with.
  `max_lifetime` must be at least Dex's `expiry.idTokens`.
- `scopes` asks for `groups`, which carries the static user's groups, and
  `offline_access`, without which Dex issues no refresh token and a sign-in
  must be made again once it is past `auth.signin.revalidate_interval`.
- `alice@example.com` signs in as `user:people:alice@example.com`, in the
  group `group:people:platform-admins`, which `auth.admins` makes `admin`.
  Grant everyone else roles with bindings (ACCESS.md):
  `orkctl admin binding create ops --role operator --scope namespace:payments --subject group:people:payments-ops`.
- The sign-in key and the engine signing key are made as above ("The
  sign-in key", and `identity.signing.key_files`).

**Other sources of users, later.** The same Dex can front LDAP, GitHub, a
SAML provider or another OpenID Connect provider: add a `connectors:` entry
to Dex's configuration. OrkStack's configuration does not change: it trusts
Dex's tokens, whichever connector signed the person in. A person whose
verified email is the same through two connectors is one principal; their
groups are whatever that connector gave (a GitHub team is `org:team`).

**Before production.**

- *TLS.* Dex serves `https` with a certificate the engines and every
  browser trust, at a stable issuer URL: tokens name it, and changing it
  signs everyone out.
- *Dex's storage.* `memory` loses Dex's signing keys and refresh tokens at
  every restart, which ends every sign-in at its next refresh. `sqlite3`, as
  above, keeps them for one Dex on a persistent volume; several Dex
  replicas share `postgres`, `mysql`, `etcd` or Kubernetes custom
  resources. That store is Dex's, as Temporal's database is Temporal's, and
  OrkStack never reads it.
- *Dex's signing keys.* Dex makes and rotates them itself, every
  `expiry.signingKeys` (6 hours by default), and keeps publishing the key it
  retired until the tokens it signed have expired. An engine fetches Dex's
  key set again when a token names a key it does not hold, at most once a
  minute, so a rotation needs nothing from OrkStack. Nothing is rotated by
  hand; keep the storage, since a key lost with it refuses every token it
  signed.
- *The login page.* Dex's password database has no lockout, password reset
  or second factor: put its login page behind a rate limit at the proxy in
  front of it, and move to an identity provider that has them (through a
  Dex connector) when you need them.
- *Removing a person.* First disable them,
  `orkctl admin principal disable user:people:alice@example.com`: a
  disabled principal is refused whatever it presents, a web sign-in or a
  token `orkctl login` holds (ACCESS.md). Then remove them from
  `staticPasswords` and restart Dex, so they cannot sign in again. Removing
  them from Dex alone is not enough: a sign-in ends on a refresh only when
  the issuer answers `invalid_grant`, and a token already issued stays
  valid until it expires.

## An Orker's credential

An Orker reaches the engine's API as itself with `orker.engine.*`:
`orker.engine.url` (`https`, or `http` to a loopback address), an optional
`ca_file`, and `auth.method` `token_file` (`auth.token_file`, read at every
request), `service_account` (`auth.service_account` and `auth.key_file`, a
private key read at boot, signing one assertion per request as `orkctl`
does; `orker.engine.url` is then the deployment's public URL) or `mtls`
(`auth.cert_file` and `auth.key_file`). At boot it asks
the engine who its credential is (`AuthService.GetSignin`) and refuses to
start if the engine does not accept it, or if the settings are incomplete;
with no `orker.engine.url` it asks nothing and supports no identity. The
files that make the credential are the Orker's own: no task may name them
as a secret source. Bind the Orker with `auth.orkers` or a binding to the
`orker` role at its pool (docs/ACCESS.md). The engine answers the Orker id
the credential names (`GetSigninResponse.orker_id`: its issuer's
`orker_id_claim`, a service account's `orker_id`, a certificate's SAN), and
an Orker whose `orker.id` differs refuses to start, naming both. A credential
that names no Orker, shared by a pool bound with `orker_ids: any`, lets the
Orker keep its own id, and serves no task identity and no event source.

## Who am I

`orkctl whoami` (`AuthService.GetSignin`) answers any authenticated caller,
with no permission: the principal the engine verified, how (`oidc`, `jwt`,
`service-account`, `break-glass`, …), its issuer and groups, each binding that matched it with
only the subject that matched, its permissions by scope, against the
replica's copy of the access document, and the Orker id its credential
names, when it names one. An engine in `asserted` mode verifies
nobody, so it serves no `whoami`. With `ORK_PRINCIPAL` set against an engine
in `observe` or `enforce`, `orkctl` warns once that the variable is not used.

## The first admin, and break-glass

- **`auth.admins`** (`ORK_AUTH_ADMINS`, a list of subjects) hold `admin` at
  the deployment by configuration, whatever the access document says. They
  are shown as "from configuration" and cannot be removed through the API.
  This is the bootstrap: name one group, sign in, create bindings. It is
  also the way back from a wrong document, since whoever can edit the
  engine's configuration already controls the engine.
- **Break-glass.** `auth.break_glass_key_file` names one public key (PEM,
  RSA 2048 or more, or P-256), and needs `auth.public_url`. A request
  authenticated by an assertion signed with the private half is
  `serviceaccount:break-glass` with `admin`. The assertion is valid for at
  most 60 seconds and bound to its one request: its method, its URL, the
  SHA-256 of its body and a `jti` this replica has not seen. It must carry
  an `Ork-Reason` header (1 to 256 bytes, one line), and its body is at most
1 MiB, since it is read whole to be hashed. A replica remembers the
  assertions it accepted for their life, in its own memory, so the identical
  request can be played once more at each other replica within its minute;
  it cannot be turned into another call, and each use is recorded and
  alarmed. Whoever can capture it in flight already sits inside the
  deployment's own path. Each use writes the
  audit record `auth.break-glass.used` before the request is served and
  logs at error level; if the record cannot be written, the whole record is
  logged at error level and the request is served, since break-glass exists
  for the day things are broken. Keep the private key offline.

`enforce` with no trusted issuer and no break-glass key refuses to start:
nobody could ever authenticate.

## The mode record

The first engine to run in `observe` or `enforce` writes
`metadata/identity/mode.json`: the highest mode the deployment runs. It
needs no key to read, because the engine it must stop is the one started
from an old or mistyped configuration.

| What an engine finds | At boot | While running (read again each minute) |
|---|---|---|
| a record naming its own mode or a lower one | starts, and raises the record to its own mode | nothing |
| a record naming a higher mode | refuses to start, naming both | logs at error; keeps serving, since its own configuration is what to fix |
| no record | starts; in `observe` or `enforce` writes it | a record it saw that is now gone is logged at error, and written again at its own mode |
| a record that does not decode, or names a mode or format it does not know | refuses to start, naming the object | logs at error; keeps serving on the mode it booted with |
| a store that cannot be read | waits for it: it cannot serve without the store | nothing new |

**Lowering is a command, then a restart.** `orkctl admin auth lower MODE`,
by a caller with `access.write`, rewrites the record and writes
`auth.mode.lowered`; then restart the engines in the lower mode. There is no
setting to leave behind. `enforce` to `observe` is never refused. To
`asserted` is refused until the deployment has been out of `enforce` for one
idempotency key's life: the longest `engine.idempotency_ttl` any engine
advertises on the engines' presence channel (below), or has advertised
since the answering engine started, and five minutes.
`orkctl admin auth status` shows when. A lowering whose engine cannot list
the engines is refused rather than guessed. `--accept-duplicate-retries` skips
the wait, accepting that a retried submission may start a second run, and
the audit record says it was given. A lowering is refused by an engine that
runs below the record's mode, and an engine that finds the record lowered
beneath its own mode while running logs it at error level until it is
restarted in that mode.

**A record no engine can read** refuses every engine's boot, and `orkctl`
needs a running engine. `ork --set-mode-record MODE --reason TEXT` reads the
engine's own configuration, prints the store it is about to change, refuses
a record that still decodes (that one is lowered with `orkctl`; `--force`
overrides it when no engine can start at all), writes the audit record
`auth.mode.record.set` and stops, changing nothing, if that record cannot be
written, then overwrites the record and exits without serving.

What the record cannot stop: someone who deletes it while every engine is
down, a bucket lifecycle rule or a restore that removes it, or an engine of
an earlier release, which knows nothing of it. Exclude `metadata/identity/`
from every lifecycle rule, and turn on versioning (and object lock, where
the store has it) for it.

## Every engine enforces

Every `ork` process, whatever it serves, polls one more Temporal task queue
that nothing dispatches to, the engines' presence channel `ork-engines`, and
says what it is in the identity it polls with, after its capabilities:
`mode=`, `claim-ttl=` (its `engine.idempotency_ttl`), `deployment=`,
`build=` and, when it authenticates, `admins=` and `orkers=`, the first 16
hex digits of the SHA-256 of its `auth.admins` and `auth.orkers` (entries
sorted, so the order they are written in does not matter). For example
`48213@engine-2@ ork-caps=…,authn-enforce,mode=enforce,claim-ttl=24h0m0s,deployment=orkstack`.

Once a minute each enforcing engine reads that channel. An engine whose mode
is below `enforce` is logged at error level and shown by
`orkctl admin auth status` (`not enforcing:`); so is an engine polling the
workflow channel without advertising `authn-enforce`, which is how an engine
of an earlier release, polling no presence channel, is still seen. An
authenticating engine whose `auth.admins` or `auth.orkers` differs from the
reading engine's is logged at error level and shown (`differs:`): set the
same lists on every replica. `fleet checked:` says when the cluster was last
read; a check that cannot read it keeps its last report, and that time stays
where it was. Each engine says these things of itself: nothing authenticates
a poller's identity, so the report is advice for an operator, never a
decision. It refuses nothing: a poller stays listed for minutes after its
process is gone, so a refusal would turn every deploy into an outage. The
guard against an engine that does not enforce is the mode record.

A value the identity cannot carry (over 100 bytes, or holding a space, a
comma, `=` or anything but printable ASCII) is left unsaid and logged at
boot; a claim life over ten years is said as ten years. Anyone who can read
the cluster's pollers can test a guessed `auth.admins` or `auth.orkers` list
against its digest, which is unsalted: a digest says whether two lists are
equal, never who is on one, but a short list of guessable ids can be
confirmed. A poller that advertises a longer claim life than an engine's own
holds the lowering to `asserted` off for that long; the engine logs it at
warn level, naming it.

## How old a decision's facts may be

Each replica decides from a copy of the access document at most
`auth.policy_refresh` old (`10s`), so a binding removed through one replica may
still be honoured by another for that long. The ids of namespaces and
Orkflows that pinned bindings match against ([ACCESS.md](ACCESS.md),
"Pinned bindings") are kept for the same 10 seconds. A namespace or an
Orkflow deleted and created again through another replica may therefore be
judged against its old id for up to 10 seconds: a binding pinned to the old
one may still match it there, and a binding pinned to the new one may not
match yet. A replica that made the delete or the create itself uses the new
id at once.

## The admin listener

`/metrics` and `/debug/pprof/` answer without authentication on
`ORK_ADMIN_ADDR`. A heap profile of a process that holds keys is those keys:
with a mode other than `asserted`, `ork` refuses to start with
`telemetry.pprof_enabled` unless the admin address is loopback. Keep the
admin listener on a private network.

## Settings

| Setting | Environment | Default |
|---|---|---|
| `auth.mode` | `ORK_AUTH_MODE` | `asserted` |
| `auth.public_url` | `ORK_AUTH_PUBLIC_URL` | (empty) |
| `auth.issuers` | `ORK_AUTH_ISSUERS` | (empty document) |
| `auth.admins` | `ORK_AUTH_ADMINS` | (empty list) |
| `auth.break_glass_key_file` | `ORK_AUTH_BREAK_GLASS_KEY_FILE` | (empty) |
| `auth.signin.key_files` | `ORK_AUTH_SIGNIN_KEY_FILES` | (empty list); required when an issuer offers sign-in |
| `auth.signin.max_age` | `ORK_AUTH_SIGNIN_MAX_AGE` | `12h` |
| `auth.signin.idle_timeout` | `ORK_AUTH_SIGNIN_IDLE_TIMEOUT` | `1h` |
| `auth.signin.revalidate_interval` | `ORK_AUTH_SIGNIN_REVALIDATE_INTERVAL` | `10m` |
| `auth.clock_skew` | `ORK_AUTH_CLOCK_SKEW` | `1m` |
| `auth.policy_refresh` | `ORK_AUTH_POLICY_REFRESH` | `10s` |
| `auth.policy_stale_limit` | `ORK_AUTH_POLICY_STALE_LIMIT` | `5m` |

[DEPLOY.md](./DEPLOY.md) §1 is the complete table of settings.

## Metrics

On the default sink, with no principal id and no token in any of them:
`ork.auth.requests{method,outcome}` (requests that presented a credential:
authenticated, refused or unavailable), `ork.auth.decisions{permission,decision}`
(allowed, denied, would-deny), `ork.auth.streams_closed{reason}` (denied,
disabled, unavailable, expired, signed_out), `ork.auth.break_glass` (page on any),
`ork.auth.mode_record_missing` and `ork.auth.fleet_unenforced` (engines the
last fleet check saw that do not enforce).
