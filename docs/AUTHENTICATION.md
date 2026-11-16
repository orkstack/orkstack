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

A request carries one credential, as `Authorization: Bearer`. A credential
that is presented and refused is refused (`401`), never passed on to
another method.

- **A token from a trusted issuer.** `auth.issuers` (`ORK_AUTH_ISSUERS`) is
  one YAML document of the issuers whose JWTs the engine accepts: a person's
  identity provider, or a CI or cluster issuer for services. Every claim rule
  is equality; a service issuer must name `required_claims` or say
  `dedicated: true`. The engine checks the signature against the issuer's
  published keys, the audience, the expiry and the issuer's maximum
  lifetime, with `auth.clock_skew` (1 minute) of leeway.
- **Break-glass**, for an identity provider that is down
  (below).

A person is `user:<issuer>:<username>`, a service of a trusted issuer
`service:<issuer>:<sub>`. Groups come from the issuer's `groups_claim` (at
most 200), and only the claims the issuer's `claims` list names are kept.

## From orkctl

`--token-file FILE` (`ORK_TOKEN_FILE`) presents the file's one token as
`Authorization: Bearer`, read again at every request, so a token something
else rotates (a CI job's GitHub or Kubernetes token) is sent as it is now. It
is presented only to an `https://` server or a loopback `http://` one, and
refused for any other: it would cross the network in the clear.
`--ca-file` sets the roots an `https://` server is verified against, in
place of the system's, and `--client-cert` with `--client-key` presents a
client certificate; a client certificate takes precedence over a token
file. This release's engine authenticates bearer tokens and break-glass
assertions; it does not yet take a principal from a client certificate.

## An Orker's credential

An Orker reaches the engine's API as itself with `orker.engine.*`:
`orker.engine.url` (`https`, or `http` to a loopback address), an optional
`ca_file`, and `auth.method` `token_file` (`auth.token_file`, read at every
request) or `mtls` (`auth.cert_file` and `auth.key_file`). At boot it asks
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
Orker keep its own id.

## Who am I

`orkctl whoami` (`AuthService.GetSignin`) answers any authenticated caller,
with no permission: the principal the engine verified, how (`jwt`,
`break-glass`, …), its issuer and groups, each binding that matched it with
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
| `auth.clock_skew` | `ORK_AUTH_CLOCK_SKEW` | `1m` |
| `auth.policy_refresh` | `ORK_AUTH_POLICY_REFRESH` | `10s` |
| `auth.policy_stale_limit` | `ORK_AUTH_POLICY_STALE_LIMIT` | `5m` |

[DEPLOY.md](./DEPLOY.md) §1 is the complete table of settings.

## Metrics

On the default sink, with no principal id and no token in any of them:
`ork.auth.requests{method,outcome}` (requests that presented a credential:
authenticated, refused or unavailable), `ork.auth.decisions{permission,decision}`
(allowed, denied, would-deny), `ork.auth.streams_closed{reason}` (denied,
disabled, unavailable, expired), `ork.auth.break_glass` (page on any),
`ork.auth.mode_record_missing` and `ork.auth.fleet_unenforced` (engines the
last fleet check saw that do not enforce).
