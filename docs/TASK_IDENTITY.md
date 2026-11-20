# Task identity

Task identity lets a task prove to a cloud or a secret store which
namespace, Orkflow and task it is, so that no long-lived credential needs
to be stored for it. OrkStack is an OpenID Connect issuer: it publishes its
signing keys, and a relying party such as AWS or Vault trusts tokens that
those keys signed.

This document covers the issuer: its keys, what it publishes, and the
subject a task's token names. A task cannot yet declare an identity, and no
token is issued yet. That is the next part of task identity.

## The issuer

`identity.issuer` turns task identity on. It is an `https` URL with no
query, fragment or user. It becomes the `iss` of every token, and it is
where relying parties fetch the keys. Unset, none of this document applies.
One issuer URL belongs to one deployment, because two deployments sharing
one would be indistinguishable to a relying party.

The issuer publishes three documents:

| Document | Exported as | Served by an engine at |
|---|---|---|
| the OpenID discovery document | `.well-known/openid-configuration` | `GET /.well-known/openid-configuration` |
| the public signing keys, a JWK set | `jwks.json` | `GET /v1/identity/jwks` |
| the SPIFFE bundle, only with a trust domain set | `spiffe-bundle.json` | `GET /v1/identity/spiffe-bundle` |

There are two ways to host them, and a deployment may use both:

1. **Exported**, the recommended shape. Run `orkctl admin identity issuer
   export --out DIR` and put the files on any HTTPS host at the issuer URL,
   such as a bucket behind a CDN. AWS reaches the issuer over the public
   internet and needs a publicly trusted certificate, and most deployments
   will not expose their engines there. Export again after every key
   change.
2. **Served.** With `identity.serve_discovery: true`, the default, every
   engine also answers the three routes above with no credential, in every
   authentication mode, for relying parties inside the network.

The discovery document is the same in both shapes: its `jwks_uri` is
always `<issuer>/jwks.json`. A relying party that reads the keys from an
engine is given the engine's route directly. With Vault, set
`jwks_url: https://<engine>/v1/identity/jwks` and
`bound_issuer: <identity.issuer>`, not `oidc_discovery_url`.

`identity.issuer` needs shared object storage. On the local backend no
other engine reads the keys one publishes, so the boot is refused.

## Signing keys

`identity.signing.key_files` lists the engines' PEM private keys, each an
unencrypted PKCS #8, PKCS #1 or SEC 1 file. Every engine lists the same
files. A key's id (`kid`) is its RFC 7638 thumbprint, so every engine names
a key the same way with no setting.

`identity.signing.algorithm` is `RS256`, the default, for RSA keys of at
least 2048 bits (3072 recommended), or `ES256` for P-256 keys. Every
relying party this design names accepts `RS256`. A listed key of the other
kind refuses the boot.

The public keys are one document in the object store,
`metadata/identity/issuer/jwks.json` (see [STORAGE.md](./STORAGE.md)).
Each engine adds its keys at boot and never removes one. It reads the
document again every minute and serves what it holds. Only an engine's
credential can write the document, so an Orker cannot add a key.

**Writing the document is control of the issuer.** Whoever can write
`metadata/identity/` can publish a key of their own, which relying parties
then trust, or remove a retirement. Only the engines' storage credential
may write there; an Orker's own credential cannot ([DEPLOY.md](./DEPLOY.md),
"An Orker's own storage credential"). An engine refuses what it can check:
it signs with a key only while the published entry under its id is the key
it holds, it serves a key only under its own thumbprint, and a key it has
read as retired stays retired for as long as it runs, whatever a later
document says.

### When a key signs

A key may sign once both of these hold:

1. it is published and not retired;
2. it has been published for `identity.signing.activation_delay` (24 h by
   default), or no other unretired key exists.

Before a key signs for the first time, an engine also checks that the
issuer URL serves it, by reading the discovery document there and then its
`jwks_uri`, which must be `https` on the issuer's own host
(`identity.signing.verify_published`, default `true`; turn it off only
where an engine cannot reach its own issuer). The first engine to find a
key ready records `activatedAt` on it in the document. From then on the key
signs on every engine, however the issuer URL answers later, so a CDN
outage during a rolling restart cannot stop signing.

An engine signs with the first key in its list that qualifies. When none
qualifies, the attempt to sign fails with `IDENTITY_KEY_NOT_ACTIVE`, and
`orkctl admin identity keys list` shows which condition each key fails.

The delay is a bound the operator owns. It must be longer than the longest
time any relying party keeps an old key set, plus the time an export takes
to reach every edge. AWS documents no figure, which is why the default is a
day.

Start a deployment with **one** key. Two keys published together each see
the other unretired, so neither signs until the delay has passed.

An engine signs from its last copy of the document for at most
`identity.keys_copy_max_age` (an hour by default) while the store cannot be
read. After that no key qualifies, so a retirement reaches every engine
that is still signing.

### The copy on disk

With `identity.keys_copy_file` set, an engine writes its last good read of
the document there after every read, atomically and `0600`, with the time
of that read. An engine that starts while the store is away decides from
that copy until it is `identity.keys_copy_max_age` old, counted from that
read; a copy dated in the future is too old. Once the store answers again,
its document replaces the copy. Unset, an engine restarted during a store
outage signs nothing and verifies nothing until the store returns.

The copy holds public keys only, but whoever can write it can add a key:
keep it where the key files are. The engine refuses a copy that is a link
or that others can write, and the boot refuses a path it cannot write
beside. Set `identity.keys_copy_max_age` equal on every engine, and raise
it before planned work on the object store.

### Rotation, with no outage

1. Put the new key **first** in `identity.signing.key_files` on every
   engine, keeping the old one second, and restart them in any order. Each
   publishes the new public key.
2. If the issuer is exported, run `orkctl admin identity issuer export` and
   upload.
3. After the activation delay the engines sign with the new key. After a
   further token lifetime, run `orkctl admin identity keys retire KID`,
   export and upload again, and remove the file from `key_files` when
   convenient.

A retired key never signs again on any engine, whatever its `key_files`
still lists.

**A key that has leaked** is retired as compromised from an instant:
`orkctl admin identity keys retire --compromised-from T -- KID`. T may lie
in the past; give the earliest moment the key may have leaked. What the key
signed for an instant before T still verifies, and nothing from T on does:
a run force-cancelled under it from then on reads as terminated outside
OrkStack and cannot be resumed, so ask again the force cancels you still
need. T may be moved earlier later, never later. It is no longer served, and it stays in the document, which
never drops a key. Retiring a key is the answer to a key that has leaked:
each engine stops signing with it at its next read of the document, within
a minute while the store answers, and within `identity.keys_copy_max_age`
while it does not.

`export` refuses on an engine that has not read the published keys, or
whose last read failed: a partial key set uploaded over the live one would
break verification for every relying party. An engine answers its key set
and bundle routes `503`, never an empty set, before its first read, when
its copy is older than `identity.keys_copy_max_age`, and when no key can be
served (every one is retired). An engine whose published keys document it
cannot read, or whose change to it the document's rules refuse, does not
start: neither clears by waiting.

`orkctl admin identity keys list` shows each key with when it was
published, when it began to sign, whether it is retired and by whom, and
the engines that hold it. An engine re-confirms its keys once a day, and a
confirmation older than a week no longer counts. A published key that no
engine has confirmed lately is reported in the log and in the list. That is
a report, never a control.

## Signing keys with no issuer

The engines' signing keys also sign what one engine hands another through
the cluster ([SECURITY_MODEL.md](./SECURITY_MODEL.md), "What engines sign
for each other"). A deployment whose `auth.mode` is `observe` or `enforce`
must list at least one key in `identity.signing.key_files`, also with
`identity.issuer` unset. The keys are then published to the stored document
only: no token is signed, nothing is served, and the activation delay does
not apply.

## Subjects

A token's `sub` names the task, broadest first, using names only:

```text
ork:namespace:<namespace>:orkflow:<orkflow>:task:<task>
ork:namespace:<namespace>:orkflow:<orkflow>:task:<task>:grant:<grant>
ork:namespace:<namespace>:orkflow:<owner>/<orkflow>:task:<task>
```

The third form is a shared Orkflow, named by its owner. A name holds no `:`
and no `/`, so a subject parses one way. A subject longer than 255 bytes is
refused when the Orkflow is created.

### SPIFFE

With `identity.subject_format: spiffe` the subject is a SPIFFE ID in the
deployment's trust domain, and a token is a JWT-SVID:

```text
spiffe://<trust domain>/ork/ns/<namespace>/orkflow/<orkflow>/task/<task>[/grant/<grant>]
spiffe://<trust domain>/ork/ns/<namespace>/shared/<owner>/<orkflow>/task/<task>[/grant/<grant>]
```

`identity.spiffe.trust_domain` is lowercase letters, digits, `.`, `-` and
`_`, from 1 to 255 bytes, with no scheme, port or path. The `spiffe`
format with no trust domain refuses the boot.

The trust domain must be OrkStack's own, such as `ork.acme.dev`. Never use
one a SPIRE server already serves, and never share one between two
deployments: a trust domain has one bundle, and it cannot hold two key sets.
OrkStack cannot check this. SPIRE is never required and never contacted.

With a trust domain set:

- every token carries both forms, the chosen one as `sub` and the other as
  a claim (`ork_subject` or `ork_spiffe_id`);
- both forms are held to 255 bytes whichever is chosen, so a later switch
  refuses no Orkflow that exists;
- the issuer serves and exports the key set as a SPIFFE bundle, whose keys
  are marked `jwt-svid`, with the published-keys document's sequence as
  its `spiffe_sequence` and a `spiffe_refresh_hint` of five minutes. A
  SPIRE server or a mesh federates with the trust domain through it.

To change format: move Vault roles to the claim that carries the other form
first, and have an AWS trust policy list both subjects for the time of the
change. Then switch the format. A run keeps the format it was submitted
with.

## A namespace's identity policy

Each namespace has an identity policy: the token audiences its tasks may
ask for (a trailing `*` matches any rest), its grants (a provider, the
principal there, the Orkflows that may use it, whether only a current
version may, and the kinds of start it serves), `identities_current_only`,
and `attempts_extra`, how many attempts past a task's own retries may hold
its identity, at most the deployment's `identity.attempts.max_extra`. The
API shows it as `Namespace.identity`, beside the namespace's id. It is kept
beside the id, never in the namespace document, and belongs to that
incarnation of the namespace: deleting the namespace removes it first, and
a namespace made again under the name starts with none.

An `UpdateNamespace` that changes the policy changes nothing else: it names
the policy's own `identity.etag` and needs `namespace.policy.update`. An
update whose policy is as stored leaves it alone. Each change writes an
immutable revision (`docs/STORAGE.md`). An audience is allowed by a
pattern equal to it or one whose trailing `*` matches any rest; a policy that
lists none allows none. An Orkflow whose task asks for an audience its
namespace does not allow is refused `IDENTITY_NOT_PERMITTED` when it is
created, naming the field, and again at submit, for every Orkflow the run
composes too, and at the mint. With `identities_current_only`, only the
version an Orkflow's current pointer names may hold identities. A run
whose tree holds an identity is refused `IDENTITY_STORE_UNSCOPED` at submit
while any live Orker's storage credential is not confirmed scoped, unless
`identity.unscoped_orkers` is `report`. Creating
or validating one also warns, never refuses, of a cached task that holds an
identity (a memo hit mints nothing, so it is not checked against the policy
as it is then), of a pin whose members run different images where any holds
one (a pin is one trust domain), and of an identity task whose pool more
than one namespace may use (naming the pool and how many, never which). No task can
use the policy yet; it decides what tasks may hold once task identity is
available.

## Consent

A schedule, a trigger and an event source start runs nobody submitted, on
the authority of their **binder**: who created one, changed what it submits,
or rebound it. Beside the binder each records a **consent**: the digest of
what its Orkflow asked for, given to a digest the person was shown. The
digest is `c1:` and 64 hexadecimal digits, the SHA-256 of consent
projection 1: for every task that would hold an identity (an identity, a
credential or a secret through a grant), in the Orkflow and in every Orkflow
it composes at the versions they pin, the task's id, its kind, its pool, its
join, what it asks for, and every task upstream of it, each with its id,
kind, join and needs, a composed task's Orkflow and an approval's deadline and
callback. A version that changes any of that is another consent; one that
changes only an image, a command or the input is not. An Orkflow that asks
for nothing has the empty table's digest,
`c1:8273758e29b4114ebe3e0962bfd318248772032b0727a367a63c0c3c44195545`.

| A save that | Binder | Consent |
|---|---|---|
| creates the object, or changes what it submits | the saver | needed, when the Orkflow asks for an identity: `--consent=DIGEST` |
| changes nothing it submits | unchanged | unchanged |
| says `--rebind` | the saver | unchanged |
| gives `--consent=DIGEST` | unchanged | that digest, with who gave it |

A consent is recorded only for the digest of the version the object follows
at that moment. Anything else is refused `IDENTITY_CONSENT_MISMATCH`, with
what changed and the digest to consent to, and nothing is recorded. A save
that needs one and gives none is refused the same way, handing out the
digest. An Orkflow one of whose composed Orkflows is not there cannot say
what it asks for: it is saved with no consent, and a consent given to it is
refused.

A run a schedule or a trigger starts holds an identity only while three
things hold, each checked at the run's start: the object records a verified
binder with the binding that let it bind (recorded at the save that made
it the binder; `IDENTITY_STARTER_NOT_RECORDED` otherwise); that binding
still grants the binder `run.submit` on the Orkflow, judged from the access
document alone, so a group or a claim is never read again
(`IDENTITY_STARTER_NOT_PERMITTED`); and the consent is to what the version
the run resolves asks for (`IDENTITY_STARTER_NOT_CONSENTED`). A fire so
refused fails; an invoke answers 403. A fact that could not be read is
never a refusal: the fire tries again and an invoke answers 503.

`orkctl schedule get`, `orkctl trigger list` and `orkctl event-source get`
show the consent, what the Orkflow asks for now (`asks_for`: the digest and
the distinct audiences and grants) and whether the consent is stale.
`orkctl schedule|trigger|event-source update NAME --consent` with no value,
at a terminal, shows what would be consented to and asks. Promoting a
version that asks for other identities lists every object that follows the
pointer and is left stale, each with the command that consents
(`SetCurrentOrkflowVersionResponse.stale_consent`). In an automation document
there is no key for either: `orkctl automation apply --consent
schedule/NAME=DIGEST` gives one, and the plan warns with the digest, never as
a step, so the plan's digest does not change.

## The mint

An Orker that runs an attempt of a task holding an identity asks the
engine for its tokens (`IdentityService.MintAttemptTokens`), which an
engine answers only with `auth.mode: enforce`, an issuer and an eligible
key. The engine reads the run's admission record by the digest the task's
input names, checks that the caller's Orker holds the attempt now, that
the run started as its admission says, that the task is the one admitted,
that the attempt cap allows it and that the namespace still allows each
audience, then signs one token per identity, with the claims the admission
pins and a life between `identity.token.min_ttl` and `max_ttl`. With
`identity.issuance.records: required` the record of what was issued is
written before any token is returned. [REASONS.md](./REASONS.md#the-identity-mint)
lists every refusal.

## Results the engine records

A run that holds a task identity, or belongs to a run tree in which one
does, has its results recorded by the engine: every container task of it is
in the run's admission, with its pool and spec digest, whether or not it
holds an identity. The Orker that holds an attempt hands the engine the
attempt's manifest (`AttemptService.CommitResult`), which an engine answers
only with `auth.mode: enforce`; the engine writes it, create-only, under
`metadata/attested/` (`docs/STORAGE.md`), where no Orker's storage
credential may write. It records only for an attempt the cluster reports as
held by the caller, at the pool the run was admitted with for that task,
dispatched as it was admitted, in this order:

1. the replica enforces authentication (`IDENTITY_ENGINE_REFUSED`);
2. the run's admission, by the digest the request names, has the task in
   its table (`IDENTITY_NOT_ADMITTED`);
3. the caller holds `task.result.commit` at the pool the admission gives
   the task, and its credential names one Orker;
4. the attempt is pending, started, at that number and on that Orker in a
   describe made after the request arrived (`ATTEMPT_NOT_HELD`);
5. the execution is the run the admission was written for;
6. the task's spec digest is the admitted one (`TASK_NOT_AS_ADMITTED`), and
   each task the admission says it follows has its record;
7. the manifest is the attempt's: schema 1, and its namespace, run, task,
   definition digest, idempotency key, attempt and Orker id are the
   request's, the admission's and the caller's; its image digest is the one
   the definition pins, where it pins one; it carries no recording; and the
   commit key is this task's or this item's (`COMMIT_IDENTITY_MISMATCH`);
8. the engine writes it with `recorded`: the pool, the Orker id its
   credential names, the admission's digest, the engine's build and the
   time. A record already there, this attempt's repeat or another attempt's
   that came first, is answered as it is and nothing is written.

Steps 2 and 4 to 6 are the mint's own. A request with `preflight` set asks
steps 1 to 6 and writes nothing, so an attempt the engine would refuse runs
no container. A step that could not ask (the store, the cluster) is
unavailable and is asked again. A refusal that says something about the
caller or the run writes one `task.result.refused` audit record, coalesced
per principal as denials are; a grant is counted and logged. A request is at
most the commit's own bound (4 MiB) and 64 KiB, and a result whose stored
form leaves no room under 4 MiB for the engine's recording is refused at
once; each attempt is granted at most 4 calls a minute, and each principal
draws on a bucket of 512 that refills at 128 a second, which only a call
that could not be asked gives back. The admitted definition's image pins
are read once a definition and kept, at most 256 definitions a replica. The run's submit adds
`identity.results.commit_wait` (`docs/DEPLOY.md` §1) to the run's transfer
budget, so an attempt's deadline has room to wait for an engine. Cleanup
removes a run's records with its commits.

The Orker's half, which reads, adopts and stages only from these records and
hands its manifest to the engine, is not built in this release yet: no
Orker calls `CommitResult`, and an Orker's commit store may read under
`metadata/attested/` and never writes there.

## Which Orkers serve it

An Orker serves task identity only while it has checked that it may: at
boot and every five minutes it asks the engine who its credential is
(`AuthService.GetSignin`, `orker.engine.*`), and it advertises support
(`DescribeOrkerResult.identity_supported`) only while the answer names its
own Orker id, grants it `identity.token.mint` and `task.result.commit` at its
pool (or the deployment), and its storage credential is confirmed scoped. An
engine that does not answer within 30 s, an answer older than ten minutes,
a binding that is gone or a verdict that lapses ends support, logged at
error. A credential that names no Orker (`orker_ids: any`) never serves it.
The clock check is not yet part of the verdict. An Orker with no engine
configured serves none. A submission of a run that holds an identity asks
every uncordoned Orker of every pool the run, or any child it plans, runs a
container on, and is refused `POOL_NO_IDENTITY_SUPPORT`, naming the Orker
and its build, while one does not serve it. Where another Orker of the pool
answers, an Orker that polls the pool's channel and does not answer on its
own is refused the same way, as one that did not answer, which may pass:
submit again once it answers, or cordon it. A pool none of
whose Orkers answers, and a silent Orker whose pool cannot be told, are
logged, not refused. Dispatch is the guard for those: an Orker that does
not serve task identity refuses a task that holds one before its container
is created, `IDENTITY_NOT_CONFIGURED` (no engine, not retried) or
`IDENTITY_NOT_SERVED` (its self-check does not find support now, or has not
answered since boot; retried only under the task's own `retry:`).
These checks fail early; neither is the boundary. An Orker is trusted
with nothing it reports about itself: the mint decides every token from
the cluster's record of who holds the attempt and the caller's verified
permissions ([The mint](#the-mint)).

## On the Orker

An attempt of a task that declares an identity asks the engine for its
tokens after the commit check and the memo step, so neither a committed
result nor a memo hit mints, and before the container exists. Each token is
masked in the task's logs before any file holds it, and the files are bound
read-only at `/orkstack/identity/<name>/` (`docs/OCI_ACTIVITY_CONTRACT.md`).
The request carries the task's spec digest as the Orker computes it from
its input (`task.IdentitySpecDigest`), which the engine compares with the
digest it admitted.

- **The first tokens** are asked again while the answer says asking again
  may help, by the one retry rule below, within `orker.identity.setup_budget`
  (2 minutes); past it the attempt fails with the last answer's reason. An
  answer that ends the attempt (`IDENTITY_ATTEMPTS_EXHAUSTED`,
  `IDENTITY_ATTEMPT_EXPIRED`, `TASK_NOT_AS_ADMITTED`, `IDENTITY_SPEC_SKEW`,
  `IDENTITY_REVOKED`) is never asked again.
- **A refresh** lands at a random point between a third of the token's life
  and a quarter of its life later than that, so the items of a fan-out do
  not refresh together, and never earlier than a third. A refresh that
  could not ask, a failed local write included, is retried by the one rule;
  what is on disk stays until it expires. `IDENTITY_REVOKED` stops a
  `required` identity's container at once and ends the refreshing of a
  `best_effort` one.
- **The one retry rule:** the n-th delay after a failure is drawn between
  half of `b` and `b`, where `b` is `orker.identity.retry_min` (5 s) doubled
  n times and capped at a minute: 5–10 s, 10–20, 20–40, then 30–60.
- **The watchdog** reads, every 5 s, both the boot clock since the request
  was sent (which runs on while a host is suspended) and the wall clock
  against the engine's `issued_at`, and fires at the earlier. A `required`
  identity whose token expired unrefreshed stops the container, SIGTERM
  then SIGKILL after the grace, and the attempt fails
  `IDENTITY_REFRESH_FAILED`; a `best_effort` one writes one `[ork]` line and
  the task runs on. A replaced token stays masked until it has expired plus
  `orker.log.mask_grace` (5 minutes).
- **The clock check runs at every mint.** An `issued_at` more than 30 s from
  the Orker's clock is logged; more than `orker.identity.max_clock_skew`
  (2 minutes) fails that attempt `IDENTITY_CLOCK_SKEW`, writes no token and
  is not asked again in the attempt; it is retried only under the task's
  own `retry:`. Three such mints in a row clear the Orker's identity support
  (`DescribeOrker` says why), so no identity task is sent to it; while
  unfit it asks the engine every 2 minutes instead of 5 and reads the
  `Date` header of the answer, and one within `max_clock_skew` (plus a
  second, the header's resolution) restores support. A missing or
  unreadable `Date` is no evidence. Behind a proxy that sets its own
  `Date`, the header is the proxy's clock: reach the engine directly, or
  keep the proxy's clock in sync.
- **While the attempt runs**, its heartbeat says of each identity when what
  is on disk was issued, when it expires, and whether it is `fresh`,
  `retrying` (a refresh failed since) or `expired`; never a token, a `jti`
  or a path, at most 16 entries. The run page and `orkctl run inspect` show
  it; for an attempt whose Orker says nothing they show what the records of
  what was issued imply.
- **The attempt's record** (`TaskAttempt.identities`) says, for each
  identity, its audience, how many tokens the attempt was given, the first
  and last `jti`, the last `kid` and when the last expires; never a token.
  It is best-effort, as the record is: an Orker that is killed writes none,
  and [What was issued](#what-was-issued) is the engine's own trail.
- **Where the files live** is chosen at boot (`orker.identity.dir`): under
  `auto`, the first of `$XDG_RUNTIME_DIR/orkstack` and
  `/dev/shm/orkstack-<uid>` that is memory-backed, the Orker's own at
  `0700` and readable from a probe container, else the work root, logged at
  warning. `DescribeOrker` reports `identity_storage` as `memory` or `disk`.
  A boot removes what an earlier process under the same Orker id left
  there. Memory-backed storage can still be written to swap: encrypt or
  disable swap on Orker hosts.

## What was issued

Each granted mint writes one record of what it issued, beside the run's
saved state, before any token is returned (`identity.issuance.records:
required`, the default): the task, item and attempt, the calling Orker and
principal, and for each token its name, audience, subject, `jti`, `kid`
and lifetime. No token is ever recorded. `orkctl run tokens RUN [-t TASK]
[--item N]` lists them in the order they were issued (`ListIssuedTokens`,
which needs `run.read`). While an attempt runs, `orkctl run inspect` shows
each identity it holds: when it was last minted and when that expires,
`FRESH` until then and `EXPIRED` after.

## Which run a token is for

A token is for one attempt of one task of one admitted run. A run is
admitted when it, or any run of its tree, holds an identity: before its
idempotency key is claimed and before it is started, the engine writes the
run's admission record,
`metadata/snapshots/{ns}/{run-id}/identity/admission-{sha256}.json`, under
the run id it is about to start, and one for each child run the tree
composes, each naming its parent's run and record. A record holds what each
task that runs a container may hold (its pool, its spec digest, and for a
task that holds an identity its identities, the attempts that may hold them
and each attempt's deadline) and who may start the run: an engine itself, a
schedule's fire, or the parent run. It is named by the SHA-256 of its own
bytes, and a 128-bit random nonce in it keeps that name secret until the run
starts: the run's plan carries the name, and nothing else does. A write that
fails refuses the submission, unavailable, with no key claimed; a submission
that loses its key to another deletes what it wrote. A task whose attempts,
`max(1, retry.maximum_attempts)` plus `attempts_extra`, exceed
`identity.attempts.max` is refused `IDENTITY_ATTEMPTS_OVER_LIMIT` before
anything is written. Before it signs, the engine checks:

- that the attempt is the cluster's current one for that task, held by the
  calling Orker;
- that the run is the one its record was written for;
- that the task is as it was admitted: its spec digest, one projection of
  the fields the engine copies unchanged from the definition into the
  task's input (`internal/task`, `IdentitySpecDigest`), is the one the
  record holds. It binds the pinned image, the command, `env`, `with`, the
  secrets, the network mode, `run_as_orker_user`, the identities, the
  credentials, the grants and the device profiles the task names. It does
  not bind the run's input, what a task reads from its needs, its pool or
  its resources.

A run an engine started itself is tied to its record by
`metadata/snapshots/{ns}/{run-id}/identity/chain.json`, which names the
first execution of the run's chain and the record's digest. The engine that
starts the run writes it from the start's answer. If that write is lost, it
is written when the run carries on into a new generation, after that
switch's own objects are safe and without ever failing it, or by the first
mint that needs it: only for an execution with no parent whose first
start's input names the record's digest. It is create-only: once
it is there, no other execution of that run id fits it.

**What is left** (accepted in the task identity design). After the run
starts, its record's digest is in the cluster's history, readable by
anyone who can read the cluster. Whoever holds the cluster's write access
could start a second execution under the run id, with the same input, once
the real one has ended. That execution would be taken for the run if no
engine had yet written `chain.json` for it: not the starting engine, not a
generation change and not a mint. The window is from the run's start until the first of those writes.
It is open only to a holder of the cluster's write access, who can already
drive the real run. For a run a schedule fires, the digest is also in the
fire's own history from the moment the fire prepared the run; a start under
that id must still be a child of exactly that fire's execution, so the same
holder is the only one who could use it (accepted, 2026-11-20). An approval a task follows will be guarded separately:
no approval will be applied, and no task's identity granted, without the
engine's own record of it.

## What a task follows

In a run whose tree holds an identity, each task that runs a container, is
awaited or is composed **follows** the tasks of its run whose record of
success it needs before it starts: under the default join, each task it
names in `needs:` that leaves a record (a container task that is not a
fan-out, an awaited task, a composed task); under `none_failed`, only such
needs that always run (no `condition:`, the default join, and needs that
always run); under `any_failed` or `all_done`, nothing. Through a sleeping
task or a fan-out, which leave no record of their own, a task follows what
they follow. A record stands for the chain behind it, so an approval three
tasks upstream is followed without being named.

`orkctl run plan` shows what each task follows. Creating and validating an
Orkflow warn of a task that holds an identity and does not follow an
approval or a composed task upstream of it (under its join, or through a
need that may be skipped), and of one directly behind a sleeping task: a
sleep has no record, so it does not hold the identity back.

## Administering the issuer

| Command | Does |
|---|---|
| `orkctl admin identity issuer get` | the issuer URL, the subject format and trust domain, when this engine last read the keys and what the issuer URL served |
| `orkctl admin identity issuer export --out DIR` | writes `.well-known/openid-configuration` and `jwks.json`, and `spiffe-bundle.json` with a trust domain set |
| `orkctl admin identity keys list` | every key: its id, algorithm, when it was published and activated, whether it is retired, whether this engine would sign with it and why not, and the engines that hold it |
| `orkctl admin identity keys retire [--] KID` | retires a key: no engine signs with it again, and it is no longer served. A key id may begin with `-`; give it after `--`, with every flag before the `--` |

The reads need `identity.read` at the deployment. Retiring needs
`identity.keys.retire`, which only `admin` holds
([ACCESS.md](./ACCESS.md)). With task identity off, each command says so.
