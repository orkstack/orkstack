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

## Which run a token is for

No token is issued yet. When tokens are, each will be for one attempt of
one task of one admitted run. Submit will write the run's admission record,
`metadata/snapshots/{ns}/{run-id}/identity/admission-{sha256}.json`, before
the run starts. It will hold what the run's tasks may hold and who may start the
run. It is to be named by the SHA-256 of its own bytes, and a 128-bit random nonce
in it keeps that name secret until the run starts. Before it signs, the
engine will check:

- that the attempt is the cluster's current one for that task, held by the
  calling Orker;
- that the run is the one its record was written for;
- that the task is as it was admitted.

A run an engine started itself will be tied to its record by
`metadata/snapshots/{ns}/{run-id}/identity/chain.json`, which names the
first execution of the run's chain and the record's digest. The engine that
starts the run will write it from the start's answer. If that write is lost, any
engine that later needs it will write it. That will happen when a run carries on
into a new generation, when a sealed run writes its first snapshot, and at
a mint. It will be written only for an execution with no parent whose first
start's input names the record's digest. It will be create-only: once it is
there, no other execution of that run id fits it.

**What is left** (accepted in the task identity design). After the run
starts, its record's digest is in the cluster's history, readable by
anyone who can read the cluster. Whoever holds the cluster's write access
could start a second execution under the run id, with the same input, once
the real one has ended. That execution would be taken for the run if no
engine had yet written `chain.json` for it: not the starting engine, not a
generation change and not a mint. The window is from the run's start until
the first of those writes. It is open only to a holder of the cluster's
write access, who can already drive the real run. Keep `chain.json`'s
writes healthy: an engine will count each run whose record a second generation
change also left unwritten (`ork.identity.start_record_incomplete`). An
approval a task follows will be guarded separately: no approval will be
applied, and no task's identity granted, without the engine's own record of it.

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
