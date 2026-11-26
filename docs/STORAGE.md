# Storage — what OrkStack persists, and where

OrkStack has two durable stores and no third:

- **Temporal** holds every live run — workflow state, task state, the event
  stream — plus idempotency claims, Worker Deployment versions and routing,
  and every Orkflow schedule (its coordinates, spec and fire history,
  `docs/ORKFLOW_SCHEDULES.md`). It is the read model for a run until the run
  is retired at its run retention (`docs/DEPLOY.md` §5, "Run retention").
- **Object storage** holds everything that must outlive a process or a
  retention window: Orkflow definitions, the namespace registry, deployment
  settings, artifacts, task logs, run snapshots, activity completion and
  attempt records, the audit trail, webhook delivery records, cache memos,
  and the archive of finished runs.

Nothing durable lives in an `ork` or `orker` process. There is no SQL
database, no embedded store, and no OrkStack-owned counter (AGENTS.md §13).

## Backends

`storage.backend` / `ORK_ARTIFACT_BACKEND` selects `local` (a directory,
`ORK_ARTIFACT_DIR`) or `s3` (any S3-compatible store with conditional
`PutObject`); the keys are in `docs/DEPLOY.md` §1. A replicated deployment
needs `s3`: on `local` each replica archives runs to its own disk and each
Orker writes artifacts nobody else can read. The engine reads Orkflow
definitions from S3 whatever the backend says.

The local backend mirrors the S3 key layout as directories under
`ORK_ARTIFACT_DIR`, with two private additions: `.metadata/` sidecars carrying
an object's attributes, and `.tmp/` for in-flight writes.

## Key layout

One bucket, one key family per kind of record. The builders in
`pkg/artifacts` are the single place each shape is spelled.

| Key | Holds | Written | Removed by |
|---|---|---|---|
| `metadata/orkflows/{ns}/{name}/{version}.json` | one immutable Orkflow version, source YAML included (`OrkflowKey`) | conditional create; a divergent rewrite is `ErrVersionExists` (the comparison covers the whole source, header included; any difference is a rewrite) | `orkctl orkflow delete`, which removes every version, both pointers and every derived claim after checking for recent runs |
| `metadata/orkflows/{ns}/{name}/@current.json` | `{version, updated_at}`: the version new runs execute; absent means the head (`OrkflowCurrentKey`) | compare-and-swap; a promotion swaps it only while it still names the version the promotion compared the target's declared interface against (`CurrentExpectation`), so a promotion that raced another is refused, never landed over it | the same delete |
| `metadata/orkflows/{ns}/{name}/@head.json` | `{version, applied_at, since}`: the newest created version; moving it is the create's commit point (`OrkflowHeadKey`). `since`, optional, is the instant this incarnation of the orkflow began: the `applied_at` of the apply that swapped a head in from nothing, copied by every swap after it | compare-and-swap | the same delete |
| `metadata/orkflows/{ns}/{name}/@derived-{contentHash}.json` | `{version}`: the version a document that pins none was given, keyed by `sha256(source ‖ 0x1F ‖ canonicalImages)` (`OrkflowDerivedKey`). A record, never a lock: a create reads it to stay a no-op, and a claim whose version never landed is stored under by the next create of that content. A version stored under the hash itself by an earlier release needs none | conditional create, so concurrent creates of one new document on any replica get one version; a claim that does not decode is replaced by compare-and-swap | the same delete |
| `metadata/namespaces/{name}.json` | the namespace registry entry, every field of `Namespace` (`NamespaceKey`); `default` is synthesized when absent; a listing reads the names from the keys alone (`ListNames`, sorted by name, not key order) and then only the documents it needs: the page's, when every filter is on the name | conditional PUT with an etag | `orkctl namespace delete`, refused while the namespace holds anything |
| `metadata/settings/retention-policy.json` | the deployment's maximum run retention (`RetentionPolicyKey`) | create-if-absent at first boot, then plain put by `UpdateRetentionPolicy` | nothing |
| `metadata/settings/image-policy.json` | the deployment image supply-chain floor, signing keys as public PEM (`ImagePolicyKey`) | plain put by `UpdateImagePolicy`; absent until an operator writes one | nothing |
| `metadata/triggers/{id}.json` | one inbound run starter: namespace, orkflow, pinned version, description, optional `name`, its binder and consent, and the salted digest of its token — never the token (`triggerKey`); flat, because the invoke URL carries only the id | conditional create; rewritten only by `UpdateTrigger`, through a conditional put on the ETag it read (name, pin, description and binder change; the id and the digest never do) | `orkctl trigger delete` |
| `metadata/event-sources/{ns}/{name}.json` | one event source: its document as written, `{"format":1,"eventSource":…}`, with its id, counters, pause, binder and consent; no credential and nothing about an Orker. A document of another format, or with a field this release does not read, is a newer release's and is not read ([EVENT_SOURCES.md](EVENT_SOURCES.md)) | conditional create, which keeps a name unique in its namespace; rewritten only through a conditional put on the ETag it read | `orkctl event-source delete`, after the source's specs |
| `metadata/event-source-specs/{evs-id}/{sha256}.json` | one service spec of a source: what its containers are started with, immutable, named by the source's id and its digest, never by its name | create-only, by the write that raised the source's service revision | the source's delete, for its own id only |
| `metadata/receipts/{ns}/{evs-id}/{yyyy}/{mm}/{dd}/{stamp}-{delivery:04}-{epoch}-{lane}-{seq}-{outcome}-{sha256(identity)[:16]}.json` | one settled message delivery: what was decided about it, at most 16 KiB, never its body, an attribute value, a credential or a broker's error text. The source comes first, so one source's receipts are one prefix; the stamp and date are the instant the Orker took the message, fixed for the exchange, so a retried write lands on the same object and two deliveries of one message are two. A record, never read for a decision | create-only by the Orker hosting the instance, before the verdict; "already exists" is success | a bucket lifecycle rule only |
| `metadata/staging/event-sources/{ns}/{evs-id}/{sha256(identity)}/{sha256}` | one bound part of a message on its way to a run, keyed by the message and then its content, belonging to no run | create-only by the Orker, between the message's offer and its start; bytes that do not match the key's digest are refused. The engine copies it inside the store to the run's own artifact and reads that copy back: it is referenced only once its own bytes hash to the digest, whatever the store answered, and a good copy already there is not made again. What is left is a staged object changed under its key while two engines copy it at once: the second copy can put its bytes where the first had checked good ones. Every Orker hashes an input it stages, so the task that reads it then fails `INPUT_ARTIFACT_CORRUPT`, explicitly, and never runs on other bytes | best-effort by the engine once the message's run has started; a bucket lifecycle rule of a few days for what a start that never completed left |
| `metadata/message-claims/{sha256(ns ‖ 0x1F ‖ source name ‖ 0x1F ‖ orkflow)}/{sha256(identity)}.json` | one message's claim: its identity, the run id it is bound to, when, and a nonce. Every delivery of the message starts, or finds, that one run; nothing is decided from the claim's age | create-only by the engine, after the message's admission passed and before its run starts; never rewritten | a bucket lifecycle rule only, no shorter than the broker's own retention: a delivery after its claim expired starts a second run |
| the same with the run id, `…/{sha256(identity)}.{run-id}.started.json` | that the claim's run exists, and since when. The run id is in the key, so a marker left by an earlier claim of the message is never read. One that cannot be read is as good as none: the engine asks whether the run exists | create-only by the engine once the run has started, or when a later delivery finds it | the same rule |
| `metadata/event-source-confirms/{evs-id}/{index:05}-{epoch:012}-{attempt:010}` | that this attempt of an instance confirmed with an engine: the Orker its credential named, when, and the service spec's digest and admission save the instance was scheduled with. Every later call of the attempt is held to that digest | create-only by the engine, at the attempt's first accepted confirmation; never rewritten | the source's delete, for its own id; a bucket lifecycle rule of five months or more, longer than any attempt lives: one removed under a running attempt is written again at its next confirmation |
| `metadata/event-source-tombstones/{evs-id}.json` | that the source was deleted. The key's being there is what is read, whatever the object holds | create-only by the engine, with the delete | nothing in OrkStack; a bucket lifecycle rule may expire one, no sooner than thirty days after it was written |
| `metadata/logs/{evs-id}/_source/{index:05}/{epoch:012}-{attempt:010}/{seq:020}` | one chunk of an event-source instance attempt's stdout and stderr, a rolling window | the Orker, while the attempt runs | the source's delete, for its own id only; a lifecycle rule before that |
| `metadata/attempts/{evs-id}/_source/{index:05}/{epoch:012}-{attempt:010}.json` | one attempt of one event-source instance | the Orker, at the end of the attempt, after its logs, best-effort | the same |
| `metadata/schedules/{namespace}/{schedule-id}.json` | who bound one schedule and the consent they gave, beside each submission the API accepted, at most four ([ORKFLOW_SCHEDULES.md](ORKFLOW_SCHEDULES.md#who-a-schedule-runs-as)); a record the API keeps, never the schedule's state, which is the cluster's | rewritten by every `CreateSchedule` and `UpdateSchedule` that binds or trims, through a conditional put on the ETag it read, five tries | emptied, then removed, by `DeleteSchedule`; what is left of a namespace by `DeleteNamespace` |
| `metadata/webhooks/{id}.json` | one subscription: URL, events, scope, optional `name`, and the signing secret, which no read but delivery returns (`webhookKey`) | conditional create; rewritten only by `UpdateWebhook`, through a conditional put on the ETag it read (name, URL, events and scope change; the id and the secret never do) | `orkctl webhook delete` |
| `metadata/runs/{run-id}.jsonl` | an archived run: the run record then every event, one JSON line each (`RunArchiveKey`); a run whose stream has a gap — events lost from object storage before it was archived (an earlier history's event segment missing or altered), or numbers it used without publishing an event, because object storage refused that event's record (`docs/DEPLOY.md` §6) — has one `{"kind":"gap","gap":{"first":…,"last":…,"reason":…}}` line per range, in sequence order among the events — reads over that range fail `DataLoss` naming it and its cause, and its evidence bundle names it in `event_gaps` (`docs/RUN_EVIDENCE.md`) | once, at the run's terminal event; read verbatim as `run.jsonl` by `ExportRunEvidence` (`docs/RUN_EVIDENCE.md`) | nothing |
| `metadata/index/runs/{ns}/{orkflow}/{status}/{stamp}/{run-id}` | zero bytes; the key is the index row, stamped by `created_at` (`RunIndexKey`). A child run of another namespace's shared Orkflow is indexed under `{ns}` it ran in and the segment `<owner>/<name>`, escaped like any segment that is not a clean component (`engine.OrkflowKey`), so it never lists as the namespace's own Orkflow of that name | with the archive body | nothing |
| `metadata/index/roots/{ns}/{root-run-id}/{parent-run-id}/{orkflow}/{status}/{stamp}/{run-id}` | zero bytes; the root index row (`RunRootIndexKey`), one for each run with a root of a composed chain recorded; read keys-only by a `ListRuns` filtered on `origin.root_run_id`. The parent is in the key because every run with a root is a `PARENT_RUN`, so the row answers `origin.kind` and `origin.id` too. | with the archive body, only for a run with a root recorded, before its run-index row | nothing |
| `metadata/index/origins/{ns}/{kind}/{origin-id}/{orkflow}/{status}/{stamp}/{run-id}` | zero bytes; the origin index row (`RunOriginIndexKey`): `kind` is `submit`, `trigger`, `schedule`, `parent-run`, `run-again` or `event-source`, `origin-id` the originating object's id or `none`; read keys-only by an origin-filtered `ListRuns` | with the archive body, only for a run with a recorded origin, before its run-index row | nothing |
| `metadata/index/recent/{ns}/{inverted-stamp}/{orkflow}/{status}/{run-id}` | zero bytes; the newest-first index row (`RunRecentIndexKey`): `inverted-stamp` is the `created_at` stamp with every digit `d` written `9-d`, so ascending key order is newest first; read keys-only by a `ListRuns` page, which walks it from the page cursor and stops once the page and its last instant's ties are read | with the archive body, before its run-index row | nothing |
| `metadata/index/objects/{sha256(object-id)}/{run-id}` | zero bytes; one marker per artifact object the run references (`RunObjectRefKey`) | with the archive body | cleanup at retirement when the artifact GC is `off`; otherwise the artifact GC, once it has decided every object the run names (see "Reclaiming space") |
| `metadata/index/run-objects/{run-id}.json` | the object ids the run references (`RunObjectListKey`): the union of every list an archive write of the run wrote, never shrunk by a later write | with the archive body, before it | as its markers |
| `metadata/cleanup/runs/{run-id}.json` | the sidecar recording that the run was retired and cleaned (`RunCleanupKey`); its store `LastModified` is when the run was retired, on the store's clock, which the artifact GC reads as the latest instant that stamp can stand for (a whole-second stamp as the end of its second) against a lower bound of the store's now, so a retirement never reads older than it is | once, by cleanup | nothing |
| `metadata/cleanup/objects/{run-id}.json` | the GC record: the artifact GC finished with the run, its objects deleted or kept for good (`ReclaimRecordKey`), with `late_commit` and the ids of the objects kept for good | create-only, by the artifact GC in `delete` mode | nothing |
| `metadata/cleanup/gc-clock` | a probe whose store-assigned modification time tells each engine its clock skew against the store (`ork.store.clock_skew`), and the artifact GC the store's now (`StoreClockKey`) | overwritten by every engine, once per cleanup interval, and by the artifact GC once a pass | nothing. It is read only by the engine that just wrote it, though the stamp it reads may be another engine's later write; the GC reads it as a lower bound of the store's now, a second below the stamp, so a later stamp or one rounded up never makes a retirement read older than it is |
| `metadata/snapshots/{ns}/{run-id}/{sequence}.pb` | the run snapshot behind a projected event, kept out of Temporal history (`RunSnapshotKey`); a fan-out's item events between its stride snapshots carry none, and on a run that records the `lean-event-path` version (`docs/DEPLOY.md` §6) a plain or fan-out task's `TASK_STARTED` publishes with its `TASK_SCHEDULED`'s object, since nothing changes between them | by the workflow's snapshot local activity | cleanup |
| `metadata/snapshots/{ns}/{run-id}/{sequence}.ovl` | a snapshot overlay (`RunSnapshotOverlayKey`): for an event of a run that writes overlays (`docs/DEPLOY.md` §1, `engine.snapshot_overlays`), a `RunSnapshotOverlay` holding the run's header and the task and item rows that differ from a full `{sequence}.pb` of the same run, which it names by URI and by the SHA-256 of the bytes it was written against; never another overlay. A reader verifies the base against that digest and replaces its rows by key — at most two reads; a base gone or changed is an error, never a mixed answer. A run's last event's snapshot is never an overlay | by the run's snapshot local activity, plain put, like a `.pb` | cleanup, with the run's snapshot prefix. **Never expire a `{sequence}.pb` with a lifecycle rule while its run lives**: overlays name it |
| `metadata/run-context/{ns}/{root-run-id}/{sha256}.json` | a run tree's run context (`RunContextKey`): the root's Orkflow and origin, and each composed child's entry (its parent, the composed task that started it, its Orkflow, and whether that Orkflow is another namespace's or the run is below one that is), in a pre-order walk of the tree. A run so marked reads a copy of the root's origin without its principal and reference. It names no run id of the tree: a task's input carries its run's ids and times beside the object's SHA-256 (`ExecuteTaskInput.run_context`). Content-addressed by the SHA-256 of its bytes, verified by every reader, and only the exact bytes its encoder writes are an object | create-if-absent by the submitting engine immediately before each start of the tree's root (a reissued run id gets its own copy, the same bytes), or by a schedule fire's preparation as its last step; never by a child. An Orker gets it, by the key its task's input names and with no listing, verifies it against its SHA-256 and keeps it on its disk for the process's life, for each attempt of a task that is not cached. The prefix is apart from `metadata/snapshots/`, which holds a run's saved state and admission and is closed to an Orker's credential (`docs/DEPLOY.md`, "An Orker's own storage credential") | cleanup, with the root run. **Never expire it with a lifecycle rule**: every task of a live tree reads it; a child ends before its root. A copy written under a run id the cluster refused as taken, or by an engine that died before its start, belongs to no run that started, as a `definition` object written then does |
| `metadata/snapshots/{ns}/{run-id}/resume-plans/{scope:020}.{n:03}.json` | one resume plan's immutable record (`ResumePlanKey`, a deterministic protojson `ResumePlanRecord`): the plan a resume of the run confirmed, which the reset that resumes the run names by the SHA-256 of these stored bytes, and which the reset run loads and verifies before it does anything. Its units, every task and item of the run, are packed (`ResumeUnits`) to at most 96 KiB of binary encoding, a third more as the record's base64 | create-only, before the reset; the same bytes again succeed, other bytes are refused | cleanup, with the run's snapshot prefix |
| `metadata/snapshots/{ns}/{run-id}/continuations/{generation}-{checkpoint\|events\|snapshot\|definition}-{sha256}.pb.gz` | what a run writes when it carries on into a fresh history (`RunContinuationKey`; the generation is six or more digits): its saved state at the switch (`checkpoint`, gzip members of `RunCheckpointPart`), the gzipped full `Run` at the switch (`snapshot`), or the gzipped `RunEventSegment` holding the events of the history that closed (`events`); and, for a run whose planned definition is too large to travel with its start (`docs/DEPLOY.md` §1, `engine.definition_by_reference`), that definition (`definition`, always generation `000000`: gzip members of `Orkflow`, part 0 the definition with its tasks cleared, then parts of tasks only), which every generation of the run copies into its own history before it decides anything. Content-addressed by the SHA-256 of the stored bytes, and verified against it by every reader; a definition is found by its run id and the digest its plan records, never by a stored location | create-if-absent by the run's local activities at a switch; a `definition`, by the submitting engine immediately before each start that uses it (a reissued run id gets its own copy), or by a schedule fire's preparation as its last step. An object already at the key is accepted only when its bytes hash the same | cleanup, with the run's snapshot prefix. **Never expire it with a lifecycle rule**: a live run reads its segments for its whole life, and its archive is built from them. Three kinds of `definition` object belong to no run that started: one written by an engine that died before its start, one under a run id the cluster refused as taken, and one written by a fire preparation retried after its write; each is at most a quarter of `engine.continuation.bytes` and never read. The second lies under the prefix of the run that held the id, so when that run is of the same namespace its cleanup reclaims it; every other one is left for an operator's sweep, which finds it by a run id `orkctl run get` answers `NotFound` for (`docs/DEPLOY.md` §5) |
| `metadata/snapshots/{ns}/{run-id}/identity/admission-{sha256}.json` | a run's admission record, for every run of a tree in which any run holds an identity (`AdmissionKey`; [TASK_IDENTITY.md](./TASK_IDENTITY.md#which-run-a-token-is-for)): what each task that runs a container may hold, who may start the run, and a 128-bit nonce, named by the SHA-256 of its own bytes, which the run's plan carries. Never execution state: its presence says nothing of whether the run lives | create-only, by the submitting engine under the run id it is about to claim or start, before the claim; by a schedule fire's preparation before it hands the run to the fire. A submission that ends before any start was attempted deletes what it created | cleanup, with the run's snapshot prefix. One under an id no run ever held is left for an operator's sweep, as an unread `definition` object is |
| `metadata/snapshots/{ns}/{run-id}/identity/chain.json` | a root run's chain record: the first execution of its chain and the digest of the admission it was started with (`ChainKey`). Only for a run an engine started itself; never for a child or a fired run | create-only, by the engine that started the run from its start's answer, or, from the first execution's own start, at the run's next switch to a new generation or by the first mint that needs it | cleanup, with the run's snapshot prefix |
| `metadata/logs/{run-id}/{task-id}/[{item:05}/]{attempt}/{seq}` (and `.trunc`) | live task stdout/stderr chunks under the attempt's ordinal (see "Attempt ordinals" below); order is the zero-padded key (`LogChunkKey`, `LogScopePrefix`) | by the Orker while the attempt runs | cleanup, or a bucket lifecycle rule |
| `metadata/attempts/{run-id}/{task-id}/[{item:05}/]{attempt}.json` | one `TaskAttempt`: outcome, error, exit code, the phase it ended in and every phase it passed through (`docs/OCI_ACTIVITY_CONTRACT.md` §7.1), the image it ran, the cpu and memory the Orker reserved, and what its container used as its cgroup reported it (`measured`, or why nothing was measured) (`AttemptKey`, `AttemptScopePrefix`); keyed by the attempt's ordinal (see "Attempt ordinals" below) | by the Orker at the end of every attempt, best-effort, plain put; read verbatim as `attempts/{n}.json` by `ExportRunEvidence` | cleanup, or a bucket lifecycle rule |
| `metadata/attempts/{run-id}/{task-id}/[{item:05}/]{attempt}.claim` and `{attempt}.released` | an attempt's claim on its ordinal and its release marker: JSON naming the Orker, the run, task, item and ordinal (`AttemptClaimKey`, `AttemptReleasedKey`). Protocol objects, like a commit; nothing decides from them but the next ordinal | the claim conditional create-if-absent, by the Orker before the attempt's container; the marker conditional create-if-absent, best-effort, once the attempt's container is removed or it ended after its claim without one, or by the boot reap for a container it removed | cleanup, with the run's attempt records |
| `metadata/commits/{ns}/{run-id}/{task-id}/[r{scope:020}/][{item:05}/]commit.json` | the immutable activity completion record of a task, or of one fan-out item (`CommitKey`, `ItemCommitKey`). A task a resume runs again commits under the resume's scope, the `r{scope}` segment (`ScopedCommitKey`, `ScopedItemCommitKey`), so the result it had before is never altered; a run never resumed writes no such segment. A dependent of a fan-out reads every item's to stage its files, each under the scope its input names for that item (`EachUpstream.epochs`) | conditional create-if-absent; read verbatim as `commit.json` by `ExportRunEvidence`, only at the `CommitRef.uri` the run snapshot recorded | cleanup |
| `metadata/attested/{ns}/{run-id}/{task-id}/[{item:05}/]commit.json` | a result the engine recorded, for a task or one item of a run that holds a task identity anywhere in its run tree: the `ActivityCommit` the attempt's Orker handed it, with `recorded` (pool, Orker id, admission digest, engine build, time) set by the engine (`AttestedCommitKey`, the one mapping from the commit key; `docs/TASK_IDENTITY.md`, "Results the engine records"). An Orker's credential may get and list here, never put or delete | conditional create-if-absent, by the engine only, on `AttemptService.CommitResult`; one already there is answered as it is | cleanup, with the run's commits |
| `metadata/attested/{ns}/{run-id}/{task-id}/[r{scope:020}/]{resolution,composed}.json`, `metadata/attested/{ns}/{run-id}/outputs.json` | a `RecordedValues` of such a run: an await's resolution, a composed task's values, or a child run's outputs, with what each follows (`docs/TASK_IDENTITY.md`). An Orker's credential may get and list here, never put or delete | conditional create-if-absent, by the engine only; one already there stands | cleanup, with the run's commits |
| `metadata/memo/{ns}/orkflow/{orkflow}/{task-id}/{key}.json` | one orkflow-scoped `CacheMemo`: a pointer from a cached task's 64-hex memo key to a `SUCCEEDED` commit (`MemoKey`; `docs/DSL.md` "Caching") | by the Orker after a fresh commit of a cached task, best-effort, plain put, last writer wins | `orkctl cache purge`, or a bucket lifecycle rule |
| `metadata/memo/{ns}/namespace/{task-id}/{key}.json` | one namespace-scoped `CacheMemo` | same | same |
| (either memo prefix, for a shared Orkflow) | a task of a shared Orkflow composed from another namespace keeps its memos under the namespace it ran in and its Orkflow's name, with the owner in the key, so they are never another Orkflow's of that name (`docs/DSL.md` "What the key covers") | same | same |
| `metadata/audit/{yyyy}/{mm}/{dd}/{id}.json` | one control-plane audit record: who made which write, on what, when, and whether it was refused, and with authentication on the decision and each denial (`AuditKey`); the day is the record's own `at` in UTC | create-only, by the engine after every audited write RPC and every recorded denial | a bucket lifecycle rule only |
| `metadata/identity/access.json` | the access document: custom roles, bindings, service accounts and disabled principals, with its revision and the SHA-256 of the revision before it (`AccessDocumentKey`; [ACCESS.md](./ACCESS.md)) | compare-and-swap on its etag, by an engine, after its revision is written | nothing |
| `metadata/identity/access-revisions/{revision:012}-{sha256}.json` | one immutable revision of the access document, the same bytes the head held (`AccessRevisionKey`); a record nothing decides from | create-only, before the head moves | a bucket lifecycle rule, if wanted |
| `metadata/identity/access-acks/{revision:012}-{sha256}.json` | one admin's acknowledgement of a revision written around the API: the revision, its digest, who acknowledged it and when (`AccessAckKey`; [ACCESS.md](./ACCESS.md), "Revisions, and a document written around the API"); decides whether a banner shows, never access | create-only, by `AcknowledgeAccessRevision`; a second one of the same revision keeps the first | nothing |
| `metadata/identity/mode.json` | the highest authentication mode the deployment runs, when it was raised and when it last left `enforce` (`ModeRecordKey`; [AUTHENTICATION.md](./AUTHENTICATION.md), "The mode record") | compare-and-swap: raised by an engine's boot, lowered only by `LowerAuthMode` or `ork --set-mode-record` | nothing: exclude `metadata/identity/` from every lifecycle rule |
| `metadata/identity/namespaces/{name}.json` | a namespace's immutable id (`ns_` and a UUIDv7 in lowercase base32), bound to the `created_at` of the namespace document it was minted for and ignored when that differs (`IncarnationStore`; [ACCESS.md](ACCESS.md), "Pinned bindings"); and, in its `policy` member, the namespace's identity policy (audiences, grants, `identities_current_only`, `attempts_extra`) with its generation and who changed it ([TASK_IDENTITY.md](TASK_IDENTITY.md), "A namespace's identity policy"). `default`'s id is the constant `ns_default`, never minted; its record exists only to hold its policy. Not in the namespace document, which an engine of an earlier release rewrites with unknown fields dropped | by an engine at `CreateNamespace`, or on first need for an older namespace, never by a read; replaced only when `created_at` differs. The policy by compare-and-swap on the record, after its revision is written | `DeleteNamespace`: the policy before the document, the id after it |
| `metadata/identity/namespaces/{name}/revisions/{generation:012}-{sha256}.json` | one immutable revision of a namespace's identity policy: the bytes a write proposed, written before its swap. A write that lost its race, or stopped before its swap, leaves one its record never held, so two may share a generation; the record is the policy. A record nothing decides from | create-only, before the record moves | a bucket lifecycle rule, if wanted |
| `metadata/identity/orkflows/{ns}/{name}.json` | an Orkflow's immutable id (`of_` and the same), bound to its head's `since` | by an engine when the Orkflow's first version is stored, or on first need; replaced only when `since` differs | `orkctl orkflow delete`, after the Orkflow |
| `metadata/identity/issuer/jwks.json` | the task identity issuer's public signing keys: each key's JWK and `kid`, when it was published and by which engine, when it began to sign (`activatedAt`), when it was retired and by whom, and the engines that confirmed holding it; with a sequence that counts its changes and is the SPIFFE bundle's (`PublishedKeysKey`; [TASK_IDENTITY.md](./TASK_IDENTITY.md)) | compare-and-swap: engines add their keys at boot and record an activation; only `RetireSigningKey` retires one. A key is never removed or changed | nothing |
| `metadata/deliveries/{yyyy}/{mm}/{dd}/{id}/{ns}/{run-id}/{webhook-id}/{event}/{delivered\|failed}.json` | one settled webhook delivery: every attempt, the signature sent, the body's SHA-256 — never the body (`DeliveryKey`); the day is the delivery id's stamp, and the segments after the id are the record's own coordinates, so every listing filter is decided from the key | create-only, by the engine when the delivery settles | a bucket lifecycle rule only |
| `metadata/delivery-bodies/{delivery-id}/{sha256}.json` | the signed body of one webhook delivery still being sent, so every attempt, on whichever engine replica, sends the bytes that were signed (`DeliveryBodyKey`); read back only by that delivery's attempts, verified against its SHA-256 | create-only, by the engine when it queues the delivery | the delivery's own last step deletes it; a delivery stopped from outside leaves it to a bucket lifecycle rule |
| `metadata/probes/commit-store/…/commit.json` | the probe proving conditional create works on this store | at every Orker boot | nothing |
| `metadata/probes/conditional-answers/{id}/{missing\|present}` | the probe asking the store's other conditional answers (If-Match on a missing key, a conditional delete); `{id}` is fresh for each boot | at every Orker and every engine boot | the probe itself, before the boot goes on; a killed process can leave the two small objects to a bucket lifecycle rule. On a bucket with versioning enabled, a delete keeps the object as a noncurrent version behind a delete marker ([AWS](https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectVersions.html)), so every boot leaves a noncurrent version and delete markers under this prefix; a lifecycle rule on it that expires noncurrent versions and expired delete markers reclaims them ([AWS](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-configuration-examples.html#lifecycle-config-conceptual-ex7)) |
| `metadata/probes/event-sources/{id}` | whether a message's claim could be written now, for an instance of an event source that waits; `{id}` is fresh for each | by an engine, at most once in ten seconds a replica while an instance waits | the probe itself, at once; a bucket lifecycle rule of a day for what a killed engine left |
| `orkflows/{orkflow}/{run-id}/{task-id}/{artifact}/{sha256}` | workload artifact bytes on the `s3` backend (`s3ObjectKey`) | by the Orker, content-addressed | the artifact GC in `delete` mode (see "Reclaiming space"), or a bucket lifecycle rule |
| `manifests/{object-key}` | the artifact's exact logical name, beside its object | with the object | with the object |
| `objects/n-{name}[/c-{…}]/{sha256}` | workload artifact bytes on the `local` backend (`objectKey`) | by the Orker, content-addressed | nothing automatic |

Run ids, namespaces and task ids pass through `encodeArchiveSegment`: a clean
slug is used verbatim, anything else is encoded and prefixed with `~`, and
every segment is bounded at 200 bytes. A run id is encoded, never parsed
(AGENTS.md §7). A basename beginning `@` is reserved for per-orkflow
pointers, so a pointer can never be read as a version.

## Invariants the layout carries

- **Creating an unchanged document again writes nothing, with three stated
  exceptions.** The derived claim is what finds a document's version, so each of
  these leaves the same content under two versions, and creating it again moves the head
  onto the newer one. Nothing is lost or rewritten, and a run keeps the version it
  pinned. A `Delete` that interleaves with a create can remove the claim while
  the create's version lands after it. A claim that no longer decodes is replaced,
  and the version it named cannot be known. While a replica of an earlier release
  runs beside a new one, as in a rolling upgrade, the old one stores the hash and
  the new one a minted version (`docs/UPGRADING.md`).
- **Bytes never travel through Temporal.** Events and activity inputs carry
  references into this layout; the 256 KiB payload ceiling (AGENTS.md §2.2)
  is what forces snapshots, logs and artifacts out here.
- **A durably committed result cannot become pending again.** `commit.json`
  is written with create-if-absent; an Orker retry that finds one returns it
  and never runs the container again. The boot probe refuses a store that
  cannot arbitrate that write.
- **Any replica answers for any run.** Every read of a finished run, a log, a
  definition, a namespace, a setting, an audit or delivery record goes to
  this layout or to Temporal, never to the memory of the process that wrote
  it.
- **Pointers, not copies.** `@current` and `@head` name a version and hold no
  copy of the document: a second copy is a second place to be wrong. Moving
  them is a compare-and-swap arbitrated by the store's conditional PUT, so
  two replicas promoting or creating at once need no lock, lease or Temporal
  state — the loser is told and swaps again against the winner's etag. The
  head is a pointer rather than "the greatest `created_at`" because each
  replica stamps from its own clock, and because an edit's
  `expected_head_version` must be checked by the write itself. `applied_at`
  never goes backwards, so `orkctl orkflow versions` lists versions in create
  order even when clocks disagree.
- **What the store must honour.** Beside conditional create and swap
  (If-None-Match, If-Match on PutObject), a deployment's S3-compatible store
  must answer a PutObject with If-Match on a key that does not exist with a
  refusal — 404 or 412, both read as the condition refused — and must hold a
  DeleteObject with If-Match to its condition: a wrong etag refused with 412
  and the object left in place. A store that ignored the delete condition would
  let a refused apply's rollback remove another document stored under the same
  version. Every Orker asks both at boot, after its commit probe, and refuses
  to start with `COMMIT_STORE_NOT_CONDITIONAL` on a store that gets either
  wrong; every engine, where rollbacks and promotions run, asks both before its
  API accepts an apply, and refuses to start naming the answer it got. SeaweedFS, the development store, answers 404 and honours the delete
  condition; `TestIntegration_S3_ConditionalWriteAnswers` pins both and fails
  on a store that does not.
- **A conditional write whose answer was lost is read back.** The S3 client
  resends a write whose answer it lost, and a resend of a conditional write
  meets its own first send when that send was stored: a 412, or the client
  gives up, for a write that landed. So when a conditional write's outcome is
  unknown — the client resent it, or its last answer was a 5xx or none — it
  reads its key (`putConfirmed`): a key holding exactly the bytes it sent is
  that write, landed; other bytes, or none, leave its error the answer; a read
  that fails leaves the outcome unknown, reported as such and never as a
  refusal. A 4xx to a write's only send is final: a 412 then is another
  writer's, even over equal bytes, so a refused write is never reported as
  landed. That holds while nothing between OrkStack and the store resends a
  request on its own (docs/DEPLOY.md). Equal bytes read back are this write, or
  are harmless whoever wrote them (a nanosecond stamp, a fresh id, content
  addressing), except in two places. The commit store's boot probe, whose two
  writers send one fixed candidate, never counts such a writer as the winner:
  beside the other writer's refused only send it has still proven arbitration,
  over its durable winner it was refused, and otherwise the probe fails as one
  that got no answer it can use. A store answers If-Match on a key that is
  gone (a Delete took it) with a 404, and every conditional write reads that
  as its condition refused, as a 412: the object is not the one it read (a
  head swap's head moved to none). A refused Orkflow apply removes the version
  it wrote, so a retargeted retry creates it afresh, but never one it does
  not know it wrote — two applies of one document behind the same head can be
  stamped alike, when neither caller's stamp is after the head's — nor one the
  head or the current pointer names, or whose pointers it cannot read; and
  since an identical apply can move the head, and a promotion of the listed
  version the current pointer, onto the version at any point, it reads both
  again afterwards and puts back the exact bytes of a version either names.
  An apply that moves the head onto a version it does not know it created
  checks that version is still stored as the document it read, and puts it
  back; a promotion reads its target back after its swap and, finding it
  gone while the current pointer is still its own write, puts back the bytes
  it read and stands. Each restore is created only if absent and held to its
  document, so whichever lands first the other finds there, and no pointer
  stays on a version that is not stored. A promotion whose pointer has moved
  since, or was taken by a Delete, puts nothing back and is refused as not
  found; one whose read-back finds another document under its target (the one
  it compared removed, and the version created again) is refused as such, and
  — a promotion never being an unchecked move — moves the current pointer back
  to what it named before (its exact bytes), or clears it when that version
  is no longer stored or there was none, while it is still its own write. **A promotion or apply landing inside a Delete
  loses to it.** A promotion whose swap meets a pointer a Delete took (a 404)
  is refused as the orkflow deleted, and never writes the pointer again from
  nothing, and so is an apply whose head swap meets a head a Delete took, which
  then rolls back its version; an apply that goes round again after a refused
  head swap is refused the same way when the head it now finds is gone, or of
  another incarnation (`since`, below), and otherwise reads its version back
  before it swaps again, never touching a head while its version is gone (an
  apply that does not know it created its version — it found it stored, or
  its create's answer was lost — goes on only when the head it now reads is of
  the incarnation it read a head pointer of before, and the version was
  created within it: the refused apply's rollback, which its swap undoes; it
  is otherwise refused saying it cannot tell a rollback from a Delete); one whose target is gone with no head at all — which no rollback
  leaves — removes its own pointer and is refused the same way; an apply that
  swapped a head in from nothing reads its version back and, finding it gone,
  removes its own head and is refused the same way (a Delete that stopped part
  way, before its versions, reads the same, and the refusal says so; deleting
  again finishes it — unless another apply has re-created the orkflow since,
  which the refusal says instead). After the versions and claims, a Delete reads both
  pointers once more and removes each one that names a version not stored,
  under the etag it read, so a pointer written while its versions were going
  does not outlive it; the pointers of an orkflow re-created meanwhile, with a
  version the Delete never scanned, name a stored version and stay. A
  promotion whose target is gone while a head is there tells a Delete from a
  refused apply's rollback by the head's `since`: a refused apply's version is
  always stamped after a head of its own incarnation, so a target created
  before `since` belongs to an orkflow a Delete removed, and the promotion
  removes its own pointer and is refused as the orkflow re-created, putting
  nothing back. A head without `since` — written before the field, or
  rewritten by an engine that predates it, whose swaps drop it, and which a
  newer engine's later swaps then copy as absent — tells nothing, and the
  promotion puts the target back as before the field. Two cases stay open.
  Clock skew across replicas: the `since` of an orkflow re-created after the
  Delete took all its versions is the re-creating apply's own clock, so a
  deleted version stamped after it by a replica whose clock ran ahead reads as
  this incarnation's, and a promotion of it racing the Delete puts it back: the
  deleted version is stored again and current. Two first applies racing a
  promotion: on an orkflow with no head pointer yet — brand new, from before
  head pointers, or after a Delete that stopped part way — the refused apply's
  version can be stamped before the winning apply's `since`; a promotion of it
  whose read-back follows the rollback then removes its own pointer and is
  refused as the orkflow re-created, which it was not, and a rollback whose
  re-read saw that pointer puts the version back: the version then stays
  stored and listed with no pointer naming it. The rollback removes a version only while it holds exactly the
  document the refused apply wrote, and the removal itself is held to the etag
  the rollback read (DeleteObject If-Match), so a Delete of the orkflow and a
  fresh create of that version with other content, landing before the read or
  between the read and the removal, is left alone (a pointer that cannot be read is reported, not guessed at, so no Delete
  is undone; a Delete landing between a re-read and its restore — the
  rollback's or a promotion's — can leave one version object behind, and the
  orkflow then reappears with that version until it is deleted again). (An
  identical apply that answered "already stored" with the head already past
  the version can still find it gone, as with any rollback: explicitly not
  found, never another version.) A version stored ahead of the head by an
  apply whose head swap never landed is completed by the next apply of that
  document, which moves the head onto it.
- **A `{seq}.pb` snapshot key belongs to the emit that owns its sequence.**
  It is written only by that emit, once per workflow-task attempt that runs
  it, or by the one terminal snapshot written for a sequence no emit owns (a
  run that could not carry on from its saved state). Nothing written when a
  run carries on into a fresh history ever lands on a `{seq}.pb` key: those
  objects are content-addressed under `continuations/`. The key is not "one
  content per key": a retried workflow task runs its emit again at a later
  workflow time and overwrites the key, and a failed attempt's write can land
  after the retry's. Only the attempt whose workflow task completes publishes
  the event, so every reader treats a `{seq}.pb` key as the snapshot of the
  emit that owns that sequence, as of one of its attempts, and compares
  decoded rows, never raw bytes.
  An overlay therefore never trusts a base by its key alone: it names the
  `{seq}.pb` it was written against by URI and SHA-256, and a reader that
  finds other bytes there refuses it. The same holds for a `{seq}.ovl` key,
  which belongs to the same emit.
- **A fan-out item has its own commit, attempt and log keys**, one
  zero-padded segment below its task's; artifact objects stay
  content-addressed and are shared between items that wrote the same bytes.
  Every item key stays under its run's prefix, so cleanup reclaims items with
  no rule of their own.
- **Attempt ordinals.** An Orker numbers the attempts of one scope (a task,
  or one item of a fan-out) itself, not with Temporal's attempt count. Once
  the attempt's first heartbeat is answered, and before it fetches any input
  or starts a container, it claims the next ordinal: one past every claim,
  marker and record in the scope, written create-if-absent, taking the next
  number when another attempt got there first. The attempt's record, its log
  prefix, its container's `ork.attempt` label, its commit and the result it
  reports all carry that ordinal, and so do its heartbeats once it is
  claimed. Ordinals run 1, 2, 3 in practice; a gap appears only when an
  object is lost, and readers tolerate one. An attempt that ends without a
  container (an adopted commit, a cache hit, a refusal) claims an ordinal
  best-effort to record its outcome under. An attempt stopped before its
  claim (a cancelled or terminated run, a lost heartbeat, a stopping Orker)
  claims none and leaves nothing. A claim the store will not take after four
  tries over about 10 s fails the attempt `ATTEMPT_STORE_UNAVAILABLE` with
  nothing made. Once the attempt holds nothing (its container removed, or
  none created), the Orker writes `{attempt}.released`; a claim with no
  marker is an attempt whose Orker may still hold its container. A claim
  stays unreleased, too, when its Orker died between the claim and the
  container's creation or removal, or when a create failed (it may have made a
  container the runtime did not name): the boot reap releases those it finds a
  container for, and the rest read as attempts whose outcome is unknown. A
  claim whose create answer was lost is released at once. A record
  written by a release that numbered attempts as Temporal does is passed
  over, never claimed again.
- **Every attempt is recorded, not just the winner.** Temporal discards a
  failed attempt's error when it schedules the next one, so the Orker writes
  one `TaskAttempt` per attempt beside that attempt's logs. The write is
  best-effort: a record that cannot be stored is logged and never changes
  what the attempt reported, and every reader of attempt records reports a
  missing one rather than assuming it clean.
- **Settings are documents, not live reads.** The retention maximum and the
  image policy are single documents under `metadata/settings/`, so any
  replica answers identically. A run pins the image policy (and the signing
  key) it was submitted under in its plan; a later change never reaches it.
- **A schedule is the Temporal Schedule.** Nothing about one is written here,
  so nothing here can disagree with the cluster about when it fires. The one
  trace a schedule leaves is its runs' origin-index rows
  (`metadata/index/origins/{ns}/schedule/{sch-…}/…`).
- **An awaited or composed task's `values.json` is written by the engine** —
  an await's when its resolution arrives, before the resolution is
  delivered; a composed task's when its child run succeeds (the child's
  declared outputs, stored for the PARENT run under the composed task's id).
  Both land in the same content-addressed family, with the same provenance
  metadata, as a container's. A child run needs no key family of its own: it
  is an ordinary run with its own snapshots, events, archive record and
  origin-index row (`…/parent-run/{parent}/…`).
- **No automation document is stored.** A document lives in Git;
  `PlanAutomation` reads it from the request and the namespace's live objects
  and writes nothing, and an apply writes only through the ordinary trigger,
  webhook, schedule and namespace RPCs. A trigger or webhook record carries
  an optional `name` and nothing else: no "managed-by" marker, no
  last-applied copy. A record written without a name reads as unnamed.
- **An update never recreates a delete.** Trigger and webhook records are
  rewritable by their own update only, with a conditional put naming the
  ETag the update read; a record deleted in between stays deleted, and the
  update reports NotFound.
- **A memo changes no result.** It is a pointer to a commit that already
  exists, written only after a fresh `commit.json` was created and read only
  to decide whether a container is needed; every doubt resolves to *run the
  task*. A hit writes its run's own `commit.json` whose references are the
  source's content-addressed objects — no byte is copied. A memo belongs to
  no run, so cleanup never touches it; one whose source run was retired
  simply misses (`SOURCE_COMMIT_GONE`). Deleting any memo, or the whole
  prefix, only makes the next run of a cached task run its container.
- **Logs and records, never queues.** Nothing in OrkStack reads an audit or
  delivery record to decide what to do or send; expiring one only shortens
  how far back `orkctl admin audit` or `orkctl webhook deliveries` can look.
  A stored delivery body is read only by its own delivery's attempts, and
  nothing lists `metadata/delivery-bodies/` to find work.
- **The archive is append-only.** Bodies and index rows — the run index, the
  origin index, the root index and the newest-first index alike — are never deleted; what cleanup reclaims is the
  run's logs, attempt records, snapshots, commit records and object markers,
  and it records that in the cleanup sidecar.

## Line indexes

A fan-out over an artifact's lines (`each.over_artifact`, `docs/DSL.md`,
"Fan-out over an artifact's lines") expands from a line index: one immutable
object an Orker writes once, after reading the whole source and checking every
line.

```text
metadata/commits/{namespace}/{encoded-run-id}/{task-id}/each-lines/{format}-{sha256}.idx
```

It lies beside the fan-out task's own commit, under the run's commit prefix,
so the run's cleanup reclaims it with that prefix. `{format}` is `jsonl` or
`lines` and `{sha256}` the source's committed SHA-256, so the key says what the
index is of: a producer that is run again with other bytes is indexed again,
and one that writes the same bytes shares the index (keyed by the source's
digest, never the producer's identity). It is written create-if-absent; an
object already there is accepted only when its bytes are the same, and a
different one fails the fan-out `EACH_EXPANSION_FAILED`. It is a record, never
execution state: an item re-reads it, verifies it against the source's digest
and its own entry's SHA-256, and a missing or contradictory one fails
`EACH_ITEM_UNAVAILABLE`.

Format v1 (`internal/eachlines/codec.go`), all integers little-endian:

| Part | Bytes | Fields |
|---|---|---|
| Header | 64 | magic `ORKLIDX` (7), version `1` (1), source SHA-256 (32), source size u64, format u8, reserved (3, zero), batch `K` u32, item count u32, non-empty line count u32 |
| Entry, one per item | 56 | span offset u64, span length u32, first file line u32 (1-based, empty lines counted), lines u32, reserved u32 (zero), SHA-256 of the span (32) |

The size is `64 + 56 x count` whatever the batch, at most 547 KiB at 10,000
items. An item reads only its span, by a ranged read, and holds it to the
entry's digest.

## Artifact provenance

`RunService.GetArtifactProvenance` (`orkctl run artifact provenance`, and the
**Provenance** disclosure on each artifact row of the run page) is the one
user-facing reader of the object index. It writes nothing. For one artifact,
addressed by its run, task, kind and recorded name and resolved against the
namespace-checked run — never by a caller-supplied URI or key — it answers:

- **`bytes_present`** — a `Stat` of the recorded object, no bytes
  transferred. `false` means a lifecycle rule or an operator removed the bytes
  while the references stand. A store that cannot answer fails the call; it
  never reports `false`. Bytes the artifact GC deleted answer
  `ARTIFACT_RECLAIMED` instead (see "Reclaiming space").
- **`own_reference`** — the addressed run's marker: `HELD`; `RELEASED`, with
  `released_at` from the cleanup sidecar's `temporal_deleted_at`, whenever the
  run is retired, whether its marker was released at retirement or is kept
  for the artifact GC; or `ABSENT`, meaning no marker and no sidecar (not
  archived yet, or archived before the index existed).
- **`references`** — every marker under the object's prefix whose run is in
  the calling namespace and not retired, newest `created_at` first. Markers of other
  namespaces are dropped and not counted; a marker whose run body is missing
  is counted in `references_unreadable` and listed nowhere. More than
  `MaxObjectReferences` (1000) markers is `ResourceExhausted`, never a
  truncated list.

What the list holds follows from the object key, not from a flag. An `s3`
object key embeds its run id, so its prefix holds the producing run alone —
OrkStack never dedupes across runs there. A `local` object key is
name-and-digest, so every run that produced identical bytes under one name is
listed. Markers record a run's own task outputs only (artifacts, values, and
stdout/stderr); nothing records consumption, and a bare content digest cannot
be looked up, because the marker's object segment is one-way.

## Reclaiming space

`internal/archive/cleanup.go` runs inside `ork` when `ORK_CLEANUP_ENABLED=true`
(`docs/DEPLOY.md` §1, "Retention cleanup and bucket lifecycle rules", has its
settings, its fail-closed rules and the lifecycle recipes). It walks the run
index in key order, and for each archived run past its OWN namespace's run
retention (falling back to the deployment maximum) it explicitly retires the
execution, deletes the run's `metadata/logs/{run-id}/`,
`metadata/attempts/{run-id}/`, `metadata/commits/{ns}/{run-id}/`,
`metadata/attested/{ns}/{run-id}/`, `metadata/snapshots/{ns}/{run-id}/` and
`metadata/run-context/{ns}/{run-id}/`, and writes the cleanup sidecar. With the
artifact GC `off` it also releases the run's object markers and list; in
`dry_run` and `delete` it keeps them, and the GC releases them (below).

Cleanup pages in run-index key order, because it must visit every archived
run once and order does not matter to it: the walk resumes after the last key
it read, so a whole traversal reads the index once. A run list needs the
opposite — the newest page — which the run index cannot give cheaply (its
stamp is the fourth segment, below orkflow and status), so a `ListRuns` page
reads `metadata/index/recent/{ns}/` instead: newest first by key, resumed from
the page cursor, and stopped once the page is full and the instant of its last
run has been read whole (ties are ordered by run id, which is not key order
within an instant). A page therefore reads about its own size in keys and one
run line per run, whatever the namespace holds. A query pinning both orkflow
and status reads that run-index subtree, which is already time-ordered, and an
origin filter reads the origin index, and a root filter the root index. A
root row that cannot be written fails the archive write as an origin row does:
no body is written, and the retry or the archive sweep finishes it. A run archived before the newest-first
index existed has no row in it: it stays readable by id but is absent from
the default list (there is no backfill; reset a development stack with
`make reset`).

Cleanup never touches `metadata/audit/`, `metadata/deliveries/`,
`metadata/delivery-bodies/` or `metadata/memo/`; a bucket lifecycle rule is the
only retention for each. A delivery deletes its own body when it settles, so
`metadata/delivery-bodies/` holds only deliveries in flight and the bodies of
any stopped from outside.

**Object reference accounting.** Archiving a run records which physical
objects its completion references: one zero-byte marker per object under
`metadata/index/objects/{sha256(object-id)}/`, and the run's own list under
`metadata/index/run-objects/`. The marker key is the reference — an object
with no keys under its prefix is referenced by no archived run — so counting
references is a keys-only prefix scan, and releasing a run's claims costs one
read plus one delete per object. Two details are load-bearing:

- **The object identity is the whole content-addressed key, not the bare
  digest.** Equal bytes under two logical names are two stored objects;
  refcounting on the digest alone would let one run's cleanup delete the
  other's.
- **The marker segment is a SHA-256 *of* the object id**, because an object
  key embeds a base64 logical name that easily exceeds the 200-byte segment
  bound; hashing it means no artifact name can make a run unarchivable.

Marker writes are issued 8 at a time because archiving happens inside a
projection activity with a 10-second budget whose failure is only logged, and
a run that exhausted it would have no archive body. The cost is still linear
in a run's artifact count, so a very wide run can still approach that budget.

**Claims of objects a run tree passes.** A composed task that passes
artifacts to its child (docs/DSL.md, "Passing artifacts to a child run") makes
one run reference objects another run produced, while both are open. So the
engine writes markers before any record references such an object: when it
starts the child, after it has checked every passed object is still in object
storage, it writes the parent's marker on each; the child, before its
first task, writes its own; and when a child returns artifacts, the parent
writes its marker on each before it records them as the composed task's. These claims are the same markers an archive
writes, so the artifact GC below sees them as live references. The child's
`Run.input_artifacts`, the composed task's `TaskChild.input_artifacts` and
its artifacts list what was claimed, so each run's archive lists those objects too and its
retirement releases its claims. A claim that cannot be written fails the
start it guards with `CHILD_INPUT_UNAVAILABLE`, and no child, or no task of
the child, starts. The local backend writes them too, and reclaims nothing.

`DeleteRunArtifacts` releases only the calling run's markers and list; it
never deletes artifact bytes. Retirement with the GC on keeps them
(`RetireRunArtifacts`), and provenance reports such a marker's run as
`RELEASED`, because a retired run has no live reference.

### The artifact GC

`internal/archive/reclaim.go` deletes the artifact bytes of retired runs on
the `s3` backend. It runs inside `ork` beside retention cleanup, and only when
cleanup runs (`ORK_CLEANUP_ENABLED=true`). `ORK_CLEANUP_ARTIFACT_GC` chooses
its mode: `off`, `dry_run` (the default) or `delete`. On the `local` backend
it reclaims nothing, because a local object key names no producing run; there
a bucket lifecycle rule is still the only reclaimer, as before.

An object O's producer P is the run segment of O's key. O is deleted only
when all of these hold, decided object by object:

1. **P is verified retired** more than the grace G ago
   (`ORK_CLEANUP_ARTIFACT_GC_GRACE`, 1 h, at least 10 m), measured on the
   store's clock from P's sidecar. *Verified retired* is six checks, never the
   sidecar alone: the sidecar exists; P's current archive body exists, names
   P, is terminal and records a finish; P's namespace's current run retention
   has passed since that finish; the cluster holds no execution of P; no
   resume record of P is newer than that body; and P's commit prefix
   `metadata/commits/{ns}/{P}/` and its attested prefix
   `metadata/attested/{ns}/{P}/` (what an engine recorded) both list empty.
   Anything it cannot read keeps O.
2. **Every other run holding a marker on O**, listed after the pass's walk of
   the cluster's open runs, is verified retired more than G ago.
3. **The low-water mark admits it**: L(O) + G is before LW, where L(O) is the
   latest retirement among P and those runs, and LW is the earliest creation
   of any run that is open, or closed without an archive body.

The third rule is what makes a memo hit safe without the Orker telling
anyone: a run can take O from P only while P's commit exists, so it was
created before P retired. While it is open or unarchived it holds LW at or
below its creation; once archived it has a marker and must retire like any
claimant. So every run that could reference O has either a marker the GC
lists, or holds LW low enough to keep O.

LW is computed once per pass. It needs this replica's latest archive-sweep
pass to have completed clean (else `lowwater_unknown{sweep_unclean}`, or
`{no_sweep}` before the first); a walk of every live chain, each dated by its
original creation (the memo `orkflow_created_at` or its first execution's
start; a later execution without one fails the pass closed, `no_creation`,
and so does a chain still caught between its records, `mid_switch`); and a
listing of the runs closed since the sweep's start less the cleanup interval
and G, checking for an archive body, in ascending creation, at most a batch of
them. LW is the minimum of this pass's mark and the previous pass's, so a run
the walk skipped while its record moved is still counted; the first pass after
an engine starts has no previous mark, deletes nothing and only reports
(`warming`).

A run whose objects the GC has finished with gets a create-only GC record
(`metadata/cleanup/objects/{run-id}.json`, in `delete` mode only), and its
markers and list are deleted. Before each delete the GC lists O's markers
again and P's two prefixes again: a commit or a record that appeared after P
was retired — an Orker suspended past the run's retention that then committed,
or an engine that wrote its record — keeps
everything P produced, for good (`late_commit`), and nothing ever re-empties
it. The one residue: such a commit created in the moment between that last
listing and the delete, by an Orker that thawed from a suspension longer than
the run's retention exactly then.

The GC never takes a lock, never delays a retirement, and keeps between passes
only a process-local mark, a cursor and the set of runs with a GC record. A
pass is held, and deletes nothing while still reporting what it would, when
the engines disagree on the `composed-artifacts` capability, when the mark is
unknown, when this replica's clock is more than G/4 from the store's
(`clock_skew`), when retention cleanup holds missing executions
(`executions_missing`), or on the `local` backend.

`orkctl admin gc preview` asks any replica what a pass would do now, for at
most 1000 runs, within 30 s, writing nothing (docs/CLI.md). Run it before
switching `dry_run` to `delete`.

**Reclaim lag.** Nothing produced before the oldest open or unarchived run was
created is reclaimed while that run lives: a 30-day await holds back 30 days.
It is a cost, never a loss, and the preview names the run holding the mark.

**Preconditions of `delete`.** Every engine's clock and the cluster's within
G/4 of the store's; strong list-after-write on the store (true on AWS S3 and
on single-filer SeaweedFS); the cluster's visibility index lagging less than
G; and store permissions that let `ork` delete under `orkflows/`. On a bucket
with versioning, a delete adds a delete marker: reclaiming the bytes needs a
`NoncurrentVersionExpiration` rule. An Object Lock refusal is counted
`failed{store_refused}` and the object stays.

**Reads of reclaimed objects.** An artifact download, a provenance read and
`orkctl task run --from-run` that find an object missing whose producer has a
GC record not listing it as kept answer `ARTIFACT_RECLAIMED`
(`FailedPrecondition`, docs/REASONS.md); a run evidence export records the
member `reclaimed` with the reason `artifact_reclaimed`. A run diff reads no
artifact bytes. An object missing for any other reason answers as before.

Archive bodies, memos, audit records, deliveries, orphan definitions,
multipart uploads, run evidence and `_unattributed` objects are out of the
GC's scope: a bucket lifecycle rule remains their only retention.
