# Run evidence bundles

A run evidence bundle is one file that answers *"prove exactly what ran, on
which bytes, with what inputs, and that nothing has changed since"* for one
finished run. It is a `tar.gz` assembled per request by
`RunService.ExportRunEvidence` from objects the run already recorded, and kept
nowhere: there is no export record, no evidence key family and no server-side
cache, so nothing can disagree with the archive (AGENTS.md §2.2, §13).

This page is the reference an auditor without the engine reads. The bundle
needs nothing but itself to verify: `orkctl run evidence verify` is a hashing
loop over its manifest, with no server and no credentials.

```text
orkctl run evidence export <run-id> --output PATH [--task TASK [--item N]] [--no-logs] [--no-artifacts] [--overwrite] [-n NS]
orkctl run evidence export <run-id> --json [--task TASK [--item N]] [--no-logs] [--no-artifacts] [-n NS]
orkctl run evidence verify <bundle.tar.gz> [--json]
orkctl run evidence verify <bundle.tar.gz> --against-store [-n NS] [--server URL]
```

`export` commits the file only after the stream's `last` chunk (a temporary
sibling, renamed), so an interrupted export leaves nothing behind; `--json`
prints `MANIFEST.json` itself and writes no file. The run page offers the same
bundle through its **Evidence** action, which shows the manifest before any
byte of the bundle is fetched ([UI_CLI_PARITY.md](UI_CLI_PARITY.md), "Run
evidence").

## Layout

```text
MANIFEST.json                              first member, always
run.jsonl                                  the archive body, verbatim (metadata/runs/{run-id}.jsonl)
orkflow/source.yaml                        Orkflow.source_yaml of the pinned version, verbatim bytes
tasks/{task-id}/commit.json                ActivityCommit, the stored bytes (SUCCEEDED tasks only)
tasks/{task-id}/values.json                the committed values.json
tasks/{task-id}/artifacts/{member-name}    each output artifact (SUCCEEDED and FAILED tasks)
tasks/{task-id}/attempts/{n}.json          the TaskAttempt record, the stored bytes
tasks/{task-id}/attempts/{n}/stdout.log    that attempt's uploaded streams
tasks/{task-id}/attempts/{n}/stderr.log
```

Members appear in exactly this order: the run's members, then tasks in the
run snapshot's order, and within a task commit, values, artifacts (in the
snapshot's order, which is name-byte order), then attempts ascending, each
record before its logs. The order is part of the contract: it is what lets a
reader stream-verify without buffering, and a bundle in another order fails
`verify`.

A fan-out (`docs/DSL.md` "Fan-out") contributes its node's `tasks[]` entry
(status, no attempts) followed by one entry per item carrying `"item": i`.
Each item's members use the per-task layout above under
`tasks/{task-id}/items/{i:05}/` (`commit.json`, `values.json`,
`artifacts/{member-name}`, `attempts/{n}.json`, `attempts/{n}/stdout.log`,
`attempts/{n}/stderr.log`), items ascending, and every such member entry
carries `"item": i`; `scope` carries `item` when the export was narrowed to
one. These keys are additive, so `schema_version` stays `1` and `verify`
reads bundles with and without items.

A `SKIPPED` task has no members and one `tasks[]` entry. A `FAILED` or
`CANCELED` task has no `commit.json` and no `values.json`, and has artifact
and log members for whatever its failure path uploaded.

Tar entries are regular files, mode `0644`, uid/gid 0, no user names, and a
modification time equal to the manifest's `exported_at`; the gzip header time
is `exported_at` too. Two exports of one run differ only in `exported_at`.

What the bundle does not carry, deliberately: the Orker's per-attempt
`task.json`, `config.json` and `run.json` are written into a workspace that is
removed after the attempt and are not stored; what the container was handed
is reconstructible from members the bundle does carry (the definition's
`with:`, `env`, secret names and locators, the pinned image, the run input
inside `run.jsonl`, and each upstream task's committed `values.json`; a
failure handler's `task.json` `join` was built from its needs' rows in
`run.jsonl`, and a failed need's staged files are that need's own artifact
members). The live
log chunk stream is not carried either: the durable, digested record of an
attempt's output is the `stdout.log`/`stderr.log` it uploaded.

No member carries a secret value, because no store holds one.

## MANIFEST.json

Two-space indented JSON with a trailing newline, `snake_case` keys, enum
values in the short dialect of [JSON.md](JSON.md) (`SUCCEEDED`, not
`STATUS_SUCCEEDED`). Every file digest is lowercase hex without a `sha256:`
prefix; only `definition_digest`, a definition digest rather than a file
digest, keeps the `sha256:` prefix the run recorded.

```jsonc
{
  "schema_version": 1,
  "run_id": "ork-…",
  "namespace": "payments",
  "orkflow_name": "payments-reconcile",
  "orkflow_version": "1.4.0",
  "definition_digest": "sha256:9f2c…",   // Run.orkflow_definition_digest, verbatim
  "run_status": "SUCCEEDED",
  "run_created_at": "2026-09-10T02:00:01Z",
  "run_finished_at": "2026-09-10T02:08:42Z",
  "exported_at": "2026-09-22T10:51:49Z",  // whole seconds, UTC
  "engine_version": "v0.9.3",             // the replica that exported
  "build_id": "2026.09.22-1",             // "" when versioning is off
  "scope": { "task_id": "", "logs": true, "artifacts": true },
  "definition": { "state": "present", "reason": "", "stored_definition_digest": "" },
  "tasks": [
    { "task_id": "reconcile", "status": "SUCCEEDED", "outcome_attempt": 3, "exit_code": 0,
      "attempts": [
        { "attempt": 1, "state": "present", "outcome": "FAILED" },
        { "attempt": 2, "state": "unrecorded" },
        { "attempt": 3, "state": "present", "outcome": "SUCCEEDED" } ] }
  ],
  "members": [
    { "path": "run.jsonl", "kind": "run", "origin": "metadata/runs/ork-….jsonl",
      "size_bytes": 18211, "sha256": "…", "state": "present" },
    { "path": "tasks/reconcile/artifacts/rates.parquet", "kind": "artifact", "task_id": "reconcile",
      "name": "rates.parquet", "origin": "s3://…", "size_bytes": 43212811, "sha256": "ab12…", "state": "present" }
  ],
  "counts": { "present": 39, "reclaimed": 2, "unrecorded": 1, "rejected": 0, "bytes_present": 44120335 }
}
```

`MANIFEST.json` is not listed in `members`: it cannot carry its own digest.
Each member has:

| Key | Meaning |
|---|---|
| `path` | the tar entry name |
| `kind` | `run`, `source`, `commit`, `values`, `artifact`, `attempt`, `stdout`, `stderr` |
| `task_id`, `attempt` | the task and attempt the member belongs to, when it belongs to one |
| `name` | for `artifact`: the exact recorded artifact name; `path`'s last segment is derived from it and may be encoded |
| `origin` | the object the bytes came from: the recorded URI for an artifact, values, log or commit; the store key for the archive body and an attempt record; the version document key plus `#source_yaml` for the definition. A location, never a credential |
| `size_bytes`, `sha256` | of the bytes as read (`present`) or as recorded (`reclaimed`) |
| `state`, `reason` | below |

`scope` records what was asked: `task_id` narrows the per-task members to one
task (the run's own members are always included), `logs: false` drops every
`stdout.log`/`stderr.log`, `artifacts: false` drops the output artifacts.
`values.json`, `commit.json` and attempt records are never excluded.

A change a version-1 reader could misread bumps `schema_version` (additive
keys, like the fan-out `item`, do not); `verify` refuses a version it does not
know rather than guessing.

### Version 2: a run whose stream lost events

A run that carries on into a fresh history reads the events of its earlier
histories from immutable event segments in object storage. When one of them
was missing or altered by the time the run was archived, the archive records
the lost range as a `gap` line in `run.jsonl` (`docs/STORAGE.md`) rather than
leaving the run unarchived, and the bundle of such a run is written with
`schema_version: 2` and one more key:

```json
  "event_gaps": [
    {"first": 1201, "last": 2400, "reason": "segment object missing"}
  ]
```

Every member is exactly what version 1 would list — `run.jsonl` is still the
archive body verbatim, gap lines included, and still `present` — so the
bundle is exactly what the archive holds. A version-2 manifest must carry a
non-empty `event_gaps`, and a version-1 manifest must not carry one. It is a
new version rather than an additive key on purpose: a tool older than it
refuses the bundle with its own `schema_version` message, instead of
verifying it while ignoring the gap. Every bundle of a run that lost nothing
stays version 1.

## States

| State | Meaning | `sha256` | In the tar |
|---|---|---|---|
| `present` | the bytes were read from the store and hashed; for a member with a recorded reference the hash and size **equal** the reference — a mismatch aborts the export | of the bytes | yes |
| `reclaimed` | the store answered NotFound for an object the run recorded: the cleanup worker (commits, attempt-linked logs), a bucket lifecycle rule or `DeleteUnreferencedObjects` (artifact objects), or `DeleteOrkflow` (the definition) | the **recorded** digest, preserved; empty for `source` | no |
| `unrecorded` | an attempt number between 1 and the highest known attempt for which the attempt store holds no record — the Orker died before filing one, or it was reclaimed | empty | no |
| `rejected` | an object exists at the coordinates but is not what the run pinned: today only the definition, when the stored document does not hash to the run's definition digest | empty; `definition.stored_definition_digest` carries the document's | no |

Absence is a member, not a gap: nothing the run recorded is omitted silently.
A store that cannot answer is not absence: the export fails `Unavailable`
rather than listing a member `reclaimed`.

The expected attempt numbers are `1..max(TaskRun.attempt, highest record)`.
The outcome attempt's logs come from the run snapshot (which survives the loss
of that attempt's record); every other attempt's logs are what its own record
says it uploaded. A retry that adopted a durable commit names the committing
attempt's logs as its own, so its `attempts/{m}/stdout.log` is byte-identical
to the committing attempt's, and its `attempts[]` entry carries
`commit_adopted: true`. Nothing is deduplicated: two members with one digest
are two members.

## Reason tokens

| token | sentence |
|---|---|
| `definition_deleted` | the pinned definition was deleted from the orkflow store; the run's definition digest still attests it |
| `definition_digest_mismatch` | a document exists at the pinned coordinates but does not hash to the digest this run pinned |
| `commit_missing` | the commit record was reclaimed by cleanup; its recorded digest is preserved |
| `object_missing` | the object was removed from the store by a lifecycle rule or cleanup; its recorded digest is preserved |
| `attempt_record_missing` | no record: the orker died before filing one, or it was reclaimed |

A client prints the token and its sentence; an unknown token is printed as
itself.

## Member names

An artifact's recorded name may contain `/`, `..`, spaces or 200 bytes of
Unicode; a tar path must not. A name that is already a clean component —
`^[A-Za-z0-9._-]{1,200}$` and neither `.` nor `..` — is used verbatim.
Anything else is `~` followed by unpadded URL-safe base64 of the name's bytes,
chunked at 200 characters with `~` between chunks. A clean name cannot start
with `~`, so the two forms never collide; the manifest's `name` always carries
the exact name. `commit.json` and `values.json` are not under `artifacts/`, so
a workload artifact named `commit.json` collides with nothing.

## The export, step by step

1. The run is addressed inside its namespace exactly as `GetRun` addresses
   it; another namespace's run is NotFound. Nothing is read before that check.
2. It must be finished (`FailedPrecondition` naming its status otherwise) and
   archived (`FailedPrecondition` "finished but not yet archived; retry
   shortly" in the moment between its terminal event and the archive write).
3. Every member is resolved from the **archived** run snapshot, never from a
   coordinate the caller supplied.
4. The definition is read by the run's name and version and attested by
   recomputing its definition digest; a document that does not hash to the
   pinned digest is `rejected`.
5. A commit is read only at the `CommitRef.uri` the snapshot recorded, and its
   bytes must hash to `CommitRef.sha256` (carried through, never recomputed by
   re-marshalling); otherwise the export is `DataLoss`.
6. Artifacts, values and logs are `Stat`'ed for the manifest and read only
   while streaming; each is hashed as it is copied and must equal its recorded
   size and digest. A mismatch ends the stream `DataLoss`; an object that
   vanished between the manifest and the stream ends it `Unavailable`. Either
   way the stream carries no `last` and the bundle is not a bundle.
7. The stream is 64 KiB chunks ending with an empty chunk carrying `last`.

`manifest_only` returns the `MANIFEST.json` bytes alone, uncompressed — the
same document the full bundle would carry first for the same request. Small
members (archive body, definition, commits, attempt records) are still read
and hashed; artifact and log objects are stat'ed, never read.

## verify

`orkctl run evidence verify FILE` never writes to disk and never follows a
path; entries are hashed as they stream and compared by the manifest's path
string. Its rules:

1. The first entry must be `MANIFEST.json`; anything else is `manifest not
   first` and nothing else is checked.
2. Every other entry must be a regular file whose name is exactly a manifest
   `path` in state `present`; anything else is `not in manifest` (including an
   entry for a member the manifest said would be absent).
3. An entry's bytes are hashed as read; size and digest must equal the
   member's (`size mismatch`, `digest mismatch`).
4. A `present` member with no entry is `missing from bundle`.
5. Entries must be in manifest order; a member out of order is `out of
   order`, and verification continues.
6. An absolute name, a `..` segment or a NUL is `unsafe path`; a non-regular
   entry is `not a regular file`.
7. A `schema_version` other than 1 or 2 is refused, and so is a version 2
   without `event_gaps` or a version 1 with them.
8. The manifest must agree with itself: a member whose `state` is not one of
   `present`, `reclaimed`, `unrecorded` or `rejected` is `unknown state`, and
   a `path` listed a second time is `listed twice in manifest`. The engine
   never writes either; the rule exists so that every listed member is
   accounted for exactly once, as verified, a finding, or a failure.

Reclaimed, unrecorded and rejected members are findings, not failures: the
command exits 0 and lists them with their reasons. So is each event gap of a
version-2 bundle, printed as a notice:

```text
  events lost     events 1201–2400 of this run were lost from object storage before it was archived (segment object missing)
```
 Any failure exits 1 and is
named, one line per member:

```text
verified 41 present members against MANIFEST.json
  run             ork-…                   namespace payments
  definition      sha256:9f2c…            payments-reconcile@1.4.0 (source.yaml present)
  status          SUCCEEDED               exported 2026-09-22T10:51:49Z by engine v0.9.3 (build 2026.09.22-1)
  tasks           6: 5 succeeded, 1 succeeded after 2 failed attempts
  reclaimed       2 members, recorded sha256 preserved:
                    tasks/extract/attempts/1/stdout.log  (object_missing)
                    tasks/extract/attempts/1/stderr.log  (object_missing)
  unrecorded      1 attempt: extract attempt 2 (attempt_record_missing)
  reasons         object_missing: the object was removed from the store by a lifecycle rule or cleanup; its recorded digest is preserved
                  attempt_record_missing: no record: the orker died before filing one, or it was reclaimed
FAILED: 1 member does not match MANIFEST.json
  tasks/reconcile/artifacts/rates.parquet   digest mismatch   want ab12…   got 77e0…
```

`--json` prints the report as one document — the manifest embedded, then the
`verified`, `reclaimed`, `unrecorded`, `rejected` and `failures` lists, and
`event_gaps` for a version-2 bundle.

## verify --against-store

Asks the engine for the live manifest of the bundle's run, in the bundle's
namespace and scope (`-n` set to a different namespace is refused before any
request), and compares member by member. A run's recorded references are
immutable, so only states may move:

- a live `sha256` different from the file's (both non-empty) is a failure,
  `store digest differs`; so is a different `definition_digest`;
- a member present in one manifest and not the other is a failure, `member set
  differs`;
- a file `present` that is live `reclaimed` with the same recorded digest is
  the finding `reclaimed since export` (exit 0: the bytes were true when
  exported and the digest still agrees); a file `reclaimed` that is live
  `present` is `restored since export`.

It prints the offline report, then a `store:` block (`every member agrees with
the store`, or the findings and failures), and exits 1 on any failure in
either.

## What a bundle is not

It is not a signature, a timestamp or a proof of origin: the manifest attests
bytes to digests, and who is trusted to have produced it is the operator's
choice (`gpg --detach-sign` on the file, a transparency log, whatever the
compliance regime already uses). It carries the engine version and build id
that produced it and nothing that claims more. It is not an export history —
an export is a read, never recorded — and not a way to re-run anything.
