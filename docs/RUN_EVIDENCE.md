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
orkctl run evidence export <run-id> -o PATH [-t TASK [--item N]] [--no-logs] [--no-artifacts] [--overwrite] [-n NS]
orkctl run evidence export <run-id> -j [-t TASK [--item N]] [--no-logs] [--no-artifacts] [-n NS]
orkctl run evidence verify <bundle.tar.gz> [-j]
orkctl run evidence verify <bundle.tar.gz> --against-store [-n NS] [--server URL]
```

`export` commits the file only after the stream's `last` chunk (a temporary
sibling, renamed), so an interrupted export leaves nothing behind; `--json`
prints `MANIFEST.json` itself, a character a terminal could obey written as its
JSON escape, and writes no file. The run page offers the same
bundle through its **Evidence** action, which shows the manifest before any
byte of the bundle is fetched ([UI_CLI_PARITY.md](UI_CLI_PARITY.md), "Run
evidence").

## Layout

```text
MANIFEST.json                              first member, always
run.jsonl                                  the archive body of the run's newest ending, verbatim (metadata/runs/{run-id}.jsonl, or metadata/run-resumes/{run-id}/{scope}.jsonl once it was resumed)
endings/{scope:020}.jsonl                  a resumed run's earlier endings, the first (scope 0) first, verbatim
orkflow/source.yaml                        Orkflow.source_yaml of the pinned version, verbatim bytes (a shared Orkflow's: its owner's)
tasks/{task-id}/commit.json                ActivityCommit, the stored bytes (SUCCEEDED tasks only)
tasks/{task-id}/values.json                the committed values.json
tasks/{task-id}/artifacts/{member-name}    each output artifact (SUCCEEDED and FAILED tasks)
tasks/{task-id}/attempts/{n}.json          the TaskAttempt record, the stored bytes
tasks/{task-id}/attempts/{n}/stdout.log    that attempt's uploaded streams
tasks/{task-id}/attempts/{n}/stderr.log
tasks/_inputs/artifacts/{member-name}      each artifact a child run received (after every task)
tasks/{task-id}/records/[r{scope}/]resolution.json         a resolution the engine recorded, the stored bytes (see "Records")
tasks/{task-id}/records/[r{scope}/]resolution-detail.json  that resolution's detail: who gave it, by which door, and why
tasks/{task-id}/records/[r{scope}/]composed.json           a composed task's values the engine recorded
records/outputs.json                       a child run's outputs record (last)
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
one. `verify` reads bundles with and without items.
A fan-out over an artifact's lines (`each.over_artifact`) exports the same way:
one entry per item, and a batch is one item. Its line index is a record of the
run, not a member; its source is the producing task's own `artifact` member,
and a batch's slice file is an input the Orker staged, never exported.

A child run's received artifacts (`docs/DSL.md` "Passing artifacts to a child
run") follow every task, as `artifact` members with `task_id` `_inputs` under
`tasks/_inputs/artifacts/`; there is no `tasks[]` entry for `_inputs`. A
composed task's artifacts are the ones its child returned, listed like any
task's. Both carry `source_run_id`, `source_task_id` and `source_name`. A scope of one task
carries that task's members only; `_inputs` is the scope of the received ones.

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
  "schema_version": 3,
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

Two keys appear only where they say something: `orkflow_namespace`, beside
`orkflow_name`, is the namespace that owns a shared Orkflow whose child run
this is, and `counts.withheld` counts the withheld members.

`MANIFEST.json` is not listed in `members`: it cannot carry its own digest.
Each member has:

| Key | Meaning |
|---|---|
| `path` | the tar entry name |
| `kind` | `run`, `ending`, `source`, `commit`, `values`, `artifact`, `attempt`, `stdout`, `stderr` |
| `task_id`, `attempt` | the task and attempt the member belongs to, when it belongs to one |
| `name` | for `artifact`: the exact recorded artifact name; `path`'s last segment is derived from it and may be encoded |
| `origin` | the object the bytes came from: the recorded URI for an artifact, values, log or commit; the store key for the archive body and an attempt record; the version document key plus `#source_yaml` for the definition. A location, never a credential |
| `size_bytes`, `sha256` | of the bytes as read (`present`) or as recorded (`reclaimed`) |
| `state`, `reason` | below |
| `source_run_id`, `source_task_id`, `source_name` | for an artifact passed between the runs of a tree: the run, task and artifact name that recorded it. Audit data |

`scope` records what was asked: `task_id` narrows the per-task members to one
task (the run's own members are always included), `logs: false` drops every
`stdout.log`/`stderr.log`, `artifacts: false` drops the output artifacts and the received ones.
`values.json`, `commit.json` and attempt records are never excluded.

### Versions

Every bundle is written with `schema_version: 3`, and `verify` reads only that.
A bundle of another version, earlier or later, is refused rather than guessed
at: the message names its version and the one this `orkctl` reads. The `orkctl`
that exported a bundle verifies it, so keep that release for bundles an engine
before 0.4.0 exported (versions 1 and 2). A change an older reader could misread
bumps the version.

Version 3 may carry `event_gaps`, the `withheld` state, `endings`,
`orkflow_namespace`, an artifact's `source_*` keys, and the record members
below with their `missing` state, `record_state`, `resume_scope` and the
`missing` count.

### Records

A run whose results the engine records (`docs/TASK_IDENTITY.md`) has its
records as members of their own kinds, after every task's and `_inputs`'
members: `resolution` and `resolution-detail`, `composed` and `outputs`. A
bundle holds one for every record found under the run's keys, whatever the
run's state says of its task, so an approval recorded in time and never
applied, the one to read when asking why a run timed out, is there. A
present record member carries `record_state`, as `orkctl run records` says it:
`on_record` (the run applied it), `not_applied` (recorded in time, not taken)
or `not_taken` (too late, or under a resume a later one replaced), and its
`resume_scope` when a resume made it. A resolution's detail lies beside it.
A record the run's state says should be there, that the store said twice is
not, while the run was not retired, is `missing`: the gap, which `verify`
states and never passes as present. One that went with its retired run is
`reclaimed`. A record names the pool and Orker that made it and where its
values lie, so a caller who holds neither `run.artifacts.read` on the run's
Orkflow nor `orker.read` is given it `withheld`, with no bytes, as
`orkctl run records` hides those fields from them. A record that could not be
read, or is in a format this engine does not read, or that the store listed
and then said is not there, has no state: the export fails `Unavailable`. A
run of container tasks alone whose results name no record is exported with no
read for records. A bundle of a run
with none of them holds none. Commits stay their tasks' members.

### A run whose stream has a gap

A run that carries on into a fresh history reads the events of its earlier
histories from immutable event segments in object storage. When one of them
was missing or altered by the time the run was archived, the archive records
the range it lost as a `gap` line in `run.jsonl` (`docs/STORAGE.md`) rather than
leaving the run unarchived. So it does for numbers the run used without
publishing an event, because object storage refused that event's record: they
are recorded as never published, with the reason `the run's history holds no
event under these numbers` (`docs/DEPLOY.md` §6). A run whose definition could
not be used and whose `RUN_CREATED` was refused is one: its stream starts at its
terminal event, at 2, and 1 is recorded.
The bundle of such a run has one more key:

```json
  "event_gaps": [
    {"first": 1201, "last": 2400, "reason": "segment object missing"}
  ]
```

`orkctl` names each gap for its cause: events "lost from object storage before
it was archived", or "never published".

Every member is exactly what it would be without one — `run.jsonl` is still the
archive body verbatim, gap lines included, and still `present` — so the
bundle is exactly what the archive holds.

### A resumed run

A run that was resumed ended more than once. The bundle is of its newest ending,
whose `run.jsonl` holds the whole stream, and it holds the archive body of each
earlier one too, as it was written. One more key names them all, the first
ending first:

```json
  "endings": [
    {"scope": 0, "path": "endings/00000000000000000000.jsonl", "run_status": "FAILED", "reason": "EXIT_1", "run_finished_at": "2026-09-10T02:08:42Z"},
    {"scope": 41, "path": "run.jsonl", "run_status": "SUCCEEDED", "run_finished_at": "2026-09-10T03:15:09Z"}
  ]
```

A `scope` is the number the stream goes on at after that resume. The numbers
between the ending set aside and it, if any, were never used: no `event_gaps`
names them. The other members are the newest ending's; what an earlier one
recorded and a later one replaced is named, with its digest, in that ending's
body. A result that a resume kept and the run ended before reading back is a
`CANCELED` task with `RESUME_NOT_READ_BACK` in `run.jsonl` and no
`commit.json`: its commit is named by the ending before.

An earlier ending whose body is gone is `reclaimed` (`ending_missing`), with no
digest, because nothing recorded one. It is known from the mark its resume left,
where the newest stream holds an event or a gap at that scope; a mark with
neither is a resume that never began, and no ending. One that does not decode is `rejected`
(`ending_damaged`). Neither has a `run_status` in `endings`.

## States

| State | Meaning | `sha256` | In the tar |
|---|---|---|---|
| `present` | the bytes were read from the store and hashed; for a member with a recorded reference the hash and size **equal** the reference — a mismatch aborts the export | of the bytes | yes |
| `reclaimed` | the store answered NotFound for an object the run recorded: the cleanup worker (commits, attempt-linked logs), a bucket lifecycle rule or `DeleteUnreferencedObjects` (artifact objects), or `DeleteOrkflow` (the definition) | the **recorded** digest, preserved; empty for `source` and `ending` | no |
| `unrecorded` | an attempt number between 1 and the highest known attempt for which the attempt store holds no record — the Orker died before filing one, or it was reclaimed | empty | no |
| `rejected` | an object exists at the coordinates but is not what it should be: the definition, when the stored document does not hash to the run's definition digest, or an earlier ending's body that does not decode | empty; `definition.stored_definition_digest` carries the document's | no |
| `withheld` | the exporter may not read the object: the definition of a shared Orkflow's child run, when the caller lacks `orkflow.read` on it in its owner's namespace, or a record ("Records"), when the caller holds neither `run.artifacts.read` on the run's Orkflow nor `orker.read`. While the access document cannot be read and configuration's bindings do not grant it, the export fails `Unavailable` instead | empty | no |
| `missing` | a record the engine should have made of a run whose results it records: the run's state says its task succeeded, the run was not retired, and the store said twice it is not there ("Records") | empty | no |

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
| `definition_not_readable` | the exporter may not read the shared Orkflow in its owner's namespace; the run's definition digest still attests it |
| `definition_digest_mismatch` | a document exists at the pinned coordinates but does not hash to the digest this run pinned |
| `commit_missing` | the commit record was reclaimed by cleanup; its recorded digest is preserved |
| `object_missing` | the object was removed from the store by a lifecycle rule or cleanup; its recorded digest is preserved |
| `attempt_record_missing` | no record: the orker died before filing one, or it was reclaimed |
| `ending_missing` | the archive body of this earlier ending was removed from the store |
| `ending_damaged` | an object is stored for this earlier ending, but it does not decode as an archive body of this run |
| `artifact_reclaimed` | the artifact GC deleted the object after its run was retired; its recorded digest is preserved |
| `record_missing` | the store holds no record the engine should have made: the run's state says its task succeeded, and the run was not retired |
| `record_reclaimed` | the run was retired, and its records went with its commits at cleanup |
| `record_withheld` | the exporter may read neither the run's artifacts nor the Orkers, which a record names |

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
   shortly"). That is the moment between its terminal event and the archive
   write, for a run that archives itself. A run that could not — its ending
   could not be written to object storage, or it was terminated — is archived
   from its history by the archive sweep, within about two cleanup intervals of
   its end and once object storage answers (`docs/DEPLOY.md` §1, "The archive
   sweep"), and its export can be retried then. Of a resumed run it is the newest
   ending that must be archived: an earlier one is never exported as the run's.
3. Every member is resolved from the **archived** run snapshot, never from a
   coordinate the caller supplied. An export is a read of the run:
   `run.evidence.export` gives its whole archived record, its trace context
   (`traceparent`) included, with no `run.read` asked.
4. The definition is read by the run's name and version, in the namespace that
   owns it (a shared Orkflow's child run pinned its owner's document), and
   attested by recomputing its definition digest; a document that does not hash
   to the pinned digest is `rejected`. Another namespace's document is read
   only for a caller with `orkflow.read` there, and is `withheld` from any
   other ([ACCESS.md](ACCESS.md)).
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
7. A `schema_version` other than 3 is refused ("Versions", above).
8. The manifest must agree with itself: a member whose `state` is not one of
   `present`, `reclaimed`, `unrecorded`, `rejected`, `withheld` or `missing` is
   `unknown state`, and
   a `path` listed a second time is `listed twice in manifest`. The engine
   never writes either; the rule exists so that every listed member is
   accounted for exactly once, as verified, a finding, or a failure.
   `endings` that are not the run's first and later ones in order, each with its
   member, are refused.
9. In a resumed run's bundle the event stream of each ending's body must be
   whole: from 1 on, every number is an event, lies in a `gap` line, or is
   skipped right before a scope `endings` names, where `RUN_RESUMED`, the run's
   end or the first number of a `gap` line stands, and the stream reaches that ending's own scope
   (`event sequence broken`, with the number wanted and the one found). A body that is no archive body is `not an archive body`.
10. A present `resolution` member's detail must be the `resolution-detail`
    member beside it, present, whose digest is the one the resolution's own
    bytes name (`detailSha256`); one absent or another is `detail is not the
    one its record names`, named by the detail's path.

Reclaimed, unrecorded, rejected and withheld members are findings, not
failures: the command exits 0 and lists each under its own state with its
reason, and never among the verified. So is each event gap of a
bundle, printed as a notice:

```text
  event gaps      events 1201–2400 of this run were lost from object storage before it was archived (segment object missing)
```

A gap the run never published reads `events 1–1 of this run were never
published (the run's history holds no event under these numbers)`.

A `missing` record is a gap of its own, as an event gap is: named by its
task and kind, with its reason and path, and the command still exits 0, since
the bundle is what the store holds. `--json` carries each under `missing`,
with its `task_id` and `kind`:

```text
  record gaps     task ship's composed record: NOT ON RECORD (record_missing) tasks/ship/records/composed.json
```

Any failure exits 1 and is named, one line per member:

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
`verified`, `reclaimed`, `unrecorded`, `rejected` and `failures` lists,
`withheld` when a member is, and
`event_gaps` for a bundle that names some.

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
  `present` is `restored since export`;
- a member `withheld` in one and not the other is the finding `readable to only
  one of the exporter and this caller` (exit 0: nothing moved in the store);
- a run resumed since the export is the finding `resumed since export` (exit 0):
  its members are another ending's now, so a member is compared only when the
  live manifest names it from the same object. The endings the bundle holds must
  each be in the store as the bundle has it, or `reclaimed`.
  While the run has not ended again the engine refuses the manifest.

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
