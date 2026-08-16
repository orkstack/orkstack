# Embedded / single-node Go persistence for the orkstack control plane

Research date: 2026-08-14. Documentation only — no code changes made.
Companion cache of fetched sources: `research/web/2026-08-14-go-sqlite-and-embedded-stores.md`.

## Summary

SQLite in WAL mode is the right default for a single-node workflow-engine control plane, and
`modernc.org/sqlite` is the pragmatic driver choice because CGO-free builds preserve trivial
cross-compilation; `github.com/ncruces/go-sqlite3` is the strongest CGO-free alternative and
`mattn/go-sqlite3` remains fastest-ish but forces `CGO_ENABLED=1` plus a cross-compiler toolchain.
The engineering constraint that actually matters is not driver speed but the writer model: SQLite
serializes writes, so use two `sql.DB` pools (a write pool with `SetMaxOpenConns(1)` and
`_txlock=immediate`, plus a read pool) and set `busy_timeout`, `journal_mode=WAL`,
`synchronous=NORMAL`, `foreign_keys=ON` on every connection. Indexed filters over 1k–100k rows are
comfortably sub-millisecond-to-low-millisecond; published driver benchmarks scan a million rows in
~750 ms, so this workload is not near any limit. Durability: `synchronous=NORMAL` under WAL is
crash-safe but can lose the last transactions on power loss, `VACUUM INTO` gives cheap consistent
snapshots, and Litestream adds continuous S3/MinIO replication with a ~1 s default RPO — at the cost
of remaining strictly single-writer/single-node. For migrations, `pressly/goose` with
`SetBaseFS(embed.FS)` is the lowest-friction fit. None of bbolt/Badger/Pebble/DuckDB/rqlite/dqlite/
embedded-postgres beats SQLite for a metadata store with ad-hoc indexed filters at this scale.

---

## 1. Go SQLite drivers

### Cross-compilation and CGO

`mattn/go-sqlite3` is explicit about the constraint:

> "***Important: because this is a `CGO` enabled package, you are required to set the environment
> variable `CGO_ENABLED=1` and have a `gcc` compiler present within your path.***"
> — [README.md](https://github.com/mattn/go-sqlite3/blob/master/README.md)

Its own cross-compile guidance is to install a cross toolchain (`musl-cross`) or use `xgo`, i.e.
container/toolchain plumbing in CI for every target triple.

`modernc.org/sqlite` is "a sql/database driver using a CGo-free port of the C SQLite3 library" and
enumerates its supported targets explicitly (v1.56.x ships SQLite 3.53.3):

```
//	OS      Arch    SQLite version
//	darwin	amd64   3.53.3      linux	arm64   3.53.3
//	darwin	arm64   3.53.3      linux	loong64 3.53.3
//	freebsd	amd64   3.53.3      linux	ppc64le 3.53.3
//	freebsd	arm64   3.53.3      linux	riscv64 3.53.3
//	linux	386     3.53.3      linux	s390x   3.53.3
//	linux	amd64   3.53.3      openbsd	amd64/arm64, windows 386/amd64/arm64
```
— [`doc.go`, master](https://gitlab.com/cznic/sqlite/-/blob/master/doc.go)

Note the target list is closed: an unlisted GOOS/GOARCH simply does not build. All the platforms
orkstack plausibly ships (linux amd64/arm64, darwin arm64, windows amd64) are covered.

`ncruces/go-sqlite3` is "a `cgo`-free SQLite wrapper" that "wraps a Wasm build of SQLite" with
"Go and `x/sys`" as the only direct dependencies, and CI-tests linux (amd64, arm64, 386, arm,
riscv64, ppc64le, loong64, s390x), macOS, Windows, four BSDs, illumos and Solaris
— [README](https://github.com/ncruces/go-sqlite3/blob/main/README.md).

### Performance

The most maintained public comparison is [cvilsmeier/go-sqlite-bench](https://github.com/cvilsmeier/go-sqlite-bench/blob/master/README.md)
(run 2026-03-23, Go 1.26, Debian 12, i7-1165G7, NVMe; ms, lower is better):

| Benchmark | mattn (cgo) | modernc | ncruces |
|---|---|---|---|
| Simple: insert 1M rows / query all | 1480 / 871 | 2419 / 758 | 2719 / 850 |
| Real: 100 users, tx-per-user, join query | 1586 / 104 | 1759 / 130 | 1469 / 129 |
| Complex: 420k rows insert / big JOIN | 812 / 998 | 1554 / 1068 | 1749 / 1263 |
| Many: query 1000 rows × 1000 times | 819 | 692 | 837 |
| Concurrent: 8 goroutines querying 1M rows | 2395 | 2061 | 2355 |

Read that as: the three are within ~±30 % of each other on realistic query paths; cgo mattn wins on
bulk inserts and large-row reads, modernc is competitive or better on read-heavy and concurrent
reads. The author's own disclaimer applies — "Do not trust benchmarks, write your own." His summary:
"We cannot declare a clear winner, it all depends on the use case. SQLite without CGO is possible
nowadays."

Counterpoints worth knowing: a [1M-row insert/query comparison against C sqlite3](https://engineered.at/articles/comparing-golang-sqlite-to-c-sqlite)
found every Go option far slower than the C CLI (C: 737 ms insert / 84 ms query), and
[lbe/sqlite-read-benchmark](https://github.com/lbe/sqlite-read-benchmark) reports ncruces ~314 %
faster than modernc on its specific prepared-read workload (after modernc v1.46.1 fixed prepared
statements being slower than raw). Driver choice moves single-digit-millisecond numbers, not
architecture.

### `database/sql` compatibility and gotchas

All three expose `database/sql` drivers. DSN handling differs and this is where bugs come from:

- **mattn** DSN keys (from README): `_busy_timeout`/`_timeout`, `_journal_mode`/`_journal`
  (`DELETE|TRUNCATE|PERSIST|MEMORY|WAL|OFF`), `_txlock` (`immediate|deferred|exclusive`).
- **modernc** accepts the mattn-style shorthands *and* a generic `_pragma=` list. From
  [`sqlite.go`, master](https://gitlab.com/cznic/sqlite/-/blob/master/sqlite.go): it picks
  `dsnPick(q, "_busy_timeout", "_timeout")`, validates `_txlock`, executes
  `pragma busy_timeout = …` first, then the `q["_pragma"]` list — and warns that
  "`_pragma` values … are executed verbatim and cannot be checked here, so a malformed `_pragma`
  can still fail partway." A typo in a pragma therefore fails at connection use, not at `sql.Open`.
  Practical DSN: `file:ork.db?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)&_txlock=immediate`.
- **modernc build hazard**: "When you import this package you should use in your go.mod file the
  exact same version of `modernc.org/libc` as seen in the go.mod file of this repository"
  ([issue 177](https://gitlab.com/cznic/sqlite/-/issues/177)). Pin `modernc.org/libc` deliberately;
  mismatches produce obscure runtime faults.
- **ncruces concurrency contract**: "This module behaves similarly to SQLite in multi-thread mode:
  it is goroutine-safe, provided that no single database connection, or object derived from it, is
  used concurrently by multiple goroutines." Also: "Because each database connection executes within
  a Wasm sandboxed environment, memory usage will be higher than alternatives." — relevant if you
  run a large read pool.
- ncruces uniquely ships a pure-Go VFS, plus online backup, nested transactions/savepoints, custom
  functions and virtual tables through its native API.

**Recommendation for orkstack:** `modernc.org/sqlite` for CGO-free release builds and painless
`GOOS/GOARCH` matrices; keep the driver behind a thin `sql.DB` factory so swapping to
`ncruces/go-sqlite3` (drop-in `database/sql` driver, better native-API escape hatches) stays a
one-file change.

## 2. SQLite concurrency model and the single-writer pool

Authoritative statements:

- Serializability by serializing writes — "all transactions in SQLite show 'serializable' isolation.
  SQLite implements serializable transactions by actually serializing the writes. There can only be
  a single writer at a time to an SQLite database."
  ([isolation.html](https://www.sqlite.org/isolation.html))
- WAL readers/writers — "Writers merely append new content to the end of the WAL file. Because
  writers do nothing that would interfere with the actions of readers, writers and readers can run
  at the same time. However, since there is only one WAL file, there can only be one writer at a
  time." ([wal.html §2.2](https://www.sqlite.org/wal.html))
- WAL gives snapshot isolation: "When a read transaction starts, that reader continues to see an
  unchanging 'snapshot' of the database file as it existed at the moment in time when the read
  transaction started." (isolation.html)
- The upgrade hazard that motivates `_txlock=immediate`: "The attempt by X to escalate its
  transaction from a read transaction to a write transaction fails with an `SQLITE_BUSY_SNAPSHOT`
  error… If X starts a transaction that will initially only read but X knows it will eventually want
  to write… X can issue `BEGIN IMMEDIATE`… If the `BEGIN IMMEDIATE` operation succeeds, then no
  subsequent operations in that transaction will ever fail with an `SQLITE_BUSY` error."
  (isolation.html; see also [lang_transaction.html](https://www.sqlite.org/lang_transaction.html))
  `busy_timeout` does **not** retry `SQLITE_BUSY_SNAPSHOT`, so read-then-write transactions must
  start as `BEGIN IMMEDIATE` — in Go that means `_txlock=immediate` on the write DSN.
- WAL is still not busy-free: "there are some obscure cases where a query against a WAL-mode
  database can return `SQLITE_BUSY`, so applications should be prepared for that happenstance."
  (wal.html §9) Keep a bounded retry wrapper around write transactions even with `busy_timeout` set.
- Shared memory constraint, relevant if anyone suggests NFS/EFS for the state dir: "the use of
  shared memory means that all readers must exist on the same machine. This is why the write-ahead
  log implementation will not work on a network filesystem." (wal.html §2.2)

**Why `SetMaxOpenConns(1)` on the write pool.** `sql.DB` is a pool, and Go hands each transaction an
arbitrary connection. With N>1 write connections, concurrent `BEGIN IMMEDIATE`s contend at the file
lock and burn `busy_timeout` budget instead of queueing cheaply in Go. Capping the write pool at one
connection turns lock contention into an in-process mutex. The two-pool pattern is the standard
recommendation from the mattn maintainers:

> "My general recommendation is to make two pools (as in two `sql.DB`s), one with `mode=ro` and one
> with `mode=rw`. Use wal mode (`_journal_mode=wal`), which will allow reads to happen concurrently
> with writes. Do **not** use shared cache mode."
> — [mattn/go-sqlite3 #1022](https://github.com/mattn/go-sqlite3/issues/1022)

Recommended pragmas per connection (each pragma is connection-scoped except `journal_mode`, which is
persistent in the database file):

| Pragma | Value | Why |
|---|---|---|
| `journal_mode` | `WAL` | concurrent readers with a writer; required by Litestream |
| `busy_timeout` | `5000` | Litestream's own recommendation; absorbs checkpoint write locks |
| `synchronous` | `NORMAL` | skip per-commit WAL fsync; safe against corruption in WAL |
| `foreign_keys` | `ON` | off by default for back-compat; must be set per connection |
| `_txlock` | `immediate` (write pool) | avoids `SQLITE_BUSY_SNAPSHOT` on read-then-write txns |
| `wal_autocheckpoint` | `0` **only if** running Litestream under heavy write load | see §4 |

Pool shape: write `sql.DB` with `SetMaxOpenConns(1)`; read `sql.DB` (`mode=ro`) with
`SetMaxOpenConns(runtime.NumCPU())` or similar. Note the cvilsmeier "Concurrent" row above: 8
concurrent readers over a 1M-row table finish in ~2 s total, so read concurrency scales acceptably.

## 3. Query performance realism (1k–100k rows, indexed status/created_at/name)

Yes — sub-millisecond for point/range lookups, low-milliseconds for large result sets. There is no
published benchmark for exactly this schema, so the numbers below are derived arithmetic from the
cited driver benchmark, and should be treated as an order-of-magnitude sanity check rather than a
measurement of orkstack's schema:

- "Many" benchmark: 1000 queries each returning 1000 rows completes in 692 ms (modernc) / 819 ms
  (mattn) → **~0.7–0.8 ms per 1000-row query including driver row-scan overhead**
  ([source](https://github.com/cvilsmeier/go-sqlite-bench/blob/master/README.md)).
- "Simple" benchmark: a full scan returning 1,000,000 rows into Go structs takes 758 ms (modernc)
  → **~0.76 µs per row**, so a 100k-row full scan is ~75 ms *even with no index at all*.
- With a covering index on `(status, created_at)` the engine touches only matching rows, so a
  filtered page of ≤100 rows out of 100k is dominated by driver round-trip, i.e. tens of
  microseconds.

Caveats that actually bite before row count does: `LIKE '%name%'` cannot use a B-tree index (use
FTS5 or a prefix `LIKE 'name%'` with the right collation); `ORDER BY created_at DESC LIMIT n` needs
the index column order to match or SQLite will materialize and sort; run `ANALYZE` after bulk loads
so the planner has stats. Verify with `EXPLAIN QUERY PLAN` rather than trusting these estimates.

Bottom line: at ≤100k control-plane rows, SQLite query time is not a design risk. Write throughput
(one writer, fsync policy) is the only dimension worth capacity-planning.

## 4. Durability, backup, and Litestream

**Crash safety and `synchronous`.** In WAL mode, "syncing the content to the disk is not required,
as long as the application is willing to sacrifice durability following a power loss or hard reboot.
(Writers sync the WAL on every transaction commit if `PRAGMA synchronous` is set to `FULL` but omit
this sync if `PRAGMA synchronous` is set to `NORMAL`.)" ([wal.html §2.3](https://www.sqlite.org/wal.html)).
Litestream's framing: "While in WAL journaling mode, fsync calls can be relaxed in exchange for
durability without risking data corruption… This mode will ensure that the `fsync()` calls are only
called when the WAL becomes full and has to checkpoint to the main database file. This is safe as
the WAL file is append only." ([Tips & Caveats](https://litestream.io/tips/))

Read that precisely: `synchronous=NORMAL` + WAL never corrupts the database, but a power loss or
kernel panic can lose the most recent committed transactions. A process crash (SIGKILL, panic) loses
nothing, because the WAL bytes are already in the page cache. For a workflow control plane, losing
the last few hundred milliseconds of state transitions on a hard power cut is usually acceptable
provided workers can re-report; if it is not, use `synchronous=FULL` on the write pool and accept
one fsync per commit.

**Backup options.**
- `VACUUM INTO 'file'` — "The `VACUUM` command with an `INTO` clause is an alternative to the
  [backup API](https://www.sqlite.org/backup.html) for generating backup copies of a live database.
  The advantage is that the resulting backup is a minimal-size and vacuumed file"
  ([lang_vacuum.html](https://www.sqlite.org/lang_vacuum.html)). One statement, no extra tooling,
  produces a compacted consistent snapshot — ideal for a periodic `ork admin backup` command.
  Caveat: it holds a read transaction for the duration and rewrites the whole file, so cost scales
  with database size (trivial at ≤100k rows).
- Online backup API ([backup.html](https://www.sqlite.org/backup.html)) — incremental page copy that
  can run while writes continue; restarts if the source is written mid-copy. Exposed natively by
  `ncruces/go-sqlite3` ("online backup" in its feature list), not by plain `database/sql`.
- Naive file copy of the `.db` alone is wrong in WAL mode: you must handle `-wal` and `-shm`
  together, which is exactly the failure mode Litestream warns about ("If you delete your database
  file but not your WAL file then SQLite will try to apply those old WAL pages to your new
  database").

**Litestream.** "Litestream is a streaming replication tool for SQLite databases. It runs as a
separate background process and continuously copies write-ahead log pages from disk to a replica."
It "works by effectively taking over the checkpointing process. It starts a long-running read
transaction to prevent any other process from checkpointing and restarting the WAL file. Instead, it
continually reads new WAL pages and manually calls out to SQLite to perform checkpoints as
necessary." ([How it works](https://litestream.io/how-it-works/)). In v0.5 those pages are packaged
into **LTX files** keyed by monotonic transaction IDs, compacted through levels L0→L3 (30 s / 5 min
/ 1 h by default) with a full snapshot every 24 h.

- **RPO:** "By default, Litestream will replicate new changes to an S3 replica every second. During
  this time where data has not yet been replicated, a catastrophic crash on your server will result
  in the loss of data in that time window." Graceful shutdown flushes outstanding WAL changes.
  Synchronous replication is "on the Litestream roadmap but has not yet been implemented."
  ([Tips](https://litestream.io/tips/)) So: **~1 s RPO on hard crash, 0 on clean shutdown.**
- **Restore granularity is coarser than the RPO.** "Litestream replays whole LTX files and never
  applies part of one, so your restore points are the boundaries of the files that still exist in
  the replica." After `l0-retention` (default `5m`) expires, the finest surviving endpoints are L1
  boundaries. Point-in-time recovery is therefore approximate, not transaction-exact, for anything
  older than a few minutes.
- **MinIO / S3-compatible:** fully supported. "New in v0.5.0: Litestream automatically detects
  S3-compatible providers based on the endpoint URL and configures the appropriate settings"
  ([S3-compatible guide](https://litestream.io/guides/s3-compatible/)). The provider matrix lists
  MinIO as **"Status: Fully supported… Works well with default settings"** with config
  `url: s3://bucket/path?endpoint=https://minio:9000&force-path-style=true`
  ([PROVIDER_COMPATIBILITY.md @ ab551949](https://github.com/benbjohnson/litestream/blob/ab551949/docs/PROVIDER_COMPATIBILITY.md)).
  `skip-verify` exists for self-signed local MinIO certs.
- **Constraints:** WAL mode only; single writer (it is a disaster-recovery tool, not clustering);
  "Each database replicates to a single replica destination"; "Multiple applications replicating
  into the same bucket & path can cause situations where you will be unable to restore"; requires
  `busy_timeout` set in the app because Litestream itself takes short write locks when
  checkpointing; and under tens of thousands of small write transactions per second you should set
  `PRAGMA wal_autocheckpoint = 0` so the app does not checkpoint behind Litestream's back.

## 5. Migration tooling

**`pressly/goose` — recommended.** `goose.SetBaseFS(embedMigrations)` is documented for exactly this:
"SetBaseFS sets a base FS to discover migrations. It can be used with 'embed' package"
([goose.go](https://github.com/pressly/goose/blob/main/goose.go)). Working SQLite example in the
[embedding-migrations post](https://pressly.github.io/goose/blog/2021/embed-sql-migrations/), and
goose's own `goose_embed_test.go` uses `sql.Open("sqlite", …)` with `db.SetMaxOpenConns(1)` —
i.e. the modernc driver plus the single-writer pool, matching the design above
([goose_embed_test.go](https://github.com/pressly/goose/blob/main/goose_embed_test.go)). Documented
limitation: "This feature can be used only for applying existing migrations. Modifying operations
such as `fix` and `create` will continue to operate on OS filesystem." goose also supports Go
(function) migrations, which matters for SQLite table rebuilds. The newer `goose.NewProvider(...)`
API takes an `fs.FS` directly and pairs with `fs.Sub(embeddedFS, "migrations")`.

**`golang-migrate/migrate`** supports embedding via its `source/iofs` driver and has both `sqlite3`
(cgo) and `sqlite` (modernc) database drivers — pick the non-cgo one to keep the build CGO-free. It
is more CLI/ops-centric and its dirty-state handling requires manual `force` intervention after a
failed migration, which is friction for an embedded control plane.

**`ariga/atlas`** is declarative (desired-schema diffing) with an optional versioned workflow; it
brings a separate binary/HCL toolchain and a hosted-service orientation. Powerful for large teams,
disproportionate for a single embedded SQLite file.

**SQLite ALTER TABLE limits** ([lang_altertable.html](https://www.sqlite.org/lang_altertable.html)):
only RENAME TABLE, RENAME COLUMN, ADD COLUMN and DROP COLUMN are supported. Anything else — changing
a column type, adding/removing a constraint or a `NOT NULL`/`DEFAULT` on existing data, reordering
columns — requires the documented 12-step table-rebuild procedure (create new table, copy, drop,
rename) executed inside a transaction with `PRAGMA foreign_keys=OFF` and a
`PRAGMA foreign_key_check` before commit. ADD COLUMN also restricts defaults: the default must be a
constant, and you cannot add a `PRIMARY KEY`/`UNIQUE` column or a `NOT NULL` column without a
non-null default. Plan the schema so that most changes are additive, and write rebuild migrations
by hand (goose SQL migrations handle this fine as long as you disable FK enforcement around them).

## 6. Alternatives — do any beat SQLite here?

| Option | Query / filter capability | Ops cost | Serious contender? |
|---|---|---|---|
| **bbolt** ([etcd-io/bbolt](https://github.com/etcd-io/bbolt)) | Pure key/value B+tree; no SQL, no secondary indexes — every `status`/`created_at`/`name` filter becomes a hand-rolled index bucket you maintain and keep consistent | Very low: single file, pure Go, no daemon | **No** for ad-hoc filters. Same single-writer story anyway: "Bolt allows only one read-write transaction at a time but allows as many read-only transactions as you want." Documented caveat that "random writes can be slow" |
| **Badger** ([design docs](https://dgraph.io/docs/badger/design)) | LSM + value log KV; "Sorted KV access", ACID with SSI, no query language | Higher: compaction tuning, GC of the value log, larger disk footprint | **No** — optimized for write throughput on SSDs, which is not the bottleneck for control-plane metadata |
| **Pebble** (CockroachDB's engine) | KV LSM, RocksDB lineage; explicitly an engine for a database, not a database. No query layer | Similar to Badger; API is low-level and stability guarantees target CockroachDB's use | **No** |
| **DuckDB** ([go-duckdb](https://github.com/marcboeker/go-duckdb)) | Full SQL, superb analytics — but columnar OLAP, poor fit for row-level UPDATE-heavy state transitions | Requires cgo: "This error is due to the Go compiler determining that CGO is unavailable"; cross-compilation is called out as a known pain point | **No** for OLTP control plane; interesting later for offline workflow analytics over exported data |
| **rqlite** ([design](https://rqlite.io/docs/design/)) | Full SQLite SQL over Raft; separate server process, network hop per query | Multi-node cluster to operate; statement-based replication requires deterministic statements | **No** for single-node; the right answer only if HA becomes a hard requirement |
| **dqlite** ([Canonical docs](https://canonical.com/dqlite/docs)) | Embedded SQLite + C-Raft, frame-based replication, full transaction support, non-deterministic statements OK | C library (cgo) plus cluster membership management | **No** now; the most natural HA upgrade path from SQLite later, at the cost of cgo |
| **embedded-postgres** ([fergusstrange](https://github.com/fergusstrange/embedded-postgres)) | Real Postgres — best query capability of anything on this list | Downloads and supervises a real Postgres binary per platform; the README positions it for *tests*: "Run a real Postgres database locally… as part of another Go application or test. When testing this provides a higher level of confidence than using any in memory alternative" | **No** as a shipped control-plane store; **yes** as an integration-test harness if you ever add a Postgres backend |

The decisive criterion is the one in the brief: *ad-hoc filters*. Only SQLite, DuckDB, rqlite/dqlite
and Postgres give you a query planner and secondary indexes; of those, only SQLite is CGO-free,
single-process, zero-ops, and OLTP-shaped. The KV stores would force you to hand-build and
hand-maintain every index, which is the failure mode this decision should avoid.

## Conflicts, gaps, and limitations

- **Driver benchmarks disagree by workload.** cvilsmeier shows modernc ≈ ncruces ≈ mattn within
  ~30 %; lbe/sqlite-read-benchmark shows ncruces 314 % faster than modernc on prepared reads;
  engineered.at shows all Go drivers well behind the C CLI. Treat all of these as directional.
- **No benchmark found for the exact §3 query shape.** The per-query figures there are my arithmetic
  on cvilsmeier's aggregate numbers, not a measured result on an orkstack-shaped schema. Measure
  with `EXPLAIN QUERY PLAN` plus a Go benchmark before relying on a specific latency budget.
- **Litestream v0.5 is a significant redesign** (LTX/TXID replaced the v0.3.x generation + shadow-WAL
  model). Documentation and blog posts written before v0.5 describe different retention and restore
  semantics; verify against the current docs before configuring retention.
- **modernc/libc version coupling** is a real operational hazard called out by the maintainer; not
  quantified here beyond the upstream issue link.
- I did not independently verify the goose/golang-migrate/atlas comparison against a running SQLite
  database; the goose claims are sourced from upstream code and docs, the golang-migrate and atlas
  characterizations are the weakest-sourced items in this document.
