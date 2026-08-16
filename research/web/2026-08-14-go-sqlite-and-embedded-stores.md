---
title: Go SQLite drivers, SQLite concurrency/durability, and embedded store alternatives
date: 2026-08-14
topic: embedded/single-node Go persistence for the orkstack control plane
status: cached research notes
findings: research/web/sqlite-findings.md
sources:
  - https://github.com/cvilsmeier/go-sqlite-bench/blob/master/README.md
  - https://gitlab.com/cznic/sqlite/-/blob/master/doc.go
  - https://gitlab.com/cznic/sqlite/-/blob/master/sqlite.go
  - https://github.com/ncruces/go-sqlite3/blob/main/README.md
  - https://github.com/mattn/go-sqlite3/blob/master/README.md
  - https://github.com/mattn/go-sqlite3/issues/1022
  - https://www.sqlite.org/wal.html
  - https://www.sqlite.org/isolation.html
  - https://www.sqlite.org/lang_transaction.html
  - https://www.sqlite.org/lang_vacuum.html
  - https://www.sqlite.org/backup.html
  - https://www.sqlite.org/lang_altertable.html
  - https://www.sqlite.org/pragma.html
  - https://litestream.io/how-it-works/
  - https://litestream.io/tips/
  - https://litestream.io/guides/s3-compatible/
  - https://github.com/benbjohnson/litestream/blob/ab551949/docs/PROVIDER_COMPATIBILITY.md
  - https://pressly.github.io/goose/blog/2021/embed-sql-migrations/
  - https://github.com/pressly/goose/blob/main/goose.go
  - https://github.com/etcd-io/bbolt
  - https://dgraph.io/docs/badger/design
  - https://github.com/marcboeker/go-duckdb
  - https://rqlite.io/docs/design/
  - https://canonical.com/dqlite/docs
  - https://github.com/fergusstrange/embedded-postgres
---

# Cached excerpts

## cvilsmeier/go-sqlite-bench (run 2026-03-23, Go 1.26, Debian 12 amd64, i7-1165G7, NVMe; ms)

```
Simple;      insert;  query;      # 1M rows inserted in one tx, then query all
mattn;         1480;    871;
modernc;       2419;    758;
ncruces;       2719;    850;

Real;        insert;  query;      # 100 users, tx per user, join articles+comments
mattn;         1586;    104;
modernc;       1759;    130;
ncruces;       1469;    129;

Complex;     insert;  query;      # 420k rows, then one big JOIN
mattn;          812;    998;
modernc;       1554;   1068;
ncruces;       1749;   1263;

Many;        query/N=10; N=100; N=1000;   # query all users 1000 times
mattn;               20;    96;     819;
modernc;             31;    87;     692;
ncruces;             33;   103;     837;

Concurrent;  query/N=2; N=4; N=8;         # goroutines querying 1M rows
mattn;             948; 1237; 2395;
modernc;           850; 1190; 2061;
ncruces;           981; 1261; 2355;
```

Author's caveats: "Do not trust benchmarks, write your own." / "We cannot declare a clear winner,
it all depends on the use case. SQLite without CGO is possible nowadays."
Schema used `journal_mode=DELETE`, `synchronous=FULL`, `foreign_keys=1`, `busy_timeout=5000`.

## modernc.org/sqlite — doc.go (master)

- "Package sqlite is a sql/database driver using a CGo-free port of the C SQLite3 library."
- Supported GOOS/GOARCH (SQLite 3.53.3): darwin amd64/arm64; freebsd amd64/arm64;
  linux 386/amd64/arm/arm64/loong64/ppc64le/riscv64/s390x; openbsd amd64/arm64;
  windows 386/amd64/arm64.
- "Fragile modernc.org/libc dependency … you should use in your go.mod file the exact same version
  of modernc.org/libc as seen in the go.mod file of this repository."
  (https://gitlab.com/cznic/sqlite/-/issues/177)
- `NewConnector` returns a `driver.Connector` for `sql.OpenDB`.

## modernc.org/sqlite — sqlite.go DSN handling (master, grep)

```
305: // _pragma values are the one exception: they are executed verbatim and
306: // cannot be checked here, so a malformed _pragma can still fail partway.
307: busyKey, busyTimeout := dsnPick(q, "_busy_timeout", "_timeout")
358: if v := q.Get("_time_format"); v != "" {
385: if v := q.Get("_txlock"); v != "" {
418:   c.exec(ctx, "pragma busy_timeout = "+busyTimeout, nil)
433: for _, v := range q["_pragma"] { ... }   // busy_timeout pushed first, rest lexicographic
```

## mattn/go-sqlite3 — README (master, grep)

- "_go-sqlite3_ is *cgo* package."
- "***Important: because this is a `CGO` enabled package, you are required to set the environment
  variable `CGO_ENABLED=1` and have a `gcc` compiler present within your path.***"
- DSN keys: `_busy_timeout`|`_timeout`; `_journal_mode`|`_journal`
  (DELETE/TRUNCATE/PERSIST/MEMORY/WAL/OFF); `_txlock` (immediate/deferred/exclusive).
- Cross-compile: needs `CC` cross compiler; suggests `xgo` or musl-cross, e.g.
  `CC=x86_64-linux-musl-gcc GOARCH=amd64 GOOS=linux CGO_ENABLED=1 go build -ldflags "-linkmode external -extldflags -static"`.

## mattn/go-sqlite3 issue #1022 (maintainer guidance)

> "My general recommendation is to make two pools (as in two `sql.DB`s), one with `mode=ro` and one
> with `mode=rw`. Use wal mode (`_journal_mode=wal`), which will allow reads to happen concurrently
> with writes. Do **not** use shared cache mode." … "you should not call `sql.Open` once per worker,
> as `sql.DB` itself represents a pool, not an individual connection."

## ncruces/go-sqlite3 — README (main)

- "cgo-free SQLite wrapper… wraps a Wasm build of SQLite, and uses wasm2go to translate it to Go.
  Go and x/sys are the only direct dependencies."
- Features: incremental BLOB I/O, nested transactions (savepoints), custom functions, virtual
  tables, custom VFSes, online backup, JSON, encryption at rest.
- "Because each database connection executes within a Wasm sandboxed environment, memory usage will
  be higher than alternatives."
- "This module behaves similarly to SQLite in multi-thread mode: it is goroutine-safe, provided that
  no single database connection, or object derived from it, is used concurrently by multiple
  goroutines."
- CI: linux amd64/arm64/386/arm/riscv64/ppc64le/loong64/s390x, macOS, Windows, FreeBSD, NetBSD,
  DragonFly, OpenBSD, illumos, Solaris. Tested with SQLite's mptest.

## sqlite.org/isolation.html

- "all transactions in SQLite show 'serializable' isolation. SQLite implements serializable
  transactions by actually serializing the writes. There can only be a single writer at a time to an
  SQLite database."
- WAL gives "snapshot isolation": a reader "continues to see an unchanging 'snapshot' of the database
  file as it existed at the moment in time when the read transaction started."
- "The attempt by X to escalate its transaction from a read transaction to a write transaction fails
  with an SQLITE_BUSY_SNAPSHOT error… X can issue BEGIN IMMEDIATE to start its transaction instead
  of just an ordinary BEGIN… If the BEGIN IMMEDIATE operation succeeds, then no subsequent
  operations in that transaction will ever fail with an SQLITE_BUSY error."
- "there is no isolation between operations that occur within the same database connection."

## sqlite.org/wal.html

- §2.2: "Writers merely append new content to the end of the WAL file. Because writers do nothing
  that would interfere with the actions of readers, writers and readers can run at the same time.
  However, since there is only one WAL file, there can only be one writer at a time."
- §2.2: "the use of shared memory means that all readers must exist on the same machine. This is why
  the write-ahead log implementation will not work on a network filesystem."
- §2.2: "a long-running read transaction can prevent a checkpointer from making progress."
- §2.3: "syncing the content to the disk is not required, as long as the application is willing to
  sacrifice durability following a power loss or hard reboot. (Writers sync the WAL on every
  transaction commit if PRAGMA synchronous is set to FULL but omit this sync if PRAGMA synchronous
  is set to NORMAL.)"
- §2.3: "read performance deteriorates as the WAL file grows in size."
- §3.1: automatic checkpoint when the WAL reaches 1000 pages by default.
- §9 "Sometimes Queries Return SQLITE_BUSY In WAL Mode": "there are some obscure cases where a query
  against a WAL-mode database can return SQLITE_BUSY, so applications should be prepared for that
  happenstance."

## sqlite.org/lang_vacuum.html

- "The VACUUM command with an INTO clause is an alternative to the backup API for generating backup
  copies of a live database. The advantage is that the resulting backup is a minimal-size and
  vacuumed file."

## sqlite.org/lang_altertable.html

- Supported: RENAME TO, RENAME COLUMN, ADD COLUMN, DROP COLUMN only.
- Everything else needs the documented multi-step table-rebuild procedure (new table, copy, drop,
  rename) inside a transaction, with `PRAGMA foreign_keys=OFF` and `PRAGMA foreign_key_check`.
- ADD COLUMN restrictions: constant default only; no PRIMARY KEY/UNIQUE; no NOT NULL without a
  non-NULL default.

## Litestream — how-it-works / tips / s3-compatible

- "Litestream is a streaming replication tool for SQLite databases. It runs as a separate background
  process and continuously copies write-ahead log pages from disk to a replica."
- "It starts a long-running read transaction to prevent any other process from checkpointing and
  restarting the WAL file. Instead, it continually reads new WAL pages and manually calls out to
  SQLite to perform checkpoints as necessary."
- v0.5: WAL pages → LTX files with monotonic TXIDs; levels L0 (per sync), L1/L2/L3 compacted every
  30s / 5m / 1h; snapshot every 24h; retention default 24h. v0.3.x used generations + shadow WAL.
- "Litestream replays whole LTX files and never applies part of one, so your restore points are the
  boundaries of the files that still exist in the replica." `l0-retention` default `5m`.
- "Each database replicates to a single replica destination."
- RPO: "By default, Litestream will replicate new changes to an S3 replica every second. During this
  time where data has not yet been replicated, a catastrophic crash on your server will result in
  the loss of data in that time window." Clean shutdown flushes. "Synchronous replication is on the
  Litestream roadmap but has not yet been implemented."
- Requires WAL; recommends `PRAGMA busy_timeout = 5000`, `PRAGMA foreign_keys = ON`,
  `PRAGMA synchronous = NORMAL`, and `PRAGMA wal_autocheckpoint = 0` under very high write load.
- "Multiple applications replicating into the same bucket & path can cause situations where you will
  be unable to restore."
- Deleting a database requires deleting `.db`, `-wal`, and `-shm`; also `litestream reset`.
- S3-compatible: "New in v0.5.0: Litestream automatically detects S3-compatible providers based on
  the endpoint URL." MinIO — "Status: Fully supported… Works well with default settings",
  `url: s3://bucket/path?endpoint=https://minio:9000&force-path-style=true`; `skip-verify` for
  self-signed certs. (docs/PROVIDER_COMPATIBILITY.md @ ab551949)

## goose

- `SetBaseFS(fsys fs.FS)` — "sets a base FS to discover migrations. It can be used with 'embed'
  package. Calling with 'nil' argument leads to default behaviour… Note that modifying operations
  like Create will use os filesystem anyway." (goose.go, main)
- pkg.go.dev: embedding "can be used only for applying existing migrations. Modifying operations
  such as `fix` and `create` will continue to operate on OS filesystem."
- `goose_embed_test.go` uses `sql.Open("sqlite", …)` + `db.SetMaxOpenConns(1)` + `fs.Sub`.
- Provider API takes an `fs.FS` (os.DirFS or embed.FS) directly.

## Alternatives

- bbolt README: "Bolt allows only one read-write transaction at a time but allows as many read-only
  transactions as you want." B+tree single file, pure Go; caveats note random writes can be slow and
  suggest `DB.Batch()`; no query language / secondary indexes.
- Badger design table: LSM + value log, pure Go, ACID with SSI, sorted KV access, TTL; no SQL.
- Pebble: pure-Go RocksDB-lineage LSM, the storage engine under CockroachDB; low-level KV API.
- go-duckdb: cgo required ("`undefined: conn`" when CGO is unavailable; cross-compilation called out
  as a cause); columnar OLAP engine.
- rqlite (rqlite.io/docs/design): Raft-replicated SQLite, separate server, statement-based
  replication (statements must be deterministic).
- dqlite (canonical.com/dqlite/docs): embedded SQLite + C-Raft; vs rqlite — "Can be embedded in any
  language that can inter-operate with C; Full support for transactions; No need for statements to
  be deterministic (e.g. you can use time()); Frame-based replication instead of statement-based."
- embedded-postgres: "Run a real Postgres database locally on Linux, OSX or Windows as part of
  another Go application or test. When testing this provides a higher level of confidence than using
  any in memory alternative."
