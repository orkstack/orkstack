# S3/MinIO as a workflow-engine metadata store vs PostgreSQL

Date: 2026-08-14. Documentation only; no code changed. Raw excerpts cached in
`research/web/2026-08-14-s3-metadata-and-postgres.md`.

## Summary

S3/MinIO is not a viable metadata store for a workflow engine's query path. The API offers no
server-side filtering beyond key-prefix/delimiter/start-after, caps listings at 1,000 keys per
round trip, has no general append, and imposes ~100–200 ms small-object latency in AWS, so any
"filter runs by status/label" query degenerates into a full prefix scan plus an N+1 GET fan-out
(seconds to minutes at 10k–100k objects, and per-request billing). The one genuinely useful
primitive is conditional writes (`If-None-Match: *`, `If-Match: <etag>`), which give
compare-and-swap on a single object — enough for a manifest/pointer object, not enough for queries.

PostgreSQL solves the query side directly: indexed columns for hot predicates plus a `jsonb` blob
with GIN/`jsonb_path_ops` for the rest, sub-millisecond to low-millisecond lookups at 1M rows.
Crucially, the operational cost in dev is near zero: Temporal's own docker-compose already ships
PostgreSQL 16, so you can reuse the same container (separate database) rather than adding a service.

Recommendation: PostgreSQL as the metadata store; keep S3/MinIO for bulk artifacts (logs, payload
blobs, attachments) referenced by URI from Postgres rows.

## Detailed Findings

### 1. ListObjectsV2: 1,000-key cap, pagination, no server-side filtering

**Source:** [ListObjectsV2 — Amazon S3 API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)
**Authority:** normative API contract.

- "Returns some or all (**up to 1,000**) of the objects in a bucket with each request." `max-keys`
  cannot exceed 1,000; more keys require paging on `continuation-token` (returned as
  `NextContinuationToken` when `IsTruncated` is true).
- The complete request-parameter set is `prefix`, `delimiter`, `max-keys`, `start-after`,
  `continuation-token`, `encoding-type`, `fetch-owner`, `optional-object-attributes`,
  `x-amz-request-payer`, `x-amz-expected-bucket-owner`. **Confirmed: the only selection criteria are
  prefix, delimiter, and start-after.** There is no filter on size, mtime, tags, user metadata, or
  content. `start-after` is only meaningful on the first page (it is ignored once a continuation
  token is supplied).
- MinIO implements the same surface; its S3 API compatibility page lists ListObjectsV2 without extra
  predicates ([MinIO S3 API compatibility](https://docs.min.io/aistor/developers/s3-api-compatibility/)).
- S3 Select (per-object SQL over CSV/JSON/Parquet) was the only "server-side filter" and it is
  per-object, not per-listing; AWS has since deprecated it for new customers. Object tagging is
  filterable only by lifecycle/replication rules, never by List.

**Latency and cost of a prefix scan.** AWS publishes small-object/first-byte latency of "roughly
100–200 milliseconds" ([performance best practices](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html)).
List requests are billed at the PUT/COPY/POST/**LIST** rate, $0.005 per 1,000 requests
([S3 pricing](https://aws.amazon.com/s3/pricing/)):

| Objects under prefix | List round trips | Serial wall-clock @150 ms | List cost |
| --- | --- | --- | --- |
| 1,000 | 1 | ~0.15 s | $0.000005 |
| 10,000 | 10 | ~1.5 s | $0.00005 |
| 100,000 | 100 | ~15 s | $0.0005 |

Pagination is inherently serial (each token depends on the previous response), so it cannot be
parallelized within one prefix; only splitting into disjoint prefixes parallelizes. MinIO on local
disk is faster (sub-10 ms per page on a laptop) which will mask the problem in dev and expose it in
production.

### 2. Strong consistency (Dec 2020) — and what LIST guarantees

**Source:** [Amazon S3 Strong Consistency](https://aws.amazon.com/s3/consistency/),
[AWS News blog, 1 Dec 2020](https://aws.amazon.com/blogs/aws/amazon-s3-update-strong-read-after-write-consistency/),
[Werner Vogels, "Diving Deep on S3 Consistency"](https://www.allthingsdistributed.com/2021/04/s3-strong-consistency.html)

- Covered: PUT of a new object, overwrite PUT, and DELETE — "any subsequent read request immediately
  receives the latest version of the object." **LIST is included**: S3 "provides strong consistency
  for list operations, so after a write, you can immediately perform a listing of the objects in a
  bucket with any changes reflected." No performance, availability, or cost penalty; applies to all
  regions and all requests.
- Caveats that still bite: bucket *configuration* (ACLs, policies, CORS, lifecycle) remains
  eventually consistent; cross-region replication is asynchronous; and S3 gives no multi-object
  atomicity or transactions. Strong consistency ≠ isolation — two writers still race unless you use
  conditional writes (§3).

**MinIO:** "MinIO follows strict read-after-write and list-after-write consistency model for all i/o
operations both in distributed and standalone modes. This consistency model is only guaranteed if
you use disk filesystems such as xfs, zfs or btrfs… **Please avoid ext4**"
([minio/minio docs/distributed/README.md](https://github.com/minio/minio/blob/master/docs/distributed/README.md#consistency-guarantees)).
Note the filesystem precondition and the degraded-mode caveat raised in
[minio/minio#21284](https://github.com/minio/minio/discussions/21284), where users observed deleted
objects still listed while a node in the pool was offline. Treat MinIO's guarantee as
"strict when healthy," not as a formal linearizability claim.

### 3. Conditional writes (optimistic concurrency)

**Sources:** [AWS What's New, 20 Aug 2024](https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/),
[AWS What's New, 25 Nov 2024](https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-functionality-conditional-writes/),
[S3 user guide: conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)

- Aug 2024: `If-None-Match: *` on `PutObject` / `CompleteMultipartUpload` — create-if-absent, returns
  `412 PreconditionFailed` if the key exists. Works in general purpose and directory buckets.
- Nov 2024: `If-Match: <etag>` on PUT/CompleteMultipartUpload — update-only-if-unchanged, i.e. real
  compare-and-swap. This is the primitive Iceberg/Delta catalogs now use to commit without a lock
  service.
- Semantics follow RFC 7232 §3.1/§3.2 ([RFC 7232](https://datatracker.ietf.org/doc/html/rfc7232));
  precedence rules in §6 dictate 412 (not 304) for non-GET methods.

**MinIO** supported both headers *before* AWS: PR
[minio/minio#16551 "support if-match/if-none-match with s3 uploads"](https://github.com/minio/minio/pull/16551),
merged 2023-02-07 (merge commit `11c7ecb5cf947e6428968571cafc0494bb572845`), and MinIO's own blog
["Leading the Way: MinIO's Conditional Write Feature"](https://www.min.io/blog/leading-the-way-minios-conditional-write-feature-for-modern-data-workloads)
states "we initially merged support for conditional writes back in February 2023." So any MinIO
release from `RELEASE.2023-02-xx` onward has it. RFC-compliance fixes landed later:
[#19442](https://github.com/minio/minio/issues/19442) (PUT returned 304 instead of 412) fixed by
commit [7bb0f32 "make if-none-match PUT/POST RFC compliant (#19448)"](https://github.com/minio/minio/commit/7bb0f3233205d68e12081fb6a90ce0429663a89f),
and 2025 fixes for multipart and non-existent-object cases (minio/minio #21550, #21567). Pin a
2025-or-later MinIO if you depend on exact 412 semantics, and assert them in a conformance test —
S3-compatible gateways vary here.

Practically: conditional writes give you CAS on one object. That is enough for a single
"current state pointer" or manifest object per run, and it is the correct pattern if you insist on
object storage. It gives you no cross-object transaction, no secondary index, and no queue.

### 4. Append: effectively unavailable → read-modify-write per event

**Source:** [Appending data to objects in directory buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-objects-append.html)

- "Appending data to objects is **only supported for objects in directory buckets that are stored in
  the S3 Express One Zone storage class**." Mechanism: `PutObject` with `x-amz-write-offset-bytes`.
- Even there, limits are hard: max 5 GB per append, and "each object can have up to 10,000 parts…
  you can append data to an object **up to 10,000 times**" before `TooManyParts` (recoverable only
  via `CopyObject` to reset). "Each successful append operation is billed as a PutObject request."
- Standard general-purpose S3 has **no append**. `PutObject` docs: "You cannot use PutObject to only
  update a single piece of metadata for an existing object. You must put the entire object."

**Consequence for `runs/{id}.jsonl`:** on standard S3/MinIO every appended event requires
GET (whole object) → concat → PUT (whole object). For a run emitting N events the total bytes
written are O(N²) in event size, and you need `If-Match` on each PUT to avoid lost updates under
concurrent writers, with retry on 412. A 5,000-event run averaging 500 B/event ends at 2.5 MB and
has written ~6 GB cumulatively. Even on S3 Express, 10,000 appends caps the event count per run.
Workarounds are all "don't use one object": one object per event (`runs/{id}/events/{seq}.json`,
which reintroduces the N+1 read problem in §6) or periodic batch flushing (which loses the
low-latency tail).

**MinIO:** community MinIO has no append —
[minio/minio#20949 "Support s3 WriteOffsetBytes"](https://github.com/minio/minio/issues/20949),
[minio-go#1167](https://github.com/minio/minio-go/issues/1167) ("S3 API doesn't implement append
APIs"), [minio-java#980](https://github.com/minio/minio-java/issues/980) ("You would need to
download… then upload the file"). Append exists only in the commercial AIStor product under
"S3 Express mode": "S3 Express mode supports appending data to an existing object through the
`AppendObject` behavior… Appending is available only when S3 Express mode is enabled"
([AIStor S3 Express mode](https://docs.min.io/aistor/reference/aistor-server/s3-express/)). Relying
on it means both a paid MinIO tier and an AWS storage class you probably aren't using in prod.

### 5. Request-rate limits per prefix

**Source:** [Best practices design patterns: optimizing Amazon S3 performance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html)

> "your application can achieve at least **3,500 PUT/COPY/POST/DELETE or 5,500 GET/HEAD requests per
> second per partitioned Amazon S3 prefix**. There are no limits to the number of prefixes in a
> bucket."

Two operational caveats from the same page: partitioning "happens gradually and is not
instantaneous," and during scale-up "you may see some 503 (Slow Down) errors." A hot-run design that
concentrates writes under `runs/{id}/` therefore has a hard ~3,500 writes/s ceiling for that run,
and a newly created prefix starts below it. LIST requests count against the write-side bucket.

### 6. Object storage as a database: patterns and the N+1 cost

**Pattern that works — manifest/index objects.** Iceberg and Delta encode the queryable index into
a small number of objects (metadata JSON → manifest list → manifests), so a query reads O(few)
objects instead of listing millions. Delta/Iceberg catalogs now commit via `If-Match`/`If-None-Match`
CAS on the pointer object. The equivalent here is: one `runs/index.json` (or per-shard index)
maintained with conditional writes, with per-run objects read only after the index narrows the set.

**Pattern that fails — list-then-GET-each (N+1).** Sources documenting exactly this failure:
- [Apache Iceberg Production Anti-Patterns](https://iomete.com/resources/blog/apache-iceberg-production-antipatterns-2026):
  "With 100,000 files, that's 100,000 metadata reads. Each read has a fixed cost—object storage
  latency, HTTP overhead, deserialization. The query planning phase that used to take 200
  milliseconds now takes 45 seconds."
- [Iceberg Metadata at Scale](https://lakeops.dev/blog/iceberg-metadata-at-scale): "Query planning
  drifts from milliseconds to minutes. Coordinators OOM. S3 GET costs spike."
- [Iceberg, The Right Idea — The Wrong Spec](https://www.database-doctor.com/posts/iceberg-is-wrong-2)
  enumerates the structural gaps of object storage as a DB: "Granular Concurrency control /
  Atomicity across multiple objects / … Low latency, high throughput access to metadata."
- Real-world blowback even with pooling:
  [apache/iceberg#14951](https://github.com/apache/iceberg/issues/14951) — TIME_WAIT socket
  exhaustion from manifest reads.

Quantifying the N+1 for "list 1,000 keys, then GET each," using AWS's own 100–200 ms figure:

| Concurrency | Wall clock (1,000 GETs @150 ms) | Wall clock (100k GETs) |
| --- | --- | --- |
| 1 (serial) | ~150 s | ~4.2 h |
| 64 | ~2.3 s | ~4 min |
| 256 | ~0.6 s | ~1 min |
| 1,000 | ~0.15 s (hits per-prefix caps at scale) | ~15 s |

Cost per full scan: 1,000 GETs = $0.0004; 100k GETs = $0.04. A dashboard polling a 100k-run
namespace every 10 s costs ~$10k/month in GET requests alone and needs hundreds of concurrent
connections. The equivalent Postgres query is one indexed scan. MinIO on local disk hides all of
this in dev, which is the trap.

Additional reading on the general question:
[Building a database on S3 (Brantner et al., SIGMOD 2008)](https://dl.acm.org/doi/10.1145/1376616.1376645)
— the original analysis of latency/consistency tradeoffs; and
[Amazon S3 Express One Zone](https://aws.amazon.com/s3/storage-classes/express-one-zone/), whose
single-digit-millisecond pitch exists precisely because standard S3 latency is unsuitable for
metadata access.

---

## PART B — PostgreSQL

### 7. JSONB indexing, expected latency, and protobuf-as-JSONB

**Source:** [PostgreSQL 8.14. JSON Types](https://www.postgresql.org/docs/current/datatype-json.html)

- "Two GIN 'operator classes' are provided… The default GIN operator class for `jsonb` supports
  queries with the key-exists operators `?`, `?|` and `?&`, the containment operator `@>`, and the
  jsonpath match operators `@?` and `@@`… The non-default GIN operator class `jsonb_path_ops` does
  not support the key-exists operators, but it does support `@>`, `@?` and `@@`."
- `jsonb_path_ops` indexes hashes of full paths, so it is materially smaller and faster for pure
  containment workloads — the usual choice when your only predicate is `meta @> '{"status":"failed"}'`.
  Use the default class if you need `?` key-existence.
- Targeted expression indexes beat blanket GIN: "targeted expression indexes are likely to be smaller
  and faster to search than a simple index." Also see
  [Postgres GIN documentation](https://www.postgresql.org/docs/current/gin-intro.html) on
  `fastupdate` and the pending list — GIN insert cost is the main write-side tradeoff, tunable via
  `gin_pending_list_limit`.

**Practical schema advice for a workflow engine:** promote hot predicates
(`run_id`, `workflow_id`, `status`, `created_at`, `namespace`, `parent_id`) to real typed columns
with btree indexes; keep the rest of the metadata in one `jsonb` column with `jsonb_path_ops` GIN.
B-tree lookups on an indexed column are ~0.05–0.5 ms at 1M rows once the index is cached; a GIN
containment probe returning few rows is typically low single-digit ms; the cost driver is recheck on
low-selectivity predicates, not index size. At the 1k–1M row scale in question, everything fits in
shared buffers on a modest instance and none of this is a performance question — measure with
`EXPLAIN (ANALYZE, BUFFERS)` rather than trusting any published number, including these.

**Protobuf as JSONB — sane, with rules.** Use `protojson` (canonical JSON mapping) rather than a
hand-rolled encoder, so field names, enums, and well-known types round-trip predictably. Caveats:
(a) `jsonb` does not preserve key order or duplicate keys, so never treat the stored bytes as the
canonical wire message — if you need byte-exact protobuf, store `BYTEA` alongside and treat `jsonb`
as a derived, queryable projection; (b) `protojson` output is explicitly not stable across library
versions, so don't hash it; (c) int64 fields serialize as JSON *strings* in canonical protojson,
which affects your predicates; (d) `jsonb` numbers are `numeric` — no float precision surprises, but
also no int64 fast path. Storing both `BYTEA` (source of truth) and `jsonb` (index/query) is the
common, defensible design.

### 8. Go driver: pgx v5 pool vs database/sql

**Source:** [jackc/pgx v5 package docs](https://pkg.go.dev/github.com/jackc/pgx/v5),
[pgxpool](https://pkg.go.dev/github.com/jackc/pgx/v5/pgxpool)

- pgx offers two modes: the native interface (`pgx.Conn` / `pgxpool.Pool`) and a `database/sql`
  driver via `pgx/stdlib`. The native path is the recommended one when you're Postgres-only.
- Native advantages that matter here: the extended query protocol with native binary encoding
  (avoids text round-trips for timestamps/numerics/UUIDs), `pgtype` support for arrays, `jsonb`,
  `hstore`, and composite types, `CopyFrom` for bulk inserts, `SendBatch` for pipelining, and
  `LISTEN`/`NOTIFY` — none of which `database/sql`'s interface exposes cleanly.
- `pgxpool` is a dedicated connection pool with `MaxConns`, `MinConns`, `MaxConnLifetime`,
  `MaxConnIdleTime`, `HealthCheckPeriod`, and `BeforeAcquire`/`AfterRelease` hooks; it's generally
  better behaved than `database/sql`'s pool for Postgres-specific lifecycle needs.
- Choose `database/sql` + `pgx/stdlib` only if you need `sqlx`/ORM/mock ecosystem compatibility or
  genuine multi-database portability. Note that pgx v5 changed `QueryRow`/scan semantics and the
  `pgtype` API versus v4; budget for that if any existing code is on v4.
- Migration tooling: the low-cost options are
  [`golang-migrate/migrate`](https://github.com/golang-migrate/migrate) (plain up/down SQL files,
  works as a library or CLI),
  [`pressly/goose`](https://github.com/pressly/goose) (SQL or Go migrations, embeddable via
  `embed.FS`), and [Atlas](https://atlasgo.io/) (declarative/versioned, heavier). For a project of
  this size, goose or golang-migrate with `embed.FS` costs roughly a day: one `migrations/` dir, one
  `Migrate()` call at startup, one CI check that the schema is current. Note golang-migrate's dirty-
  state handling on failed migrations requires a documented manual recovery step.

### 9. Operational cost in a dev stack that already runs Temporal + MinIO — near zero

**Source:** [temporalio/docker-compose](https://github.com/temporalio/docker-compose) @
`cd6d3b19a3307e2cbbc4c1830228166e92caef24` (main as of 2026-01-05)

Confirmed: **Temporal's docker-compose already ships PostgreSQL 16** in the postgres variants.

- [`.env`](https://github.com/temporalio/docker-compose/blob/cd6d3b19a3307e2cbbc4c1830228166e92caef24/.env):
  `POSTGRESQL_VERSION=16`, `TEMPORAL_VERSION=1.29.1`, `TEMPORAL_UI_VERSION=2.34.0`,
  `ELASTICSEARCH_VERSION=7.17.27`, plus `POSTGRES_USER=temporal` / `POSTGRES_PASSWORD=temporal`.
- Files containing a `postgresql` service:
  - [`docker-compose.yml`](https://github.com/temporalio/docker-compose/blob/cd6d3b19a3307e2cbbc4c1830228166e92caef24/docker-compose.yml)
    — the **default** file: PostgreSQL + Elasticsearch + Temporal + UI.
  - [`docker-compose-postgres.yml`](https://github.com/temporalio/docker-compose/blob/cd6d3b19a3307e2cbbc4c1830228166e92caef24/docker-compose-postgres.yml)
    — PostgreSQL only (no Elasticsearch), publishes `5432:5432` on the host.
  - [`docker-compose-postgres-opensearch.yml`](https://github.com/temporalio/docker-compose/blob/cd6d3b19a3307e2cbbc4c1830228166e92caef24/docker-compose-postgres-opensearch.yml)
    — PostgreSQL + OpenSearch.
  - Non-postgres variants: `docker-compose-mysql.yml`, `docker-compose-mysql-es.yml`,
    `docker-compose-cass-es.yml`.

Service definition, verbatim from `docker-compose-postgres.yml` at that SHA:

```yaml
  postgresql:
    container_name: temporal-postgresql
    environment:
      POSTGRES_PASSWORD: temporal
      POSTGRES_USER: temporal
    image: postgres:${POSTGRESQL_VERSION}
    networks:
      - temporal-network
    ports:
      - 5432:5432
```

The [README](https://github.com/temporalio/docker-compose/blob/cd6d3b19a3307e2cbbc4c1830228166e92caef24/README.md)
confirms: "The default configuration file (`docker-compose.yml`) uses a PostgreSQL database, an
Elasticsearch instance, and exposes the Temporal gRPC Frontend on port 7233."

**What this means for cost:** adding Postgres to your stack is not adding a container — it's adding
a `CREATE DATABASE orkstack;` to an already-running one. Two options:

1. *Reuse the Temporal Postgres, separate database.* Zero new containers, zero new RAM, one init SQL
   file. Correct for dev. Note the compose file uses an anonymous volume
   (`volumes: - /var/lib/postgresql/data`, no host/named mapping), so data does **not** survive
   `docker compose down -v` and is not guaranteed across recreates — fine for dev, but add a named
   volume if you want persistence.
2. *Separate `orkstack-postgres` container.* Cleaner isolation and matches prod topology, at the cost
   of ~1 extra container and ~50–100 MB idle RSS. Worth it if Temporal's compose file is treated as
   a vendored, unmodified artifact.

Either way the marginal operational cost is far below the cost of building indexing, pagination, and
concurrency control on top of object storage. In production, managed Postgres (RDS/Cloud SQL) is a
solved operational problem; S3-as-metadata-store is not.

## Additional Resources

- [Amazon S3 Update – Strong Read-After-Write Consistency (Barr, 1 Dec 2020)](https://aws.amazon.com/blogs/aws/amazon-s3-update-strong-read-after-write-consistency/)
- [Diving Deep on S3 Consistency (Vogels, Apr 2021)](https://www.allthingsdistributed.com/2021/04/s3-strong-consistency.html)
- [RFC 7232 — HTTP/1.1 Conditional Requests](https://datatracker.ietf.org/doc/html/rfc7232) (§3.1 If-Match, §3.2 If-None-Match, §6 precedence)
- [Simplify log rotation with S3 Express One Zone (AWS Storage Blog)](https://aws.amazon.com/blogs/storage/simplify-log-rotation-with-amazon-s3-express-one-zone/) — the append use case, and its limits
- [MinIO: Strict Consistency is a Hard Requirement for Primary Storage](https://www.min.io/blog/strict-consistency-hard-requirement-for-primary-storage)
- [PostgreSQL GIN index internals](https://www.postgresql.org/docs/current/gin-intro.html)
- [pgxpool package docs](https://pkg.go.dev/github.com/jackc/pgx/v5/pgxpool)

## Conflicts, Gaps, or Limitations

- **Latency figures are AWS's own published range (100–200 ms), not independently benchmarked here.**
  The N+1 table extrapolates from that figure; I did not run a benchmark. Actual per-request latency
  varies with region, object size, connection reuse, and whether the client is in-VPC. MinIO on local
  disk is roughly an order of magnitude faster and will not reproduce these numbers in dev.
- **PostgreSQL query-latency figures are engineering estimates, not measurements.** No benchmark was
  run against a 1M-row table in this session. Treat them as order-of-magnitude only and verify with
  `EXPLAIN (ANALYZE, BUFFERS)` on your real schema.
- **MinIO conditional-write correctness is version-sensitive.** Support dates to Feb 2023 (PR #16551),
  but RFC-precedence and multipart bugs were still being fixed in 2024 (#19448) and 2025 (#21550,
  #21567). I confirmed the PR merge date and commit via the GitHub API but did not verify behavior
  against a running MinIO. Pin a recent release and write a conformance test if you depend on 412.
- **MinIO's consistency guarantee is not formally specified** and has a documented filesystem
  precondition plus reported anomalies during node outages (minio/minio#21284, unresolved
  discussion). It should not be treated as equivalent to S3's post-2020 guarantee.
- **Temporal compose facts are pinned to `cd6d3b19` (main, 2026-01-05).** `POSTGRESQL_VERSION=16` and
  `TEMPORAL_VERSION=1.29.1` will drift; re-check `.env` at whatever tag you vendor. Also note the
  compose files still declare `version: "3.5"`, which modern Docker Compose warns about as obsolete.
- **`gh` CLI was unauthenticated in this session**, so repository facts were gathered via the public
  GitHub REST API and raw.githubusercontent.com rather than `gh`. Content is verbatim from the pinned
  SHA.
- **Not investigated:** S3 Tables / S3 Metadata Tables (2024–2025 Iceberg-backed offerings), which
  are a genuinely different answer to "queryable metadata on S3" but introduce Iceberg and an AWS
  lock-in surface; and DynamoDB or SQLite-on-object-storage alternatives, which were out of scope.
