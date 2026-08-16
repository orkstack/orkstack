---
title: S3/MinIO as metadata store vs PostgreSQL — cached source excerpts
date: 2026-08-14
tags: [s3, minio, postgres, workflow-engine, metadata]
sources:
  - https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
  - https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html
  - https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html
  - https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-objects-append.html
  - https://aws.amazon.com/s3/consistency/
  - https://github.com/minio/minio/blob/master/docs/distributed/README.md
  - https://docs.min.io/aistor/reference/aistor-server/s3-express/
  - https://www.postgresql.org/docs/current/datatype-json.html
  - https://github.com/temporalio/docker-compose
---

Excerpts cached from the fetches performed on 2026-08-14; see
`research/web/s3-postgres-findings.md` for the analysis.

## S3 performance doc (verbatim)
"your application can achieve at least 3,500 PUT/COPY/POST/DELETE or 5,500 GET/HEAD requests per
second per partitioned Amazon S3 prefix. There are no limits to the number of prefixes in a bucket."
"These applications can achieve consistent small object latencies (and first-byte-out latencies for
larger objects) of roughly 100–200 milliseconds."

## ListObjectsV2
"Returns some or all (up to 1,000) of the objects in a bucket with each request."
Request params: `prefix`, `delimiter`, `max-keys`, `start-after`, `continuation-token`,
`encoding-type`, `fetch-owner`, `optional-object-attributes`, `x-amz-request-payer`,
`x-amz-expected-bucket-owner`. No value/attribute predicates.

## S3 Express append
"Appending data to objects is only supported for objects in directory buckets that are stored in the
S3 Express One Zone storage class." Max 5 GB per append; "each object can have up to 10,000 parts …
you can append data to an object up to 10,000 times"; "Each successful append operation is billed as
a PutObject request."

## MinIO consistency
"MinIO follows strict read-after-write and list-after-write consistency model for all i/o operations
both in distributed and standalone modes. This consistency model is only guaranteed if you use disk
filesystems such as xfs, zfs or btrfs … Please avoid ext4."

## MinIO conditional writes
Blog: "we initially merged support for conditional writes back in February 2023"; PR minio/minio#16551
"support if-match/if-none-match with s3 uploads", merged 2023-02-07, merge commit
11c7ecb5cf947e6428968571cafc0494bb572845.

## MinIO append
AIStor S3 Express mode only: "S3 Express mode supports appending data to an existing object through
the AppendObject behavior … issue a PutObject (HTTP PUT) request that includes the
x-amz-write-offset-bytes header."

## Temporal docker-compose (@ cd6d3b19a3307e2cbbc4c1830228166e92caef24)
`.env`: TEMPORAL_VERSION=1.29.1, POSTGRESQL_VERSION=16, ELASTICSEARCH_VERSION=7.17.27.
Postgres-bearing files: docker-compose.yml (pg + elasticsearch),
docker-compose-postgres.yml (pg only), docker-compose-postgres-opensearch.yml.

## S3 request pricing (us-east-1, S3 Standard, as listed 2026-08-14)
PUT/COPY/POST/LIST $0.005 per 1,000 requests; GET/SELECT $0.0004 per 1,000 requests.

## PostgreSQL jsonb indexing
"Two GIN 'operator classes' are provided… The default GIN operator class for jsonb supports queries
with the key-exists operators ?, ?| and ?&, the containment operator @>, and the jsonpath match
operators @? and @@… The non-default GIN operator class jsonb_path_ops does not support the
key-exists operators, but it does support @>, @? and @@."
