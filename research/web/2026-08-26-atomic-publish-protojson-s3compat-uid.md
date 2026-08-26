---
source_url:
  - https://pubs.opengroup.org/onlinepubs/9799919799.2024edition/functions/linkat.html
  - https://man7.org/linux/man-pages/man2/link.2.html
  - https://pkg.go.dev/google.golang.org/protobuf/encoding/protojson
  - https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html
  - https://docs.docker.com/engine/storage/bind-mounts/
fetched_at: 2026-08-26
fetch_method: fetch_content, web_search, GitHub API, git source inspection
topic: atomic filesystem publication, protojson stability, S3-compatible conditional writes, bind-mount UID/GID behavior
---

# Atomic publish, protojson, S3-compatible conditional writes, and UID passthrough

## Executive conclusions

1. POSIX explicitly requires `link()` to atomically create the hard link, and both POSIX and Linux specify failure with `EEXIST` when the destination exists. This is a genuine atomic create-if-absent primitive on one filesystem. Ordinary `rename()` is atomic publication but **silently replaces** an existing destination, so it is not create-if-absent. Linux adds `renameat2(..., RENAME_NOREPLACE)` and the unnamed-file `O_TMPFILE` + `linkat()` pattern.
2. Go `protojson` output is deliberately destabilized with extra whitespace. It must not be used for byte comparison or hashing. Binary `proto.MarshalOptions{Deterministic:true}` is repeatable within the same binary, but its own current doc explicitly says it is not canonical and is not guaranteed stable over time.
3. `If-None-Match: *` on `PutObject` is not a generic consequence of “S3 compatibility.” Current evidence: MinIO yes; Ceph RGW yes for simple unversioned PUT but with version/multipart caveats; Cloudflare R2 yes; GCS XML **no for that header** (use `x-goog-if-generation-match: 0`); Backblaze B2 no; SeaweedFS yes from 3.97; Garage no; LocalStack yes.
4. With a native Linux Docker Engine daemon and no user namespace, a bind mount is the host inode and numeric ownership is not remapped. Docker Desktop uses VM file-sharing mediation (VirtioFS/gRPC-FUSE), so native-Linux UID conclusions cannot be carried over. However, the blanket statement “Desktop remaps ownership to the container user” is too strong for current VirtioFS: behavior is virtualized and version/backend-dependent, and Docker release notes even document fake `chown` whose result is not reflected by `stat`.

## 1. Filesystem publication

### POSIX `link(2)` is atomic and create-if-absent

The current [Open Group POSIX.1-2024 `link`/`linkat` specification](https://pubs.opengroup.org/onlinepubs/9799919799.2024edition/functions/linkat.html) says verbatim:

> “The `link()` function shall atomically create a new hard link for the existing file and the link count of the file shall be incremented by one.”

It also requires:

> “[EEXIST] The path2 argument resolves to an existing directory entry or refers to a symbolic link.”

and:

> “If `link()` fails, no link shall be created and the link count of the file shall remain unchanged.”

The current [Linux `link(2)` manual](https://man7.org/linux/man-pages/man2/link.2.html) agrees:

> “If newpath exists, it will not be overwritten.”
>
> “EEXIST newpath already exists.”

Constraints that matter: old and new names must be on the same mounted filesystem (`EXDEV` otherwise), and the filesystem must support hard links.

### Is this a canonical idiom?

**Yes for hard-link lock/create-if-absent schemes and Git loose-object publication; no, not accurately, for canonical Maildir delivery.** The [Linux `open(2)` manual's `O_EXCL` NFS discussion](https://man7.org/linux/man-pages/man2/open.2.html) describes the portable lockfile algorithm: create a unique file on the same filesystem, then use `link(2)` to the lock name; success of `link()` establishes the lock atomically. The [lockfile-progs `lockfile_create(3)` manual](https://man.archlinux.org/man/lockfile_create.3.en) likewise says, “Then the lockfile is created using link(2). The return value of link is ignored.”

Git is direct production evidence. At current Git commit [`f78ce2f7b6df702f93d40b85d6bda92a3f65da79`](https://github.com/git/git/blob/f78ce2f7b6df702f93d40b85d6bda92a3f65da79/object-file.c#L401-L419), loose-object finalization does:

```c
if (cfg->object_creation_mode == OBJECT_CREATION_USES_RENAMES)
        goto try_rename;
else if (link(tmpfile, filename))
        ret = errno;
else
        unlink_or_warn(tmpfile);
...
if (ret && ret != EEXIST) {
```

Linus Torvalds' contemporary Git mailing-list explanation is unusually explicit: [“create a temporary file … and then when it's all done, I do a `link()` to the final place and unlink the original”](https://public-inbox.org/git/118833cc050504023569e00d38@mail.gmail.com/T/).

The Maildir example should be corrected rather than repeated. D. J. Bernstein's canonical [Maildir delivery procedure](https://cr.yp.to/proto/maildir.html) writes and closes `tmp/unique`, then says:

> “Now move `tmp/unique` to `new/unique`. The message has been delivered.”

Maildir depends on a unique final name and an atomic move/rename; it is not the canonical fixed-name hard-link create-if-absent example.

### NFS caveat

The [Linux `link(2)` BUGS section](https://man7.org/linux/man-pages/man2/link.2.html#BUGS) is exact:

> “On NFS filesystems, the return code may be wrong in case the NFS server performs the link creation and dies before it can say so. Use `stat(2)` to find out if the link got created.”

Thus namespace creation remains server-atomic, but an RPC failure can make the client's result ambiguous. A retry can encounter `EEXIST` for its own successful first attempt; verify identity/link state rather than treating every reported failure as proof of nonpublication.

### `rename(2)` replaces the destination

The [Linux `rename(2)` manual](https://man7.org/linux/man-pages/man2/rename.2.html) says:

> “If newpath already exists, it will be atomically replaced, so that there is no point at which another process attempting to access newpath will find it missing.”

POSIX likewise states in the [Open Group `rename` specification](https://pubs.opengroup.org/onlinepubs/9799919799/functions/rename.html) that if `new` names an existing non-directory file, it “shall be removed and `old` renamed to `new`.” Consequently plain rename is atomic replacement, **not** an atomic “win only if absent” operation.

### Linux alternatives

* [`renameat2(2)`](https://man7.org/linux/man-pages/man2/rename.2.html) was added in **Linux 3.15**. `RENAME_NOREPLACE` means: “Don't overwrite newpath of the rename. Return an error if newpath already exists.” It requires filesystem support. The same man page dates support: ext4 3.15; btrfs/tmpfs/cifs 3.17; XFS 4.0; broad filesystem support (ext2, minix, reiserfs, jfs, vfat, bpf, overlayfs) 4.9. It is Linux-specific, so callers need an unsupported-kernel/filesystem fallback.
* [`open(2)` `O_TMPFILE`](https://man7.org/linux/man-pages/man2/open.2.html) creates an unnamed inode in a directory, allowing the caller to write, `fsync`, set metadata, and only then attach it to the namespace with [`linkat(fd, "", dirfd, name, AT_EMPTY_PATH)`](https://man7.org/linux/man-pages/man2/link.2.html). Do **not** include `O_EXCL` if it must later be linked: the open manual says `O_EXCL` with `O_TMPFILE` prevents linking. `O_TMPFILE` is Linux-specific and underlying filesystem support varies; the open manual lists initial support in Linux 3.11 (ext2/3/4, UDF, Minix) and later additions including XFS 3.15, Btrfs/F2FS 3.16, ubifs 4.9, tmpfs 5.11.

## 2. Go protobuf JSON and deterministic binary serialization

### `protojson` is deliberately unstable

The current [`protojson` package docs](https://pkg.go.dev/google.golang.org/protobuf/encoding/protojson#Marshal) say for `Marshal`:

> “Do not depend on the output being stable. Its output will change across different builds of your program, even when using the same version of the protobuf module.”

The same warning is attached to `MarshalOptions.Marshal` and `Format`. Therefore JSON bytes are unsupported for hashes, signatures, content addresses, cache keys, or byte-for-byte golden comparisons. Compare decoded semantics instead.

Upstream commit [`582ab3de426ef0758666e018b422dd20390f7f26`](https://github.com/protocolbuffers/protobuf-go/commit/582ab3de426ef0758666e018b422dd20390f7f26) is titled **“encoding/protojson: add random whitespaces in encoding output”** and its commit message says:

> “This is meant to deter users from doing byte for byte comparison.”

The current implementation still does so. At commit [`f4a54025970a405fb35cada3b865101d8fa8ab9d`](https://github.com/protocolbuffers/protobuf-go/blob/f4a54025970a405fb35cada3b865101d8fa8ab9d/internal/encoding/json/encode.go#L234-L243):

```go
// For single-line output, add a random extra space after each
// comma to make output unstable.
if detrand.Bool() {
        e.out = append(e.out, ' ')
}
```

and multiline output has the analogous extra-space branch at [lines 270–275](https://github.com/protocolbuffers/protobuf-go/blob/f4a54025970a405fb35cada3b865101d8fa8ab9d/internal/encoding/json/encode.go#L270-L275).

Relevant upstream `golang/protobuf` issues are [#1082](https://github.com/golang/protobuf/issues/1082) (request to opt out rejected; maintainers explain no canonical JSON serialization), [#1121](https://github.com/golang/protobuf/issues/1121) (request for canonical output; explicitly identifies the detrand change), [#1269](https://github.com/golang/protobuf/issues/1269), [#1351](https://github.com/golang/protobuf/issues/1351), [#1373](https://github.com/golang/protobuf/issues/1373), and [#1628](https://github.com/golang/protobuf/issues/1628). In #1373 the maintainer response says this is “operating as intended” and explains that minor differences are deliberately introduced because protobuf JSON defines valid semantics, not a canonical byte format.

### Supported repeatable form: deterministic protobuf wire bytes

Use:

```go
b, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
```

The **current complete relevant doc comment**, verbatim from [`proto/encode.go` at `f4a540…`](https://github.com/protocolbuffers/protobuf-go/blob/f4a54025970a405fb35cada3b865101d8fa8ab9d/proto/encode.go#L34-L55), is:

> “Deterministic controls whether the same message will always be serialized to the same bytes within the same binary.
>
> Setting this option guarantees that repeated serialization of the same message will return the same bytes, and that different processes of the same binary (which may be executing on different machines) will serialize equal messages to the same bytes. It has no effect on the resulting size of the encoded message compared to a non-deterministic marshal.
>
> Note that the deterministic serialization is NOT canonical across languages. It is not guaranteed to remain stable over time. It is unstable across different builds with schema changes due to unknown fields. Users who need canonical serialization (e.g., persistent storage in a canonical form, fingerprinting, etc.) must define their own canonicalization specification and implement their own serializer rather than relying on this API.
>
> If deterministic serialization is requested, map entries will be sorted by keys in lexographical order. This is an implementation detail and subject to change.”

So this API supports same-message repeatability within/same binary, **not a permanent cross-version content-addressing contract**.

## 3. `PutObject` with `If-None-Match: *` across S3-compatible stores

AWS itself documents the semantics in [“How to prevent object overwrites with conditional writes”](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html):

> “The `If-None-Match` header prevents overwrites of existing data by validating that there's not an object with the same key name already in your bucket.”

Backend status as of 2026-08-26:

| Backend | Status | Verifiable evidence and qualification |
|---|---|---|
| **MinIO / AIStor** | **Supported** | Official [S3 API compatibility docs](https://docs.min.io/aistor/developers/s3-api-compatibility/) list `If-None-Match` for `PutObject`. Upstream [PR #16551](https://github.com/minio/minio/pull/16551) (“support if-match/if-none-match with s3 uploads”) merged 2023-02-07; it is included in [RELEASE.2023-02-17T17-52-43Z](https://github.com/minio/minio/releases/tag/RELEASE.2023-02-17T17-52-43Z). Caveat: later reports identify edge bugs under lost read quorum ([#21603](https://github.com/minio/minio/issues/21603)) and some `If-Match` behavior ([#21526](https://github.com/minio/minio/issues/21526)); pin/test the deployed release. |
| **Ceph RADOS Gateway** | **Supported for ordinary unversioned single PUT; caveats** | Ceph tracker [#68183](https://tracker.ceph.com/issues/68183) says “Ceph supports conditional writing since 2014” and demonstrates second `If-None-Match: *` simple PUT returning 412. It also documents that affected Ceph v18.2.2 ignored the condition for versioned buckets and multipart; fix merged as `785022…`, “Fixed In: v20.3.0-1937…”, with Squid/Tentacle backports tracked. Treat versioned and multipart use as release-specific until verified. |
| **Cloudflare R2** | **Supported** | Official [R2 S3 extensions docs](https://developers.cloudflare.com/r2/api/s3/extensions/#conditional-operations-in-putobject) state: “`PutObject` supports conditional uploads via the If-Match, If-None-Match, If-Modified-Since, and If-Unmodified-Since headers,” rejected with `412 PreconditionFailed` when unsatisfied. |
| **Google Cloud Storage XML API** | **Not supported under the S3 header; equivalent vendor precondition supported** | Official [XML `PUT Object` docs](https://cloud.google.com/storage/docs/xml-api/put-object) do not list `If-None-Match`; official [header reference](https://cloud.google.com/storage/docs/xml-api/reference-headers#ifnonematch) limits it to HEAD/GET. Use `x-goog-if-generation-match: 0`, documented as “only created if there is no existing object.” Therefore software hard-coded to AWS's `If-None-Match: *` is not portable to GCS XML unchanged. |
| **Backblaze B2 S3 API** | **Not supported** | Backblaze's official [S3 compatibility page](https://www.backblaze.com/docs/cloud-storage-s3-compatible-api) does not claim conditional PUT support. Current empirical integration evidence is explicit: a 2025 Proxmox patch says B2 rejects `If-None-Match` on PUT with HTTP 501 and therefore adds a provider quirk ([mail/patch](https://lore.proxmox.com/all/20250728100154.556438-3-c.ebner@proxmox.com/)); [ZeroFS #95](https://github.com/Barre/ZeroFS/issues/95) independently reports the same limitation. This “not supported” classification is supported by tests, not an explicit Backblaze statement. |
| **SeaweedFS** | **Supported since 3.97** | Upstream [PR #7154](https://github.com/seaweedfs/seaweedfs/pull/7154) says write operations `PUT` and `CompleteMultipartUpload` return 412 on conditional failure; it merged as [`34773c8…`](https://github.com/seaweedfs/seaweedfs/commit/34773c8e138b7f35f68f2b289051b441993e92fd) on 2025-08-22. Git tag containment shows the first release tag containing it is **3.97**. The project [conditional-operations wiki](https://github.com/seaweedfs/seaweedfs/wiki/S3-Conditional-Operations) describes the check/write as atomic cluster-wide. |
| **Garage** | **Not supported, by design** | Official [Garage known issues](https://garagehq.deuxfleurs.fr/documentation/reference-manual/known-issues/#no-conditional-writes-locking-worm-support-if-none-match-) says: “No conditional writes / locking / WORM support (`if-none-match`, ...)” and calls it structurally impossible without consensus, which Garage deliberately omits. See open feature [#1052](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/1052). |
| **LocalStack** | **Supported** | LocalStack commit [`b28bad3bfd2de9cd320b3da55cd168b3b7c94988`](https://github.com/localstack/localstack/commit/b28bad3bfd2de9cd320b3da55cd168b3b7c94988) (2024-11-27), “implement S3 conditional write `IfMatch` (#11941),” retains and tests the existing `IfNoneMatch == "*"` PutObject path while adding `IfMatch`; later [PR #12624](https://github.com/localstack/localstack/pull/12624) fixes enforcement in presigned URLs. Use a post-2024-11-27 release and test presigned flows on a version containing #12624. |

**Design consequence:** advertising “S3-compatible object storage” while requiring atomic `If-None-Match: *` is inaccurate unless compatibility requirements explicitly name conditional `PutObject`, return semantics (412/409), simple versus multipart behavior, versioning behavior, and tested minimum backend versions. A capability probe must race two writes—not merely inspect headers or do sequential requests—if atomicity is required.

## 4. Docker bind mounts and numeric ownership

### Native Linux versus Docker Desktop

Docker's [bind-mount documentation](https://docs.docker.com/engine/storage/bind-mounts/) defines a bind mount as a host file/directory mounted directly into the container and warns that container processes can modify host files. On a **native Linux rootful daemon without user namespaces**, the kernel bind mount exposes the same inode metadata; Docker performs no per-mount UID/GID translation. Thus a host file owned by numeric `1234:5678` appears as `1234:5678` in the container (names may differ because `/etc/passwd` differs), and a process running as UID 1000 creates host files as numeric UID 1000.

This conclusion has explicit boundaries. Docker's official [UID/GID mapping docs](https://docs.docker.com/engine/security/rootless/uid-gid-mapping/) say rootless and `userns-remap` “map container UIDs and GIDs to the host differently,” and spell out the subordinate-ID formulas. SELinux labels and mode/ACL permissions are independent additional constraints.

Docker Desktop is not a native bind mount to macOS. The bind docs note that Desktop's daemon runs in a Linux VM, and official Desktop [settings](https://docs.docker.com/desktop/settings-and-maintenance/settings/#file-sharing) identify **VirtioFS** and **gRPC FUSE** as host/VM file-sharing implementations. This mediation virtualizes ownership. But do not promise a simple “always remapped to the container user” rule for current Mac Desktop: behavior differs by backend/release. Docker's [release notes](https://docs.docker.com/desktop/release-notes/) state for modern Mac VirtioFS that fake ownership changes are not persisted: “Calls to `chown` will succeed, but `stat` will not be affected.” Older official osxfs documentation described initial `stat` ownership as the requesting container process, but that is not authoritative for current VirtioFS/gRPC-FUSE. Test the supported Desktop version if exact in-container ownership matters.

### Supported ways to make written files accessible across differing IDs

* **Run as the intended host numeric ID:** Docker `-u/--user` accepts [`<name|uid>[:<group|gid>]`](https://docs.docker.com/reference/cli/docker/container/run/#user). On native rootful Linux without userns, `--user "$(id -u):$(id -g)"` makes newly created bind-mounted files use those host numbers. This is the simplest passthrough mechanism, assuming the image/app works without its baked-in user.
* **Share by group:** provision the host directory with a shared GID (often setgid plus a cooperative umask/default ACL), then pass that numeric group with Docker [`--group-add`](https://docs.docker.com/engine/containers/run/#additional-groups). Docker says this flag adds supplementary groups to the process. This preserves the primary application UID while allowing group read/write; the host process must also be in that GID.
* **Daemon-level user namespaces:** Docker [`userns-remap`](https://docs.docker.com/engine/security/userns-remap/) maps container root and other IDs into subordinate host UID/GID ranges. It is a security isolation facility, not an automatic “match arbitrary host user” switch; Docker explicitly warns it adds complexity for bind mounts whose host locations the remapped user cannot write. It normally requires daemon configuration/restart and host ownership/ACL preparation.
* **Podman keep-id:** official [`--userns=keep-id`](https://docs.podman.io/en/latest/markdown/options/userns.container.html) maps the current rootless user's UID:GID to the same values inside the container and runs init as that user, making it the direct Podman analogue for host-ID-friendly bind writes.
* **`podman unshare`:** official [`podman-unshare(1)`](https://docs.podman.io/en/latest/markdown/podman-unshare.1.html) launches a shell in Podman's user namespace, mapping the invoking user to namespace UID/GID 0. It is useful to inspect or `chown` files in rootless Podman storage/mappings. It is an administration/repair tool, not by itself a runtime ownership option; use it to translate namespace ownership when preparing or repairing mounted paths.

None of these replaces ordinary Unix access design: use suitable modes, a shared group/default ACL, and an appropriate umask. “Readable” can often be achieved without changing owner; “writable by both” needs deliberate group/ACL semantics.

## Conflicts, gaps, and version boundaries

* Backblaze's own compatibility page neither promises nor explicitly denies conditional PUT; the “not supported” result relies on two current independent integrations observing HTTP 501.
* Ceph's documentation still lags implementation; its tracker provides stronger, version-specific evidence and documents unresolved/backport caveats.
* SeaweedFS support is recent (3.97). Older deployments should be classified unsupported.
* Docker's current public docs do not provide one stable UID/GID mapping contract for every Mac file-sharing backend. The defensible contrast is “native Linux preserves numeric inode ownership absent userns; Desktop mediates/virtualizes it,” not “Desktop always reports the active container user.”
