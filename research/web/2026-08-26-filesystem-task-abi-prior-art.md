---
title: "Filesystem task ABIs and durable output commit: prior art"
date: 2026-08-26
updated: 2026-08-26
status: complete
scope: "Argo Workflows, Tekton Pipelines, Kubernetes, Airflow KPO, Nextflow, Cromwell/WDL, AWS Batch/Step Functions, S3 conditional writes and consistency"
sources: "Official documentation and full-SHA upstream source permalinks"
---

# Filesystem task ABIs and durable output commit: prior art

## Summary

The strongest precedents are: **(1)** task code reads/writes declared files, while an executor wrapper owns staging, exit status and publishing; **(2)** small control values are file-backed but copied into a bounded control plane; **(3)** large values remain artifacts/workspace files or object-store objects; and **(4)** completion is represented by a last, separately visible marker. Argo comes closest to a post-task publisher: its executor stages inputs, waits for the process, captures parameters, uploads artifacts, reports outputs, and finally marks output reporting complete. Tekton uses filesystem results and filesystem sequencing markers, but ordinarily transports results through Kubernetes termination messages. Nextflow and Cromwell use an exit-code file as the completion boundary. For an object-store commit marker, S3 `PutObject` with `If-None-Match: *` supplies atomic create-if-absent; strong consistency makes a successful subsequent `HEAD`/`GET` existence check authoritative.

## 1. Argo Workflows

### Public container I/O contract

Argo does **not** reserve one fixed public input/output tree. The workflow author declares each artifact's in-container `path`:

* `inputs.artifacts[].path` is where Argo unpacks/downloads the input. The official example uses **`/tmp/message`**.
* `outputs.artifacts[].path` is the file or directory Argo collects after the container. The example uses **`/tmp/hello_world.txt`**.
* `outputs.parameters[].valueFrom.path` makes the file contents a parameter; the official example uses **`/tmp/hello_world.txt`**.

See [Artifacts](https://argo-workflows.readthedocs.io/en/latest/walk-through/artifacts/) and [Output Parameters](https://argo-workflows.readthedocs.io/en/latest/walk-through/output-parameters/). Files/directories are tar+gzip archived by default unless `archive.none` is selected.

For `outputs.result`, current docs specify **up to 256 KB of stdout**. Source enforces `256 * 1024` bytes and retains the last 256 KiB. Argo's example warns that output parameters/results are carried via annotations and values larger than 256 KB should be artifacts. This is best described as the documented result/control-plane limit, not a universal per-file limit for every `valueFrom.path`: [docs](https://argo-workflows.readthedocs.io/en/latest/walk-through/output-parameters/#result-output-parameter), [source](https://github.com/argoproj/argo-workflows/blob/8f0d28008aaf288fe73bc380c174a8623d73a786/workflow/executor/executor.go#L952-L966):

```go
const maxAnnotationSize int = 256 * (1 << 10) // 256 kB
if len(out) > maxAnnotationSize {
    out = out[len(out)-maxAnnotationSize:]
}
```

### Init, wait, staging and post-task publication

The [Workflow Executors docs](https://argo-workflows.readthedocs.io/en/latest/workflow-executors/) say the executor runs as init container and sidecar to monitor logs, provide/collect artifacts, and manage lifecycles. In the legacy layout:

* **`init`** initializes executor state, stages the script/resource files, and downloads inputs. Source: [`cmd/argoexec/commands/init.go`](https://github.com/argoproj/argo-workflows/blob/8f0d28008aaf288fe73bc380c174a8623d73a786/cmd/argoexec/commands/init.go#L44-L59):
  ```go
  wfExecutor.StageFiles(ctx)
  // Download input artifacts
  wfExecutor.LoadArtifactsWithoutPlugins(ctx)
  ```
* **`wait` sidecar** waits for the workload, captures stdout/parameters, uploads artifacts and logs, reports outputs, then finalizes output reporting. The shared post-main sequence is explicit in [`workflow/executor/postmain.go`](https://github.com/argoproj/argo-workflows/blob/8f0d28008aaf288fe73bc380c174a8623d73a786/workflow/executor/postmain.go#L27-L68): `Wait`, `CaptureScriptResult`, `SaveParameters`, `SaveArtifacts`, `SaveLogs`, `ReportOutputs`.
* The closest thing to a commit record is **not an artifact named `commit.json`**. After output reporting, `FinalizeOutput` patches `workflows.argoproj.io/report-outputs-completed=true` with retries: [`workflow/executor/executor.go`](https://github.com/argoproj/argo-workflows/blob/8f0d28008aaf288fe73bc380c174a8623d73a786/workflow/executor/executor.go#L970-L997). Thus Argo has a “publish data, then mark reporting complete” protocol, but does not promise a single atomic transaction spanning all artifact uploads and status publication.

Internal exact paths:

* **`/argo/staging`** is the staging `emptyDir`; script source is **`/argo/staging/script`**. [`workflow/common/common.go`](https://github.com/argoproj/argo-workflows/blob/8f0d28008aaf288fe73bc380c174a8623d73a786/workflow/common/common.go#L132-L145)
* **`/argo/inputs/artifacts`** is the executor artifact base dir; **`/mainctrfs`** gives init/wait access to main-container volume mounts. Same source block.
* Since current v4 development also has an `initlessPod` layout, a concurrent **`supervisor`** replaces init+wait duties. Treat “init + wait sidecar” as the established/legacy contract, not the only current internal layout.

### Emissary exit status and signals

Since Argo 3.4, [emissary is the only executor](https://argo-workflows.readthedocs.io/en/latest/workflow-executors/#emissary-executor). It replaces the container command with `argoexec`, launches the original command as a subprocess, and writes **`/var/run/argo/ctr/<containerName>/exitcode`**. It may also produce `combined`, `stdout`, and a `signal` control file. The authoritative layout is documented in source at [`workflow/executor/emissary/emissary.go`](https://github.com/argoproj/argo-workflows/blob/8f0d28008aaf288fe73bc380c174a8623d73a786/workflow/executor/emissary/emissary.go#L27-L49):

```text
/var/run/argo/ctr/${containerName}/exitcode
/var/run/argo/ctr/${containerName}/combined
/var/run/argo/ctr/${containerName}/stdout
/var/run/argo/ctr/${containerName}/signal
```

The actual deferred write is in [`cmd/argoexec/commands/emissary.go`](https://github.com/argoproj/argo-workflows/blob/8f0d28008aaf288fe73bc380c174a8623d73a786/cmd/argoexec/commands/emissary.go#L61-L69). Signals received by `argoexec` are forwarded to the subprocess; it also watches the per-container `signal` file: [same file](https://github.com/argoproj/argo-workflows/blob/8f0d28008aaf288fe73bc380c174a8623d73a786/cmd/argoexec/commands/emissary.go#L180-L184) and [`signal.go`](https://github.com/argoproj/argo-workflows/blob/8f0d28008aaf288fe73bc380c174a8623d73a786/cmd/argoexec/commands/signal.go#L17-L66). Argo's executor kill path sends **SIGTERM first, then SIGKILL after the grace period** ([source](https://github.com/argoproj/argo-workflows/blob/8f0d28008aaf288fe73bc380c174a8623d73a786/workflow/executor/executor.go#L171-L183)). Output collection uses a background context so termination cancellation does not automatically suppress post-main capture ([source](https://github.com/argoproj/argo-workflows/blob/8f0d28008aaf288fe73bc380c174a8623d73a786/workflow/executor/postmain.go#L20-L27)). SIGKILL itself cannot be caught or forwarded; forced termination can therefore preclude a completed output marker.

## 2. Tekton Pipelines

### Results and workspaces

A Task declares `results`; a step writes the value to **`$(results.<name>.path)`**, concretely **`/tekton/results/<name>`**. Official [Tasks docs](https://tekton.dev/docs/pipelines/tasks/#emitting-results) and the current source docs describe `/tekton/results` as an `emptyDir` mounted into all step containers: [`docs/developers/results-lifecycle.md`](https://github.com/tektoncd/pipeline/blob/bbc5b442326349b85db01c9d2a206619b4aedf57/docs/developers/results-lifecycle.md#L10-L59).

Classic result transport JSON-encodes those files into a termination message. The maximum aggregate Task results are documented as **4096 bytes**, with actual usable payload slightly less due to Tekton metadata and potentially less per container because Kubernetes allocates 12 KiB across containers: [Tasks docs](https://tekton.dev/docs/pipelines/tasks/#emitting-results) and pinned [`docs/tasks.md`](https://github.com/tektoncd/pipeline/blob/bbc5b442326349b85db01c9d2a206619b4aedf57/docs/tasks.md#L1017-L1037).

The newer **Larger Results via Sidecar Logs** feature became stable in **v0.61.0**. With `results-from: sidecar-logs`, an injected sidecar watches `/tekton/results`, emits each result to stdout, and the controller reads pod logs. Default is **4096 bytes per result**, unlimited result count except the TaskRun CRD's practical **1.5 MB** ceiling; `max-result-size` configures another per-result byte limit. [Official docs](https://tekton.dev/docs/pipelines/tasks/#larger-results-using-sidecar-logs), pinned [`docs/additional-configs.md`](https://github.com/tektoncd/pipeline/blob/bbc5b442326349b85db01c9d2a206619b4aedf57/docs/additional-configs.md#L500-L535).

`workspaces` are runtime-provided filesystem regions backed by PVC, `emptyDir`, Secret, ConfigMap, etc. The default mount is exactly **`/workspace/<name>`** unless `mountPath` overrides it. [Official Workspaces docs](https://tekton.dev/docs/pipelines/workspaces/#using-workspaces-in-tasks), pinned [`docs/workspaces.md`](https://github.com/tektoncd/pipeline/blob/bbc5b442326349b85db01c9d2a206619b4aedf57/docs/workspaces.md#L72-L92). Workspaces are the documented recommendation for results larger than about a kilobyte in the classic path.

### Step-container ABI and sequencing

Each `step` is a container in one TaskRun Pod. Tekton rewrites each image's command to **`/tekton/bin/entrypoint`**, which wraps the original process. It gates the next step with files under **`/tekton/run/<n>/out`**:

* step `n` waits for the previous step's `out` (or detects `out.err`);
* on completion, it writes its own `out`, or **`out.err`** on failure.

The pinned entrypoint README says exactly this and gives the example `-wait_file /tekton/run/3/out` and `-post_file /tekton/run/4/out`: [`cmd/entrypoint/README.md`](https://github.com/tektoncd/pipeline/blob/bbc5b442326349b85db01c9d2a206619b4aedf57/cmd/entrypoint/README.md#L1-L54). `/tekton/run` is an implicit volume specifically for coordinating step ordering: [`docs/developers/taskruns.md`](https://github.com/tektoncd/pipeline/blob/bbc5b442326349b85db01c9d2a206619b4aedf57/docs/developers/taskruns.md#L124-L170).

## 3. Kubernetes termination messages

[Kubernetes' official task documentation](https://kubernetes.io/docs/tasks/debug/debug-application/determine-reason-pod-failure/#customizing-the-termination-message) defines:

* `terminationMessagePath`: file kubelet reads into `ContainerStateTerminated.message`; default **`/dev/termination-log`**; immutable after Pod launch.
* `terminationMessagePolicy: File` (default): only that file.
* `FallbackToLogsOnError`: if the file is empty and exit is an error, use the last log chunk, limited to **2048 bytes or 80 lines**, whichever is smaller.
* File message is truncated above **4096 bytes per container**.
* Total across regular and init containers is **12 KiB**, divided equally; 12 containers therefore get 1024 bytes each.

This is status/debug metadata, not a durable bulk-output channel.

## 4. Airflow KubernetesPodOperator XCom

With `do_xcom_push=True`, KPO injects an **XCom sidecar** and shared volume. User code must write valid JSON to exactly **`/airflow/xcom/return.json`**; invalid JSON fails, and XCom is pushed only when the task succeeds. [Official provider docs](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html#how-does-xcom-work).

**Size-limit finding:** the KPO documentation specifies **no fixed byte limit** for `return.json`. Do not repeat commonly circulated database-specific XCom limits as a KPO ABI limit. Effective capacity depends on the configured Airflow XCom backend/database and the transfer path; large objects should use object storage with an XCom reference/object-storage backend. This is a documented gap rather than a numeric KPO guarantee.

## 5. Nextflow

Each task receives a unique hash-derived work directory. Inputs, generated script, wrapper/helper files and outputs live together. Official [Caching and resuming docs](https://www.nextflow.io/docs/latest/cache-and-resume.html#work-directory) say resume succeeds only when both the task-cache hash matches and required outputs remain in the corresponding work directory with a valid exit code. The metadata cache is **`.nextflow/cache/<session-id>`**.

Exact control filenames are constants in current source, [`TaskRun.groovy`](https://github.com/nextflow-io/nextflow/blob/640ddc3542b7025fd34bb6baf92dd5b77e53c39d/modules/nextflow/src/main/groovy/nextflow/processor/TaskRun.groovy#L575-L584):

```groovy
.command.log
.command.sh       // rendered user task script
.command.in
.command.out      // stdout
.command.err      // stderr
.exitcode         // numeric completion status
.command.begin    // start marker
.command.run      // executor/container/staging wrapper
```

The wrapper touches `.command.begin`, tees streams to `.command.out`/`.command.err`, and writes `.exitcode`; see the generated wrapper template/source context in [`command-run.txt`](https://github.com/nextflow-io/nextflow/blob/640ddc3542b7025fd34bb6baf92dd5b77e53c39d/modules/nextflow/src/main/resources/nextflow/executor/command-run.txt#L143-L180) and builder tests [`test-bash-wrapper.txt`](https://github.com/nextflow-io/nextflow/blob/640ddc3542b7025fd34bb6baf92dd5b77e53c39d/modules/nextflow/src/test/resources/nextflow/executor/test-bash-wrapper.txt#L92-L103).

Input staging modes are exactly **`copy`**, **`link`** (hard link), **`rellink`** (relative symlink), and **`symlink`** (absolute symlink, default): [official `stageInMode`](https://www.nextflow.io/docs/latest/reference/process.html#stageinmode), pinned [`stage-in-mode.mdx`](https://github.com/nextflow-io/nextflow/blob/640ddc3542b7025fd34bb6baf92dd5b77e53c39d/docs/reference/process/directives/stage-in-mode.mdx#L6-L28).

This is strong prior art for “directory + files = task ABI,” but cache validity is not merely “directory exists”: Nextflow combines the separate cache entry, hash identity, required-output presence and valid `.exitcode`.

## 6. Cromwell / WDL (brief)

Cromwell's execution directory uses exact filenames **`script`**, **`rc`**, **`stdout`**, and **`stderr`**. Current source names these in [`backend/io/JobPaths.scala`](https://github.com/broadinstitute/cromwell/blob/785071c718eac991a804bc283ebebb07c1673110/backend/src/main/scala/cromwell/backend/io/JobPaths.scala#L14-L17) and [`JobPaths.scala`](https://github.com/broadinstitute/cromwell/blob/785071c718eac991a804bc283ebebb07c1673110/backend/src/main/scala/cromwell/backend/io/JobPaths.scala#L42-L81):

```scala
def returnCodeFilename = "rc"
def defaultStdoutFilename = "stdout"
def defaultStderrFilename = "stderr"
def scriptFilename = "script"
```

The wrapper commits return status safely via a temporary file then rename, `mv $rcTmpPath $rcPath`, in [`StandardAsyncExecutionActor.scala`](https://github.com/broadinstitute/cromwell/blob/785071c718eac991a804bc283ebebb07c1673110/backend/src/main/scala/cromwell/backend/standard/StandardAsyncExecutionActor.scala#L457-L547). Official [container docs](https://cromwell.readthedocs.io/en/stable/tutorials/Containers/#how-does-cromwell-know-when-a-job-or-container-has-completed) explicitly say Cromwell watches for the existence of `rc` to know completion. WDL exposes logs through `stdout()` and `stderr()` functions.

## 7. AWS Batch and Step Functions

AWS Batch job definitions describe command, parameters, environment, mounts and resources; Batch has no Argo-like result-file ABI and no native bulk job-output return channel. Applications conventionally read/write S3, EFS or another durable store and return identifiers/metadata. The [Step Functions optimized Batch integration](https://docs.aws.amazon.com/step-functions/latest/dg/connect-batch.html) submits with `arn:aws:states:::batch:submitJob.sync` and waits for completion.

Step Functions imposes a hard maximum **256 KiB (262,144 bytes), UTF-8 encoded**, on input or output for a task, state, or execution: [official quotas](https://docs.aws.amazon.com/step-functions/latest/dg/limits-overview.html#service-limits-task-executions). AWS explicitly recommends putting data that may exceed 262,144 bytes in **S3** and passing/parsing the S3 bucket/key ARN/reference instead: [Input/output docs](https://docs.aws.amazon.com/step-functions/latest/dg/input-output-inputpath-params.html#input-output-inputpath-params-s3). Thus a Batch task should return a small S3 reference, not content, through Step Functions.

## 8. S3 conditional writes and MinIO

### AWS S3

On **2024-08-20**, AWS [announced conditional create writes](https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/) for `PutObject` and `CompleteMultipartUpload`. Exact wire condition:

```http
If-None-Match: *
```

SDK/CLI parameter names are generally `IfNoneMatch` / `--if-none-match "*"`. If a current object already has that key, S3 returns **HTTP 412 `PreconditionFailed` / Precondition Failed**. A concurrent delete/write race can instead produce **409 `ConditionalRequestConflict`**, requiring retry (and a fresh multipart upload for `CompleteMultipartUpload`). Authoritative references: [PutObject API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html), [CompleteMultipartUpload API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html), and [conditional-write behavior](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html#conditional-error-response).

On **2024-11-25**, AWS [announced ETag compare-and-swap writes](https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-functionality-conditional-writes/) for `PutObject` and `CompleteMultipartUpload`:

```http
If-Match: "<current-etag>"
```

Mismatch returns **412**; concurrent conflicts may return 409. This November announcement did **not** announce general-purpose conditional delete. `DeleteObject` `If-Match` for general-purpose buckets arrived later, on **2025-09-16**: [AWS announcement](https://aws.amazon.com/about-aws/whats-new/2025/09/amazon-s3-conditional-deletes-s3-general-purpose-buckets/) and [DeleteObject API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html). Current `DeleteObject` accepts `If-Match: <etag>` or `*`; mismatch is **412**. `x-amz-if-match-last-modified-time` and `x-amz-if-match-size` remain directory-bucket-only.

For `commit.json`, a single `PutObject` with `If-None-Match: *` is preferable to `HEAD` then unconditional `PUT`: it is atomic create-if-absent at the object key and prevents two publishers both winning.

### MinIO compatibility and release boundary

MinIO server supports conditional **PutObject** `If-Match`/`If-None-Match` since commit [`11c7ecb5cf947e6428968571cafc0494bb572845`](https://github.com/minio/minio/commit/11c7ecb5cf947e6428968571cafc0494bb572845), first included in **`RELEASE.2023-02-09T05-16-53Z`**. The current implementation installs the PUT precondition callback when either header exists: [`cmd/object-handlers.go`](https://github.com/minio/minio/blob/7aac2a2c5b7c882e68c1ce017d8256be2feea27f/cmd/object-handlers.go#L2017-L2029):

```go
if r.Header.Get(xhttp.IfMatch) != "" ||
   r.Header.Get(xhttp.IfNoneMatch) != "" {
    opts.CheckPrecondFn = func(oi ObjectInfo) bool {
        return checkPreconditionsPUT(ctx, w, r, oi, opts)
    }
}
```

Multipart conditional-check support/fixes landed in [`07c3a429bfed433e49018cb0f78a52145d4bedeb`](https://github.com/minio/minio/commit/07c3a429bfed433e49018cb0f78a52145d4bedeb), first included in **`RELEASE.2025-09-07T16-13-09Z`**; current handler source is [`cmd/object-multipart-handlers.go`](https://github.com/minio/minio/blob/7aac2a2c5b7c882e68c1ce017d8256be2feea27f/cmd/object-multipart-handlers.go#L203-L215). Therefore, for portable commit markers against older MinIO, use a one-shot conditional `PutObject`; do not assume AWS-equivalent conditional `CompleteMultipartUpload` before the September 2025 release. MinIO returns its S3-compatible **412 `PreconditionFailed`** mapping ([source](https://github.com/minio/minio/blob/7aac2a2c5b7c882e68c1ce017d8256be2feea27f/cmd/api-errors.go#L708-L712)). MinIO's public `mc put` docs do not expose an `--if-none-match` flag; use an S3 SDK/API client that can set the header.

## 9. S3 read-after-write consistency and commit existence

Amazon S3 has provided strong read-after-write and strong LIST consistency in all Regions since **2020-12-01**: [announcement](https://aws.amazon.com/about-aws/whats-new/2020/12/amazon-s3-now-delivers-strong-read-after-write-consistency-automatically-for-all-applications/) and [current consistency page](https://aws.amazon.com/s3/consistency/). After a successful create, overwrite, or delete, subsequent reads immediately see the latest state; LIST reflects the change too.

Consequences for `commit.json`:

* After the conditional commit PUT returns success, a subsequent `HEAD` or `GET` for that key will see it; no eventual-consistency sleep is required.
* If `HEAD commit.json` returns 404 in an authorized, correctly routed request, there is no successfully visible current commit object at that instant.
* Strong consistency does **not** make a multi-object publication atomic. Upload all immutable outputs first, then conditionally create `commit.json` last; readers treat only the marker as publication. The marker should contain immutable keys plus checksums/ETags so readers can validate the referenced set.
* A preliminary “does it exist?” read is safe as an observation but not as mutual exclusion: another writer can win immediately afterward. The authoritative race-free decision must be `If-None-Match: *` on the commit PUT.
* Distinguish absence from authorization/network errors; do not map every failed `HEAD` to “not committed.”

## Conflicts, gaps, and version notes

1. Argo's “256 KB parameter limit” is exact for captured stdout `outputs.result`; docs/examples generalize the annotation constraint and recommend artifacts above that size, but `valueFrom.path` is not independently truncated by the cited result-capture code.
2. Current Argo v4 source includes an initless supervisor layout. The widely documented init + wait sidecar model remains relevant, but is no longer the sole implementation.
3. Airflow KPO publishes no stable numeric `return.json` limit. Capacity is backend/deployment dependent.
4. AWS November 2024 added `If-Match` to writes, not general-purpose deletes. Conditional deletes were announced September 2025.
5. MinIO PutObject conditional support dates to February 2023, while the verified multipart path has a September 2025 boundary; pin/test MinIO when relying on multipart conditional completion.
