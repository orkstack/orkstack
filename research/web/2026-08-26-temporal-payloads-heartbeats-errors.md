---
source_url:
  - https://docs.temporal.io/cloud/limits
  - https://docs.temporal.io/self-hosted-guide/defaults
  - https://docs.temporal.io/develop/go/activities/timeouts
  - https://docs.temporal.io/encyclopedia/detecting-activity-failures
  - https://docs.temporal.io/external-storage
  - https://docs.temporal.io/develop/go/data-handling/external-storage
  - https://docs.temporal.io/codec-server
  - https://github.com/temporalio/temporal
  - https://github.com/temporalio/sdk-go
  - https://github.com/temporalio/samples-go
fetched_at: 2026-08-26T00:00:00Z
fetch_method: fetch_content, web_search, git clone/source inspection
topic: Temporal Go SDK payload limits, external-storage claim check, Activity heartbeats/cancellation/timeouts, errors, and retry-stable identifiers
---

# Temporal Go SDK: payloads, heartbeats, errors, and idempotency

## Summary

* **Payload limits:** Temporal Cloud documents **4 MB per received gRPC message**, **2 MB max payload for a single request**, and a **4 MB Event History transaction**. Workflow history warns at **10 MB / 10,240 Events** and terminates at **50 MB / 51,200 Events** ([Cloud limits](https://docs.temporal.io/cloud/limits#programming-model-level)). In exact binary values the current server defaults are 2,097,152 bytes, 52,428,800 bytes, and 51,200 Events.
* **Important warning-threshold conflict:** the current self-hosted docs say the blob warning is **256 KB**, but current server source at commit `9ad49ef…` sets `BlobSizeLimitWarn` to **512 × 1024 = 524,288 bytes (512 KiB)**. Do not confuse either with Go SDK External Storage's separate default offload threshold of **256 KiB**.
* **Large payloads:** current official guidance is the claim-check pattern: offload to S3/GCS/external storage and keep a small reference in History. Go SDK now has first-party `ExternalStorage`; a custom `PayloadCodec`/`DataConverter` can also do remote I/O, and a Codec Server lets UI/CLI resolve/decode references.
* **Cancellation:** an Activity receives server cancellation through `ctx.Done()` **only through heartbeating**. A `HeartbeatTimeout` is not technically required to transmit explicit `activity.RecordHeartbeat` calls: without it, Go uses the default **30-second** heartbeat throttle interval. But an Activity that never calls `RecordHeartbeat` will not receive workflow cancellation through its context. Set a Heartbeat Timeout anyway for timely crash detection.
* **Idempotency:** for one logical workflow-scheduled Activity Execution, `ActivityInfo.ActivityID` and `ActivityInfo.WorkflowExecution.RunID` remain stable across Activity retry attempts; `ActivityInfo.Attempt` starts at **1** and increments for each retry. Run ID changes across Workflow Continue-As-New or Workflow retry, not Activity retry.

## 1. Payload and Event History limits

### Official documented limits

[Temporal Cloud programming-model limits](https://docs.temporal.io/cloud/limits#programming-model-level) state:

* “Each gRPC message received has a limit of **4 MB**.” This applies to all endpoints and the complete protobuf request, not merely application data.
* “The max payload for a single request is **2 MB**,” and an Event History transaction is at most **4 MB**.
* “A Workflow Execution's Event History is limited to **51,200 Events or 50 MB**. It warns you after **10,240 Events or 10 MB**.” Exceeding the hard history limit terminates the Workflow Execution.

The [self-hosted defaults page](https://docs.temporal.io/self-hosted-guide/defaults) likewise quotes **4 MB** gRPC, a payload warning at **256 KB**, payload error at **2 MB**, history warning/error at **10/50 MB**, and count warning/error at **10,240/51,200**.

### Current server dynamic-config defaults (source of truth for self-hosted main)

At server commit [`9ad49ef57877124670bef37c854db282d0bab8af`](https://github.com/temporalio/temporal/commit/9ad49ef57877124670bef37c854db282d0bab8af), [`common/dynamicconfig/constants.go` lines 392–460](https://github.com/temporalio/temporal/blob/9ad49ef57877124670bef37c854db282d0bab8af/common/dynamicconfig/constants.go#L392-L460) says:

```go
BlobSizeLimitError = NewNamespaceIntSetting("limit.blobSize.error", 2*1024*1024, ...)
BlobSizeLimitWarn  = NewNamespaceIntSetting("limit.blobSize.warn", 512*1024, ...)
HistorySizeLimitError = NewNamespaceIntSetting("limit.historySize.error", 50*1024*1024, ...)
HistorySizeLimitWarn  = NewNamespaceIntSetting("limit.historySize.warn", 10*1024*1024, ...)
HistoryCountLimitError = NewNamespaceIntSetting("limit.historyCount.error", 50*1024, ...)
HistoryCountLimitWarn  = NewNamespaceIntSetting("limit.historyCount.warn", 10*1024, ...)
```

Exact defaults:

| Dynamic config key | Exact default | Conventional label |
|---|---:|---:|
| `limit.blobSize.error` (`BlobSizeLimitError`) | **2,097,152 bytes** | **2 MiB** |
| `limit.blobSize.warn` (`BlobSizeLimitWarn`) | **524,288 bytes** | **512 KiB** |
| `limit.historySize.error` (`HistorySizeLimitError`) | **52,428,800 bytes** | **50 MiB** |
| `limit.historySize.warn` | **10,485,760 bytes** | **10 MiB** |
| `limit.historyCount.error` (`HistoryCountLimitError`) | **51,200 Events** | — |
| `limit.historyCount.warn` | **10,240 Events** | — |

**Version/conflict note:** the current docs’ **256 KB** warn claim conflicts with current `main`’s **512 KiB** dynamic default. Cloud may enforce its documented policy independently, and self-hosted values are configurable. The hard blob limit is **2 MiB**, not 4 MiB; **4 MiB** is the whole received gRPC message / history transaction. Multiple individually valid payloads can therefore exceed the message limit together.

Heartbeat `details` are serialized Payloads in the heartbeat RPC, so they have no special large-data exemption: keep every encoded payload below the blob hard limit and the complete heartbeat request below gRPC's 4 MB limit. Payload data also contributes to History when recorded in events (for example, last heartbeat details surfaced on timeout).

## 2. Claim check / large payload pattern

[External Storage](https://docs.temporal.io/external-storage) explicitly calls this the **claim check pattern**: the SDK uploads a large payload to an external store such as Amazon S3 and puts a small reference token in Event History. It recommends durable, strongly consistent object storage reachable by Clients, Workers, and the Codec Server, and a lifecycle satisfying:

```text
TTL > Maximum Workflow Run Timeout + Namespace Retention Period
```

For Go, use the official [Go External Storage guide](https://docs.temporal.io/develop/go/data-handling/external-storage). The Go SDK provides first-party S3 and GCS drivers, transparently applies them to all Workflow/Activity serialization, and defaults `PayloadSizeThreshold` to exactly **256 KiB**. A zero threshold selects that default; set it to **1** to externalize everything. This is a **client-side offload threshold**, not `BlobSizeLimitWarn`.

Current recommended pipeline order is Payload Converter → Payload Codec (compression/encryption) → External Storage. A custom [Payload Codec](https://docs.temporal.io/payload-codec) is allowed to perform nondeterministic remote calls, so a custom codec/data converter can implement S3 claim checks, but built-in Go `ExternalStorage` is now the more direct supported mechanism.

A [Codec Server](https://docs.temporal.io/codec-server#codec-server-with-external-storage) is for Temporal Web/CLI visibility. It hosts matching codec/storage logic and exposes `/encode`, `/decode`, and (with External Storage) `/download`; it is not the Temporal Service and must be secured like a Worker.

Official Go sample: [`samples-go/external-storage`](https://github.com/temporalio/samples-go/tree/3c8a12e407354af125d86e654bd38621452d5714/external-storage). Its pinned README says payloads are compressed, checked against **256 KiB**, stored in S3, and replaced with claim-check references ([lines 1–18](https://github.com/temporalio/samples-go/blob/3c8a12e407354af125d86e654bd38621452d5714/external-storage/README.md#L1-L18)):

```text
This sample demonstrates how to offload large workflow payloads to S3-compatible
object storage using the Temporal Go SDK's built-in ExternalStorage system...
checks the compressed size against the default 256 KiB threshold; payloads
still above it are stored in S3 and replaced inline with compact claim-check references.
```

The official generic Codec Server sample is [`samples-go/codec-server`](https://github.com/temporalio/samples-go/tree/3c8a12e407354af125d86e654bd38621452d5714/codec-server). I found no current `temporalio/large-payload-service` repository; the current maintained sample is `samples-go/external-storage`.

## 3. Go Activity heartbeats and cancellation

Use [`activity.RecordHeartbeat(ctx, details...)`](https://pkg.go.dev/go.temporal.io/sdk/activity#RecordHeartbeat). The SDK source at [`activity/activity.go` lines 57–79](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/activity/activity.go#L57-L79) documents that a heartbeat response indicating cancellation cancels the Activity context:

```go
// RecordHeartbeat sends a heartbeat for the currently executing activity.
// If the activity is either canceled or the workflow/activity doesn't exist,
// then we would cancel the context with error context.Canceled.
func RecordHeartbeat(ctx context.Context, details ...any) {
    internal.RecordActivityHeartbeat(ctx, details...)
}
```

[Official heartbeat docs](https://docs.temporal.io/develop/go/activities/timeouts#activity-heartbeats) state exactly: “Activity Cancellations are delivered to Activities from the Temporal Service when they Heartbeat. Activities that don't Heartbeat can't receive a Cancellation.” Heartbeats can checkpoint progress; retry attempts retrieve the last delivered details using `activity.HasHeartbeatDetails` / `activity.GetHeartbeatDetails`. On a missed heartbeat, Workflow code receives a `TimeoutError` with timeout type `Heartbeat` and the last heartbeat details.

### Critical answer: HeartbeatTimeout unset

* **No call to `RecordHeartbeat`: no cancellation delivery.** Merely selecting on `ctx.Done()` is insufficient; the context does not become canceled from a workflow cancellation until the Worker learns it from a heartbeat response.
* **Calls `RecordHeartbeat`, but `HeartbeatTimeout == 0`: cancellation can still arrive.** `ActivityInfo` explicitly says “0 means no heartbeat needed” ([source lines 50–65](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/internal/activity.go#L50-L65)), not that heartbeat RPCs are forbidden. The Go worker's throttle algorithm uses `heartbeatTimeout * 0.8` when set, otherwise the default throttle interval; current defaults are exactly **30 seconds**, capped at **60 seconds** ([source lines 44–46 and 2597–2618](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/internal/internal_task_handlers.go#L44-L46), [algorithm](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/internal/internal_task_handlers.go#L2597-L2618)). The official [throttling docs](https://docs.temporal.io/encyclopedia/detecting-activity-failures#throttling) confirm those same numbers.
* Nonetheless, set a short Heartbeat Timeout for long activities: without one a Worker crash is detected only by Start-To-Close, even if the Activity emits heartbeats.

## 4. ActivityOptions and timeout behavior

The authoritative Go fields and comments are in [`internal/activity.go` lines 100–149](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/internal/activity.go#L100-L149):

* `ScheduleToCloseTimeout`: end-to-end maximum for the **logical Activity Execution**, including every retry/backoff.
* `StartToCloseTimeout`: maximum for **one Activity Task attempt**. A Start-To-Close timeout can be retried; the attempt increments and its timer resets.
* `ScheduleToStartTimeout`: maximum queue wait for each Activity Task. It is deliberately **non-retryable**, because rescheduling to the same queue is not useful. Default is unlimited and docs recommend monitoring queue latency rather than setting it except for host-specific routing.
* At least one of Start-To-Close or Schedule-To-Close is required. If only Schedule-To-Close is supplied, Start-To-Close defaults to it.
* `HeartbeatTimeout`: maximum between heartbeats. Missing it means no heartbeat-failure detector.
* `WaitForCancellation` defaults **false**. If false, the workflow-side Activity Future becomes canceled without waiting for the Worker Activity to acknowledge/finish. If true, workflow execution waits for the canceled Activity to report a terminal response: it may complete, fail, or accept cancellation. This is needed when the Workflow must wait for Activity cleanup before proceeding, but requires heartbeating for a running Activity to learn of cancellation.

See the official [Go timeout guide](https://docs.temporal.io/develop/go/activities/timeouts) and [failure detection encyclopedia](https://docs.temporal.io/encyclopedia/detecting-activity-failures). A Start-To-Close expiry produces `TimeoutError.TimeoutType() == TIMEOUT_TYPE_START_TO_CLOSE`; a missed heartbeat produces `TIMEOUT_TYPE_HEARTBEAT` and may carry last heartbeat details. Retries happen according to RetryPolicy until a non-retryable condition, attempt cap, or Schedule-To-Close deadline ends the logical Activity.

## 5. Error typing and canceled results

Pinned SDK API source: [`temporal/error.go` lines 158–234](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/temporal/error.go#L158-L234).

* `temporal.NewApplicationError(message, errType, details...)` creates a **retryable** `*ApplicationError`.
* `temporal.NewNonRetryableApplicationError(message, errType, cause, details...)` sets its `NonRetryable` bit; it will not retry regardless of policy.
* `ApplicationError.Type()` returns the explicit `errType` string ([implementation](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/internal/error.go#L723-L733)). The official [Retry Policy docs](https://docs.temporal.io/encyclopedia/retry-policies#non-retryable-errors) say policy entries are matched against the Application Failure `type` field. Thus `RetryPolicy.NonRetryableErrorTypes: []string{"InvalidInput"}` stops retries for `NewApplicationError(..., "InvalidInput", ...)`; matching is by the exact type string, not Go `errors.Is` or the message. The constructor-level non-retryable flag always wins even if the policy list is empty.
* Plain `errors.New` / `fmt.Errorf` returned from an Activity is converted to `*ApplicationError`; its type is derived by the failure converter rather than a caller-selected semantic string. Prefer explicit `NewApplicationError` when policy matching is important.
* Workflow-side `ExecuteActivity(...).Get(...)` returns an outer `*temporal.ActivityError`; unwrap it / use `errors.As` to find its cause, which can be `*ApplicationError`, `*TimeoutError`, `*CanceledError`, or `*PanicError` ([SDK source](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/internal/workflow.go#L1018-L1024)). `temporal.IsCanceledError(err)` uses `errors.As`, so it traverses wrapping and can recognize a CanceledError in the chain.

### Returning a canceled result from a Worker

After server cancellation has reached the Activity context, either return `ctx.Err()` (normally `context.Canceled`) or `temporal.NewCanceledError(details...)`. The latter is the explicit SDK API for a successful canceled result and can carry details. Current result conversion accepts either when cancellation was actually requested ([`internal_task_pollers.go` lines 1638–1665](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/internal/internal_task_pollers.go#L1638-L1665)):

```go
if cancelAllowed {
    if errors.As(err, &canceledErr) { return RespondActivityTaskCanceledRequest{...} }
    if errors.Is(err, context.Canceled) { return RespondActivityTaskCanceledRequest{...} }
}
```

Do **not** return `nil` if the desired Activity status is Canceled: success is processed first and reports Activity completion. Also, returning `NewCanceledError` spontaneously before the server has allowed cancellation is converted to an unexpected-cancel failure ([lines 1668–1676](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/internal/internal_task_pollers.go#L1668-L1676)).

## 6. Idempotency and retry-stable identifiers

The pinned [`ActivityInfo` struct](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/internal/activity.go#L32-L77) says:

```go
WorkflowExecution WorkflowExecution
// ActivityID is the ID of the activity.
ActivityID string
...
// Attempt starts from 1, and increased by 1 for every retry if retry policy is specified.
Attempt int32
```

The `WorkflowExecution` value contains Workflow ID and Run ID; the Worker populates it from the task's workflow execution ([lines 379–382](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/internal/activity.go#L379-L382)):

```go
WorkflowExecution{ID: task.WorkflowExecution.GetWorkflowId(), RunID: task.WorkflowExecution.GetRunId()}
```

Therefore, for retries of **one logical workflow-scheduled Activity Execution**:

| Field | Across Activity retries | Meaning/use |
|---|---|---|
| `ActivityInfo.ActivityID` | **Stable** | ID of the logical Activity scheduled by the Workflow; best per-activity idempotency component. |
| `ActivityInfo.Attempt` | **Changes**: 1, 2, 3… | Physical attempt number; use for observability/backoff, not an idempotency key. |
| `ActivityInfo.WorkflowExecution.ID` | **Stable** | Business Workflow ID. May be reused across Runs. |
| `ActivityInfo.WorkflowExecution.RunID` | **Stable across Activity retries**, changes for Continue-As-New / Workflow retry / new Run | Disambiguates the exact Workflow Run. |

A robust uniqueness key for one Activity scheduling within a Run is `(Namespace, WorkflowExecution.ID, WorkflowExecution.RunID, ActivityID)`. If business idempotency should intentionally span Continue-As-New or Workflow retries, omit RunID only with a deliberate business key/version scheme. Temporal retries Activity Tasks automatically, so external side effects still need deduplication/upsert semantics; `Attempt` is not a dedupe key.

## Conflicts, gaps, and version boundaries

1. **256 KiB versus 512 KiB:** current self-hosted documentation reports a **256 KB** blob warning, but current server main source defaults to **512 KiB**. Separately, Go External Storage defaults to an exact **256 KiB** offload threshold. These are three deployment/version-sensitive facts, not one setting.
2. Cloud limits are fixed as documented; self-hosted dynamic config is configurable and the cited values are from server commit `9ad49ef57877124670bef37c854db282d0bab8af` on 2026-08-26.
3. SDK behavior/source is pinned to commit `9e67f43b78c6f0174e06009a2eded936829de59c`; the official sample is pinned to `3c8a12e407354af125d86e654bd38621452d5714`.
4. No current official repository named `temporalio/large-payload-service` was found. Current official Go materials use built-in External Storage and `samples-go/external-storage`.
