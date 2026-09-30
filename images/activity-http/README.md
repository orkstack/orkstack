# activity-http

Performs one HTTP request and reports the response as data. A reference
implementation of the OCI Activity Contract: `curl` makes the request, `jq`
writes the result, and there is no OrkStack SDK.

### Why `curl`, when §10.1 says "jq is the only SDK"

That annotation sits on **activity-shell's** Dockerfile, and the rule it states
is that no image needs an **OrkStack** SDK — not that an image may install no
packages. An HTTP client that cannot make an HTTP request would be a strange
reading of it. What the rule forbids is a library that knows about OrkStack:
some `orkstack-sdk` a workload must link against to be scheduled, report
progress, or hand back results. This image links against nothing of the sort.
It reads `/orkstack/input/config.json`, writes `/orkstack/output/values.json`,
and exits — which is the entire ABI, and is exactly what a third-party image
built on `curl` would do.

The falsification test for the whole design is the Bash fixture, and it needs
`cat`, `jq` and a redirect. That is the claim §10.1 is protecting.

```yaml
- id: notify
  uses: http
  with:
    op: post
    url: https://example.test/hooks/deploy
    headers:
      content-type: application/json
    body: '{"status":"ok"}'
```

## Configuration

Read from `/orkstack/input/config.json` (§3.3).

| Key | Type | Meaning |
|---|---|---|
| `op` | string | `get` (default), `post`, `put`, `patch`, `delete`, `head` |
| `url` | string | **Required.** The absolute request URL. |
| `headers` | object | Request headers, one per key |
| `body` | string | Request body, sent verbatim |
| `timeoutSeconds` | number | Passed to curl as `--max-time` |
| `captureBodyAs` | string | Save the response body as this output artifact |

`op` is the portable spelling of the HTTP method; the two are the same thing.
Header values are quoted before they reach argv, so a rendered value cannot
inject a curl argument — the same argv discipline `activity-shell` applies to
`command`.

## A non-2xx response is not a failure

**This image exits `0` for any response it received**, and reports the code in
`status`. An HTTP status is data; an activity failure is a failure to *perform*
the request. Branch on `status` in your Orkflow:

```yaml
condition: '{{ eq .tasks.notify.output.status 200.0 }}'
```

The activity fails (non-zero exit) only when the request could not be
performed at all — DNS failure, connection refused, TLS failure, or the
request exceeding `timeoutSeconds`. Those are the cases a retry can fix, so
they are the cases that become retryable `ActivityFailed`.

This is a documented contract of *this image*, not a platform rule.

## Output

`/orkstack/output/values.json`:

```json
{
  "status": 200,
  "headers": {"content-type": "application/json", "content-length": "18"},
  "durationMs": 42.318,
  "sizeBytes": 18,
  "json": {"ok": true}
}
```

| Key | Presence | Meaning |
|---|---|---|
| `status` | always | HTTP status code |
| `headers` | always | Response headers. A repeated header keeps its last value. |
| `durationMs` | always | Wall-clock time of the request, milliseconds |
| `sizeBytes` | always | Response body size |
| `json` | conditional | The parsed body, present only when it is valid JSON **and** at most 1 MiB. Absence means "not JSON, or too large" — never "empty". |

When `captureBodyAs` is set, the raw response body is written to
`/orkstack/output/artifacts/<name>` and becomes an output artifact. It must be
a plain file name: no `/`, and not `.` or `..`.

## Interface

The image declares its `with:` contract in the
`org.orkstack.activity.interface` label (`docs/ACTIVITY_IMAGES.md`,
"Declaring the interface"), so `orkctl orkflow validate` and apply check a
task's `with:` before anything runs: a missing required key or an unknown
`op` is an error, an undeclared key a warning. Five shared keys, six ops that read nothing beyond them, `get` when `op` is absent, and every `values.json` key of Output — `json` included, although it is present only for a JSON body of at most 1 MiB. The label, as the
Dockerfile carries it:

```dockerfile
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"fields":{"url":{"type":"string","required":true},"headers":{"type":"object"},"body":{"type":"string"},"timeoutSeconds":{"type":"number"},"captureBodyAs":{"type":"string"}},"ops":{"get":{},"post":{},"put":{},"patch":{},"delete":{},"head":{}},"defaultOp":"get","values":{"status":{"type":"number"},"headers":{"type":"object"},"durationMs":{"type":"number"},"sizeBytes":{"type":"number"},"json":{"type":"object"}}}'
```

`orkctl activity inspect http` renders it as a table.

## Cancellation

curl runs in the background under a TERM trap (§10.2 rule 2a), so cancellation
interrupts an in-flight request within the grace period rather than being
escalated to SIGKILL.

## Building

```bash
make -C images image-http
```
