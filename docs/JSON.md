# JSON reference — OrkStack's enum dialect

Proto names every enum value with its type as a prefix: `STATUS_SUCCEEDED`,
`EVENT_TYPE_RUN_FAILED`. That prefix is dead weight in JSON, where the field
name already says which enum you are reading. OrkStack strips it on the way out
and accepts it on the way in.

```jsonc
// orkctl run get <id> --json
{
  "run": {
    "id": "ork-1786837143070480000-1",
    "status": "SUCCEEDED",          // not STATUS_SUCCEEDED
    "taskRuns": [{ "taskId": "fetch", "status": "SUCCEEDED" }]
  }
}
```

```jsonc
// a frame from ws://…/ws/runs/{id}
{ "runId": "ork-…-1", "sequence": "2", "type": "RUN_STARTED", "status": "RUNNING" }
```

## What emits which spelling

| surface | enum spelling | why |
|---|---|---|
| `orkctl … --json` (every data command) | short — `SUCCEEDED` | humans and `jq` read it |
| WebSocket bridge `/ws/runs/{id}` | short — `RUNNING` | the browser's realtime feed |
| Connect JSON **responses** | canonical — `STATUS_SUCCEEDED` | generated clients (protobuf-es `fromJson`) only know declared names and reject anything else |
| Connect JSON **requests** | **both accepted** | migration: old and new clients both work |
| proto / gRPC binary | unchanged | the contract in `proto/` still declares `STATUS_*` |

The proto contract is untouched: `Status` and `EventType` keep their `STATUS_*`
and `EVENT_TYPE_*` value names. This is a serialization dialect, not a rename.

## Input tolerance

Requests may spell an enum value three ways. All of these select the same
filter:

```bash
curl -H 'Content-Type: application/json' -d '{"status":"RUNNING"}'        …/RunService/ListRuns
curl -H 'Content-Type: application/json' -d '{"status":"STATUS_RUNNING"}' …/RunService/ListRuns
curl -H 'Content-Type: application/json' -d '{"status":"running"}'        …/RunService/ListRuns
```

A name that is neither spelling is not invented into a value: Connect's codec
contract discards unknown input, so the field stays unset.

`orkctl --status` has always accepted both forms (`cmd/orkctl/input.go`).

## Implementation

`internal/api/json.go` owns the dialect:

- `api.MarshalJSON` / `api.MarshalJSONOptions` — encode with short enum names.
  Used by the WebSocket bridge (`internal/api/ws.go`) and by `orkctl`
  (`cmd/orkctl/output.go`, one shared encoder, no second implementation).
- `api.UnmarshalJSON` / `api.UnmarshalJSONOptions` — decode either spelling.
- `internal/api/codec.go` registers a Connect codec built on the tolerant
  decoder for all three services.

The rewrite is **descriptor-driven**: it walks the message schema alongside the
JSON tree and only renames values sitting in an enum-typed field. Consequences
worth knowing:

- Opaque user data is never touched. A `Payload` whose string value is
  literally `"STATUS_SUCCEEDED"` survives verbatim.
- Well-known types (`google.protobuf.Struct`, `Timestamp`, `Any`, …) are
  skipped — their JSON forms do not mirror their fields.
- Field order matches protojson (message field order); only the enum values
  differ. The output is compacted, which also removes protojson's deliberately
  unstable inter-token whitespace.
- A prefix that would collide with a declared short value is kept.

Browsers decoding WebSocket frames with the generated `EventSchema` restore the
prefix first — see `withProtoEnumNames` in `ui/src/lib/runEvents.ts`.
