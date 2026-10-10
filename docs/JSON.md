# JSON reference — OrkStack's enum dialect

## Why JSON, not YAML

Run input, task/run output, and `values.json` are JSON — never YAML — even
though the Orkflow DSL itself is YAML (`docs/DSL.md`). The two formats do
different jobs: YAML is for a document a person authors, with comments and
multi-line blocks — an Orkflow. JSON is for runtime data a system exchanges —
what a run was submitted with, what it produced — validated against a proto
`Struct` and carried over the API and CLI with no authoring step in between.

JSON's explicit `null`, `true`, `false`, number, and string types have no
implicit coercion to catch a reader by surprise. YAML's unquoted `no`, `yes`,
`on`, `off`, and octal-looking numbers are exactly the silent-coercion class
of bug the browser's typed input editor is built to avoid — see
`docs/UI_CLI_PARITY.md`'s "false/zero/empty string not dropped" and "invalid
numeric/JSON data cannot silently coerce" requirements. Standardizing on one
format also keeps the "CLI equivalent" honest: `orkctl run submit
--input-json`, the Connect API's `Payload`, and the browser's JSON/Fields
editor all agree on this one wire shape, so a copied command reproduces
exactly what the UI would have sent — a second, YAML, input mode would need
its own YAML→JSON translation step, and a gap between what a reader typed and
what was actually submitted is exactly what this dialect exists to prevent.

## The enum dialect

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
| Connect JSON **requests** | **both accepted** | generated clients send the canonical name; hand-written ones may send the short form |
| proto / gRPC binary | unchanged | the contract in `proto/` still declares `STATUS_*` |

The proto contract is untouched: `Status` and `EventType` keep their `STATUS_*`
and `EVENT_TYPE_*` value names. This is a serialization dialect, not a rename.

## Input tolerance

Requests may spell an enum value three ways. All of these select the same
runs — the filter's operator is the request's enum, and the status, a filter
value, is read in the same dialect:

```bash
curl -H 'Content-Type: application/json' -d '{"filters":[{"field":"status","op":"EQUALS","values":["RUNNING"]}]}'                 …/RunService/ListRuns
curl -H 'Content-Type: application/json' -d '{"filters":[{"field":"status","op":"FILTER_OPERATOR_EQUALS","values":["STATUS_RUNNING"]}]}' …/RunService/ListRuns
curl -H 'Content-Type: application/json' -d '{"filters":[{"field":"status","op":"equals","values":["running"]}]}'                 …/RunService/ListRuns
```

A name that is neither spelling is not invented into a value: Connect's codec
contract discards unknown input, so the field stays unset — and a filter whose
operator is unset is refused (`filters[0]: status: no operator`), never read
as no filter.

`--filter` enum values accept both forms, in any case.

### Filter values

A list filter's values ([UI_CLI_PARITY.md](UI_CLI_PARITY.md), "Filtering
lists") are this dialect's spellings: an enum value by its short name
(`FAILED`), `true`/`false`, an RFC 3339 instant with a zone, text verbatim —
exactly what `--json` prints, so a value copied from `--json` output filters
for it. An enum value is read with this dialect's tolerance — any case, the
type prefix optional — and one more: `-` reads as `_`, so `parent-run` is
`PARENT_RUN`. The JSON codec accepts the same `-` spelling, since one rule
reads both.

## Implementation

`internal/enumname` is the one implementation of the short and canonical
spellings (`Short`, `Proto`, `Prefix`), shared by the JSON codec and the list
filter compiler (`internal/listfilter`). `internal/api/json.go` owns the
codec:

- `api.MarshalJSON` / `api.MarshalJSONOptions` — encode with short enum names.
  Used by the WebSocket bridge (`internal/api/ws.go`) and by `orkctl`
  (`cmd/orkctl/output.go`, one shared encoder, no second implementation).
- `api.UnmarshalJSON` / `api.UnmarshalJSONOptions` — decode either spelling.
- `internal/api/codec.go` registers a Connect codec built on the tolerant
  decoder for every service (`HandlerOptions`).

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
