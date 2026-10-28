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
An authored document may write runtime data as YAML; the strict mapping below
turns it into this JSON, never a lenient decode.

## Data written in an authored document

An Orkflow and an automation document are YAML, and some of what they hold is
runtime data: an Orkflow's `inputs:` (their defaults), every task's `with:` —
an `http` `body:`, jq `vars:`, a composed task's input — an await's `schema:`,
and a schedule's `input:`. It is written as YAML and becomes JSON through one
strict mapping, never a lenient decode. On the wire it is the JSON this page
describes, and the API, the CLI and the UI take only JSON (`AGENTS.md` §3.1).

| YAML | JSON |
|---|---|
| a mapping | an object, keys as written |
| a sequence | an array |
| a quoted or block scalar (`'…'`, `"…"`, `\|`, `>`) | a string, exactly |
| `null`, `~`, nothing | `null` |
| `true`, `false` (also `True`, `TRUE`, `False`, `FALSE`) | a boolean |
| a decimal number (`42`, `-1`, `+12`, `1.5`, `.5`, `1.5e+3`), or hex `0x1F` | a number |
| any other plain text | a string |

A plain (unquoted) value is accepted only when the YAML readers an author's
tools use agree on it — YAML 1.2 (editors), YAML 1.1 (many libraries) and the
reader earlier OrkStack releases used — and the JSON it becomes keeps its value
exactly. Anything else is refused, naming the path and the line. Quote it for
the string, or write the number as JSON writes it:

| Refused | Why | Examples |
|---|---|---|
| YAML 1.1 booleans | a boolean to YAML 1.1, a string to YAML 1.2 | `yes`, `No`, `on`, `OFF`, `y`, `n` |
| numbers YAML 1.1 reads differently | two readers, two numbers — or a number and a string | `0755` (493 or 755), `0b101`, `1_000`, `-0x1F`, `1:30` (90, base 60) |
| numbers YAML 1.1 reads as text | write `1000`, `1.0e+3` or `-0.5` | `1e3`, `1.5e3`, `-.5`, `0o17`, `08` |
| a number earlier releases read differently | write `31` | `0X1F` |
| dates and times | a timestamp to some readers; JSON has none | `2026-01-02`, `2026-01-02T03:04:05Z` |
| numbers JSON cannot hold | | `.inf`, `.nan`, `1e400`, `9007199254740993` (beyond 2^53) |
| numbers JSON would change | a JSON number keeps about 17 significant digits; write the number it becomes, or quote it | `9007199254740993.0` (becomes `9007199254740992`), `0.30000000000000001` (becomes `0.3`), `1.0e-400` (becomes `0`) |
| tags, anchors, aliases, merge keys | write the data out | `!!str 1`, `&base`, `*base`, `<<:` |
| a null key, or a key the readers disagree on | a JSON key is a string | `~:`, `yes:`, `0755:` |
| a key written twice in one mapping | a JSON object holds each key once | `a: 1` and `a: 2` in one mapping, or `1:` beside `"1":` |

A version-like value in one of these places is data like any other: under
`with:`, `version: 1.10` is the number `1.1`, as JSON's own `1.10` is. Quote
it for the text: `version: '1.10'`. An Orkflow's own top-level `version:` is
not runtime data: it is always read as text, so `version: 1.10` there names
the version `1.10`.

`orkctl export` writes a schedule's `input:` back as a mapping, quoting every
string a YAML reader could read as something else, so an export applies
unchanged.

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
