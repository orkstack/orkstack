# JSON values

A run's input, a task's `values.json` and a run's recorded output are JSON, never YAML. This page shows what that means when you submit input, and which YAML values in an Orkflow OrkStack refuses.

## Why JSON

An Orkflow is YAML, because a person writes it with comments and blocks. The data a run exchanges is JSON. JSON has explicit `true`, `false`, `null`, numbers and strings, and no silent conversions. The same JSON goes through the API, `orkctl --input-json` and the UI's JSON editor. A command you copy from the UI or from `--json` output therefore sends exactly what was submitted.

## Pass input as JSON

```bash
orkctl run submit my-orkflow --input-json '{"count": 3, "tags": ["a", "b"], "dry_run": false}'
orkctl run submit my-orkflow -i count=3 -i 'tags=["a","b"]' -i dry_run=false
```

`--input key=value` parses the value as JSON when it can, so `3`, `true` and `["a","b"]` keep their types. A value that is not JSON stays a string. `--input-json` takes one object, and the `--input` values are merged over it.

The input is one JSON object. The HTTP routes, such as a trigger's, read one JSON value per request body and refuse a second value rather than ignoring it.

## What a task writes

A task writes its result to `/orkstack/output/values.json`. The file must hold one JSON object. A task with no result writes `{}`. A container that exits `0` and writes no `values.json` fails with `VALUES_MISSING`.

## YAML inside an Orkflow

An Orkflow, an automation document and an activity interface are YAML. Runtime data inside them is YAML too, for example an Orkflow's `inputs:` defaults, a task's `with:`, an await's `schema:` and a schedule's `input:`. OrkStack converts that data to JSON with one set of rules. It never decodes leniently.

| YAML | JSON |
|---|---|
| a mapping | an object, with its keys as written |
| a sequence | an array |
| a quoted or block string, such as `'…'`, `"…"`, `\|` or `>` | a string, exactly |
| `null`, `~` or nothing | `null` |
| `true` or `false`, in any case | a boolean |
| a decimal number such as `42`, `-1`, `1.5` or `1.5e+3`, or hex `0x1F` | a number |
| any other plain text | a string |

### What is refused

A plain value is accepted only when the common YAML readers agree on it. Anything else is refused, and the error names the path and the line. Quote the value to keep it as text, or write the number the way JSON writes it.

| Refused | Examples | Write instead |
|---|---|---|
| Words that some YAML readers read as booleans | `yes`, `no`, `on`, `off` | `"yes"` for the text, or `true` for a boolean |
| Numbers that readers disagree on, or read as text | `0755`, `1:30`, `1_000`, `0b101`, `1e3`, `0o17` | the number as JSON writes it, such as `755`, `1000` or `15`, or a quoted string such as `"0755"` |
| Dates and times | `2026-01-02` | `"2026-01-02"` |
| Numbers JSON cannot hold | `.inf`, `1e400`, `9007199254740993` | a quoted string, or a number within JSON's range |
| Numbers JSON would change | `0.30000000000000001` | `0.3` |
| Tags, anchors, aliases and merge keys | `!!str 1`, `&base`, `*base`, `<<:` | the data, written out |
| A key written twice in one mapping | `a: 1` and `a: 2` | one key |

Values that look like versions need care. Under `with:`, `version: 1.10` is the number `1.1`. Quote it to keep the text: `version: '1.10'`. An Orkflow's own top-level `version:` is always text, so `version: 1.10` there names the version `1.10`.

## Enum values in the output

Enum values print in their short form. `orkctl -j` prints `SUCCEEDED`, and the WebSocket feed prints `RUNNING`. Responses from the Connect API print the full form, `STATUS_SUCCEEDED`. Requests accept either form.

## See also

- [docs/JSON.md, "Why JSON, not YAML"](https://github.com/orkstack/orkstack/blob/main/docs/JSON.md#why-json-not-yaml): the reasons in full
- [docs/JSON.md, "Data written in an authored document"](https://github.com/orkstack/orkstack/blob/main/docs/JSON.md#data-written-in-an-authored-document): the strict mapping, and the refused values
- [docs/JSON.md, "The enum dialect"](https://github.com/orkstack/orkstack/blob/main/docs/JSON.md#the-enum-dialect): the short and full enum forms
- [docs/JSON.md, "Input tolerance"](https://github.com/orkstack/orkstack/blob/main/docs/JSON.md#input-tolerance): what requests accept
- [docs/DSL.md, "Inputs"](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#inputs): how an Orkflow declares its input
- [docs/ACTIVITY_IMAGES.md, "Declaring the interface"](https://github.com/orkstack/orkstack/blob/main/docs/ACTIVITY_IMAGES.md#declaring-the-interface): runtime data in an activity interface
