# Pass inputs in and return outputs

Declare the values a run takes with `inputs:`. Declare the values it returns with `outputs:`.

## Example

In [`examples/inputs-and-escaping.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/inputs-and-escaping.ork.yaml), the run needs a `release`. The other inputs have defaults:

```yaml
inputs:
  release:
    type: string
    required: true
  note:
    type: string
    default: "it's a test; echo unsafe"
  retries:
    type: number
    default: 3
  dry_run:
    type: boolean
    default: true
```

Submit it:

```bash
orkctl apply examples/inputs-and-escaping.ork.yaml
orkctl run submit inputs-and-escaping --input release=1.4.2 --input dry_run=false --watch
```

A value that is valid JSON keeps its type. `dry_run=false` is a boolean. `release=1.4.2` is text.

## Declare an input

| Key | Default | Meaning |
|---|---|---|
| `type` | any JSON value | `string`, `number`, `boolean`, `object` or `array`. |
| `default` | none | The value used when the run does not pass the input. |
| `required` | `false` | `true` makes the input required, unless it has a `default`. |

Read an input as `{{ .inputs.<name> }}` in `with:`, `condition:` or `outputs:`.

## Pass an input at submit

- Give each input as `--input name=value`. Repeat the flag for each input.
- Give an object or an array as `--input-json '{"name": value}'`. Its keys merge under the `--input` values.
- Submit fills in the defaults and checks the size of the input. It does not refuse a missing required input, or a value of the wrong type. A task that reads a missing input fails with `RENDER_FAILED`.

## Write defaults in YAML

The file is YAML. The values a run holds are JSON. Write each default the way JSON writes it.

The strict mapping refuses plain `yes`, `0755`, `1:30` and dates, and names the line. Quote them, as in `'yes'`.

## Return outputs

In [`examples/notify-block.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/notify-block.ork.yaml), the block returns the HTTP status of its one request:

```yaml
outputs:
  status: '{{ .tasks.post.output.status }}'
```

- When every task has finished, the engine renders each template once. The rendered map is the run's output.
- A composing Orkflow receives the same map. See [Run one Orkflow inside another](/docs/how-to/compose/).
- Every value is a string.
- A name is lowercase letters, digits and underscores. It starts with a letter, and it is at most 128 bytes.
- A template can read any task of the Orkflow. A task that did not succeed has no output, so guard the read: `{{ with index .tasks "sign" }}{{ .output.value }}{{ end }}`.

## When an output fails

| Code | Meaning |
|---|---|
| `OUTPUTS_RENDER_FAILED` | A template did not render. |
| `OUTPUTS_TOO_LARGE` | The rendered outputs are over the values limit, 256 KiB by default. |
| `OUTPUTS_UNAVAILABLE` | A task's value could not be read. |

The run ends `FAILED`, even when every task succeeded.

## Limits

- An input over 256 KiB is refused at submit with `INPUT_TOO_LARGE`. The limit counts the JSON the run is submitted with, so quotes and line breaks count twice.
- For large data, pass a URL or an object key, not the bytes. To pass files between tasks, see [Pass files between tasks](/docs/how-to/pass-files/).

## See also

- [DSL reference, “Inputs”](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#inputs) and [“Outputs”](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#outputs): every rule
- [JSON reference, “Data written in an authored document”](https://github.com/orkstack/orkstack/blob/main/docs/JSON.md#data-written-in-an-authored-document): the YAML-to-JSON rules
- [Orkflow keys](/docs/reference/orkflow-keys/): every key on one page
