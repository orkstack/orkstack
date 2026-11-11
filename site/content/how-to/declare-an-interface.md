# Declare an interface

Write down what an activity image reads and writes, in one YAML file beside its Dockerfile. OrkStack packs the file into the image. Then `orkctl` checks every task's `with:` against it, and your editor can complete the keys. The image can also draw an icon and describe itself.

## Write interface.ork.yaml

Put this file next to the Dockerfile:

```yaml
# interface.ork.yaml, beside the Dockerfile.
apiVersion: orkstack/v1
kind: ActivityInterface
description: >-
  Copies one file into an output artifact.
ops:
  copy:
    description: Copies one file.
    fields:
      src:
        type: string
        required: true
        description: The file to copy, under `/orkstack/input`.
      as:
        type: string
defaultOp: copy
values:
  path:
    type: string
    description: The artifact the copy wrote.
```

The header is the same as an Orkflow's. `kind: ActivityInterface` says what the file is.

| Key | What it declares |
|---|---|
| `ops:` | The operations the image runs, chosen by the `op:` key in `with:`. Each op lists its own `fields:`. |
| `fields:` | The `with:` keys every op reads. An image with no ops lists its whole `with:` here. |
| `defaultOp:` | The op that runs when `op:` is absent. Without it, a task with `ops:` must name its `op:`. |
| `values:` | The keys the image writes to `values.json`. This is documentation. |
| `type:`, `required:`, `default:` | Each key's type (`string`, `number`, `boolean`, `object`, `array`, or several joined by `|`). `default:` is documentation only. |
| `description:` | One plain paragraph. Text in backticks is shown as code. |
| `icon:` | One SVG drawn on a 16 by 16 grid. Omit it if the image has none. |

Use the same words as an Orkflow's `inputs:`, with no pattern, range or enum. Write `required:` and `default:` as YAML. OrkStack converts them to JSON, and it refuses a plain value such as `yes` or `0755` that readers disagree on. See [JSON values](/docs/reference/json/).

## Pack it into the image

Build the image with `orkctl`, which packs the file into three labels:

```bash
orkctl activity build ./my-image --tag registry.example.com/acme/copy:1 --interface interface.ork.yaml
```

The build pushes by default. `--push=false` builds without pushing, which cannot be pinned.

Another toolchain takes the packed value. Print it with `pack`, then pass it to the build:

```bash
orkctl activity interface pack interface.ork.yaml                 # the interface label
orkctl activity interface pack interface.ork.yaml --label icon    # or icon, or docs
docker build --label "org.orkstack.activity.interface=$(orkctl activity interface pack interface.ork.yaml)" .
```

Pack checks the document with the same rules the engine uses, so a value it prints is never refused. It writes the same bytes for the same document.

## What `orkctl orkflow create` and the editor do with it

- `orkctl orkflow validate` and `orkctl orkflow create` check each task's `with:` against the interface. A missing required key, or an `op:` the image does not declare, is an error, and `orkctl orkflow create` refuses. A key the interface does not declare is a warning.
- `orkctl orkflow create` never stores the interface document. It refuses one named on the command line, and skips one it finds in a directory, naming it on stderr.
- Editors complete keys from `docs/schemas/activityinterface.v1.json`. The `*.ork.yaml` file mapping in the repository's DSL reference covers `interface.ork.yaml` too.
- The interface is read from the image each time it is needed. Nothing about it is stored, so it cannot disagree with the image that runs.

Run `orkctl activity inspect <image>` to see what an image declares. The UI's Activities page shows the same interface.

## Describe the activity and draw an icon

The `description:` keys become their own label. They show in clients beside the interface. Every key and op you describe must be one the interface declares.

An icon is one small SVG. Keep it to the allowed elements, with a stroke and no text, and draw it with `fill="none"` on outlines. Pack refuses an icon with anything else, and a client then draws the default icon.

A label the engine cannot read does not stop a run. The image still runs, and the engine reports the unreadable label.

## See also

- [docs/ACTIVITY_IMAGES.md, "Declaring the interface"](https://github.com/orkstack/orkstack/blob/main/docs/ACTIVITY_IMAGES.md#declaring-the-interface): the document, the labels and the validation rules
- [docs/ACTIVITY_IMAGES.md, "Describing the activity"](https://github.com/orkstack/orkstack/blob/main/docs/ACTIVITY_IMAGES.md#describing-the-activity): the descriptions and their bounds
- [docs/ACTIVITY_IMAGES.md, "The icon"](https://github.com/orkstack/orkstack/blob/main/docs/ACTIVITY_IMAGES.md#the-icon): the icon rules
- [docs/ACTIVITY_IMAGES.md, "The label"](https://github.com/orkstack/orkstack/blob/main/docs/ACTIVITY_IMAGES.md#the-label): the label's value
- [docs/CLI.md, "orkctl activity build"](https://github.com/orkstack/orkstack/blob/main/docs/CLI.md#orkctl-activity-build): every build flag
