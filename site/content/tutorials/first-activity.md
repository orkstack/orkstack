# Your first activity image

Build the shell image in `examples/custom-image-shell/`, check the settings it declares, and run one of its tasks on your machine.

> **This tutorial uses the development stack.** It runs OrkStack's processes on your machine, beside a registry on `localhost:5001` that the steps push the image to. If the quickstart is running, stop it first with `docker compose down`: the two use the same ports.

You need Docker, Go, Bun, make, git and curl, as [Contributing](/docs/contributing/) lists.

## 1. Start the local stack

```bash
make infra-up
make -C images push
make up
```

- `make infra-up` starts Temporal, SeaweedFS and a registry on `localhost:5001`.
- `make -C images push` builds the first-party activities and pushes them to that registry. The example Orkflow in step 7 uses them.
- `make up` builds `./bin/ork`, `./bin/orker` and `./bin/orkctl`, and starts them with the UI. It prints `OrkStack is up` when every part answers.

The steps below use `./bin/orkctl`.

## 2. Read the image

The image is two files: a `Dockerfile` and an `entrypoint.sh` script. The script reads two files the platform writes, `/orkstack/input/task.json` and `/orkstack/input/config.json`. The second one holds the task's `with:` block. It writes `/orkstack/output/values.json`, and, when you set `src:`, a report file in `/orkstack/output/artifacts/`.

The script exits `0` on success. On any error it exits `1` and writes one line to stderr. The image has no SDK and no OrkStack code. It is an activity only because it follows the file contract.

## 3. Build and push the images

```bash
./bin/orkctl activity build examples/custom-image-shell --tag localhost:5001/orkstack-examples/custom-image-shell:dev
./bin/orkctl activity build examples/custom-image --tag localhost:5001/orkstack-examples/custom-image:dev
```

Each build pushes its image and prints two `uses:` lines. One has the tag. The other has the same name with `@sha256:` and a digest. The second command builds the Go image of the same activity, which the example Orkflow also uses.

## 4. Check the settings the image declares

```bash
./bin/orkctl activity inspect localhost:5001/orkstack-examples/custom-image-shell:dev
```

The image declares three `with:` keys: `label`, `src` and `as`. Without `src`, it writes no report. `orkctl` reads these keys from a label on the image. The container never reads the label.

## 5. Run one task on your machine

The task `report-shell` runs this image. It reads the output of `probe`, a task you have not run. So you supply that output with `--output`:

```bash
./bin/orkctl task run --file examples/custom-image.ork.yaml --task report-shell \
  --output probe='{"idempotencyKey":"x","attempt":1,"taskId":"probe","argv":[]}' \
  --keep
```

`task run` starts the container on this machine. It writes nothing to the deployment: no run, no commit and no log. Any external side effect the container performs is real.

Expect this, in order:

- The rendered `config.json` is printed before the container starts.
- A result line, `SUCCEEDED`, and an `exit code` of `0`.
- `values`: a JSON object with `idempotencyKey`, `attempt`, `taskId` and `argv`. It also has `label`, because the task sets `label:`, and `source`, `artifact` and `stats`, because `src` is set.
- `artifacts`: one row, `report-shell.txt`, with its size and sha256.
- `workspace kept at <path>`, because you passed `--keep`.

## 6. Read the report

Replace `<path>` with the path from step 5:

```bash
cat <path>/output/artifacts/report-shell.txt
```

The report is exactly seven lines: `activity report`, then `label`, `source`, `argv`, `lines`, `words` and `bytes`. The `argv` line shows `{{ .inputs.label }}` as written. The task's `command:` is passed as written, and OrkStack does not render it.

## 7. Run the whole example (optional)

The example Orkflow runs both images:

```bash
./bin/orkctl apply examples/custom-image.ork.yaml
./bin/orkctl run submit custom-image --watch
```

The Go image runs `probe`, `report` and `per-label`. The shell image runs `report-shell`. Given the same configuration and the same source file, both images write byte-identical reports. The `collate` task joins them into `both-reports.txt`. The run should finish with `finished with status SUCCEEDED`. The repository's test suite runs this example end to end.

## Do not use `orkctl activity test` on this image

`orkctl activity test` drives the shell activity's `op: exec` configuration. This image does not read that configuration, so the test's assertions do not describe it. Use `task run`, as in step 5, to try this image.

## Next

- [Build an activity image](/docs/how-to/build-an-activity-image/): write an image of your own.
- [Test an activity image](/docs/how-to/test-an-activity-image/): the test command, and its limits.
- [Activities](/docs/concepts/activities/): the contract this image follows.
- [Custom image example](/docs/examples/custom-image/): the Orkflow that uses both images.

## See also

- [examples/custom-image-shell/README.md](https://github.com/orkstack/orkstack/blob/main/examples/custom-image-shell/README.md): the image's configuration, output and errors.
- [docs/OCI_ACTIVITY_CONTRACT.md](https://github.com/orkstack/orkstack/blob/main/docs/OCI_ACTIVITY_CONTRACT.md#0-the-one-paragraph-version): the contract in one paragraph.
- [docs/ACTIVITY_IMAGES.md](https://github.com/orkstack/orkstack/blob/main/docs/ACTIVITY_IMAGES.md): the conventions the first-party images share.
