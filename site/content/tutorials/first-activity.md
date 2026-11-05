# Your first activity image

Turn a small shell script into an image, push it to the quickstart's registry, and run it as a task of an Orkflow. It takes about ten minutes.

## What you need

This tutorial continues the quickstart. Finish [Your first Orkflow](/docs/get-started/first-orkflow/), or [Run it locally](/docs/get-started/run-locally/), first, so the stack is up. You need Docker and the clone from those steps. Nothing else: no Go, no Bun, no make.

An activity is an image that reads its inputs from `/orkstack/input`, writes its results to `/orkstack/output`, and exits. This one has no OrkStack SDK or library. It follows the file contract and nothing more.

## 1. Check the stack

Run these commands in the clone's folder, in one terminal window. Set the `orkctl` alias in that window too. The alias lasts only for that window, so set it again in a new one:

```bash
alias orkctl='docker compose run --rm orkctl'
docker compose ps
```

The `temporal`, `seaweedfs`, `registry`, `ork` and `orker` services should be running, and `ork` and `orker` should be healthy. If one is not, start the stack again with `docker compose up --build -d --wait`.

## 2. Write the image

In the clone's folder, create a folder named `hello-image` with two files.

`hello-image/Dockerfile`:

```dockerfile
# Alpine Linux, pinned by digest.
FROM alpine:3.24@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a
RUN apk add --no-cache 'jq~1.8'
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod 0555 /usr/local/bin/entrypoint.sh
# never root
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
```

`hello-image/entrypoint.sh`:

```sh
#!/bin/sh
# Greets the name in the task's with: block.
set -eu
umask 022

who=$(jq -r '.who' /orkstack/input/config.json)
echo "hello $who from my own image"
jq -nc --arg greeting "hello $who" '{greeting: $greeting}' > /orkstack/output/values.json
```

The script reads the task's `with:` block from `/orkstack/input/config.json`. What it prints goes to the task's log. It writes one JSON object to `/orkstack/output/values.json`, which is the task's output. Exit `0` means success. Any failure should exit non-zero, and `set -eu` does that for a failed command.

## 3. Build and push the image

```bash
docker build -t 127.0.0.1:5002/hello-image:0.1 hello-image
docker push 127.0.0.1:5002/hello-image:0.1
```

The registry is the quickstart's own. It listens on this machine only, on port 5002. Its address is the one the engine reads, and the Orker's Docker pulls the image from it when a task runs. If you set `ORKSTACK_REGISTRY_PORT` in `.env`, use that port here and in the Orkflow. Write `127.0.0.1` as shown, not `localhost`.

## 4. Write the Orkflow

Save this as `hello-image.ork.yaml`, in the clone's folder:

```yaml
apiVersion: orkstack/v1
kind: Orkflow
name: hello-image
version: "0.1.0"

inputs:
  who:
    type: string
    default: world

tasks:
  - id: greet
    uses: 127.0.0.1:5002/hello-image:0.1
    with:
      who: "{{ .inputs.who }}"
```

`uses:` names the image by its registry address, not by a catalog name. Apply resolves the tag to a digest, and the run uses that digest.

## 5. Apply it and run it

```bash
orkctl apply hello-image.ork.yaml
orkctl run submit hello-image --input who=Ada --watch
```

`run submit` prints `Submitted run` with the run's ID, then follows the run until it ends. The last line reads `Run <id> finished with status SUCCEEDED`.

## 6. Read the log and the output

Use the run ID from step 5 in place of `<run-id>`:

```bash
orkctl run logs <run-id> --task greet
```

The log reads `hello Ada from my own image`. Leave out `--input who=Ada` and the task greets `world`.

The output is the task's `values.json`. Open the run in the UI. The run page's **Leaf task values** card shows it under the task's name: `"greet": {"greeting": "hello Ada"}`.

**If you installed orkctl.** The alias cannot write files, so save the output with the installed binary, called as `./orkctl` because the alias wins over it. [Run it locally](/docs/get-started/run-locally/#install-orkctl-on-your-machine) explains how to install it.

```bash
./orkctl run artifact download <run-id> --task greet --kind values --name values.json --output greeting.json
cat greeting.json
```

The file holds one JSON object, `{"greeting":"hello Ada"}`.

## Next

- [Build an activity image](/docs/activities/build-an-image/): the rules every image follows, and how to declare what it accepts.
- [Test an activity image](/docs/activities/test-an-image/): the test command, and its limits.
- [Activities](/docs/concepts/activities/): the contract this image follows.

## See also

- [docs/GETTING_STARTED.md, "2. Start OrkStack"](https://github.com/orkstack/orkstack/blob/main/docs/GETTING_STARTED.md#2-start-orkstack): the stack, its ports and the registry port.
- [docs/OCI_ACTIVITY_CONTRACT.md, "0. The one-paragraph version"](https://github.com/orkstack/orkstack/blob/main/docs/OCI_ACTIVITY_CONTRACT.md#0-the-one-paragraph-version): the contract in one paragraph.
- [docs/OCI_ACTIVITY_CONTRACT.md, "10.1 The Dockerfile and entry-script rules"](https://github.com/orkstack/orkstack/blob/main/docs/OCI_ACTIVITY_CONTRACT.md#101-the-dockerfile-and-entry-script-rules): what a Dockerfile and its entry script must do.
- [docs/ACTIVITY_IMAGES.md, "The op convention"](https://github.com/orkstack/orkstack/blob/main/docs/ACTIVITY_IMAGES.md#the-op-convention): a task that puts a registry reference directly in `uses:`.
- [docs/CLI.md, "orkctl run artifact download"](https://github.com/orkstack/orkstack/blob/main/docs/CLI.md#orkctl-run-artifact-download): every flag of the download.
- [examples/custom-image-shell/README.md](https://github.com/orkstack/orkstack/blob/main/examples/custom-image-shell/README.md): a larger image of the same kind, in POSIX shell.
