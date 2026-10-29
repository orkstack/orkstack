# Your first Orkflow

Clone OrkStack and start it with Docker, then apply an Orkflow, run it, and read each item's log and the merged result. This takes about ten minutes, most of it the first build.

## What you need

- Docker, with Compose v2. Docker Engine or Docker Desktop.
- git, to clone the repository.

Nothing else is needed: no toolchain and no database. The compose file builds the whole stack in containers and starts Temporal, an S3-compatible object store, the engine, an Orker and the UI.

## 1. Clone the repository

```bash
git clone https://github.com/orkstack/orkstack.git
cd orkstack
```

Run the rest of the steps in this folder.

## 2. Start the stack

```bash
docker compose up --build -d --wait
```

The first run builds every image from the clone, which takes a few minutes. The command returns when every service passes its health check. Open the UI at http://localhost:5173. The engine listens on port 8080 and the Temporal UI on 8233. All of them listen on this machine only.

Listening on this machine only is not a login. Any program on it can start a run, and a run is a container on your Docker. Stop the stack with `docker compose down` when you are done.

If a port is taken, set `ORKSTACK_UI_PORT` or `ORKSTACK_API_PORT` in a `.env` file in this folder.

## 3. Set up orkctl

```bash
alias orkctl='docker compose run --rm orkctl'
```

This runs `orkctl` from the image the stack built. The container sees the folder read-only, so `orkctl apply examples/...` works.

## 4. Apply the Orkflow

```bash
orkctl apply examples/each-fan-out.ork.yaml
```

The output names the Orkflow and its version, `0.3.0`. Applying checks the document, resolves each image to a digest, and stores an immutable version. Nothing runs yet.

The UI's Apply page does the same, and from there a run is planned, submitted and followed to its artifacts:

<!-- site-media: e2e-demo -->

## 5. Plan the run, then start it

```bash
orkctl run plan each-fan-out
orkctl run submit each-fan-out --watch
```

`run plan` lists the tasks a run would execute, and starts nothing. `run submit` prints `Submitted run` with the run's ID, then follows the run until it ends. The last line reads `Run <id> finished with status SUCCEEDED`.

The UI shows the same run as a graph, with each item as it runs:

<!-- site-media: ui-run-live -->

## 6. Read the results

Use the run ID from step 5 in place of `<run-id>`:

```bash
orkctl run logs <run-id> --task convert --item 2
orkctl run logs <run-id> --task merge
orkctl run artifacts <run-id>
```

- The first command prints item 2's log: `converted c (2 of 4)`. Items count from 0, so item 2 is the third element.
- The second prints the merged list, one name per line: `a`, `b`, `c` and `d`. The task writes the merged file and copies it to its log.
- The third lists the artifacts the run's tasks wrote, including `merged.txt`.

The run page shows each task's attempts, timing, logs and artifacts:

<!-- site-media: ui-run-finished -->

## 7. Stop the stack

```bash
docker compose down
```

This stops the stack and keeps the object store's data. To delete the stored data as well, run `docker compose down -v`. To update later, run `git pull`, then the command from step 2 again.

## Next

- [Your first activity image](/docs/tutorials/first-activity/): build a container and run it as a task.
- [Fan out over a list](/docs/how-to/fan-out/): every rule behind the fan-out in this tutorial.
- [Examples](/docs/examples/): the other example Orkflows, and what each one needs.

## See also

- [docs/GETTING_STARTED.md](https://github.com/orkstack/orkstack/blob/main/docs/GETTING_STARTED.md): the same steps, with what the compose file builds and how to install `orkctl` as a binary.
- [examples/each-fan-out.ork.yaml](https://github.com/orkstack/orkstack/blob/main/examples/each-fan-out.ork.yaml): the Orkflow this tutorial runs.
