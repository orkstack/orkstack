# Your first Orkflow

Start OrkStack with Docker, then apply an Orkflow, run it, and read each item's log and the merged result. This takes about ten minutes.

> **This works once the first release is published.** Until then there is no bundle to download. [Build from source](/docs/contributing/) instead.

## What you need

- Docker, with Compose v2. Docker Engine or Docker Desktop.
- curl and tar, to unpack the bundle.

Nothing else is needed: no toolchain and no database. The bundle starts Temporal, an S3-compatible object store, the engine, an Orker and the UI.

## 1. Download the bundle

```bash
curl -fsSLO https://github.com/orkstack/orkstack/releases/latest/download/orkstack-quickstart.tar.gz
tar -xzf orkstack-quickstart.tar.gz
cd orkstack-quickstart
```

The folder holds `compose.yaml`, an `examples/` folder, and a `.env` file. Run `ls -a` to see `.env`.

## 2. Start the stack

```bash
docker compose up -d --wait
```

The command returns when every service passes its health check. Open the UI at http://localhost:5173. The engine listens on port 8080 and the Temporal UI on 8233. All of them listen on this machine only.

Listening on this machine only is not a login. Any program on it can start a run, and a run is a container on your Docker. Stop the stack with `docker compose down` when you are done.

If a port is taken, set `ORKSTACK_UI_PORT` or `ORKSTACK_API_PORT` in `.env`.

## 3. Set up orkctl

```bash
alias orkctl='docker compose run --rm orkctl'
```

Run the rest of the steps in the bundle folder. The `orkctl` container sees that folder read-only, so `orkctl apply examples/...` works.

## 4. Apply the Orkflow

```bash
orkctl apply examples/each-fan-out.ork.yaml
```

The output names the Orkflow and its version, `0.3.0`. Applying checks the document, resolves each image to a digest, and stores an immutable version. Nothing runs yet.

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

This stops the stack and keeps the object store's data. To delete the stored data as well, run `docker compose down -v`.

## Next

- [Your first activity image](/docs/tutorials/first-activity/): build a container and run it as a task.
- [Fan out over a list](/docs/how-to/fan-out/): every rule behind the fan-out in this tutorial.
- [Examples](/docs/examples/): the other example Orkflows, and what each one needs.

## See also

- [docs/GETTING_STARTED.md](https://github.com/orkstack/orkstack/blob/main/docs/GETTING_STARTED.md): the same steps, with the checksum check and how to install `orkctl` as a binary.
- [examples/each-fan-out.ork.yaml](https://github.com/orkstack/orkstack/blob/main/examples/each-fan-out.ork.yaml): the Orkflow this tutorial runs.
