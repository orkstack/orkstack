# Run it locally

Start the whole OrkStack stack on your machine with one command, then apply an example Orkflow and run it. It takes a few minutes, most of it the first build.

## What you need

- Docker, with Compose v2. Docker Engine or Docker Desktop.
- git, to clone the repository.

Nothing else. There is no toolchain to install and no database to set up. The compose file starts Temporal, an S3-compatible object store, an image registry, the engine (which serves the UI) and one Orker, each in its own container.

This path is checked on Linux with Docker Engine, on amd64. Docker Desktop on macOS and Windows, and arm64, are not checked yet.

## 1. Start the stack

Run these in a folder where you keep your projects. The first line clones the repository and moves into it:

```bash
git clone https://github.com/orkstack/orkstack.git && cd orkstack
docker compose up --build -d --wait
alias orkctl='docker compose run --rm orkctl'
orkctl apply examples/each-fan-out.ork.yaml
orkctl run submit each-fan-out --watch
```

The first `up` builds every image from your clone, which takes a few minutes. Later runs reuse Docker's build cache. The command returns once every service passes its health check.

The `alias` runs `orkctl` from the image the stack built, so you install nothing else. The container sees the clone's folder read-only, which is enough for `orkctl apply examples/...`.

<!-- site-media: cli-quickstart -->

## 2. Open the UI

Open http://localhost:8080. The engine serves the UI beside its API, and the run you just started is in the Runs list.

The Temporal UI is on http://localhost:8233. Both addresses listen on this machine only.

Listening on this machine is not a login. Any program on this machine can start a run, and each task runs as a container on your Docker.

If a port is taken, create a `.env` file beside `compose.yaml` and set `ORKSTACK_API_PORT` (it moves the UI with the API), `ORKSTACK_TEMPORAL_UI_PORT`, or `ORKSTACK_REGISTRY_PORT`.

## 3. Stop it and start over

```bash
docker compose down                   # stop; the object store keeps its data
docker compose down -v                # stop and delete the stored data too
docker compose up --build -d --wait   # start again
```

The quickstart's Temporal keeps run histories in memory. A stopped stack forgets its runs. The Orkflows and the artifacts are in the object store, which `down` keeps unless you add `-v`.

To update to the newest code later, run `git pull`, then the `up` command again. `--build` rebuilds only what the pull changed.

## Install orkctl on your machine

The alias runs `orkctl` in a container. The container sees the clone's folder read-only, and it has no Docker inside it. The alias lasts only for the terminal window where you set it. In a new window, set it again.

Some commands write a file, or need Docker. They need the installed binary:

- `orkctl run evidence export --output` and `orkctl run artifact download --output`, which write a file;
- `orkctl task run` and `orkctl activity build`, which need Docker.

To install the binary, build it with Go (`make build` writes `bin/orkctl`), or take it from a release's archives.

## Next

- [Your first Orkflow](/docs/get-started/first-orkflow/): read the run's results, one item at a time, and plan a run before it starts.

## See also

- [README, Quickstart](https://github.com/orkstack/orkstack/blob/main/README.md#quickstart): the same commands, with the repository's overview
- [docs/GETTING_STARTED.md, "3. Get orkctl"](https://github.com/orkstack/orkstack/blob/main/docs/GETTING_STARTED.md#3-get-orkctl): the alias, and how to install the binary
- [CONTRIBUTING.md](https://github.com/orkstack/orkstack/blob/main/CONTRIBUTING.md): building `bin/orkctl` with `make build`
- [docs/UPGRADING.md, "Release artifacts"](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#release-artifacts): the release archives that carry the binary
- [docs/GETTING_STARTED.md, "2. Start OrkStack"](https://github.com/orkstack/orkstack/blob/main/docs/GETTING_STARTED.md#2-start-orkstack): what the compose file builds and starts, and the ports
- [docs/GETTING_STARTED.md, "7. Update, stop, or start over"](https://github.com/orkstack/orkstack/blob/main/docs/GETTING_STARTED.md#7-update-stop-or-start-over): updating, stopping and the stored data
