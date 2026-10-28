# Contribute

Build OrkStack from source, run the development stack, and send a change.
The full contributor references live in the repository.

## What you need

| Tool | Version |
|---|---|
| Docker, with Compose v2 | any current release |
| Go | the version in [`go.mod`](https://github.com/orkstack/orkstack/blob/main/go.mod) |
| Bun | the version CI pins |
| make, git, curl | any |

## Run it from source

```bash
git clone https://github.com/orkstack/orkstack.git
cd orkstack
make infra-up          # Temporal, an object store and a local registry, in Docker
make -C images push    # build the first-party activity images into that registry
make up                # build ork, orker and orkctl; start them and the UI
```

The UI is at http://localhost:5173. Use `./bin/orkctl` wherever these docs
say `orkctl`. `make down` stops what `make up` started.

## Check a change

```bash
make check     # lint, vet and the unit tests: no Docker, a few minutes
```

Every change ships with tests, and lands only after a full proof of the
merged tree passes; CI runs it, sharded, as the `proof` check. A feature
proof on a branch is an optional, scoped check. A parser
of input OrkStack does not control also gets a fuzz target; `make check` runs
its seeds, and `go test -list '^Fuzz' ./...` lists them all (see "Fuzzing" in
[CONTRIBUTING.md](https://github.com/orkstack/orkstack/blob/main/CONTRIBUTING.md)).

## See also

- [CONTRIBUTING.md](https://github.com/orkstack/orkstack/blob/main/CONTRIBUTING.md): the checklist, the development stack, and what CI runs
- [AGENTS.md](https://github.com/orkstack/orkstack/blob/main/AGENTS.md): the engineering contract every change follows
- [docs/PROOFS.md](https://github.com/orkstack/orkstack/blob/main/docs/PROOFS.md): how changes are proven and merged
- [Changelog](/docs/reference/changelog/)
