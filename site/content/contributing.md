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
make up                # build ork, orker and orkctl; start them and the UI's dev server
```

The stack listens on loopback only, the engine included: it is reachable from
this machine and from no other. `ORK_HTTP_ADDR=:8080 make up` opens the engine
to other machines.

The UI is at http://localhost:5173, served by its dev server with hot reload;
its API and live-stream calls go to the engine on 8080. Use `./bin/orkctl`
wherever these docs say `orkctl`. `make down` stops what `make up` started.

The engine serves the UI built into `bin/ork` at http://localhost:8080. A
build from Go alone serves a placeholder page there: `make ui` builds the UI
into `ork`, in Docker, and `make build` then embeds it.

## Check a change

```bash
make check     # lint, vet, the unit tests and the UI's type-check: no Docker, a few minutes
```

Every change ships with tests, and lands only after a full proof of the
merged tree passes; CI runs it, sharded, as the `proof` check. A parser
of input OrkStack does not control also gets a fuzz target; `make check` runs
its seeds, and `go test -list '^Fuzz' ./...` lists them all (see "Fuzzing" in
[CONTRIBUTING.md](https://github.com/orkstack/orkstack/blob/main/CONTRIBUTING.md)).

## How changes are proven

Each change goes through the same checks, at different scopes:

| Check | When it runs | Counts as merge evidence |
|---|---|---|
| Full proof (`make proof`, or CI's `proof` check) | Before a change lands on `main`: every test against a real cluster and object store, and the unit tests under the race detector | Yes |
| Nightly proof | Every day on `main`, on GitHub: the largest scale and soak tests, the busiest packages' unit tests raced three times over, and every fuzz target for a bounded time | No |
| Pre-release proof (`make proof-release`) | Before a release tag, on the exact commit | Required for a release |

A change is done only after a full proof passes on the exact tree that lands. The reference is [docs/PROVEN.md](https://github.com/orkstack/orkstack/blob/main/docs/PROVEN.md#the-proof-tiers), and [What is proven](/docs/concepts/proven/) explains the results for users.

## Open the pull request

A pull request lands as one squash commit: its title becomes the subject and its
description the body. So the title must be a Conventional Commit, such as
`fix(orker): keep the shared directory on a retry`, and a required check refuses
one that is not. The description names the tests and specs that prove the change.
The rule, and how a landing is checked, are in
[docs/PROOFS.md](https://github.com/orkstack/orkstack/blob/main/docs/PROOFS.md#6-the-merge-queue-the-nightly-tier-and-the-pre-release-gate).

## Record the change

A change that may affect users adds its entry under `[Unreleased]` in the
[changelog](/docs/reference/changelog/), with a breaking one marked
**BREAKING**. At a release that section becomes the version's own, and its
text is the release's notes on GitHub, word for word, above a list of the
release's image digests (see "The changelog" in
[CONTRIBUTING.md](https://github.com/orkstack/orkstack/blob/main/CONTRIBUTING.md)).

The entry is a sentence or two on what changed and what users must do about it,
if anything, with an absolute link to the doc where one holds more detail and,
once changes land through pull requests, a link to its pull request. Every
security fix to what a release publishes has one, a vulnerable dependency's bump
among them.
A change that cannot affect users gets none: a documentation, test or
refactoring change, a cosmetic change to the UI, internal tooling, a dependency
bump that cannot affect them, or a CI change that leaves what a release
publishes as it was.

## See also

- [CONTRIBUTING.md](https://github.com/orkstack/orkstack/blob/main/CONTRIBUTING.md): the checklist, the development stack, and what CI runs
- [AGENTS.md](https://github.com/orkstack/orkstack/blob/main/AGENTS.md): the engineering contract every change follows
- [docs/PROOFS.md](https://github.com/orkstack/orkstack/blob/main/docs/PROOFS.md): how changes are proven and merged
- [Changelog](/docs/reference/changelog/)
