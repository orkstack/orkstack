# CI

What each GitHub Actions workflow of this repository runs, when it runs,
whether a merge waits for it, how long it takes, and what to do when it is
red. [PROOFS.md](PROOFS.md) is the reference for the proof machinery behind
`proof.yml`, `nightly.yml` and `release.yml`, and `AGENTS.md` §11 states the
rules every proof follows.

A workflow's colour is not always its verdict. A proof's verdict is the
`proof.json` in its `proof-<run>-a<attempt>` artifact: an excused runner-size
miss leaves a shard red while the proof passes ([PROOFS.md §12](PROOFS.md#12-known-gaps)).

| Workflow › job | Trigger | Purpose | Required | Typical duration | When it is red |
|---|---|---|---|---|---|
| `ci.yml` › `check` | every push, except to a Dependabot branch, and every pull request | `make check`: lint, vet, the unit tests with `-short` and the UI's type-check and unit tests, no Docker | no | 2–5 min | Run `make check` locally and fix what it names. This job stops nothing else: the full proof's `fast` part runs its own unit gate, without `-short`, and a failing gate there is what cancels the proof's shards. |
| `ci.yml` › `vuln` | every push, except to a Dependabot branch, and every pull request | `make vuln`: known Go vulnerabilities the code can reach | no | under 1 min | Bump the module to the exact version the report names, and prove that change like any other ([PROOFS.md §10](PROOFS.md#10-procedures), "Read the nightly after a merge"). |
| `proof.yml` | a pull request, a merge group, a push to `validate/full/**`, a dispatch, or a call from `nightly.yml`, `release.yml` or `proof-label.yml`; a Dependabot pull request runs it only with the `ci:proof` label | the full proof, sharded across runners: every part at once, a failing `fast` gate cancelling the rest (except on a fork's or Dependabot's pull request, whose token cannot), then the `proof` job merges every part ([PROOFS.md §5](PROOFS.md#5-the-ci-job-graph)) | the `proof` job is the merge gate: required once the gate is enforced, and until then the evidence `make merge-check PROOF=` reads | about 15–18 min on a green run; a failing `fast` gate cancels the run when `fast` ends, 12–16 min in ([PROOFS.md §11.5](PROOFS.md#115-measured-numbers)) | On a pull request, `base` fails with "update this branch" when the branch lacks `main`'s tip (merge `main` into it), a draft's `proof` job fails until it is marked ready, and a Dependabot pull request's fails until someone adds the `ci:proof` label ([PROOFS.md §5](PROOFS.md#5-the-ci-job-graph)). Otherwise read `proof.json`: `result`, `merge_eligible`, `ineligible_reasons` ([PROOFS.md §8](PROOFS.md#8-reading-a-failed-proof)). A part whose runner was lost, or whose job failed before its proof step, may be re-run alone; a failing test never is: fix it ([PROOFS.md §10](PROOFS.md#10-procedures)). |
| `proof-label.yml` | any label added to any pull request; it runs only for `ci:proof`, added by a person to a Dependabot pull request that is not a draft | calls `proof.yml` on that event, then reports its verdict as `proof` | that one event's `proof` job is the merge gate for that head; on any other label it is skipped under another name and answers for nothing | a full proof's, or nothing when skipped | Read the called proof's `proof.json`, as for `proof.yml`. "update this branch": comment `@dependabot rebase`, and the labelled pull request's new head is proved. To prove the same head again, remove and add the label; a "Re-run" of a run from before the label never starts it ([PROOFS.md §10](PROOFS.md#10-procedures)). |
| `pr-title.yml` › `pr-title` | a pull request opened, its title edited, a new head, or a merge group | refuses a title that is not a Conventional Commit, because a pull request lands as one squash commit whose subject is its title ([PROOFS.md §6](PROOFS.md#6-the-merge-queue-the-nightly-tier-and-the-pre-release-gate)) | yes, once the gate is enforced, beside `proof` | not measured | Edit the title to `type(scope)!: description`, the scope and the `!` optional, its type one of `feat`, `fix`, `docs`, `chore`, `test`, `ci`, `refactor`, `perf`, `build`, `style`, `revert`. The edit runs the check again; nothing needs pushing. |
| `nightly.yml` | 05:17 UTC every day (GitHub has started it hours late) and a dispatch | the deferred scale and soak tests, the race repeat, the fuzz lane, `vuln`, `vuln-js` and `vuln-images`, `release-notes` (every published release's notes against its CHANGELOG.md section), the README's Quickstart, and every shipped example on that quickstart (`examples`) | no: a red nightly holds no merge, but it is a regression on `main` and holds the release; `examples` alone, which reaches third parties, holds no release | about 130 min, the fuzz lane's budget | Fix it, or revert what broke it, as the next work, then dispatch a nightly of the fix ([PROOFS.md §10](PROOFS.md#10-procedures), "Read the nightly after a merge"). A part whose runner was lost, or whose job failed before its proof step, is not a regression: re-run that part. |
| `release.yml` | a dispatch naming a `version` | the release tier's proof, the multi-arch build and both smokes, then, on `main` and after one approval, the tag, the images and the GitHub release | the gate of its own release | 4 h 14 min for v0.1.0 (run 41080710050), its approval and the wait for its nightly included | A refused version or changelog: fix it and dispatch again. A red `gate` is read like a full proof's, by its `proof.json`; `verify` refusing it, or a failing build or smoke in `artifacts`, needs a fix and a new dispatch. `nightly` refusing because the commit's nightly is still running or has not been dispatched: re-run that job once it passes. `nightly` refusing because that nightly's `release-notes` job failed: a published release's notes differ from its CHANGELOG.md section, so republish it (`make release-notes-publish VERSION=vX.Y.Z`), dispatch a nightly, and re-run the job once it passes. A failed `publish`: re-run it within 7 days ([PROOFS.md §10](PROOFS.md#10-procedures), "Run the release gate", "Recover a failed publish"). |
| `site.yml` | a pull request that touches a path the site reads, a push to `main`, or a dispatch | builds the docs site; on `main`, deploys it to GitHub Pages | no | about 1 min | Run `cd site && bun run check && bun run build` and fix what it names: a page that cites a missing doc or heading, a broken link, a colour outside the UI's tokens. |
| `podman.yml` | a push (except to a Dependabot branch) or pull request that touches the Orker's runtime, 05:41 UTC every day, and a dispatch | the runtime suites on Podman, rootless and rootful ([DEPLOY.md](DEPLOY.md), "Running on Podman") | no | about 5 min | The job's summary and its `logs` step name the failing suite; reproduce with `scripts/integration.sh ./internal/orker` on a Podman host. |
| `kind-orker.yml` | a push (except to a Dependabot branch) or pull request that touches the Kubernetes Orker bases or the Orker, 06:29 UTC every day, and a dispatch | `scripts/kind-orker.sh`: the Kubernetes Orker check on a throwaway kind cluster ([DEPLOY.md](DEPLOY.md), "Orkers on Kubernetes") | no | about 7 min | Its summary names the failed check; the `kind-orker-<run>-a<attempt>` artifact holds the logs and the cluster's state. |

Every action outside this repository, in every workflow and composite
action, is pinned by the full commit SHA of a release of its latest major,
with that release in a comment on the same line (`@<sha> # vX.Y.Z`), and Go,
Bun, Node and cosign to exact versions; Dependabot moves them, the SHA and its
comment together, one pull request and one proof at a time
([PROOFS.md §5](PROOFS.md#5-the-ci-job-graph)). No `run:` step splices a workflow
input or an event field into its shell: each reaches the step through `env:`.

## Dependency updates

`.github/dependabot.yml` has one entry per ecosystem: Go modules, Dockerfiles,
compose files, GitHub Actions and Bun. Each runs monthly and opens at most one
pull request, holding all of that ecosystem's updates, so a month costs at most
five full proofs, each only when someone asks for it. Nothing rebases an open
one; comment `@dependabot rebase` when it is next to merge. GitHub runs every entry when the file itself changes; a push
that leaves it alone starts no `Dependabot Updates` run. That workflow is
GitHub's own and has no file here.

A `chore(deps)` pull request waits for a person before its full proof: until
someone adds the `ci:proof` label its `proof` check fails, naming the label, and it
cannot merge. Adding the label starts the proof on its current head, and every new
head after that is proved too ([PROOFS.md §5](PROOFS.md#5-the-ci-job-graph)). Adding
a label takes triage access or more. The label is created once, by a maintainer:

```bash
gh label create ci:proof --repo orkstack/orkstack --color 0e8a16 --description "Run the full proof on this pull request"
```

Without it nobody can start the proof, and the pull requests wait, unmergeable.
Otherwise such a pull request starts what any pull request would, and nothing as
a push:

| Workflow | When | Cost |
|---|---|---|
| `ci.yml` › `check`, `vuln`, `proto` | always | minutes; red until the pins Dependabot does not move are moved by hand, which `.github/dependabot.yml` lists |
| `pr-title.yml` | always | seconds; its `chore(deps)` title passes |
| `proof.yml` | always, and with the `ci:proof` label its jobs run; without it only `proof`, which fails | seconds to wait; the merge gate, a whole sharded proof, once labelled |
| `proof-label.yml` | on each label; it runs only for `ci:proof` | none, or that whole sharded proof |
| `site.yml` | when it moves `ui/package.json` or anything under `site/` | minutes |
| `kind-orker.yml` | when it moves `deploy/source/` or `deploy/Dockerfile` | about 15 minutes |
| `podman.yml` | when it touches the Orker, which an update does not | none |

## See also

- [PROOFS.md](PROOFS.md): the tiers, the job graph, the artifacts, reading a
  failed proof, and every procedure
- [CONTRIBUTING.md](../CONTRIBUTING.md): what CI runs on a pull request, and the
  development loop
