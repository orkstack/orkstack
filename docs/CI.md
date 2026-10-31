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
| `ci.yml` › `check` | every push and every pull request | `make check`: lint, vet and the unit tests with `-short`, no Docker | no | 2–5 min | Run `make check` locally and fix what it names. A failing unit test also stops the full proof's shards. |
| `ci.yml` › `vuln` | every push and every pull request | `make vuln`: known Go vulnerabilities the code can reach | no | under 1 min | Bump the module to the exact version the report names, and prove that change like any other ([PROOFS.md §10](PROOFS.md#10-procedures), "Read the nightly after a merge"). |
| `ci.yml` › `feature` | the `ci:feature` label, a push to `validate/feature/**`, or a dispatch | the optional feature proof of a branch | no: it gates nothing | not measured | Read its README; a refusal (exit 2) names the path or evidence it could not account for ([PROOFS.md §3](PROOFS.md#3-how-proofrun-select-picks-evidence)). |
| `proof.yml` | a pull request, a merge group, a push to `validate/full/**`, a dispatch, or a call from `nightly.yml` or `release.yml` | the full proof, sharded across runners: every part at once, a failing `fast` gate cancelling the rest, then the `proof` job merges every part ([PROOFS.md §5](PROOFS.md#5-the-ci-job-graph)) | the `proof` job is the merge gate: required once the gate is enforced, and until then the evidence `make merge-check PROOF=` reads | about 15–18 min on a green run; a failing `fast` gate cancels the run when `fast` ends, 12–16 min in ([PROOFS.md §11.5](PROOFS.md#115-measured-numbers)) | On a pull request, `base` fails with "update this branch" when the branch lacks `main`'s tip (merge `main` into it), and a draft's `proof` job fails until it is marked ready ([PROOFS.md §5](PROOFS.md#5-the-ci-job-graph)). Otherwise read `proof.json`: `result`, `merge_eligible`, `ineligible_reasons` ([PROOFS.md §8](PROOFS.md#8-reading-a-failed-proof)). A part whose runner was lost or measured below the floor, or that failed in a setup step, may be re-run alone; a failing test never is: fix it ([PROOFS.md §10](PROOFS.md#10-procedures)). |
| `proof-rerun.yml` › `rerun` | the completion of a failed `proof` run of a `validate/full/**` push, or of a failed scheduled or dispatched `nightly` run on `main` | runs the failed jobs again, when nothing in them recorded a result | no; off unless the repository variable `PROOF_AUTO_RERUN` is `true` (it is, since 2026-10-30) | about 1 min when it only plans; it has rerun nothing yet ([PROOFS.md §12](PROOFS.md#12-known-gaps)) | The planner or the rerun itself failed: re-run the lost, degraded or setup-failed jobs by hand ([PROOFS.md §10](PROOFS.md#10-procedures)). |
| `nightly.yml` | 05:17 UTC every day (GitHub has started it hours late) and a dispatch | the deferred scale and soak tests, the race repeat, the fuzz lane, `vuln`, the README's Quickstart, and the flake report | no: a red nightly holds no merge, but it is a regression on `main` and holds the release | about 110 min, the fuzz lane's budget | Fix it, or revert what broke it, as the next work, then dispatch a nightly of the fix ([PROOFS.md §10](PROOFS.md#10-procedures), "Read the nightly after a merge"). A part whose runner was lost or measured below the floor, or that failed in a setup step, is not a regression: re-run that part. |
| `release.yml` | a dispatch naming a `version` | the release tier's proof, the multi-arch build and both smokes, then, on `main` and after one approval, the tag, the images and the GitHub release | the gate of its own release | 4 h 14 min for v0.1.0 (run 41080710050), its approval and the wait for its nightly included | A refused version or changelog: fix it and dispatch again. A red `gate` is read like a full proof's, by its `proof.json`; `verify` refusing it, or a failing build or smoke in `artifacts`, needs a fix and a new dispatch. `nightly` refusing because the commit's nightly is still running or has not been dispatched: re-run that job once it passes. A failed `publish`: re-run it within 7 days ([PROOFS.md §10](PROOFS.md#10-procedures), "Run the release gate", "Recover a failed publish"). |
| `site.yml` | a pull request that touches a path the site reads, a push to `main`, or a dispatch | builds the docs site; on `main`, deploys it to GitHub Pages | no | about 1 min | Run `cd site && bun run check && bun run build` and fix what it names: a page that cites a missing doc or heading, a broken link, a colour outside the UI's tokens. |
| `podman.yml` | a push or pull request that touches the Orker's runtime, 05:41 UTC every day, and a dispatch | the runtime suites on Podman, rootless and rootful ([DEPLOY.md](DEPLOY.md), "Running on Podman") | no | about 5 min | The job's summary and its `logs` step name the failing suite; reproduce with `scripts/integration.sh ./internal/orker` on a Podman host. |
| `kind-orker.yml` | a push or pull request that touches the Kubernetes Orker bases or the Orker, 06:29 UTC every day, and a dispatch | `scripts/kind-orker.sh`: the Kubernetes Orker check on a throwaway kind cluster ([DEPLOY.md](DEPLOY.md), "Orkers on Kubernetes") | no | about 7 min | Its summary names the failed check; the `kind-orker-<run>-a<attempt>` artifact holds the logs and the cluster's state. |

Every action a workflow uses is pinned by commit SHA to its latest release,
and Go, Bun, Node and cosign to exact versions; Dependabot moves them, one
pull request and one proof at a time. No `run:` step splices a workflow input
or an event field into its shell: each reaches the step through `env:`.

## See also

- [PROOFS.md](PROOFS.md): the tiers, the job graph, the artifacts, reading a
  failed proof, and every procedure
- [CONTRIBUTING.md](../CONTRIBUTING.md): what CI runs on a pull request, and the
  development loop
