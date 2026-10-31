# Read a red CI run

Find out why a check on your branch or pull request is red, and whether the
answer is a fix or a job run again.

## Find the workflow

Only the `proof` job of the `proof` workflow gates a merge. `check` runs
`make check` in a few minutes and is the first to report a lint, vet or unit
failure. The nightly, release, Podman, Kubernetes and site workflows report on
`main` and on releases. Each one, and what its red means, is a row of
[docs/CI.md](https://github.com/orkstack/orkstack/blob/main/docs/CI.md#ci).

## Read the proof's verdict

The verdict is the merged `proof.json`, not the run's colour: a shard that
missed a wall-clock budget only because its runner is small stays red while
the proof passes.

```bash
gh run view <run> --json attempt -q .attempt
gh run download <run> -n proof-<run>-a<attempt> -D proof/ci-<run>
jq '{result, merge_eligible, ineligible_reasons}' proof/ci-<run>/proof.json
```

## Fix it, or run one job again

- **A failing test, lint, build or vet:** fix it and push. When `fast` fails,
  the integration, UI and activity-fixture shards never start, on purpose: the
  proof could not pass anyway.
- **A runner GitHub lost, a runner below the health floor, or a fetch or
  install step that failed:** `ineligible_reasons` says so. Re-run that job
  alone, the same day. Re-running `fast` runs the shards that waited for it
  too.
- **Anything else:** never re-run a job that recorded a test result; it makes
  the proof ineligible. Push a fix, which starts a new run.

## A red nightly

A red nightly of `main` holds no merge, but it is a regression: fixing it, or
reverting what broke it, is the next work, and no release is tagged until a
later nightly passes. Once the fix is on `main`, dispatch a nightly of it:

```bash
gh workflow run nightly.yml --ref main
```

## See also

- [CI](https://github.com/orkstack/orkstack/blob/main/docs/CI.md#ci): every workflow, its trigger, its duration and what to do when it is red
- [Proofs, §8, reading a failed proof](https://github.com/orkstack/orkstack/blob/main/docs/PROOFS.md#8-reading-a-failed-proof): where each failure is recorded
- [Proofs, §10, procedures](https://github.com/orkstack/orkstack/blob/main/docs/PROOFS.md#10-procedures): running a part again, reading the nightly, recovering a release
- [Contribute](/docs/contributing/)
