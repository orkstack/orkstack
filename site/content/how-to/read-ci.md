# Read a red CI run

Find out why a check on your branch or pull request is red, and whether the
answer is a fix or a job run again.

## Find the workflow

The `proof` job of the `proof` workflow and the `pr-title` job gate a merge, once the GitHub rule is on (see [How OrkStack is proven](/docs/concepts/proven/)). `check` runs
`make check` in a few minutes and is the first to report a lint, vet, unit or
UI type failure. None of the others gates a merge: the nightly and the release run on
`main`, and the Podman, Kubernetes and site workflows on the changes that
touch their paths. Each one, and what its red means, is a row of
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

- **`pr-title`:** your pull request lands as one squash commit whose subject is
  its title, and the title is not a Conventional Commit. Edit it to
  `type(scope): description`, such as `fix(orker): keep the shared directory`;
  the edit runs the check again, and nothing needs pushing.
- **"update this branch":** your branch does not contain `main`'s tip, and
  the proof proves only the tree that lands. Merge `main` into it and push.
- **A draft:** the `proof` job fails until the pull request is marked ready
  for review.
- **A failing test, lint, build or vet:** fix it and push. When a gate of
  `fast` fails, it cancels the run, on purpose: the proof could not pass
  anyway, and the shards it cancelled are not run again.
- **A runner GitHub lost, a runner below the health floor, or a fetch or
  install step that failed:** `ineligible_reasons` says so. Re-run that job
  alone, the same day.
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
