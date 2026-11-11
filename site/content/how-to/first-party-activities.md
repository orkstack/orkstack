# Use the first-party activities

Use the six images that ship with OrkStack by their short names: `http`, `shell`, `file`, `json`, `git` and `template`. Write `uses:` with the name, then set `op:` and the keys for that op.

## Example

An excerpt from [`examples/http-post.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/http-post.ork.yaml), with its comments removed. `publish` makes one request and keeps the response. `archive` copies the response under a name you choose. Each task names one image and one op:

```yaml
  - id: publish
    uses: http
    with:
      op: post
      url: "{{ .inputs.url }}"
      body:
        message: "{{ .inputs.message }}"
      captureBodyAs: response

  - id: archive
    uses: file
    needs:
      - publish
    network: none
    with:
      op: copy
      src: '{{ artifact "publish/response" }}'
      as: response.json
```

Create the Orkflow from the file, then run it:

```bash
orkctl orkflow create examples/http-post.ork.yaml
orkctl run submit http-post -w
```

The example posts to `httpbin.org`. Pass `--input url=<your URL>` to use another endpoint.

## Pick a name

Each name maps to one image in the deployment's catalog. `orkctl activity list` shows the catalog.

| `uses:` | What it does |
|---|---|
| `http` | One HTTP request. |
| `shell` | A command, a script or an evaluation. |
| `file` | Copies, compresses, archives, checksums or joins files. |
| `json` | One jq filter, or a deep merge of JSON objects. |
| `git` | A read-only HTTPS checkout, or the commit a ref names. |
| `template` | Renders one template file. |

## Set the op

`http` defaults to `get`, and `shell` defaults to `exec`. The other four need `op:` in every task.

`orkctl orkflow create` checks each task's `with:` keys against the image's label. It refuses a missing required key, a missing `op` where there is no default, and an unknown `op`. The keys for each name are in the [first-party reference](/docs/reference/first-party-activities/).

## Read the outputs

Each image writes `values.json`. A later task reads a key with `{{ .tasks.<id>.output.<key> }}`, in `with:` or in `condition:`.

| Image | Keys |
|---|---|
| `http` | `status`, `headers`, `durationMs`, `sizeBytes`, `json` |
| `shell` | `exitCode`; `scriptPath` for `script`; `value` for `eval` |
| `file` | `path`, `sizeBytes`, `sha256` |
| `json` | `result`, or `path`, `sizeBytes`, `sha256` when `as:` is set |
| `git` | `commit`, `ref`; `clone` adds `path`, and `sizeBytes` and `sha256` for a tarball |
| `template` | `path`, `sizeBytes`, `sha256` |

## Pass files between tasks

An artifact is a file. Give it a name where the image accepts one: `as:`, or `captureBodyAs:` on `http`. A later task reads it with `{{ artifact "<task>/<name>" }}` in `with:`, as `archive` does above.

## Use a private repository

`git` reads a token from a secret. Name the secret under `secrets:`, and point `tokenSecret:` at it. This excerpt is from the [activity-git README](https://github.com/orkstack/orkstack/blob/main/images/activity-git/README.md):

```yaml
- id: checkout
  uses: git
  needs:
    - resolve
  secrets:
    - name: git-token
      source: env:GIT_TOKEN
  retry:
    maximum_attempts: 3
    initial_interval_seconds: 2
  with:
    op: clone
    url: https://github.com/acme/app.git
    ref: '{{ .tasks.resolve.output.commit }}'
    tokenSecret: git-token
```

The token is a file. The image never puts it in an argument, a URL or a log.

## Cache a task

`cache:` tells OrkStack that reusing a result is safe. You make that promise. Cache only tasks whose result depends on nothing outside their inputs.

| Image | Safe to cache? |
|---|---|
| `http` | Never. A request reads or changes something outside the run. |
| `shell` | Only when the command reads nothing outside its inputs, and mounts no secret. |
| `file` | Yes, unless the task mounts a secret. A `tar` output is not byte-for-byte reproducible, so tasks after a `tar` never hit the cache. |
| `json` | Yes, unless the filter reads the clock, a run's id or a variable that differs per container. |
| `git` | `clone` by a full commit id, for a public repository. Never `resolve`. |
| `template` | Yes, unless the template calls a function that reads the clock, randomness, the network or undeclared files. |

A task that mounts a secret is never cached. See [Cache](/docs/build/cache/).

## When a task fails

- A non-zero exit fails the attempt. Add `retry:` for network faults, because a failed fetch is an ordinary failure.
- An HTTP status is data. `http` exits `0` for any response it receives. Check `status` in the next task with a `condition:`:

```yaml
condition: '{{ eq .tasks.notify.output.status 200.0 }}'
```

## Limits

- `json` computes numbers as IEEE doubles. An integer above 2^53 loses precision. Keep such integers in a file, not in a template.
- `json` takes exactly one input form per `query`: `src`, `srcs`, `gather` or `nullInput: true`.
- `template` fails on a missing key. `default` does not rescue an absent key.
- `git` takes only `https://` URLs with no credentials in them.
- `json` and `template` refuse a task that mounts a secret at all.

## See also

- [Activity images and the `op` convention](https://github.com/orkstack/orkstack/blob/main/docs/ACTIVITY_IMAGES.md#first-party-reference-images): the shared conventions and the six images
- [Network and caching of the first-party images](https://github.com/orkstack/orkstack/blob/main/docs/ACTIVITY_IMAGES.md#network-and-caching-of-the-first-party-images): the full caching rules
- [First-party activities reference](/docs/reference/first-party-activities/): every key, default and output
- [Pass files](/docs/build/pass-files/) and [Conditions](/docs/build/conditions/): moving files and data between tasks
