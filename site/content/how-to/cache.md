# Reuse an earlier result

Mark a task `cache:` to reuse the result of an identical earlier task. The container does not run again while the inputs stay the same.

## Example

In [`examples/config-from-git.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/config-from-git.ork.yaml), `tmpl` cuts one template out of a cloned repository. Its result is cached for a day:

```yaml
  - id: tmpl
    uses: file
    needs:
      - clone
    network: none
    cache:
      enabled: true
      ttl_seconds: 86400
    with:
      op: untar
      src: '{{ artifact "clone/src.tar.gz" }}'
      member: '{{ .inputs.template_path }}'
      as: app.conf.tmpl
```

`clone` is not cached. It mounts the Git token, and a revoked token must fail the next run.

The example needs a Git repository that the Orker can reach, and `GIT_TOKEN` in the Orker's environment. Run it twice with the same input:

```bash
orkctl apply examples/config-from-git.ork.yaml
orkctl run submit config-from-git --input repo=https://git.example.com/acme/app.git --input environment=prod --watch
orkctl run submit config-from-git --input repo=https://git.example.com/acme/app.git --input environment=prod --watch
```

If the commit has not moved, the cached tasks finish as `SUCCEEDED (cached)` in `orkctl run get`.

## Set the cache

| Key | Default | Meaning |
|---|---|---|
| `enabled` | required | `true` opts the task in. It is your statement that the task is safe to reuse. |
| `ttl_seconds` | the deployment's cap, 168 hours by default | How long a result may be reused, from when it was made. `0` means the cap. |
| `scope` | `orkflow` | `orkflow` reuses a result across runs of this Orkflow. `namespace` reuses it across the Orkflows of the namespace whose task has the same `id`, image, settings and inputs. |

OrkStack cannot check that a task is safe to reuse. A hit skips the container. A task with side effects outside OrkStack must not be cached.

## What makes a hit

The key covers the task's image, command, environment, secret names and sources, rendered settings, run input and staged input files. It never covers secret values.

It does not cover timeouts, retries, `needs:`, the condition, the pool or the run ID. Editing another task does not change this task's key.

- On a hit, no image is pulled, no secret is read and no container runs. The task shows `SUCCEEDED (cached)`.
- On a miss, the task runs. The attempt records the reason, such as `MEMO_ABSENT`. A miss is never a failure.
- The run input is part of the key. A new input misses every cached task. `orkctl run again` keeps the input, so it hits.

## Run without the cache

- `orkctl run submit --no-cache` runs every cached task and refreshes its stored result.
- `orkctl cache purge --orkflow <name>` deletes the stored results of one Orkflow. The next run runs the containers. No result changes.
- `orkctl cache stats` shows the hit rate of each cached task.

## Do not cache these

- A task with a `pin:` cannot be cached. Neither can an `await:`, a composed task (`uses: orkflow:…`) or a failure handler.
- A task that mounts a secret. A hit reads no secret. See [Pass secrets and environment variables](/docs/how-to/secrets-and-env/).
- A task with side effects outside OrkStack.

## Limits

- A stored result is reused only while it is younger than the task's `ttl_seconds`. It is never reused past the operator's cap, `ORK_CACHE_MAX_TTL`.
- A stored result is never shared across namespaces.
- Only `orkctl cache purge` and a bucket lifecycle rule delete stored results.

## See also

- [DSL reference, “Caching”](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#caching-cache): every rule
- [DSL reference, “What the key covers”](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#what-the-key-covers): the key, field by field
- [ORKFLOW_PATTERNS, “Cache only what you assert is reusable”](https://github.com/orkstack/orkstack/blob/main/docs/ORKFLOW_PATTERNS.md#cache-only-what-you-assert-is-reusable): when a cache is safe
