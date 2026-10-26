# Start runs over HTTP with a trigger

Create a trigger, then send one `POST` with its token to start a run. A CI job, a cron host or `curl` can do it. No client library is needed.

## Example

Create a trigger for the `hello-ork` Orkflow. The command prints the token once, with a ready-to-run `curl` command:

```bash
orkctl trigger create --orkflow hello-ork --name hello --description 'nightly kickoff'
```

Then send the request. Replace `trg-…` and `<token>` with the values the command printed:

```bash
curl -X POST http://localhost:8080/v1/triggers/trg-… \
  -H 'Authorization: Bearer <token>' \
  -H 'Content-Type: application/json' \
  -d '{}'
```

An empty object `{}` starts a run with no input, so the Orkflow's defaults apply. A `201` response holds the new run's `id`.

## Create a trigger

| Flag | Meaning |
|---|---|
| `--orkflow NAME` | The Orkflow the trigger starts. Required. |
| `--version V` | Pin one version. Without it, each call starts the current version. |
| `--name NAME` | A handle that an automation document uses to find the trigger. Unique in the namespace. |
| `--description TEXT` | A label. It shows on every run the trigger starts. |

The token is shown once. A listing never shows it. To rotate a token, delete the trigger and create a new one. The new trigger has a new id and URL.

`orkctl trigger update` renames a trigger, changes its pin or changes its label. It keeps the token.

## Send the request

| Header | Required | Meaning |
|---|---|---|
| `Authorization: Bearer <token>` | yes | The trigger's token. |
| `Idempotency-Key` | no | A key for the request. A repeated key returns the run it started, not a new one. The key is scoped to the namespace. |
| `Ork-Origin-Reference` | no | A reference recorded on the run, such as a CI job's URL. Up to 256 bytes. |
| `Ork-Principal` | no | A name recorded on the run. OrkStack records it as asserted. It does not check it. |

The body is the run's input, as one JSON object. For example, `{"count": 3}` sets the input `count` to `3`. The body can be at most 1 MiB.

## When a request fails

A failed request starts no run. Each status has a stable `error.code`.

| Status | `error.code` | What to do |
|---|---|---|
| `401` | `unauthenticated` | Send the `Authorization` header, with the right token. |
| `404` | `not_found` | The trigger was deleted, or its Orkflow or pinned version no longer exists. |
| `400` | `invalid_argument` | The body is not one JSON object, or it is over 1 MiB. Or `Ork-Origin-Reference` is invalid. |
| `412` | `failed_precondition` | The namespace is on hold or at its `max_running` limit, or no Orker serves the pool. Nothing started. |
| `503` | `unavailable` | Storage or the engine is unavailable. Retry the request. Any status other than `201` means no run started. |
| `500` | `internal` or `storage_error` | An unexpected error, or the trigger store could not be read. |

## Limits

- The trigger has no rate limit. Put your gateway in front of it.
- A trigger does not retry. It returns one result for one request.
- A trigger starts only the Orkflow it was created for. A request cannot change the Orkflow or the version.
- `Idempotency-Key` is scoped to the namespace, not to the trigger. Two triggers in one namespace that send the same key start one run.

## See also

- [Inbound Orkflow triggers](https://github.com/orkstack/orkstack/blob/main/docs/ORKFLOW_TRIGGERS.md#invoking-a-trigger): the full request and response rules
- [Trigger management and the CLI](https://github.com/orkstack/orkstack/blob/main/docs/ORKFLOW_TRIGGERS.md#cli): every `orkctl trigger` command
- [Declarative automation contract](https://github.com/orkstack/orkstack/blob/main/docs/AUTOMATION.md#triggers): naming triggers in a document
- [Schedules](/docs/how-to/schedules/) and [Webhooks](/docs/how-to/webhooks/): the other ways to start runs and to hear about them
