# `http` plugin

Perform an HTTP request and optionally capture the response body as an artifact.

## Ops

`op` is the generic operation selector every OrkStack plugin shares
(see [docs/PLUGIN_DESIGN.md](../../../docs/PLUGIN_DESIGN.md)). For `http` it
names the HTTP verb.

| `op`     | verb     |
|----------|----------|
| `get`    | `GET`    |
| `post`   | `POST`   |
| `put`    | `PUT`    |
| `patch`  | `PATCH`  |
| `delete` | `DELETE` |
| `head`   | `HEAD`   |

- Default: `get`. Values are case-insensitive.
- An unknown `op` fails the task **permanently**, before the request is built,
  with a message quoting the value and listing the supported ops:
  `http: unknown op "frobnicate" (want get, post, put, patch, delete, head)`.
- `method` is the pre-`op` spelling and still works. It accepts any verb
  (`OPTIONS`, `TRACE`, custom), so it remains the escape hatch for verbs outside
  the enum. Setting `op` and `method` to *different* verbs is a permanent error;
  setting both to the same verb is accepted.

## `with`

| field             | applies to | description                                          |
|-------------------|------------|------------------------------------------------------|
| `op`              | all        | operation selector (above)                           |
| `method`          | all        | legacy spelling of `op`; any verb                    |
| `url`             | all        | request URL (**required**)                           |
| `headers`         | all        | map of header name → value                           |
| `body`            | all        | request body string                                  |
| `capture_body_as` | all        | artifact name for the exact response bytes           |
| `timeout_seconds` | all        | per-request timeout (number or numeric string)       |

## `output`

| field         | description                                                |
|---------------|------------------------------------------------------------|
| `status`      | HTTP status code. A non-2xx status does **not** fail the task |
| `headers`     | response headers, name → list of values                    |
| `json`        | parsed body, present only when the body is valid JSON      |
| `duration_ms` | wall-clock request duration                                |

The response body is streamed to a file, so `capture_body_as` yields the exact
bytes — nothing is normalized or truncated. Bodies never travel inline in the
output payload.

## Example

```yaml
- id: publish
  plugin: http
  with:
    op: post
    url: "https://httpbin.org/post"
    headers:
      Content-Type: application/json
    body: '{"message": "hello from orkstack"}'
    capture_body_as: response
```

Runnable: [`examples/http-post.ork.yaml`](../../../examples/http-post.ork.yaml).
