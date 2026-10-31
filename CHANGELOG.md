# Changelog

All notable changes to OrkStack are recorded here. The format follows
[Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/), and versions
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Each
breaking change is marked **BREAKING**.

A change lands with its entry under `[Unreleased]`. At a release that section
becomes the version's own, and its text is the release's notes on GitHub,
word for word.

## [Unreleased]

### Added

- `SECURITY.md`: report a vulnerability privately through the repository's
  "Report a vulnerability" form, never in a public issue.
- The API listener bounds what a client may make it hold. It allows 10 s to
  send a request's headers over HTTP/1.1 and 64 KiB of them (`431` above
  that), and closes a keep-alive connection idle for `ORK_HTTP_IDLE_TIMEOUT`
  (15 minutes). It refuses a Connect request message over
  `ORK_REQUEST_MAX_BYTES` with `resource_exhausted`, as sent and once
  decompressed, holding nothing past the cap, and reads a body no further
  than twice the cap before it closes the connection (or, over h2c, resets
  the stream). The cap follows the
  task ceiling: unset, it is 1 MiB plus 4 KiB a task of
  `engine.orkflow_max_tasks` (17,432,576 bytes at the default 4,000), so it
  never limits how large an Orkflow may be, and a value under that is
  refused at boot naming both settings. The admin listener has the same
  kinds of bound. No deadline cuts a run's event stream, a live log or a
  WebSocket, so a slow body or a slow reader is the proxy's to bound. See
  DEPLOY.md, "The API listener's bounds".
- `ORK_ALLOWED_HOSTS` (`engine.allowed_hosts`): names, `*.suffix` patterns
  of two labels or more, or `*`, that the API listener answers to in a
  request's `Host`, besides the names it always admits. A pattern over a
  top-level name (`*.com`) is refused at boot.
- `ORK_ADMIN_ALLOWED_HOSTS` (`telemetry.admin_allowed_hosts`): the same for
  the admin listener, on both binaries.

### Changed

- **BREAKING**: the API listener refuses with `403` a request whose `Host` it
  does not answer to, which stops DNS rebinding through a public name. With
  nothing set it admits an IP address, `localhost` and every name under it,
  a name without a dot (such as the quickstart's `ork`, or a Kubernetes
  Service from its own namespace), and the host `ORK_HTTP_ADDR` names. Set
  `ORK_ALLOWED_HOSTS` to every name with a dot the engine is reached by:
  - a Kubernetes name such as `ork.orkstack` or
    `ork.orkstack.svc.cluster.local`, or any FQDN;
  - the browser's name, behind a proxy that keeps the browser's `Host`;
  - the name `orkctl`, `curl` or a trigger caller dials.

  The admin listener (`ORK_ADMIN_ADDR`) applies the same rule: set
  `ORK_ADMIN_ALLOWED_HOSTS` to a name with a dot a scraper dials, such as a
  Prometheus static target (`host.docker.internal`). See DEPLOY.md, "The web
  UI" and "The API listener's bounds".
- The web UI's page is sent under a strict content security policy. The UI's
  build writes it, and the engine sends it as a header. Scripts run only
  from the page's own origin, and inline ones only when the policy names
  their hash. No plugin and no `<base>` is allowed. `eval` is allowed only
  because the YAML editor's schema checks need it. Every other UI response
  forbids everything.

## [0.1.0] - 2026-10-30

First release.
