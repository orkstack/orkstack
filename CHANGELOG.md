# Changelog

Every change that may affect the people who run or use OrkStack is recorded
here. The format follows
[Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/), and versions
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Each
breaking change is marked **BREAKING**.

Such a change lands with its entry under `[Unreleased]`, saying what they must
do about it, if anything; a change that cannot affect them, such as a
documentation or test change, has none. At a release that section becomes the
version's own, and its text is the release's notes on GitHub, word for word.

## [Unreleased]

### Added

- Runs created per day or per hour, on the viewer's own calendar. `GetStats`
  takes `bucket_width` (`HOUR` or `DAY`), an IANA `timezone` and
  `bucket_count`, and adds `buckets` to the window it already answers: each
  bucket counts the runs created in it, by status, from the same read as the
  window's totals, so the finished counts add up to the window's exactly.
  `orkctl stats --by day` prints the last 7 days in this machine's zone, today
  included (`--by hour` the last 24 hours, `--buckets N`, `--timezone`); a day
  is 23 or 25 hours when the zone's clocks change in it.
- An Orker that declares no capacity says at boot that its admission is best
  effort, bounded only by `max_concurrent`
  (`capacity not declared: admission is best effort, bounded only by max_concurrent`).
- `docs/DEPLOY.md`, "Every Orker of a pool has the same settings": the settings
  every Orker of a pool shares, and the order in which to change one with runs
  in flight — widen every Orker before any task needs more, narrow only once no
  run does.
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
- The image policy verifies cosign 3's default signature format, a Sigstore bundle stored as an OCI referrer of the image, beside the legacy `sha256-<hex>.sig` layout that images signed before keep, v0.1.0's among them. Sign with cosign 2 or 3; cosign 3 keeps the signature out of the public transparency log under a signing config that names no service ([DEPLOY.md, "Image supply-chain policy"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#image-supply-chain-policy)).
- Each release's page ends with every image's digest and the `cosign verify` and `sha256sum` commands that check the images and the checksums.
- Each release attaches `SHA256SUMS.sigstore.json`, cosign's bundle of the checksum signature, beside `SHA256SUMS.sig`, which openssl still checks ([UPGRADING.md, "Release artifacts"](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#release-artifacts)).
- `make proto-breaking`, run by CI on every push and pull request, checks the proto contract against v0.1.0 with `buf breaking`.
- Each proof part that starts its own Temporal, and a local `make proof`,
  keeps that Temporal server's log as `temporal-server.log.gz` (its last
  16 MiB, compressed), so an error a test read from Temporal can be traced.
- `docs/CI.md`: every CI workflow, its trigger, whether a merge waits for
  it, how long it takes and what to do when it is red, with the site page
  "Read a red CI run".
- A task waiting on a pool whose every Orker is at its declared capacity now
  says so: the run page captions it `Waiting for capacity`, and
  `orkctl run inspect` prints `waiting for capacity … every orker of pool
  <pool> is full (reserved 4/4 cpu)`, both read from the Orker inventory
  (`InspectRun`'s new `LiveAttempt.capacity_wait`). Any other waiting task
  still reads `Waiting for an orker`.
- Capacity metrics on an Orker that declares a capacity, when metrics are on:
  `ork.orker.capacity.cpu` and `.memory`, `ork.orker.capacity.reserved.cpu`
  and `.memory`, `ork.orker.admission.wait.duration` and
  `ork.orker.admission.refusals` (docs/DEPLOY.md, "Declaring an Orker's
  capacity").
- The nightly workflow also audits the UI's and the site's JavaScript
  dependencies (`make vuln-js`, `bun audit`) and scans the images a release
  publishes (`make vuln-images`, a pinned Grype) for known vulnerabilities; a
  release is not tagged while either is red.
- A canceled run shows who canceled it: the name your gateway asserted in
  `Ork-Principal` on the cancel request (`ORK_PRINCIPAL` for `orkctl`),
  marked as asserted and never authenticated. The API returns it as the
  run's `canceled_by`, `orkctl run get` prints `Canceled by:` and
  `orkctl run list` shows `CANCELED by … (asserted)`. It stays after the run
  is retired. A cancel without the header, and runs canceled before this
  release, show no name.
- Activity images can carry an icon and descriptions of themselves, of each
  op and of each `with:` and `values.json` key. Add `icon:` and
  `description:` to an `interface.ork.yaml`; `orkctl activity build
  --interface` packs them into the image (`orkctl activity interface pack
  --label icon|docs` prints them for other build tools). `orkctl activity
  inspect` shows them, and `ListActivities` and `InspectActivity` return
  them for clients to draw and explain. All six first-party images now
  carry both.

  Compatibility: the icon and descriptions travel in two new image labels,
  `org.orkstack.activity.icon` and `org.orkstack.activity.docs`; the
  interface label is unchanged, so an engine of an earlier release still
  reads an image's whole interface. An `orkctl` of an earlier release
  refuses an `interface.ork.yaml` that uses `icon:` or `description:`: build
  such images with this release's `orkctl`. Against an engine of an earlier
  release, which reads no icon, `orkctl activity inspect` says the image
  declares none.
- `InspectActivity` can answer a reference pinned by digest from what that
  engine already read for it, when asked (`allow_cached`), so a client
  showing many tasks' images costs no registry read each time.

### Changed

- The documentation site moved to ten sections, from Get started to Concepts,
  so its pages have new addresses. An old address redirects to the page's new
  one, so there is nothing to do.
- The web UI has a new look, Harbour: a light sidebar with the namespace
  switch, Runs, Orkflows, Apply and the Operate pages; Inter and JetBrains
  Mono; and the OrkStack mark. Statuses read Running, Succeeded, Failed and
  Canceled, each with its own mark as well as its colour. The Runs page opens
  with the last day's counts, which filter the list, and the last 7 days'
  outcomes, durations, most failing Orkflows, failures by reason and flakiest
  tasks. A run page shows its progress, who started it and, for a cancel, who
  asked (always marked asserted, never verified). Updates can be paused and
  resumed without losing any. Command lines sit behind the terminal icon on
  each page. Destructive actions (Cancel run…, Cordon…) ask for a reason in a
  dialog, and a cordon needs one. The Orkflow editor keeps its problems in the
  editor card and shows the docs of the activity under the caret beside the
  code. Nothing to do: links and the API are unchanged.
- **BREAKING:** An Orker's secret floor never admits the Orker's own sources,
  whatever `ORK_ORKER_SECRET_ALLOW` says, empty or not: its OrkStack and
  telemetry settings (`env:ORK_*`, `env:OTEL_*`), its proxy settings, the
  container CLI's settings (`env:DOCKER_*`, `env:CONTAINER_HOST` and the like)
  and its registry credential helpers' (`env:AWS_*`, `env:AZURE_*`,
  `env:CLOUDSDK_*`, `env:GOOGLE_APPLICATION_CREDENTIALS`), `/proc`, `/dev`,
  its container's credentials under `/run/secrets` and `/var/run/secrets`, its
  work root, its configuration file and each file that reads, its telemetry
  sinks' header sources, the container engine's state (the defaults, and
  wherever the engine reports keeping it at boot; an engine that cannot say
  leaves the defaults and logs a warning), and the credential files its
  settings name. A `file:` source is the Orker's own when the way
  the kernel takes to open it passes through or ends at one of these, by name
  or as the file is when the source is judged, so a link, a hard link to an
  own file and a Kubernetes mount updated after boot reach none of them. The
  work root is the temp directory when
  `ORK_ORKER_WORKDIR` is unset, so there a `file:/tmp/…` source is refused and
  a floor entry under `/tmp` stops the Orker at boot. A task that names one
  fails `SECRET_NOT_PERMITTED` where the floor is checked, after the commit
  check and a cached task's memo step and before its image is pulled, under
  `orkctl task run` as on an Orker; give the task the value under a source of
  its own. A floor entry that can match only such sources stops the Orker at
  boot, and an Orker whose floor is empty logs a
  warning at boot that names `ORK_ORKER_SECRET_ALLOW`. The refusal says only
  that the source is one of the Orker's variables or one of its files, never
  which ([SECURITY_MODEL.md](docs/SECURITY_MODEL.md), "The Orker's own
  sources").
- A secret refusal, the namespace's, the Orker's and `orkctl task run`'s
  alike, quotes a source, a scheme, a variable or a secret name longer than
  4101 bytes by its first 64 bytes and its length.
- **BREAKING:** A `file:` secret source must be a plain absolute path, with
  no `..`, `.` or empty segment, of at most 4096 bytes, under every secret
  floor, the empty one included. Any other name fails `SECRET_NOT_PERMITTED`,
  as a non-empty floor already refused it, except a name longer than 4096
  bytes, which no floor checked before and the kernel never opens.
- `scripts/k8s-orker-check.sh` runs a task's secrets through the Kubernetes
  bases' floor on a live cluster: a key of `orkstack-task-secrets` is read, and
  `ORK_S3_SECRET_KEY` and a variable the floor does not list are refused. It
  makes the `orkstack-task-secrets` Secret in its own namespace, and leaves a
  fifth Orkflow, `k8s-orker-secrets`.
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

  See DEPLOY.md, "The web UI" and "The API listener's bounds".
- **BREAKING**: the admin listener (`ORK_ADMIN_ADDR`, on `ork` and `orker`)
  applies the same rule, so a scraper that reads `/metrics` or a profile by a
  name with a dot is refused `403` until that name is listed in
  `ORK_ADMIN_ALLOWED_HOSTS` (`telemetry.admin_allowed_hosts`): a Prometheus
  static target such as `host.docker.internal` (the observability profile's,
  which `make up` and `deploy/orkstack.example.yaml` now list), or a
  Kubernetes Service FQDN. A scraper that dials an IP address needs nothing.
  See DEPLOY.md, "Telemetry".
- The web UI's page is sent under a strict content security policy. The UI's
  build writes it, and the engine sends it as a header. Scripts run only
  from the page's own origin, and inline ones only when the policy names
  their hash. No plugin and no `<base>` is allowed. `eval` is allowed only
  because the YAML editor's schema checks need it. Every other UI response
  forbids everything.
- **BREAKING** The release signs its images and `SHA256SUMS` in cosign 3's bundle format, with none of the flags cosign 3.1 deprecates, and still records nothing in a transparency log. An engine or Orker of v0.1.0 reads only the legacy layout, and v0.1.0's catalog follows the `0.1` tag to this release's activity images, so a v0.1.0 deployment whose namespace requires `cosign:orkstack` refuses them with `IMAGE_NOT_SIGNED`. Upgrade every engine and Orker, or until then name v0.1.0's activity images by digest in `ORK_ACTIVITY_CATALOG`. During the upgrade, a v0.1.0 Orker still polling refuses the bundle-signed images of a run whose namespace requires a signer too, as a contract violation that is never retried ([UPGRADING.md, "Release artifacts"](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#release-artifacts)).
- A release is refused before anything is built when its page, footer included, is over GitHub's limit of 125,000 characters for a release body, instead of failing at its last step after the tag and the images are out.
- The full proof cancels its run when a gate of its `fast` part fails, so a
  lint, build or unit failure no longer leaves the shards running for a
  proof that cannot pass. A setup failure, or a quarantined failure, cancels
  nothing, and a green run takes no longer.
- `proof-rerun.yml` also runs a failed nightly of `main` again when its only
  failures are parts whose runner was lost or measured below the floor, or
  that failed in a setup step.
- An unknown key in an Orkflow or an activity interface is refused in the
  document's own words: the key, where it stands, the document's kind and the
  keys that position takes, at the key's field path
  (`line 10: unknown key "need" in tasks[1] of the Orkflow; allowed: …`),
  followed by how many other keys are unknown, the first ten named with their
  lines. It used to repeat the YAML decoder's sentence, which named an
  internal type.
- `orkctl activity test` reports the `with:` interface the image declares in
  its `org.orkstack.activity.interface` label (declared, none or malformed),
  in its table and as `interface` in `--json`. The label is reported, never
  asserted, so it never changes the verdict.
- A task's `retry:` that sets no `maximum_interval_seconds` now waits at most
  60 seconds between attempts, instead of 100: the maximum interval
  OrkStack's engineering contract suggests (`maxInterval: 1m`, AGENTS.md
  §4). The number is decided when a run is submitted and kept for the whole
  run, so runs submitted before the upgrade keep 100 seconds. Set
  `maximum_interval_seconds: 100` to keep the old spacing.
- Fair dispatch across namespaces now also keys each pin's request for a pin
  host, so where the cluster has fair dispatch enabled, pin requests are
  ordered by namespace as tasks are, rather than first in, first out. It
  applies to runs submitted from this release on. Fan-out items,
  pinned tasks, runs that carry on and composed child runs already carried
  the key; this is now stated and tested.
- Every Go dependency is on its latest release: the Temporal Go SDK 1.50.0
  (API 1.64.0), Connect 1.21.1, gRPC 1.85.0, OpenTelemetry Go 1.47.0 (its logs
  API and SDK are now stable, 1.47.0) with otelhttp 0.72.0, the AWS SDK for Go
  v2 1.47.5 (S3 1.114.6) and the Prometheus client 1.25.0. The Temporal SDK's
  `temporal_activity_execution_failed` and
  `temporal_local_activity_execution_failed` metrics now carry a
  `failure_reason` attribute, `ActivityError`, or `PayloadsTooLarge` for an
  activity whose result was over the payload limit, and
  `temporal_sticky_cache_total_forced_eviction` no longer counts a worker's
  shutdown, so a dashboard built on them may need adjusting.
- Code generation is pinned exactly: buf 1.75.0, protobuf-es and
  protoc-gen-es 2.16.0, and connect-es 2.3.0 in `ui/package.json`. The Go
  protoc plugins are `go.mod` tool directives, so each generates with the
  version of the runtime it links against, and `make tools` installs only buf,
  at the release `deploy/buf-image.txt` pins, and refuses a `ui/package.json`
  that pins another.
- **BREAKING** for a rollback and for an unversioned fleet that mixes
  releases: the Temporal Go SDK 1.50 records a marker in a run's history the
  first time one cancellation reaches more than one of the run's open scopes
  (an operator's cancel of a run that declares a failure handler, or a run
  that fails or is canceled while two or more awaits or pins are open). An
  engine of 0.1.0 cannot replay such a run: it fails the run's workflow task
  on every attempt (`unknown SDK flag: 9`), and the run stays `RUNNING` until
  an engine of this release picks it up. With versioning on, roll back as
  before; with versioning off, roll back below this release only when no run
  is open, and replace every engine replica together when rolling forward
  (`docs/DEPLOY.md` §4, `docs/UPGRADING.md`).
- Connect 1.21 codes four failures differently. A connection reset under a
  call reaches a Go client as `canceled` when the client's read is the first
  to meet it; before, it was `unavailable` when the reset came before any
  response, `invalid_argument` when it cut a stream between messages, and
  `unknown` when it cut one inside a message. A connection that ends between
  messages of a stream is `internal` (it was `invalid_argument`); that is
  what the engine closing it gives, and what a reset gives when the client's
  own write meets it first (an HTTP/2 acknowledgement or flow-control
  update). Such an end is still `unavailable` before any response and
  `unknown` inside a message. A stream that ends cleanly partway through a
  message is `internal` (it was `invalid_argument`). And an engine's write to
  a client that has gone away is `canceled` (it was `unknown`).
  `orkctl run watch` and `orkctl run inspect --watch` resume on a `canceled`
  they did not cause as on `unavailable`, so a watch whose stream is reset
  partway through now resumes where it used to stop when the reset reaches
  it as `canceled`. A Ctrl-C still ends them, and so does a connection that
  ends partway through a stream (`internal` or `unknown`). Other `orkctl`
  commands print the new code for the same failure, with the same exit
  status. None of the four reaches an audit record, which is written after a
  request is read and before its response is; no OrkStack metric carries a
  Connect code; and the Temporal SDK's `failure_reason` attribute names only
  its own failure kinds, so none of them changes.
- `ork` and `orker` turn off the environment report in the Temporal SDK's
  worker heartbeats. From SDK 1.49 a worker's first heartbeat would otherwise
  also send the Temporal server the Go version, the hosting environment the
  SDK detects (a container, Kubernetes, a cloud platform) and the OS
  platform, which nothing in OrkStack asks for. The heartbeats are otherwise
  unchanged: they still carry what the SDK sends for its own worker
  accounting, among it the host name, the process id, the host's CPU and
  memory use, the SDK version, the task queue and the deployment version.
- The web UI is built with SvelteKit 3, Vite 8 and Svelte 5.57. An open UI
  tab now also asks the engine for the UI's build version
  (`/_app/version.json`) every hour and when the tab regains focus, as
  SvelteKit 3 does by default.
- The web UI and the site are type-checked with TypeScript 6.0.3. TypeScript
  7 waits until SvelteKit, svelte-check and `@astrojs/check`, which all
  accept only TypeScript 6, support it.
- **BREAKING** Every first-party activity image ships jq 1.8 (was 1.7) and BusyBox 1.37 (was 1.36), the `git` image git 2.54 (was 2.45), and the `http` image curl 8.23 (was 8.14). jq 1.8 changes what some filters compute, both a `json` task's filter and any `jq` a `shell` task's command runs: `indices`, `index` and `rindex` count code points rather than bytes; `tonumber` refuses leading or trailing white space; `ltrimstr` and `rtrimstr` fail on a non-string input or argument; `last(empty)` yields nothing; `limit` fails on a negative count; `pow10` is gone (use `exp10`); an `as` binding takes the whole expression before it, operators included (`[-1 as $x | 1, $x]` gives `[1,-1]`, was `[-1,-1]`); and a `#` comment that ends in a backslash continues on the next line. Check those filters and commands against jq 1.8 before you upgrade. A `template` task's `jq` is gomplate's own (gojq 0.12.19, was 0.12.17), where such a binding, a parse error before, now binds the whole expression too.
- **BREAKING** The `template` activity image renders with gomplate 5.3.1 (was 4.3.3). gomplate 5 removed most of the names gomplate 4 still served with a deprecation warning, so a template that calls one fails to render: use `strings.Contains`, `strings.HasPrefix`, `strings.HasSuffix`, `strings.Split`, `strings.SplitN` and `strings.Trim` for the bare `contains`, `hasPrefix`, `hasSuffix`, `split`, `splitN` and `trim`, with the input last (`{{ contains .s "x" }}` becomes `{{ strings.Contains "x" .s }}`); `conv.ToBool` for `bool` and `conv.Bool`; `coll.Sort` for `strings.Sort`; and `net.ParseAddr`, `net.ParsePrefix` and `net.ParseRange` for `net.ParseIP`, `net.ParseIPPrefix` and `net.ParseIPRange`. Bare `slice` does not fail but now slices (Go's builtin): build a list with `coll.Slice`. A `GOMPLATE_CONFIG` file that lists `templates` as a sequence no longer parses (gomplate 5 reads a map), and the image's `--missing-key error` now beats the file's `missingKey`. The `aws` functions run on AWS SDK v2 and no longer read `AWS_META_ENDPOINT`: set `AWS_EC2_METADATA_SERVICE_ENDPOINT` instead. The key generators and the RSA and AES functions no longer need `GOMPLATE_EXPERIMENTAL`, and the image's README lists which of them make a cached render wrong.
- **BREAKING** The Orker's boot probe runs `docker.io/library/alpine:3.24` by digest (was alpine 3.20). An Orker that cannot pull it refuses to start with `BIND_PROBE_IMAGE_UNAVAILABLE`: on an air-gapped or rate-limited host, mirror or load it before you upgrade (UPGRADING.md, "Upgrading the deployment").
- The development stack and the quickstart run Temporal's development server 1.9.1 (Temporal Server 1.32.0) and SeaweedFS 4.51.
- The development stack's observability profile runs OpenTelemetry Collector 0.164.0, Loki 3.7.9, Tempo 3.1.1, Prometheus 3.15.1 and Grafana 13.3.0.
- `CancelRun` answers `UNAVAILABLE` (or `RESOURCE_EXHAUSTED`) when the
  cluster could not take the cancellation, and `CANCELED` or
  `DEADLINE_EXCEEDED` when the caller gave up first, instead of `INTERNAL`.
  A client may retry the first two.
- The reason an image's interface could not be read (`interface_error`, in
  `orkctl activity inspect` and the API) now shows every control or format
  character it quotes as an escape, such as `\u001b`, and is cut at 512
  bytes, so an image cannot write to an operator's terminal through it.

### Fixed

- `GetStats` and `orkctl stats` no longer count a run twice — once as
  finished, once as running — in the moment after it ends, before the
  cluster's index of executions catches up with its close. A run the archive
  holds is counted as finished only.
- An Orker's own policy checks — its cpu and memory caps, secret schemes and
  `ORK_ORKER_SECRET_ALLOW` floor — now run after commit adoption and the memo
  step. A retry whose result another Orker of the pool already committed, or a
  cached task whose memo hits, no longer fails its run with
  `RESOURCE_REQUEST_REFUSED`, `SECRET_SCHEME_FORBIDDEN` or
  `SECRET_NOT_PERMITTED` when it lands on an Orker with narrower settings. A
  task with neither is still refused, before any secret is resolved or any
  image pulled. `RUN_AS_ORKER_UNAVAILABLE`, already raised after the commit
  check and the memo step, is now raised with them, before secrets are staged
  and the image is pulled.
- `orkctl task run --secret NAME=file:PATH` says why a file cannot be used, as
  the file system gave it, rather than calling every such file missing.
- `make reset` no longer fails on a workspace an Orker left read-only under
  `.data/work`, and takes the observability profile's containers and volumes
  down with the rest instead of reporting them as survivors.
- The Orker's boot probe no longer leaves `ork-bindprobe-*` or
  `ork-sigprobe-*` containers behind when its boot is stopped or refused
  between a container's create and its start. Both probe containers carry
  the Orker's `ork.orker` label, the Orker removes each itself however the
  probe ends, and one it could not remove (it was killed first) is reaped by
  the next boot under the same identity; until then
  `docker ps --all --filter label=ork.orker=<id>` lists it.
- The proof's automatic rerun and its job records read GitHub's job logs
  again: gh 2.103 refuses to print a log that carries terminal escape
  sequences unless it is given `--allow-escape-sequences`, so every log read
  came back empty. The job records now keep a bounded log of each failed job:
  every line the merger reads and its last 200 lines. A log that cannot be
  read or kept, other than one GitHub does not have, fails its job closed.
- A local proof's Playwright browser server is no longer stopped by another
  proof's start while its gate runs: it is labelled with the proof itself as
  its owner, not with the short-lived process that starts it, and no sweep
  stops a container whose proof's record says it still runs.
- A proof shard cancelled, or whose runner was lost, before its artifact was
  uploaded is read by the test it was running: the integration gate now also
  prints its `proofrun` progress lines to the job's log, where none reached
  before.
- A task whose `retry:` sets an `initial_interval_seconds` above the default
  maximum interval, and no `maximum_interval_seconds`, no longer leaves its run
  stuck: Temporal refused the retry policy and retried the refusal forever.
  An unset maximum interval is now never below the initial interval, in every
  run this release's engine executes, a run submitted before the upgrade
  included. On a deployment with versioning, a run v0.1.0 already left stuck
  stays on v0.1.0 and never gets the fix: cancel it, and terminate it if it
  stays running (UPGRADING.md, "Upgrading Orkflows with the release").
- **BREAKING**: An Orkflow whose `retry:` sets a negative
  `initial_interval_seconds` or `maximum_interval_seconds`, a
  `maximum_interval_seconds` below its `initial_interval_seconds`, or a
  `backoff_coefficient` below 1 or not finite, is now refused: validate
  reports an error at the field, and apply and plan refuse the document as
  an invalid argument, each naming the field and its value. Such a document
  applied and ran before. Temporal refused a maximum below the initial
  interval and a coefficient between 0 and 1, and retried the refusal
  forever, so every run that retried the task was stuck; the other values
  have no meaning as a wait. The refusal holds whatever `maximum_attempts`
  says, so a document that pairs such a policy with one attempt is refused
  too: fix the document before you next apply it. A version applied before
  this release keeps starting, and such a task is scheduled with the nearest
  policy Temporal accepts: the maximum raised to the initial interval, a
  coefficient between 0 and 1 raised to 1, and, in a run submitted from this
  release on, a coefficient that is not a number read as the default 2.
- A run whose definition, passed by reference, could not be used no longer
  serves a stream that silently begins at sequence 2 when object storage
  refused its `RUN_CREATED`. `RUN_CREATED` is now written again under the same
  number, so a store that is back for that write leaves both events, from 1.
  When it is refused again, the terminal event is still published at 2, and
  the missing 1 is said, not skipped: a read of the run's stream from its
  start now fails `DataLoss`, naming 1 as never published, live and from its
  archive, which records it as a gap line; a read from 1 serves the terminal
  event; and the run's evidence bundle is `schema_version` 2, with the gap in
  `event_gaps`, as every bundle of a gapped archive already was. A run a
  release before this left the same way, after one refused write, is read the
  same way (`docs/DEPLOY.md` §6, "An ending before start writes `RUN_CREATED`
  first").
- A gap in a run's events is named for its cause wherever it is reported —
  the `DataLoss` error, the evidence bundle's notice, and `orkctl run evidence
  verify`, whose rows are now labelled `event gaps` rather than `events lost`:
  events "lost from object storage before the run was archived", or "never
  published". The run page's socket (`/ws/runs/{id}`) now closes with that
  sentence, shortened to fit a close frame, instead of "event stream failed".
- `ExportRunEvidence` of a finished run that published no terminal event
  (its ending could not be written to object storage, or it was terminated)
  now says the run has no archive and never will, instead of asking to retry.
  It does not cover every run left without an archive: one that published its
  terminal event, but whose archive write then failed on every attempt, still
  gets "retry shortly" (`docs/RUN_EVIDENCE.md`, "The export, step by step").

### Security

- The development stack (`deploy/docker-compose.yml`, `make infra-up`)
  publishes every port on `127.0.0.1` only. Temporal's frontend, the object
  store with its development key, the unauthenticated registry and, in the
  observability profile, Grafana with anonymous admin were reachable from
  any host that could reach the contributor's machine. A contributor who
  reached the stack from another machine must now tunnel or forward the
  port, for example `ssh -L 7233:127.0.0.1:7233 <host>` for Temporal. The
  next `make infra-up` on a running stack recreates its containers to apply
  this: the object store and the registry keep their volumes, and Temporal's
  development server, which keeps its history in memory, starts empty.
- The first-party activity images move to Alpine 3.24; Alpine 3.20, their base until now, is past its support window.
- Every workflow and composite action pins every action by commit SHA, and
  Dependabot now also bumps the composite actions.
- The nightly image scan waives GHSA-mgcc-cww5-fp2q (go-jose/v4 v4.1.4, high,
  fixed in v4.1.6) in the template activity's gomplate alone: gomplate's
  latest release, v5.3.1, still carries v4.1.4, and gomplate's `main` already
  requires v4.1.6. The waiver is reviewed by 2026-11-30 and is removed when
  gomplate releases with go-jose v4.1.6 or later.

## [0.1.0] - 2026-10-30

First release.
