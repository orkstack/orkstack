# orkctl

<!-- Generated from the Cobra tree by `make cli-reference`; do not edit. -->

orkctl drives the OrkStack engine: apply orkflow DSL files, submit runs,
watch their events live, and inspect runs, orkflows, and activities.

Everything an orkflow or a run belongs to is scoped by a namespace.
--namespace picks one for a single command; `orkctl namespace use`
remembers one so it does not have to be typed again.

## `orkctl activity`

Inspect the activities this deployment can run

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl activity build`

Build an activity image and print the reference to pin

Build an OCI image from a Dockerfile context and push it, printing the
pinned reference to paste into `uses:`.

The push is what mints a real registry manifest digest. A locally built,
never-pushed image has none — a local image ID is not a manifest digest and
does not transfer to a second Orker — so `--push=false` produces an image you
can smoke-test but cannot pin.

This wraps the ordinary container toolchain and adds no privileged path: a
first-party image is built exactly the way any third-party image is.

Flags:

- `--file STRING` — path to the Dockerfile
- `--platform STRING` — target platform(s), e.g. linux/amd64,linux/arm64
- `--push` (default `true`) — push after building; --push=false builds only
- `--tag STRING` — the registry reference to build and push (required)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl activity inspect`

Resolve one activity and show the with: interface its image declares

Resolve one reference the way `uses:` would — a catalog name ("file") or a
registry reference ("ghcr.io/acme/migrate:v3") — and show the digest it pins
and the with: interface the image declares in its
org.orkstack.activity.interface label: which keys each op reads, their type,
whether each is required, and the keys the image writes to values.json.

An image without the label is an ordinary activity; its with: is checked by the
image itself at run time. This reads engine configuration and the registry,
never an Orker, and writes nothing.

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl activity list`

List the deployment's activity catalog

List the activities this deployment can run.

An activity is an OCI image. The catalog maps a short name — "shell" — to a
registry reference and the index digest it resolved to, so what a name will
actually run is visible before a run pins it.

This reads engine configuration and never asks an Orker: an Orker executes
images, it does not decide which ones exist. Use `orkctl pool list` to ask
whether a pool has Orkers to run them.

Filters (--filter FIELD OP VALUE, repeatable; every filter must hold):
  FIELD          OPERATORS       VALUES
  name           = != =a,b ~ ^   text
  reference      = != =a,b ~ ^   text
  index_digest   = != =a,b       text
Operators: = equals, != not equals, =a,b in, ~ contains, ^ prefix, >= at or after, < before.
A label is labels.KEY. \ escapes the next character; a comma inside a value is \,.

Flags:

- `--filter STRINGARRAY` — keep only rows where FIELD OP VALUE holds (repeatable; every filter must hold; --filter help lists the fields)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl activity test`

Check an image against the OCI Activity Contract

Run an image against the activity ABI, with no Orkflow and no Temporal.

The image is executed through the same dispatcher an Orker uses, so a pass
here means the same workspace construction, output scan, readability check
and commit protocol that production will apply. Nothing is reimplemented for
the test.

The image reference is used verbatim: this command does not resolve it
through the catalog, because the point is to test an image you just built.

Each probe is a command run inside the image through activity-shell's config
(with: op exec or eval, plus a command), so the image needs sh and jq and must
accept that config. An image with its own config contract (activity-http,
activity-file) is refused before any assertion, as "cannot be driven"; test
it with a fixture that speaks its config (test/fixtures/).

Requires a working container runtime. Exits non-zero if any assertion fails.

Flags:

- `--grace-period DURATION` (default `10s`) — SIGTERM-to-SIGKILL window used by the cancellation assertion
- `--keep` — keep the temporary workspaces for inspection
- `--timeout DURATION` (default `2m0s`) — per-assertion activity timeout

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)

## `orkctl admin`

Inspect the deployment: what it is made of, and what is serving it

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl admin audit`

List control-plane writes: who changed what, when, newest first

List the control-plane audit trail: one immutable record per write RPC
(namespace, orkflow, trigger, webhook, schedule, retention, image policy,
release, cache purge, run cancel, await resolve and Orker cordon changes),
succeeded or refused, read from the shared store so any replica answers.
Reads, run submissions and trigger invocations are never recorded.

Every namespace is listed unless a namespace filter narrows it; -n is refused
here, and ORK_NAMESPACE and the namespace saved by 'orkctl namespace use' are
ignored. Deployment-wide writes (retention, image policy, webhooks, releases,
Orker cordons) carry no namespace: --filter 'namespace=' selects them.

The window is at: --filter 'at<T' ends it (default now) and --filter 'at>=T'
starts it (default seven days before its end), both RFC 3339 instants with a
zone. A window with more records than the engine will scan for one page is
refused — narrow it.

PRINCIPAL is whatever the caller's gateway asserted in the Ork-Principal
header (ORK_PRINCIPAL makes this CLI assert one); OrkStack records it and
does not authenticate it, so a gateway that authenticates callers must
overwrite the header rather than pass it through.

Filters (--filter FIELD OP VALUE, repeatable; every filter must hold):
  FIELD        OPERATORS       VALUES
  at           >= <            RFC 3339 instant with a zone (2026-10-09T00:00:00Z)
  namespace    = != =a,b ~ ^   a name (a-z, 0-9, -; at most 200 bytes) or empty
  action       = != =a,b ~ ^   text
  principal    = != =a,b ~ ^   text
  target       = != =a,b ~ ^   text
  succeeded    =               true false
  error_code   = != =a,b       text
Operators: = equals, != not equals, =a,b in, ~ contains, ^ prefix, >= at or after, < before.
A label is labels.KEY. \ escapes the next character; a comma inside a value is \,.

Flags:

- `--filter STRINGARRAY` — keep only rows where FIELD OP VALUE holds (repeatable; every filter must hold; --filter help lists the fields)
- `--limit INT32` — records per page (0 = server default, 50; at most 200)
- `--page-token STRING` — page token from a previous listing with the same filters

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl admin image-policy`

Read or change the deployment's image supply-chain policy

image-policy reads or changes which images this deployment may apply and run:
whether a `uses:` may be a mutable tag, which registries images may come from,
and which cosign public keys a namespace may require by name. A namespace may
tighten this policy (`orkctl namespace update --require-signed-by
cosign:<key>`) and may never loosen it.

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl admin image-policy get`

Read the deployment's image policy

Read the deployment's image policy from the store, so any replica answers the
same. A deployment that never wrote one has the empty policy: every image is
permitted, as before the policy existed.

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl admin image-policy set`

Change the deployment's image policy

set reads the policy, applies the flags given, and writes the whole document
back. A flag left out keeps its current value.

--allowed-registry replaces the whole list (repeat it for several prefixes);
--clear-allowed-registries removes the restriction. A prefix is a literal
{registry}/{repository} prefix matched on "/" boundaries, e.g. ghcr.io/acme/.

--signing-key NAME=PATH adds or replaces a cosign PUBLIC key (cosign.pub; PATH
may be - for stdin). --remove-signing-key NAME removes one; the engine refuses
the removal while a namespace still names it.

A list that would exclude an activity of the engine's catalog is refused and
names the activities. Definitions already stored are re-checked at their next
submit; runs already accepted finish on the policy they were submitted under.

Flags:

- `--allowed-registry STRINGARRAY` — a {registry}/{repository} prefix images may come from, e.g. ghcr.io/acme/ (repeatable; replaces the whole list)
- `--clear-allowed-registries` — remove the registry restriction: every registry is permitted
- `--remove-signing-key STRINGARRAY` — remove the signing key NAME (repeatable; refused while a namespace names it)
- `--require-digest` — refuse a uses: written as a mutable tag (--require-digest=false clears it); catalog names are exempt
- `--signing-key STRINGARRAY` — add or replace a cosign public key as NAME=PATH (PATH of - reads stdin; repeatable)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl admin info`

Describe the engine replica that answers

Describe the engine replica that answers.

Every value except the server time is read from the replica's own build and
configuration, not observed from the cluster. Two replicas of one release
therefore answer identically; mid-rollout the build id is what differs, which
is the field to compare. Behind a load balancer this describes whichever
replica took the request.

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl admin retention`

Read or change the deployment's maximum run retention

retention reads or changes how long a finished run may be kept, at most, in
this deployment. Every namespace's own run retention (`orkctl namespace
update --run-retention`) is bounded by it, and a namespace that sets none
inherits it.

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl admin retention get`

Read the deployment's maximum run retention

Read the deployment's maximum run retention from the store, so any replica
answers the same.

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl admin retention set`

Change the deployment's maximum run retention

Change the deployment's maximum run retention.

<duration> uses Go's time.ParseDuration syntax ("168h", "24h30m", …), the same
flavour every ORK_*_TTL/ORK_*_INTERVAL environment variable in this deployment
uses. It must be positive: a zero or negative duration is refused before any
request leaves this process.

Lowering it below a namespace's own run retention is refused and names that
namespace — lower the namespace first (`orkctl namespace update
--run-retention`). Namespaces that set no run retention follow the new maximum
at once: finished runs already older than it are retired on the next cleanup
pass.

Lowering it reads the current maximum, states what the next cleanup pass
removes and asks for confirmation; it refuses when standard input is not a
terminal unless --yes is given. Raising it asks nothing.

Flags:

- `--yes` — do not ask for confirmation (required when standard input is not a terminal)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl apply`

Apply orkflow and automation documents

apply uploads orkflow DSL documents to the engine, which compiles,
validates, and stores them. Compilation errors are reported by the engine.

Each file's kind: decides what it is: kind: Orkflow is an orkflow document
and kind: Automation an automation document; both begin with apiVersion:
orkstack/v1, and a file with no kind is refused before anything is sent.

It also applies automation documents (kind: Automation): one YAML file per
namespace declaring its settings, schedules, triggers and webhooks by name
(docs/AUTOMATION.md). The engine plans; this command sends each planned
write to the existing RPC it names, in order. A directory is read
non-recursively for *.ork.yaml and *.ork.yml files; orkflows are applied
before automation, and nothing is written until every document has been
validated and planned.

An automation document names its own namespace, which is authoritative:
only an -n given on this command line is checked against it, and
ORK_NAMESPACE and the saved default never apply to it.

--dry-run prints the plan and writes nothing. --check is a dry run that
exits 4 when anything would change. Only --prune deletes, and only named
objects of the sections a document declares. --expect-plan refuses a plan
whose digest differs from the one reviewed. A trigger token or a generated
webhook secret is shown once; --secrets-out writes them to a new 0600 file
instead.

Flags:

- `--check` — a dry run that exits 4 when anything would change
- `--dry-run` — print the plan and write nothing
- `--expect-plan STRING` — refuse unless the plan digest equals this one (sha256:HEX)
- `--prune` — also delete named objects of a declared section the document no longer declares
- `--secrets-out STRING` — write disclosed one-time secrets to this NEW file (mode 0600) instead of stdout

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl cache`

Read and trim the memos cached tasks reuse

A cached task (cache: in an Orkflow) reuses the result of an identical earlier
run through a memo. Memos are advisory: purging them changes no result, only
whether the next run of a cached task runs its container.

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl cache purge`

Delete memos, so the next run of those cached tasks runs its container

purge deletes memos and nothing else: no run, commit or artifact is touched.
It changes no result — only whether the next run of a cached task runs its
container and writes a fresh memo. It never has a default scope.

It names the memos it will delete and asks for confirmation first, and
refuses when standard input is not a terminal unless --yes is given.

Flags:

- `--all` — purge every memo of the namespace
- `--namespace-scoped` — purge the namespace-scoped memos
- `--older-than STRING` — keep memos written within this duration, e.g. 72h
- `--orkflow STRING` — purge this orkflow's orkflow-scoped memos
- `--task STRING` — narrow --orkflow or --namespace-scoped to one task
- `--yes` — do not ask for confirmation (required when standard input is not a terminal)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl cache stats`

Hit rate per cached task over a window, and the memos stored now

Flags:

- `--orkflow STRING` — only this orkflow's runs and orkflow-scoped memos
- `--since STRING` — how far back to look: a duration such as 24h, or an RFC3339 instant (default: the last 24 hours)
- `--until STRING` — the window's exclusive end, an RFC3339 instant (default: now)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl export`

Print a namespace's automation as a document

export renders the automation document a namespace's current settings,
schedules, triggers and webhooks would be applied from (docs/AUTOMATION.md),
so a namespace managed by hand can start being reviewed in Git:

  orkctl export -n payments > automation.ork.yaml

An unnamed trigger or webhook is outside every document. The export lists
each one in a trailing comment with the command that names it in place —
keeping its token or secret — and repeats that list on stderr.

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl namespace`

Manage namespaces

A namespace scopes orkflow names, runs and pool defaults. Two namespaces
may each hold an orkflow called "deploy" and never see each other's.

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl namespace create`

Register a namespace

create registers a namespace. It is idempotent for an identical spec, so a
provisioning script can be re-run; a second create that differs is refused
and says which field differs.

Flags:

- `--allow-pool STRINGARRAY` — pool this namespace may dispatch onto (repeatable; empty = unrestricted)
- `--allow-secret STRINGARRAY` — secret source this namespace's tasks may name, as scheme:pattern, e.g. 'env:SANDBOX_*' (repeatable; empty = unrestricted; on update, replaces the whole set)
- `--budget-cpu-seconds INT64` — bound on the namespace's RESERVED cpu-seconds (cpu x attempt time) over --budget-period; 0 unbounds the dimension
- `--budget-enforce` — refuse new runs once the budget is reached (=false only reports it); check that 'orkctl stats usage --since <period>' answers before enforcing: an enforced budget that cannot be measured refuses every submit
- `--budget-gib-seconds INT64` — bound on the namespace's RESERVED GiB-seconds (memory x attempt time) over --budget-period; 0 unbounds the dimension
- `--budget-period STRING` — the rolling period a budget is held against, e.g. 720h (1h0m0s to 744h0m0s)
- `--default-pool STRING` — pool a task of this namespace runs on when it names none (default "main")
- `--description STRING` — human description
- `--hold STRING` — close this namespace to new runs, with the reason the next operator reads (runs already accepted finish untouched)
- `--label STRINGARRAY` — label as key=value (repeatable)
- `--max-running INT32` — most runs of this namespace in flight at once (0 = unlimited)
- `--require-signed-by STRING` — require every image executed in this namespace to carry a valid signature, as cosign:KEY naming a key of 'orkctl admin image-policy get' (tightens the deployment policy; never loosens it)
- `--run-retention STRING` — how long a finished run stays readable, e.g. "168h" (default: the deployment maximum; "inherit" clears a value)
- `--task-logs STRING` (default `inherit`) — override the Orker's task-log export for this namespace's runs: enabled, disabled, or inherit
- `--telemetry-attribute STRINGARRAY` — attribute stamped on every signal of this namespace's runs, as key=value (repeatable; on update, replaces the whole set)
- `--telemetry-sink STRING` — telemetry sink this namespace's runs use, by the name the deployment declares in ORK_TELEMETRY_SINKS; an unknown name is refused with the known list (default: the deployment's default sink)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl namespace delete`

Remove an empty namespace

delete removes a namespace that holds no orkflows. One that still holds any
is refused, and the refusal names them: removing the namespace would not
remove the definitions, and they would survive where nothing lists them.

It asks for confirmation first, and refuses when standard input is not a
terminal unless --yes is given.

Flags:

- `--yes` — do not ask for confirmation (required when standard input is not a terminal)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl namespace get`

Show one namespace

get shows one namespace. With no argument it shows the one this command
would act in — the --namespace flag, ORK_NAMESPACE, or the saved default.

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl namespace list`

List namespaces

list shows every namespace, ordered by name, narrowed by --filter — the same
filters Admin → Namespaces offers. Every match is listed; the browser's pages
are presentation.

Filters (--filter FIELD OP VALUE, repeatable; every filter must hold):
  FIELD               OPERATORS       VALUES
  name                = != =a,b ~ ^   a name (a-z, 0-9, -; at most 200 bytes) or empty
  description         = != =a,b ~ ^   text
  labels.KEY          = != =a,b ~ ^   text
  default_pool        = != =a,b ~ ^   a name (a-z, 0-9, -; at most 200 bytes) or empty
  hold.reason         = != =a,b ~ ^   text
  require_signed_by   = != =a,b ~ ^   text
  telemetry.sink      = != =a,b ~ ^   text
  created_at          >= <            RFC 3339 instant with a zone (2026-10-09T00:00:00Z)
  updated_at          >= <            RFC 3339 instant with a zone (2026-10-09T00:00:00Z)
Operators: = equals, != not equals, =a,b in, ~ contains, ^ prefix, >= at or after, < before.
A label is labels.KEY. \ escapes the next character; a comma inside a value is \,.

Flags:

- `--filter STRINGARRAY` — keep only rows where FIELD OP VALUE holds (repeatable; every filter must hold; --filter help lists the fields)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl namespace update`

Change a namespace

update reads the namespace, applies the flags given, and writes it back
compare-and-swapping on the version it read. A flag left out keeps its
current value; if somebody else changed the namespace in between, the
write is refused rather than silently overwriting theirs.

--hold REASON closes the namespace to new runs (runs already accepted
finish untouched); --release-hold reopens it.

--budget-cpu-seconds/--budget-gib-seconds/--budget-period bound the
namespace's RESERVED cpu and memory over a rolling period; --budget-enforce
refuses new runs once it is reached, --budget-enforce=false only reports it,
and --clear-budget removes it.

Flags:

- `--allow-pool STRINGARRAY` — pool this namespace may dispatch onto (repeatable; empty = unrestricted)
- `--allow-secret STRINGARRAY` — secret source this namespace's tasks may name, as scheme:pattern, e.g. 'env:SANDBOX_*' (repeatable; empty = unrestricted; on update, replaces the whole set)
- `--budget-cpu-seconds INT64` — bound on the namespace's RESERVED cpu-seconds (cpu x attempt time) over --budget-period; 0 unbounds the dimension
- `--budget-enforce` — refuse new runs once the budget is reached (=false only reports it); check that 'orkctl stats usage --since <period>' answers before enforcing: an enforced budget that cannot be measured refuses every submit
- `--budget-gib-seconds INT64` — bound on the namespace's RESERVED GiB-seconds (memory x attempt time) over --budget-period; 0 unbounds the dimension
- `--budget-period STRING` — the rolling period a budget is held against, e.g. 720h (1h0m0s to 744h0m0s)
- `--clear-allowed-secrets` — remove the namespace's secret-source list, leaving it unrestricted (each Orker's ORK_ORKER_SECRET_ALLOW still applies)
- `--clear-budget` — remove the namespace's budget, leaving it unbounded
- `--clear-require-signed-by` — stop requiring a signature: the deployment image policy alone applies
- `--default-pool STRING` — pool a task of this namespace runs on when it names none (default "main")
- `--description STRING` — human description
- `--hold STRING` — close this namespace to new runs, with the reason the next operator reads (runs already accepted finish untouched)
- `--label STRINGARRAY` — label as key=value (repeatable)
- `--max-running INT32` — most runs of this namespace in flight at once (0 = unlimited)
- `--release-hold` — reopen a held namespace to new runs (nothing refused while it was held is started)
- `--require-signed-by STRING` — require every image executed in this namespace to carry a valid signature, as cosign:KEY naming a key of 'orkctl admin image-policy get' (tightens the deployment policy; never loosens it)
- `--run-retention STRING` — how long a finished run stays readable, e.g. "168h" (default: the deployment maximum; "inherit" clears a value)
- `--task-logs STRING` (default `inherit`) — override the Orker's task-log export for this namespace's runs: enabled, disabled, or inherit
- `--telemetry-attribute STRINGARRAY` — attribute stamped on every signal of this namespace's runs, as key=value (repeatable; on update, replaces the whole set)
- `--telemetry-sink STRING` — telemetry sink this namespace's runs use, by the name the deployment declares in ORK_TELEMETRY_SINKS; an unknown name is refused with the known list (default: the deployment's default sink)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl namespace use`

Remember a namespace as the default for later commands

use saves a namespace in orkctl's config file so it does not have to be
typed on every command. --namespace and ORK_NAMESPACE still win over it.

The namespace is read from the engine first: saving a name that does not
exist would make every later command fail for a reason recorded in a file
the user has stopped thinking about.

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl orker`

List the deployment's Orkers, cordon one, or wait for one to drain

An Orker is one execution process. This command lists the deployment's
Orkers, cordons one so it takes no new task and opens no new pin, and waits
for one to drain. Orkers are deployment infrastructure, not namespace
objects: these commands ignore --namespace.

A cordon is a flag in the Orker process and nowhere else: it touches nothing
already running, and a restarted Orker comes back ready.

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl orker cordon`

Stop one Orker taking new tasks and opening new pins

Cordon one Orker: it takes no new task and opens no new pin, and everything
already running on it — containers and open pins — finishes where it is.
The reason is required; it is what every reader of the fleet sees beside
the state. The cordon is lost if the Orker restarts.

Flags:

- `--reason STRING` — why this Orker is cordoned (required)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl orker drain`

Cordon one Orker and wait until it runs no task and hosts no pin

Cordon one Orker, then ask it every few seconds how much work it still holds,
until it reports no running task and no open pin.

Exit 0 means the Orker measured zero: it is safe to stop the process.
Exit 4 means --timeout passed first; what is still running is printed, and
stopping the process then fails any open pin's run with PIN_HOST_LOST.
Exit 1 means the Orker stopped answering, or was uncordoned or restarted
(a restart clears the cordon, so it is taking work again).

Nothing server-side holds drain state: this is a loop in orkctl.

Flags:

- `--reason STRING` — cordon reason; default keeps an existing one, else "orkctl orker drain"
- `--timeout DURATION` (default `30m0s`) — how long to wait for the Orker to finish its tasks and pins (on this command, not the per-request timeout)
- `--watch` — print a line each time the running tasks or open pins change

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)

## `orkctl orker get`

Show one Orker: its state, load and open pins

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl orker list`

List every live Orker with its pool, load and cordon state

List every live Orker of the deployment, narrowed by --filter
(--filter pool=build keeps the Orkers that report serving build).

The list comes from the cluster (who is polling) and from each Orker's own
answer, so any replica answers the same. An Orker that could not be asked is
listed as `unknown (unreachable)` with the reason under the table: its load
is unknown, never zero, and a filter on pool, build_id or cordoned keeps it.
PINS is open/max for a pin host and - otherwise. CAPACITY is what an Orker
declaring one admits tasks within, and RESERVED what stands reserved against
it, pinned members included and each outstanding poll counted at the per-task
cap, so an idle Orker shows its pollers' share; both are - for an Orker that
declares none.

Filters (--filter FIELD OP VALUE, repeatable; every filter must hold):
  FIELD      OPERATORS       VALUES
  orker_id   = != =a,b ~ ^   an Orker id
  pool       = != =a,b ~ ^   a name (a-z, 0-9, -; at most 200 bytes) or empty; an unreachable row is kept
  build_id   = != =a,b ~ ^   text; an unreachable row is kept
  cordoned   =               true false; an unreachable row is kept
Operators: = equals, != not equals, =a,b in, ~ contains, ^ prefix, >= at or after, < before.
A label is labels.KEY. \ escapes the next character; a comma inside a value is \,.

Flags:

- `--filter STRINGARRAY` — keep only rows where FIELD OP VALUE holds (repeatable; every filter must hold; --filter help lists the fields)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl orker uncordon`

Let a cordoned Orker take work again

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl orkflow`

Inspect registered orkflows and their versions

An orkflow's versions are immutable: applying an edit adds a version and
never rewrites one. Which version runs is a pointer you move with
`promote`, and move back the same way.

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl orkflow callers`

List what in this namespace starts or composes an orkflow, and the version it pins

callers lists every composing orkflow task, trigger and schedule in the
namespace that references the orkflow, with the version each one pins, or
`current` when it follows the current version — which is what a promotion
moves. A composing orkflow always pins a version.

It is a scan made per request (there is no caller index): by default the
version each orkflow runs now; --all-versions reads every stored version.

Flags:

- `--all-versions` — scan every stored version of every orkflow, not only the version each one runs now

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl orkflow compat`

Show what a version changes in the declared interface (inputs and outputs)

compat classifies how the declared interface of one stored version — its
inputs: and its outputs: — changes in another. Every input and output gets a
finding marked compatible, BREAKING or unknown. Tasks, images, with:, pools
and output templates are implementation and never findings; `orkflow diff`
shows them.

With two arguments the comparison is from the current version. An empty
version ("") on either side also means the current one.

An orkflow with no outputs: declares no return contract, so its output side
is unknown, never compatible. compat is a read: it exits 0 whatever the
verdict; --json's verdict is what a CI job checks.

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl orkflow delete`

Delete an orkflow and all its versions

delete permanently removes an orkflow definition and all its stored versions.
This action will delete all versions and cannot be reversed.

It asks for confirmation first, and refuses when standard input is not a
terminal unless --yes is given. Deletion is still refused when active or
recent runs use the orkflow: --yes skips only the local confirmation prompt;
server run-safety checks still apply.

Flags:

- `--yes` — do not ask for confirmation (required when standard input is not a terminal); server run-safety checks still apply

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl orkflow diff`

Show what changed between two versions

diff renders the unified diff between two versions' source YAML. The diff
is computed by the engine, so every client shows the same answer.

An empty version ("") on either side means the current one, so
`orkctl orkflow diff deploy "" 2.0.0` reads as "what would promoting change".

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl orkflow edit`

Edit an orkflow in $EDITOR and apply the result

edit fetches a version's source YAML, opens it in $EDITOR, and applies
what you save as a NEW version — a stored version is never rewritten.
Saving without changing anything applies nothing.

--version picks the version to start from. ANY stored version is a valid
base, and editing an old one is how you fork forward from it: the result
becomes the newest applied version, and what runs does not change until
you promote it. Without --version the base is the newest applied version.

If somebody applies a version while your editor is open, the apply is
refused and names both versions so you can diff them — whichever version
you started from. Your edit is kept in a file the refusal names.

Flags:

- `--editor STRING` — editor command to open the document with (default: $ORK_EDITOR, $VISUAL, $EDITOR)
- `--version STRING` — version to edit; any stored version is a valid base (default: the newest applied)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl orkflow get`

Show one orkflow

Flags:

- `--version STRING` — orkflow version (default: the current one)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl orkflow list`

List registered orkflows

list shows the current version of every orkflow in the namespace, ordered by
name, narrowed by --filter. Filters apply before the page is cut; when more
orkflows match than one page holds, the command prints the next page's
command.

Filters (--filter FIELD OP VALUE, repeatable; every filter must hold):
  FIELD               OPERATORS       VALUES
  name                = != =a,b ~ ^   a name (a-z, 0-9, -; at most 200 bytes) or empty
  version             = != =a,b ~ ^   text
  description         = != =a,b ~ ^   text
  labels.KEY          = != =a,b ~ ^   text
  definition_digest   = != =a,b       text
  created_at          >= <            RFC 3339 instant with a zone (2026-10-09T00:00:00Z)
Operators: = equals, != not equals, =a,b in, ~ contains, ^ prefix, >= at or after, < before.
A label is labels.KEY. \ escapes the next character; a comma inside a value is \,.

Flags:

- `--filter STRINGARRAY` — keep only rows where FIELD OP VALUE holds (repeatable; every filter must hold; --filter help lists the fields)
- `--limit INT32` — maximum number of orkflows to return (0 = server default)
- `--page-token STRING` — page token from a previous list

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl orkflow promote`

Make one version the current one

promote points an orkflow at an existing version. Runs submitted after it
with no --version execute that version; runs already in flight finish on
the version they started with.

Rolling back is the same command with the older version.

Promoting compares the version's declared interface (its inputs: and
outputs:) with the version that runs now. A BREAKING change is refused,
naming each change, until you pass --acknowledge-breaking; compatible and
unknown changes are never refused. A rollback is gated alike: rolling back
across an added input removes it. See orkctl orkflow compat and callers.

Flags:

- `--acknowledge-breaking` — promote even when the version breaks the declared interface of the one that runs now; see orkctl orkflow compat

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl orkflow validate`

Validate an orkflow without storing it

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl orkflow versions`

List an orkflow's versions, newest first

versions lists every stored version of one orkflow, newest applied first.
CURRENT marks the version a run with no --version executes: the promoted
one, or the most recently applied when nothing was ever promoted.

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl pool`

Inspect the pools a namespace may dispatch onto

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl pool list`

List the pools in scope, with the Orkers serving each

List the pools this namespace may dispatch onto.

A pool is a set of Orkers. The counts come from the cluster — who is polling
for the pool's work right now — so any replica answers the same. A pool with
no live Orkers is listed, and says so: a task sent there waits for a worker
that is not running.

This read NARROWS and never refuses. A pool the cluster could not answer for
is still listed, with `unknown (unreachable)` where its count would be and the
reason printed under the table — one unreachable pool must not hide the
healthy ones. `unknown` and `0` are different answers: 0 means start an Orker,
unknown means ask again.

Which activities a pool can run is engine configuration, not something an
Orker reports: use `orkctl activity list` for the catalog; this command
answers who is available to run it.

LAST SEEN is how fresh the count beside it is; a pool whose Orkers have all
gone quiet shows no time at all rather than the moment the last one was
heard.

Filters (--filter FIELD OP VALUE, repeatable; every filter must hold):
  FIELD        OPERATORS       VALUES
  name         = != =a,b ~ ^   a name (a-z, 0-9, -; at most 200 bytes) or empty
  is_default   =               true false
Operators: = equals, != not equals, =a,b in, ~ contains, ^ prefix, >= at or after, < before.
A label is labels.KEY. \ escapes the next character; a comma inside a value is \,.

Flags:

- `--filter STRINGARRAY` — keep only rows where FIELD OP VALUE holds (repeatable; every filter must hold; --filter help lists the fields)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl release`

Show which release takes new runs, change it, and wait for one to drain

A release is one build of OrkStack: ork and orker built and deployed together,
named by ORK_BUILD_ID. These commands show which release takes new runs,
change it, and wait for the release before it to finish its runs. Releases
are deployment-wide, not namespace objects: these commands ignore --namespace.

ramp and promote are refused, with nothing changed, when the release could not
carry the runs it would take: no engine of it is running, a pool in use has no
uncordoned Orker of it, a pool that hosts pins has no pin host of it, or runs
started before any release was current are still executing.

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl release promote`

Make one release take every new run

Make one release take every new run. Runs already executing on the release before it
finish there and are never moved. Refused, with nothing changed, when the release
could not carry the runs it would take; the refusal names every reason and what to
start. There is no override: the cluster's own commands (docs/DEPLOY.md) remain.

It names the release that is current now and asks for confirmation before the
change, and refuses when standard input is not a terminal unless --yes is given.

Flags:

- `--yes` — do not ask for confirmation (required when standard input is not a terminal)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl release ramp`

Send a share of new runs to a release that is not current

Send N% of new runs to one release that is not current, and the rest to the
current release. --percent 0 stops the ramp of the release that is ramping.
A ramp above 0 is refused, with nothing changed, when the release could not
carry the runs it would take. Runs already executing are never moved.

Flags:

- `--percent INT` — share of new runs, 1–100; 0 stops the ramp (required)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl release rollback`

Return new runs to a release that was current before

Return new runs to a release that was current before: promote, aimed only at a
previous release. Runs executing on the release rolled back from finish there.

It names the release that is current now and asks for confirmation before the
change, and refuses when standard input is not a terminal unless --yes is given.

Flags:

- `--yes` — do not ask for confirmation (required when standard input is not a terminal)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl release status`

List every release, its role, its engines and Orkers, its runs, and what blocks it

List every release the cluster knows: its role (current, ramping N%, previous,
candidate), its engines and Orkers, the pools it serves, its runs in flight,
whether it is drained, and every reason a release that does not take new runs
may not start to. A count that could not be read is `unknown`, never 0.

--watch reprints the view every few seconds when it changes, until interrupted.
--filter selects release rows; every other line is the deployment's, and
blockers are still judged against the whole deployment.

Filters (--filter FIELD OP VALUE, repeatable; every filter must hold):
  FIELD      OPERATORS       VALUES
  build_id   = != =a,b ~ ^   text
  role       = != =a,b       CURRENT RAMPING PREVIOUS CANDIDATE
  drained    =               true false
Operators: = equals, != not equals, =a,b in, ~ contains, ^ prefix, >= at or after, < before.
A label is labels.KEY. \ escapes the next character; a comma inside a value is \,.

Flags:

- `--filter STRINGARRAY` — keep only rows where FIELD OP VALUE holds (repeatable; every filter must hold; --filter help lists the fields)
- `--watch` — reprint the view whenever it changes, until interrupted

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl release wait`

Wait until a release that takes no new runs is drained and may be stopped

Ask every few seconds until the release is drained: the cluster reports none of
its runs executing AND no Orker of it hosts an open pin. Each change in what still
holds it — its runs, and each open pin with the Orker it lives on — is printed.

Exit 0 means it is drained (or the cluster has already forgotten it): its engines
and Orkers may be stopped. Exit 4 means --timeout passed first; what remains is
printed. Exit 1 means it takes new runs, is unknown, or the engine stopped
answering. The cluster refreshes its drain verdict on a delay of minutes.

Nothing server-side holds wait state: this is a loop in orkctl.

Flags:

- `--timeout DURATION` (default `1h0m0s`) — how long to wait for the release to drain (on this command, not the per-request timeout)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)

## `orkctl run`

Submit and observe orkflow runs

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl run again`

Submit a new run from a prior run's pinned definition and inputs

Flags:

- `--allow-empty-pool` — submit even when the selected pool has no live Orkers
- `--dry-run` — print the exact new submission without starting it
- `--idempotency-key STRING` — fresh dedupe key for the new run
- `--input STRINGARRAY` — override source input as key=value (repeatable)
- `--input-json STRING` — JSON object merged into source inputs before --input
- `--no-cache` — ignore every memo for this run: each cached task runs its container and refreshes its memo
- `--origin-ref STRING` — correlation reference recorded on the run, e.g. a CI job URL or a ticket; at most 256 bytes
- `--version STRING` — override the source run's pinned orkflow version

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl run artifact`

Read one recorded artifact

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl run artifact download`

Download one recorded run artifact

Flags:

- `--item INT32` — the item of the fan-out --task names (required for a fan-out)
- `--kind STRING` — artifact kind: output, values, stdout, stderr
- `--name STRING` — exact artifact name returned by run artifacts
- `--output STRING` — destination file
- `--overwrite` — replace an existing destination file
- `--task STRING` — task that recorded the artifact

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl run artifact provenance`

Show whether a recorded artifact's bytes remain and which archived runs recorded the same object

provenance reads the object index the run archive keeps for cleanup: whether the
artifact's bytes are still in the object store, whether this run's reference is held,
released by cleanup, or absent, and every archived run of this namespace that recorded
the same object. Missing bytes are a finding, not an error: the command exits 0.

Flags:

- `--expect-sha256 STRING` — fail unless the recorded digest is this lowercase hex sha256
- `--item INT32` — the item of the fan-out --task names (required for a fan-out)
- `--kind STRING` — artifact kind: output, values, stdout, stderr
- `--name STRING` — exact artifact name returned by run artifacts
- `--task STRING` — task that recorded the artifact

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl run artifacts`

List artifacts recorded for a run

Flags:

- `--item INT32` — only this item of the fan-out --task names (0-based)
- `--task STRING` — only artifacts for this task

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl run attempts`

List every attempt of a run's tasks, including retried-away ones

attempts prints one row per try of each task of a run, oldest first.

A retry is invisible in `run get`: Temporal keeps only the final outcome
of a task, so the try that failed and was retried away is not in the run
snapshot. This reads the record the Orker filed for each attempt instead,
which is where the failed try's error, phase, exit code and Orker live.

An attempt whose Orker died before it could file one is still listed, as
UNREPORTED with dashes: the timeline never has a silent hole, and an
unreported attempt is an absent record rather than a failure.

With --task the full error of every failed attempt is printed under the
table, each with the command that prints that attempt's output.

Records are read from the artifact store, never from the engine's memory,
so any replica answers — including one that never started the run.

Flags:

- `--item INT32` — only this item of the fan-out --task names (0-based)
- `--task STRING` — only this task's attempts, with the full error of every failed one

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl run cancel`

Request cancellation of a run

cancel requests cancellation of a run of the namespace this command acts
in. A run of another namespace is not found here: cancelling is a write,
and holding an id is not authority over somebody else's tenant.

A failing run's failure handlers are stopped too; the run still ends
FAILED with its original reason.

Flags:

- `--reason STRING` — human readable cancellation reason

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl run diff`

Compare two runs field by field

diff compares two runs of one namespace: the pinned definition, the input,
and per task its status, attempt, pool, pin, orker, exit code, error,
duration, the image it was dispatched with and the platform manifest the
Orker ran. Only differences are listed.

A value that does not apply prints as -, and evidence that is gone (a
reclaimed attempt record, a deleted definition) prints as unavailable with
its reason under the table: the two never look alike.

Two runs of different orkflows are compared and flagged, never refused.
--only narrows the fields (and what the engine reads); a task only one run
has is always listed, whatever --only names. The comparison is computed by
the engine on every call and stored nowhere. Exit status is 0 whenever the
comparison succeeded, identical or not; scripts read --json.

Flags:

- `--only STRINGSLICE` — compare only these fields (comma-separated or repeated); presence is always compared

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl run evidence`

Export and verify a finished run's evidence bundle

A run's evidence bundle is one tar.gz holding everything the run recorded:
the archived run and events, the pinned definition's YAML, and per task
commit.json, values.json, every output artifact, every attempt record and
every attempt's stdout.log and stderr.log. Its first member, MANIFEST.json,
names each member's origin, size, sha256 and state. See docs/RUN_EVIDENCE.md.

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl run evidence export`

Write a finished run's evidence bundle to a file

Export assembles a finished run's evidence bundle on the engine and writes it
to --output. The file is committed only once the engine confirms a complete
bundle; an interrupted export leaves nothing behind.

--json writes no file: it prints MANIFEST.json itself — the exact document the
bundle would carry as its first member, not a proto envelope.

Flags:

- `--item INT32` — narrow a fan-out --task to one item
- `--no-artifacts` — leave out the tasks' output artifacts
- `--no-logs` — leave out every attempt's stdout.log and stderr.log
- `-o, --output STRING` — destination file (required unless --json)
- `--overwrite` — replace an existing destination file
- `--task STRING` — narrow the per-task members to one task

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl run evidence verify`

Check an evidence bundle's members against its MANIFEST.json

Verify hashes every member of a bundle and compares it with MANIFEST.json. It
needs no server, no credentials and writes nothing to disk. Reclaimed and
unrecorded members are findings, not failures; any member whose bytes do not
match is named and the command exits 1.

--against-store additionally asks the engine for the live manifest of the same
run and scope and compares the two member by member.

Flags:

- `--against-store` — also compare the bundle with the engine's live manifest of the run

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl run get`

Show one run

get shows one run of the namespace this command acts in. A run id
addresses a run inside a namespace, so a run of another one is not found
here even when its id is known.

The task table shows each task's FINAL outcome, which is all Temporal
keeps: a try that failed and was retried away is not in it. --attempts
adds the per-attempt timeline underneath, the same table
`orkctl run attempts` prints. --timing adds the wait/run/total table and
each attempt's phases, the same output `orkctl run timing` prints.

Flags:

- `--attempts` — also print every attempt of every task, including the ones that were retried away
- `--depth INT` — with --tree, stop descending below this level of child runs (default: the whole tree)
- `--expand STRINGARRAY` — print every item of this fan-out task (repeatable)
- `--timing` — also print each task's wait, run and total time and every attempt's phases
- `--tree` — also print every child run a composed task started, and theirs, depth first

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl run inspect`

Show what each in-flight task of a run is doing right now

inspect prints, for every task of a run, what its in-flight attempt is
doing now: its state, the phase the Orker reports, the container, the
bytes the workload has written to stdout and stderr, how long the
attempt has been running and how long ago its Orker last heartbeated.

The detail is the Orker's heartbeat, read from the cluster. A heartbeat
that is not there is reported as unavailable with its reason, never as
zero. A heartbeat older than two ceiling ticks is STALE: the Orker is gone
or partitioned, and the footer names the instant Temporal will time the
attempt out.

With --watch the table is printed again every --interval until the run is
terminal, and the exit code is the run's (0 succeeded, 2 failed,
3 canceled). --json prints the raw response; with --watch, one compact
response per line.

Flags:

- `--interval DURATION` (default `5s`) — poll period under --watch (at least 1s)
- `--item INT32` — only this item of the fan-out --task names
- `--task STRING` — only this task
- `--watch` — poll until the run is terminal

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl run list`

List runs, newest first

list shows runs of the namespace this command acts in, live and finished,
newest first, narrowed by --filter. When more runs match than one page
holds, the command prints the next page's command.

--filter origin.kind=KIND keeps runs by what started them (SUBMIT, TRIGGER,
SCHEDULE, PARENT_RUN or RUN_AGAIN), and --filter origin.id=ID beside it
narrows that to one originating object (a trigger or schedule id, a source
or parent run id). A run started before origins were recorded matches no
origin.kind filter.

Filters (--filter FIELD OP VALUE, repeatable; every filter must hold):
  FIELD          OPERATORS       VALUES
  orkflow_name   = != =a,b ~ ^   a name (a-z, 0-9, -; at most 200 bytes) or empty
  status         = != =a,b       PENDING RUNNING SUCCEEDED FAILED CANCELED SKIPPED
  created_at     >= <            RFC 3339 instant with a zone (2026-10-09T00:00:00Z)
  origin.kind    =               SUBMIT TRIGGER SCHEDULE PARENT_RUN RUN_AGAIN
  origin.id      =               text; needs origin.kind=
Operators: = equals, != not equals, =a,b in, ~ contains, ^ prefix, >= at or after, < before.
A label is labels.KEY. \ escapes the next character; a comma inside a value is \,.

Flags:

- `--filter STRINGARRAY` — keep only rows where FIELD OP VALUE holds (repeatable; every filter must hold; --filter help lists the fields)
- `--limit INT32` — maximum number of runs to return, at most 200 (0 = server default, 200)
- `--page-token STRING` — page token from a previous list

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl run logs`

Print a run's task output, and follow it live with -f

logs prints what a run's tasks wrote, in order.

With -f it follows the output as it is produced and returns when the run
reaches a terminal state. Without -f it prints what has been stored and
returns, even while the run is still going: a replay never turns into a
follow. Naming one task with --task prints that task's output verbatim;
without it every task of the run is printed, each line prefixed with the
task that wrote it.

Output is read from the artifact store, never from the engine's memory, so
any replica answers — including one that never started the run, one whose
Orker has exited, and a run that finished before this command was typed.

--from-seq resumes after a chunk sequence already seen, which is what a
reconnecting client hands back. A sequence is dense PER ATTEMPT and starts
again at 1 on a retry, so a cursor has to say which attempt it counted:
pin the try with --attempt, or name it with --from-attempt. A cursor with
no attempt is refused rather than applied to whichever try happens to be
latest, which would silently skip that try's first chunks.

The exit code reports whether the log could be read, not how the run
ended; `orkctl run watch` is the command whose exit code is the outcome.

A fan-out task (each:) writes one log per item: name it with --task and
--item N. Without --task a fan-out prints one line naming that command.

Flags:

- `--attempt INT32` — print this attempt of the task (0 = the latest attempt that produced output)
- `-f, --follow` — follow the output live, returning when the run reaches a terminal state
- `--from-attempt INT32` — the attempt --from-seq counts chunks of (0 = the attempt --attempt pins)
- `--from-seq INT64` — resume after this chunk sequence (0 = from the start of the task's output)
- `--item INT32` — print this item of the fan-out --task names (0-based)
- `--task STRING` — print only this task's output

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl run plan`

Show which tasks a run would execute, without starting anything

plan answers, for one input, which tasks of an orkflow would run, which
would be skipped and exactly why, and in which dispatch wave each would
start. The engine evaluates every condition with the interpreter's own
evaluator; nothing is started, stored, or recorded.

Plan a registered orkflow by name (--version pins one; the default is the
current one), or an unapplied document with --file.

A condition that reads an upstream task's output sees an empty output
unless one is supplied with --output TASK=JSON or --outputs-file PATH.

--assert-runs and --assert-skipped make the plan a CI check: a plan that
contradicts them exits 4, distinct from 1 for a plan that could not be
computed. Each takes ONE task per flag: --assert-runs rollback
--assert-runs notify checks two tasks, and --assert-runs rollback,notify
asks about one task named "rollback,notify". A plan proves wiring, never
behaviour, and is not an admission check: pools, holds, and limits are
asked by run submit.

--assume-failed TASK plans the run as if TASK, when it would run, failed
after its retries: its failure handlers are planned, every task that
depends on it and is not a failure handler is unreached, and independent
work is planned as if nothing had failed. Combine with --assert-runs to
check in CI that a handler is wired:

  orkctl run plan deploy --assume-failed deploy --assert-runs rollback --assert-runs notify

Flags:

- `--assert-runs STRINGARRAY` — exit 4 unless this task is planned to run (repeatable)
- `--assert-skipped STRINGARRAY` — exit 4 unless this task is planned to be skipped (repeatable)
- `--assume-failed STRINGARRAY` — plan as if this task, when it would run, failed after its retries (repeatable)
- `-f, --file STRING` — plan an unapplied orkflow document instead of a registered one
- `--input STRINGARRAY` — run input as key=value (repeatable); values that parse as JSON keep their JSON type
- `--input-json STRING` — run input as a JSON object; merged under --input values
- `--output STRINGARRAY` — assume a task's values.json as TASK=JSON-object (repeatable); wins over --outputs-file
- `--outputs-file STRING` — a JSON object keyed by task id whose values are the assumed values.json objects
- `--version STRING` — orkflow version (default: the current one)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl run resolve`

Complete or reject an awaited task of a running run

resolve delivers the outcome of an awaited task from outside its run.

A completion's values become the task's values.json, byte for byte:
--values-file sends the file's bytes verbatim (- reads stdin), and
neither --values nor --values-file means {}. --reject fails the task
with AWAIT_REJECTED and needs a --reason.

The resolution is stored and delivered to the run before this returns;
the run applies it next. Follow the run to see the outcome: a resolution
that loses a race is recorded on the run, never applied twice.

Flags:

- `--reason STRING` — why the await is rejected (with --reject)
- `--reject` — reject the await instead of completing it
- `--task STRING` — the awaited task to resolve (required)
- `--values STRING` — the completion's values as one JSON object (default {})
- `--values-file STRING` — read the completion's values from a file, verbatim (- for stdin)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl run submit`

Submit a run of a registered orkflow

submit starts a run of a registered orkflow.

Each --input value is parsed as JSON when it is valid JSON (so 3, true,
[1,2] and {"a":1} keep their types) and kept as a plain string otherwise.

Flags:

- `--allow-empty-pool` — submit even when the selected pool has no live Orkers
- `--idempotency-key STRING` — dedupe key for repeated submissions, scoped to this namespace
- `--input STRINGARRAY` — run input as key=value (repeatable); values that parse as JSON keep their JSON type
- `--input-json STRING` — run input as a JSON object; merged under --input values
- `--no-cache` — ignore every memo for this run: each cached task runs its container and refreshes its memo
- `--origin-ref STRING` — correlation reference recorded on the run, e.g. a CI job URL or a ticket; at most 256 bytes
- `--version STRING` — orkflow version (default: the current one)
- `--watch` — stream events until the run reaches a terminal state

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl run timing`

Show where each task's time went: waiting, running, and each phase

timing splits each task's time into WAIT (scheduling to the start of the
attempt that produced its outcome), RUN (that start to its finish) and
TOTAL, and lists the phases every attempt passed through.

A value that cannot be known — a host that died before reporting, a run
recorded before scheduled time existed, attempt records that were
reclaimed, two clocks that disagree — prints `unavailable`, never 0s. A
dash means the quantity does not apply: the task was never scheduled or
has not finished yet.

It reads the run snapshot and the per-attempt records; nothing is
computed on the server.

--json prints this command's own timing document (the table's data), not a
response envelope.

Flags:

- `--item INT32` — only this item of the fan-out --task names, with its attempts
- `--task STRING` — only this task, with its phases (a fan-out also lists its items)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl run watch`

Stream a run's events until it reaches a terminal state

watch consumes RunService.StreamEvents and prints each event as it arrives,
exiting when the run reaches a terminal state. The exit code reflects the
final status: 0 succeeded, 2 failed, 3 canceled.

The run is addressed inside the namespace this command acts in; a run of
another namespace is not found here.

Flags:

- `--from-sequence INT64` — resume after this event sequence (0 = from the start of the run)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl schedule`

Start runs of an orkflow on a schedule

A schedule starts a run of one orkflow at the times its cron
expressions or interval name, in its time zone. The cluster keeps the
schedule: overlap, catch-up, pause and backfill are its own, and
OrkStack stores nothing else about a schedule.

Every run a schedule starts carries origin schedule:<id>, so
'orkctl run list --filter origin.kind=SCHEDULE --filter origin.id=<id>'
is its audit trail.

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl schedule backfill`

Fire once for every time the schedule names in a past window

backfill fires once for every nominal time the schedule's spec names in
[--start, --end], as if that time passed now. Each run's origin reference
is its nominal time. Without --overlap the schedule's own overlap policy
applies. The window is at most 366 days.

Flags:

- `--end STRING` — last nominal time to fire for, RFC3339 (required)
- `--overlap STRING` — override the schedule's overlap policy for this request: skip|buffer-one|buffer-all|cancel-other|allow-all (default: none, the schedule's own policy applies)
- `--start STRING` — first nominal time to fire for, RFC3339 (required)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl schedule create`

Create a schedule that starts one orkflow

create binds a new schedule, named NAME in this namespace, to one orkflow.

--version pins a version; omitting it follows the orkflow's current
pointer, resolved at each fire. --overlap decides what happens when a
fire is due while the previous run still executes; the default is skip.
--paused creates it paused, so 'orkctl schedule get' shows the next fire
times before it ever fires.

Flags:

- `--catchup-window STRING` — how late a fire missed while the cluster was down may still be taken (at least 10s)
- `--cron STRINGARRAY` — cron expression (5-7 fields, @daily, "@every 15m"); repeatable, fires at the union
- `--description STRING` — operator label for this schedule
- `--end-at STRING` — no fire after this RFC3339 time
- `--input STRINGARRAY` — input value as key=value, submitted at every fire (repeatable)
- `--input-json STRING` — input as one JSON object, submitted at every fire
- `--interval STRING` — fire every DURATION, aligned to the Unix epoch (e.g. 1h, 15m)
- `--jitter STRING` — delay each fire by a random amount up to DURATION
- `--orkflow STRING` — orkflow every fire starts (required)
- `--overlap STRING` — when a fire is due while the previous run still executes: skip|buffer-one|buffer-all|cancel-other|allow-all (default skip)
- `--pause-on-failure` — pause the schedule when a fire fails or is refused
- `--paused` — create the schedule paused
- `--start-at STRING` — no fire before this RFC3339 time
- `--timezone STRING` — IANA time zone the cron expressions are evaluated in (default UTC)
- `--version STRING` — orkflow version to pin (default: the current one at each fire)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl schedule delete`

Delete a schedule

delete removes the schedule from the cluster. Runs it already started
continue; they keep their origin.

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl schedule fire`

Fire a schedule once, now

fire starts one fire now, as if its time had come, even while the
schedule is paused. Without --overlap the schedule's own overlap policy
applies. The run is not known when this returns: it appears in
'orkctl schedule get NAME' once prepared.

Flags:

- `--overlap STRING` — override the schedule's overlap policy for this request: skip|buffer-one|buffer-all|cancel-other|allow-all (default: none, the schedule's own policy applies)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl schedule get`

Show one schedule, its next fire times and recent fires

get reads one schedule from the cluster: its spec, state, next fire
times, and recent fires with the run each one started or why it
started none.

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl schedule list`

List the schedules of this namespace

list shows the schedules of the namespace this command acts in, each
described on the cluster for this call, narrowed by --filter: a filter on
id, name or orkflow_name is decided before a schedule is described. A row
the cluster could not describe reads 'unavailable', and is kept under a
filter on a field it could not report.

Filters (--filter FIELD OP VALUE, repeatable; every filter must hold):
  FIELD             OPERATORS       VALUES
  id                = != =a,b       text
  name              = != =a,b ~ ^   a name (a-z, 0-9, -; at most 200 bytes) or empty
  orkflow_name      = != =a,b ~ ^   a name (a-z, 0-9, -; at most 200 bytes) or empty
  orkflow_version   = != =a,b ~ ^   text; an unreachable row is kept
  description       = != =a,b ~ ^   text; an unreachable row is kept
  paused            =               true false; an unreachable row is kept
  created_at        >= <            RFC 3339 instant with a zone (2026-10-09T00:00:00Z); an unreachable row is kept
Operators: = equals, != not equals, =a,b in, ~ contains, ^ prefix, >= at or after, < before.
A label is labels.KEY. \ escapes the next character; a comma inside a value is \,.

Flags:

- `--filter STRINGARRAY` — keep only rows where FIELD OP VALUE holds (repeatable; every filter must hold; --filter help lists the fields)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl schedule pause`

Stop a schedule's fires without deleting anything

pause stops fires until 'orkctl schedule resume'. The reason is kept as
the schedule's note and shown on every read. 'schedule fire' still works
on a paused schedule.

Flags:

- `--reason STRING` — why the schedule is paused (at most 256 bytes)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl schedule resume`

Let a paused schedule fire again

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl schedule update`

Change a schedule's version pin, input, description or spec

update reads the schedule, replaces only the fields whose flags you set,
and writes the whole schedule back. --cron given replaces every cron
expression. --follow-current clears a version pin; --clear-input clears
the input. The orkflow a schedule starts cannot change: delete and
create instead.

Last writer wins: two concurrent updates are not detected.

Flags:

- `--catchup-window STRING` — how late a fire missed while the cluster was down may still be taken (at least 10s)
- `--clear-input` — clear the input
- `--cron STRINGARRAY` — cron expression (5-7 fields, @daily, "@every 15m"); repeatable, fires at the union
- `--description STRING` — replace the description
- `--end-at STRING` — no fire after this RFC3339 time
- `--follow-current` — clear the version pin: follow the current pointer at each fire
- `--input STRINGARRAY` — replace the input with key=value pairs (repeatable)
- `--input-json STRING` — replace the input with one JSON object
- `--interval STRING` — fire every DURATION, aligned to the Unix epoch (e.g. 1h, 15m)
- `--jitter STRING` — delay each fire by a random amount up to DURATION
- `--overlap STRING` — when a fire is due while the previous run still executes: skip|buffer-one|buffer-all|cancel-other|allow-all (default skip)
- `--pause-on-failure` — pause the schedule when a fire fails or is refused (--pause-on-failure=false turns it off)
- `--start-at STRING` — no fire before this RFC3339 time
- `--timezone STRING` — IANA time zone the cron expressions are evaluated in (default UTC)
- `--version STRING` — pin this orkflow version

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl stats`

Summarise a namespace's runs

stats reports how a namespace is doing over a time window: how many runs
are running and how the finished ones ended, the success rate, how long
runs take, which orkflows fail most, and which pools have Orkers.

Every number is about the runs the window CREATED, whenever they end, so
the percentiles are over the same runs the success rate is a rate over.
A run started inside the window and still going is counted as running and
has no duration yet.

Every number is derived when it is asked for, from the same records
`orkctl run list` and `orkctl namespace get` read, so any engine replica
gives the same answer. Nothing here is a stored counter.

Flags:

- `--since STRING` — how far back to look: a duration such as 6h, or an RFC3339 instant (default: the last 24 hours)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl stats failures`

Group a namespace's failed runs by what failed and why

failures groups the window's failed runs by fingerprint — which orkflow,
which task failed first, its code and failure category, and on request the
image the final attempt ran, the pool, the Orker and the phase it ended in —
worst first. The runs counted are the same ones `orkctl stats` counts as
failed for the same window.

Image and phase come from attempt records, which are reclaimed on the run's
retention schedule: a group whose evidence is gone says `unavailable`, never
a blank. A window with more failed runs than the server reads at once is
refused rather than answered from a subset; narrow it with --since, --until
or --orkflow.

Flags:

- `--category STRING` — only failures of one category: activity_failed, infrastructure_error, contract_violation, timed_out, cancelled
- `--group-by STRING` — comma-separated dimensions, in column order: orkflow, task, code, category, image, pool, phase, orker (default: orkflow,task,code,category)
- `--limit INT32` — groups to show, worst first (0: the server's default)
- `--orkflow STRING` — only this orkflow's failed runs
- `--since STRING` — how far back to look: a duration such as 6h, or an RFC3339 instant (default: the last 24 hours)
- `--until STRING` — the window's exclusive end, an RFC3339 instant (default: now); a duration --since is measured back from it

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl stats flakes`

Show which tasks needed more than one attempt, across a namespace's runs

flakes groups the window's dispatched tasks — of every finished run, succeeded,
failed or canceled — and reports how often each needed more than one attempt,
flakiest first. A task that failed twice and passed on the third attempt makes
a SUCCEEDED run: invisible to `orkctl stats` and `orkctl stats failures`, and
counted here. The runs counted are the ones `orkctl stats` counts as finished
for the same window.

The numbers come from the per-attempt records the Orker writes, which are
reclaimed on the run's retention schedule. A run whose records are gone is
EXCLUDED from every rate and counted, never treated as clean; a task with no
record anywhere in the window reads `unavailable`, never 0%. A window with more
finished runs than the server reads at once is refused rather than answered
from a subset; narrow it with --since, --until or --orkflow.

Flags:

- `--group-by STRING` — comma-separated dimensions, in column order: orkflow, task, image, pool, phase, orker (default: task; orkflow is always a column)
- `--limit INT32` — groups to show, flakiest first (0: the server's default)
- `--orkflow STRING` — only this orkflow's runs
- `--since STRING` — how far back to look: a duration such as 168h, or an RFC3339 instant (default: the last 24 hours)
- `--until STRING` — the window's exclusive end, an RFC3339 instant (default: now); a duration --since is measured back from it

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl stats usage`

Show how much cpu and memory a namespace RESERVED over a window

usage reports a namespace's reserved consumption over a window, from the
per-attempt records the Orker writes: task-seconds (how long Orkers held
its attempts), and cpu-seconds and GiB-seconds — each attempt's reserved
cpu and memory multiplied by that time. They are RESERVATIONS, not measured
utilization. The runs counted are the window's finished runs, the ones
`orkctl stats` counts as finished; a run still in flight is counted when it
finishes.

Records reclaimed on the run's retention schedule are reported as missing
evidence with a coverage boundary, never folded into a smaller number.
A window with more finished runs than the server reads at once is refused
rather than answered from a subset.

With a budget, `--since <its period>` asks the question the admission gate
asks, and prints the budget's share. --group-by namespace reports every
namespace side by side, one call each.

Flags:

- `--group-by STRING` — comma-separated dimensions, in column order: orkflow, task, pool; or namespace alone, which reports every namespace (default: the namespace total)
- `--since STRING` — how far back to look: a duration such as 720h, or an RFC3339 instant (default: the last 24 hours)
- `--until STRING` — the window's exclusive end, an RFC3339 instant (default: now); a duration --since is measured back from it

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl task`

Run one task of an orkflow on this machine

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl task run`

Run one task locally, against a run's recorded bytes or local files

run executes ONE task of an orkflow on this machine, through the same dispatcher,
renderer and workspace an Orker uses, and prints the rendered config.json before
the container starts.

With --from-run RUN, the task's upstream values and artifacts are the run's
committed bytes, read through the engine's integrity-checked artifact reads. With
--file, an edited, unapplied document is validated by the engine and run instead
of the stored definition. --output and --artifact supply an upstream task's
values.json and output files from local files.

A failure handler (join: any_failed or all_done) is told how its needs ended:
with --from-run, as the run recorded it; --need TASK=STATUS overrides one direct
need ("what would this handler do had smoke failed?"), and without --from-run
every direct need must be named. A failed need's files come from the run or
from --artifact TASK/NAME=PATH.

Nothing is written to the deployment: no run, no commit, no attempt record, no
log. A local run is a new logical activity with its own run id and idempotency
key, and any external side effect it performs is real. Every secret the task
declares must be supplied with --secret or skipped with --omit-secret.

Exit status: 0 succeeded (or rendered), 2 the task failed, 3 interrupted, 1 the
command could not run the task.

Flags:

- `--artifact STRINGARRAY` — supply one output artifact of an upstream task as TASK/NAME=PATH (repeatable)
- `-f, --file STRING` — run this unapplied orkflow document instead of the stored definition
- `--from-run STRING` — replay against this run's recorded upstream bytes
- `--input STRINGARRAY` — run input as key=value (repeatable); with --from-run, overrides the run's input
- `--input-json STRING` — run input as a JSON object; merged under --input values
- `--item INT32` — run this item of a fan-out task (required for one)
- `--keep` — keep the work directory and print its path
- `--need STRINGARRAY` — a failure handler's direct need and how it ended, TASK=succeeded|failed|skipped|canceled (repeatable); with --from-run it overrides the recorded outcome, without it every direct need must be named
- `--omit-secret STRINGARRAY` — run without this declared secret (repeatable)
- `--output STRINGARRAY` — supply an upstream task's values.json as TASK=JSON-object (repeatable); wins over --outputs-file
- `--outputs-file STRING` — a JSON object keyed by task id whose values are the supplied values.json objects
- `--render-only` — stop once the input tree is sealed; start no container
- `--secret STRINGARRAY` — supply a declared secret as NAME=file:PATH or NAME=env:VAR (repeatable)
- `--shared STRING` — seed a pinned task's /orkstack/shared from this directory
- `--task STRING` — the task to run (required)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl trigger`

Create and inspect inbound run starters

A trigger lets an external caller start a run over plain HTTP:
POST /v1/triggers/<id> with the trigger's bearer token.

It is the mirror image of a webhook. A webhook is outbound and
deployment-wide unless scoped; a trigger is inbound and bound to one
orkflow in one namespace. The orkflow a trigger starts comes from the
stored trigger and never from the request, so a token cannot be
redirected.

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl trigger create`

Create a trigger that starts one orkflow

create binds a new trigger to one orkflow in this namespace and prints
its bearer token once.

--version pins the trigger to that version forever; omitting it follows
the orkflow's current pointer, resolved at each invoke.

The token is shown here and never again: a listing cannot disclose it,
because only a salted digest of it is stored. Rotating a token is
delete then create.

--name gives the trigger a handle, unique in the namespace, by which an
automation document addresses it (docs/AUTOMATION.md).

Flags:

- `--description STRING` — operator label for this trigger
- `--name STRING` — handle an automation document addresses this trigger by (a slug, unique in the namespace)
- `--orkflow STRING` — orkflow this trigger starts (required)
- `--version STRING` — orkflow version to pin (default: the current one at invoke time)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl trigger delete`

Remove a trigger

delete removes a trigger of the namespace this command acts in. A
trigger of another namespace is not found here.

Its token authenticates nothing afterwards: a later invoke of that id
is 404 on every replica.

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl trigger list`

List the triggers of this namespace

list shows the triggers of the namespace this command acts in, narrowed by
--filter: --filter orkflow_name=NAME lists one orkflow's triggers.

A listing never contains a bearer token: the plaintext exists only in
the output of `trigger create`.

Filters (--filter FIELD OP VALUE, repeatable; every filter must hold):
  FIELD             OPERATORS       VALUES
  id                = != =a,b       text
  name              = != =a,b ~ ^   a name (a-z, 0-9, -; at most 200 bytes) or empty
  orkflow_name      = != =a,b ~ ^   a name (a-z, 0-9, -; at most 200 bytes) or empty
  orkflow_version   = != =a,b ~ ^   text
  description       = != =a,b ~ ^   text
  created_at        >= <            RFC 3339 instant with a zone (2026-10-09T00:00:00Z)
Operators: = equals, != not equals, =a,b in, ~ contains, ^ prefix, >= at or after, < before.
A label is labels.KEY. \ escapes the next character; a comma inside a value is \,.

Flags:

- `--filter STRINGARRAY` — keep only rows where FIELD OP VALUE holds (repeatable; every filter must hold; --filter help lists the fields)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl trigger update`

Rename, re-pin or relabel a trigger in place, keeping its token

update changes a trigger's name, pinned version and description in place.
Its id, URL, orkflow and bearer token never change: an update never
re-issues a token, so the caller holding it keeps working.

Only the flags given change anything: --name "" makes the trigger
unnamed, --follow-current clears the pin, --description "" clears the
label. A trigger's orkflow cannot change: that is delete then create.

Naming an unnamed trigger is how a hand-made one is brought under an
automation document (orkctl export) without re-issuing its token.

Flags:

- `--description STRING` — new operator label ("" clears it)
- `--follow-current` — clear the pin: follow the orkflow's current pointer at each invoke
- `--name STRING` — new handle ("" makes the trigger unnamed)
- `--version STRING` — orkflow version to pin

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl version`

Print the orkctl version

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl webhook`

Register and inspect run-event callbacks

A webhook receives a signed POST when a run or one of its tasks changes
state. Run transitions: RUN_STARTED, RUN_SUCCEEDED, RUN_FAILED,
RUN_CANCELED. Task events: TASK_SCHEDULED, TASK_STARTED, TASK_AWAITING,
TASK_RESOLVED, TASK_SUCCEEDED, TASK_FAILED.

Omitting --event means every run transition; task events are opt-in by
name. Task events are best-effort hints with a short retry budget: a
subscriber that must not miss one reconciles with GetRun.

A subscription is deployment-wide unless it names a scope: a namespace
(-n), an orkflow (--orkflow) and a task (--task), each an exact match.

What each delivery did is recorded; `deliveries` lists the records, `delivery`
shows one, `redeliver` sends one again by hand.

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl webhook delete`

Remove a registered webhook

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl webhook deliveries`

List what each webhook delivery did, newest first

List delivery records: one immutable record per settled delivery — every
attempt, its status and error, the signature sent and the SHA-256 of the
exact body bytes (the body itself is not stored). Records are read from the
shared store, so any replica answers. Nothing is queued: a record is a log.

Every namespace is listed unless a namespace filter narrows it; -n is refused
here, and ORK_NAMESPACE and the namespace saved by 'orkctl namespace use' are
ignored. Every filter is decided from a record's key, so a body is read only
for a record that is listed.

The window is created_at: --filter 'created_at<T' ends it (default now) and
--filter 'created_at>=T' starts it, both RFC 3339 instants with a zone. With
no start, one run_id= filter starts the window at that run's start; otherwise
it is the seven days before its end. A window with more keys than the engine
will scan for one page is refused — narrow it.

Filters (--filter FIELD OP VALUE, repeatable; every filter must hold):
  FIELD        OPERATORS       VALUES
  created_at   >= <            RFC 3339 instant with a zone (2026-10-09T00:00:00Z)
  namespace    = != =a,b ~ ^   a name (a-z, 0-9, -; at most 200 bytes) or empty
  run_id       = != =a,b       text
  webhook_id   = != =a,b       text
  event        = != =a,b       RUN_STARTED RUN_SUCCEEDED RUN_FAILED RUN_CANCELED TASK_SCHEDULED TASK_STARTED TASK_AWAITING TASK_RESOLVED TASK_SUCCEEDED TASK_FAILED
  delivered    =               true false
Operators: = equals, != not equals, =a,b in, ~ contains, ^ prefix, >= at or after, < before.
A label is labels.KEY. \ escapes the next character; a comma inside a value is \,.

Flags:

- `--filter STRINGARRAY` — keep only rows where FIELD OP VALUE holds (repeatable; every filter must hold; --filter help lists the fields)
- `--limit INT32` — records per page (0 = server default, 50; at most 200)
- `--page-token STRING` — page token from a previous listing with the same filters

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl webhook delivery`

Show one delivery record: every attempt, the signature and the body digest

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl webhook list`

List registered webhooks

list shows the deployment's webhook subscriptions, narrowed by --filter
(--filter namespace= keeps the deployment-wide ones).

Filters (--filter FIELD OP VALUE, repeatable; every filter must hold):
  FIELD          OPERATORS       VALUES
  id             = != =a,b       text
  name           = != =a,b ~ ^   a name (a-z, 0-9, -; at most 200 bytes) or empty
  url            = != =a,b ~ ^   text
  namespace      = != =a,b ~ ^   a name (a-z, 0-9, -; at most 200 bytes) or empty
  orkflow_name   = != =a,b ~ ^   a name (a-z, 0-9, -; at most 200 bytes) or empty
  task_id        = != =a,b ~ ^   a name (a-z, 0-9, -; at most 200 bytes) or empty
  created_at     >= <            RFC 3339 instant with a zone (2026-10-09T00:00:00Z)
Operators: = equals, != not equals, =a,b in, ~ contains, ^ prefix, >= at or after, < before.
A label is labels.KEY. \ escapes the next character; a comma inside a value is \,.

Flags:

- `--filter STRINGARRAY` — keep only rows where FIELD OP VALUE holds (repeatable; every filter must hold; --filter help lists the fields)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl webhook redeliver`

POST one delivery again, once, now — and record it as a new delivery

Redeliver POSTs the delivery's event to its subscription once, now,
synchronously, signed with the subscription's CURRENT secret and embedding
the run as it reads today; the transition's own fields and timestamp are the
original's, and X-Ork-Redelivery-Of names the delivery it repeats.
Nothing is queued and nothing retries: you are the retry policy.

The outcome is recorded as a new delivery. The command exits 1 when the
receiver did not accept it (after printing it, --json included).

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl webhook register`

Register a callback URL for run events

Register a callback URL. Repeat --event to select transitions;
omitting it subscribes to every run transition. Task events are opt-in
and are best-effort hints: reconcile with GetRun if one must not be missed.

Scope defaults to every namespace; only an explicit -n narrows it.
ORK_NAMESPACE and the saved default do not apply here. --orkflow and
--task narrow further; each is an exact match, empty means every, and
--task needs at least one TASK_* event.

--name gives the subscription a handle, unique in its namespace scope,
by which an automation document addresses it; it needs an explicit -n.

The signing secret is printed once, here. Deliveries carry it as
an HMAC-SHA256 of the request body in the X-Ork-Signature header.

Flags:

- `--event STRINGARRAY` — event to subscribe to (repeatable): RUN_STARTED | TASK_SCHEDULED | TASK_STARTED | TASK_AWAITING | TASK_RESOLVED | TASK_SUCCEEDED | TASK_FAILED | RUN_SUCCEEDED | RUN_FAILED | RUN_CANCELED
- `--name STRING` — handle an automation document addresses this subscription by (needs -n)
- `--orkflow STRING` — narrow to runs of this orkflow (exact name; empty means every orkflow)
- `--secret STRING` — signing secret to use instead of a generated one
- `--task STRING` — narrow task events to this task (exact id; needs a TASK_* --event)

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

## `orkctl webhook update`

Rename, re-point or re-scope a webhook in place, keeping its secret

update changes a subscription's name, URL, events and scope in place. Its
id and signing secret never change, so a receiver keeps verifying and the
subscription's delivery history stays continuous.

Only the flags given change anything: --event replaces the event list,
--clear-events empties it (every run transition), an explicit -n sets the
namespace scope and --deployment-wide clears it, --orkflow "" and
--task "" clear those, --name "" makes the subscription unnamed. A
name needs a namespace scope. Rotating a secret is delete then register.

Flags:

- `--clear-events` — empty the event list: every run transition
- `--deployment-wide` — clear the namespace scope
- `--event STRINGARRAY` — replace the event list (repeatable): RUN_STARTED | TASK_SCHEDULED | TASK_STARTED | TASK_AWAITING | TASK_RESOLVED | TASK_SUCCEEDED | TASK_FAILED | RUN_SUCCEEDED | RUN_FAILED | RUN_CANCELED
- `--name STRING` — new handle ("" makes the subscription unnamed; needs a namespace scope)
- `--orkflow STRING` — narrow to runs of this orkflow ("" clears it)
- `--task STRING` — narrow task events to this task ("" clears it)
- `--url STRING` — new callback URL

Inherited flags:

- `--json` — emit machine-readable JSON instead of human tables
- `-n, --namespace STRING` — namespace to act in (env ORK_NAMESPACE, then the one saved by 'orkctl namespace use'; default "default")
- `--server STRING` (default `http://localhost:8080`) — OrkStack engine base URL (env ORK_SERVER, then the config file's server)
- `--timeout DURATION` (default `30s`) — timeout for each API request (0 disables it)

