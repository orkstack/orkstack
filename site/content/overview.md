# OrkStack documentation

OrkStack runs durable, multi-step Orkflows written in YAML, with each step an
ordinary OCI container, on Temporal and S3-compatible object storage. These
pages are ported from the repository's own docs on every build, so they say
what the code on `main` does.

## Start here

- [Install and run locally](/docs/getting-started/install/) — the local stack
  and a first run, step by step.
- [Examples](/docs/getting-started/examples/) — every example Orkflow, with
  its YAML.
- [Architecture](/docs/concepts/architecture/) — the components, and how a run
  flows through them.

## By task

| You want to | Read |
|---|---|
| write an Orkflow | [Orkflow DSL reference](/docs/authoring/dsl/), [Orkflow patterns](/docs/authoring/patterns/) |
| turn an image into an activity | [OCI activity contract](/docs/activities/contract/), [Activity images](/docs/activities/images/) |
| start runs on a schedule or from outside | [Schedules](/docs/starting-runs/schedules/), [Triggers](/docs/starting-runs/triggers/), [Automation documents](/docs/starting-runs/automation/) |
| run a deployment | [Deployment and operations](/docs/operating/deployment/), [Configuration](/docs/operating/configuration/) |
| look up a command | [orkctl reference](/docs/reference/cli/) |
| change OrkStack itself | [Contributing](/docs/contributing/), [Engineering contract](/docs/contributing/engineering-contract/) |

## What it does

**Authoring** ([DSL reference](/docs/authoring/dsl/),
[patterns](/docs/authoring/patterns/))

- A DAG of tasks (`needs:`), typed `inputs:`, templated `with:` and
  `condition`, retries and timeouts; immutable versions pinned by a SHA-256
  definition digest.
- `pool:` selects which Orkers run a task; `pin:` keeps a group of tasks on
  one Orker sharing `/orkstack/shared`.
- `each:` fans one task out over an array, one item per element.
- `uses: orkflow:<name>@<version>` composes another Orkflow as a child run,
  returning its declared `outputs:`.
- `await:` pauses a task until it is resolved from outside (an approval, an
  external completion, a one-time callback) or its deadline passes.
- `cache:` reuses a prior successful result for identical inputs (a memo).
- `join:` decides when a task may start from how its needs ended; failure
  handlers run compensation in a failing run.
- Images may declare their `with:` interface (`orkctl activity inspect`);
  promoting a version that breaks the Orkflow's declared interface needs an
  explicit acknowledgement (`orkctl orkflow compat`, `callers`).

**Starting runs**

- `orkctl run submit` (idempotency keys, `--origin-ref`), `run again`, and
  `run plan` to see which tasks would run without starting anything.
- [Triggers](/docs/starting-runs/triggers/): a bearer-token plain-HTTP
  starter.
- [Schedules](/docs/starting-runs/schedules/): cron or interval, backfill,
  manual fire.
- Every run records its origin: what started it, a caller reference and the
  asserted principal.

**Operating** ([deployment and operations](/docs/operating/deployment/))

- Namespaces with allowed pools, run retention, admission (`hold`,
  `max_running`), reserved cpu/memory budgets, allowed secret sources, a
  required image signer and a telemetry policy.
- A deployment image policy: require digests, allow registries, cosign keys.
- Releases: `orkctl release status|ramp|promote|rollback|wait` rolls a new
  build out without breaking running runs.
- Orker fleet: `orkctl orker list|get|cordon|uncordon|drain`.
- [Automation documents](/docs/starting-runs/automation/): one YAML document
  per namespace for its settings, schedules, triggers and webhooks, applied
  with `orkctl apply` and exported with `orkctl export`.
- [Webhooks](/docs/starting-runs/webhooks/) scoped by namespace, Orkflow and
  task, with signed bodies, delivery records and redelivery.
- A control-plane audit trail (`orkctl admin audit`).
- OpenTelemetry logs, metrics and traces, off by default, with named sinks
  per namespace.

**Inspecting** ([stats](/docs/operating/stats/),
[run evidence](/docs/operating/run-evidence/))

- `orkctl run get|watch|logs|attempts|timing|inspect|diff|artifacts`: live
  attempt state, per-phase timing, every retry, and run-to-run comparison.
- Artifact provenance: whether a recorded artifact's bytes remain and which
  archived runs recorded the same object.
- `orkctl stats` with failure fingerprints, a flake index, reserved usage and
  cache hit rates, all derived on read.
- Evidence bundles: `orkctl run evidence export|verify`, verifiable offline.
- `orkctl task run` replays one task locally against a run's recorded bytes;
  `orkctl activity test` checks an image against the activity contract.
