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
