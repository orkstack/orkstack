# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users
Two audiences of equal weight, both engineers at a desk, often mid-task and often under pressure when a run has failed.

- **Orkflow authors.** Engineers who build advanced Orkflows (YAML documents that compile to durable Temporal workflows whose tasks are OCI containers). They apply and plan Orkflows, submit and watch runs, read task logs and artifacts, compare runs and diagnose failures.
- **Platform operators.** Engineers who run a deployment: Orkers and pools, namespaces, run retention, image policy, releases, usage and budgets, caches, webhooks, audit, and the namespace automation document.

## Product Purpose
OrkStack is a thin, opinionated durable execution platform: Temporal owns execution, object storage owns data and artifacts, OCI containers own computation. Any OCI image becomes a durable production activity without an OrkStack SDK or a custom worker service. The UI is the place to see that work as it happens and to act on it: apply an Orkflow, plan it, run it, watch it, understand why it ended as it did, and operate the deployment around it. Success is an engineer who can always tell what is running, what ended and why, and what to do next, without reading a database or a Temporal history.

## Positioning
Two things a neighbouring product could not truthfully copy, both to be shown by the UI:

- **Any OCI image as a durable activity, shown as it runs.** The DAG, per-task attempts, logs, artifacts, cache hits and the exact image digest, with no SDK or custom worker behind it.
- **Proof and inspectability.** Plan before you run, compare runs, evidence bundles and explicit machine-readable failure reasons: there is no stuck, half-completed or unknown state, and the UI never hides why something happened.

## Operating Context
- Engineers work beside a terminal (`orkctl`) and an editor; the CLI and the UI are two surfaces of one contract (`docs/UI_CLI_PARITY.md`), so a copied CLI command reproduces what the UI submitted.
- Authored documents (an Orkflow, an automation document) are YAML. Structured runtime data (a run's input, its output, `values.json`) is always JSON. The UI has a typed JSON input editor and no YAML input mode.
- Runs are long-lived and live: events and task logs stream. Finished runs come from an archive; a run may be large (the task ceiling is 4000 and the run page is held to a 5 s budget at that size).
- Routes today: runs (list, run page, compare), Orkflows (list, detail), Apply, and admin (namespaces, pools, activities, releases, retention, usage, cache, image policy, webhooks, audit, automation).

## Capabilities and Constraints
- SvelteKit with Svelte 5 runes, Vite, Tailwind 4 and Bun, with generated Connect clients under `ui/src/lib/gen`, one client module per service. All design tokens are to live in one file under `ui/` (light and dark colours, fonts, monospace, radii, spacing), importable by other surfaces.
- Vocabulary is deliberate (AGENTS.md §7). The UI never names a queue, session, generation, checkpoint, task queue, workflow id, search attribute or any other Temporal mechanism. A run is one run: it "carries on", and its checkpoint is its "saved state". Use Orkflow, pool, pin, schedule, trigger, fire, origin, release and the other defined words as defined.
- The principal is asserted by a gateway, not authenticated. No surface may present it as a verified identity; every rendering says it was asserted.
- A bearer credential (a trigger token, an await callback token) is shown once and never again.
- Non-goals: a visual workflow designer, an AI agent builder, a forms platform. The UI is not an editor of a graph canvas.
- The UI is tested by Playwright specs against a real engine, Orker, Temporal and MinIO; browser-only behaviour is proven with a spec and screenshots.
- Undecided: a final product name treatment and logo (OrkStack as a name is binding), and any accessibility standard beyond good practice.

## Brand Commitments
The name OrkStack, and the words Orkflow, Orker, `ork` and `orkctl`, are fixed. The product is never compared to or named against another tool in its own copy. No logo or brand asset exists yet.

## Evidence on Hand
- Real running behaviour: the current UI, 225 Playwright specs with screenshots, and the example Orkflows in `examples/`.
- Reference docs under `docs/` (DSL, OCI activity contract, deployment, schedules, triggers, automation, patterns).
- Absences future work must not fabricate: no customers, testimonials, benchmarks beyond the measured run-page budget, pricing or hosted offering.

## Product Principles
- **Show the cause, not just the state.** Every terminal state carries its machine-readable reason and a human explanation, and the UI leads with it.
- **Inspectable beats impressive.** A view earns its place by answering an operational question: what ran, on which image, why this outcome, what changed between two runs.
- **Fast and light.** Large runs and live streams must stay responsive; the interface weighs little and loads quickly.
- **One contract, two surfaces.** The UI and the CLI show and accept the same things in the same words; nothing is possible only in one.
- **Say what is asserted.** Never imply authentication, exactly-once execution or verification that does not exist.

## Accessibility & Inclusion
No product-specific standard established yet; to be decided. Keyboard operation of the run, log and admin views matters because the users are engineers working quickly.
