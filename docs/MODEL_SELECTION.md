# OrkStack model routing policy

This policy optimizes frontier-model use without lowering the engineering bar.
It applies to every custom workflow and agent run in this repository. Model
choice follows risk and evidence, not a fixed role label.

## Operating rules

1. **Quality first.** Specifications, artifacts, deterministic checks, fresh
   review, adversarial analysis and reproducible evidence decide acceptance.
   Model agreement never replaces executable proof.
2. **Use the least costly capable model.** Route by architectural novelty,
   coupling, blast radius, concurrency or distributed state, ambiguity,
   deterministic-verification difficulty, security or data-integrity impact,
   and migration or compatibility risk. Score each from 0 to 2.
3. **Escalate on evidence.** Repeated implementation failure, unresolved
   ambiguity, reviewer disagreement, an invariant violation, non-local
   debugging, security risk or hard state/concurrency reasoning justifies a
   stronger model. Do not escalate for prettier code.
4. **Keep context small.** Pass specifications, ADRs, contracts, findings and
   test evidence as focused artifacts. Reviewers judge the diff and evidence,
   not the implementer's reasoning transcript.
5. **Never weaken a requirement or test.** A model may flag an ambiguity, but
   it may not silently narrow the contract. Resolve material ambiguity before
   implementation.
6. **Deterministic checks come first.** Run compile, type, unit, integration,
   schema, lint, security, migration, fuzz, property and runtime checks that
   apply before spending tokens on review. Use durable workflow tool nodes for
   checks and side effects; use their real results in later stages.

## Risk bands and defaults

These are starting points. Evidence may move a task up or down.

| Score | Work shape | Default route |
|---|---|---|
| 0–3 | Routine, bounded | Sonnet plan/implementation; Luna first review; cheap adversary when useful |
| 4–7 | Moderately coupled | Opus planning when architecture changes; Sonnet implementation; Terra when deeper review is needed; cheap adversary |
| 8–11 | High complexity or risk | Opus architecture; Sonnet for bounded slices or Opus for difficult implementation; Terra review; stronger adversary if warranted |
| 12+ | Exceptional or high blast radius | Fable architecture/arbitration; Opus implementation; Terra or Sol review; strong adversary |

Fable is scarce. Use it only for multiple major architecture choices,
irreversible decisions, severe ambiguity, distributed consistency, security,
large migrations, unresolved Opus work or material disagreement. Do not use it
for CRUD, boilerplate, routine refactors, ordinary tests or known designs.

## Capability tiers

The ids below are catalog-checked examples. Before adding or pinning a model,
inspect the configured catalog and use only a returned `fullId` plus a thinking
suffix listed for that model. An empty catalog means stages stay unpinned.

| Tier | Models | Use |
|---|---|---|
| Exceptional architecture | `anthropic/claude-fable-5:max` | Arbitration and rare architecture work only |
| Difficult implementation | `anthropic/claude-opus-5:high` | Non-local bugs, subtle state, security, or failed Sonnet work |
| Default implementation | `anthropic/claude-sonnet-5:high` | Approved, bounded feature work and repairs |
| First review | `openai-codex/gpt-5.6-luna:medium` | Compliance, obvious regressions, test and API gaps |
| Deep review | `openai-codex/gpt-5.6-terra:high` | Invariants, architecture, concurrency, migration and semantic risk |
| Review escalation | `openai-codex/gpt-5.6-sol:max` | Critical changes, disagreement or unresolved severe findings |
| Adversary | `openai-codex/gpt-5.4-mini:high`, `anthropic/claude-haiku-4-5:high` | Inexpensive high-thinking falsification, edge cases and hidden failure modes with OpenAI/Anthropic provider diversity; no code edits |
| Reporting | `anthropic/claude-sonnet-5:medium` | Short factual reports; use a cheaper catalog model when suitable |

Thinking level follows role and failure cost: `max` is reserved for security,
identity, high-cost adversarial challenge and final approval; `high` for
architecture, planning, compatibility, triage, repair and routine adversarial
analysis; `medium` for user-impact review and reporting. An explicit user level
wins only when the catalog supports it.

## Workflow rules

- **Implementation:** Sonnet is the default after contracts and acceptance
  criteria are explicit. Return repairs to the original implementer and give it
  only the confirmed finding, failing evidence, contract and affected region.
- **Escalation:** If the same model fails to fix a materially identical issue
  twice, move Sonnet repair to Opus; move unresolved Opus reasoning to Fable or
  use Fable as arbiter. Keep loops bounded and stop at the iteration limit.
- **Review:** Start with Luna. Use Terra for non-trivial semantic review or
  meaningful Luna findings. Reserve Sol for critical changes, disagreement or
  unresolved severe findings. Reviewers use fresh context and do not modify
  implementation code.
- **Adversarial work:** Prefer `openai-codex/gpt-5.4-mini:high`, with
  `anthropic/claude-haiku-4-5:high` as its provider-diverse fallback, for
  inexpensive high-thinking analysis. Require each finding to cite evidence
  and each probe to name its command, input, expected result and risk. Execute
  selected probes deterministically; do not treat the adversary's report as
  proof.
- **Consolidation:** Classify findings as confirmed, probable, ambiguity,
  architectural concern, test deficiency, style, duplicate or false positive.
  Repair only confirmed defects, accepted architecture concerns, required test
  gaps and resolved ambiguities.
- **Provider diversity:** Use different families for implementation, review and
  adversarial work when that reduces correlated errors. Do not force every
  fallback chain to span every provider; capability and budget come first.
- **Fallbacks:** A fallback handles availability or provider failure. It does
  not hide a failed check or replace failure-driven escalation.
- **Budgets:** Below 60% allowance use normal routing; at 60–80% avoid Fable and
  prefer Sonnet/Luna/Terra; at 80–90% reserve Fable, Opus and Sol for blocking
  or high-risk work; above 90% use frontier capacity only for project-blocking
  or high-risk work. Use cheaper configured pools for routine adversarial work.
- **Metrics:** Where available, track first-pass acceptance, repair rounds,
  defects by reviewer tier, false positives, escalations, tokens, cost,
  latency, escaped defects, test failures after acceptance, Fable/Opus use and
  reviewer disagreement. Raise or lower routing for task classes from evidence.

## Routes used by the custom workflows

`.atomic/workflows/support/model-policy.ts` is the shared source for current
routes. It uses Opus/high for the established high-risk architecture stages,
Sonnet/high for implementation, Luna/medium for first review, Terra/high for
non-trivial deep review, GPT-5.4 Mini/high with Haiku/high fallback for
inexpensive provider-diverse adversarial probes, and Sonnet/medium for
reporting. Fable and Sol are not routine workflow stages; pass them only when
the risk evidence meets the escalation rules above.
