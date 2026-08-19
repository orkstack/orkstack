# OrkStack Model Selection Policy

How this project assigns models to agent work. Every custom workflow that
dispatches agents must follow this policy: **per-role chains spanning all three
subscriptions** (openai-codex, opencode-go, anthropic) so no single quota can
block a run.

Reference: `docs/models/model-selection.md` in the Atomic install. The gpt-5.6
family is the cost/accuracy Pareto frontier; kimi-k3 is a near-top frontier
model at low cost; a third model family (glm) decorrelates review errors.

## Capability tiers used in this project

| Tier | Models | Role |
|------|--------|------|
| Architecture / design | `anthropic/claude-fable-5:max` | Contract-setting design (one wrong call propagates into every slice) |
| Accuracy ceiling | `openai-codex/gpt-5.6-sol:max` | Judgment gate (a wrong verdict wastes a whole loop) |
| Workhorse (frontier, best value) | `openai-codex/gpt-5.6-luna:max`, `opencode-go/gpt-5.6-luna:max` | Implementation, repair, research |
| Frontier, low cost, diversity | `opencode-go/kimi-k3:max` | Implementation/repair primary on alternating rounds |
| Third family (decorrelate errors) | `opencode-go/glm-5.2:max` | Second reviewer chain |
| Anthropic fallback | `anthropic/claude-opus-5:max` (judge, architect) / `:high` (worker) | Fallback + second-reviewer primary + architect fallback |
| Budget tail | `opencode-go/grok-4.5:high` | Cheap last resort |

## Chain rules (all mandatory)

1. **Every stage's fallback chain spans all three providers.** `fallbackModels`
   advances on rate limits AND usage-limit exhaustion, so a provider outage
   degrades instead of blocking.
2. **Worker primaries alternate per round.** Round 1, 3, ... lead with
   `openai-codex/gpt-5.6-luna:max`; round 2, 4, ... lead with
   `opencode-go/kimi-k3:max`. This rotates actual credit consumption rather
   than merely falling back when a limit is already hit.
3. **Both reviewers must approve.** Reviewer A leads with the accuracy ceiling
   (gpt-5.6-sol). Reviewer B leads with a different family
   (claude-opus-5) and includes a THIRD family (glm-5.2) so review errors
   decorrelate.
4. **Never pin a stage without a fallback chain.** A bare `model:` defeats the
   whole point of the policy.
5. **Thinking level** is suffixed on each model string (`:max`, `:high`, ...)
   and must be one the model actually supports (e.g. kimi-k3 supports only
   `:max`; grok-4.5 supports up to `:high`; claude-fable-5 supports
   `minimal`..`max` but has no `:off`).
6. **Architecture and design stages run at `:max`, and so does every fallback
   in their chain.** A design stage is not planning chatter: it fixes the
   proto contract, the storage layout and the routing rule that every later
   slice inherits, and a slice cannot cheaply revise a decision it was handed.
   That failure cost is the judgment-gate's, not the workhorse's, so the level
   does not decay down the chain — an architect that fell back to `:high`
   would quietly buy a cheaper contract for the same eight slices.

## Current chains (parent workflows: the architecture stage)

Applies to the `design` stage of `v003-milestone` and `production-hardening`,
and to any future stage whose output is a contract later stages must obey.

```text
ARCHITECT        : anthropic/claude-fable-5:max
                    -> anthropic/claude-opus-5:max
                    -> openai-codex/gpt-5.6-sol:max
                    -> opencode-go/kimi-k3:max
```

Two anthropic entries lead deliberately: fable-5 is the architecture primary
and opus-5 is its nearest peer, so the first fallback preserves the reasoning
depth rather than the provider spread. The chain still spans all three
subscriptions (rule 1) from the third entry on.

## Current chains (milestone-finish)

```text
WORKER round odd : openai-codex/gpt-5.6-luna:max
                    -> opencode-go/kimi-k3:max
                    -> opencode-go/gpt-5.6-luna:max
                    -> anthropic/claude-opus-5:high
                    -> opencode-go/grok-4.5:high
WORKER round even: opencode-go/kimi-k3:max
                    -> opencode-go/gpt-5.6-luna:max
                    -> openai-codex/gpt-5.6-luna:max
                    -> openai-codex/gpt-5.6-terra:max
                    -> anthropic/claude-opus-5:high
JUDGE A          : openai-codex/gpt-5.6-sol:max
                    -> opencode-go/kimi-k3:max
                    -> anthropic/claude-opus-5:max
                    -> opencode-go/gpt-5.6-luna:max
JUDGE B          : anthropic/claude-opus-5:max
                    -> opencode-go/glm-5.2:max
                    -> openai-codex/gpt-5.6-terra:max
                    -> opencode-go/kimi-k3:max
```

## When a new provider appears

Add its capable models to the chains as primaries or fallbacks per the tiers
above; keep the alternation and the all-provider-spanning rule. Never remove a
provider from the chains unless its subscription is closed.
