// Shared model routes for OrkStack workflows.
//
// These routes are capability defaults, not permanent role assignments. A
// caller may choose a stronger route when the risk evidence warrants it, but
// must record why. Model ids and thinking levels are checked against the
// configured catalog before a workflow run.

export type ModelRoute = {
  readonly model: string;
  readonly fallbackModels: readonly string[];
};

// Architecture is high risk in these workflows, but Fable is reserved for
// unresolved ambiguity, irreversible choices, or an Opus failure. Do not add
// it as a routine fallback.
export const ARCHITECTURE_ROUTE: ModelRoute = {
  model: "anthropic/claude-opus-5:high",
  fallbackModels: [
    "anthropic/claude-sonnet-5:high",
    "openai-codex/gpt-5.6-luna:high",
  ],
};

// Sonnet is the default implementer. The fallback list handles provider or
// availability failures; it is not a substitute for evidence-based escalation.
const IMPLEMENTATION_ROUTE: ModelRoute = {
  model: "anthropic/claude-sonnet-5:high",
  fallbackModels: ["openai-codex/gpt-5.6-luna:high"],
};

const ESCALATED_IMPLEMENTATION_ROUTE: ModelRoute = {
  model: "anthropic/claude-opus-5:high",
  fallbackModels: [
    "openai-codex/gpt-5.6-terra:high",
    "anthropic/claude-sonnet-5:high",
  ],
};

export function implementationRoute(escalated = false): ModelRoute {
  return escalated ? ESCALATED_IMPLEMENTATION_ROUTE : IMPLEMENTATION_ROUTE;
}

// Luna is the cheap first-pass reviewer. Terra is reserved for non-trivial
// semantic review or a Luna finding that needs deeper reasoning.
export const FIRST_REVIEW_ROUTE: ModelRoute = {
  model: "openai-codex/gpt-5.6-luna:medium",
  fallbackModels: ["anthropic/claude-sonnet-5:medium"],
};

export const DEEP_REVIEW_ROUTE: ModelRoute = {
  model: "openai-codex/gpt-5.6-terra:high",
  fallbackModels: [
    "anthropic/claude-opus-5:high",
    "anthropic/claude-sonnet-5:high",
  ],
};

// GPT-5.4 Mini is the inexpensive high-thinking adversary. Haiku preserves
// provider diversity as its fallback; neither model owns repairs.
export const ADVERSARY_ROUTE: ModelRoute = {
  model: "openai-codex/gpt-5.4-mini:high",
  fallbackModels: ["anthropic/claude-haiku-4-5:high"],
};

export const REPORT_ROUTE: ModelRoute = {
  model: "anthropic/claude-sonnet-5:medium",
  fallbackModels: ["anthropic/claude-haiku-4-5:medium"],
};
