// The Admin namespace forms' local validation (M11.6, design §6.2).
//
// This is a UX PREFILTER, never a gate: the server (pkg/artifacts/namespace_store.go,
// validNamespacePools / validPoolName / namespaceNamePattern) is the authority
// and re-validates every field regardless of what this module decided. Being
// caught here only saves a round trip; being missed here costs nothing more
// than the server saying so — the rule can only be too LAX, never a bypass.
// Keeping the two in sync is a courtesy to the person filling the form, not a
// security boundary.
//
// The slug rule is restated here rather than imported because there is
// nothing to import across the browser/server boundary: this is TypeScript,
// the server is Go. AGENTS.md §1.10 asks that the rule be "stated where each
// boundary needs it, over shared test vectors" — this is that restatement,
// and namespaceRules.test.ts is what a reviewer diffs against the Go pattern
// by hand.

// SLUG_PATTERN mirrors internal/dsl.slugPattern / internal/task.poolPattern /
// pkg/artifacts/namespace_store.go's namespaceNamePattern and poolNamePattern:
// one rule, restated at every boundary that needs it (AGENTS.md §1.10).
export const SLUG_PATTERN = /^[a-z][a-z0-9-]*$/;

// MAX_SLUG mirrors dsl.MaxSlug / task.MaxPoolName / the store's maxPoolName —
// all 200, the same bound stated everywhere a slug is accepted.
export const MAX_SLUG = 200;

// DEFAULT_POOL_NAME is what an empty default_pool RESOLVES to
// (pkg/artifacts/namespace_store.go's DefaultPool = "main"), which is what the
// allowed_pools-contains-default rule is actually checked against.
export const DEFAULT_POOL_NAME = 'main';

// slugError names why a slug cannot be stored, or null when it can. `kind` is
// the field's name in the sentence a person reads ("namespace name", "default
// pool", "allowed pool").
export function slugError(kind: string, value: string): string | null {
	if (value === '') return `${kind} is required`;
	const bytes = new TextEncoder().encode(value).length;
	if (bytes > MAX_SLUG) {
		return `${kind} of ${bytes} bytes exceeds the ${MAX_SLUG}-byte limit`;
	}
	if (!SLUG_PATTERN.test(value)) {
		return `${kind} ${JSON.stringify(value)} must match ${SLUG_PATTERN}`;
	}
	return null;
}

// poolSlugError is the same rule for a pool name, which may be empty (empty
// default_pool means "main"; an empty entry inside allowed_pools is instead
// rejected by the caller, which never offers one).
export function poolSlugError(kind: string, value: string): string | null {
	if (value === '') return null;
	return slugError(kind, value);
}

export interface NamespaceFormValues {
	name: string;
	defaultPool: string;
	allowedPools: string[];
}

export interface NamespaceFormErrors {
	name?: string;
	defaultPool?: string;
	allowedPools?: string;
}

// validateNamespaceForm holds a namespace create/update form to the same
// shape the registry enforces (validNamespacePools, pkg/artifacts/namespace_store.go):
// every pool named is a valid slug, and a non-empty allow-list must contain
// the resolved default pool — an allow-list that excluded the namespace's own
// default would refuse every task that names no pool.
export function validateNamespaceForm(
	values: NamespaceFormValues,
	opts: { requireName: boolean } = { requireName: true }
): NamespaceFormErrors {
	const errors: NamespaceFormErrors = {};

	if (opts.requireName || values.name !== '') {
		const err = slugError('namespace name', values.name);
		if (err) errors.name = err;
	}

	const resolvedDefault = values.defaultPool || DEFAULT_POOL_NAME;
	const defaultErr = poolSlugError('default pool', values.defaultPool);
	if (defaultErr) errors.defaultPool = defaultErr;

	for (const pool of values.allowedPools) {
		const err = slugError('allowed pool', pool);
		if (err) {
			errors.allowedPools = err;
			break;
		}
	}
	if (!errors.allowedPools && values.allowedPools.length > 0 && !values.allowedPools.includes(resolvedDefault)) {
		errors.allowedPools =
			`allowed pools (${values.allowedPools.join(', ')}) must include the default pool ` +
			`(${resolvedDefault}); an allow-list that excludes it would refuse every task ` +
			'that names none';
	}

	return errors;
}

// parsePoolList turns the comma/whitespace-separated text a form field holds
// into the slug list the request carries. Blank entries (from a trailing
// comma or repeated separators) are dropped rather than validated as empty
// slugs — that is a formatting slip, not a pool named "".
export function parsePoolList(text: string): string[] {
	return text
		.split(/[,\s]+/)
		.map((s) => s.trim())
		.filter((s) => s.length > 0);
}
