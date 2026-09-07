// The Admin namespace forms' local validation.
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
// the server is Go. AGENTS.md §7 asks that the rule be "stated where each
// boundary needs it, over shared test vectors" — this is that restatement,
// and namespaceRules.test.ts is what a reviewer diffs against the Go pattern
// by hand.

import { parseRetentionHours } from './retentionForm';

// SLUG_PATTERN mirrors internal/dsl.slugPattern / internal/task.poolPattern /
// pkg/artifacts/namespace_store.go's namespaceNamePattern and poolNamePattern:
// one rule, restated at every boundary that needs it (AGENTS.md §7).
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
	// runRetentionHours is blank text, never a parsed value: blank means
	// "inherit the deployment maximum", which is a value only the write path
	// (this form's caller) can turn into "send no field at all".
	runRetentionHours?: string;
	// telemetryAttributesText is the raw textarea contents, one `key=value`
	// per line — blank means the namespace sets none of its own, same as
	// runRetentionHours's blank-means-inherit above.
	telemetryAttributesText?: string;
}

export interface NamespaceFormErrors {
	name?: string;
	defaultPool?: string;
	allowedPools?: string;
	runRetention?: string;
	telemetryAttributes?: string;
}

// ATTRIBUTE_KEY_PATTERN mirrors internal/dsl.attributeKeyPattern — the one
// rule internal/dsl.ValidateAttributeMap holds every bounded string-map in
// the tree to, restated here the same way SLUG_PATTERN restates the
// registry's own namespace/pool rule (AGENTS.md §7): this is TypeScript, the
// server is Go, so there is nothing to import across the boundary.
export const ATTRIBUTE_KEY_PATTERN = /^[a-z][a-z0-9_.-]{0,63}$/;

// MAX_ATTRIBUTE_VALUE_BYTES and MAX_TELEMETRY_ATTRIBUTES mirror
// internal/dsl's maxAttributeValueBytes (256) and the 16-entry limit
// internal/api.validNamespaceTelemetry passes to ValidateAttributeMap for
// telemetry.attributes specifically (a namespace's `labels` share the same
// 256-byte value rule but a different, larger entry count).
export const MAX_ATTRIBUTE_VALUE_BYTES = 256;
export const MAX_TELEMETRY_ATTRIBUTES = 16;

// RESERVED_ATTRIBUTE_PREFIXES mirrors internal/api.reservedAttributePrefixes
// / pkg/artifacts.reservedTelemetryAttributePrefixes: the namespaces the
// platform's own instrumentation stamps (internal/engine.telemetryFor and
// OpenTelemetry's resource attributes), which a namespace admin cannot
// shadow.
export const RESERVED_ATTRIBUTE_PREFIXES = ['ork.', 'service.', 'host.'];

// parseAttributeLines turns a textarea's `key=value` lines into the map a
// request carries. Blank lines are dropped rather than validated as empty
// entries — trailing whitespace in a textarea is a formatting slip, not an
// attribute named "". A line with no "=" at all IS an error, unlike
// parsePoolList's comma-separated slugs, because "key=value" has no sensible
// reading of a bare token.
export function parseAttributeLines(text: string): { attributes: Record<string, string>; error?: string } {
	const attributes: Record<string, string> = {};
	for (const raw of text.split('\n')) {
		const line = raw.trim();
		if (line === '') continue;
		const at = line.indexOf('=');
		if (at <= 0) {
			return { attributes: {}, error: `invalid attribute ${JSON.stringify(line)}: want key=value` };
		}
		attributes[line.slice(0, at)] = line.slice(at + 1);
	}
	return { attributes };
}

// telemetryAttributesError holds a parsed attribute map to the same shape
// internal/dsl.ValidateAttributeMap enforces, plus the reserved prefixes
// internal/api.validNamespaceTelemetry additionally refuses. Returns the
// FIRST violation, the same "one message, not a list" shape slugError and
// the allowed-pools check above already follow.
export function telemetryAttributesError(attributes: Record<string, string>): string | null {
	const keys = Object.keys(attributes);
	if (keys.length > MAX_TELEMETRY_ATTRIBUTES) {
		return `${keys.length} telemetry attributes exceed the ${MAX_TELEMETRY_ATTRIBUTES}-entry limit`;
	}
	for (const key of keys) {
		if (!ATTRIBUTE_KEY_PATTERN.test(key)) {
			return `telemetry attribute key ${JSON.stringify(key)} must match ${ATTRIBUTE_KEY_PATTERN}`;
		}
		for (const prefix of RESERVED_ATTRIBUTE_PREFIXES) {
			if (key.startsWith(prefix)) {
				return (
					`telemetry attribute key ${JSON.stringify(key)} uses prefix ${JSON.stringify(prefix)}, ` +
					"which is the platform's own"
				);
			}
		}
		const bytes = new TextEncoder().encode(attributes[key]).length;
		if (bytes > MAX_ATTRIBUTE_VALUE_BYTES) {
			return `telemetry attribute ${JSON.stringify(key)} value of ${bytes} bytes exceeds the ${MAX_ATTRIBUTE_VALUE_BYTES}-byte limit`;
		}
	}
	return null;
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

	// Blank means inherit — the same rule the deployment-maximum form
	// ($lib/retentionForm.ts) uses, checked here only when the field carries
	// something. This module is a prefilter, never a gate (see the file's
	// own doc comment above): whether the value fits under the deployment
	// maximum is the server's call (internal/api.validNamespaceRetention),
	// shown verbatim from its own InvalidArgument, never re-derived here.
	if (values.runRetentionHours !== undefined && values.runRetentionHours.trim() !== '') {
		const parsed = parseRetentionHours(values.runRetentionHours);
		if ('error' in parsed) errors.runRetention = parsed.error;
	}

	// Blank means no telemetry attributes of this namespace's own, checked
	// only when the field carries something — the same "blank means inherit"
	// rule run_retention just above follows. Whether a SINK NAME is one this
	// deployment declared is not checked here: that answer lives only on the
	// server (SystemService.ListTelemetrySinks / internal/api.
	// validNamespaceTelemetry), so the form instead offers a select fed from
	// that RPC and has nothing of its own to validate for it.
	if (values.telemetryAttributesText !== undefined && values.telemetryAttributesText.trim() !== '') {
		const parsed = parseAttributeLines(values.telemetryAttributesText);
		if (parsed.error) {
			errors.telemetryAttributes = parsed.error;
		} else {
			const err = telemetryAttributesError(parsed.attributes);
			if (err) errors.telemetryAttributes = err;
		}
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
