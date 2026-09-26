import { expect, test } from 'bun:test';
import {
	DEFAULT_POOL_NAME,
	holdReasonError,
	MAX_HOLD_REASON_BYTES,
	MAX_TELEMETRY_ATTRIBUTES,
	maxRunningError,
	parseAttributeLines,
	parsePoolList,
	slugError,
	telemetryAttributesError,
	validateNamespaceForm
} from '../src/lib/namespaceRules';

// The Admin namespace form's local validation mirrors the registry's own rule
// (pkg/artifacts/namespace_store.go: namespaceNamePattern, validNamespacePools)
// so a bad slug or an allow-list that excludes its own default is caught
// before an RPC, not after.

test('a slug that starts with a digit or holds uppercase is refused, same as the registry', () => {
	expect(slugError('namespace name', '9ops')).toContain('must match');
	expect(slugError('namespace name', 'Payments')).toContain('must match');
	expect(slugError('namespace name', 'pay_ments')).toContain('must match');
	expect(slugError('namespace name', 'payments')).toBe(null);
	expect(slugError('namespace name', 'ops-2')).toBe(null);
});

test('an empty slug is refused by name, not by the pattern', () => {
	const err = slugError('namespace name', '');
	expect(err).toBe('namespace name is required');
});

test('a slug over the 200-byte bound is refused, naming the bound', () => {
	const long = 'a'.repeat(201);
	expect(slugError('namespace name', long)).toContain('200-byte limit');
	expect(slugError('namespace name', 'a'.repeat(200))).toBe(null);
});

test('an allow-list that excludes the resolved default pool is refused', () => {
	const errors = validateNamespaceForm({
		name: 'payments',
		defaultPool: 'gpu',
		allowedPools: ['cpu', 'io']
	});
	expect(errors.allowedPools).toContain('gpu');
	expect(errors.allowedPools).toContain('must include the default pool');
});

test('an allow-list that includes the resolved default pool is accepted', () => {
	const errors = validateNamespaceForm({
		name: 'payments',
		defaultPool: 'gpu',
		allowedPools: ['cpu', 'gpu']
	});
	expect(errors.allowedPools).toBe(undefined);
});

// An UNSET default_pool resolves to "main" (pkg/artifacts/namespace_store.go
// DefaultPool), which is what an allow-list is actually checked against — an
// allow-list of [cpu, io] with no default_pool set is refused because the
// task that names neither a pool nor a namespace default runs on "main".
test('an unset default pool resolves to "main" for the allow-list check', () => {
	expect(DEFAULT_POOL_NAME).toBe('main');
	const refused = validateNamespaceForm({
		name: 'payments',
		defaultPool: '',
		allowedPools: ['cpu', 'io']
	});
	expect(refused.allowedPools).toContain('main');

	const accepted = validateNamespaceForm({
		name: 'payments',
		defaultPool: '',
		allowedPools: ['cpu', 'main']
	});
	expect(accepted.allowedPools).toBe(undefined);
});

test('an empty allow-list is unrestricted and never refused', () => {
	const errors = validateNamespaceForm({ name: 'payments', defaultPool: 'gpu', allowedPools: [] });
	expect(errors.allowedPools).toBe(undefined);
});

test('a bad pool slug inside the allow-list is named, not just the missing default', () => {
	const errors = validateNamespaceForm({
		name: 'payments',
		defaultPool: 'main',
		allowedPools: ['Bad_Pool']
	});
	expect(errors.allowedPools).toContain('Bad_Pool');
});

test('an update form with no name to change (name field disabled) skips the name rule when told to', () => {
	const errors = validateNamespaceForm(
		{ name: '', defaultPool: 'main', allowedPools: [] },
		{ requireName: false }
	);
	expect(errors.name).toBe(undefined);
});

test('parsePoolList splits on commas and whitespace and drops blanks', () => {
	expect(parsePoolList('cpu, gpu,  io ')).toEqual(['cpu', 'gpu', 'io']);
	expect(parsePoolList('')).toEqual([]);
	expect(parsePoolList('  ,, cpu ,')).toEqual(['cpu']);
});

// runRetentionHours is a UX prefilter for the same rule
// internal/api.validateRetentionDuration enforces (positive, finite); the
// deployment-maximum check itself is the server's, never re-derived here.
test('a blank run retention is inherit, never refused', () => {
	const errors = validateNamespaceForm({
		name: 'payments',
		defaultPool: 'main',
		allowedPools: [],
		runRetentionHours: ''
	});
	expect(errors.runRetention).toBe(undefined);
});

test('a non-positive or unparseable run retention is refused', () => {
	for (const raw of ['0', '-3', 'abc']) {
		const errors = validateNamespaceForm({
			name: 'payments',
			defaultPool: 'main',
			allowedPools: [],
			runRetentionHours: raw
		});
		expect(errors.runRetention).not.toBe(undefined);
	}
});

test('a positive run retention is accepted', () => {
	const errors = validateNamespaceForm({
		name: 'payments',
		defaultPool: 'main',
		allowedPools: [],
		runRetentionHours: '168'
	});
	expect(errors.runRetention).toBe(undefined);
});

// ---------------------------------------------------------------------------
// telemetry attributes
//
// This mirrors internal/dsl.ValidateAttributeMap (key pattern, 256-byte
// values, entry count) plus the reserved prefixes
// internal/api.validNamespaceTelemetry additionally refuses — the same
// restatement-at-every-boundary rule the slug tests above already follow.
// Sink NAME validity is deliberately untested here: this module never
// checks it (the form instead selects from SystemService.ListTelemetrySinks),
// so there is nothing of the server's own rule to restate.
// ---------------------------------------------------------------------------

test('parseAttributeLines splits key=value lines and drops blanks', () => {
	expect(parseAttributeLines('team=payments\n\nservice=checkout\n')).toEqual({
		attributes: { team: 'payments', service: 'checkout' }
	});
	expect(parseAttributeLines('')).toEqual({ attributes: {} });
	expect(parseAttributeLines('  \n  \n')).toEqual({ attributes: {} });
});

test('parseAttributeLines refuses a line with no "="', () => {
	const result = parseAttributeLines('team=payments\nno-equals-sign');
	expect('error' in result).toBe(true);
	if ('error' in result) {
		expect(result.error).toContain('no-equals-sign');
	}
});

test('a lowercase dotted attribute key is accepted, same as the server pattern', () => {
	expect(telemetryAttributesError({ team: 'payments', 'app.tier': 'a' })).toBe(null);
});

test('an attribute key that starts with a digit or holds uppercase is refused, same as the server', () => {
	expect(telemetryAttributesError({ Team: 'payments' })).toContain('must match');
	expect(telemetryAttributesError({ '9team': 'payments' })).toContain('must match');
});

test('a reserved prefix (ork., service., host.) is refused and names the prefix', () => {
	for (const prefix of ['ork.', 'service.', 'host.']) {
		const err = telemetryAttributesError({ [`${prefix}x`]: '1' });
		expect(err).toContain(prefix);
		expect(err).toContain("platform's own");
	}
});

test('an attribute value over the 256-byte bound is refused, naming the bound', () => {
	const long = 'a'.repeat(257);
	expect(telemetryAttributesError({ team: long })).toContain('256-byte limit');
	expect(telemetryAttributesError({ team: 'a'.repeat(256) })).toBe(null);
});

test('more than 16 telemetry attributes are refused, naming the limit', () => {
	const attrs: Record<string, string> = {};
	for (let i = 0; i < MAX_TELEMETRY_ATTRIBUTES + 1; i++) {
		attrs[`key${i}`] = 'v';
	}
	expect(telemetryAttributesError(attrs)).toContain(`${MAX_TELEMETRY_ATTRIBUTES}-entry limit`);
});

test('exactly 16 telemetry attributes are accepted', () => {
	const attrs: Record<string, string> = {};
	for (let i = 0; i < MAX_TELEMETRY_ATTRIBUTES; i++) {
		attrs[`key${i}`] = 'v';
	}
	expect(telemetryAttributesError(attrs)).toBe(null);
});

test('a blank telemetry-attributes field is inherit, never refused', () => {
	const errors = validateNamespaceForm({
		name: 'payments',
		defaultPool: 'main',
		allowedPools: [],
		telemetryAttributesText: ''
	});
	expect(errors.telemetryAttributes).toBe(undefined);
});

test('a malformed telemetry-attributes field surfaces on the form', () => {
	const errors = validateNamespaceForm({
		name: 'payments',
		defaultPool: 'main',
		allowedPools: [],
		telemetryAttributesText: 'ork.reserved=1'
	});
	expect(errors.telemetryAttributes).toContain('ork.');
});

test('a well-formed telemetry-attributes field is accepted', () => {
	const errors = validateNamespaceForm({
		name: 'payments',
		defaultPool: 'main',
		allowedPools: [],
		telemetryAttributesText: 'team=payments\napp.tier=checkout'
	});
	expect(errors.telemetryAttributes).toBe(undefined);
});

// Namespace admission control: a hold's reason and max_running, prefiltered
// to the registry's own rule (pkg/artifacts/namespace_store.go
// validNamespaceHold / validNamespaceMaxRunning).
test('holdReasonError accepts blank (accepting) and a short reason', () => {
	expect(holdReasonError('')).toBe(null);
	expect(holdReasonError('   ')).toBe(null);
	expect(holdReasonError('partner CI loop, INC-4412')).toBe(null);
	expect(holdReasonError('x'.repeat(MAX_HOLD_REASON_BYTES))).toBe(null);
});

test('holdReasonError refuses 513 bytes and a control character', () => {
	expect(holdReasonError('x'.repeat(513))).toContain('512-byte limit');
	expect(holdReasonError('loop\nINC-4412')).toContain('control character');
	expect(holdReasonError('loop\u007f')).toContain('control character');
	// Bytes, not characters: 171 three-byte characters are 513 bytes.
	expect(holdReasonError('€'.repeat(171))).toContain('512-byte limit');
});

test('maxRunningError accepts blank, 0 and 20, refuses -1, 1.5 and abc', () => {
	expect(maxRunningError('')).toBe(null);
	expect(maxRunningError('0')).toBe(null);
	expect(maxRunningError('20')).toBe(null);
	expect(maxRunningError('-1')).toContain('non-negative whole number');
	expect(maxRunningError('1.5')).toContain('non-negative whole number');
	expect(maxRunningError('abc')).toContain('non-negative whole number');
});

test('validateNamespaceForm reports holdReason and maxRunning', () => {
	const errors = validateNamespaceForm({
		name: 'payments',
		defaultPool: '',
		allowedPools: [],
		holdReason: 'x'.repeat(513),
		maxRunning: '-1'
	});
	expect(errors.holdReason).toContain('512-byte limit');
	expect(errors.maxRunning).toContain('non-negative whole number');
	const clean = validateNamespaceForm({
		name: 'payments',
		defaultPool: '',
		allowedPools: [],
		holdReason: '',
		maxRunning: '0'
	});
	expect(clean.holdReason).toBe(undefined);
	expect(clean.maxRunning).toBe(undefined);
});
