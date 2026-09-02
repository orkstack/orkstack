import { expect, test } from 'bun:test';
import {
	DEFAULT_POOL_NAME,
	parsePoolList,
	slugError,
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
