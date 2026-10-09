import { expect, test } from 'bun:test';
import { createdNamespaceHref, namespaceFilterParam, namespacesEmptyState, NAMESPACE_FILTER_MAX } from '../src/lib/namespaceList';

test('the name filter is the trimmed query parameter', () => {
	expect(namespaceFilterParam('  pay  ')).toBe('pay');
	expect(namespaceFilterParam('')).toBe('');
	// Never shortened: a filter longer than a name is the engine's refusal to
	// make, shown as one, not a different filter applied in silence.
	expect(namespaceFilterParam('a'.repeat(NAMESPACE_FILTER_MAX + 5))).toBe('a'.repeat(NAMESPACE_FILTER_MAX + 5));
});

test('a created namespace is reached through the filter, on a first page', () => {
	const coordinates = { ns: 'default', q: 'other', size: '10', cursor: 'abc', page: '4' };
	expect(createdNamespaceHref(coordinates, 'payments')).toBe('/admin/namespaces?ns=default&q=payments&size=10');
});

test('the empty state says which emptiness this is', () => {
	// A filter that matches nothing names the filter.
	expect(namespacesEmptyState({ filter: 'zzz', page: 1, count: 0, hasNext: false })).toBe('no-match');
	// Past the end of an unfiltered walk (the rows were deleted meanwhile).
	expect(namespacesEmptyState({ filter: '', page: 3, count: 0, hasNext: false })).toBe('past-end');
	// A deployment with only `default`: the existing first-run sentence.
	expect(namespacesEmptyState({ filter: '', page: 1, count: 1, hasNext: false })).toBe('only-default');
	// A filter that matches only `default` is a match, not "nothing beyond default".
	expect(namespacesEmptyState({ filter: 'fault', page: 1, count: 1, hasNext: false })).toBeNull();
	// One row on a page with more after it is a page of size 1, not a lonely registry.
	expect(namespacesEmptyState({ filter: '', page: 1, count: 1, hasNext: true })).toBeNull();
	expect(namespacesEmptyState({ filter: '', page: 2, count: 1, hasNext: false })).toBeNull();
	expect(namespacesEmptyState({ filter: '', page: 1, count: 7, hasNext: false })).toBeNull();
});
