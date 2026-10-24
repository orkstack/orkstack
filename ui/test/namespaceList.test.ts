import { expect, test } from 'bun:test';
import { createdNamespaceHref, namespacesEmptyState } from '../src/lib/namespaceList';

test('a created namespace is reached through a name filter, on a first page', () => {
	const coordinates = { ns: 'default', size: '10', cursor: 'abc', page: '4' };
	const href = createdNamespaceHref(coordinates, 'payments');
	expect(href).toBe('/admin/namespaces?ns=default&filter=name%3Dpayments&size=10');
	expect(new URL(`http://x${href}`).searchParams.getAll('filter')).toEqual(['name=payments']);
});

test('the empty state says which emptiness this is', () => {
	// Filters that match nothing.
	expect(namespacesEmptyState({ filtered: true, page: 1, count: 0, hasNext: false })).toBe('no-match');
	// Past the end of an unfiltered walk (the rows were deleted meanwhile).
	expect(namespacesEmptyState({ filtered: false, page: 3, count: 0, hasNext: false })).toBe('past-end');
	// A deployment with only `default`: the existing first-run sentence.
	expect(namespacesEmptyState({ filtered: false, page: 1, count: 1, hasNext: false })).toBe('only-default');
	// Filters that match only `default` are a match, not "nothing beyond default".
	expect(namespacesEmptyState({ filtered: true, page: 1, count: 1, hasNext: false })).toBeNull();
	// One row on a page with more after it is a page of size 1, not a lonely registry.
	expect(namespacesEmptyState({ filtered: false, page: 1, count: 1, hasNext: true })).toBeNull();
	expect(namespacesEmptyState({ filtered: false, page: 2, count: 1, hasNext: false })).toBeNull();
	expect(namespacesEmptyState({ filtered: false, page: 1, count: 7, hasNext: false })).toBeNull();
});
