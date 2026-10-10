import { expect, test } from 'bun:test';
import { FilterOperator, filterOf } from '../src/lib/listFilter';
import {
	listHref,
	pageNumber,
	pageSize,
	pageStackKey,
	readPageStack,
	runFilterError,
	sinceInputValue,
	statsWindowError,
	statusFromParam,
	writePageStack,
	type PageStackStorage
} from '../src/lib/browsing';
import { Status } from '../src/lib/gen/orkstack/v1/common_pb';

// A Map with Storage's two methods, so the stack helpers are tested without a
// browser and without a global to reset between tests.
function fakeStorage(): PageStackStorage & { map: Map<string, string> } {
	const map = new Map<string, string>();
	return { map, getItem: (key) => map.get(key) ?? null, setItem: (key, value) => void map.set(key, value) };
}

test('run browsing accepts only supported page sizes and statuses', () => {
	expect(pageSize('50')).toBe(50);
	expect(pageSize('999')).toBe(25);
	expect(statusFromParam('failed')).toBe(Status.FAILED);
	expect(statusFromParam('unknown')).toBe(Status.UNSPECIFIED);
});
test('invalid and inverted time filters do not turn into an unfiltered request', () => {
	expect(runFilterError('bogus', '', '')).toContain('Unknown');
	expect(runFilterError('', 'not-a-date', '')).toContain('valid timestamp');
	expect(runFilterError('', '2026-01-02T00:00', '2026-01-01T00:00')).toContain('after');
});
test('invalid dashboard windows and custom dates are explicit errors', () => {
	expect(statsWindowError('tomorrow', '')).toContain('Unknown');
	expect(statsWindowError('custom', '')).toContain('require');
	expect(statsWindowError('custom', 'not-a-date')).toContain('valid');
});

// THE BUG THIS FILE EXISTS FOR: a page-size change used to be concatenated
// onto a query that already carried `size`, so the URL held `size=25&size=50`
// and `searchParams.get('size')` answered with the value the reader had just
// left. A builder that sets each key once cannot express that URL.
test('a changed coordinate replaces its key instead of adding a second one', () => {
	const coordinates = { ns: 'default', size: '25', cursor: 'abc', page: '2' };
	const params = new URL(`http://x${listHref('/orkflows', { ...coordinates, size: '50' })}`).searchParams;
	expect(params.getAll('size')).toEqual(['50']);
	expect(params.get('cursor')).toBe('abc');
});
test('the address carries only coordinates that have a value, namespace first', () => {
	const href = listHref('/runs', { ns: 'default', orkflow: '', status: 'failed', size: '10', since: '', until: '', cursor: '', page: '1' });
	expect(href).toBe('/runs?ns=default&status=failed&size=10');
	// page 1 and an empty cursor are what an address with neither already
	// means, so writing them would be noise a reader has to discount.
	expect(href).not.toContain('page=');
	expect(href).not.toContain('cursor=');
	expect(listHref('/runs', { ns: 'default', cursor: 'tok', page: '3' })).toBe('/runs?ns=default&cursor=tok&page=3');
	// Nothing to say is the bare path, never a dangling '?'.
	expect(listHref('/admin/releases', {})).toBe('/admin/releases');
	expect(listHref('/admin/releases', {}, { filter: [] })).toBe('/admin/releases');
});
test('a page number is a positive integer or the first page', () => {
	expect(pageNumber('3')).toBe(3);
	expect(pageNumber('0')).toBe(1);
	expect(pageNumber('x')).toBe(1);
	expect(pageNumber('')).toBe(1);
	expect(pageNumber('-2')).toBe(1);
	expect(pageNumber('2.5')).toBe(1);
});
test('a cursor stack belongs to one filter set and round-trips through storage', () => {
	const storage = fakeStorage();
	const coordinates = { ns: 'default', orkflow: 'deploy', size: '10' };
	const key = pageStackKey('/runs', { ...coordinates, cursor: 'c2', page: '2' });
	// The position within the filter set is NOT part of the key: one stack
	// describes the whole walk, and a stack written under other filters must
	// never be applied here.
	expect(key).toBe('orkstack.pages:/runs?ns=default&orkflow=deploy&size=10');
	expect(key).not.toBe(pageStackKey('/runs', { ...coordinates, size: '25' }));
	writePageStack(key, ['', 'c2', 'c3'], storage);
	expect(readPageStack(key, storage)).toEqual(['', 'c2', 'c3']);
	expect(readPageStack(pageStackKey('/runs', { ...coordinates, size: '25' }), storage)).toEqual([]);
});
test('a stack that does not describe this page is not the path back', () => {
	const storage = fakeStorage();
	const key = pageStackKey('/orkflows', { ns: 'default', size: '10' });
	writePageStack(key, ['', 'c2'], storage);
	const stack = readPageStack(key, storage);
	// Page 2 reached by Next: the stack names this page's own cursor at its
	// index, so the entry before it is the cursor to go back to.
	expect(stack[2 - 1] === 'c2').toBe(true);
	// The same page reached by a pasted link whose cursor this stack has never
	// seen: nothing here names the page before it, and Previous must say so
	// rather than send the reader to some other page's cursor.
	expect(stack[2 - 1] === 'other-cursor').toBe(false);
});
test('storage that refuses to answer leaves the reader on a page that works', () => {
	const hostile: PageStackStorage = {
		getItem() { throw new Error('private mode'); },
		setItem() { throw new Error('private mode'); }
	};
	// Neither call throws — a page whose Previous cannot be remembered still
	// renders; it offers First page instead.
	writePageStack('orkstack.pages:/runs?ns=default', ['', 'c2'], hostile);
	expect(readPageStack('orkstack.pages:/runs?ns=default', hostile)).toEqual([]);
	// Malformed contents are not a stack either.
	const storage = fakeStorage();
	storage.map.set('k', '{');
	expect(readPageStack('k', storage)).toEqual([]);
	storage.map.set('k', '[1]');
	expect(readPageStack('k', storage)).toEqual([]);
});
test('the custom window round-trips between the input and the URL', () => {
	const typed = '2026-09-10T08:30';
	expect(sinceInputValue(new Date(typed).toISOString())).toBe(typed);
	expect(sinceInputValue('')).toBe('');
	expect(sinceInputValue('not-a-date')).toBe('');
});

// Filters ride the address as one repeated key per list, in canonical order,
// after `ns` and before every scalar coordinate (docs/UI_CLI_PARITY.md
// "Filtering lists").
test('filters are the one repeated key, in canonical order, after ns', () => {
	const filters = [
		filterOf('name', FilterOperator.PREFIX, ['tenant-']),
		filterOf('labels.team', FilterOperator.EQUALS, ['payments']),
		filterOf('name', FilterOperator.PREFIX, ['tenant-'])
	];
	const href = listHref('/admin/namespaces', { ns: 'default', size: '10', cursor: '', page: '1' }, { filter: filters });
	expect(href).toBe('/admin/namespaces?ns=default&filter=labels.team%3Dpayments&filter=name%5Etenant-&size=10');
	const params = new URL(`http://x${href}`).searchParams;
	expect(params.getAll('filter')).toEqual(['labels.team=payments', 'name^tenant-']);
	// Scalars are still set once.
	expect(params.getAll('size')).toEqual(['10']);
	// The same set in another order is the same address.
	expect(listHref('/admin/namespaces', { ns: 'default', size: '10' }, { filter: [...filters].reverse() })).toBe(href);
	// A second list's parameter is its own key.
	expect(listHref('/admin/webhooks', { ns: 'default' }, { deliveryFilter: [filterOf('delivered', FilterOperator.EQUALS, ['false'])] }))
		.toBe('/admin/webhooks?ns=default&deliveryFilter=delivered%3Dfalse');
	// A value with a + is percent-encoded, so the link round-trips.
	const instant = listHref('/orkflows', { ns: 'default' }, { filter: [filterOf('created_at', FilterOperator.AT_OR_AFTER, ['2026-10-09T00:00:00+02:00'])] });
	expect(instant).toBe('/orkflows?ns=default&filter=created_at%3E%3D2026-10-09T00%3A00%3A00%2B02%3A00');
	expect(new URL(`http://x${instant}`).searchParams.getAll('filter')).toEqual(['created_at>=2026-10-09T00:00:00+02:00']);
});

test('a page stack belongs to one filter set', () => {
	const storage = fakeStorage();
	const team = { filter: [filterOf('labels.team', FilterOperator.EQUALS, ['payments'])] };
	const key = pageStackKey('/admin/namespaces', { ns: 'default', size: '10', cursor: 'c2', page: '2' }, team);
	expect(key).toBe('orkstack.pages:/admin/namespaces?ns=default&filter=labels.team%3Dpayments&size=10');
	expect(key).not.toBe(pageStackKey('/admin/namespaces', { ns: 'default', size: '10' }));
	expect(key).not.toBe(pageStackKey('/admin/namespaces', { ns: 'default', size: '10' }, { filter: [filterOf('labels.team', FilterOperator.EQUALS, ['search'])] }));
	writePageStack(key, ['', 'c2'], storage);
	expect(readPageStack(pageStackKey('/admin/namespaces', { ns: 'default', size: '10' }), storage)).toEqual([]);
});
