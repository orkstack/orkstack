import { expect, test } from 'bun:test';
import { collectPages, cursorHistory, pageSize, runFilterError, statsWindowError, statusFromParam } from '../src/lib/browsing';
import { Status } from '../src/lib/gen/orkstack/v1/common_pb';

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
test('cursor history ignores malformed URL state', () => {
	expect(cursorHistory('["a","b"]')).toEqual(['a', 'b']);
	expect(cursorHistory('{')).toEqual([]);
	expect(cursorHistory('[1]')).toEqual([]);
});
test('catalog collection follows every opaque page, including records after the default page', async () => {
	const seen: string[] = [];
	const all = await collectPages(async (cursor) => {
		seen.push(cursor);
		return cursor === '' ? { items: Array.from({ length: 25 }, (_, i) => i), nextPageToken: 'second' } : { items: Array.from({ length: 2 }, (_, i) => i + 25), nextPageToken: '' };
	});
	expect(seen).toEqual(['', 'second']);
	expect(all).toHaveLength(27);
});
test('invalid dashboard windows and custom dates are explicit errors', () => {
	expect(statsWindowError('tomorrow', '')).toContain('Unknown');
	expect(statsWindowError('custom', '')).toContain('require');
	expect(statsWindowError('custom', 'not-a-date')).toContain('valid');
});
