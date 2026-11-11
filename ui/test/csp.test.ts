import { expect, test } from 'bun:test';
import { CspWatch, violationsError } from '../e2e/csp';

// The shared Playwright fixture fails a spec on any refusal it did not
// declare (e2e/test.ts). This is its verdict, without a browser.

test('a test that left no refusal passes', () => {
	expect(violationsError([])).toBeNull();
});

test('each refusal left fails the test, named once however often it was reported', () => {
	// One refused script under the header and the meta policy is two reports.
	const err = violationsError([
		'/orkflows/+new: script-src-elem inline',
		'/orkflows/+new: script-src-elem inline',
		'/runs: img-src https://elsewhere.example/x.png'
	]);
	expect(err?.message).toBe(
		'the content security policy refused 2 thing(s) a page tried, which the spec did not expect:\n' +
			'  /orkflows/+new: script-src-elem inline\n' +
			'  /runs: img-src https://elsewhere.example/x.png'
	);
});

test('a refusal the test declared is kept apart, and only that one', () => {
	const watch = new CspWatch();
	watch.record('/: style-src-elem inline');
	watch.expect(/^\/orkflows\/\+new: script-src-elem inline$/g);
	for (const what of ['/orkflows/+new: script-src-elem inline', '/orkflows/+new: script-src-elem inline', '/orkflows/+new: img-src data:']) {
		watch.record(what);
	}
	expect(watch.expected).toEqual(['/orkflows/+new: script-src-elem inline', '/orkflows/+new: script-src-elem inline']);
	expect(watch.unexpected).toEqual(['/: style-src-elem inline', '/orkflows/+new: img-src data:']);
	expect(violationsError(watch.unexpected)).not.toBeNull();
});
