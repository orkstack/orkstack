import { expect, test } from 'bun:test';
import { principalMark } from '../src/lib/principal';

// Regression: every principal rendered as "asserted", verified ones included
// (AGENTS.md §7: no surface may present one as the other).
test('a principal OrkStack verified is marked verified, and only that one', () => {
	const verified = { word: 'verified', tip: 'Verified by OrkStack', sr: 'verified by OrkStack' };
	expect(principalMark(true)).toEqual(verified);
	expect(principalMark(true, '  ')).toEqual(verified);
	expect(principalMark(true, 'oidc')).toEqual({
		word: 'verified',
		tip: 'Verified by OrkStack (oidc)',
		sr: 'verified by OrkStack (oidc)'
	});
});

test('a principal a record does not say was verified is marked asserted', () => {
	const asserted = {
		word: 'asserted',
		tip: 'Asserted by your gateway, not verified by OrkStack',
		sr: 'asserted by your gateway, not verified by OrkStack'
	};
	expect(principalMark(false)).toEqual(asserted);
	// A method never makes an unverified principal read as verified.
	expect(principalMark(false, 'oidc')).toEqual(asserted);
	// Anything but a real true (an absent field reaching the template) is asserted.
	expect(principalMark(undefined as unknown as boolean)).toEqual(asserted);
	expect(principalMark('true' as unknown as boolean)).toEqual(asserted);
});
