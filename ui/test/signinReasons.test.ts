import { expect, test } from 'bun:test';
import { issuerHost, problemOf } from '../src/lib/signinReasons';

test('every reason the engine sends has its own words, and an unknown one is still said', () => {
	for (const r of ['no_sign_in_in_progress', 'sign_in_expired', 'state_mismatch', 'issuer_mismatch', 'denied', 'disabled', 'principal_refused', 'issuer_unavailable']) {
		expect(problemOf(r)?.title).not.toBe(problemOf('exchange_failed')?.title);
	}
	expect(problemOf('exchange_failed')?.title).toBe('The sign-in could not be completed.');
	expect(problemOf(null)).toBeNull();
	expect(problemOf('')).toBeNull();
});

test('issuerHost names the host, or the text when it is not a URL', () => {
	expect(issuerHost('https://login.example.com/realms/acme')).toBe('login.example.com');
	expect(issuerHost('orkstack-dev')).toBe('orkstack-dev');
});
