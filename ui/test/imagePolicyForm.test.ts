import { expect, test } from 'bun:test';
import {
	fingerprint,
	MAX_ALLOWED_REGISTRIES,
	MAX_SIGNING_KEYS,
	parseRegistryLines,
	registryPrefixError,
	registryPrefixesError,
	signingKeyNameError,
	signingKeyPemError
} from '../src/lib/imagePolicyForm';

// The Image policy page's prefilter restates internal/task.ValidRegistryPrefix
// and the signing-key rules over literals copied from the Go vectors
// (internal/task/image_policy_vectors.go, image_policy_test.go). The engine
// is the authority; these only save a round trip.

// FIXED_KEY / FIXED_FINGERPRINT are task.FixedSigningKeyPEM and
// task.FixedSigningKeyFingerprint, copied as literals.
const FIXED_KEY = `-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEu53TEXjEgvYqzDzOZvb8ZJcT+95O
VM++cNZJJfWL6BwbqKs4tIX+kOtqfQqOF0yrl/Wh6uZ/Za70t1qHI/xFfQ==
-----END PUBLIC KEY-----
`;
const FIXED_FINGERPRINT = 'sha256:f5a5f53127e68edd8110a0d95cb84f3a2832c15af3833f64da493fcf23306c41';

test('parseRegistryLines drops blanks and trims', () => {
	expect(parseRegistryLines('  ghcr.io/acme/  \n\n localhost:5001/orkstack/\n   \n')).toEqual([
		'ghcr.io/acme/',
		'localhost:5001/orkstack/'
	]);
	expect(parseRegistryLines('')).toEqual([]);
});

test('registryPrefixError agrees with the Go refusal rows', () => {
	// The shared vectors' invalid rows, and TestValidRegistryPrefix_RefusesEveryMalformedShape.
	const refused: [string, string][] = [
		['GHCR.io/acme/', 'not lower-case'],
		['acme/', 'write docker.io/acme/'],
		['ghcr.io/acme/*', '"*"'],
		['ghcr.io/acme@sha256:' + '0'.repeat(64), '"@"'],
		['ghcr.io/acme/x:v1', '":" after the host'],
		['ghcr io/', 'whitespace'],
		['ghcr.io/' + 'a'.repeat(256 - 'ghcr.io/'.length), '256 bytes'],
		['', 'empty']
	];
	for (const [entry, why] of refused) {
		const err = registryPrefixError(entry);
		expect(err === null).toBe(false);
		expect(err!).toContain(why);
	}
	for (const ok of ['ghcr.io/acme/', 'ghcr.io/acme', 'localhost:5001/', 'localhost/x', 'docker.io/library/', '123.dkr.ecr.eu-west-1.amazonaws.com/']) {
		expect(registryPrefixError(ok)).toBeNull();
	}
});

test('registryPrefixesError bounds the list and refuses a trailing-slash duplicate', () => {
	const many = Array.from({ length: MAX_ALLOWED_REGISTRIES }, (_, i) => `ghcr.io/team${i}/`);
	expect(registryPrefixesError(many)).toBeNull();
	expect(registryPrefixesError([...many, 'ghcr.io/one-more/'])).toContain('65 allowed registries');
	expect(registryPrefixesError(['ghcr.io/acme', 'ghcr.io/acme/'])).toContain('line 2');
});

test('signingKeyNameError is the slug rule bounded at 64 bytes', () => {
	expect(signingKeyNameError('acme-release')).toBeNull();
	expect(signingKeyNameError('Acme')).toContain('must match');
	expect(signingKeyNameError('a'.repeat(65))).toContain('65 bytes');
	expect(signingKeyNameError('')).toContain('required');
	expect(MAX_SIGNING_KEYS).toBe(16);
});

test('signingKeyPemError accepts a PUBLIC KEY block and refuses an EC PRIVATE KEY block', () => {
	expect(signingKeyPemError(FIXED_KEY)).toBeNull();
	expect(signingKeyPemError('-----BEGIN EC PRIVATE KEY-----\nMHcCAQEE\n-----END EC PRIVATE KEY-----\n')).toContain(
		'"EC PRIVATE KEY"'
	);
	expect(signingKeyPemError('hello')).toContain('not a single PEM block');
	expect(signingKeyPemError(FIXED_KEY + ' '.repeat(4097))).toContain('bytes of PEM');
});

test('fingerprint of the fixed key equals the Go literal', async () => {
	expect(await fingerprint(FIXED_KEY)).toBe(FIXED_FINGERPRINT);
	expect(await fingerprint('not a pem')).toBeNull();
});
