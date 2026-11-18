import { expect, test } from 'bun:test';
import { generateKeyPair, keyFileName, thumbprint } from '../src/lib/keypair';

test('generateKeyPair makes a P-256 pair as PEM: SPKI public, PKCS #8 private', async () => {
	const { publicPem, privatePem } = await generateKeyPair();
	expect(publicPem.startsWith('-----BEGIN PUBLIC KEY-----\n')).toBe(true);
	expect(privatePem.startsWith('-----BEGIN PRIVATE KEY-----\n')).toBe(true);
	const der = Uint8Array.from(atob(publicPem.split('\n').slice(1, -2).join('')), (c) => c.charCodeAt(0));
	const key = await crypto.subtle.importKey('spki', der, { name: 'ECDSA', namedCurve: 'P-256' }, true, ['verify']);
	expect(key.type).toBe('public');
});

test('thumbprint is RFC 7638’s, as go-jose computes it for the engine', async () => {
	// RFC 7515 A.3's P-256 key; the value is what internal/jwt's Thumbprint gives.
	const jwk = { kty: 'EC', crv: 'P-256', x: 'f83OJ3D2xF1Bg8vub9tLe1gHMzV76e8Tus9uPHvRVEU', y: 'x_FEzRu9m36HLN_tue659LNpXW6pCyStikYjKIWI5a0' };
	expect(await thumbprint(jwk)).toBe('oKIywvGUpTVTyxMQ3bwIIeQUudfr_CkLMjCE19ECD-U');
});

test('generateKeyPair names its key by the public half’s thumbprint', async () => {
	const { publicPem, kid } = await generateKeyPair();
	const der = Uint8Array.from(atob(publicPem.split('\n').slice(1, -2).join('')), (c) => c.charCodeAt(0));
	const key = await crypto.subtle.importKey('spki', der, { name: 'ECDSA', namedCurve: 'P-256' }, true, ['verify']);
	expect(kid).toBe(await thumbprint(await crypto.subtle.exportKey('jwk', key)));
	expect(/^[A-Za-z0-9_-]{43}$/.test(kid)).toBe(true);
});

test('keyFileName tells two keys of one account apart', () => {
	expect(keyFileName('deployer', 'oKIywvGUpTVTyxMQ3bwIIeQUudfr_CkLMjCE19ECD-U')).toBe('deployer-oKIywvGUpTVT.pem');
	expect(keyFileName('deployer', 'abc')).not.toBe(keyFileName('deployer', 'abd'));
});
