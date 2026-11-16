import { expect, test } from 'bun:test';
import { generateKeyPair } from '../src/lib/keypair';

test('generateKeyPair makes a P-256 pair as PEM: SPKI public, PKCS #8 private', async () => {
	const { publicPem, privatePem } = await generateKeyPair();
	expect(publicPem.startsWith('-----BEGIN PUBLIC KEY-----\n')).toBe(true);
	expect(privatePem.startsWith('-----BEGIN PRIVATE KEY-----\n')).toBe(true);
	const der = Uint8Array.from(atob(publicPem.split('\n').slice(1, -2).join('')), (c) => c.charCodeAt(0));
	const key = await crypto.subtle.importKey('spki', der, { name: 'ECDSA', namedCurve: 'P-256' }, true, ['verify']);
	expect(key.type).toBe('public');
});
