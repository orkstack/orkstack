// A service account's key pair, made in the browser (UX35: "a private key
// the UI generates for a service account is offered once, as a download").
// ECDSA P-256, the private half as PKCS #8 PEM, never kept by the page.

function pem(label: string, der: ArrayBuffer): string {
	const b64 = btoa(String.fromCharCode(...new Uint8Array(der)));
	const lines = b64.match(/.{1,64}/g) ?? [];
	return `-----BEGIN ${label}-----\n${lines.join('\n')}\n-----END ${label}-----\n`;
}

export interface KeyPairPEM {
	publicPem: string;
	privatePem: string;
	/** The key's id, as the engine will name it: its RFC 7638 thumbprint. */
	kid: string;
}

const b64url = (buf: ArrayBuffer) => btoa(String.fromCharCode(...new Uint8Array(buf))).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');

/** thumbprint is an EC public key's RFC 7638 thumbprint (SHA-256, base64url):
 * the hash of its required members in lexicographic order, no whitespace. */
export async function thumbprint(jwk: { crv?: string; x?: string; y?: string }, subtle: SubtleCrypto = crypto.subtle): Promise<string> {
	const canonical = JSON.stringify({ crv: jwk.crv, kty: 'EC', x: jwk.x, y: jwk.y });
	return b64url(await subtle.digest('SHA-256', new TextEncoder().encode(canonical)));
}

export async function generateKeyPair(subtle: SubtleCrypto = crypto.subtle): Promise<KeyPairPEM> {
	const pair = await subtle.generateKey({ name: 'ECDSA', namedCurve: 'P-256' }, true, ['sign', 'verify']);
	const [spki, pkcs8, jwk] = await Promise.all([subtle.exportKey('spki', pair.publicKey), subtle.exportKey('pkcs8', pair.privateKey), subtle.exportKey('jwk', pair.publicKey)]);
	return { publicPem: pem('PUBLIC KEY', spki), privatePem: pem('PRIVATE KEY', pkcs8), kid: await thumbprint(jwk, subtle) };
}

/** keyFileName names a private key's file by its account and the start of
 * its id, so two keys of one account never share a name. */
export function keyFileName(account: string, kid: string): string {
	return `${account}-${kid.slice(0, 12)}.pem`;
}

/** offerDownload hands the browser a file to save, then lets go of it. */
export function offerDownload(name: string, text: string): void {
	const url = URL.createObjectURL(new Blob([text], { type: 'application/x-pem-file' }));
	const a = document.createElement('a');
	a.href = url;
	a.download = name;
	a.click();
	// Later than the click: some browsers start the download asynchronously.
	setTimeout(() => URL.revokeObjectURL(url), 1000);
}
