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
}

export async function generateKeyPair(subtle: SubtleCrypto = crypto.subtle): Promise<KeyPairPEM> {
	const pair = await subtle.generateKey({ name: 'ECDSA', namedCurve: 'P-256' }, true, ['sign', 'verify']);
	const [spki, pkcs8] = await Promise.all([subtle.exportKey('spki', pair.publicKey), subtle.exportKey('pkcs8', pair.privateKey)]);
	return { publicPem: pem('PUBLIC KEY', spki), privatePem: pem('PRIVATE KEY', pkcs8) };
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
