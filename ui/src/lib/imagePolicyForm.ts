// The Admin → Image policy form's local validation, and the key fingerprint
// both clients print.
//
// This is a PREFILTER, never a gate, exactly as $lib/retentionForm.ts is: the
// engine (internal/task.ValidRegistryPrefix, ValidSigningKeys) is the
// authority, re-validates every entry, and its refusal is shown verbatim. The
// rules are restated here because there is nothing to import across the
// browser/server boundary — TypeScript here, Go there — over the literal
// vectors ui/test/imagePolicyForm.test.ts copies from
// internal/task/image_policy_vectors.go (AGENTS.md §7: one rule, restated at
// each boundary over shared vectors).

// MAX_ALLOWED_REGISTRIES, MAX_ALLOWED_REGISTRY_BYTES, MAX_SIGNING_KEYS,
// MAX_SIGNING_KEY_PEM_BYTES and MAX_SIGNING_KEY_NAME_BYTES mirror
// internal/task's constants of the same names.
export const MAX_ALLOWED_REGISTRIES = 64;
export const MAX_ALLOWED_REGISTRY_BYTES = 255;
export const MAX_SIGNING_KEYS = 16;
export const MAX_SIGNING_KEY_PEM_BYTES = 4096;
export const MAX_SIGNING_KEY_NAME_BYTES = 64;

// SIGNING_KEY_NAME_PATTERN is the slug rule every OrkStack name follows.
export const SIGNING_KEY_NAME_PATTERN = /^[a-z][a-z0-9-]*$/;

/** parseRegistryLines turns the textarea into the list a request carries:
 * one prefix per line, trimmed, blank lines dropped. Blank = unrestricted. */
export function parseRegistryLines(text: string): string[] {
	return text
		.split('\n')
		.map((line) => line.trim())
		.filter((line) => line.length > 0);
}

/** registryPrefixError restates internal/task.ValidRegistryPrefix: why one
 * allowed_registries entry cannot be stored, or null. */
export function registryPrefixError(entry: string): string | null {
	const quoted = JSON.stringify(entry);
	if (entry === '') {
		return 'allowed registry entry is empty; write a {registry}/{repository} prefix such as ghcr.io/acme/';
	}
	const bytes = new TextEncoder().encode(entry).length;
	if (bytes > MAX_ALLOWED_REGISTRY_BYTES) {
		return `allowed registry ${quoted} is ${bytes} bytes; the limit is ${MAX_ALLOWED_REGISTRY_BYTES}`;
	}
	for (const ch of entry) {
		const code = ch.codePointAt(0) ?? 0;
		if (/\s/u.test(ch)) return `allowed registry ${quoted} contains whitespace`;
		if (code < 0x20 || code === 0x7f) return `allowed registry ${quoted} contains a control character`;
		if (ch === '*') {
			return `allowed registry ${quoted} contains "*"; entries are literal prefixes matched on "/" boundaries, with no wildcard`;
		}
		if (ch === '@') return `allowed registry ${quoted} contains "@"; an entry is a registry/repository prefix, never a digest`;
		if (ch === ',') return `allowed registry ${quoted} contains ","; give one prefix per line`;
	}
	if (entry.toLowerCase() !== entry) {
		return `allowed registry ${quoted} is not lower-case; registry hosts and OCI repository paths are, so write ${JSON.stringify(entry.toLowerCase())}`;
	}
	const trimmed = entry.endsWith('/') ? entry.slice(0, -1) : entry;
	if (trimmed === '') return `allowed registry ${quoted} names no registry`;
	const slash = trimmed.indexOf('/');
	const host = slash >= 0 ? trimmed.slice(0, slash) : trimmed;
	const rest = slash >= 0 ? trimmed.slice(slash + 1) : '';
	if (host === '') return `allowed registry ${quoted} begins with "/"; it must begin with a registry host`;
	if (!(host === 'localhost' || host.includes('.') || host.includes(':'))) {
		return `allowed registry ${quoted} does not begin with a registry host; a first segment with no "." or ":" is a Docker Hub namespace, so write docker.io/${entry}`;
	}
	if (rest.includes(':')) {
		return `allowed registry ${quoted} contains ":" after the host; an entry is a registry/repository prefix, never a tag`;
	}
	if (trimmed.includes('//')) return `allowed registry ${quoted} contains an empty path segment`;
	return null;
}

/** registryPrefixesError is internal/task.ValidRegistryPrefixes: the count,
 * then the first malformed entry, then the first duplicate (trailing "/"
 * ignored), naming the line. */
export function registryPrefixesError(entries: string[]): string | null {
	if (entries.length > MAX_ALLOWED_REGISTRIES) {
		return `${entries.length} allowed registries exceed the ${MAX_ALLOWED_REGISTRIES}-entry limit`;
	}
	const seen = new Map<string, number>();
	for (const [i, entry] of entries.entries()) {
		const err = registryPrefixError(entry);
		if (err) return `line ${i + 1}: ${err}`;
		const key = entry.endsWith('/') ? entry.slice(0, -1) : entry;
		const first = seen.get(key);
		if (first !== undefined) {
			return `line ${i + 1}: allowed registry ${JSON.stringify(entry)} duplicates line ${first + 1}`;
		}
		seen.set(key, i);
	}
	return null;
}

/** signingKeyNameError is internal/task.ValidSigningKeyName. */
export function signingKeyNameError(name: string): string | null {
	if (name === '') return 'signing key name is required';
	const bytes = new TextEncoder().encode(name).length;
	if (bytes > MAX_SIGNING_KEY_NAME_BYTES) {
		return `signing key name ${JSON.stringify(name)} is ${bytes} bytes; the limit is ${MAX_SIGNING_KEY_NAME_BYTES}`;
	}
	if (!SIGNING_KEY_NAME_PATTERN.test(name)) {
		return `signing key name ${JSON.stringify(name)} must match ${SIGNING_KEY_NAME_PATTERN}`;
	}
	return null;
}

const PEM_BLOCK = /^-----BEGIN ([A-Z0-9 ]+)-----\r?\n([A-Za-z0-9+/=\r\n]+?)\r?\n?-----END \1-----\s*$/;

/** signingKeyPemError accepts exactly one PEM "PUBLIC KEY" block of at most
 * MAX_SIGNING_KEY_PEM_BYTES. That it is an ECDSA P-256 key is the engine's
 * check (internal/task.ParseSigningKey); its refusal is shown verbatim. */
export function signingKeyPemError(pem: string): string | null {
	const text = pem.trim();
	if (text === '') return 'paste the PEM public key (cosign.pub)';
	const bytes = new TextEncoder().encode(pem).length;
	if (bytes > MAX_SIGNING_KEY_PEM_BYTES) {
		return `signing key is ${bytes} bytes of PEM; the limit is ${MAX_SIGNING_KEY_PEM_BYTES}`;
	}
	const match = PEM_BLOCK.exec(text);
	if (!match) return 'not a single PEM block; paste cosign.pub, which begins "-----BEGIN PUBLIC KEY-----"';
	if (match[1] !== 'PUBLIC KEY') {
		return `is a PEM ${JSON.stringify(match[1])} block; paste the PUBLIC KEY (cosign.pub) — OrkStack never holds a private key`;
	}
	return null;
}

function pemDer(pem: string): Uint8Array | null {
	const match = PEM_BLOCK.exec(pem.trim());
	if (!match) return null;
	const body = match[2].replace(/\s+/g, '');
	try {
		const binary = atob(body);
		const der = new Uint8Array(binary.length);
		for (let i = 0; i < binary.length; i++) der[i] = binary.charCodeAt(i);
		return der;
	} catch {
		return null;
	}
}

/** fingerprint is internal/task.SigningKeyFingerprint in the browser:
 * "sha256:<64 hex>" over the PEM's DER body (the SubjectPublicKeyInfo), so
 * this page and `orkctl admin image-policy get` print the same string.
 * Null for a PEM that does not decode. */
export async function fingerprint(pem: string): Promise<string | null> {
	const der = pemDer(pem);
	if (!der) return null;
	const digest = await crypto.subtle.digest('SHA-256', der as BufferSource);
	const hex = Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, '0')).join('');
	return `sha256:${hex}`;
}
