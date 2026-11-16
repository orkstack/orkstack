// The deployment's authentication mode, as the UI reads it: GetSystemInfo's
// auth_mode. An engine that refuses that call without a credential
// authenticates. One that does not answer at all is read as authenticating
// too: the page then hides what it cannot show is allowed, rather than
// offering everything, and the server decides each request either way.
import { systemClient } from '#lib/clients/system.ts';

export type AuthMode = 'asserted' | 'observe' | 'enforce';

export async function readAuthMode(fetchFn: typeof fetch): Promise<AuthMode> {
	try {
		const mode = (await systemClient(fetchFn).getSystemInfo({})).authMode;
		return mode === 'observe' || mode === 'enforce' ? mode : 'asserted';
	} catch {
		// Unauthenticated (it authenticates), or no answer (read as if it did).
		return 'enforce';
	}
}

// Who the caller is, as the root layout last asked (GetSignin), kept for a
// few seconds: a navigation that only changes a list's filter does not ask
// again. forgetWho makes the next navigation ask.
const WHO_TTL_MS = 10_000;
let who: { at: number; value: unknown } | null = null;

export function cachedWho<T>(): T | null {
	return who && Date.now() - who.at < WHO_TTL_MS ? (who.value as T) : null;
}

export function rememberWho(value: unknown): void {
	who = { at: Date.now(), value };
}

export function forgetWho(): void {
	who = null;
}
