// Page providers: what the page you are on adds to the palette (jump to a
// task, Cancel run…, Submit a run…).
//
// A page registers ONE provider when it mounts and removes it when it is
// destroyed. Registering builds nothing: `commands(query)` runs only while
// the palette is open, once per query, and returns at most PROVIDER_LIMIT
// commands, so a run page of 4,000 tasks never holds a command per task.
// Providers are registered in the browser only (on mount), so a server render
// never sees another request's.
import type { Command } from './commands';

export const PROVIDER_LIMIT = 8;

export interface PaletteProvider {
	id: string;
	commands(query: string): Command[];
}

const providers = new Map<string, { provider: PaletteProvider; token: number }>();
let nextToken = 1;

/** Registers a provider and returns its removal, which removes only this registration. */
export function registerProvider(provider: PaletteProvider): () => void {
	const token = nextToken++;
	providers.set(provider.id, { provider, token });
	return () => {
		if (providers.get(provider.id)?.token === token) providers.delete(provider.id);
	};
}

/** Every registered provider's commands for this query, each provider capped. */
export function providedCommands(query: string): Command[] {
	const out: Command[] = [];
	for (const { provider } of providers.values()) {
		try {
			out.push(...provider.commands(query).slice(0, PROVIDER_LIMIT));
		} catch {
			// A provider that throws offers nothing; the palette still opens.
		}
	}
	return out;
}

/** For tests. */
export function providerCount(): number {
	return providers.size;
}
