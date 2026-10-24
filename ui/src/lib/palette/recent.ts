// Recently opened runs and Orkflows, for the palette's RECENT group.
//
// This browser only, in localStorage, and only ids and names: never a run's
// inputs (docs/UI_CLI_PARITY.md: run inputs never enter a shareable URL, and
// a palette history is no place for them either). Every read and write is
// wrapped: a browser that refuses storage simply has no recents.

export const RECENT_KEY = 'ork-palette-recent-v1';
export const RECENT_LIMIT = 8;

export interface Recent {
	kind: 'run' | 'orkflow';
	/** A run id or an Orkflow name. */
	id: string;
	namespace: string;
	/** For a run: its Orkflow's name, if known. */
	orkflow?: string;
	at: number;
}

interface StorageLike {
	getItem(key: string): string | null;
	setItem(key: string, value: string): void;
}

function storage(): StorageLike | null {
	try {
		return globalThis.localStorage ?? null;
	} catch {
		return null;
	}
}

function clean(value: unknown): Recent[] {
	if (!Array.isArray(value)) return [];
	const out: Recent[] = [];
	for (const v of value) {
		if (!v || typeof v !== 'object') continue;
		const r = v as Record<string, unknown>;
		if ((r.kind !== 'run' && r.kind !== 'orkflow') || typeof r.id !== 'string' || typeof r.namespace !== 'string') continue;
		out.push({
			kind: r.kind,
			id: r.id,
			namespace: r.namespace,
			orkflow: typeof r.orkflow === 'string' ? r.orkflow : undefined,
			at: typeof r.at === 'number' ? r.at : 0
		});
	}
	return out.slice(0, RECENT_LIMIT);
}

export function readRecents(store: StorageLike | null = storage()): Recent[] {
	try {
		const raw = store?.getItem(RECENT_KEY);
		return raw ? clean(JSON.parse(raw)) : [];
	} catch {
		return [];
	}
}

/** Records an opening, newest first, at most RECENT_LIMIT, and only the fields of Recent. */
export function rememberRecent(entry: Omit<Recent, 'at'>, now = Date.now(), store: StorageLike | null = storage()): void {
	const fresh: Recent = { kind: entry.kind, id: entry.id, namespace: entry.namespace, orkflow: entry.orkflow, at: now };
	const rest = readRecents(store).filter((r) => !(r.kind === fresh.kind && r.id === fresh.id && r.namespace === fresh.namespace));
	try {
		store?.setItem(RECENT_KEY, JSON.stringify([fresh, ...rest].slice(0, RECENT_LIMIT)));
	} catch {
		// Storage refused: no recents this time.
	}
}
