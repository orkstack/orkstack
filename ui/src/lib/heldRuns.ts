// Changed-run marks: a run whose status changed since this browser last saw
// it keeps its alert in the runs list until it is opened.
//
// The state is THIS BROWSER'S only (localStorage, `ork-runs-seen-v1`): a
// per-viewer presentation of the statuses `orkctl run list` prints, never a
// record anyone else reads. Every read and write is wrapped, so a blocked or
// corrupt store holds nothing and breaks nothing.
//
// A listing row is only ever RUNNING, SUCCEEDED, FAILED or CANCELED, and a run
// only moves from RUNNING to one of the other three, so a held run is always a
// finished one. A run that starts waiting on an approval stays RUNNING in its
// row (AWAITING is a task status), so it cannot raise a mark here.
//
// The rules, per namespace:
//   1. the first visit sets the watermark to now and holds nothing;
//   2. an unknown listed run is recorded as seen at its status — unless it was
//      created after the watermark and FAILED: then it is recorded as never
//      seen, and is held;
//   3. a recorded run whose status differs from the status it was seen at is
//      held (its current status is kept, the seen one is not moved);
//   4. opening the run page records it seen at its status;
//   5. Mark all seen records every held run seen at its current status;
//   6. the watermark advances to now at the end of each list visit (a poll
//      while the list is open is not a new visit).

export const HELD_KEY = 'ork-runs-seen-v1';
export const MAX_RUNS_PER_NAMESPACE = 500;
/** The status a run "was seen at" when it was never seen: it failed while you were away. */
export const NEVER_SEEN = 'UNSPECIFIED';

export interface RunRecord {
	/** The status this browser last saw the run at (NEVER_SEEN for never). */
	seen: string;
	/** The status the list last showed. */
	now: string;
	orkflow: string;
	/** When this record was last touched, for eviction. */
	t: number;
}

export interface NamespaceRecord {
	watermark: number;
	runs: Record<string, RunRecord>;
}

export type HeldStore = Record<string, NamespaceRecord>;

export interface ListedRun {
	id: string;
	/** The status label (RUNNING, FAILED, …). */
	status: string;
	createdMs: number;
	orkflow: string;
}

export interface HeldRun {
	id: string;
	seen: string;
	now: string;
	orkflow: string;
}

function evict(runs: Record<string, RunRecord>): Record<string, RunRecord> {
	const ids = Object.keys(runs);
	if (ids.length <= MAX_RUNS_PER_NAMESPACE) return runs;
	const kept = ids.sort((a, b) => runs[b].t - runs[a].t).slice(0, MAX_RUNS_PER_NAMESPACE);
	return Object.fromEntries(kept.map((id) => [id, runs[id]]));
}

/**
 * Applies a listing to the store (rules 1–3, 6) and returns the new store and
 * the rows that are held, mapped to the status each was seen at. A poll
 * (`{ poll: true }`) patches statuses and leaves the watermark where the
 * visit put it.
 */
export function observeList(
	store: HeldStore,
	namespace: string,
	rows: readonly ListedRun[],
	nowMs: number,
	opts: { poll?: boolean } = {}
): { store: HeldStore; held: Map<string, string> } {
	const existing = store[namespace];
	const watermark = existing?.watermark ?? nowMs;
	const runs: Record<string, RunRecord> = { ...(existing?.runs ?? {}) };
	const held = new Map<string, string>();
	for (const r of rows) {
		const known = runs[r.id];
		if (!known) {
			const failedAway = existing !== undefined && r.status === 'FAILED' && r.createdMs > watermark;
			runs[r.id] = { seen: failedAway ? NEVER_SEEN : r.status, now: r.status, orkflow: r.orkflow, t: nowMs };
		} else {
			runs[r.id] = { ...known, now: r.status, orkflow: r.orkflow || known.orkflow, t: nowMs };
		}
		if (runs[r.id].seen !== r.status) held.set(r.id, runs[r.id].seen);
	}
	const next: NamespaceRecord = { watermark: opts.poll && existing ? watermark : nowMs, runs: evict(runs) };
	return { store: { ...store, [namespace]: next }, held };
}

/** The status a run was seen at if it is held at `status`, else null. */
export function heldFrom(store: HeldStore, namespace: string, id: string, status: string): string | null {
	const rec = store[namespace]?.runs[id];
	return rec && rec.seen !== status ? rec.seen : null;
}

/** Rule 4: the run page records the run seen at its status. */
export function markSeen(store: HeldStore, namespace: string, id: string, status: string, orkflow: string, nowMs: number): HeldStore {
	const ns = store[namespace] ?? { watermark: nowMs, runs: {} };
	const runs = { ...ns.runs, [id]: { seen: status, now: status, orkflow: orkflow || ns.runs[id]?.orkflow || '', t: nowMs } };
	return { ...store, [namespace]: { ...ns, runs: evict(runs) } };
}

/** Rule 5. */
export function markAllSeen(store: HeldStore, namespace: string, nowMs: number): HeldStore {
	const ns = store[namespace];
	if (!ns) return store;
	const runs: Record<string, RunRecord> = {};
	for (const [id, rec] of Object.entries(ns.runs)) runs[id] = rec.seen === rec.now ? rec : { ...rec, seen: rec.now, t: nowMs };
	return { ...store, [namespace]: { ...ns, runs } };
}

/** Every held run of a namespace, most recently touched first. */
export function heldRuns(store: HeldStore, namespace: string): HeldRun[] {
	const ns = store[namespace];
	if (!ns) return [];
	return Object.entries(ns.runs)
		.filter(([, rec]) => rec.seen !== rec.now)
		.sort(([, a], [, b]) => b.t - a.t)
		.map(([id, rec]) => ({ id, seen: rec.seen, now: rec.now, orkflow: rec.orkflow }));
}

/** The words a held mark announces (as an attribute, never as text). */
export function heldLabel(seen: string): string {
	return seen === NEVER_SEEN
		? 'Changed since you last looked; it failed while you were away'
		: `Changed since you last looked; it was ${seen.toLowerCase()}`;
}

// ---- Storage ----

interface StorageLike {
	getItem(key: string): string | null;
	setItem(key: string, value: string): void;
}

function browserStorage(): StorageLike | undefined {
	try {
		return globalThis.localStorage ?? undefined;
	} catch {
		return undefined;
	}
}

function validRecord(v: unknown): v is RunRecord {
	if (!v || typeof v !== 'object') return false;
	const r = v as Record<string, unknown>;
	return typeof r.seen === 'string' && typeof r.now === 'string' && typeof r.orkflow === 'string' && typeof r.t === 'number';
}

/** Reads the store; anything unreadable or misshapen reads as empty. */
export function loadHeld(storage: StorageLike | undefined = browserStorage()): HeldStore {
	if (!storage) return {};
	try {
		const raw = storage.getItem(HELD_KEY);
		if (!raw) return {};
		const parsed: unknown = JSON.parse(raw);
		if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return {};
		const out: HeldStore = {};
		for (const [ns, value] of Object.entries(parsed as Record<string, unknown>)) {
			const rec = value as Record<string, unknown> | null;
			if (!rec || typeof rec.watermark !== 'number' || !rec.runs || typeof rec.runs !== 'object') return {};
			const runs: Record<string, RunRecord> = {};
			for (const [id, r] of Object.entries(rec.runs as Record<string, unknown>)) if (validRecord(r)) runs[id] = r;
			out[ns] = { watermark: rec.watermark, runs };
		}
		return out;
	} catch {
		return {};
	}
}

export function saveHeld(store: HeldStore, storage: StorageLike | undefined = browserStorage()): void {
	if (!storage) return;
	try {
		storage.setItem(HELD_KEY, JSON.stringify(store));
	} catch {
		// Storage is full or blocked: the marks live for this page only.
	}
}

/** Rule 4 against browser storage: what the run page calls. */
export function recordSeen(namespace: string, id: string, status: string, orkflow: string): void {
	saveHeld(markSeen(loadHeld(), namespace, id, status, orkflow, Date.now()));
}
