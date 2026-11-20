// What an image says about itself — its icon and its words — for the tasks of
// one Orkflow. The engine reads them from the image's labels
// (ActivityInfo.icon and .docs); a task names the image by its pinned digest,
// or by a catalog name while it has no pin.
//
// One answer per image per session: the catalog is read once, a pin the
// catalog does not hold is asked about once (InspectActivity with allow_cached,
// so a read never reaches the registry when the engine already knows), and a
// failed read is forgotten, so a later page asks again. Every doubt draws the
// default icon: this never fails a page.
//
// A read never outlives the pages waiting for it. Each holds one of the
// browser's few connections to the engine (six over HTTP/1.1), and reads
// nobody waited for any more once left the next page's own reads queued
// behind them (proof 44642268123). So every read carries a timeout of its
// own, pages that share it each pass their signal, and it is aborted when the
// last of them has gone; an aborted or timed-out read is forgotten like a
// failed one.
import type { ActivityInfo } from './gen/orkstack/v1/activity_service_pb';

/** How long one catalog or image read may take before it draws the default. */
export const ACTIVITY_READ_TIMEOUT_MS = 5_000;

type CallOptions = { signal?: AbortSignal; timeoutMs?: number };

export interface ActivityReads {
	listActivities(request: Record<string, never>, options?: CallOptions): Promise<{ activities: ActivityInfo[] }>;
	inspectActivity(request: { reference: string; allowCached: boolean }, options?: CallOptions): Promise<{ activity?: ActivityInfo }>;
}

/** The fields of a task this reads. */
export interface ActivityTask {
	id: string;
	uses: string;
	image?: { pinned: string; indexDigest: string };
	orkflow?: unknown;
	await?: unknown;
	sleep?: unknown;
}

/** One read shared by the pages waiting for it. */
interface Shared<T> {
	answer: Promise<T>;
	controller: AbortController;
	waiting: number;
	settled: boolean;
}

/**
 * share starts a read, or joins the one in `cache` under `key`, for a page
 * whose signal says when it stops waiting. The read is aborted once every
 * page that joined it has stopped; a failure, a timeout or an abort is
 * forgotten (it answers `fallback`), so a later page asks again.
 */
function share<T>(cache: Map<string, Shared<T>>, key: string, signal: AbortSignal | undefined, start: (options: CallOptions) => Promise<T>, fallback: T): Promise<T> {
	let entry = cache.get(key);
	if (!entry) {
		const controller = new AbortController();
		const created: Shared<T> = { answer: Promise.resolve(fallback), controller, waiting: 0, settled: false };
		created.answer = start({ signal: controller.signal, timeoutMs: ACTIVITY_READ_TIMEOUT_MS }).then(
			(value) => {
				created.settled = true;
				return value;
			},
			() => {
				created.settled = true;
				if (cache.get(key) === created) cache.delete(key);
				return fallback;
			}
		);
		cache.set(key, created);
		entry = created;
	}
	if (entry.settled) return entry.answer;
	const joined = entry;
	joined.waiting++;
	if (!signal) return joined.answer;
	if (signal.aborted) {
		leave(cache, key, joined);
		return Promise.resolve(fallback);
	}
	return new Promise<T>((resolve) => {
		const stop = () => {
			leave(cache, key, joined);
			resolve(fallback);
		};
		signal.addEventListener('abort', stop, { once: true });
		void joined.answer.then((value) => {
			signal.removeEventListener('abort', stop);
			resolve(value);
		});
	});
}

function leave<T>(cache: Map<string, Shared<T>>, key: string, entry: Shared<T>): void {
	entry.waiting--;
	if (entry.waiting > 0 || entry.settled) return;
	if (cache.get(key) === entry) cache.delete(key);
	entry.controller.abort();
}

const catalog = new Map<string, Shared<ActivityInfo[]>>();
const inspected = new Map<string, Shared<ActivityInfo | null>>();

function readCatalog(reads: ActivityReads, signal: AbortSignal | undefined): Promise<ActivityInfo[]> {
	return share(catalog, '', signal, (options) => reads.listActivities({}, options).then((res) => res.activities), []);
}

function inspect(reads: ActivityReads, reference: string, signal: AbortSignal | undefined): Promise<ActivityInfo | null> {
	return share(inspected, reference, signal, (options) => reads.inspectActivity({ reference, allowCached: true }, options).then((res) => res.activity ?? null), null);
}

/** The catalog name in a `uses:` — the part before a tag or a digest. */
export function catalogName(uses: string): string {
	return uses.split(/[@:]/)[0];
}

const CONCURRENT_READS = 4;

/**
 * What each container task's image declares, by task id. A composed, awaited
 * or sleeping task runs no image of its own and is absent; so is a task whose
 * image could not be read.
 */
export async function loadActivities(tasks: readonly ActivityTask[], reads: ActivityReads, signal?: AbortSignal): Promise<Map<string, ActivityInfo>> {
	const containers = tasks.filter(
		(task) => task.orkflow === undefined && task.await === undefined && task.sleep === undefined
	);
	const out = new Map<string, ActivityInfo>();
	if (containers.length === 0) return out;
	const known = await readCatalog(reads, signal);
	const byDigest = new Map(known.filter((a) => a.indexDigest).map((a) => [a.indexDigest, a]));
	const byName = new Map(known.map((a) => [a.name, a]));

	const pending = new Map<string, string[]>();
	for (const task of containers) {
		const digest = task.image?.indexDigest ?? '';
		const pin = task.image?.pinned ?? '';
		const fromCatalog = (digest && byDigest.get(digest)) || (!pin ? byName.get(catalogName(task.uses)) : undefined);
		if (fromCatalog) out.set(task.id, fromCatalog);
		else if (pin) {
			const ids = pending.get(pin);
			if (ids) ids.push(task.id);
			else pending.set(pin, [task.id]);
		}
	}
	const pins = [...pending.keys()];
	const worker = async () => {
		for (let pin = pins.shift(); pin !== undefined && !signal?.aborted; pin = pins.shift()) {
			const info = await inspect(reads, pin, signal);
			if (info) for (const id of pending.get(pin) ?? []) out.set(id, info);
		}
	};
	await Promise.all(Array.from({ length: Math.min(CONCURRENT_READS, pins.length) }, worker));
	return out;
}

/** Test seam: forgets every answer. */
export function forgetActivities(): void {
	catalog.clear();
	inspected.clear();
}
