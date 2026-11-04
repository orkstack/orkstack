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
import type { ActivityInfo } from './gen/orkstack/v1/activity_service_pb';

export interface ActivityReads {
	listActivities(request: Record<string, never>): Promise<{ activities: ActivityInfo[] }>;
	inspectActivity(request: { reference: string; allowCached: boolean }): Promise<{ activity?: ActivityInfo }>;
}

/** The fields of a task this reads. */
export interface ActivityTask {
	id: string;
	uses: string;
	image?: { pinned: string; indexDigest: string };
	orkflow?: unknown;
	await?: unknown;
}

let catalog: Promise<ActivityInfo[]> | null = null;
const inspected = new Map<string, Promise<ActivityInfo | null>>();

function readCatalog(reads: ActivityReads): Promise<ActivityInfo[]> {
	if (!catalog) {
		catalog = reads.listActivities({}).then(
			(res) => res.activities,
			() => {
				catalog = null;
				return [];
			}
		);
	}
	return catalog;
}

function inspect(reads: ActivityReads, reference: string): Promise<ActivityInfo | null> {
	let answer = inspected.get(reference);
	if (!answer) {
		answer = reads.inspectActivity({ reference, allowCached: true }).then(
			(res) => res.activity ?? null,
			() => {
				inspected.delete(reference);
				return null;
			}
		);
		inspected.set(reference, answer);
	}
	return answer;
}

/** The catalog name in a `uses:` — the part before a tag or a digest. */
export function catalogName(uses: string): string {
	return uses.split(/[@:]/)[0];
}

const CONCURRENT_READS = 4;

/**
 * What each container task's image declares, by task id. A composed or awaited
 * task runs no image of its own and is absent; so is a task whose image could
 * not be read.
 */
export async function loadActivities(tasks: readonly ActivityTask[], reads: ActivityReads): Promise<Map<string, ActivityInfo>> {
	const containers = tasks.filter((task) => task.orkflow === undefined && task.await === undefined);
	const out = new Map<string, ActivityInfo>();
	if (containers.length === 0) return out;
	const known = await readCatalog(reads);
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
		for (let pin = pins.shift(); pin !== undefined; pin = pins.shift()) {
			const info = await inspect(reads, pin);
			if (info) for (const id of pending.get(pin) ?? []) out.set(id, info);
		}
	};
	await Promise.all(Array.from({ length: Math.min(CONCURRENT_READS, pins.length) }, worker));
	return out;
}

/** Test seam: forgets every answer. */
export function forgetActivities(): void {
	catalog = null;
	inspected.clear();
}
