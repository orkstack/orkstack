// Admin → Usage, paged: one GetUsage per namespace ON THE PAGE, never one per
// namespace in the deployment.
//
// The page used to read every namespace and then ask each one's usage, four
// at a time, the moment it opened — a deployment with two hundred namespaces
// paid two hundred usage computations for a table a reader sees fifty rows
// of. It now pages and filters the namespace list exactly as Admin →
// Namespaces does (ListNamespaces with page_size, page_token and the one
// structured filter), and measures only the rows the page shows.
import { durationFromMs, type Duration } from '@bufbuild/protobuf/wkt';
import type { FieldFilter } from './listFilter';

/** Rows per page unless the address asks for another of the offered sizes. */
export const ADMIN_USAGE_PAGE_SIZE = 50;

/** How many GetUsage calls are in flight at once. */
export const ADMIN_USAGE_FAN_OUT = 4;

/** The page sizes offered; anything else is the default. */
export function usagePageSize(value: string | null): number {
	const parsed = Number(value);
	return [10, 25, 50, 100].includes(parsed) ? parsed : ADMIN_USAGE_PAGE_SIZE;
}

export const USAGE_WINDOW_MS: Record<string, number> = {
	'24h': 24 * 3_600_000,
	'168h': 168 * 3_600_000,
	'720h': 720 * 3_600_000
};

/** The two reads the page makes, as narrow as a test double needs. */
export interface UsageReads {
	listNamespaces(req: {
		pageSize: number;
		pageToken: string;
		filters: FieldFilter[];
	}): Promise<{ namespaces: { name: string }[]; nextPageToken: string }>;
	getUsage(req: { namespace: string; lookback: Duration }): Promise<unknown>;
}

export type UsageRow<T> = { loading: boolean; resp: T | null; error: string | null };

/**
 * measureUsage asks each named namespace's usage, ADMIN_USAGE_FAN_OUT at a
 * time, reporting each row as it lands. `current` lets a superseded
 * measurement (a new page, a new window) stop writing rows.
 */
export async function measureUsage<T>(
	names: string[],
	window: string,
	getUsage: (req: { namespace: string; lookback: Duration }) => Promise<T>,
	onRow: (name: string, row: UsageRow<T>) => void,
	describe: (err: unknown) => string,
	current: () => boolean = () => true
): Promise<void> {
	let next = 0;
	const lookback = durationFromMs(USAGE_WINDOW_MS[window] ?? USAGE_WINDOW_MS['24h']);
	async function worker() {
		while (next < names.length) {
			const name = names[next++];
			try {
				const resp = await getUsage({ namespace: name, lookback });
				if (!current()) return;
				onRow(name, { loading: false, resp, error: null });
			} catch (err) {
				if (!current()) return;
				onRow(name, { loading: false, resp: null, error: describe(err) });
			}
		}
	}
	await Promise.all(Array.from({ length: Math.min(ADMIN_USAGE_FAN_OUT, names.length) }, worker));
}
