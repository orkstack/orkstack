import { expect, test } from 'bun:test';
import { ADMIN_USAGE_FAN_OUT, ADMIN_USAGE_PAGE_SIZE, measureUsage, usagePageSize } from '../src/lib/adminUsage';

// F-ui-perf-8: Admin → Usage asks one GetUsage per namespace on the PAGE, not
// one per namespace in the deployment.

const ALL = Array.from({ length: 200 }, (_, i) => `ns-${String(i).padStart(3, '0')}`);

// ListNamespaces as the registry pages it: page_size rows from page_token on.
function listNamespaces(req: { pageSize: number; pageToken: string }) {
	const from = req.pageToken ? Number(req.pageToken) : 0;
	const page = ALL.slice(from, from + req.pageSize);
	const next = from + req.pageSize < ALL.length ? String(from + req.pageSize) : '';
	return { namespaces: page.map((name) => ({ name })), nextPageToken: next };
}

test('the page size is 50 unless the address asks for an offered one', () => {
	expect(ADMIN_USAGE_PAGE_SIZE).toBe(50);
	expect(usagePageSize(null)).toBe(50);
	expect(usagePageSize('100')).toBe(100);
	expect(usagePageSize('7')).toBe(50);
});

test('200 namespaces cost 50 GetUsage calls on page 1, and page 2 asks its own 50', async () => {
	const asked: string[] = [];
	let inFlight = 0;
	let widest = 0;
	const getUsage = async (req: { namespace: string }) => {
		asked.push(req.namespace);
		inFlight++;
		widest = Math.max(widest, inFlight);
		await Promise.resolve();
		inFlight--;
		return { namespace: req.namespace };
	};
	const rows: Record<string, unknown> = {};
	const first = listNamespaces({ pageSize: usagePageSize(null), pageToken: '' });
	await measureUsage(first.namespaces.map((n) => n.name), '24h', getUsage, (name, row) => (rows[name] = row), String);
	expect(asked).toHaveLength(50);
	expect(asked).toEqual(ALL.slice(0, 50));
	expect(Object.keys(rows)).toHaveLength(50);
	expect(widest <= ADMIN_USAGE_FAN_OUT).toBe(true);

	asked.length = 0;
	const second = listNamespaces({ pageSize: 50, pageToken: first.nextPageToken });
	await measureUsage(second.namespaces.map((n) => n.name), '24h', getUsage, () => {}, String);
	expect(asked).toEqual(ALL.slice(50, 100));
});

test('a failing namespace is its own row, and a superseded measurement writes nothing', async () => {
	const rows: Record<string, { error: string | null }> = {};
	await measureUsage(
		['a', 'b'],
		'168h',
		async (req) => {
			if (req.namespace === 'b') throw new Error('store unavailable');
			return { ok: true };
		},
		(name, row) => (rows[name] = row),
		(err) => (err as Error).message
	);
	expect(rows.a.error).toBeNull();
	expect(rows.b.error).toBe('store unavailable');

	let written = 0;
	await measureUsage(['a', 'b', 'c'], '24h', async () => ({}), () => written++, String, () => false);
	expect(written).toBe(0);
});
