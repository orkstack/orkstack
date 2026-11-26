import { expect, test } from 'bun:test';
import { load } from '../src/routes/admin/concurrency/+page';

// The Concurrency page's load: a failed read is an error, never an empty list
// or an unused key. GetConcurrencyLimit answers NOT_FOUND for an unused key
// and for a namespace that does not exist, so the page confirms the namespace
// before it says a key is unused.

type Answer = { status: number; body: unknown };
const ok = (body: unknown): Answer => ({ status: 200, body });
const refused = (status: number, code: string, message: string): Answer => ({ status, body: { code, message } });
const unused = refused(404, 'not_found', 'no run holds or waits for concurrency key k in namespace "payments"');

async function read(search: string, answers: Record<string, Answer>) {
	const calls: { method: string; body: unknown }[] = [];
	const fetchFn = (async (input: RequestInfo | URL, init?: RequestInit) => {
		const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
		const method = url.slice(url.lastIndexOf('/') + 1);
		const raw = init?.body;
		const text = raw instanceof Uint8Array ? new TextDecoder().decode(raw) : String(raw ?? '{}');
		calls.push({ method, body: JSON.parse(text) });
		const answer = answers[method];
		if (!answer) throw new Error(`${method} not stubbed`);
		return new Response(JSON.stringify(answer.body), { status: answer.status, headers: { 'content-type': 'application/json' } });
	}) as typeof fetch;
	const loaded = (await (load as unknown as (e: unknown) => Promise<unknown>)({
		fetch: fetchFn,
		url: new URL(`http://ui.test/admin/concurrency${search}`)
	})) as { key: string; limit: { key: string } | null; list: { limits: unknown[] } | null; error: string | null };
	return { ...loaded, calls };
}

test('one key is read by itself', async () => {
	const got = await read('?ns=payments&key=k', { GetConcurrencyLimit: ok({ limit: { key: 'k', answering: true } }) });
	expect([got.limit?.key, got.list, got.error]).toEqual(['k', null, null]);
	expect(got.calls).toEqual([{ method: 'GetConcurrencyLimit', body: { namespace: 'payments', key: 'k' } }]);
});

test('a key nothing uses, in a namespace that is there, is an answer', async () => {
	const got = await read('?ns=payments&key=k', { GetConcurrencyLimit: unused, ListConcurrencyLimits: ok({}) });
	expect([got.key, got.limit, got.error]).toEqual(['k', null, null]);
	// The confirmation names the namespace and no filter: a key= list would answer empty for a missing one.
	expect(got.calls[1]).toEqual({ method: 'ListConcurrencyLimits', body: { namespace: 'payments', pageSize: 1 } });
});

test('a namespace that does not exist is an error, never an unused key', async () => {
	const gone = refused(404, 'not_found', 'namespace "paymnets" does not exist');
	const got = await read('?ns=paymnets&key=k', { GetConcurrencyLimit: gone, ListConcurrencyLimits: gone });
	expect(got.limit).toBeNull();
	expect(got.error).toContain('paymnets');
});

test('a key that cannot be confirmed unused is an error', async () => {
	const got = await read('?ns=payments&key=k', { GetConcurrencyLimit: unused, ListConcurrencyLimits: refused(503, 'unavailable', 'try again') });
	expect(got.error).toContain('try again');
});

test('any other failure of one key is its error, and asks nothing more', async () => {
	const got = await read('?ns=payments&key=k', { GetConcurrencyLimit: refused(503, 'unavailable', 'not wired') });
	expect(got.error).toContain('not wired');
	expect(got.calls.length).toBe(1);
	expect((await read('?ns=payments&key=k', { GetConcurrencyLimit: ok({}) })).error).toContain('came back empty');
});

test('a failed list is an error, and a filter that does not parse is one without a request', async () => {
	const failed = await read('?ns=payments', { ListConcurrencyLimits: refused(503, 'unavailable', 'try again') });
	expect([failed.list, failed.error?.includes('try again')]).toEqual([null, true]);
	const bad = await read('?ns=payments&filter=nonsense', {});
	expect(bad.error).not.toBeNull();
	expect(bad.calls).toEqual([]);
});

test('the list is read with its filters and cursor', async () => {
	const got = await read('?ns=payments&filter=key%5Edb-&cursor=tok&page=2', { ListConcurrencyLimits: ok({ limits: [{ key: 'db-a', answering: true }] }) });
	expect(got.list?.limits.length).toBe(1);
	expect(got.calls[0].body).toEqual({ namespace: 'payments', filters: [{ field: 'key', op: 'FILTER_OPERATOR_PREFIX', values: ['db-'] }], pageToken: 'tok' });
});
