import { expect, test } from 'bun:test';
import { create, toJson } from '@bufbuild/protobuf';
import { Code, ConnectError } from '@connectrpc/connect';
import { GetRunResponseSchema } from '../src/lib/gen/orkstack/v1/service_pb';
import { TaskAttemptSchema } from '../src/lib/gen/orkstack/v1/run_pb';
import { readAttemptSeed } from '../src/lib/attempts';
import { load } from '../src/routes/runs/[id]/+page';

// F-ui-perf-1: the run page renders after GetRun. Its loader never asks for
// the run's attempt timeline — the page reads that once it has rendered — so
// a run whose timeline is slow (a thousand fan-out items) draws at once.

function connectFetch(calls: string[], answers: Record<string, unknown>): typeof fetch {
	return (async (input: RequestInfo | URL) => {
		const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
		const method = url.slice(url.lastIndexOf('/') + 1);
		calls.push(method);
		if (method === 'ListTaskAttempts') return new Promise<Response>(() => {}); // never answers
		const body = answers[method];
		if (body === undefined) {
			return new Response(JSON.stringify({ code: 'not_found', message: `${method} not stubbed` }), {
				status: 404,
				headers: { 'content-type': 'application/json' }
			});
		}
		return new Response(JSON.stringify(body), { headers: { 'content-type': 'application/json' } });
	}) as typeof fetch;
}

test('the loader answers from GetRun and never waits on the attempt timeline', async () => {
	const calls: string[] = [];
	const run = toJson(
		GetRunResponseSchema,
		create(GetRunResponseSchema, { run: { id: 'ork-1', orkflowName: 'fan', orkflowVersion: '0.1.0' } })
	);
	const fetchFn = connectFetch(calls, {
		GetRun: run,
		ListRunArtifacts: { artifacts: [] },
		GetOrkflow: {}
	});
	const loaded = (await (load as unknown as (e: unknown) => Promise<Record<string, unknown>>)({
		fetch: fetchFn,
		params: { id: 'ork-1' },
		url: new URL('http://ui.test/runs/ork-1?ns=default')
	})) as { run: { id: string } | null; error: string | null };
	expect(loaded.error).toBeNull();
	expect(loaded.run?.id).toBe('ork-1');
	expect(calls.includes('ListTaskAttempts')).toBe(false);
	expect('attempts' in loaded).toBe(false);
	expect(calls[0]).toBe('GetRun');
});

test('the seed is the whole run in one call, read tolerantly', async () => {
	const asked: unknown[] = [];
	const attempt = create(TaskAttemptSchema, { runId: 'ork-1', taskId: 'build', attempt: 1 });
	const ok = await readAttemptSeed(
		{ listTaskAttempts: async (req) => (asked.push(req), { attempts: [attempt] }) },
		'ork-1',
		'payments',
		String,
		() => false
	);
	expect(asked).toEqual([{ runId: 'ork-1', namespace: 'payments', taskId: '' }]);
	expect(ok.attempts).toHaveLength(1);
	expect(ok.error).toBeNull();

	const absent = await readAttemptSeed(
		{
			listTaskAttempts: async () => {
				throw new ConnectError('task attempt store is not configured', Code.FailedPrecondition);
			}
		},
		'ork-1',
		'',
		(err) => (err as Error).message,
		(err) => err instanceof ConnectError && err.code === Code.FailedPrecondition
	);
	expect(absent.storeAbsent).toBe(true);
	expect(absent.attempts).toEqual([]);
	expect(absent.error).toContain('not configured');

	const down = await readAttemptSeed(
		{
			listTaskAttempts: async () => {
				throw new ConnectError('store unreachable', Code.Unavailable);
			}
		},
		'ork-1',
		'',
		(err) => (err as Error).message,
		(err) => err instanceof ConnectError && err.code === Code.FailedPrecondition
	);
	expect(down.storeAbsent).toBe(false);
	expect(down.error).toContain('store unreachable');
});
