import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { timestampFromDate } from '@bufbuild/protobuf/wkt';
import {
	CacheOutcome,
	CacheResultSchema,
	CacheScope,
	CacheSourceSchema
} from '../src/lib/gen/orkstack/v1/activity_pb';
import { CacheSpecSchema } from '../src/lib/gen/orkstack/v1/orkflow_pb';
import { TaskRunSchema } from '../src/lib/gen/orkstack/v1/run_pb';
import {
	cacheOutcomeLabel,
	cacheOutcomeWord,
	cachePlanDetail,
	cacheSourceHref,
	cacheSpecLabel,
	cachedCount,
	memoWriteSentence,
	missReasonSentence,
	noCacheFlag,
	parseGoDuration,
	purgeCommand,
	purgeSelectorError,
	sourceAttemptDuration,
	ttlLabel
} from '../src/lib/cache';

test('cacheOutcomeLabel and cacheOutcomeWord', () => {
	expect(cacheOutcomeLabel(CacheOutcome.HIT)).toBe('Cached');
	expect(cacheOutcomeLabel(CacheOutcome.MISS)).toBe('Cache miss');
	expect(cacheOutcomeLabel(CacheOutcome.BYPASSED)).toBe('Cache bypassed');
	expect(cacheOutcomeLabel(CacheOutcome.UNSPECIFIED)).toBe('');
	expect(cacheOutcomeLabel(undefined)).toBe('');
	expect(cacheOutcomeWord(CacheOutcome.HIT)).toBe('hit');
	expect(cacheOutcomeWord(CacheOutcome.BYPASSED)).toBe('bypassed');
	expect(cacheOutcomeWord(undefined)).toBe('');
});

test('missReasonSentence covers every token and a token it has never heard of', () => {
	for (const token of [
		'MEMO_ABSENT',
		'MEMO_UNREADABLE',
		'MEMO_MISMATCH',
		'MEMO_EXPIRED',
		'SOURCE_COMMIT_GONE',
		'SOURCE_COMMIT_CHANGED',
		'SOURCE_COMMIT_UNREADABLE',
		'SOURCE_COMMIT_MISMATCH',
		'SOURCE_OVER_LIMITS',
		'SOURCE_OBJECT_UNAVAILABLE',
		'INPUT_UNHASHED',
		'MEMO_STORE_ABSENT'
	]) {
		const sentence = missReasonSentence(token);
		expect(sentence.length > 10).toBe(true);
		expect(sentence.endsWith('.')).toBe(true);
	}
	expect(missReasonSentence('MEMO_ABSENT')).toContain('No memo exists');
	expect(missReasonSentence('FROM_THE_FUTURE', 'the orker said so')).toBe('the orker said so');
	expect(missReasonSentence('FROM_THE_FUTURE')).toContain('FROM_THE_FUTURE');
});

test('cacheSourceHref keeps the namespace and escapes the id', () => {
	expect(cacheSourceHref('ml', create(CacheSourceSchema, { runId: 'ork-1/2' }))).toBe('/runs/ork-1%2F2?ns=ml');
	expect(cacheSourceHref('', create(CacheSourceSchema, { runId: 'ork-1' }))).toBe('/runs/ork-1?ns=default');
});

test('cachedCount counts hits on tasks and on fan-out items', () => {
	const hit = create(CacheResultSchema, { outcome: CacheOutcome.HIT });
	const miss = create(CacheResultSchema, { outcome: CacheOutcome.MISS });
	const tasks = [
		create(TaskRunSchema, { taskId: 'a', cache: hit }),
		create(TaskRunSchema, { taskId: 'b', cache: miss }),
		create(TaskRunSchema, { taskId: 'c' }),
		create(TaskRunSchema, {
			taskId: 'convert',
			items: [
				create(TaskRunSchema, { cache: hit }),
				create(TaskRunSchema, { cache: hit }),
				create(TaskRunSchema, { cache: miss })
			]
		})
	];
	expect(cachedCount(tasks)).toBe(3);
	expect(cachedCount([])).toBe(0);
});

test('ttlLabel, cacheSpecLabel and cachePlanDetail', () => {
	expect(ttlLabel(create(CacheSpecSchema, { ttlSeconds: 86400 }))).toBe('24h');
	expect(ttlLabel(create(CacheSpecSchema, { ttlSeconds: 5400 }))).toBe('1h30m');
	expect(ttlLabel(create(CacheSpecSchema, { ttlSeconds: 90 }))).toBe('1m30s');
	expect(ttlLabel(create(CacheSpecSchema, {}))).toBe('deployment cap');
	expect(ttlLabel(undefined)).toBe('deployment cap');
	expect(cacheSpecLabel(create(CacheSpecSchema, { ttlSeconds: 86400, scope: CacheScope.ORKFLOW }))).toBe('24h · orkflow');
	expect(cacheSpecLabel(create(CacheSpecSchema, { scope: CacheScope.NAMESPACE }))).toBe('deployment cap · namespace');
	expect(cacheSpecLabel(undefined)).toBe('—');
	expect(cachePlanDetail(create(CacheSpecSchema, { ttlSeconds: 86400, scope: CacheScope.ORKFLOW }))).toBe(
		'cached across runs (ttl 24h, orkflow scope); whether it reuses a result is decided by the Orker when the run reaches it'
	);
	expect(cachePlanDetail(create(CacheSpecSchema, { scope: CacheScope.ORKFLOW }))).toContain(
		"ttl: the deployment's cap"
	);
});

test('sourceAttemptDuration and memoWriteSentence', () => {
	const start = new Date('2026-09-25T10:00:00Z');
	const source = create(CacheSourceSchema, {
		startedAt: timestampFromDate(start),
		finishedAt: timestampFromDate(new Date(start.getTime() + 482_000))
	});
	expect(sourceAttemptDuration(source)).toBe('8m02s');
	expect(sourceAttemptDuration(undefined)).toBe('');
	expect(memoWriteSentence(create(CacheResultSchema, { outcome: CacheOutcome.MISS, memoWritten: true }))).toBe(
		'Its memo was written.'
	);
	expect(
		memoWriteSentence(create(CacheResultSchema, { outcome: CacheOutcome.MISS, memoWriteError: '503' }))
	).toBe('Its memo could not be written: 503');
	expect(memoWriteSentence(create(CacheResultSchema, { outcome: CacheOutcome.HIT }))).toBe('');
});

test('purgeCommand names exactly one selector and quotes what a page supplies', () => {
	expect(purgeCommand({ namespace: 'ml', orkflow: 'cached-pipeline', task: 'featurize' })).toBe(
		"orkctl --namespace ml cache purge --orkflow cached-pipeline --task featurize"
	);
	expect(purgeCommand({ namespace: '', namespaceScoped: true })).toBe(
		"orkctl --namespace default cache purge --namespace-scoped"
	);
	expect(purgeCommand({ namespace: 'ml', all: true, task: 'ignored', olderThan: '72h' })).toBe(
		"orkctl --namespace ml cache purge --all --older-than 72h"
	);
	// A quote in a task id cannot become shell syntax.
	const hostile = purgeCommand({ namespace: 'ml', orkflow: 'p', task: "x'; rm -rf / #" });
	expect(hostile).toContain(`--task 'x'"'"'; rm -rf / #'`);
	expect(purgeSelectorError({ namespace: 'ml' })).toContain('exactly one');
	expect(purgeSelectorError({ namespace: 'ml', orkflow: 'p', all: true })).toContain('exactly one');
	expect(purgeSelectorError({ namespace: 'ml', all: true, task: 't' })).toContain('does not apply to all');
	expect(purgeSelectorError({ namespace: 'ml', all: true })).toBe('');
});

test('noCacheFlag and parseGoDuration', () => {
	expect(noCacheFlag(true)).toBe('--no-cache');
	expect(noCacheFlag(false)).toBe('');
	expect(parseGoDuration('72h')).toBe(259200);
	expect(parseGoDuration('1h30m')).toBe(5400);
	expect(parseGoDuration('')).toBeNull();
	expect(parseGoDuration('3 days')).toBeNull();
});

import { cacheStatsCli, cacheWindowFrom, hitRateLabel, inventoryRowId, reusedLabel, attemptCacheLine, keyComponentRows } from '../src/lib/cache';

test('cacheWindowFrom, cacheStatsCli, hitRateLabel, reusedLabel and inventoryRowId', () => {
	expect(cacheWindowFrom(new URL('http://x/admin/cache'))).toEqual({ window: '24h', lookbackSeconds: 86400 });
	expect(cacheWindowFrom(new URL('http://x/admin/cache?since=168h'))).toEqual({ window: '168h', lookbackSeconds: 604800 });
	expect(cacheWindowFrom(new URL('http://x/admin/cache?since=2026-09-20T00:00:00Z'))).toEqual({
		window: 'custom',
		since: '2026-09-20T00:00:00Z'
	});
	expect(cacheWindowFrom(new URL('http://x/admin/cache?since=junk')).window).toBe('24h');
	expect(cacheStatsCli('ml', { window: '168h' }, 'cached-pipeline')).toBe(
		"orkctl --namespace ml cache stats --since 168h --orkflow cached-pipeline"
	);
	expect(hitRateLabel({ hitRate: 8 / 9, hitRateDefined: true })).toBe('88.9%');
	expect(hitRateLabel({ hitRate: 0, hitRateDefined: false })).toBe('unavailable');
	expect(reusedLabel({ seconds: 3856n, nanos: 0 })).toBe('1h04m16s');
	expect(reusedLabel({ seconds: 0n, nanos: 0 })).toBe('—');
	expect(reusedLabel(undefined)).toBe('—');
	expect(inventoryRowId({ scope: CacheScope.NAMESPACE, orkflowName: '', taskId: 'toolchain' })).toBe('namespace---toolchain');
	expect(inventoryRowId({ scope: CacheScope.ORKFLOW, orkflowName: 'p', taskId: 't' })).toBe('orkflow-p-t');
});

test('attemptCacheLine and keyComponentRows', () => {
	expect(
		attemptCacheLine(
			create(CacheResultSchema, { outcome: CacheOutcome.HIT, source: create(CacheSourceSchema, { runId: 'ork-1' }) })
		)
	).toBe('cache hit from ork-1');
	expect(attemptCacheLine(create(CacheResultSchema, { outcome: CacheOutcome.MISS, missReason: 'MEMO_EXPIRED' }))).toBe(
		'cache miss: MEMO_EXPIRED'
	);
	expect(attemptCacheLine(create(CacheResultSchema, { outcome: CacheOutcome.BYPASSED }))).toBe('cache bypassed');
	expect(attemptCacheLine(undefined)).toBe('');
	expect(keyComponentRows({ inputs: 'b', config: 'a' })).toEqual([
		['config', 'a'],
		['inputs', 'b']
	]);
});

import { Status } from '../src/lib/gen/orkstack/v1/common_pb';
import { eachSummary, itemAriaLabel, isCachedItem, type ItemView } from '../src/lib/each';

test('a cached fan-out item is marked in the summary and its label', () => {
	const hit = create(CacheResultSchema, { outcome: CacheOutcome.HIT });
	const item = (index: number, cached: boolean): ItemView => ({
		index,
		status: Status.SUCCEEDED,
		attempt: 1,
		message: '',
		run: create(TaskRunSchema, { item: index, status: Status.SUCCEEDED, ...(cached ? { cache: hit } : {}) })
	});
	const items = [item(0, true), item(1, true), item(2, false)];
	expect(eachSummary(true, items)).toBe('3 of 3 settled — 3 succeeded · 2 cached');
	expect(itemAriaLabel(items[0])).toBe('item 0, SUCCEEDED, attempt 1, cached');
	expect(itemAriaLabel(items[2])).toBe('item 2, SUCCEEDED, attempt 1');
	expect(isCachedItem(items[1])).toBe(true);
});
