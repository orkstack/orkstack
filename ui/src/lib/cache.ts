// Task caching on the run, Orkflow and admin pages (docs/DSL.md "Caching"):
// the only logic the UI holds for it, pure and unit-tested. Every sentence
// mirrors cmd/orkctl/cache.go.
import { CacheOutcome, CacheScope, type CacheResult, type CacheSource } from './gen/orkstack/v1/activity_pb';
import type { CacheSpec } from './gen/orkstack/v1/orkflow_pb';
import type { TaskRun } from './gen/orkstack/v1/run_pb';
import { withNamespace } from './namespace';
import { shellQuote } from './runDetail';

/** The badge's `data-outcome` and label: hit, miss, bypassed, or none. */
export type CacheBadgeOutcome = 'hit' | 'miss' | 'bypassed' | 'unreported' | '';

/** cacheOutcomeLabel renders a memo decision: "Cached", "Cache miss", … */
export function cacheOutcomeLabel(outcome: CacheOutcome | undefined): string {
	switch (outcome) {
		case CacheOutcome.HIT:
			return 'Cached';
		case CacheOutcome.MISS:
			return 'Cache miss';
		case CacheOutcome.BYPASSED:
			return 'Cache bypassed';
		default:
			return '';
	}
}

/** cacheOutcomeWord is the badge's data-outcome for a decision. */
export function cacheOutcomeWord(outcome: CacheOutcome | undefined): CacheBadgeOutcome {
	switch (outcome) {
		case CacheOutcome.HIT:
			return 'hit';
		case CacheOutcome.MISS:
			return 'miss';
		case CacheOutcome.BYPASSED:
			return 'bypassed';
		default:
			return '';
	}
}

/** One sentence per miss token the Orker records (internal/orker/memo.go). */
const MISS_SENTENCES: Record<string, string> = {
	MEMO_ABSENT: 'No memo exists for this key: no earlier run with exactly these inputs succeeded.',
	MEMO_UNREADABLE: 'The memo could not be read, so the task ran.',
	MEMO_MISMATCH: 'The memo stored at this key did not describe this task, so it was ignored.',
	MEMO_EXPIRED: "The memoized result is older than this task's TTL.",
	SOURCE_COMMIT_GONE: 'The run that produced the memoized result was retired; its commit is gone.',
	SOURCE_COMMIT_CHANGED: 'The memoized commit changed after it was memoized, so it was not reused.',
	SOURCE_COMMIT_UNREADABLE: 'The memoized commit could not be read.',
	SOURCE_COMMIT_MISMATCH: 'The memoized commit did not match this task.',
	SOURCE_OVER_LIMITS: "The memoized result does not fit this run's limits.",
	SOURCE_OBJECT_UNAVAILABLE: 'An object of the memoized result is gone, so the task ran.',
	INPUT_UNHASHED: 'An input carries no digest, so no memo key could be computed.',
	MEMO_STORE_ABSENT: 'The Orker that ran this task has no memo store.'
};

/**
 * missReasonSentence is the sentence for a miss token. A token this build has
 * never heard of reads the Orker's own detail, or a generic sentence.
 */
export function missReasonSentence(reason: string, detail = ''): string {
	const known = MISS_SENTENCES[reason];
	if (known) return known;
	if (detail) return detail;
	return `The task ran: no usable memo (${reason || 'unknown reason'}).`;
}

/** cacheSourceHref is the producing run's page, in the namespace. */
export function cacheSourceHref(namespace: string, source: Pick<CacheSource, 'runId'> | undefined): string {
	return withNamespace(`/runs/${encodeURIComponent(source?.runId ?? '')}`, namespace);
}

/** cachedCount counts reused results: tasks, and items of fan-outs. */
export function cachedCount(tasks: Pick<TaskRun, 'cache' | 'items'>[]): number {
	let n = 0;
	for (const task of tasks) {
		if (task.cache?.outcome === CacheOutcome.HIT) n++;
		for (const item of task.items ?? []) {
			if (item.cache?.outcome === CacheOutcome.HIT) n++;
		}
	}
	return n;
}

/** shortDuration spells 86400s as 24h, 5400s as 1h30m, 90s as 1m30s. */
function shortDuration(seconds: number): string {
	const h = Math.floor(seconds / 3600);
	const m = Math.floor((seconds % 3600) / 60);
	const s = seconds % 60;
	let out = '';
	if (h) out += `${h}h`;
	if (m) out += `${m}m`;
	if (s || out === '') out += `${s}s`;
	return out;
}

/** ttlLabel is a CacheSpec's TTL: "24h", or "deployment cap" when unset. */
export function ttlLabel(spec: Pick<CacheSpec, 'ttlSeconds'> | undefined): string {
	if (!spec || spec.ttlSeconds <= 0) return 'deployment cap';
	return shortDuration(spec.ttlSeconds);
}

/** scopeName renders a scope: "orkflow", "namespace", or "". */
export function scopeName(scope: CacheScope | undefined): string {
	switch (scope) {
		case CacheScope.ORKFLOW:
			return 'orkflow';
		case CacheScope.NAMESPACE:
			return 'namespace';
		default:
			return '';
	}
}

/** cacheSpecLabel is the Orkflow page's cell: "24h · orkflow". */
export function cacheSpecLabel(spec: CacheSpec | undefined): string {
	if (!spec) return '—';
	return `${ttlLabel(spec)} · ${scopeName(spec.scope) || 'orkflow'}`;
}

/** cachePlanDetail is the plan table's DETAIL for a cached task (cmd/orkctl). */
export function cachePlanDetail(spec: CacheSpec): string {
	const ttl = spec.ttlSeconds > 0 ? `ttl ${ttlLabel(spec)}` : "ttl: the deployment's cap";
	return `cached across runs (${ttl}, ${scopeName(spec.scope) || 'orkflow'} scope); whether it reuses a result is decided by the Orker when the run reaches it`;
}

/** sourceAttemptDuration is "8m02s": how long the producing attempt took. */
export function sourceAttemptDuration(source: Pick<CacheSource, 'startedAt' | 'finishedAt'> | undefined): string {
	const s = source?.startedAt;
	const f = source?.finishedAt;
	if (!s || !f) return '';
	const ms = (Number(f.seconds) - Number(s.seconds)) * 1000 + (f.nanos - s.nanos) / 1e6;
	if (ms < 0) return '';
	const total = Math.round(ms / 1000);
	const h = Math.floor(total / 3600);
	const m = Math.floor((total % 3600) / 60);
	const sec = total % 60;
	const pad = (v: number) => String(v).padStart(2, '0');
	if (h > 0) return `${h}h${pad(m)}m${pad(sec)}s`;
	if (m > 0) return `${m}m${pad(sec)}s`;
	return `${sec}s`;
}

/** memoWriteSentence says whether a fresh run refreshed its memo. */
export function memoWriteSentence(cache: Pick<CacheResult, 'outcome' | 'memoWritten' | 'memoWriteError'>): string {
	if (cache.outcome === CacheOutcome.HIT) return '';
	if (cache.memoWritten) return 'Its memo was written.';
	if (cache.memoWriteError) return `Its memo could not be written: ${cache.memoWriteError}`;
	return '';
}

/** The purge selector the dialog builds. Exactly one of the three is set. */
export interface PurgeSelector {
	namespace: string;
	orkflow?: string;
	namespaceScoped?: boolean;
	all?: boolean;
	task?: string;
	/** A Go duration, e.g. "72h"; empty purges every selected memo. */
	olderThan?: string;
}

/** purgeSelectorError is the refusal the CLI makes client-side, or "". */
export function purgeSelectorError(s: PurgeSelector): string {
	const n = [Boolean(s.orkflow), Boolean(s.namespaceScoped), Boolean(s.all)].filter(Boolean).length;
	if (n !== 1) return 'name exactly one of an orkflow, the namespace-scoped memos or all — a purge never has a default scope';
	if (s.all && s.task) return 'a task narrows an orkflow or the namespace-scoped memos; it does not apply to all';
	return '';
}

/** purgeCommand is the `orkctl cache purge …` the dialog's CliEquivalent shows. */
export function purgeCommand(s: PurgeSelector): string {
	const args = ['orkctl', '--namespace', shellQuote(s.namespace || 'default'), 'cache', 'purge'];
	if (s.orkflow) args.push('--orkflow', shellQuote(s.orkflow));
	else if (s.namespaceScoped) args.push('--namespace-scoped');
	else if (s.all) args.push('--all');
	if (s.task && !s.all) args.push('--task', shellQuote(s.task));
	if (s.olderThan) args.push('--older-than', shellQuote(s.olderThan));
	return args.join(' ');
}

/** noCacheFlag is what the submit panel's CliEquivalent appends. */
export function noCacheFlag(noCache: boolean): string {
	return noCache ? '--no-cache' : '';
}

/** parseGoDuration reads "72h", "90m", "1h30m" into seconds, or null. */
export function parseGoDuration(raw: string): number | null {
	const text = raw.trim();
	if (!text) return null;
	const re = /(\d+(?:\.\d+)?)(h|m|s)/gy;
	let total = 0;
	let consumed = 0;
	for (const match of text.matchAll(re)) {
		const value = Number(match[1]);
		total += match[2] === 'h' ? value * 3600 : match[2] === 'm' ? value * 60 : value;
		consumed += match[0].length;
	}
	if (consumed !== text.length) return null;
	return total;
}

/** attemptCacheLine is one attempt record's memo decision, as the timeline says it. */
export function attemptCacheLine(cache: Pick<CacheResult, 'outcome' | 'missReason' | 'source'> | undefined): string {
	switch (cache?.outcome) {
		case CacheOutcome.HIT:
			return `cache hit from ${cache.source?.runId ?? 'an earlier run'}`;
		case CacheOutcome.MISS:
			return `cache miss: ${cache.missReason || 'unknown reason'}`;
		case CacheOutcome.BYPASSED:
			return 'cache bypassed';
		default:
			return '';
	}
}

/** keyComponentRows lists a record's key components, sorted by name. */
export function keyComponentRows(components: Record<string, string> | undefined): [string, string][] {
	return Object.entries(components ?? {}).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
}

/** The Admin → Cache window presets, on the dashboard's `?since=` vocabulary. */
export const CACHE_WINDOWS = ['24h', '168h'] as const;

/** CacheWindow is the page's window: a preset, or a custom `since` instant. */
export interface CacheWindow {
	window: string;
	/** Seconds of lookback for a preset. */
	lookbackSeconds?: number;
	/** A custom window's start, as typed (RFC3339 or a date). */
	since?: string;
}

/** cacheWindowFrom reads `?since=` (a preset or an instant), defaulting to 24h. */
export function cacheWindowFrom(url: URL): CacheWindow {
	const raw = url.searchParams.get('since') ?? '24h';
	if ((CACHE_WINDOWS as readonly string[]).includes(raw)) {
		return { window: raw, lookbackSeconds: parseGoDuration(raw) ?? 86400 };
	}
	if (!Number.isNaN(Date.parse(raw))) return { window: 'custom', since: raw };
	return { window: '24h', lookbackSeconds: 86400 };
}

/** cacheStatsCli is `orkctl cache stats` for the page's question. */
export function cacheStatsCli(namespace: string, window: CacheWindow, orkflow: string): string {
	const args = ['orkctl', '--namespace', shellQuote(namespace || 'default'), 'cache', 'stats'];
	args.push('--since', shellQuote(window.window === 'custom' ? (window.since ?? '') : window.window));
	if (orkflow) args.push('--orkflow', shellQuote(orkflow));
	return args.join(' ');
}

/** hitRateLabel is "88.9%", or "unavailable" for a rate over nothing. */
export function hitRateLabel(row: { hitRate: number; hitRateDefined: boolean }): string {
	return row.hitRateDefined ? `${(row.hitRate * 100).toFixed(1)}%` : 'unavailable';
}

/** reusedLabel is "1h04m16s", or "—" for none. */
export function reusedLabel(reused: { seconds: bigint; nanos: number } | undefined): string {
	if (!reused) return '—';
	const total = Math.round(Number(reused.seconds) + reused.nanos / 1e9);
	if (total <= 0) return '—';
	const h = Math.floor(total / 3600);
	const m = Math.floor((total % 3600) / 60);
	const s = total % 60;
	const pad = (v: number) => String(v).padStart(2, '0');
	if (h > 0) return `${h}h${pad(m)}m${pad(s)}s`;
	if (m > 0) return `${m}m${pad(s)}s`;
	return `${s}s`;
}

/** inventoryRowId is the memo table's row key: "{scope}-{orkflow or '-'}-{task}". */
export function inventoryRowId(row: { scope: CacheScope; orkflowName: string; taskId: string }): string {
	return `${scopeName(row.scope) || 'unspecified'}-${row.orkflowName || '-'}-${row.taskId}`;
}
