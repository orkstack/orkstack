// Operate's Concurrency page: a limit as it stands, in the words `orkctl
// concurrency list` and `get` print (cmd/orkctl/concurrency.go;
// docs/UI_CLI_PARITY.md "Concurrency limits"), its addresses and its commands.
import type { ConcurrencyLimit, ConcurrencySlotHolder } from '#lib/gen/orkstack/v1/concurrency_service_pb.ts';
import { ConcurrencyRebuild } from '#lib/gen/orkstack/v1/concurrency_service_pb.ts';
import { ConcurrencyStrategy } from '#lib/gen/orkstack/v1/orkflow_pb.ts';
import { listHref, pageStackKey } from '#lib/browsing.ts';
import { instantSeconds } from '#lib/deliveries.ts';
import { filterFlags, type FieldFilter } from '#lib/listFilter.ts';
import { shellQuote } from '#lib/runDetail.ts';

/** A limit's state as its pill reads it, and why when the word alone does not say. */
export interface LimitState {
	word: string;
	tone: 'ok' | 'wait' | 'fail';
	detail: string;
}

/**
 * limitState is a limit's State, in `orkctl concurrency`'s words. A rebuild
 * this build does not know is named by its number and never read as fine.
 */
export function limitState(l: ConcurrencyLimit): LimitState {
	if (!l.answering) return { word: 'Not answering', tone: 'fail', detail: l.notAnsweringReason };
	switch (l.rebuild) {
		case ConcurrencyRebuild.UNSPECIFIED:
		case ConcurrencyRebuild.NONE:
			return { word: 'OK', tone: 'ok', detail: '' };
		case ConcurrencyRebuild.READING:
		case ConcurrencyRebuild.SETTLING:
			return { word: 'Reading its holders', tone: 'wait', detail: '' };
		case ConcurrencyRebuild.FAILING:
			return { word: 'Reading its holders', tone: 'fail', detail: `failing since ${instantSeconds(l.rebuildFailingSince) || '-'}` };
		default:
			return { word: `Unknown (${String(l.rebuild)})`, tone: 'wait', detail: '' };
	}
}

/** entryLevel is whether a slot, or a place in line, is the run's own or one of its tasks'. */
export function entryLevel(task: boolean): string {
	return task ? 'task' : 'run';
}

/** strategyName is a declaration's on_limit, as the DSL writes it. */
export function strategyName(s: ConcurrencyStrategy): string {
	switch (s) {
		case ConcurrencyStrategy.QUEUE:
			return 'queue';
		case ConcurrencyStrategy.REFUSE:
			return 'refuse';
		case ConcurrencyStrategy.SKIP:
			return 'skip';
		default:
			return '-';
	}
}

/** holderNote is what an operator must know of a holder: a stalled one never ends by itself. */
export function holderNote(h: ConcurrencySlotHolder): string {
	return h.stalled ? 'not making progress; an operator must end it' : '';
}

/** shownOf says a list is the start of a longer one; empty when it is whole. */
export function shownOf(shown: number, total: number, word: 'holding' | 'waiting'): string {
	return total > shown ? `The first ${shown} of ${total} ${word}` : '';
}

/** Where the list of keys stands. */
export interface ConcurrencyCoordinates {
	ns: string;
	filters: FieldFilter[];
	cursor: string;
	page: number;
}

function coordinatesOf(c: ConcurrencyCoordinates): Record<string, string> {
	// A page number without a cursor names no page the engine could return.
	return { ns: c.ns || 'default', cursor: c.cursor, page: c.cursor ? String(c.page) : '' };
}

/** The list's address — listHref's rules (lib/browsing.ts). */
export function concurrencyHref(c: ConcurrencyCoordinates, changes: Partial<ConcurrencyCoordinates> = {}): string {
	const next = { ...c, ...changes };
	return listHref('/admin/concurrency', coordinatesOf(next), { filter: next.filters });
}

/** The sessionStorage key of this filter set's cursor stack. */
export function concurrencyStackKey(c: ConcurrencyCoordinates): string {
	return pageStackKey('/admin/concurrency', coordinatesOf(c), { filter: c.filters });
}

/** `orkctl concurrency list` for the same list. */
export function concurrencyListCli(namespace: string, filters: FieldFilter[]): string {
	return ['orkctl', '-n', shellQuote(namespace || 'default'), 'concurrency', 'list', ...filterFlags(filters)].join(' ');
}

/** `orkctl concurrency get` for one key; `--` keeps a key that begins with a dash a key. */
export function concurrencyGetCli(namespace: string, key: string): string {
	const args = key.startsWith('-') ? ['--', shellQuote(key)] : [shellQuote(key)];
	return ['orkctl', '-n', shellQuote(namespace || 'default'), 'concurrency', 'get', ...args].join(' ');
}
