// Fan-out on the run page (spec each-fan-out §10): the page's only logic
// about items, kept here so it is unit-tested. An item is addressed by its
// 0-based index and rendered convert[97] — a rendering, never parsed.

import { CacheOutcome } from './gen/orkstack/v1/activity_pb';
import { Status } from './gen/orkstack/v1/common_pb';
import type { Event, TaskRun } from './gen/orkstack/v1/run_pb';
import type { EachSpec } from './gen/orkstack/v1/orkflow_pb';
import type { PlanTask } from './gen/orkstack/v1/service_pb';
import { runCommand } from './runDetail';
import { isTerminal, statusLabel } from './status';

/** One item's cell and panel facts: a TaskRun of the item, plus its index. */
export interface ItemView {
	index: number;
	status: Status;
	attempt: number;
	message: string;
	run?: TaskRun;
}

/** itemLabel renders one item as convert[97]. */
export function itemLabel(taskId: string, index: number): string {
	return `${taskId}[${index}]`;
}

/** itemsFromTaskRun seeds the item views from a snapshot's fan-out row. */
export function itemsFromTaskRun(taskRun: TaskRun | undefined): ItemView[] {
	return (taskRun?.items ?? []).map((item, i) => ({
		index: item.item ?? i,
		status: item.status,
		attempt: item.attempt,
		message: item.error?.message ?? '',
		run: item
	}));
}

export interface EachProgress {
	count: number;
	settled: number;
	succeeded: number;
	failed: number;
	canceled: number;
	running: number;
	pending: number;
	retried: number;
}

/** eachProgress tallies a fan-out's items by status, and how many retried. */
export function eachProgress(items: ItemView[]): EachProgress {
	const p: EachProgress = { count: items.length, settled: 0, succeeded: 0, failed: 0, canceled: 0, running: 0, pending: 0, retried: 0 };
	for (const item of items) {
		switch (item.status) {
			case Status.SUCCEEDED:
				p.succeeded++;
				break;
			case Status.FAILED:
				p.failed++;
				break;
			case Status.CANCELED:
				p.canceled++;
				break;
			case Status.RUNNING:
				p.running++;
				break;
			default:
				p.pending++;
		}
		if (isTerminal(item.status)) p.settled++;
		if (item.attempt > 1) p.retried++;
	}
	return p;
}

/**
 * eachSummary is the card's one line: "97 of 120 settled — 95 succeeded, 2
 * failed · 2 retried", "fan-out over 0 items", or, before expansion, "Will
 * fan out over {over} (at most {max} items)".
 */
export function eachSummary(expanded: boolean, items: ItemView[], spec?: EachSpec): string {
	if (!expanded) {
		return spec ? `Will fan out over ${spec.over} (at most ${spec.maxItems} items)` : 'Will fan out when the run reaches it';
	}
	if (items.length === 0) return 'fan-out over 0 items';
	const p = eachProgress(items);
	const parts = [`${p.succeeded} succeeded`];
	if (p.failed > 0) parts.push(`${p.failed} failed`);
	if (p.canceled > 0) parts.push(`${p.canceled} canceled`);
	let line = `${p.settled} of ${p.count} settled — ${parts.join(', ')}`;
	if (p.retried > 0) line += ` · ${p.retried} retried`;
	const cached = cachedItems(items);
	if (cached > 0) line += ` · ${cached} cached`;
	return line;
}

/** isCachedItem is true for an item whose result was reused (a cache hit). */
export function isCachedItem(item: ItemView): boolean {
	return item.run?.cache?.outcome === CacheOutcome.HIT;
}

/** cachedItems counts a fan-out's reused items. */
export function cachedItems(items: ItemView[]): number {
	return items.filter(isCachedItem).length;
}

/**
 * applyItemEvent moves ONE cell for an item TASK_LOG — its status and its
 * message — and returns a new array. An event without an item is ignored:
 * it is about the task, never one of its items.
 */
export function applyItemEvent(items: ItemView[], event: Event): ItemView[] {
	if (event.item === undefined) return items;
	const index = event.item;
	if (index < 0) return items;
	const next = items.slice();
	// An item event can outrun the snapshot that tells the page how many
	// items there are: grow to it with PENDING cells rather than drop it.
	for (let i = next.length; i <= index; i++) {
		next.push({ index: i, status: Status.PENDING, attempt: 0, message: '' });
	}
	const status = event.status === Status.UNSPECIFIED ? next[index].status : event.status;
	next[index] = { ...next[index], status, message: event.message };
	return next;
}

/**
 * mergeItems folds a snapshot's items over the ones the page already holds.
 * The snapshot wins — it carries every per-attempt fact an event does not —
 * except where an event already settled an item the snapshot, read a moment
 * earlier, still calls unsettled: an item never moves back from a terminal
 * status.
 */
export function mergeItems(existing: ItemView[], snapshot: ItemView[]): ItemView[] {
	if (snapshot.length === 0) return existing;
	const merged = snapshot.map((item) => {
		const held = existing[item.index];
		if (held && isTerminal(held.status) && !isTerminal(item.status)) return held;
		return item;
	});
	for (let i = merged.length; i < existing.length; i++) merged.push(existing[i]);
	return merged;
}

/** itemFromUrl reads ?item=N, or undefined when it is absent or malformed. */
export function itemFromUrl(url: URL): number | undefined {
	const raw = url.searchParams.get('item');
	if (raw === null || !/^\d+$/.test(raw)) return undefined;
	const n = Number(raw);
	return Number.isSafeInteger(n) ? n : undefined;
}

/**
 * itemCli is the CLI equivalent for one item: `orkctl … run <command> <run>
 * --task <task> --item <n>`, every argument shell-quoted by runCommand.
 */
export function itemCli(opts: { namespace: string; runId: string; taskId: string; item: number; command: string }): string {
	return runCommand(opts.namespace, ['run', opts.command, opts.runId, '--task', opts.taskId, '--item', String(opts.item)]);
}

/** eachPlanDetail is the plan's DETAIL for a fan-out, as the CLI prints it. */
export function eachPlanDetail(task: PlanTask): string {
	const spec = task.each;
	if (!spec) return '';
	const parallel = spec.maxParallel > 0 ? `, ${spec.maxParallel} at a time` : '';
	if (task.eachCount !== undefined) {
		const noun = task.eachCount === 1 ? 'item' : 'items';
		return `each over ${spec.over}: ${task.eachCount} ${noun} (≤ ${spec.maxItems}${parallel})`;
	}
	return `each over ${spec.over} (≤ ${spec.maxItems} items${parallel}; counted when the run reaches it)`;
}

/**
 * itemStatusText is a cell's status in words: its status, except that an item
 * the run ended before it was ever dispatched reads "CANCELED (never
 * started)" — CANCELED alone would say it was running and got stopped.
 */
export function itemStatusText(item: ItemView): string {
	const label = statusLabel(item.status);
	if (item.status === Status.CANCELED && !item.run?.scheduledAt) return `${label} (never started)`;
	return label;
}

/** itemAriaLabel is a cell's accessible name: "item 97, SUCCEEDED, attempt 2". */
export function itemAriaLabel(item: ItemView): string {
	return `item ${item.index}, ${itemStatusText(item)}, attempt ${item.attempt}${isCachedItem(item) ? ', cached' : ''}`;
}

/** retriedItemsLabel is the fan-out card's retry badge, or null for none. */
export function retriedItemsLabel(items: ItemView[]): string | null {
	const n = eachProgress(items).retried;
	if (n === 0) return null;
	return n === 1 ? '1 item retried' : `${n} items retried`;
}
