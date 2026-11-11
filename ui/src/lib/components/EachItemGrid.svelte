<script lang="ts">
	// A fan-out's items under its row (docs/DSL.md "Fan-out"): one summary line
	// and one button per item. Pressing one selects it: the Item card and the
	// log pane follow. Nothing here reads on its own; the page owns the run's
	// state and this draws it and reports what the reader chose.
	import type { EachSpec } from '#lib/gen/orkstack/v1/orkflow_pb.ts';
	import type { TaskEach } from '#lib/gen/orkstack/v1/run_pb.ts';
	import { Status } from '#lib/gen/orkstack/v1/common_pb.ts';
	import { eachCounts, eachSummary, isCachedItem, itemAriaLabel, itemLabel, type ItemView } from '#lib/each.ts';
	import { statusClassWord } from '#lib/statusMarks.ts';

	interface Props {
		taskId: string;
		each?: TaskEach;
		/** The definition's `each:`, for the not-yet-expanded summary. */
		spec?: EachSpec;
		items: ItemView[];
		/** The selected item, or undefined for none. */
		selected?: number;
		onSelect: (item: number | undefined) => void;
		/** Items the live read says are waiting for capacity. */
		waitingOf?: (item: number) => boolean;
		/** What those items are waiting for, in words, when it is not capacity. */
		waitingWord?: string;
	}

	let { taskId, each, spec, items, selected, onSelect, waitingOf, waitingWord = 'waiting for capacity' }: Props = $props();

	// Past a few hundred items a cell is only its index, and the grid is skipped off screen.
	const compact = $derived(items.length > 200);
	// A cell's name starts with what it shows, so a voice command can say it.
	function cellLabel(item: ItemView): string {
		const words = compact ? itemAriaLabel(item) : itemAriaLabel(item).replace(/^item \d+/, itemLabel(taskId, item.index));
		return item.status === Status.RUNNING && waitingOf?.(item.index) ? `${words}, ${waitingWord}` : words;
	}
	const summary = $derived(items.length > 0 ? eachCounts(items, waitingOf, waitingWord) : eachSummary(each?.expanded ?? false, items, spec));

	let grid = $state<HTMLDivElement | undefined>();

	// One tab stop for the whole grid, however many items it holds: the last
	// cell focused, else the selected one, else the first. The arrows, Home and
	// End walk the rest.
	let cursor = $state<number | undefined>(undefined);
	const tabStop = $derived(cursor ?? selected ?? items[0]?.index);

	// The grid is one tab stop's worth of buttons a keyboard can walk: the
	// arrows move between cells, Home and End jump to either end. Every cell
	// is still a real button, so Tab and Enter work without any of this.
	function onGridKey(event: KeyboardEvent): void {
		const cells = grid ? Array.from(grid.querySelectorAll<HTMLButtonElement>('button[data-item]')) : [];
		const at = cells.indexOf(document.activeElement as HTMLButtonElement);
		if (at < 0) return;
		let next = at;
		switch (event.key) {
			case 'ArrowRight':
			case 'ArrowDown':
				next = Math.min(at + 1, cells.length - 1);
				break;
			case 'ArrowLeft':
			case 'ArrowUp':
				next = Math.max(at - 1, 0);
				break;
			case 'Home':
				next = 0;
				break;
			case 'End':
				next = cells.length - 1;
				break;
			default:
				return;
		}
		event.preventDefault();
		cells[next]?.focus();
	}
</script>

<div class="items" data-testid="task-{taskId}-each">
	<p class="items-head">
		<b>Items of {taskId}</b>
		<span class="text-ink-3" data-testid="task-{taskId}-each-summary">{summary}{#if spec?.maxParallel}{' · '}<span data-testid="task-{taskId}-each-parallel">{spec.maxParallel} at a time</span>{/if}{#if selected !== undefined}{' · '}selected <b class="font-semibold text-ink-2">{itemLabel(taskId, selected)}</b>{/if}</span>
	</p>
	{#if items.length > 0}
		<!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
		<div bind:this={grid} role="group" aria-label="Items of {taskId}" class="items-grid" class:compact data-testid="task-{taskId}-item-grid" onkeydown={onGridKey} onfocusin={(e) => { const at = (e.target as HTMLElement).dataset.item; if (at !== undefined) cursor = Number(at); }}>
			{#each items as item (item.index)}
				<button
					type="button"
					data-item={item.index}
					data-testid="task-{taskId}-item-{item.index}"
					data-status={Status[item.status ?? Status.UNSPECIFIED]}
					data-cached={isCachedItem(item) ? 'true' : undefined}
					aria-label={cellLabel(item)}
					aria-pressed={selected === item.index}
					tabindex={tabStop === item.index ? 0 : -1}
					data-tip={compact ? undefined : cellLabel(item)}
					onclick={() => onSelect(selected === item.index ? undefined : item.index)}
					class="item is-{item.status === Status.RUNNING && waitingOf?.(item.index) ? 'wait' : statusClassWord(item.status)}"
				>{compact ? item.index : itemLabel(taskId, item.index)}</button>
			{/each}
		</div>
	{/if}
</div>

<style>
	.items {
		display: grid;
		grid-template-columns: minmax(0, 1fr);
		gap: 12px;
		padding: 14px 16px;
		border-bottom: 1px solid var(--line);
		background: var(--surface-2);
	}
	.items-head {
		margin: 0;
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 4px 12px;
		font-size: 13px;
	}
	.items-grid {
		display: grid;
		grid-template-columns: repeat(auto-fill, minmax(124px, 1fr));
		gap: 6px;
	}
	.item {
		position: relative;
		display: flex;
		align-items: center;
		gap: 7px;
		height: 34px;
		padding: 0 10px;
		border: 1px solid var(--line);
		border-radius: var(--r-sm);
		background: var(--surface);
		overflow: hidden;
		color: var(--ink-3);
		font-family: var(--font-code);
		font-size: 12.5px;
		text-overflow: ellipsis;
		white-space: nowrap;
		cursor: pointer;
		min-width: 0;
	}
	@media (max-width: 640px) {
		.item {
			height: 40px;
		}
	}
	.item::before {
		content: '';
		flex: none;
		width: 8px;
		height: 8px;
		border: 1.5px solid var(--pend-mark);
		border-radius: 2px;
	}
	.item:hover {
		border-color: var(--line-strong);
	}
	.compact {
		display: flex;
		flex-wrap: wrap;
		gap: 4px;
		/* Room for a focused or selected cell's ring: this grid clips its paint. */
		padding: 4px;
		content-visibility: auto;
		contain-intrinsic-size: auto 800px;
	}
	.compact .item {
		height: 28px;
		padding: 0 6px;
		gap: 4px;
	}
	.compact .item::before {
		width: 6px;
		height: 6px;
	}
	.item[data-cached]::after {
		content: '';
		position: absolute;
		top: 4px;
		right: 4px;
		width: 6px;
		height: 6px;
		border-radius: 50%;
		box-shadow: inset 0 0 0 1.5px var(--ok-mark);
	}
	.is-succeeded::before {
		border: 0;
		border-radius: 2px;
		background: var(--ok-mark);
	}
	.is-running::before {
		border: 0;
		border-radius: 50%;
		background: var(--run);
	}
	.is-running {
		border-color: var(--line-2);
	}
	.is-wait::before {
		width: 9px;
		height: 9px;
		border: 2px solid var(--wait-mark);
		border-radius: 50%;
	}
	.is-wait {
		border-color: var(--wait-mark);
	}
	.is-failed::before {
		width: 9px;
		height: 9px;
		border: 0;
		border-radius: 0;
		background: var(--fail-mark);
		clip-path: polygon(50% 0, 100% 100%, 0 100%);
	}
	.is-failed {
		border-color: var(--fail-mark);
		background: var(--fail-tint);
	}
	.is-canceled::before,
	.is-skipped::before {
		height: 0;
		border: 0;
		border-top: 2px solid var(--cancel);
		border-radius: 0;
	}
	.item[aria-pressed='true'] {
		box-shadow: 0 0 0 2px var(--select);
	}
	.cached {
		position: absolute;
		top: 4px;
		right: 4px;
		width: 6px;
		height: 6px;
		border-radius: 50%;
		box-shadow: inset 0 0 0 1.5px var(--ok-mark);
	}
	@media (forced-colors: active) {
		.item::before {
			forced-color-adjust: none;
			outline: 1px solid CanvasText;
		}
		.item[aria-pressed='true'] {
			outline: 2px solid Highlight;
		}
	}
</style>
