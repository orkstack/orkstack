<script lang="ts">
	// A fan-out's items under its row (docs/DSL.md "Fan-out"): one summary line
	// and one button per item. Pressing one selects it: the Item card and the
	// log pane follow. Nothing here reads on its own; the page owns the run's
	// state and this draws it and reports what the reader chose.
	import type { EachSpec } from '#lib/gen/orkstack/v1/orkflow_pb.ts';
	import type { TaskEach } from '#lib/gen/orkstack/v1/run_pb.ts';
	import { Status } from '#lib/gen/orkstack/v1/common_pb.ts';
	import { eachSummary, isCachedItem, itemAriaLabel, itemLabel, type ItemView } from '#lib/each.ts';
	import { statusClassWord } from '#lib/statusMarks.ts';
	import { statusLabel } from '#lib/status.ts';

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
	}

	let { taskId, each, spec, items, selected, onSelect, waitingOf }: Props = $props();

	const summary = $derived(eachSummary(each?.expanded ?? false, items, spec));

	let grid = $state<HTMLDivElement | undefined>();

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
		<span class="text-ink-3" data-testid="task-{taskId}-each-summary">{summary}{#if selected !== undefined} · selected <b class="font-semibold text-ink-2">{itemLabel(taskId, selected)}</b>{/if}</span>
	</p>
	{#if items.length > 0}
		<!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
		<div bind:this={grid} role="group" aria-label="Items of {taskId}" class="items-grid" data-testid="task-{taskId}-item-grid" onkeydown={onGridKey}>
			{#each items as item (item.index)}
				{@const waiting = item.status === Status.RUNNING && waitingOf?.(item.index)}
				<button
					type="button"
					data-item={item.index}
					data-testid="task-{taskId}-item-{item.index}"
					data-status={Status[item.status ?? Status.UNSPECIFIED]}
					data-cached={isCachedItem(item) ? 'true' : undefined}
					aria-label={itemAriaLabel(item)}
					aria-pressed={selected === item.index}
					aria-controls="item-card"
					data-tip={itemAriaLabel(item)}
					onclick={() => onSelect(selected === item.index ? undefined : item.index)}
					class="item is-{waiting ? 'wait' : statusClassWord(item.status)}"
				><span class="sr">{waiting ? 'Waiting for capacity' : statusLabel(item.status)}: </span><span class="ix">{itemLabel(taskId, item.index)}</span>{#if isCachedItem(item)}<span class="cached" aria-hidden="true"></span>{/if}</button>
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
		font-size: 13.5px;
		cursor: pointer;
		min-width: 0;
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
	.ix {
		overflow: hidden;
		color: var(--ink-3);
		font-family: var(--font-code);
		font-size: 12.5px;
		text-overflow: ellipsis;
		white-space: nowrap;
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
		border-color: var(--run);
		background: var(--run-tint);
	}
	.is-wait::before {
		width: 9px;
		height: 9px;
		border: 2px solid var(--wait-mark);
		border-radius: 50%;
	}
	.is-wait {
		border-color: var(--wait-mark);
		background: var(--wait-tint);
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
