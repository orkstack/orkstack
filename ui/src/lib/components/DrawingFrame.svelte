<script module lang="ts">
	export type DrawingView = 'dag' | 'map';
</script>

<script lang="ts">
	// The frame a run's (or an Orkflow's) tasks are drawn in: a box with a
	// 1.5 px ink rule, its view tabs and caption on a paper patch at the top
	// left, and — behind the DAG only — the drafting grid. Its height is CSS,
	// never measured: the task map's frame is --spacing-drawing tall, and the
	// DAG's is its drawing plus the patch and padding, up to that height
	// (min(--spacing-drawing, content + 48 px)), so a six-task DAG is not
	// a page of empty grid.
	//
	// The DAG is BOUNDED, on every page: up to DAG_DRAW_LIMIT tasks it is
	// drawn; up to DAG_LEGIBLE_LIMIT on request; beyond that it is never laid
	// out, and the run page draws the selected task's one-hop neighbourhood
	// instead.
	//
	// A drawing is scaled to the frame's width (DagGraph, down to
	// DAG_MIN_SCALE). One still wider than that scrolls inside the frame: the
	// frame says so with a pair of scroll controls on its top band, beside the
	// tabs and never over a node, the one with nothing more to show marked
	// aria-disabled (never `disabled`, which would drop a keyboard user's focus);
	// and on first drawing it brings the selected task — else the failed one,
	// whose failure handlers follow it — into view.
	import { tick, type Snippet } from 'svelte';
	import type { Task } from '$lib/gen/orkstack/v1/orkflow_pb';
	import DagGraph from './DagGraph.svelte';
	import Icon from './Icon.svelte';
	import { DAG_LEGIBLE_LIMIT, dagBound, neighbourhood, type NodeState } from '$lib/dag';

	interface Props {
		testid: string;
		/** "order-saga @ 0.4.2 · 6 tasks" */
		caption: string;
		/** The tabs, when there is more than the DAG; with none the frame shows only the DAG. */
		views?: { id: DrawingView; label: string; testid: string }[];
		view?: DrawingView;
		onview?: (view: DrawingView) => void;
		/** The definition's tasks, or null when it could not be read. */
		tasks: Task[] | null;
		/** Why the definition could not be read (the run page's NOTE). */
		unavailable?: string;
		/** "order-saga@0.4.2": what the NOTEs name. */
		orkflowLabel: string;
		/** Which page it is on: the testids of its DAG and its NOTEs follow. */
		on?: 'orkflow' | 'run';
		dagLabel?: string;
		stateOf?: (id: string) => NodeState | undefined;
		selected?: string;
		hrefOf?: (id: string) => string;
		onselect?: (id: string) => void;
		/** Past DAG_LEGIBLE_LIMIT: draw the selected task's neighbourhood (the run page), else point elsewhere. */
		neighbourhoodWhenLarge?: boolean;
		/** Where a reader goes instead past the limit, for a page with no selection (the Orkflow page). */
		largeHint?: string;
		map?: Snippet;
	}

	let {
		testid,
		caption,
		views = [],
		view = 'dag',
		onview,
		tasks,
		unavailable = '',
		orkflowLabel,
		on = 'orkflow',
		dagLabel = 'Orkflow task DAG',
		stateOf,
		selected,
		hrefOf,
		onselect,
		neighbourhoodWhenLarge = false,
		largeHint = 'The task register below lists every task and what it needs.',
		map
	}: Props = $props();

	const uid = $props.id();
	let drawAll = $state(false);
	const bound = $derived(tasks ? dagBound(tasks.length) : 'draw');
	const near = $derived(
		tasks && bound === 'too-large' && neighbourhoodWhenLarge && selected ? neighbourhood(tasks, selected) : []
	);
	const fmt = new Intl.NumberFormat('en-US');
	const prefix = $derived(on === 'run' ? 'run-dag' : 'dag');

	const drawn = $derived(
		view === 'dag' && !!tasks && (bound === 'draw' || (bound === 'on-request' && drawAll) || near.length > 0)
	);
	let panel = $state<HTMLDivElement | undefined>(undefined);
	let hidden = $state({ left: false, right: false });

	function measure(): void {
		if (!panel) return;
		const max = panel.scrollWidth - panel.clientWidth;
		hidden = { left: panel.scrollLeft > 1, right: panel.scrollLeft < max - 1 };
	}

	$effect(() => {
		const el = panel;
		if (!el || !drawn || typeof ResizeObserver === 'undefined') {
			hidden = { left: false, right: false };
			return;
		}
		const observer = new ResizeObserver(measure);
		observer.observe(el);
		const drawing = el.querySelector('svg');
		if (drawing) observer.observe(drawing);
		measure();
		return () => observer.disconnect();
	});

	// Once per drawing: a drawing that does not fit shows the task a reader
	// came for, not its first layer.
	let placed = false;
	$effect(() => {
		const el = panel;
		if (!el || !drawn) {
			placed = false;
			return;
		}
		if (placed) return;
		placed = true;
		void tick().then(() => bringIntoView(el));
	});

	function bringIntoView(el: HTMLDivElement): void {
		if (el.scrollWidth <= el.clientWidth + 1 && el.scrollHeight <= el.clientHeight + 1) return;
		const target = el.querySelector('.dag-node.is-selected') ?? el.querySelector('.dag-node.is-failed');
		if (!target) return;
		const node = target.getBoundingClientRect();
		const frame = el.getBoundingClientRect();
		const margin = Math.min(frame.width * 0.15, 48);
		el.scrollLeft += node.left - frame.left - margin;
		if (node.top < frame.top || node.bottom > frame.bottom) el.scrollTop += node.top - frame.top - margin;
		measure();
	}

	function scrollBy(direction: 1 | -1): void {
		const still = matchMedia('(prefers-reduced-motion: reduce)').matches;
		panel?.scrollBy({ left: direction * panel.clientWidth * 0.75, behavior: still ? 'auto' : 'smooth' });
	}

	function onTabKey(event: KeyboardEvent, index: number): void {
		if (event.key !== 'ArrowRight' && event.key !== 'ArrowLeft') return;
		event.preventDefault();
		const next = views[(index + (event.key === 'ArrowRight' ? 1 : views.length - 1)) % views.length];
		onview?.(next.id);
		document.getElementById(`${uid}-tab-${next.id}`)?.focus();
	}
</script>

{#snippet note(text: string, testidValue: string, action?: Snippet)}
	<div data-testid={testidValue} class="note m-4 flex max-w-prose flex-wrap items-start gap-x-3 gap-y-2 px-3 py-2 text-sm">
		<span class="caps text-ink-muted">Note</span>
		<p class="min-w-0 flex-1">{text}</p>
		{#if action}{@render action()}{/if}
	</div>
{/snippet}

{#snippet drawAllButton()}
	<button type="button" data-testid={on === 'run' ? 'run-dag-draw-all' : 'dag-draw-all'} class="btn btn-sm" onclick={() => (drawAll = true)}
		>Show the DAG of all {fmt.format(tasks?.length ?? 0)} tasks</button
	>
{/snippet}

<section
	data-testid={testid}
	aria-label="Tasks of {orkflowLabel}"
	class="frame relative isolate flex flex-col overflow-hidden {view === 'dag' ? 'max-h-drawing' : 'h-drawing'}"
>
	<div
		class="relative z-10 flex h-8 shrink-0 items-stretch self-start border-r border-b border-ink bg-paper {drawn && (hidden.left || hidden.right)
			? 'max-w-[calc(100%-4rem)]'
			: 'max-w-full'}"
	>
		{#if views.length > 1}
			<div role="tablist" aria-label="View" class="flex shrink-0">
				{#each views as v, i (v.id)}
					<button
						type="button"
						role="tab"
						id="{uid}-tab-{v.id}"
						data-testid={v.testid}
						aria-selected={view === v.id}
						aria-controls="{uid}-panel"
						tabindex={view === v.id ? 0 : -1}
						onclick={() => onview?.(v.id)}
						onkeydown={(e) => onTabKey(e, i)}
						class="caps border-r border-rule px-3 whitespace-nowrap {view === v.id
							? 'bg-paper text-ink underline decoration-2 decoration-blue underline-offset-4'
							: 'text-ink-muted hover:text-ink'}">{v.label}</button
					>
				{/each}
			</div>
		{/if}
		<p class="flex min-w-0 items-center px-3 font-mono text-xs text-ink-secondary" title={caption}><span class="truncate">{caption}</span></p>
	</div>
	<div
		bind:this={panel}
		onscroll={measure}
		id="{uid}-panel"
		role={views.length > 1 ? 'tabpanel' : undefined}
		aria-labelledby={views.length > 1 ? `${uid}-tab-${view}` : undefined}
		class="min-h-0 flex-1 {view === 'dag' ? 'drafting-grid overflow-auto' : 'overflow-hidden'}"
	>
		{#if view === 'map' && map}
			{@render map()}
		{:else if !tasks}
			{@render note(
				`The DAG needs ${orkflowLabel}'s definition, which could not be read: ${unavailable}. The task map and every task below are unaffected.`,
				'run-drawing-unavailable'
			)}
		{:else if bound === 'draw' || (bound === 'on-request' && drawAll)}
			<div class="p-2">
				<DagGraph tasks={tasks} {on} label={dagLabel} {stateOf} {selected} {hrefOf} {onselect} />
			</div>
		{:else if bound === 'on-request'}
			{@render note(
				`This Orkflow declares ${fmt.format(tasks.length)} tasks. A DAG of that many is drawn on request.`,
				on === 'run' ? 'run-dag-large' : 'dag-large',
				drawAllButton
			)}
		{:else}
			{@render note(
				`This Orkflow declares ${fmt.format(tasks.length)} tasks, past the ${DAG_LEGIBLE_LIMIT} a legible DAG can show. ${
					neighbourhoodWhenLarge
						? selected
							? `Shown: ${selected}, what it needs and what needs it.`
							: 'Select a task in the task map to see what it needs and what needs it.'
						: largeHint
				}`,
				on === 'run' ? 'run-dag-too-large' : 'dag-too-large'
			)}
			{#if near.length}
				<div class="p-2">
					<DagGraph tasks={near} {on} label={dagLabel} {stateOf} {selected} {hrefOf} {onselect} />
				</div>
			{/if}
		{/if}
	</div>
	{#if drawn && (hidden.left || hidden.right)}
		<div class="absolute top-0 right-0 z-10 flex h-8 border-b border-l border-ink bg-paper">
			<button
				type="button"
				data-testid="{prefix}-scroll-left"
				class="inline-flex size-8 items-center justify-center border-r border-rule text-ink hover:bg-hover aria-disabled:cursor-default aria-disabled:text-ink-muted aria-disabled:opacity-50"
				aria-label="Scroll the DAG left"
				title="More of the DAG to the left"
				aria-disabled={!hidden.left}
				onclick={() => hidden.left && scrollBy(-1)}><Icon name="chevronLeft" /></button
			>
			<button
				type="button"
				data-testid="{prefix}-scroll-right"
				class="inline-flex size-8 items-center justify-center text-ink hover:bg-hover aria-disabled:cursor-default aria-disabled:text-ink-muted aria-disabled:opacity-50"
				aria-label="Scroll the DAG right"
				title="More of the DAG to the right"
				aria-disabled={!hidden.right}
				onclick={() => hidden.right && scrollBy(1)}><Icon name="chevronRight" /></button
			>
		</div>
	{/if}
</section>
