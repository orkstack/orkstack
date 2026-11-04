<script module lang="ts">
	export type DrawingView = 'dag' | 'map';
</script>

<script lang="ts">
	// The frame a run's (or an Orkflow's) tasks are drawn in: a box with a
	// 1.5 px ink rule, its view tabs and caption on a paper patch at the top
	// left, and — behind the DAG only — the drafting grid. It is its content
	// plus the patch and padding, up to --spacing-drawing
	// (min(--spacing-drawing, content + 48 px)), so a six-task run is not a
	// page of empty frame: the DAG's height is the drawing's own (CSS, from
	// its aspect), the task map's is computed from the frame's width before
	// first paint (mapHeight).
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
	import type { Task } from '#lib/gen/orkstack/v1/orkflow_pb.ts';
	import type { DeclaredIcon } from '#lib/activityIcon.ts';
	import DagGraph from './DagGraph.svelte';
	import Icon from './Icon.svelte';
	import { DAG_LEGIBLE_LIMIT, chooseDagDirection, dagBound, neighbourhood, type DagDirection, type NodeState } from '#lib/dag.ts';
	import { mapGeometry } from '#lib/taskMap.ts';

	interface Props {
		testid: string;
		/** "order-saga @ 0.4.2 · 6 tasks" */
		caption: string;
		/** The part of the caption that may be shortened to fit ("order-saga"); the rest is kept whole. */
		captionLead?: string;
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
		/**
		 * The task map's sections (its tasks, then each fan-out's items), for
		 * sizing its frame: like the DAG's, the map's frame is its content plus
		 * the patch and padding, up to --spacing-drawing.
		 */
		mapSections?: { count: number; labelled: boolean }[];
		/**
		 * Which way the DAG reads: left to right (the default), top to
		 * bottom, or `auto` — whichever leaves less of it to scroll in this
		 * frame (#lib/dag.ts chooseDagDirection).
		 */
		direction?: DagDirection | 'auto';
		/** The card's heading: with one the head is a card head, not the caption band. */
		title?: string;
		sub?: string;
		/** Controls at the head's end, beside Fit. */
		actions?: Snippet;
		/** The row under the drawing (its legend). */
		footer?: Snippet;
		/** What a task's image declares as its icon. */
		iconOf?: (id: string) => DeclaredIcon | undefined;
	}

	let {
		testid,
		caption,
		captionLead = undefined,
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
		map,
		mapSections = undefined,
		direction = 'LR',
		title = undefined,
		sub = '',
		actions = undefined,
		footer = undefined,
		iconOf = undefined
	}: Props = $props();

	const uid = $props.id();
	let drawAll = $state(false);
	let fit = $state(false);
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
	let hidden = $state({ left: false, right: false, up: false, down: false });
	// A drawing that does not fit is a scroll region a keyboard can reach (WCAG 2.1.1).
	const overflowing = $derived(drawn && (hidden.left || hidden.right || hidden.up || hidden.down));

	function measure(): void {
		if (!panel) return;
		const maxX = panel.scrollWidth - panel.clientWidth;
		const maxY = panel.scrollHeight - panel.clientHeight;
		hidden = {
			left: panel.scrollLeft > 1,
			right: panel.scrollLeft < maxX - 1,
			up: panel.scrollTop > 1,
			down: panel.scrollTop < maxY - 1
		};
	}

	$effect(() => {
		const el = panel;
		if (!el || !drawn || typeof ResizeObserver === 'undefined') {
			hidden = { left: false, right: false, up: false, down: false };
			return;
		}
		const observer = new ResizeObserver(measure);
		observer.observe(el);
		const drawing = el.querySelector('svg');
		if (drawing) observer.observe(drawing);
		// The observer answers for what it was just given, once the page has laid
		// out: measuring here would make the browser lay it out in mid-render.
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

	// A drawing that does not fit is centred on the task a reader came for: on
	// a phone the first view is the one that matters. A later selection is
	// followed the same way, but only when it is out of view.
	function bringIntoView(el: HTMLDivElement, onlyIfHidden = false): void {
		if (el.scrollWidth <= el.clientWidth + 1 && el.scrollHeight <= el.clientHeight + 1) return;
		const target = el.querySelector('.dag-node.is-selected') ?? el.querySelector('.dag-node.is-failed');
		if (!target) return;
		const node = target.getBoundingClientRect();
		const frame = el.getBoundingClientRect();
		if (onlyIfHidden && node.left >= frame.left && node.right <= frame.right && node.top >= frame.top && node.bottom <= frame.bottom) return;
		el.scrollLeft += node.left + node.width / 2 - (frame.left + frame.width / 2);
		if (node.top < frame.top || node.bottom > frame.bottom) el.scrollTop += node.top + node.height / 2 - (frame.top + frame.height / 2);
		measure();
	}
	$effect(() => {
		void selected;
		const el = panel;
		if (!el || !placed) return;
		void tick().then(() => bringIntoView(el, true));
	});

	// The task map's frame: as tall as the map the frame's width lays out at
	// the largest pitch that fits, plus the 32 px patch, the 16 px padding and
	// the 3 px of rules; the whole --spacing-drawing when even the smallest
	// pitch does not fit (the map then scrolls inside it). Measured once the
	// frame is laid out, before its first paint, from the same geometry the
	// map itself is drawn with.
	let section = $state<HTMLElement | undefined>(undefined);
	let frameBox = $state({ width: 0, max: 0 });
	$effect(() => {
		const el = section;
		if (!el || (!mapSections && direction !== 'auto') || typeof ResizeObserver === 'undefined') return;
		const observer = new ResizeObserver(() => {
			frameBox = { width: el.clientWidth, max: parseFloat(getComputedStyle(el).maxHeight) || 0 };
		});
		observer.observe(el);
		return () => observer.disconnect();
	});
	const MAP_CHROME = 32 + 16 + 3;
	// `auto`: decided once the frame is laid out (before its first paint),
	// for the tasks it draws.
	const dagDirection = $derived.by((): DagDirection => {
		if (direction !== 'auto') return direction;
		const drawnTasks = near.length ? near : tasks;
		if (!drawn || !drawnTasks || frameBox.width <= 0 || frameBox.max <= 0) return 'LR';
		return chooseDagDirection(drawnTasks, { width: frameBox.width - 16, height: frameBox.max - MAP_CHROME });
	});
	const mapHeight = $derived.by(() => {
		if (view !== 'map' || !mapSections || frameBox.width <= 0 || frameBox.max <= 0) return undefined;
		const room = frameBox.max - MAP_CHROME;
		const g = mapGeometry(mapSections, frameBox.width - 16, room);
		// At least one line of text: the map's "Laying out…" before it paints.
		return g.scroll > 0 ? frameBox.max : MAP_CHROME + Math.max(g.height, 20);
	});

	function scrollBy(direction: 1 | -1, axis: 'x' | 'y' = 'x'): void {
		const still = matchMedia('(prefers-reduced-motion: reduce)').matches;
		if (!panel) return;
		const step = axis === 'x' ? { left: direction * panel.clientWidth * 0.75 } : { top: direction * panel.clientHeight * 0.75 };
		panel.scrollBy({ ...step, behavior: still ? 'auto' : 'smooth' });
	}

	function onTabKey(event: KeyboardEvent, index: number): void {
		if (event.key !== 'ArrowRight' && event.key !== 'ArrowLeft') return;
		event.preventDefault();
		const next = views[(index + (event.key === 'ArrowRight' ? 1 : views.length - 1)) % views.length];
		onview?.(next.id);
		document.getElementById(`${uid}-tab-${next.id}`)?.focus();
	}
</script>

{#snippet scroller(testid: string, label: string, title: string, icon: 'chev-left' | 'chev-right' | 'chev-up' | 'chev', more: boolean, go: () => void, last: boolean)}
	<button
		type="button"
		data-testid={testid}
		class="inline-flex size-8 items-center justify-center text-ink hover:bg-hover aria-disabled:cursor-default aria-disabled:text-ink-3 {last ? '' : 'border-r border-line'}"
		aria-label={label}
		data-tip={title}
		aria-disabled={!more}
		onclick={() => more && go()}><Icon name={icon} /></button
	>
{/snippet}

{#snippet scrollers()}
	<!-- A phone swipes its drawing, and a keyboard scrolls its region: the arrows are for a pointer. -->
	<div class="flex h-8 overflow-hidden rounded-sm border border-line-2 bg-surface max-sm:hidden">
		{#if hidden.up || hidden.down}
			{@render scroller(`${prefix}-scroll-up`, 'Scroll the DAG up', 'More of the DAG above', 'chev-up', hidden.up, () => scrollBy(-1, 'y'), false)}
			{@render scroller(`${prefix}-scroll-down`, 'Scroll the DAG down', 'More of the DAG below', 'chev', hidden.down, () => scrollBy(1, 'y'), !(hidden.left || hidden.right))}
		{/if}
		{#if hidden.left || hidden.right}
			{@render scroller(`${prefix}-scroll-left`, 'Scroll the DAG left', 'More of the DAG to the left', 'chev-left', hidden.left, () => scrollBy(-1), false)}
			{@render scroller(`${prefix}-scroll-right`, 'Scroll the DAG right', 'More of the DAG to the right', 'chev-right', hidden.right, () => scrollBy(1), true)}
		{/if}
	</div>
{/snippet}

{#snippet note(text: string, testidValue: string, action?: Snippet)}
	<div data-testid={testidValue} class="note m-4 flex max-w-prose flex-wrap items-start gap-x-3 gap-y-2 px-3 py-2 text-dense">
		<span class="font-semibold text-ink-3">Note</span>
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
	bind:this={section}
	data-testid={testid}
	aria-label="Tasks of {orkflowLabel}"
	class="card relative isolate flex flex-col overflow-hidden max-h-drawing max-sm:max-h-drawing-tall {view === 'dag' || mapHeight !== undefined ? '' : 'h-drawing'}"
	style:height={mapHeight !== undefined ? `${mapHeight}px` : undefined}
>
	{#if title}
		<div class="card-head shrink-0">
			<h2>{title}</h2>
			{#if sub}<span class="card-sub" data-testid="{testid}-sub">{sub}</span>{/if}
			<div class="card-end">
				{#if drawn && (hidden.left || hidden.right || hidden.up || hidden.down)}{@render scrollers()}{/if}
				{#if actions}{@render actions()}{/if}
				{#if drawn}
					<button type="button" class="btn btn-sm btn-quiet" data-testid="{prefix}-fit" aria-pressed={fit} onclick={() => (fit = !fit)}>Fit</button>
				{/if}
			</div>
		</div>
	{:else}
	<div
		class="relative z-10 flex min-h-12 shrink-0 flex-wrap items-center gap-3 self-stretch border-b border-line bg-surface px-3 py-2 {!drawn
			? 'max-w-full'
			: (hidden.left || hidden.right) && (hidden.up || hidden.down)
				? 'max-w-[calc(100%-8rem)]'
				: hidden.left || hidden.right || hidden.up || hidden.down
					? 'max-w-[calc(100%-4rem)]'
					: 'max-w-full'}"
	>
		{#if views.length > 1}
			<div role="tablist" aria-label="View" class="seg">
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
						>{v.label}</button
					>
				{/each}
			</div>
		{/if}
		<!-- The Orkflow's name gives way; its version and task count never do. -->
		<p class="flex min-w-0 items-center text-dense text-ink-3" data-tip={caption}>
			{#if captionLead && caption.startsWith(captionLead)}<span class="truncate">{captionLead}</span><span class="shrink-0 whitespace-pre"
					>{caption.slice(captionLead.length)}</span
				>{:else}<span class="truncate">{caption}</span>{/if}
		</p>
	</div>
	{/if}
	<!-- svelte-ignore a11y_no_noninteractive_tabindex -->
	<div
		bind:this={panel}
		onscroll={measure}
		id="{uid}-panel"
		role={views.length > 1 ? 'tabpanel' : overflowing ? 'region' : undefined}
		aria-labelledby={views.length > 1 ? `${uid}-tab-${view}` : undefined}
		aria-label={views.length <= 1 && overflowing ? dagLabel : undefined}
		tabindex={views.length <= 1 && overflowing ? 0 : undefined}
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
				<DagGraph tasks={tasks} {on} label={dagLabel} {stateOf} {selected} {hrefOf} {onselect} direction={dagDirection} {iconOf} {fit} />
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
					<DagGraph tasks={near} {on} label={dagLabel} {stateOf} {selected} {hrefOf} {onselect} direction={dagDirection} {iconOf} {fit} />
				</div>
			{/if}
		{/if}
	</div>
	{#if !title && drawn && (hidden.left || hidden.right || hidden.up || hidden.down)}
		<!-- A drawing that does not fit says so on the frame's top band, for
		     each way it overflows: the pair for a way with more to show. -->
		<div class="absolute top-2 right-2 z-10">{@render scrollers()}</div>
	{/if}
	{#if footer}{@render footer()}{/if}
</section>

<style>
	.seg {
		display: inline-flex;
		flex: none;
		gap: 2px;
		padding: 3px;
		border: 1px solid var(--line);
		border-radius: 8px;
		background: var(--surface-2);
	}
	.seg button {
		display: inline-flex;
		align-items: center;
		height: 28px;
		padding: 0 12px;
		border: 0;
		border-radius: 6px;
		background: transparent;
		color: var(--ink-2);
		font-size: 13.5px;
		font-weight: 600;
		white-space: nowrap;
		cursor: pointer;
	}
	.seg button[aria-selected='true'] {
		background: var(--surface);
		color: var(--navy-text);
		box-shadow:
			0 0 0 1px var(--line-2),
			var(--shadow),
			inset 0 -2px 0 var(--navy-text);
		font-weight: 700;
	}
	.seg button:focus-visible {
		outline-offset: -2px;
	}
	@media (forced-colors: active) {
		.seg button[aria-selected='true'] {
			outline: 2px solid Highlight;
		}
	}
</style>

