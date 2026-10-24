<script lang="ts">
	// The task map: every task and every fan-out item of a run as one cell,
	// drawn by status, on one <canvas>. See $lib/taskMap for the model, the
	// geometry and the keys; this component only wires them to the page.
	//
	// NOTHING here runs on the path that draws the run's header and cards. The
	// first render is the empty map area at the frame's CSS height and one
	// muted line; an effect after it asks for ONE idle callback, which orders
	// the tasks, fills the status bytes and paints. Live changes arrive as a
	// `revision` the page bumps once per event batch: the map re-reads the
	// statuses then, at most once per animation frame, and repaints only the
	// cells that moved. Svelte never tracks 14,000 statuses.
	//
	// Keyboard and screen readers: the canvas is aria-hidden; over it sits one
	// focusable listbox holding ONE visually hidden option, the cursor's cell,
	// whose id alternates on every move so the change is announced. Every cell
	// has a full-size equivalent below the map (its task's card, its item's
	// button), which is why cells may be smaller than 24 px (WCAG 2.5.8).
	import { onDestroy, untrack } from 'svelte';
	import { Status } from '$lib/gen/orkstack/v1/common_pb';
	import { layerOrder } from '$lib/dag';
	import {
		LABEL_H,
		browserIdle,
		buildModel,
		cellAt,
		countStatuses,
		fillStatuses,
		mapGeometry,
		mapKeydown,
		mapSummary,
		paintCells,
		readColors,
		readoutPosition,
		rectOf,
		runningPulse,
		scrollForCursor,
		type MapColors,
		type MapGeometry,
		type MapModel,
		type MapTask
	} from '$lib/taskMap';

	interface Props {
		/** The run's tasks as the map reads them; called only in idle time and per revision. */
		source: () => MapTask[];
		/** Whether the tasks carry their definition's needs (DAG order), or only the run's order. */
		definition: boolean;
		/** Bumped by the page once per event batch. */
		revision: number;
		/** The selected task, and item. */
		selected: { taskId: string; item?: number } | null;
		onselect: (taskId: string, item?: number) => void;
		/** The readout's words for a cell: "t123 · FAILED · CONTAINER_TIMEOUT · attempt 1 of 3". */
		describe: (taskId: string, item?: number) => string;
		/** Reports the legend once the map has painted, and on every change. */
		onlegend?: (counts: Map<Status, number>) => void;
	}

	let { source, definition, revision, selected, onselect, describe, onlegend }: Props = $props();

	const uid = $props.id();

	let area = $state<HTMLDivElement | undefined>(undefined);
	let canvas = $state<HTMLCanvasElement | undefined>(undefined);
	let width = $state(0);
	let height = $state(0);
	let painted = $state(false);
	// The model is replaced whole, never mutated; `built` counts the builds.
	let model = $state.raw<MapModel | null>(null);
	let statuses = new Uint8Array(0);
	let built = $state(0);
	let colors: MapColors | null = null;

	let cursor = $state(0);
	let focused = $state(false);
	let hover = $state(-1);
	let optionFlip = $state(false);
	let counts = $state.raw(new Map<Status, number>());

	const geometry = $derived.by((): MapGeometry | null => {
		void built;
		if (!model || width <= 0) return null;
		return mapGeometry(
			model.segments.map((s) => ({ count: s.count, labelled: s.labelled })),
			width,
			height
		);
	});

	const selectedIndex = $derived.by(() => {
		void built;
		if (!model || !selected) return -1;
		return model.indexOf(selected.taskId, selected.item);
	});

	// ---- Size: the area's content box, from a ResizeObserver (after layout,
	// so reading it forces no reflow of the cards below). ----
	$effect(() => {
		if (!area || typeof ResizeObserver === 'undefined') return;
		const observer = new ResizeObserver(([entry]) => {
			width = Math.floor(entry.contentRect.width);
			height = Math.floor(entry.contentRect.height);
		});
		observer.observe(area);
		return () => observer.disconnect();
	});

	// ---- The one idle build ----
	let cancelIdle: (() => void) | null = null;
	$effect(() => {
		cancelIdle = browserIdle(() => {
			cancelIdle = null;
			rebuild();
		});
		return () => cancelIdle?.();
	});

	// The shape the model was built for: the task count and each fan-out's
	// item count. A live batch that changes it rebuilds; any other repaints.
	let builtFor = '';
	function rebuild(): void {
		const tasks = untrack(source);
		builtFor = structureOf(tasks);
		model = buildModel(tasks, untrack(() => definition), layerOrder);
		statuses = new Uint8Array(model.cells.length);
		fillStatuses(model, tasks, statuses);
		counts = countStatuses(statuses, model.kinds);
		onlegend?.(counts);
		built++;
	}

	function structureOf(tasks: MapTask[]): string {
		let sig = `${tasks.length}`;
		for (const t of tasks) if (t.items) sig += `:${t.id}=${t.items.length}`;
		return sig;
	}

	// ---- Live: once per revision, at most once per frame ----
	let frame = 0;
	$effect(() => {
		void revision;
		if (!untrack(() => built) || frame) return;
		frame = requestAnimationFrame(() => {
			frame = 0;
			if (!model) return;
			const tasks = untrack(source);
			if (structureOf(tasks) !== builtFor) {
				rebuild();
				return;
			}
			const dirty = fillStatuses(model, tasks, statuses);
			if (dirty.length === 0) return;
			counts = countStatuses(statuses, model.kinds);
			onlegend?.(counts);
			paint(dirty);
		});
	});
	onDestroy(() => {
		if (frame) cancelAnimationFrame(frame);
	});

	// ---- Paint ----
	function paint(only?: number[], runningAlpha = 1): void {
		const g = geometry;
		if (!canvas || !g || !model) return;
		colors ??= readColors();
		const dpr = globalThis.devicePixelRatio || 1;
		const w = g.columns * g.pitch;
		const ctx = canvas.getContext('2d');
		if (!ctx) return;
		if (!only) {
			canvas.width = Math.ceil(w * dpr);
			canvas.height = Math.ceil(g.height * dpr);
			canvas.style.width = `${w}px`;
			canvas.style.height = `${g.height}px`;
		}
		ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
		paintCells(ctx, g, statuses, model.kinds, colors, only, runningAlpha);
		if (!only) painted = true;
	}

	// ---- Running cells breathe ----
	// While any cell is RUNNING, its mark fades and returns every 2.4 s, as a
	// running DAG node's ground does: only those cells are repainted, at most
	// fifteen times a second, and not at all while the page is hidden. Under
	// reduced motion nothing moves and the mark stays whole.
	$effect(() => {
		if (!painted || (counts.get(Status.RUNNING) ?? 0) === 0) return;
		if (typeof matchMedia !== 'function' || typeof requestAnimationFrame !== 'function') return;
		const still = matchMedia('(prefers-reduced-motion: reduce)');
		const runningCells = () => {
			const out: number[] = [];
			for (let i = 0; i < statuses.length; i++) if (statuses[i] === Status.RUNNING) out.push(i);
			return out;
		};
		let pulse = 0;
		let last = 0;
		let faded = false;
		const tick = (t: number) => {
			pulse = requestAnimationFrame(tick);
			if (still.matches || document.hidden) {
				// Stopping mid-breath leaves the cells whole, not half faded.
				if (faded) paint(runningCells());
				faded = false;
				return;
			}
			if (t - last < 66) return;
			last = t;
			paint(runningCells(), runningPulse(t));
			faded = true;
		};
		pulse = requestAnimationFrame(tick);
		return () => {
			cancelAnimationFrame(pulse);
			if (faded) paint(runningCells());
		};
	});

	// A full paint whenever the geometry changes (first build, resize).
	$effect(() => {
		const g = geometry;
		if (!g || !canvas) return;
		untrack(() => paint());
	});

	// Colours follow the theme and forced colours.
	$effect(() => {
		if (typeof MutationObserver === 'undefined') return;
		const repaint = () => {
			colors = null;
			paint();
		};
		const observer = new MutationObserver(repaint);
		observer.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
		const forced = typeof matchMedia === 'function' ? matchMedia('(forced-colors: active)') : null;
		forced?.addEventListener('change', repaint);
		return () => {
			observer.disconnect();
			forced?.removeEventListener('change', repaint);
		};
	});

	// ---- Pointer and keys ----
	function cellFromPointer(event: PointerEvent | MouseEvent): number {
		const g = geometry;
		if (!g || !area) return -1;
		const box = (event.currentTarget as HTMLElement).getBoundingClientRect();
		return cellAt(g, event.clientX - box.left, event.clientY - box.top);
	}

	function choose(i: number): void {
		if (!model || i < 0) return;
		const cell = model.cells[i];
		cursor = i;
		onselect(cell.taskId, cell.item);
	}

	function onkeydown(event: KeyboardEvent): void {
		const g = geometry;
		if (!g || !model) return;
		const visibleRows = Math.max(1, Math.floor(height / g.pitch));
		const result = mapKeydown(g, cursor, event, { statuses, visibleRows });
		if (!result) return;
		event.preventDefault();
		cursor = result.cursor;
		optionFlip = !optionFlip;
		if (area) area.scrollTop = scrollForCursor(g, cursor, area.scrollTop, height);
		if (result.select) choose(cursor);
	}

	/** Moves the cursor to the first cell of a status (the legend's buttons). */
	export function focusStatus(status: Status): void {
		const i = statuses.indexOf(status);
		if (i < 0) return;
		cursor = i;
		optionFlip = !optionFlip;
		const g = geometry;
		if (g && area) area.scrollTop = scrollForCursor(g, cursor, area.scrollTop, height);
		listbox?.focus();
	}

	let listbox = $state<HTMLDivElement | undefined>(undefined);
	let root = $state<HTMLDivElement | undefined>(undefined);
	let readoutEl = $state<HTMLDivElement | undefined>(undefined);
	let scrollTop = $state(0);

	// What the readout describes: the hovered cell, else the cursor while focused.
	const readoutCell = $derived(hover >= 0 ? hover : focused ? cursor : -1);
	const readoutText = $derived.by(() => {
		void built;
		if (!model || readoutCell < 0 || readoutCell >= model.cells.length) return '';
		const c = model.cells[readoutCell];
		return describe(c.taskId, c.item);
	});
	let readoutSize = $state({ w: 0, h: 0 });
	$effect(() => {
		void readoutText;
		if (readoutEl) readoutSize = { w: readoutEl.offsetWidth, h: readoutEl.offsetHeight };
	});
	const PAD = 8;
	const readoutAt = $derived.by(() => {
		const g = geometry;
		if (!g || readoutCell < 0 || !root) return null;
		const r = rectOf(g, readoutCell);
		const frame = { w: root.clientWidth, h: root.clientHeight };
		return readoutPosition({ ...r, x: r.x + PAD, y: r.y + PAD - scrollTop }, readoutSize, frame);
	});

	const cursorRect = $derived(geometry && model && model.cells.length ? rectOf(geometry, Math.min(cursor, model.cells.length - 1)) : null);
	const selectedRect = $derived(geometry && selectedIndex >= 0 ? rectOf(geometry, selectedIndex) : null);

	const summary = $derived(model ? mapSummary(counts, model.cells.length, model.order) : '');
	const optionText = $derived.by(() => {
		void built;
		if (!model || !model.cells.length) return '';
		const c = model.cells[Math.min(cursor, model.cells.length - 1)];
		return describe(c.taskId, c.item);
	});
	const taskCount = $derived.by(() => {
		void built;
		return model ? model.segments[0].count : 0;
	});
	const sections = $derived.by(() => {
		const g = geometry;
		if (!g || !model) return [];
		return model.segments.slice(1).map((s, k) => {
			let failed = 0;
			for (let i = s.start; i < s.start + s.count; i++) if (statuses[i] === Status.FAILED) failed++;
			void counts;
			return { taskId: s.taskId ?? '', count: s.count, failed, y: g.segments[k + 1].y - LABEL_H };
		});
	});
	const fmt = new Intl.NumberFormat('en-US');
	const selectedAttr = $derived(selected ? (selected.item === undefined ? selected.taskId : `${selected.taskId}[${selected.item}]`) : '');
</script>

<div
	bind:this={root}
	data-testid="task-map"
	data-cells={model ? model.cells.length : undefined}
	data-columns={geometry?.columns}
	data-pitch={geometry?.pitch}
	data-selected={selectedAttr || undefined}
	data-order={model?.order}
	data-painted={painted ? 'true' : undefined}
	class="relative h-full"
>
	<div
		bind:this={area}
		data-testid="task-map-area"
		class="h-full overflow-auto p-2"
		onscroll={() => (scrollTop = area?.scrollTop ?? 0)}
	>
		{#if !painted}
			<p class="text-sm text-ink-muted">Laying out the task map…</p>
		{/if}
		<div
			class="relative"
			style:width={geometry ? `${geometry.columns * geometry.pitch}px` : undefined}
			style:height={geometry ? `${geometry.height}px` : undefined}
		>
			<canvas bind:this={canvas} data-testid="task-map-canvas" aria-hidden="true" class="block"></canvas>
			{#each sections as s (s.taskId)}
				<div
					data-testid="task-map-section-{s.taskId}"
					class="caps absolute left-0 whitespace-nowrap text-ink-muted"
					style:top="{s.y}px"
				>
					{s.taskId} · {fmt.format(s.count)} items{#if s.failed} · <span class="text-red">{fmt.format(s.failed)} failed</span>{/if}
				</div>
			{/each}
			{#if selectedRect}
				<div
					aria-hidden="true"
					class="pointer-events-none absolute border-2 border-ink"
					style:left="{selectedRect.x - 2}px"
					style:top="{selectedRect.y - 2}px"
					style:width="{selectedRect.w + 4}px"
					style:height="{selectedRect.h + 4}px"
				></div>
			{/if}
			{#if focused && cursorRect}
				<div
					data-testid="task-map-cursor"
					aria-hidden="true"
					class="pointer-events-none absolute border-2 border-focus"
					style:left="{cursorRect.x - 3}px"
					style:top="{cursorRect.y - 3}px"
					style:width="{cursorRect.w + 6}px"
					style:height="{cursorRect.h + 6}px"
				></div>
			{/if}
			{#if model && geometry}
				<div
					bind:this={listbox}
					role="listbox"
					tabindex="0"
					aria-label="Task map, {fmt.format(taskCount)} {taskCount === 1 ? 'task' : 'tasks'}"
					aria-describedby="{uid}-summary"
					aria-activedescendant="{uid}-option-{optionFlip ? 'b' : 'a'}"
					class="absolute inset-0 cursor-pointer"
					onfocus={() => (focused = true)}
					onblur={() => (focused = false)}
					onpointermove={(e) => (hover = cellFromPointer(e))}
					onpointerleave={() => (hover = -1)}
					onclick={(e) => choose(cellFromPointer(e))}
					{onkeydown}
				>
					<div
						id="{uid}-option-{optionFlip ? 'b' : 'a'}"
						role="option"
						aria-selected={cursor === selectedIndex}
						aria-setsize={model.cells.length}
						aria-posinset={Math.min(cursor, model.cells.length - 1) + 1}
						class="sr-only"
					>
						{optionText}
					</div>
				</div>
			{/if}
		</div>
	</div>
	<p id="{uid}-summary" class="sr-only">{summary}</p>
	<div
		bind:this={readoutEl}
		data-testid="task-map-readout"
		aria-hidden="true"
		class="pointer-events-none absolute z-10 border border-ink bg-sheet px-2 py-1 font-mono text-xs whitespace-nowrap text-ink shadow-overlay {readoutAt
			? ''
			: 'invisible'}"
		style:left="{readoutAt?.left ?? 0}px"
		style:top="{readoutAt?.top ?? 0}px"
	>
		{readoutText}
	</div>
</div>
