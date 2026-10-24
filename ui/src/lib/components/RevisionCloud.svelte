<script lang="ts">
	// A run's failure, drawn the way a drawing marks what changed: a scalloped
	// red outline (an arc every ~14 px, computed from the box) round the
	// reason. Before the box is measured — the first paint — it is a dashed
	// red outline with a rounded corner, so nothing jumps when the scallops
	// arrive.
	//
	// It appears once per page, round the run's own failure. Task and item
	// failures keep their compact panel: clouds everywhere would stop meaning
	// anything.
	//
	// The one authored motion of the interface: when a run turns FAILED while
	// someone is watching it, the outline draws itself in once. On load it is
	// simply there, and under reduced motion it never moves.
	import type { Snippet } from 'svelte';

	interface Props {
		/** "VERSION 0.4.2 · charge failed" */
		tag: string;
		/** "attempt 1 of 1" */
		attempts?: string;
		/** Draw the outline in (the run failed while it was watched). */
		animate?: boolean;
		children: Snippet;
	}

	let { tag, attempts = '', animate = false, children }: Props = $props();

	let box = $state<HTMLDivElement | undefined>(undefined);
	let size = $state({ w: 0, h: 0 });

	$effect(() => {
		if (!box || typeof ResizeObserver === 'undefined') return;
		const observer = new ResizeObserver(([entry]) => {
			const r = entry.borderBoxSize?.[0];
			size = r ? { w: r.inlineSize, h: r.blockSize } : { w: box!.offsetWidth, h: box!.offsetHeight };
		});
		observer.observe(box);
		return () => observer.disconnect();
	});

	// Drawing geometry: the scallop step and how far inside the box it runs.
	const STEP = 14;
	const INSET = 7;

	/** The outline: arcs bulging outward, clockwise from the top-left corner. */
	function scallops(w: number, h: number): string {
		const x0 = INSET;
		const y0 = INSET;
		const iw = w - 2 * INSET;
		const ih = h - 2 * INSET;
		if (iw <= 0 || ih <= 0) return '';
		const across = Math.max(1, Math.round(iw / STEP));
		const down = Math.max(1, Math.round(ih / STEP));
		const sx = iw / across;
		const sy = ih / down;
		const arc = (dx: number, dy: number, r: number) => `a ${r} ${r} 0 0 1 ${dx} ${dy}`;
		const parts = [`M ${x0} ${y0}`];
		for (let i = 0; i < across; i++) parts.push(arc(sx, 0, sx / 2));
		for (let i = 0; i < down; i++) parts.push(arc(0, sy, sy / 2));
		for (let i = 0; i < across; i++) parts.push(arc(-sx, 0, sx / 2));
		for (let i = 0; i < down; i++) parts.push(arc(0, -sy, sy / 2));
		return parts.join(' ') + ' Z';
	}

	const path = $derived(size.w > 0 ? scallops(size.w, size.h) : '');
</script>

<div bind:this={box} class="relative {path ? '' : 'rounded-cloud border-2 border-dashed border-red'}">
	{#if path}
		<svg aria-hidden="true" class="pointer-events-none absolute inset-0 overflow-visible" width={size.w} height={size.h}>
			<path d={path} pathLength="1" class="cloud-line {animate ? 'cloud-draw' : ''}" />
		</svg>
	{/if}
	<div class="relative px-5 py-4">
		<p class="caps mb-2 text-red-strong" data-testid="run-error-tag">{tag}</p>
		{@render children()}
		{#if attempts}<p class="figure mt-2 text-xs text-ink-secondary" data-testid="run-error-attempts">{attempts}</p>{/if}
	</div>
</div>

<style>
	.cloud-line {
		fill: none;
		stroke: var(--color-red);
		stroke-width: var(--rule-region);
		stroke-linejoin: round;
	}
	/* pathLength="1": one dash the length of the outline, drawn in from an
	   offset of one. */
	.cloud-draw {
		stroke-dasharray: 1;
		animation: var(--animate-cloud-draw);
	}
	@media (prefers-reduced-motion: reduce) {
		.cloud-draw {
			animation: none;
		}
	}
	@media (forced-colors: active) {
		.cloud-line {
			stroke: CanvasText;
		}
	}
</style>
