<script lang="ts">
	// Layout C, the one layout of both Orkflow editors — the Create Orkflow
	// page and an Orkflow's Edit tab: the editor card, with what a create
	// answered in a notice slot under its header, beside the docs rail of the
	// task the caret is in (EditorDocsRail).
	//
	// The rail starts open from 1280 px, the floor the UI is drawn for, and
	// shut below it: a 44 px strip beside the card down to 1181 px, then, in
	// one column, a bar held at the viewport's foot. The toggle decides from
	// then on, for as long as the editor is shown.
	import type { Snippet } from 'svelte';
	import EditorDocsRail from './EditorDocsRail.svelte';
	import type { YamlSession } from '#lib/yaml/session.svelte.ts';

	interface Props {
		/** The document's session: the rail shows the task at its caret. */
		session: YamlSession;
		/** The card heading's id, which names the card. */
		labelledby: string;
		/** The page's test-id prefix: the notice slot is `{testid}-notices`. */
		testid: string;
		/** Whether the notice slot holds an answer; empty, it takes no room. */
		noticed: boolean;
		/** The card header: its heading, its subtitle and a `.card-end`. */
		head: Snippet;
		/** What a create answered. */
		notices: Snippet;
		/** The rest of the card: the YAML field and anything above it. */
		children: Snippet;
	}

	let { session, labelledby, testid, noticed, head, notices, children }: Props = $props();

	// The UI renders only in the browser (routes/+layout.ts), so the first
	// frame already has the width it opens at.
	let railOpen = $state(matchMedia('(min-width: 1280px)').matches);

	let slot = $state<HTMLElement>();
	let slotOverflows = $state(false);
	$effect(() => {
		if (!slot) return;
		const el = slot;
		const measure = () => (slotOverflows = el.scrollHeight > el.clientHeight + 1);
		measure();
		const ro = new ResizeObserver(measure);
		ro.observe(el);
		return () => ro.disconnect();
	});
</script>

<div class="ed-grid" class:rail-closed={!railOpen}>
	<section class="card edcard" aria-labelledby={labelledby}>
		<div class="card-head">{@render head()}</div>
		<!-- What a create answered, beside the code and in the card that stays in view. A tab stop only while it scrolls (WCAG 2.1.1). -->
		<!-- svelte-ignore a11y_no_noninteractive_tabindex -->
		<div
			bind:this={slot}
			class="ed-notices scroll-panel"
			class:filled={noticed}
			data-testid="{testid}-notices"
			role={slotOverflows ? 'region' : undefined}
			aria-label={slotOverflows ? 'What the create answered' : undefined}
			tabindex={slotOverflows ? 0 : undefined}
		>
			{@render notices()}
		</div>
		{@render children()}
	</section>
	<div class="rail-slot">
		<EditorDocsRail task={session.caretTask} open={railOpen} ontoggle={() => (railOpen = !railOpen)} />
	</div>
</div>

<style>
	.ed-grid {
		display: grid;
		grid-template-columns: minmax(0, 1fr) var(--spacing-aside);
		gap: 16px;
		align-items: start;
	}
	/*
	 * Beside the code the docs never make the row taller than the card: they
	 * scroll inside it, with the code still beside them, instead of hanging
	 * below the window's edge.
	 */
	.rail-slot {
		position: relative;
		align-self: stretch;
		min-width: 0;
	}
	.rail-slot :global(.rail) {
		position: absolute;
		inset: 0 0 auto;
		max-height: 100%;
	}
	.edcard {
		position: sticky;
		top: calc(var(--spacing-bar) + 16px);
		display: flex;
		flex-direction: column;
		overflow: hidden;
	}
	.edcard :global(.cm-editor) {
		height: max(420px, calc(100dvh - var(--spacing-bar) - 300px)) !important;
		min-height: 0 !important;
		max-height: none !important;
		border: 0 !important;
		border-radius: 0;
	}
	/* Empty, it takes no room: only the always-present live region is inside. */
	.ed-notices.filled {
		flex: none;
		max-height: min(30dvh, 216px);
		padding: 8px 16px;
		overflow-y: auto;
		border-bottom: 1px solid var(--line);
		background: var(--surface);
	}
	.ed-notices:focus-visible {
		outline-offset: -2px;
	}
	.ed-notices > :global(* + *:not(:empty)) {
		margin-top: 8px;
	}
	/* The problems list, between the code and the line under it. */
	.edcard :global(.plist) {
		flex: none;
		max-height: 196px;
		overflow-y: auto;
		scrollbar-gutter: stable;
		padding: 0 16px;
		border-top: 1px solid var(--line);
		background: var(--surface);
	}
	.edcard :global(.plist .yp-group:first-child) {
		border-top: 0;
	}
	/* One line under the code: what is known on the left, the keys on the right. */
	.edcard :global(.yaml-foot) {
		flex-wrap: nowrap;
		align-items: center;
		gap: 12px;
		min-height: 40px;
		margin: 0;
		padding: 6px 16px;
		border-top: 1px solid var(--line);
		background: var(--surface);
		font-size: 13px;
	}
	.edcard :global(.yaml-status) {
		min-width: 0;
	}
	/* As a strip it keeps one line, its whole sentence in a tip. */
	.edcard :global(.yaml-foot.strip .yaml-status) {
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}
	.edcard :global(.yaml-foot.strip .yaml-status[data-tone='problems']) {
		flex: none;
		font-weight: 600;
	}
	/* Why the engine could not answer is never cut. */
	.edcard :global(.yaml-foot.strip .yaml-status[data-tone='unavailable']) {
		white-space: normal;
	}
	.edcard :global(.yaml-keys) {
		flex: none;
	}
	@media (max-width: 1359px) {
		.edcard :global(.k-long) {
			display: none;
		}
	}
	@media (min-width: 1181px) {
		.ed-grid.rail-closed {
			grid-template-columns: minmax(0, 1fr) 44px;
		}
		.ed-grid.rail-closed :global(.rail .rail-head > p) {
			display: none;
		}
		.ed-grid.rail-closed :global(.rail .rail-head) {
			padding: 8px 2px;
		}
	}
	@media (max-width: 1180px) {
		.ed-grid {
			grid-template-columns: minmax(0, 1fr);
		}
		.rail-slot {
			display: contents;
		}
		.rail-slot :global(.rail) {
			position: static;
			max-height: none;
		}
		.edcard {
			position: static;
		}
		.edcard :global(.cm-editor) {
			height: clamp(380px, 58dvh, 680px) !important;
		}
		.ed-grid.rail-closed :global(.rail) {
			position: sticky;
			bottom: 0;
			z-index: 10;
		}
		:global(html:has(.ed-grid.rail-closed)) {
			scroll-padding-bottom: 80px;
		}
	}
	@media (max-width: 640px) {
		.ed-grid.rail-closed :global(.rail) {
			position: static;
		}
		.card-head > :global(.card-end) {
			flex: 1 1 100%;
			min-width: 0;
			margin-left: 0;
		}
		.edcard :global(.cm-lineNumbers .cm-gutterElement) {
			min-width: 36px;
		}
		.edcard :global(.cm-gutter-lint) {
			width: 12px;
		}
		.edcard :global(.yaml-foot) {
			flex-wrap: wrap;
		}
		.edcard :global(.yaml-foot.strip .yaml-status),
		.edcard :global(.ptext) {
			overflow: visible;
			white-space: normal;
		}
		.edcard :global(.yaml-first) {
			flex-wrap: wrap;
		}
		.edcard :global(.yaml-keys) {
			margin-left: 0;
			white-space: normal;
		}
		:global(html:has(.ed-grid.rail-closed)) {
			scroll-padding-bottom: 0;
		}
	}
</style>
