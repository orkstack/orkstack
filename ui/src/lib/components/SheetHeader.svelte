<script lang="ts">
	// The head of every page: its title, an optional line of facts, and the
	// page's primary action top right — the one ink-filled button on the page.
	import type { Snippet } from 'svelte';
	import CliPopover, { type CliCommand } from './CliPopover.svelte';

	interface Props {
		title: string;
		testid?: string;
		/** The title in the mono: a run id, a digest. */
		mono?: boolean;
		/**
		 * The title as drawn below 640 px ("ork-…873153"), where the whole of
		 * a long id would take the first screen. The full title stays the
		 * heading for assistive technology, find-in-page and copy.
		 */
		short?: string;
		meta?: Snippet;
		actions?: Snippet;
		/**
		 * This page's command lines: the one terminal icon a page has, after its
		 * actions (ui/DESIGN.md, the Shell Icon Rule). A row's or a dialog's own
		 * command stays with it, because it carries that row's values.
		 */
		commands?: CliCommand[];
		/** Title and actions on one row with the facts full width below; narrow, the actions come before the facts. */
		actionsFirst?: boolean;
	}

	let { title, testid = undefined, mono = false, short = undefined, meta = undefined, actions = undefined, commands = [], actionsFirst = false }: Props = $props();
</script>

<header class="ph mb-5 flex flex-wrap items-start justify-between gap-x-5 gap-y-3" class:ph-grid={actionsFirst}>
	<div class="ph-main min-w-0">
		<h1
			data-testid={testid}
			class="page-h1 text-ink {mono ? 'font-mono break-all' : ''} {short ? 'max-sm:sr-only' : ''}"
		>
			{title}
		</h1>
		{#if short}
			<p
				aria-hidden="true"
				data-tip={title}
				class="page-h1 whitespace-nowrap text-ink sm:hidden {mono ? 'font-mono' : ''}"
			>
				{short}
			</p>
		{/if}
		{#if meta}<div class="ph-meta mt-2 text-dense text-ink-2">{@render meta()}</div>{/if}
	</div>
	{#if actions || commands.length > 0}
		<div class="ph-actions flex max-w-full min-w-0 flex-wrap items-center gap-2">
			{#if actions}{@render actions()}{/if}
			{#if commands.length > 0}<CliPopover testid="page-cli" title="Command line: {title}" {commands} />{/if}
		</div>
	{/if}
</header>

<style>
	/* The title takes the row; a phone's actions go under it, not beside it. */
	.ph-main {
		flex: 1 1 420px;
	}
	.ph-grid {
		display: grid;
		grid-template-columns: minmax(0, 1fr) auto;
		column-gap: 20px;
		row-gap: 0;
	}
	.ph-grid .ph-main {
		display: contents;
	}
	.ph-grid .page-h1 {
		grid-column: 1;
		grid-row: 1;
		align-self: center;
	}
	.ph-grid .ph-actions {
		grid-column: 2;
		grid-row: 1;
		align-self: center;
	}
	.ph-grid .ph-meta {
		grid-column: 1 / -1;
		grid-row: 2;
	}
	@media (max-width: 1099px) {
		.ph-grid {
			grid-template-columns: minmax(0, 1fr);
		}
		.ph-grid .ph-actions {
			grid-column: 1;
			grid-row: 2;
			margin-top: 10px;
		}
		.ph-grid .ph-meta {
			grid-row: 3;
		}
	}
</style>
