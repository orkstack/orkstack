<script lang="ts" module>
	export interface Crumb {
		label: string;
		href?: string;
		/** The namespace crumb: the one place a page says which namespace it shows. */
		namespace?: boolean;
	}
</script>

<script lang="ts">
	// The top bar: the menu button (below 900 px), the breadcrumb, and Jump —
	// a search over pages and recent runs (Ctrl K or ⌘K opens the dialog).
	import Icon from './Icon.svelte';

	interface Props {
		crumbs: Crumb[];
		drawerOpen: boolean;
		shortcut: string;
		shortcutAria: string;
		onmenu: () => void;
		onpalette: () => void;
	}

	let { crumbs, drawerOpen, shortcut, shortcutAria, onmenu, onpalette }: Props = $props();
</script>

<header class="top" data-testid="top-bar">
	<button
		type="button"
		class="btn btn-sm menu-btn"
		aria-expanded={drawerOpen}
		aria-controls="side"
		data-testid="menu-open"
		onclick={onmenu}><Icon name="menu" />Menu</button
	>
	<nav class="crumbs" aria-label="Breadcrumb" data-testid="breadcrumb">
		{#each crumbs as crumb, i (i)}
			{#if i > 0}<Icon name="chev-right" class="crumb-sep" />{/if}
			{#if i === crumbs.length - 1}
				<b aria-current="page" data-testid="breadcrumb-current">{crumb.label}</b>
			{:else if crumb.namespace && crumb.href}
				<a class="tag crumb-ns" href={crumb.href} aria-label="Namespace {crumb.label}" data-tip="Namespace">{crumb.label}</a>
			{:else if crumb.href}
				<a href={crumb.href}>{crumb.label}</a>
			{:else}
				<span>{crumb.label}</span>
			{/if}
		{/each}
	</nav>
	<button
		type="button"
		class="jump"
		aria-haspopup="dialog"
		aria-keyshortcuts={shortcutAria}
		data-testid="command-palette-open"
		onclick={onpalette}
		><Icon name="search" /><span class="jump-l">Jump to a run, Orkflow or page</span><kbd class="kbd jump-k">{shortcut}</kbd></button
	>
</header>

<style>
	.top {
		position: sticky;
		top: 0;
		z-index: 20;
		display: flex;
		align-items: center;
		gap: 12px;
		height: var(--spacing-bar);
		padding: 0 28px;
		border-bottom: 1px solid var(--line);
		background: var(--surface);
	}
	.menu-btn {
		display: none;
	}
	.crumbs {
		display: flex;
		align-items: center;
		gap: 6px;
		min-width: 0;
		overflow: hidden;
		white-space: nowrap;
		font-size: 13.5px;
		color: var(--ink-3);
	}
	.crumbs :global(.crumb-sep) {
		flex: none;
		width: 14px;
		height: 14px;
	}
	.crumbs a.crumb-ns {
		flex: none;
		color: var(--navy-text);
	}
	.crumbs a {
		color: var(--ink-2);
		text-decoration: none;
	}
	.crumbs a:hover {
		text-decoration: underline;
	}
	.crumbs b {
		overflow: hidden;
		text-overflow: ellipsis;
		font-weight: 600;
		color: var(--ink);
	}
	.jump {
		margin-left: auto;
		display: flex;
		flex: none;
		align-items: center;
		gap: 8px;
		width: 320px;
		max-width: 40vw;
		height: 36px;
		padding: 0 10px;
		border: 1px solid var(--line-2);
		border-radius: var(--r-sm);
		background: var(--surface-2);
		color: var(--ink-3);
		font: inherit;
		font-size: 13.5px;
		cursor: pointer;
	}
	.jump :global(svg) {
		width: 15px;
		height: 15px;
	}
	.jump-l {
		min-width: 0;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}
	.jump-k {
		margin-left: auto;
	}
	@media (max-width: 900px) {
		.top {
			padding: 0 16px;
		}
		.menu-btn {
			display: inline-flex;
		}
		.jump {
			width: auto;
			min-width: 40px;
			justify-content: center;
		}
		/* Only the icon shows, and the words still name the button. */
		.jump-l {
			position: absolute;
			width: 1px;
			height: 1px;
			overflow: hidden;
			clip-path: inset(50%);
			white-space: nowrap;
		}
		.jump-k {
			display: none;
		}
	}
</style>
