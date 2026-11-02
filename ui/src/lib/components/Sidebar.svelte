<script lang="ts">
	// The light sidebar (248 px): the brand, the namespace switch, Runs (with
	// how many runs are going now), Orkflows and Apply, then Operate, a static
	// group of the deployment's pages; the OrkStack plate and the appearance
	// switch at its foot. The active page is a navy pill. Below 900 px it is
	// the drawer the menu button opens (the layout owns that state).
	import type { Snippet } from 'svelte';
	import { BUILD_PAGES, OPERATE_PAGES, buildPageOf, operatePageOf } from '#lib/navigation.ts';
	import Icon from './Icon.svelte';
	import Mark from './Mark.svelte';

	interface Props {
		pathname: string;
		link: (path: string) => string;
		/** Runs not finished now, or null while unknown. */
		running: number | null;
		/** OrkStack's own build, for the foot's plate. */
		version: string;
		picker: Snippet;
		themeSwitch: Snippet;
		/** Set while it is the open drawer: the close button shows. */
		drawer: boolean;
		onclose: () => void;
	}

	let { pathname, link, running, version, picker, themeSwitch, drawer, onclose }: Props = $props();

	const [, runs, orkflows, apply] = BUILD_PAGES;
	const build = $derived(buildPageOf(pathname)?.id);
	const operate = $derived(operatePageOf(pathname)?.id);
	const current = (on: boolean) => (on ? 'page' : undefined);
</script>

<nav data-side id="side" class="side scroll-panel" class:open={drawer} aria-label="Main" data-testid="sidebar">
	{#if drawer}
		<button type="button" class="btn btn-sm self-end" data-testid="sidebar-close" onclick={onclose}>Close menu</button>
	{/if}
	<a class="brand" href={link('/')} data-testid="nav-dashboard" aria-current={current(build === 'overview')}
		><Mark />OrkStack</a
	>
	<div class="mb-2.5">{@render picker()}</div>

	<a class="nav" href={link(runs.href)} data-testid="nav-runs" aria-current={current(build === 'runs')}
		><Icon name="runs" />Runs{#if running !== null && running > 0}<span class="live" data-testid="nav-running"
				><i class="mark-dot" aria-hidden="true"></i>{running} running</span
			>{/if}</a
	>
	<a class="nav" href={link(orkflows.href)} data-testid="nav-orkflows" aria-current={current(build === 'orkflows')}
		><Icon name="flow" />Orkflows</a
	>
	<a class="nav" href={link(apply.href)} data-testid="nav-apply" aria-current={current(build === 'apply')}
		><Icon name="apply" />Apply</a
	>

	<span class="side-h" id="side-operate">Operate</span>
	<div role="group" aria-labelledby="side-operate" class="grid gap-0.5" data-testid="admin-nav">
		{#each OPERATE_PAGES as p (p.id)}
			<a
				class="nav"
				href={link(p.href)}
				data-testid={p.id === 'overview' ? 'nav-admin' : `admin-tab-${p.id}`}
				aria-current={current(operate === p.id)}><Icon name={p.icon} />{p.label}</a
			>
		{/each}
	</div>

	<div class="foot">
		<span class="plate plate-sm" data-testid="orkstack-version"
			><span class="plate-k">OrkStack</span><span class="plate-v">{version || '…'}</span></span
		>
		{@render themeSwitch()}
	</div>
</nav>

<style>
	.side {
		display: flex;
		flex-direction: column;
		gap: 2px;
		position: sticky;
		top: 0;
		height: 100dvh;
		overflow-y: auto;
		padding: 14px 12px 16px;
		background: var(--side);
		color: var(--side-ink);
		border-right: 1px solid var(--side-line);
	}
	.brand {
		display: flex;
		align-items: center;
		gap: 10px;
		padding: 4px 8px 14px;
		font-size: 16.5px;
		font-weight: 700;
		letter-spacing: -0.012em;
		color: var(--side-strong);
		text-decoration: none;
		border-radius: var(--r-sm);
	}
	.nav {
		position: relative;
		display: flex;
		align-items: center;
		gap: 10px;
		min-height: 34px;
		padding: 6px 10px;
		border-radius: var(--r-sm);
		color: var(--side-ink);
		text-decoration: none;
		font-size: 14px;
		font-weight: 500;
	}
	.nav :global(svg) {
		width: 16px;
		height: 16px;
		color: var(--side-ink-2);
	}
	.nav:hover {
		background: var(--side-hover);
		color: var(--side-strong);
	}
	.nav[aria-current='page'] {
		background: var(--side-active);
		color: var(--side-active-ink);
		font-weight: 650;
	}
	.nav[aria-current='page'] :global(svg) {
		color: var(--side-active-ink);
	}
	.live {
		margin-left: auto;
		display: inline-flex;
		align-items: center;
		gap: 6px;
		height: 22px;
		padding: 0 8px;
		border-radius: 99px;
		background: var(--run-tint);
		color: var(--run);
		font-size: 13px;
		font-weight: 600;
		font-variant-numeric: tabular-nums;
	}
	.side-h {
		padding: 14px 10px 4px;
		font-size: 13px;
		font-weight: 600;
		color: var(--side-ink-2);
	}
	.foot {
		margin-top: auto;
		padding: 12px 4px 0;
		border-top: 1px solid var(--side-line);
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		justify-content: space-between;
		gap: 8px;
	}
	@media (max-width: 900px) {
		.side {
			position: fixed;
			left: 0;
			top: 0;
			bottom: 0;
			width: min(300px, 86vw);
			height: auto;
			z-index: 40;
			box-shadow: var(--shadow-lg);
			transform: translateX(-102%);
			visibility: hidden;
			transition:
				transform var(--dur-fast) var(--ease-in),
				visibility 0s linear var(--dur-fast);
		}
		.side.open {
			transform: none;
			visibility: visible;
			transition: transform var(--dur-fast) var(--ease-in);
		}
		.nav {
			min-height: 40px;
		}
	}
	@media (max-width: 900px) and (prefers-reduced-motion: reduce) {
		.side,
		.side.open {
			transition: none;
		}
	}
	@media (forced-colors: active) {
		.nav[aria-current='page'] {
			outline: 2px solid CanvasText;
		}
	}
</style>
