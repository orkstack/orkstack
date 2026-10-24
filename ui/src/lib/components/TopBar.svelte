<script lang="ts">
	// The slim bar across the top of every page: the wordmark, the two modes
	// with Build's three daily surfaces, the namespace, the palette's button and
	// the appearance switch.
	//
	// It holds THE Primary navigation — one `nav[aria-label="Primary"]` on the
	// page at any time — so while the menu drawer is open (it renders its own
	// copy) this one is not rendered, and neither is the namespace picker.
	import type { Snippet } from 'svelte';
	import Icon from './Icon.svelte';
	import { BUILD_PAGES, modeOf, type Mode } from '$lib/navigation';

	interface Props {
		pathname: string;
		link: (path: string) => string;
		drawerOpen: boolean;
		shortcut: string;
		shortcutAria: string;
		onmenu: () => void;
		onpalette: () => void;
		picker: Snippet;
		themeSwitch: Snippet;
	}

	let { pathname, link, drawerOpen, shortcut, shortcutAria, onmenu, onpalette, picker, themeSwitch }: Props = $props();

	const mode = $derived<Mode>(modeOf(pathname));
	const [overview, runs, orkflows, apply] = BUILD_PAGES;

	function current(href: string): 'page' | undefined {
		if (href === '/') return pathname === '/' ? 'page' : undefined;
		return pathname === href || pathname.startsWith(href + '/') ? 'page' : undefined;
	}

	// A mode's label is current for its whole mode: "page" on its own landing
	// page, "true" anywhere else in it.
	function modeCurrent(m: Mode, landing: string): 'page' | 'true' | undefined {
		if (mode !== m) return undefined;
		return pathname === landing ? 'page' : 'true';
	}

	const MODE =
		"caps relative inline-flex h-bar items-center px-2.5 text-ink-muted transition-colors hover:text-ink aria-[current]:text-ink aria-[current]:after:absolute aria-[current]:after:inset-x-2.5 aria-[current]:after:bottom-0 aria-[current]:after:h-0.5 aria-[current]:after:bg-blue";
	const ITEM =
		'inline-flex h-bar items-center px-2 text-sm font-medium text-ink-secondary transition-colors hover:text-ink hover:underline current:font-semibold current:text-ink current:underline current:decoration-2 current:underline-offset-8';
</script>

<header class="sticky top-0 z-40 border-b-(length:--rule-region) border-ink bg-paper">
	<div class="flex h-bar items-center gap-2 px-gutter sm:px-sheet">
		<a href={link('/')} class="mr-2 flex shrink-0 items-center gap-2 text-ink" aria-label="OrkStack, Overview">
			<svg viewBox="0 0 16 16" class="size-4" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true">
				<path d="M1.75 1.75h12.5v12.5H1.75zM1.75 10.25h12.5M8.75 10.25v4" />
			</svg>
			<span class="caps text-xs tracking-caps">OrkStack</span>
		</a>

		{#if !drawerOpen}
			<nav aria-label="Primary" class="flex min-w-0 items-center max-sm:hidden">
				<span class="mx-1 h-5 border-l border-rule" aria-hidden="true"></span>
				<a href={link(overview.href)} data-testid="nav-dashboard" aria-current={modeCurrent('build', '/')} class={MODE}
					>Build</a
				>
				<a href={link(runs.href)} aria-current={current(runs.href)} class="{ITEM} max-lg:hidden">{runs.label}</a>
				<a href={link(orkflows.href)} aria-current={current(orkflows.href)} class="{ITEM} max-lg:hidden">{orkflows.label}</a>
				<a href={link(apply.href)} aria-current={current(apply.href)} class="{ITEM} max-lg:hidden"
					>{apply.label}<span class="sr-only"> orkflow</span></a
				>
				<span class="mx-1 h-5 border-l border-rule" aria-hidden="true"></span>
				<a href={link('/admin')} data-testid="nav-admin" aria-current={modeCurrent('operate', '/admin')} class={MODE}
					>Operate</a
				>
			</nav>
		{/if}

		<div class="ml-auto flex items-center gap-3">
			{#if !drawerOpen}
				<div class="flex items-center gap-2 max-lg:hidden">
					<span class="caps text-ink-muted" aria-hidden="true">Namespace</span>
					<div class="w-40">{@render picker()}</div>
				</div>
			{/if}
			<button
				type="button"
				data-testid="command-palette-open"
				aria-keyshortcuts={shortcutAria}
				onclick={onpalette}
				class="inline-flex h-8 items-center gap-2 rounded-sm border border-rule-strong bg-paper px-2.5 text-sm text-ink-secondary transition-colors hover:bg-hover hover:text-ink"
			>
				<Icon name="search" class="size-3.5" />
				<span class="max-lg:sr-only">Jump to…</span>
				<kbd class="kbd max-sm:hidden">{shortcut}</kbd>
			</button>
			{#if !drawerOpen}<div class="max-lg:hidden">{@render themeSwitch()}</div>{/if}
			<button
				type="button"
				onclick={onmenu}
				aria-haspopup="dialog"
				class="inline-flex h-8 items-center gap-1.5 rounded-sm border border-rule-strong px-2.5 text-sm font-medium text-ink lg:hidden"
			>
				<Icon name="menu" class="size-3.5" />Menu
			</button>
		</div>
	</div>
</header>
