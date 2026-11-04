<script lang="ts">
	import '../app.css';
	import { goto, invalidateAll, replaceState } from '$app/navigation';
	import { navigating, page } from '$app/state';
	import { onMount, tick, untrack, type Component } from 'svelte';
	import ErrorPanel from '#lib/components/ErrorPanel.svelte';
	import IconSprite from '#lib/components/IconSprite.svelte';
	import Select from '#lib/components/Select.svelte';
	import Sidebar from '#lib/components/Sidebar.svelte';
	import ThemeSwitch from '#lib/components/ThemeSwitch.svelte';
	import Tooltip from '#lib/components/Tooltip.svelte';
	import TopBar, { type Crumb } from '#lib/components/TopBar.svelte';
	import { pageOf } from '#lib/navigation.ts';
	import { shortRunId } from '#lib/runDetail.ts';
	import { pollWhileVisible } from '#lib/visibility.ts';
	import { statsClient } from '#lib/clients/stats.ts';
	import { systemClient } from '#lib/clients/system.ts';
	import { OVERLAY_SELECTOR, currentPlatform, isApplePlatform, paletteKey, shortcutAria, shortcutHint } from '#lib/palette/keys.ts';
	import { provideTitleBlock } from '#lib/sheet.svelte.ts';
	import { appearance } from '#lib/theme.svelte.ts';
	import {
		DEFAULT_NAMESPACE,
		hasNamespaceParam,
		namespaceLabel,
		rememberNamespace,
		rememberedNamespace,
		withNamespace
	} from '#lib/namespace.ts';
	import { watchSystemTheme } from '#lib/theme.ts';
	import type { LayoutProps } from './$types';

	let { data, children }: LayoutProps = $props();

	// Navigation feedback for every route: a slow loader (a run with many
	// tasks, a cold archive read) otherwise leaves the old page on screen with
	// nothing saying the click was heard. The bar appears only once a
	// navigation has been pending for NAV_PROGRESS_DELAY_MS, so a fast one
	// does not flash it, and <main> is aria-busy for exactly as long as it shows.
	const NAV_PROGRESS_DELAY_MS = 150;
	let navPending = $state(false);
	$effect(() => {
		if (!navigating.to) {
			navPending = false;
			return;
		}
		const timer = setTimeout(() => (navPending = true), NAV_PROGRESS_DELAY_MS);
		return () => clearTimeout(timer);
	});

	// The namespace this page is in is always offered, even when the list could
	// not be read: a picker that silently dropped it would move you somewhere
	// else the moment the registry hiccupped.
	const options = $derived(
		[...new Set([DEFAULT_NAMESPACE, ...data.namespaces, namespaceLabel(data.namespace)])].sort()
	);

	// Switching keeps the page you are on and drops the filters belonging to the
	// namespace you left: an orkflow name is unique WITHIN a namespace, so
	// carrying `?filter=orkflow_name=deploy` across names a different orkflow
	// or none.
	function switchTo(name: string) {
		// Remembered BEFORE the navigation, and remembered under its label, so
		// that a later bare URL restores this choice rather than overriding it.
		rememberNamespace(name);
		// `default` is written into the URL like any other name. A choice the
		// address bar does not state is a choice the next reader's browser
		// answers for them, and then one link opens two different namespaces.
		void goto(withNamespace(page.url.pathname, name), { invalidateAll: true });
	}

	// Every page states the namespace it is showing, and a URL that names none
	// is answered here.
	//
	// Two different answers, because they are two different situations:
	//
	//   - this browser HAS chosen a namespace: that choice is restored, which
	//     is a real change of what the page shows, so it is a navigation.
	//     goto(..., { replace: true }) rather than the shallow
	//     replaceState helper, because the URL is the loaders' input: rewriting
	//     it without re-running them would leave the address bar saying
	//     `payments` over `default`'s data, the same lie with better manners.
	//     It replaces rather than pushes, so the back button still leaves.
	//
	//   - this browser has chosen NOTHING: the page is in `default` and only
	//     the address bar is wrong. It is rewritten shallowly — an absent `ns`
	//     and `ns=default` are the same wire value, so there is nothing to
	//     re-read — and then a link copied out of it names the namespace the
	//     sender was looking at instead of leaving the reader's own browser to
	//     answer for them.
	$effect(() => {
		const url = page.url;
		if (hasNamespaceParam(url)) {
			rememberNamespace(data.namespace);
			return;
		}
		const remembered = rememberedNamespace();
		if (remembered === null) {
			// page.state is read untracked: replaceState gives the page a new
			// state object, and a shallow write leaves page.url bare, so an
			// effect that tracked it would write again, endlessly.
			replaceState(withNamespace(url.pathname + url.search, DEFAULT_NAMESPACE), untrack(() => page.state)).catch(() => {
				// replaceState rejects before the router is initialised. The URL
				// then stays bare, which still reads `default`; the next
				// navigation writes it.
			});
			return;
		}
		void goto(withNamespace(url.pathname + url.search, remembered), {
			replace: true,
			invalidateAll: true,
			reset: false
		});
	});

	let retryingNamespaces = $state(false);
	async function retryNamespaces() {
		retryingNamespaces = true;
		try {
			await invalidateAll();
		} finally {
			retryingNamespaces = false;
		}
	}

	const link = (path: string) => withNamespace(path, data.namespace);

	// ---- The drawer (below 900 px) ----
	// The sidebar becomes a drawer: opening it makes the page inert, Escape
	// closes it, a navigation closes it, and so does the window growing back
	// past 900 px, where the sidebar is part of the page again.
	let drawerOpen = $state(false);
	$effect(() => {
		void page.url.pathname;
		drawerOpen = false;
	});
	onMount(() => {
		const wide = matchMedia('(min-width: 901px)');
		const onWide = () => {
			if (wide.matches) drawerOpen = false;
		};
		wide.addEventListener('change', onWide);
		return () => wide.removeEventListener('change', onWide);
	});
	// The page behind the drawer is inert, Menu with it: the drawer takes the
	// focus when it opens, and every way of closing it gives it back to Menu
	// once the page is no longer inert.
	async function openDrawer(): Promise<void> {
		drawerOpen = true;
		await tick();
		document.querySelector<HTMLElement>('[data-testid="sidebar-close"]')?.focus();
	}
	async function closeDrawer(): Promise<void> {
		drawerOpen = false;
		await tick();
		document.querySelector<HTMLElement>('[data-testid="menu-open"]')?.focus();
	}
	function onDrawerKey(event: KeyboardEvent): void {
		if (event.key === 'Escape' && drawerOpen) {
			event.preventDefault();
			void closeDrawer();
		}
	}

	// ---- The sidebar's live count and OrkStack's own build ----
	// "N running" is every run of this namespace not finished now, the same
	// population GetStats calls running; read once a page opens and every
	// 30 s while the tab is visible. The build is read once.
	let running = $state<number | null>(null);
	let version = $state('');
	$effect(() => {
		const ns = data.namespace;
		running = null;
		let live = true;
		const stop = pollWhileVisible(
			() => {
				statsClient(fetch)
					.getStats({ namespace: ns, lookback: { seconds: 3600n, nanos: 0 } })
					.then((res) => {
						if (live) running = res.running.length;
					})
					.catch(() => {
						if (live) running = null;
					});
			},
			30_000,
			{ immediate: true }
		);
		return () => {
			live = false;
			stop();
		};
	});
	onMount(() => {
		systemClient(fetch)
			.getSystemInfo({})
			.then((res) => (version = res.engineVersion))
			.catch(() => {});
	});

	// ---- Appearance ----
	// app.html's inline script already painted the resolved appearance before
	// this component mounted; this only reads the reader's CHOICE (which of the
	// three buttons is pressed) and follows the OS while that choice is "system".
	onMount(() => {
		appearance.load();
		return watchSystemTheme(() => appearance.choice);
	});

	// ---- The command palette (Ctrl/Cmd+K) ----
	// The palette component and the clients it searches with are imported on
	// first open, so the shell ships only this listener and the button.
	let apple = $state(false);
	let paletteOpen = $state(false);
	let Palette = $state<Component<{
		namespace: string;
		namespaces: string[];
		onclose: () => void;
		onnamespace: (name: string) => void;
	}> | null>(null);

	async function openPalette(): Promise<void> {
		if (!Palette) Palette = (await import('#lib/components/CommandPalette.svelte')).default;
		drawerOpen = false;
		paletteOpen = true;
	}

	onMount(() => {
		apple = isApplePlatform(currentPlatform());
		// Capture phase, on the document: the shortcut works from inside a text
		// field, and before anything below can claim the key.
		function onKey(event: KeyboardEvent): void {
			const decision = paletteKey(event, apple, document.querySelector(OVERLAY_SELECTOR) !== null);
			if (decision === 'ignore') return;
			event.preventDefault();
			if (decision === 'consume') return;
			if (paletteOpen) paletteOpen = false;
			else void openPalette();
		}
		document.addEventListener('keydown', onKey, { capture: true });
		return () => document.removeEventListener('keydown', onKey, { capture: true });
	});

	// ---- The breadcrumb ----
	// The namespace, the page, then where on it: a run, an Orkflow, a
	// comparison. Pages still register their facts with the title-block
	// holder; the breadcrumb reads none of them.
	provideTitleBlock();
	const crumbs = $derived.by((): Crumb[] => {
		const path = page.url.pathname;
		const here = pageOf(path);
		const out: Crumb[] = [{ label: namespaceLabel(data.namespace), href: link('/runs') }];
		if (here && here.href !== '/' && here.href !== '/admin' && path.startsWith('/admin')) out.push({ label: 'Operate', href: link('/admin') });
		out.push({ label: here?.label ?? 'OrkStack', href: here ? link(here.href) : undefined });
		const rest = here && here.href !== '/' ? path.slice(here.href.length).split('/').filter(Boolean) : [];
		for (const [i, part] of rest.entries()) {
			const value = decodeURIComponent(part);
			// A run's crumb is its Orkflow, once the page has read it; the run id is the page's own plate.
			const runName = (page.data as { run?: { orkflowName?: string } | null }).run?.orkflowName;
			const label = here?.id === 'runs' && i === 0 ? (runName ?? shortRunId(value)) : value === 'compare' ? 'Compare' : value;
			out.push({ label, href: link(`${here!.href}/${rest.slice(0, i + 1).join('/')}`) });
		}
		return out;
	});
</script>

{#snippet namespacePicker()}
	<Select
		testid="namespace-picker"
		label="Namespace"
		variant="namespace"
		value={namespaceLabel(data.namespace)}
		options={options.map((name) => ({ value: name, label: name }))}
		onchange={switchTo}
	/>
{/snippet}

{#snippet themeSwitch()}
	<ThemeSwitch />
{/snippet}

<svelte:window onkeydown={onDrawerKey} />

<a href="#content" class="skip" data-testid="skip-link">Skip to content</a>
<IconSprite />

{#if navPending}
	<div
		data-testid="nav-progress"
		role="progressbar"
		aria-label="Loading the next page"
		class="pointer-events-none fixed inset-x-0 top-0 z-[60] h-0.5 overflow-hidden bg-navy-tint"
	>
		<div class="h-full w-2/5 bg-navy-text animate-nav-progress motion-reduce:animate-live-dot"></div>
	</div>
{/if}

<div class="app">
	<Sidebar
		pathname={page.url.pathname}
		{link}
		{running}
		{version}
		picker={namespacePicker}
		{themeSwitch}
		drawer={drawerOpen}
		onclose={() => void closeDrawer()}
	/>
	{#if drawerOpen}
		<button type="button" class="scrim" tabindex="-1" aria-hidden="true" onclick={() => void closeDrawer()}></button>
	{/if}

	<div class="main" inert={drawerOpen}>
		<TopBar
			{crumbs}
			{drawerOpen}
			shortcut={shortcutHint(apple)}
			shortcutAria={shortcutAria(apple)}
			onmenu={() => void openDrawer()}
			onpalette={() => void openPalette()}
		/>

		{#if data.namespaceError}
			<div class="content-pad pb-0">
				<ErrorPanel
					testid="namespace-error"
					retryTestid="namespace-retry"
					subject="the other namespaces (showing {namespaceLabel(data.namespace)})"
					message={data.namespaceError}
					busy={retryingNamespaces}
					onretry={retryNamespaces}
				/>
			</div>
		{/if}

		<main id="content" tabindex="-1" aria-busy={navPending} class="content-pad">
			{@render children()}
		</main>
	</div>
</div>

<Tooltip />

{#if paletteOpen && Palette}
	<Palette
		namespace={namespaceLabel(data.namespace)}
		namespaces={options}
		onclose={() => (paletteOpen = false)}
		onnamespace={switchTo}
	/>
{/if}

<style>
	.skip {
		position: fixed;
		left: 12px;
		top: -60px;
		z-index: 100;
		padding: 8px 14px;
		border-radius: var(--r-sm);
		background: var(--ink);
		color: var(--bg);
		font-weight: 700;
		text-decoration: none;
	}
	.skip:focus {
		top: 10px;
	}
	.app {
		display: grid;
		grid-template-columns: var(--spacing-side) minmax(0, 1fr);
		min-height: 100dvh;
	}
	.main {
		display: flex;
		min-width: 0;
		flex-direction: column;
	}
	.content-pad {
		width: 100%;
		max-width: var(--spacing-content);
		padding: 24px 28px 48px;
	}
	.scrim {
		display: none;
	}
	@media (max-width: 900px) {
		.app {
			grid-template-columns: minmax(0, 1fr);
		}
		.content-pad {
			padding: 16px 16px 40px;
		}
		.scrim {
			display: block;
			position: fixed;
			inset: 0;
			z-index: 39;
			border: 0;
			padding: 0;
			background: var(--scrim);
		}
	}
</style>
