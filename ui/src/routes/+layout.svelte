<script lang="ts">
	import '../app.css';
	import { goto, invalidateAll, replaceState } from '$app/navigation';
	import { navigating, page } from '$app/state';
	import { onMount, type Component } from 'svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import MenuDrawer from '$lib/components/MenuDrawer.svelte';
	import Select from '$lib/components/Select.svelte';
	import ThemeSwitch from '$lib/components/ThemeSwitch.svelte';
	import TitleBlock from '$lib/components/TitleBlock.svelte';
	import TopBar from '$lib/components/TopBar.svelte';
	import { operatePageOf, operateScope, pageOf } from '$lib/navigation';
	import { OVERLAY_SELECTOR, currentPlatform, isApplePlatform, paletteKey, shortcutAria, shortcutHint } from '$lib/palette/keys';
	import { provideTitleBlock } from '$lib/sheet.svelte';
	import type { TitleField } from '$lib/sheet';
	import { appearance } from '$lib/theme.svelte';
	import {
		DEFAULT_NAMESPACE,
		hasNamespaceParam,
		namespaceLabel,
		rememberNamespace,
		rememberedNamespace,
		withNamespace
	} from '$lib/namespace';
	import { watchSystemTheme } from '$lib/theme';
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
	//     goto(..., { replaceState: true }) rather than the shallow
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
			try {
				replaceState(withNamespace(url.pathname + url.search, DEFAULT_NAMESPACE), page.state);
			} catch {
				// replaceState throws before the router is initialised. The URL
				// then stays bare, which still reads `default`; the next
				// navigation writes it.
			}
			return;
		}
		void goto(withNamespace(url.pathname + url.search, remembered), {
			replaceState: true,
			invalidateAll: true,
			noScroll: true,
			keepFocus: true
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

	// ---- The menu drawer (below 1024 px) ----
	let drawerOpen = $state(false);

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
		if (!Palette) Palette = (await import('$lib/components/CommandPalette.svelte')).default;
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

	// ---- The title block ----
	const titleBlock = provideTitleBlock();
	const titleFields = $derived.by((): TitleField[] => {
		void titleBlock.version;
		const own = titleBlock.holder.fields();
		if (own) return own;
		const path = page.url.pathname;
		const fields: TitleField[] = [{ label: 'PAGE', value: pageOf(path)?.title ?? 'OrkStack' }];
		fields.push({ label: 'NAMESPACE', value: namespaceLabel(data.namespace) });
		const operate = operatePageOf(path);
		if (operate) fields.push({ label: 'SCOPE', value: operateScope(operate.id) });
		return fields;
	});

	// The block lifts (a soft shadow on its top edge) only while content runs
	// on beneath it: a sentinel at the end of <main> says whether it is in view.
	let lifted = $state(false);
	let sentinel = $state<HTMLElement | undefined>(undefined);
	$effect(() => {
		if (!sentinel || typeof IntersectionObserver === 'undefined') return;
		const observer = new IntersectionObserver(([entry]) => (lifted = !entry.isIntersecting));
		observer.observe(sentinel);
		return () => observer.disconnect();
	});
</script>

{#snippet namespacePicker()}
	<Select
		testid="namespace-picker"
		label="Namespace"
		value={namespaceLabel(data.namespace)}
		options={options.map((name) => ({ value: name, label: name }))}
		onchange={switchTo}
	/>
{/snippet}

{#snippet themeSwitch()}
	<ThemeSwitch />
{/snippet}

<a
	href="#content"
	class="sr-only z-50 bg-ink px-3 py-2 text-sm font-semibold text-on-ink focus:not-sr-only focus:fixed focus:top-2 focus:left-2"
	>Skip to content</a
>

{#if navPending}
	<div
		data-testid="nav-progress"
		role="progressbar"
		aria-label="Loading the next page"
		class="pointer-events-none fixed inset-x-0 top-0 z-[60] h-0.5 overflow-hidden bg-blue-tint"
	>
		<div class="h-full w-2/5 bg-blue animate-nav-progress motion-reduce:animate-live-dot"></div>
	</div>
{/if}

<div class="flex min-h-screen flex-col">
	<TopBar
		pathname={page.url.pathname}
		{link}
		{drawerOpen}
		shortcut={shortcutHint(apple)}
		shortcutAria={shortcutAria(apple)}
		onmenu={() => (drawerOpen = true)}
		onpalette={() => void openPalette()}
		picker={namespacePicker}
		{themeSwitch}
	/>

	{#if drawerOpen}
		<MenuDrawer
			pathname={page.url.pathname}
			{link}
			onclose={() => (drawerOpen = false)}
			picker={namespacePicker}
			{themeSwitch}
		/>
	{/if}

	{#if data.namespaceError}
		<div class="px-gutter pt-4 sm:px-sheet">
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

	<main
		id="content"
		tabindex="-1"
		aria-busy={navPending}
		class="mx-auto w-full max-w-[1440px] flex-1 px-gutter py-6 focus:outline-none sm:px-sheet"
	>
		{@render children()}
		<div bind:this={sentinel} aria-hidden="true" class="h-px"></div>
	</main>

	<TitleBlock fields={titleFields} {lifted} />
</div>

{#if paletteOpen && Palette}
	<Palette
		namespace={namespaceLabel(data.namespace)}
		namespaces={options}
		onclose={() => (paletteOpen = false)}
		onnamespace={switchTo}
	/>
{/if}
