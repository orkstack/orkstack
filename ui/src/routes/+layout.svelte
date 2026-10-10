<script lang="ts">
	import '../app.css';
	import { goto, invalidateAll, replaceState } from '$app/navigation';
	import { page } from '$app/state';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import Select from '$lib/components/Select.svelte';
	import {
		DEFAULT_NAMESPACE,
		hasNamespaceParam,
		namespaceLabel,
		rememberNamespace,
		rememberedNamespace,
		withNamespace
	} from '$lib/namespace';
	import { applyTheme, rememberTheme, rememberedTheme, watchSystemTheme, type Theme } from '$lib/theme';
	import type { LayoutProps } from './$types';

	let { data, children }: LayoutProps = $props();

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

	// A nav item is current for its own route and everything under it, except
	// Dashboard: every path starts with "/", so its match has to be exact.
	function isActive(path: string): boolean {
		const p = page.url.pathname;
		if (path === '/') return p === '/';
		return p === path || p.startsWith(path + '/');
	}

	const NAV_ITEM =
		'flex h-9 items-center gap-2.5 rounded-md px-2.5 text-sm font-medium text-fg-secondary transition-colors duration-150 hover:bg-surface-hover hover:text-fg current:bg-brand-50 current:text-brand-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500/40';
	const GROUP_LABEL = 'mt-5 mb-1 px-2.5 text-[11px] font-medium tracking-wider text-fg-muted uppercase';

	let mobileNavOpen = $state(false);

	// The reader's CHOICE (light/dark/system), not the resolved appearance —
	// app.html's own inline script already painted the resolved one before
	// this component ever mounted, so there is nothing to apply here on
	// load. This state exists to know which of the three buttons below is
	// pressed, and to give `setTheme` something to change.
	let theme = $state<Theme>(rememberedTheme());

	function setTheme(next: Theme) {
		theme = next;
		rememberTheme(next);
		applyTheme(next);
	}

	// Runs once: reads no reactive value directly (only inside the closure
	// passed below), so it sets up ONE listener for the life of this layout
	// rather than tearing one down and rebuilding it on every click.
	$effect(() => {
		return watchSystemTheme(() => theme);
	});

	const THEME_BTN =
		'flex h-7 items-center justify-center rounded-sm text-fg-muted transition-colors duration-150 hover:text-fg focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500/40';
	const THEME_BTN_ON = 'bg-surface text-fg shadow-xs';
</script>

{#snippet mark()}
	<!-- Fixed dark regardless of page theme, like most brand marks: bg-gray-900
	     rather than bg-fg on purpose — the raw gray scale is not themed (see
	     app.css), so this never inverts to a light square under a white icon. -->
	<span
		class="flex size-6 shrink-0 items-center justify-center rounded-md bg-gray-900"
		aria-hidden="true"
	>
		<svg width="14" height="14" viewBox="0 0 14 14" fill="none">
			<circle cx="2.5" cy="2.5" r="1.5" fill="white" />
			<circle cx="11.5" cy="2.5" r="1.5" fill="white" />
			<circle cx="7" cy="11.5" r="1.5" fill="white" />
			<path
				d="M3.7 3.4 L9.9 3.4 M3.4 3.7 L6.3 10.3 M10.2 3.7 L7.7 10.3"
				stroke="white"
				stroke-width="1.1"
				stroke-linecap="round"
			/>
		</svg>
	</span>
{/snippet}

{#snippet wordmark()}
	<a href={link('/')} class="flex h-14 items-center gap-2 border-b border-border-subtle px-4">
		{@render mark()}
		<span class="text-[15px] font-semibold tracking-tight text-fg">OrkStack</span>
	</a>
{/snippet}

{#snippet namespacePicker()}
	<div class="px-3 pt-4 pb-2">
		<div class="mb-1.5 px-0.5 text-[11px] font-medium tracking-wider text-fg-muted uppercase">
			Namespace
		</div>
		<Select
			testid="namespace-picker"
			label="Namespace"
			value={namespaceLabel(data.namespace)}
			options={options.map((name) => ({ value: name, label: name }))}
			onchange={switchTo}
		/>
	</div>
{/snippet}

{#snippet primaryNav()}
	<nav aria-label="Primary" class="flex-1 px-3">
		<a href={link('/')} data-testid="nav-dashboard" aria-current={isActive('/') ? 'page' : undefined} class={NAV_ITEM}
			>Dashboard</a
		>
		<a
			href={link('/orkflows')}
			aria-current={isActive('/orkflows') ? 'page' : undefined}
			class={NAV_ITEM}>Orkflows</a
		>
		<a href={link('/runs')} aria-current={isActive('/runs') ? 'page' : undefined} class={NAV_ITEM}
			>Runs</a
		>

		<div class={GROUP_LABEL}>Author</div>
		<a href={link('/apply')} aria-current={isActive('/apply') ? 'page' : undefined} class={NAV_ITEM}
			>Apply orkflow</a
		>

		<div class={GROUP_LABEL}>Admin</div>
		<a
			href={link('/admin')}
			data-testid="nav-admin"
			aria-current={isActive('/admin') ? 'page' : undefined}
			class={NAV_ITEM}>Admin</a
		>
	</nav>
{/snippet}

{#snippet themeToggle()}
	<div class="border-t border-border-subtle p-3">
		<div class="mb-1.5 px-0.5 text-[11px] font-medium tracking-wider text-fg-muted uppercase">
			Appearance
		</div>
		<div class="grid grid-cols-3 gap-1 rounded-md border border-border bg-surface-sunken p-1">
			<button
				type="button"
				data-testid="theme-system"
				aria-label="Use system appearance"
				aria-pressed={theme === 'system'}
				onclick={() => setTheme('system')}
				class="{THEME_BTN} {theme === 'system' ? THEME_BTN_ON : ''}"
			>
				<svg width="14" height="14" viewBox="0 0 16 16" fill="none" aria-hidden="true">
					<rect x="1.5" y="2.5" width="13" height="8.5" rx="1.2" stroke="currentColor" stroke-width="1.5" />
					<path d="M5.5 14h5M8 11v3" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" />
				</svg>
			</button>
			<button
				type="button"
				data-testid="theme-light"
				aria-label="Light appearance"
				aria-pressed={theme === 'light'}
				onclick={() => setTheme('light')}
				class="{THEME_BTN} {theme === 'light' ? THEME_BTN_ON : ''}"
			>
				<svg width="14" height="14" viewBox="0 0 16 16" fill="none" aria-hidden="true">
					<circle cx="8" cy="8" r="3" stroke="currentColor" stroke-width="1.5" />
					<path
						d="M8 1.5v2M8 12.5v2M1.5 8h2M12.5 8h2M3.5 3.5l1.4 1.4M11.1 11.1l1.4 1.4M3.5 12.5l1.4-1.4M11.1 4.9l1.4-1.4"
						stroke="currentColor"
						stroke-width="1.5"
						stroke-linecap="round"
					/>
				</svg>
			</button>
			<button
				type="button"
				data-testid="theme-dark"
				aria-label="Dark appearance"
				aria-pressed={theme === 'dark'}
				onclick={() => setTheme('dark')}
				class="{THEME_BTN} {theme === 'dark' ? THEME_BTN_ON : ''}"
			>
				<svg width="14" height="14" viewBox="0 0 16 16" fill="none" aria-hidden="true">
					<path
						d="M13.5 9.3A5.5 5.5 0 1 1 6.7 2.5a4.5 4.5 0 0 0 6.8 6.8z"
						stroke="currentColor"
						stroke-width="1.5"
						stroke-linejoin="round"
					/>
				</svg>
			</button>
		</div>
	</div>
{/snippet}

<div class="flex min-h-screen">
	<aside
		class="sticky top-0 hidden h-screen w-60 shrink-0 flex-col border-r border-border bg-surface lg:flex"
	>
		{@render wordmark()}
		{@render namespacePicker()}
		{@render primaryNav()}
		{@render themeToggle()}
	</aside>

	<div class="flex min-w-0 flex-1 flex-col">
		<header
			class="sticky top-0 z-40 flex h-14 items-center gap-3 border-b border-border bg-surface px-4 lg:hidden"
		>
			<button
				type="button"
				aria-label="Open navigation"
				onclick={() => (mobileNavOpen = true)}
				class="inline-flex size-9 items-center justify-center rounded-md text-fg-secondary transition-colors duration-150 hover:bg-surface-hover hover:text-fg focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500/40"
			>
				<svg width="18" height="18" viewBox="0 0 18 18" fill="none" aria-hidden="true">
					<path
						d="M2 5h14M2 9h14M2 13h14"
						stroke="currentColor"
						stroke-width="1.5"
						stroke-linecap="round"
					/>
				</svg>
			</button>
			<a href={link('/')} class="flex items-center gap-2">
				{@render mark()}
				<span class="text-[15px] font-semibold tracking-tight text-fg">OrkStack</span>
			</a>
		</header>

		{#if mobileNavOpen}
			<div class="fixed inset-0 z-50 lg:hidden">
				<div
					class="fixed inset-0 bg-gray-950/40 animate-fade-in motion-reduce:animate-none"
					onclick={() => (mobileNavOpen = false)}
					role="presentation"
				></div>
				<div class="fixed inset-y-0 left-0 z-50 flex w-72 flex-col bg-surface shadow-md">
					<div class="flex items-center justify-between border-b border-border-subtle px-4">
						<a href={link('/')} class="flex h-14 items-center gap-2" onclick={() => (mobileNavOpen = false)}>
							{@render mark()}
							<span class="text-[15px] font-semibold tracking-tight text-fg">OrkStack</span>
						</a>
						<button
							type="button"
							aria-label="Close navigation"
							onclick={() => (mobileNavOpen = false)}
							class="inline-flex size-8 items-center justify-center rounded-md text-fg-secondary hover:bg-surface-hover hover:text-fg"
						>
							<svg width="16" height="16" viewBox="0 0 16 16" fill="none" aria-hidden="true">
								<path
									d="M3 3l10 10M13 3 3 13"
									stroke="currentColor"
									stroke-width="1.5"
									stroke-linecap="round"
								/>
							</svg>
						</button>
					</div>
					{@render namespacePicker()}
					<div class="flex-1 overflow-y-auto" onclick={() => (mobileNavOpen = false)} role="presentation">
						{@render primaryNav()}
					</div>
					{@render themeToggle()}
				</div>
			</div>
		{/if}

		{#if data.namespaceError}
			<div class="px-4 pt-4 sm:px-6 lg:px-8">
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

		<main class="mx-auto w-full max-w-[1400px] flex-1 px-4 py-6 sm:px-6 lg:px-8">
			{@render children()}
		</main>
	</div>
</div>
