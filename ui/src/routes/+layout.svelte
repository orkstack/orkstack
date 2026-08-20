<script lang="ts">
	import '../app.css';
	import { goto, invalidateAll } from '$app/navigation';
	import { page } from '$app/state';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import {
		DEFAULT_NAMESPACE,
		hasNamespaceParam,
		namespaceLabel,
		rememberNamespace,
		rememberedNamespace,
		withNamespace
	} from '$lib/namespace';
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
	// carrying `?orkflow=deploy` across names a different orkflow or none.
	function switchTo(name: string) {
		// Remembered BEFORE the navigation, and remembered under its label, so
		// that choosing `default` — which carries no parameter — is a choice the
		// restore below honours rather than an absence it overrides.
		rememberNamespace(name);
		const next = name === DEFAULT_NAMESPACE ? '' : name;
		void goto(withNamespace(page.url.pathname, next), { invalidateAll: true });
	}

	// A URL that names no namespace is answered by the last one CHOSEN in this
	// browser, and the choice is written back into the address bar (design
	// §1.9). Without this, opening a bookmarked `/orkflows` drops somebody
	// working in `payments` back into `default` with no error and no hint.
	//
	// goto(..., { replaceState: true }) rather than the shallow replaceState
	// helper: the URL is the loaders' input, so rewriting it without re-running
	// them would leave the address bar saying `payments` over `default`'s data,
	// which is the same lie with better manners. It replaces rather than pushes
	// so the back button still leaves the app.
	$effect(() => {
		const url = page.url;
		if (hasNamespaceParam(url)) {
			rememberNamespace(data.namespace);
			return;
		}
		const remembered = rememberedNamespace();
		if (!remembered) return;
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
</script>

<div class="min-h-screen">
	<header class="border-b border-zinc-800 bg-zinc-900/60">
		<nav class="mx-auto flex max-w-6xl items-center gap-6 px-6 py-3">
			<a href={link('/orkflows')} class="text-lg font-bold tracking-tight text-zinc-50">
				Ork<span class="text-emerald-400">Stack</span>
			</a>
			<a href={link('/orkflows')} class="text-sm text-zinc-300 hover:text-white">Orkflows</a>
			<a href={link('/runs')} class="text-sm text-zinc-300 hover:text-white">Runs</a>
			<a href={link('/submit')} class="text-sm text-zinc-300 hover:text-white">Submit run</a>
			<a href={link('/apply')} class="text-sm text-zinc-300 hover:text-white">Apply orkflow</a>
			<label class="ml-auto flex items-center gap-2 text-sm text-zinc-400">
				<span>Namespace</span>
				<select
					data-testid="namespace-picker"
					class="rounded-md border border-zinc-700 bg-zinc-900 px-2 py-1 text-sm text-zinc-100"
					value={namespaceLabel(data.namespace)}
					onchange={(e) => switchTo(e.currentTarget.value)}
				>
					{#each options as name (name)}
						<option value={name}>{name}</option>
					{/each}
				</select>
			</label>
		</nav>
		{#if data.namespaceError}
			<div class="mx-auto max-w-6xl px-6 pb-3">
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
	</header>
	<main class="mx-auto max-w-6xl px-6 py-8">
		{@render children()}
	</main>
</div>
