<script lang="ts">
	import '../app.css';
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import { DEFAULT_NAMESPACE, namespaceLabel, withNamespace } from '$lib/namespace';
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
		const next = name === DEFAULT_NAMESPACE ? '' : name;
		void goto(withNamespace(page.url.pathname, next), { invalidateAll: true });
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
			<p data-testid="namespace-error" class="mx-auto max-w-6xl px-6 pb-2 text-xs text-amber-300/80">
				Showing namespace {namespaceLabel(data.namespace)}; the other namespaces could not be listed:
				{data.namespaceError}
			</p>
		{/if}
	</header>
	<main class="mx-auto max-w-6xl px-6 py-8">
		{@render children()}
	</main>
</div>
