<script lang="ts">
	// Plugin inventory (design §6.1): what this deployment can run, as
	// reported by the connected Orkers, GLOBAL and not namespace-scoped.
	//
	// PluginInfo.version/description/schemas are reported only by the
	// deployment's DEFAULT pool (proto/orkstack/v1/service.proto); a plugin
	// found only on another pool arrives with a bare name and EVERY other
	// field empty — "not reported by the pool that has it", never "this
	// plugin declares none" (the proto's own words). That is a single
	// condition with four symptoms, and this page treats it as one: a
	// PARTIAL row, named as such, with every missing field saying so — not
	// just the version, and not a description rendered as a bare `—` a
	// reader would read as "no description" rather than "unasked".
	//
	// Entries are keyed by (name, version) on the wire (the proto's own
	// words again — two pools mid-rollout can genuinely report two versions
	// of one plugin), so this page's OWN keys — the `expanded` map, every
	// data-testid — are keyed the same way. Keying by name alone made a
	// second row for the same name (the ordinary shape of a partial entry:
	// the SAME plugin, reported once in full and once by name from another
	// pool) share one row's disclosure state and one row's identity in a
	// test, which is a collision this page can have every time a deployment
	// runs more than one pool.
	import { invalidateAll } from '$app/navigation';
	import { navigating, page } from '$app/state';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import type { PluginInfo } from '$lib/gen/orkstack/v1/service_pb';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	let retrying = $state(false);
	async function retry() {
		retrying = true;
		try {
			await invalidateAll();
		} finally {
			retrying = false;
		}
	}

	// navigatingHere covers a re-entry into this same URL a delayed RPC
	// could stall on (the Retry click above sets `retrying` itself, since it
	// calls `invalidateAll` directly rather than `goto`).
	const navigatingHere = $derived(
		navigating.to !== null && navigating.to.url.pathname === page.url.pathname
	);
	const busy = $derived(retrying || navigatingHere);


	// pluginKey is this page's identity for one inventory row: the same
	// (name, version) pair the wire keys entries by, joined into one string
	// for a Svelte #key and a data-testid. A missing version is spelled
	// `unreported` rather than left blank, so two partial entries for
	// different plugins never collide on an empty suffix.
	function pluginKey(plugin: PluginInfo): string {
		return `${plugin.name}@${plugin.version || 'unreported'}`;
	}

	// isPartial is true for a plugin reported by name only, from a pool that
	// is not this deployment's default (PluginInfo's own doc comment,
	// proto/orkstack/v1/service.proto). version, description and both
	// schemas travel together — reported by the SAME source, the default
	// pool's direct descriptor — so an absent version means all four are
	// unasked, not that the plugin happens to declare no description.
	function isPartial(plugin: PluginInfo): boolean {
		return plugin.version === '';
	}

	let expanded = $state<Record<string, boolean>>({});
	function toggle(key: string) {
		expanded[key] = !expanded[key];
	}
</script>

<svelte:head><title>Admin — Plugins — OrkStack</title></svelte:head>

<p class="mb-4 text-sm text-zinc-500" data-testid="plugins-scope-caption">
	As reported by connected Orkers, across this whole deployment.
</p>

{#if data.pluginsError}
	<ErrorPanel
		testid="plugins-error"
		retryTestid="plugins-retry"
		subject="the plugin inventory"
		message={data.pluginsError}
		busy={retrying}
		onretry={retry}
	/>
{:else if busy}
	<p class="text-sm text-zinc-400" data-testid="plugins-loading">Loading plugins…</p>
{:else if data.plugins.length === 0}
	<p class="text-sm text-zinc-400" data-testid="plugins-empty">
		No plugins were reported. Nothing has answered yet, or nothing is installed.
	</p>
{:else}
	<div class="overflow-hidden rounded-lg border border-zinc-800" data-testid="plugin-list">
		<table class="w-full text-left text-sm">
			<thead class="bg-zinc-900 text-xs uppercase tracking-wide text-zinc-400">
				<tr>
					<th class="px-4 py-3">Name</th>
					<th class="px-4 py-3">Version</th>
					<th class="px-4 py-3">Description</th>
					<th class="px-4 py-3">Schema</th>
				</tr>
			</thead>
			<tbody class="divide-y divide-zinc-800">
				{#each data.plugins as plugin (pluginKey(plugin))}
					{@const key = pluginKey(plugin)}
					{@const partial = isPartial(plugin)}
					<tr class="hover:bg-zinc-900/60" data-testid="plugin-row-{key}">
						<td class="px-4 py-3 font-medium text-zinc-100">
							{plugin.name}
							{#if partial}
								<span
									class="ml-2 rounded-full bg-amber-500/15 px-2 py-0.5 text-xs font-medium text-amber-300"
									data-testid="plugin-partial-{key}"
									title="reported by name only, from a pool that is not this deployment's default — version, description and schemas were never asked of it"
								>
									name only
								</span>
							{/if}
						</td>
						<td class="px-4 py-3 text-zinc-300">
							{#if partial}
								<span
									class="text-zinc-500"
									data-testid="plugin-unreported-{key}"
									title="reported by name only from a non-default pool — not that it declares no version"
								>
									not reported
								</span>
							{:else}
								{plugin.version}
							{/if}
						</td>
						<td class="max-w-md truncate px-4 py-3 text-zinc-400">
							{#if partial}
								<span
									class="text-zinc-500"
									data-testid="plugin-description-unreported-{key}"
									title="reported by name only from a non-default pool — not that it declares no description"
								>
									not reported
								</span>
							{:else}
								{plugin.description || '—'}
							{/if}
						</td>
						<td class="px-4 py-3 text-right">
							{#if partial}
								<span
									class="text-xs text-zinc-600"
									data-testid="plugin-schema-unreported-{key}"
									title="reported by name only from a non-default pool — not that it declares no schema"
								>
									not reported
								</span>
							{:else if plugin.inputSchema || plugin.outputSchema}
								<button
									type="button"
									data-testid="plugin-schema-toggle-{key}"
									onclick={() => toggle(key)}
									class="rounded-md border border-zinc-700 px-3 py-1 text-xs text-zinc-200 hover:bg-zinc-800"
								>
									{expanded[key] ? 'Hide schema' : 'Show schema'}
								</button>
							{:else}
								<span class="text-xs text-zinc-600">no schema declared</span>
							{/if}
						</td>
					</tr>
					{#if !partial && expanded[key] && (plugin.inputSchema || plugin.outputSchema)}
						<tr>
							<td colspan="4" class="bg-zinc-950/60 px-4 py-4">
								<div class="grid gap-4 sm:grid-cols-2" data-testid="plugin-schema-{key}">
									<div>
										<h4 class="mb-1 text-xs font-semibold uppercase text-zinc-500">
											Input schema
										</h4>
										{#if plugin.inputSchema}
											<pre
												class="overflow-x-auto rounded-md bg-zinc-900 p-3 text-xs text-zinc-300"
												data-testid="plugin-input-schema-{key}">{JSON.stringify(
													plugin.inputSchema,
													null,
													2
												)}</pre>
										{:else}
											<p class="text-xs text-zinc-600">no schema declared</p>
										{/if}
									</div>
									<div>
										<h4 class="mb-1 text-xs font-semibold uppercase text-zinc-500">
											Output schema
										</h4>
										{#if plugin.outputSchema}
											<pre
												class="overflow-x-auto rounded-md bg-zinc-900 p-3 text-xs text-zinc-300"
												data-testid="plugin-output-schema-{key}">{JSON.stringify(
													plugin.outputSchema,
													null,
													2
												)}</pre>
										{:else}
											<p class="text-xs text-zinc-600">no schema declared</p>
										{/if}
									</div>
								</div>
							</td>
						</tr>
					{/if}
				{/each}
			</tbody>
		</table>
	</div>
{/if}
