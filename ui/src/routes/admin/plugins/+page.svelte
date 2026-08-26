<script lang="ts">
	// Plugin inventory (design §6.1): what this deployment can run, as
	// reported by the connected Orkers, GLOBAL and not namespace-scoped.
	//
	// PluginInfo.version/description/schemas are reported only by the
	// deployment's DEFAULT pool (proto/orkstack/v1/service.proto); a plugin
	// found only on another pool arrives with a bare name. That is shown as a
	// gap, not filled in — acceptance 3's rule applies here too.
	import { invalidateAll } from '$app/navigation';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
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

	let expanded = $state<Record<string, boolean>>({});
	function toggle(name: string) {
		expanded[name] = !expanded[name];
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
{:else if retrying}
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
					<th class="px-4 py-3"></th>
				</tr>
			</thead>
			<tbody class="divide-y divide-zinc-800">
				{#each data.plugins as plugin (plugin.name + '@' + plugin.version)}
					<tr class="hover:bg-zinc-900/60" data-testid="plugin-row-{plugin.name}">
						<td class="px-4 py-3 font-medium text-zinc-100">{plugin.name}</td>
						<td class="px-4 py-3 text-zinc-300">
							{#if plugin.version}
								{plugin.version}
							{:else}
								<span
									class="text-zinc-500"
									data-testid="plugin-unreported-{plugin.name}"
									title="reported by name only from a non-default pool — not that it declares no version"
								>
									not reported
								</span>
							{/if}
						</td>
						<td class="max-w-md truncate px-4 py-3 text-zinc-400">{plugin.description || '—'}</td>
						<td class="px-4 py-3 text-right">
							{#if plugin.inputSchema || plugin.outputSchema}
								<button
									type="button"
									data-testid="plugin-schema-toggle-{plugin.name}"
									onclick={() => toggle(plugin.name)}
									class="rounded-md border border-zinc-700 px-3 py-1 text-xs text-zinc-200 hover:bg-zinc-800"
								>
									{expanded[plugin.name] ? 'Hide schema' : 'Show schema'}
								</button>
							{/if}
						</td>
					</tr>
					{#if expanded[plugin.name] && (plugin.inputSchema || plugin.outputSchema)}
						<tr>
							<td colspan="4" class="bg-zinc-950/60 px-4 py-4">
								<div class="grid gap-4 sm:grid-cols-2" data-testid="plugin-schema-{plugin.name}">
									<div>
										<h4 class="mb-1 text-xs font-semibold uppercase text-zinc-500">
											Input schema
										</h4>
										{#if plugin.inputSchema}
											<pre
												class="overflow-x-auto rounded-md bg-zinc-900 p-3 text-xs text-zinc-300"
												data-testid="plugin-input-schema-{plugin.name}">{JSON.stringify(
													plugin.inputSchema,
													null,
													2
												)}</pre>
										{:else}
											<p class="text-xs text-zinc-600">not reported</p>
										{/if}
									</div>
									<div>
										<h4 class="mb-1 text-xs font-semibold uppercase text-zinc-500">
											Output schema
										</h4>
										{#if plugin.outputSchema}
											<pre
												class="overflow-x-auto rounded-md bg-zinc-900 p-3 text-xs text-zinc-300"
												data-testid="plugin-output-schema-{plugin.name}">{JSON.stringify(
													plugin.outputSchema,
													null,
													2
												)}</pre>
										{:else}
											<p class="text-xs text-zinc-600">not reported</p>
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
