<script lang="ts">
	// What one image declares about its `with:` block — the
	// org.orkstack.activity.interface label the engine read from the image's
	// config. The words are `orkctl activity inspect`'s own (lib/activity.ts
	// mirrors cmd/orkctl/activity.go), so a reader switching between the two
	// never has to translate.
	//
	// "none" and "malformed" are answers, not failures: an image without the
	// label is an ordinary activity whose with: the image itself checks at run
	// time, and a malformed label still pinned and still runs.
	import type { ActivityInfo } from '$lib/gen/orkstack/v1/activity_service_pb';
	import { interfaceHeadline, interfaceRows, valueRows } from '$lib/activity';

	interface Props {
		info: ActivityInfo;
	}

	let { info }: Props = $props();

	const iface = $derived(info.interfaceError ? undefined : info.interface);
	const rows = $derived(iface ? interfaceRows(iface) : []);
	const values = $derived(iface ? valueRows(iface) : []);
	const hasOps = $derived(iface ? Object.keys(iface.ops).length > 0 : false);
</script>

<div class="space-y-3 text-sm" data-testid="activity-interface">
	{#if info.interfaceError}
		<p class="text-warning-700" data-testid="activity-interface-malformed">
			<span class="font-medium">Interface</span> {interfaceHeadline(info)}
		</p>
	{:else if !iface}
		<p class="text-fg-muted" data-testid="activity-interface-none">
			<span class="font-medium text-fg-secondary">Interface</span> {interfaceHeadline(info)}
		</p>
	{:else}
		<p class="text-fg-secondary" data-testid="activity-interface-summary">
			<span class="font-medium">Interface</span> {interfaceHeadline(info)}
		</p>
		<div class="overflow-x-auto rounded-md border border-border-subtle">
			<table class="w-full border-collapse text-left text-xs" data-testid="activity-interface-table">
				<thead class="bg-surface-sunken">
					<tr>
						{#if hasOps}<th class="h-8 px-3 font-medium text-fg-muted">Op</th>{/if}
						<th class="h-8 px-3 font-medium text-fg-muted">Key</th>
						<th class="h-8 px-3 font-medium text-fg-muted">Type</th>
						<th class="h-8 px-3 font-medium text-fg-muted">Required</th>
						<th class="h-8 px-3 font-medium text-fg-muted">Default</th>
					</tr>
				</thead>
				<tbody>
					{#each rows as row (`${row.op}-${row.key}`)}
						<tr class="border-t border-border-subtle" data-testid="activity-interface-row-{row.op}-{row.key}">
							{#if hasOps}<td class="h-8 px-3 font-mono text-fg-secondary">{row.op}</td>{/if}
							<td class="h-8 px-3 font-mono text-fg">{row.key}</td>
							<td class="h-8 px-3 text-fg-secondary">{row.type}</td>
							<td class="h-8 px-3 {row.required === 'yes' ? 'font-medium text-fg' : 'text-fg-muted'}">
								{row.required === 'yes' ? 'required' : row.required === 'no' ? 'optional' : '-'}
							</td>
							<td class="h-8 px-3 font-mono text-fg-muted">{row.default}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
		{#if values.length > 0}
			<div>
				<p class="mb-1 text-[11px] font-medium tracking-wider text-fg-muted uppercase">
					Values the image writes to values.json
				</p>
				<table class="border-collapse text-left text-xs" data-testid="activity-interface-values">
					<tbody>
						{#each values as value (value.key)}
							<tr data-testid="activity-interface-value-{value.key}">
								<td class="py-0.5 pr-4 font-mono text-fg">{value.key}</td>
								<td class="py-0.5 text-fg-secondary">{value.type}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>
