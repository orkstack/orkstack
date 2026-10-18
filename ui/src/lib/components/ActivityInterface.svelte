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
		<p class="text-ink-secondary" data-testid="activity-interface-malformed">
			<span class="caps text-ink-muted">Interface</span> {interfaceHeadline(info)}
		</p>
	{:else if !iface}
		<p class="text-ink-muted" data-testid="activity-interface-none">
			<span class="caps text-ink-muted">Interface</span> {interfaceHeadline(info)}
		</p>
	{:else}
		<p class="text-ink-secondary" data-testid="activity-interface-summary">
			<span class="caps text-ink-muted">Interface</span> {interfaceHeadline(info)}
		</p>
		<div class="overflow-x-auto">
			<table class="register text-xs" data-testid="activity-interface-table">
				<thead>
					<tr>
						{#if hasOps}<th scope="col">Op</th>{/if}
						<th scope="col">Key</th>
						<th scope="col">Type</th>
						<th scope="col">Required</th>
						<th scope="col">Default</th>
					</tr>
				</thead>
				<tbody>
					{#each rows as row (`${row.op}-${row.key}`)}
						<tr data-testid="activity-interface-row-{row.op}-{row.key}">
							{#if hasOps}<td class="py-1 font-mono text-ink-secondary">{row.op}</td>{/if}
							<td class="py-1 font-mono text-ink">{row.key}</td>
							<td class="py-1 font-mono text-ink-secondary">{row.type}</td>
							<td class="py-1 {row.required === 'yes' ? 'font-semibold text-ink' : 'text-ink-muted'}">
								{row.required === 'yes' ? 'required' : row.required === 'no' ? 'optional' : '-'}
							</td>
							<td class="py-1 font-mono text-ink-muted">{row.default}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
		{#if values.length > 0}
			<div>
				<p class="caps mb-1 text-ink-muted">
					Values the image writes to values.json
				</p>
				<table class="border-collapse text-left text-xs" data-testid="activity-interface-values">
					<tbody>
						{#each values as value (value.key)}
							<tr data-testid="activity-interface-value-{value.key}">
								<td class="py-0.5 pr-4 font-mono text-ink">{value.key}</td>
								<td class="py-0.5 font-mono text-ink-secondary">{value.type}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>
