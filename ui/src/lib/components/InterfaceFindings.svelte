<script lang="ts">
	// One CompareInterfacesResponse, rendered the way `orkctl orkflow compat`
	// prints it: a heading naming both versions, the verdict line, and one row
	// per finding. Every word of a finding is the engine's; a row that is not
	// compatible carries its message beneath it, verbatim.
	import { changeWord, verdictChipClass, verdictLine, verdictWord } from '$lib/compat';
	import { InterfaceVerdict, type CompareInterfacesResponse } from '$lib/gen/orkstack/v1/orkflow_service_pb';

	interface Props {
		name: string;
		response: CompareInterfacesResponse;
	}

	let { name, response }: Props = $props();
</script>

<div data-testid="compat-result">
	<h4 class="mb-1 text-sm font-semibold text-fg" data-testid="compat-heading">
		{name}
		{response.fromVersion} → {response.toVersion}
	</h4>
	<p
		class="mb-3 text-sm {response.verdict === InterfaceVerdict.BREAKING
			? 'font-medium text-danger-700'
			: 'text-fg-secondary'}"
		data-testid="compat-verdict"
	>
		{verdictLine(response.verdict, response.findings)}
	</p>
	<div class="overflow-x-auto rounded-lg border border-border bg-surface shadow-xs">
		<table class="w-full min-w-[640px] border-collapse text-left text-sm" data-testid="compat-findings">
			<thead class="bg-surface-sunken">
				<tr>
					<th class="h-10 px-4 pl-5 text-xs font-medium text-fg-muted">Field</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">Change</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">From</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">To</th>
					<th class="h-10 px-4 pr-5 text-xs font-medium text-fg-muted">Verdict</th>
				</tr>
			</thead>
			<tbody>
				{#each response.findings as finding, index (index)}
					<tr
						class="border-t border-border-subtle align-top"
						data-testid="compat-finding-{finding.field}-{changeWord(finding.change)}"
					>
						<td class="px-4 py-2.5 pl-5 font-mono text-xs text-fg">{finding.field}</td>
						<td class="px-4 py-2.5 font-mono text-xs text-fg-secondary">{changeWord(finding.change)}</td>
						<td class="px-4 py-2.5 text-xs text-fg-secondary">{finding.from || '—'}</td>
						<td class="px-4 py-2.5 text-xs text-fg-secondary">{finding.to || '—'}</td>
						<td class="px-4 py-2.5 pr-5">
							<span class={verdictChipClass(finding.verdict)}>{verdictWord(finding.verdict)}</span>
						</td>
					</tr>
					{#if finding.verdict !== InterfaceVerdict.COMPATIBLE}
						<tr data-testid="compat-message-{finding.field}-{changeWord(finding.change)}">
							<td colspan="5" class="px-4 pb-3 pl-5 text-xs text-fg-secondary">{finding.message}</td>
						</tr>
					{/if}
				{/each}
			</tbody>
		</table>
	</div>
</div>
