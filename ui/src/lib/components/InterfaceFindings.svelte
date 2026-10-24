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
	<h4 class="mb-1 text-sm font-semibold text-ink" data-testid="compat-heading">
		{name}
		{response.fromVersion} → {response.toVersion}
	</h4>
	<p
		class="mb-3 text-sm {response.verdict === InterfaceVerdict.BREAKING
			? 'severity-error font-semibold'
			: 'text-ink-secondary'}"
		data-testid="compat-verdict"
	>
		{verdictLine(response.verdict, response.findings)}
	</p>
	<div class="overflow-x-auto border-t-2 border-ink">
		<table class="register min-w-[640px]" data-testid="compat-findings">
			<thead>
				<tr>
					<th>Field</th>
					<th>Change</th>
					<th>From</th>
					<th>To</th>
					<th>Verdict</th>
				</tr>
			</thead>
			<tbody>
				{#each response.findings as finding, index (index)}
					<tr
						class="align-top"
						data-testid="compat-finding-{finding.field}-{changeWord(finding.change)}"
					>
						<td class="font-mono text-xs text-ink">{finding.field}</td>
						<td class="font-mono text-xs text-ink-secondary">{changeWord(finding.change)}</td>
						<td class="text-xs text-ink-secondary">{finding.from || '—'}</td>
						<td class="text-xs text-ink-secondary">{finding.to || '—'}</td>
						<td>
							<span class={verdictChipClass(finding.verdict)}>{verdictWord(finding.verdict)}</span>
						</td>
					</tr>
					{#if finding.verdict !== InterfaceVerdict.COMPATIBLE}
						<tr data-testid="compat-message-{finding.field}-{changeWord(finding.change)}">
							<td colspan="5" class="text-xs text-ink-secondary">{finding.message}</td>
						</tr>
					{/if}
				{/each}
			</tbody>
		</table>
	</div>
</div>
