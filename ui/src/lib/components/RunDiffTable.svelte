<script lang="ts">
	// The task rows of a run comparison, in the order the engine returned them.
	//
	// Every value is the engine's rendered string (DiffRuns), so this table only
	// decides how a side LOOKS: `-` for "does not apply", a muted `unavailable`
	// carrying its reason sentence for evidence that is gone — never alike, the
	// same rule `orkctl run diff` prints by. Only the two value cells of a
	// differing pair are tinted, never the row: a diff row is a difference, not
	// an outcome.
	import type { RunFieldDiff } from '$lib/gen/orkstack/v1/run_service_pb';
	import { reasonText, renderDiffValue } from '$lib/runDiff';
	import { itemLabel } from '$lib/each';

	interface Props {
		rows: RunFieldDiff[];
	}

	let { rows }: Props = $props();

	const taskRows = $derived(rows.filter((row) => row.taskId !== ''));

	function cellClass(available: boolean, tint: string): string {
		if (!available) return 'font-normal text-fg-muted italic';
		return tint;
	}

	// A pair where both sides answered and differ is the ordinary row; a pair
	// with an unavailable side is tinted on the side that answered only.
	const FROM_TINT = 'bg-failed-bg text-failed-fg';
	const TO_TINT = 'bg-succeeded-bg text-succeeded-fg';
</script>

{#if taskRows.length === 0}
	<p class="text-sm text-fg-muted" data-testid="run-diff-empty">No task differences.</p>
{:else}
	<div class="overflow-x-auto rounded-md border border-border">
		<table class="w-full border-collapse text-xs" data-testid="run-diff-table">
			<thead class="bg-surface-sunken">
				<tr>
					<th class="h-8 px-3 text-left font-medium text-fg-muted">TASK</th>
					<th class="h-8 px-3 text-left font-medium text-fg-muted">FIELD</th>
					<th class="h-8 w-[38%] px-3 text-left font-medium text-fg-muted">FROM</th>
					<th class="h-8 w-[38%] px-3 text-left font-medium text-fg-muted">TO</th>
				</tr>
			</thead>
			<tbody>
				{#each taskRows as row, i (i)}
					{@const label = row.item !== undefined ? itemLabel(row.taskId, row.item) : row.taskId}
					<tr class="border-t border-border-subtle" data-testid="run-diff-row-{label}-{row.field}">
						<td class="px-3 py-2 align-top font-mono text-fg">{label}</td>
						<td class="px-3 py-2 align-top font-mono text-fg-secondary">{row.field}</td>
						<td
							class="px-3 py-2 align-top font-mono break-all {cellClass(row.fromAvailable, FROM_TINT)}"
							data-testid="run-diff-row-{label}-{row.field}-from"
							data-unavailable={row.fromAvailable ? 'false' : 'true'}
							title={row.fromAvailable ? undefined : reasonText(row.fromReason)}
						>{renderDiffValue(row.fromValue, row.fromAvailable)}</td>
						<td
							class="px-3 py-2 align-top font-mono break-all {cellClass(row.toAvailable, TO_TINT)}"
							data-testid="run-diff-row-{label}-{row.field}-to"
							data-unavailable={row.toAvailable ? 'false' : 'true'}
							title={row.toAvailable ? undefined : reasonText(row.toReason)}
						>{renderDiffValue(row.toValue, row.toAvailable)}</td>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
{/if}
