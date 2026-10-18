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
	import Icon from './Icon.svelte';

	interface Props {
		rows: RunFieldDiff[];
	}

	let { rows }: Props = $props();

	const taskRows = $derived(rows.filter((row) => row.taskId !== ''));

	function cellClass(available: boolean, tint: string): string {
		if (!available) return 'font-normal text-ink-muted italic';
		return tint;
	}

	// A pair where both sides answered and differ is the ordinary row: FROM
	// is what was (secondary ink), TO is what changed (the blue tint, with the
	// drawing's changed-mark). Red is failure on every page and means nothing
	// about direction here.
	const FROM_TINT = 'text-ink-secondary';
	const TO_TINT = 'bg-blue-tint text-ink';
</script>

{#if taskRows.length === 0}
	<p class="text-sm text-ink-muted" data-testid="run-diff-empty">No task differences.</p>
{:else}
	<div class="overflow-x-auto border-t-2 border-ink">
		<table class="register text-xs" data-testid="run-diff-table">
			<thead>
				<tr>
					<th scope="col">Task</th>
					<th scope="col">Field</th>
					<th scope="col" class="w-[38%]">From</th>
					<th scope="col" class="w-[38%]">To</th>
				</tr>
			</thead>
			<tbody>
				{#each taskRows as row, i (i)}
					{@const label = row.item !== undefined ? itemLabel(row.taskId, row.item) : row.taskId}
					<tr data-testid="run-diff-row-{label}-{row.field}">
						<td class="align-top font-mono text-ink">{label}</td>
						<td class="align-top font-mono text-ink-secondary">{row.field}</td>
						<td
							class="align-top font-mono break-all {cellClass(row.fromAvailable, FROM_TINT)}"
							data-testid="run-diff-row-{label}-{row.field}-from"
							data-unavailable={row.fromAvailable ? 'false' : 'true'}
							title={row.fromAvailable ? undefined : reasonText(row.fromReason)}
						>{renderDiffValue(row.fromValue, row.fromAvailable)}</td>
						<td
							class="relative pl-6 align-top font-mono break-all {cellClass(row.toAvailable, TO_TINT)}"
							data-testid="run-diff-row-{label}-{row.field}-to"
							data-unavailable={row.toAvailable ? 'false' : 'true'}
							title={row.toAvailable ? undefined : reasonText(row.toReason)}
						><Icon name="delta" class="absolute top-2.5 left-2 size-3 text-blue" />{renderDiffValue(row.toValue, row.toAvailable)}</td>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
{/if}
