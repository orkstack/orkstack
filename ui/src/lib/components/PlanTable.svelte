<script lang="ts">
	// One plan, rendered the way `orkctl run plan` prints it: the summary
	// sentence, the FAILS sentence when the run would fail, a Wave · Task ·
	// Outcome · Detail table in the CLI's row order, and the caveat lines.
	//
	// Every row order and sentence comes from $lib/plan, which mirrors
	// cmd/orkctl/run_plan.go, so this component holds no logic of its own. A
	// plan is not a run: the outcome badges are deliberately NOT the status
	// palette's succeeded/failed, because nothing succeeded or failed.
	import { PlanOutcome, type PlanRunResponse } from '$lib/gen/orkstack/v1/service_pb';
	import {
		planCaveats,
		planDetail,
		planFailure,
		planOutcomeWord,
		planRows,
		planSummary,
		planWaveLabel
	} from '$lib/plan';

	interface Props {
		plan: PlanRunResponse;
	}

	let { plan }: Props = $props();

	const rows = $derived(planRows(plan));
	const failure = $derived(planFailure(plan));
	const caveats = $derived(planCaveats(plan));

	const OUTCOME_CLASSES: Record<PlanOutcome, string> = {
		[PlanOutcome.UNSPECIFIED]: 'border-border bg-surface text-fg-muted',
		[PlanOutcome.RUNS]: 'border-brand-200 bg-brand-50 text-brand-700',
		[PlanOutcome.SKIPPED]: 'border-dashed border-skipped-border bg-skipped-bg text-skipped-fg',
		[PlanOutcome.FAILS]: 'border-failed-border bg-failed-bg text-failed-fg',
		[PlanOutcome.UNREACHED]: 'border-dashed border-danger-200 bg-surface text-danger-700'
	};
</script>

<section class="space-y-3" data-testid="run-plan-result">
	<p class="text-sm text-fg" data-testid="run-plan-summary">{planSummary(plan)}</p>
	{#if failure}
		<p
			class="rounded-md border border-warning-200 bg-warning-50 px-3 py-2 text-sm text-warning-800"
			data-testid="run-plan-fails"
		>
			{failure}
		</p>
	{/if}
	<div class="overflow-x-auto rounded-md border border-border">
		<table class="w-full border-collapse text-xs" data-testid="run-plan-table">
			<thead class="bg-surface-sunken">
				<tr>
					<th class="h-8 w-14 px-3 text-left font-medium text-fg-muted">Wave</th>
					<th class="h-8 px-3 text-left font-medium text-fg-muted">Task</th>
					<th class="h-8 px-3 text-left font-medium text-fg-muted">Outcome</th>
					<th class="h-8 px-3 text-left font-medium text-fg-muted">Detail</th>
				</tr>
			</thead>
			<tbody>
				{#each rows as row (row.taskId)}
					<tr class="border-t border-border-subtle" data-testid="run-plan-row-{row.taskId}">
						<td class="px-3 py-2 align-top font-mono text-fg-secondary" data-testid="run-plan-row-{row.taskId}-wave"
							>{planWaveLabel(row)}</td
						>
						<td class="px-3 py-2 align-top font-mono text-fg">{row.taskId}</td>
						<td class="px-3 py-2 align-top">
							<span
								data-testid="run-plan-row-{row.taskId}-outcome"
								class="inline-flex items-center rounded-full border px-2 py-0.5 text-[11px] leading-4 font-medium whitespace-nowrap {OUTCOME_CLASSES[
									row.outcome
								]}">{planOutcomeWord(row.outcome)}</span
							>
						</td>
						<td
							class="px-3 py-2 align-top font-mono break-all text-fg-secondary"
							data-testid="run-plan-row-{row.taskId}-detail">{planDetail(row, plan)}</td
						>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
	{#each caveats as caveat, i (i)}
		<p class="text-xs text-fg-muted" data-testid="run-plan-caveat">{caveat}</p>
	{/each}
</section>
