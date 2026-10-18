<script lang="ts">
	// One plan, rendered the way `orkctl run plan` prints it: the summary
	// sentence, the FAILS sentence when the run would fail, a Wave · Task ·
	// Outcome · Detail table in the CLI's row order, and the caveat lines.
	//
	// Every row order and sentence comes from $lib/plan, which mirrors
	// cmd/orkctl/run_plan.go, so this component holds no logic of its own. A
	// plan is not a run: the outcome badges are deliberately NOT the status
	// palette's succeeded/failed, because nothing succeeded or failed.
	import { PlanOutcome, type PlanRunResponse } from '$lib/gen/orkstack/v1/orkflow_service_pb';
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
		/** The task ids the request assumed failed, for the older-server caveat. */
		assumeFailed?: readonly string[];
	}

	let { plan, assumeFailed = [] }: Props = $props();

	const rows = $derived(planRows(plan));
	const failure = $derived(planFailure(plan));
	const caveats = $derived(planCaveats(plan, assumeFailed));

	const OUTCOME_CLASSES: Record<PlanOutcome, string> = {
		[PlanOutcome.UNSPECIFIED]: 'border-rule-strong bg-paper text-ink-muted',
		[PlanOutcome.RUNS]: 'border-blue bg-blue-tint text-blue-strong',
		[PlanOutcome.SKIPPED]: 'border-dashed border-skipped-border bg-skipped-bg text-skipped-fg',
		[PlanOutcome.FAILS]: 'border-failed-border bg-failed-bg text-failed-fg',
		[PlanOutcome.UNREACHED]: 'border-dashed border-red bg-paper text-red-strong',
		[PlanOutcome.AWAITS]: 'border-awaiting-border bg-awaiting-bg text-awaiting-fg',
		// The request's assumption, not a prediction: drawn dashed, in the
		// failure palette.
		[PlanOutcome.ASSUMED_FAILED]: 'border-dashed border-failed-border bg-failed-bg text-failed-fg'
	};
</script>

<section class="space-y-3" data-testid="run-plan-result">
	<p class="text-sm text-ink" data-testid="run-plan-summary">{planSummary(plan)}</p>
	{#if failure}
		<p
			class="severity-error border border-red bg-red-tint px-3 py-2 text-sm"
			data-testid="run-plan-fails"
		>
			{failure}
		</p>
	{/if}
	<div class="overflow-x-auto border-t-2 border-ink">
		<table class="register text-xs" data-testid="run-plan-table">
			<thead>
				<tr>
					<th class="w-14">Wave</th>
					<th>Task</th>
					<th>Outcome</th>
					<th>Detail</th>
				</tr>
			</thead>
			<tbody>
				{#each rows as row (row.taskId)}
					<tr data-testid="run-plan-row-{row.taskId}">
						<td class="align-top font-mono text-ink-secondary" data-testid="run-plan-row-{row.taskId}-wave"
							>{planWaveLabel(row)}</td
						>
						<td class="align-top font-mono text-ink">{row.taskId}</td>
						<td class="align-top">
							<span
								data-testid="run-plan-row-{row.taskId}-outcome"
								class="stamp {OUTCOME_CLASSES[
									row.outcome
								]}">{planOutcomeWord(row.outcome)}</span
							>
						</td>
						<td
							class="align-top font-mono break-all text-ink-secondary"
							data-testid="run-plan-row-{row.taskId}-detail">{planDetail(row, plan)}</td
						>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
	{#each caveats as caveat, i (i)}
		<p class="text-xs text-ink-muted" data-testid="run-plan-caveat">{caveat}</p>
	{/each}
</section>
