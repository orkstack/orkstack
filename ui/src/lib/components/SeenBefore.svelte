<script lang="ts">
	// "Has this failed this way before?" One grouped read of the last seven
	// days of this Orkflow's failures, the same scan `orkctl stats failures`
	// prints, after the page has drawn. A read that fails says so and costs the
	// page nothing.
	import { durationFromMs } from '@bufbuild/protobuf/wkt';
	import { errorMessage } from '#lib/client.ts';
	import { statsClient } from '#lib/clients/stats.ts';
	import { FailureGroupBy, type FailureGroup } from '#lib/gen/orkstack/v1/stats_pb.ts';
	import { fmtCategoryWords } from '#lib/failureWords.ts';
	import { fmtTimestamp } from '#lib/status.ts';
	import { shortRunId } from '#lib/runDetail.ts';
	import { withNamespace } from '#lib/namespace.ts';
	import Icon from './Icon.svelte';

	let { namespace, orkflow, taskId, code, runId }: { namespace: string; orkflow: string; taskId: string; code: string; runId: string } = $props();

	const WEEK_MS = 7 * 24 * 3600_000;
	let group = $state.raw<FailureGroup | null>(null);
	let read = $state(false);
	let failure = $state('');
	$effect(() => {
		const [ns, name, task, reason] = [namespace, orkflow, taskId, code];
		let live = true;
		statsClient(fetch)
			.getFailureBreakdown({ namespace: ns, orkflowName: name, lookback: durationFromMs(WEEK_MS), groupBy: [FailureGroupBy.TASK, FailureGroupBy.CODE, FailureGroupBy.CATEGORY] })
			.then(
				(res) => {
					if (!live) return;
					group = res.groups.find((g) => g.taskId === task && g.code === reason) ?? null;
					read = true;
				},
				(err) => {
					if (live) failure = errorMessage(err);
				}
			);
		return () => {
			live = false;
		};
	});
	const others = $derived((group?.exampleRunIds ?? []).filter((id) => id !== runId).slice(0, 2));
	const count = $derived(Number(group?.runCount ?? 0n));
</script>

<section class="card" aria-labelledby="run-seen-heading" data-testid="run-seen-before">
	<div class="card-head"><h2 id="run-seen-heading">Seen before</h2><span class="card-sub">last 7 days</span></div>
	<div class="diag">
		{#if failure}
			<p class="diag-line" data-testid="run-seen-error">It could not be read: {failure}</p>
		{:else if !read}
			<p class="diag-line">Reading this Orkflow's failures…</p>
		{:else if !group || count <= 1}
			<p class="diag-line" data-testid="run-seen-first">The first run in 7 days to fail this way.</p>
		{:else}
			<div class="r2"><span class="mark-tri" aria-hidden="true"></span><div><b data-testid="run-seen-count">{count} runs</b> failed the same way<small>{orkflow} · {taskId} · {code}{#if group.category} · {fmtCategoryWords(group.category)}{/if}</small></div></div>
			<div class="r2"><Icon name="clock" /><div>First {fmtTimestamp(group.firstSeen)} · latest {fmtTimestamp(group.lastSeen)}</div></div>
			{#if others.length}
				<div class="r2">
					<Icon name="runs" />
					<div>Also {#each others as id, i (id)}{#if i > 0} and {/if}<a class="font-mono text-sm" href={withNamespace(`/runs/${encodeURIComponent(id)}`, namespace)}>{shortRunId(id)}</a>{/each}</div>
				</div>
			{/if}
		{/if}
	</div>
</section>
