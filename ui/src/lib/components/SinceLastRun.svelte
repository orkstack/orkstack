<script lang="ts">
	// What changed since the run a reader would compare this one with: the last
	// success for a run that failed, the last finished run otherwise. RunDiff's
	// fields and nothing else: one list of recent runs, one DiffRuns, after the
	// page has drawn. A run with too many tasks to compare in passing offers the
	// comparison instead of reading it.
	import { errorMessage } from '#lib/client.ts';
	import { runClient } from '#lib/clients/runs.ts';
	import { Status } from '#lib/gen/orkstack/v1/common_pb.ts';
	import type { Run } from '#lib/gen/orkstack/v1/run_pb.ts';
	import type { DiffRunsResponse } from '#lib/gen/orkstack/v1/run_service_pb.ts';
	import { runWindowFilters } from '#lib/browsing.ts';
	import { renderDiffValue } from '#lib/runDiff.ts';
	import { isTerminal, statusLabel } from '#lib/status.ts';
	import { shortRunId } from '#lib/runDetail.ts';
	import { withNamespace } from '#lib/namespace.ts';
	import { relativeTime } from '#lib/when.ts';
	import { timestampMs } from '#lib/stats.ts';

	interface Props {
		namespace: string;
		runId: string;
		orkflow: string;
		createdAt: number | null;
		/** Compare with the last success rather than the last finished run. */
		sinceSuccess: boolean;
		/** Whether this page may read the comparison in passing. */
		read: boolean;
	}

	let { namespace, runId, orkflow, createdAt, sinceSuccess, read }: Props = $props();

	const SHOWN = 6;
	let other = $state.raw<Run | null>(null);
	let diff = $state.raw<DiffRunsResponse | null>(null);
	let settled = $state(false);
	let failure = $state('');

	$effect(() => {
		const [ns, id, name, wantSuccess, may, created] = [namespace, runId, orkflow, sinceSuccess, read, createdAt];
		let live = true;
		void (async () => {
			try {
				const client = runClient(fetch);
				const res = await client.listRuns({ namespace: ns, filters: runWindowFilters(name, '', '', ''), pageSize: 20 });
				if (!live) return;
				const earlier = res.runs.filter((r) => r.id !== id && isTerminal(r.status) && (created === null || (timestampMs(r.createdAt) ?? 0) <= created));
				const found = (wantSuccess ? earlier.find((r) => r.status === Status.SUCCEEDED) : earlier[0]) ?? null;
				other = found;
				if (found && may) {
					const answer = await client.diffRuns({ namespace: ns, fromRunId: found.id, toRunId: id, only: [] });
					if (live) diff = answer;
				}
				if (live) settled = true;
			} catch (err) {
				if (live) {
					failure = errorMessage(err);
					settled = true;
				}
			}
		})();
		return () => {
			live = false;
		};
	});

	const rows = $derived((diff?.differences ?? []).filter((d) => d.field !== 'definition' || !diff?.sameDefinitionDigest));
	const when = $derived(other?.createdAt ? relativeTime(new Date(Number(other.createdAt.seconds) * 1000)) : '');
	const fieldWord = (field: string) => field.replaceAll('_', ' ');
	// A digest in a sentence is its first twelve characters; the comparison page has it whole.
	const brief = (value: string) => value.replace(/sha256:([0-9a-f]{12})[0-9a-f]+/g, 'sha256:$1…');
</script>

<section class="card" aria-labelledby="run-since-heading" data-testid="run-since">
	<div class="card-head">
		<h2 id="run-since-heading">{sinceSuccess ? 'Since the last success' : 'Since the last run'}</h2>
		{#if other}<span class="card-sub"><span class="font-mono">{shortRunId(other.id)}</span> · {statusLabel(other.status)} {when}</span>{/if}
	</div>
	<div class="difflist">
		{#if failure}
			<p data-testid="run-since-error">It could not be read: {failure}</p>
		{:else if !settled}
			<p class="text-ink-3">Reading the last run…</p>
		{:else if !other}
			<p class="text-ink-3" data-testid="run-since-none">{sinceSuccess ? 'No earlier run of this Orkflow succeeded.' : 'This is the first run of this Orkflow.'}</p>
		{:else}
			{#each rows.slice(0, SHOWN) as row (`${row.taskId}/${row.item ?? ''}/${row.field}`)}
				<div data-testid="run-since-row">
					<b>{row.taskId || 'Run'}</b> {fieldWord(row.field)}: <span class="break-all">{brief(renderDiffValue(row.toValue, row.toAvailable))}</span>
					<div class="was">was <span class="break-all">{brief(renderDiffValue(row.fromValue, row.fromAvailable))}</span></div>
				</div>
			{/each}
			{#if rows.length > SHOWN}<p class="same">and {rows.length - SHOWN} more.</p>{/if}
			{#if diff}
				{#if rows.length === 0}<p class="same" data-testid="run-since-same">Nothing differs: same definition, input, outcomes and images.</p>
				{:else if diff.sameDefinitionDigest}<p class="same" data-testid="run-since-same">Same version and definition digest.</p>{/if}
			{/if}
			<div class="flex"><a class="btn btn-sm" data-testid="run-since-compare" href={withNamespace(`/runs/${encodeURIComponent(runId)}/compare?to=${encodeURIComponent(other.id)}`, namespace)}>Compare the two runs</a></div>
		{/if}
	</div>
</section>
