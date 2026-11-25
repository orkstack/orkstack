<script lang="ts">
	// The Force cancel side card (E88 P10): where the run's newest force cancel
	// stands, who asked and why, and what its cascade did to each run below.
	// It reports itself here; how the run ended is the run's own error card.
	import { errorMessage } from '#lib/client.ts';
	import { runClient } from '#lib/clients/runs.ts';
	import { belowWords, cascadeInProgress, cascadeNotes, cascadeWords, inProgress, needsMore, stateWords } from '#lib/forceCancel.ts';
	import type { GetForceCancelResponse } from '#lib/gen/orkstack/v1/run_service_pb.ts';
	import { shortRunId } from '#lib/runDetail.ts';
	import Principal from './Principal.svelte';
	import TimeAgo from './TimeAgo.svelte';

	interface Props {
		runId: string;
		namespace: string;
		current: GetForceCancelResponse;
		/** The run has ended. */
		ended: boolean;
		allowed: boolean;
		cascadeAllowed: boolean;
		runHref: (runId: string) => string;
		/** Opens the dialog: the same request again finishes what is left. */
		onagain: () => void;
		/** Reads where it stands again, once its cascade was asked to stop. */
		onstopped: () => Promise<void>;
		/** Why where it stands could not be read again, '' when it was. */
		stale?: string;
		/** A newer request was made and has not been read yet. */
		asked?: boolean;
	}
	let { runId, namespace, current, ended, allowed, cascadeAllowed, runHref, onagain, onstopped, stale = '', asked = false }: Props = $props();

	const cascade = $derived(current.cascade);
	const notes = $derived(cascade ? cascadeNotes(cascade) : []);
	const again = $derived(allowed && needsMore(current));
	const seconds = $derived(current.grace ? Number(current.grace.seconds) : 0);
	let stopping = $state(false);
	let stopError = $state('');

	async function stop(): Promise<void> {
		stopping = true;
		stopError = '';
		try {
			await runClient(fetch).forceCancelRun({ runId, namespace, stop: true });
			await onstopped();
		} catch (err) {
			stopError = errorMessage(err);
		} finally {
			stopping = false;
		}
	}
</script>

<section class="card" aria-labelledby="run-force-cancel-heading" data-testid="run-force-cancel">
	<div class="card-head"><h2 id="run-force-cancel-heading">Force cancel</h2></div>
	<dl class="pkv">
		<dt>Now</dt>
		<dd data-testid="run-force-cancel-state" aria-live="polite">{stateWords(current, ended)}{#if current.cause && !inProgress(current)} <span class="font-mono">{current.cause}</span>{/if}</dd>
		<dt>Asked by</dt>
		<dd data-testid="run-force-cancel-by"><Principal name={current.principal} verified={current.principalVerified} /> <TimeAgo at={current.startedAt} /></dd>
		<dt>Why</dt>
		<dd data-testid="run-force-cancel-reason">{current.reason}</dd>
		{#if seconds > 0}<dt>Grace</dt><dd class="num">{seconds >= 120 && seconds % 60 === 0 ? `${seconds / 60} minutes` : `${seconds} s`}</dd>{/if}
		{#if cascade}
			<dt>Below it</dt>
			<dd data-testid="run-force-cancel-cascade">
				{cascadeWords(cascade)}
				{#if notes.length > 0}<ul class="notes">{#each notes as n, i (i)}<li>{n}</li>{/each}</ul>{/if}
			</dd>
		{/if}
	</dl>
	{#if stale}<p class="aside-note" role="status" data-testid="run-force-cancel-stale">Not refreshed: {stale}</p>{/if}
	{#if current.unaudited}<p class="aside-note" data-testid="run-force-cancel-unaudited">A record of it could not be written to the audit trail.</p>{/if}
	{#if asked}<p class="aside-note" role="status" data-testid="run-force-cancel-asked">A newer request was made; this is the one before it.</p>{/if}
	{#if current.newerUnknown}<p class="aside-note" data-testid="run-force-cancel-newer">A newer one of this run could not be checked; this may not be the newest.</p>{/if}
	{#if cascade && cascade.runs.length > 0}
		<details class="rows" open={cascade.runs.length <= 8}>
			<summary>{cascade.runs.length} {cascade.runs.length === 1 ? 'run' : 'runs'} below</summary>
			<ul data-testid="run-force-cancel-rows">
				{#each cascade.runs as r, i (i)}
					<li>{#if r.redacted}<span>A run of another Orkflow</span>{:else}<a href={runHref(r.runId)}>{shortRunId(r.runId)}</a>{/if}<span class="what">{belowWords(r)}</span></li>
				{/each}
			</ul>
		</details>
	{/if}
	{#if current.openDescendants && (current.openDescendants.runs.length > 0 || current.openDescendants.truncatedBy || current.openDescendants.exclusions.length > 0)}
		<div class="rows" data-testid="run-force-cancel-open">
			<h3>Still open below it</h3>
			{#if current.openDescendants.truncatedBy}<p>Not every one could be listed ({current.openDescendants.truncatedBy}).</p>{/if}
			{#if current.openDescendants.exclusions.length > 0}<p>{current.openDescendants.exclusions.length} left out ({[...new Set(current.openDescendants.exclusions.map((e) => e.cause))].join(', ')}).</p>{/if}
			<ul>
				{#each current.openDescendants.runs as r, i (i)}<li>{#if r.redacted}<span>A run of another Orkflow</span>{:else}<a href={runHref(r.runId)}>{shortRunId(r.runId)}</a><span class="what">{r.orkflow}</span>{/if}</li>{/each}
			</ul>
		</div>
	{/if}
	{#if cascade?.openAfter && cascade.openAfter.runIds.length > 0}
		<div class="rows" data-testid="run-force-cancel-after">
			<h3>Still open after the cascade</h3>
			<ul>{#each cascade.openAfter.runIds as id (id)}<li><a href={runHref(id)}>{shortRunId(id)}</a></li>{/each}</ul>
		</div>
	{/if}
	{#if again || (cascadeAllowed && cascadeInProgress(current))}
		<div class="foot">
			{#if cascadeAllowed && cascadeInProgress(current)}<button type="button" class="btn btn-sm" data-testid="force-cancel-stop" aria-disabled={stopping ? 'true' : undefined} onclick={() => { if (!stopping) void stop(); }}>{stopping ? 'Stopping…' : 'Stop cascade'}</button>{/if}
			{#if again}<button type="button" class="btn btn-sm" data-testid="force-cancel-again" onclick={onagain}>Force cancel again</button>{/if}
			{#if stopError}<span role="alert" class="min-w-0 text-sm wrap-anywhere text-fail" data-testid="force-cancel-stop-error">{stopError}</span>{/if}
		</div>
	{/if}
</section>

<style>
	.pkv {
		display: grid;
		grid-template-columns: max-content 1fr;
		gap: calc(var(--spacing) * 2) calc(var(--spacing) * 4);
		margin: 0;
		padding: calc(var(--spacing) * 4);
		font-size: var(--text-sm);
	}
	.pkv dt {
		color: var(--color-ink-3);
		font-weight: var(--font-weight-semibold);
	}
	.pkv dd {
		min-width: 0;
		margin: 0;
		color: var(--color-ink);
		overflow-wrap: anywhere;
	}
	.aside-note {
		margin: 0;
		padding: 0 calc(var(--spacing) * 4) calc(var(--spacing) * 3);
		color: var(--color-ink-2);
		font-size: var(--text-sm);
	}
	.notes,
	.rows ul {
		display: grid;
		gap: calc(var(--spacing) * 1);
		margin: calc(var(--spacing) * 1.5) 0 0;
		padding: 0;
		list-style: none;
	}
	.rows {
		padding: calc(var(--spacing) * 3) calc(var(--spacing) * 4);
		border-top: var(--rule-hair) solid var(--color-line);
		font-size: var(--text-sm);
	}
	.rows summary,
	.rows h3 {
		margin: 0;
		color: var(--color-ink-2);
		font-size: var(--text-sm);
		font-weight: var(--font-weight-semibold);
	}
	.rows ul {
		max-height: 18rem;
		overflow-y: auto;
	}
	.rows li {
		display: flex;
		flex-wrap: wrap;
		justify-content: space-between;
		gap: calc(var(--spacing) * 2);
	}
	.what {
		color: var(--color-ink-2);
		overflow-wrap: anywhere;
	}
	.foot {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: calc(var(--spacing) * 2);
		padding: calc(var(--spacing) * 3) calc(var(--spacing) * 4);
		border-top: var(--rule-hair) solid var(--color-line);
	}
</style>
