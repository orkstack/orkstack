<script lang="ts">
	// Force cancel (E88 P10): the reason dialog that ends a run an ordinary
	// cancel does not. A reason is required; the grace is the server's least
	// unless a longer one is chosen. Where the caller may act on the whole
	// namespace the open runs below are asked for first, and the choice to end
	// them too is offered only when there are any.
	import { durationFromMs } from '@bufbuild/protobuf/wkt';
	import type { ForceCancelRunBelow, ForceCancelRunResponse, ForceCancelSubtree } from '#lib/gen/orkstack/v1/run_service_pb.ts';
	import { errorMessage } from '#lib/client.ts';
	import { runClient } from '#lib/clients/runs.ts';
	import { belowListed, graceChoices, reasonProblem } from '#lib/forceCancel.ts';
	import { shortRunId } from '#lib/runDetail.ts';
	import { onMount } from 'svelte';
	import ConfirmDialog from './ConfirmDialog.svelte';

	interface Props {
		runId: string;
		namespace: string;
		/** The caller holds run.cancel.force and run.read at the namespace. */
		cascadeAllowed: boolean;
		/** The run has ended: only the runs below are left to end. */
		ended?: boolean;
		runHref: (runId: string) => string;
		onrequested: (resp: ForceCancelRunResponse) => void;
		onclose: () => void;
	}
	let { runId, namespace, cascadeAllowed, ended = false, runHref, onrequested, onclose }: Props = $props();

	let reason = $state('');
	let choice = $state(0);
	let cascade = $state(false);
	let below = $state<ForceCancelSubtree | undefined>();
	let belowError = $state('');
	let busy = $state(false);
	let error = $state('');
	const problem = $derived(reasonProblem(reason));
	const count = $derived(below?.runs.length ?? 0);
	const belowStatus = $derived(belowListed(below, belowError));

	onMount(() => {
		if (cascadeAllowed) void preview();
	});

	async function preview(): Promise<void> {
		try {
			const found = (await runClient(fetch).forceCancelRun({ runId, namespace, preview: true })).openDescendants;
			if (found) below = found;
			else belowError = 'the answer lists none and says nothing of them';
		} catch (err) {
			belowError = errorMessage(err);
		}
	}

	async function request(): Promise<void> {
		if (problem) return;
		busy = true;
		error = '';
		try {
			const seconds = graceChoices[choice]?.seconds;
			const resp = await runClient(fetch).forceCancelRun({
				runId, namespace, reason: reason.trim(), cascade,
				grace: seconds == null ? undefined : durationFromMs(seconds * 1000)
			});
			onrequested(resp);
		} catch (err) {
			error = errorMessage(err);
		} finally {
			busy = false;
		}
	}
</script>

{#snippet row(r: ForceCancelRunBelow)}
	{#if r.redacted}A run of another Orkflow{:else}<a href={runHref(r.runId)}>{shortRunId(r.runId)}</a> {r.orkflow}{/if}
{/snippet}

<ConfirmDialog
	testid="force-cancel-confirm"
	title="Force cancel run {shortRunId(runId)}?"
	acceptLabel="Force cancel"
	cancelLabel="Keep running"
	glyph="ban"
	acceptTestid="force-cancel-accept"
	cancelTestid="force-cancel-dismiss"
	{busy}
	{error}
	destructive
	acceptDisabled={problem !== ''}
	acceptDisabledReason={problem}
	onaccept={() => void request()}
	oncancel={onclose}
>
	{#snippet body()}
		{#if ended}
			<p>The run has ended. This finishes what its end still needs, and can end the open runs below it.</p>
		{:else}
			<p>The run is asked to cancel, and ended only if it then makes no progress for the grace, or still has not ended at the deployment’s ceiling. Nothing of a run ended that way runs afterwards: no failure handler and no output. Its tasks read as of its last event. {cascade ? 'Each open run below it is force-canceled too, with its own grace.' : 'Its child runs are only asked to cancel.'}</p>
		{/if}
		<label class="text-dense font-semibold text-ink" for="force-cancel-reason">Reason</label>
		<textarea id="force-cancel-reason" data-testid="force-cancel-reason" bind:value={reason} rows="2" class="field w-full px-2.5 py-2 text-base" placeholder="Why must this run end?"></textarea>
		<label class="text-dense font-semibold text-ink" for="force-cancel-grace">Grace</label>
		<select id="force-cancel-grace" data-testid="force-cancel-grace" class="field w-full px-2.5 py-2 text-base" bind:value={choice} aria-describedby="force-cancel-grace-hint">
			{#each graceChoices as c, i (c.label)}<option value={i}>{c.label}</option>{/each}
		</select>
		<p id="force-cancel-grace-hint" class="text-sm text-ink-3">How long it may make no progress. A child run’s own cancellation graces are not counted: choose a longer one where they are long.</p>
		{#if cascadeAllowed}
			<p class="text-sm text-ink-3" role="status" data-testid="force-cancel-below-status">{belowStatus}</p>
		{/if}
		{#if count > 0}
			<label class="below-choice"><input type="checkbox" data-testid="force-cancel-cascade" bind:checked={cascade} /> Also force-cancel its {count} open child {count === 1 ? 'run' : 'runs'}{below?.truncatedBy ? ', and any the search missed' : ''}</label>
			<details>
				<summary class="text-sm">Which {count === 1 ? 'run' : 'runs'}</summary>
				<ul class="below" data-testid="force-cancel-below">
					{#each below?.runs ?? [] as r, i (i)}<li>{@render row(r)}</li>{/each}
				</ul>
			</details>
		{/if}
	{/snippet}
</ConfirmDialog>

<style>
	.below-choice {
		display: flex;
		align-items: baseline;
		gap: calc(var(--spacing) * 2);
		color: var(--color-ink);
	}
	.below {
		display: grid;
		gap: calc(var(--spacing) * 1);
		max-height: 10rem;
		margin: calc(var(--spacing) * 2) 0 0;
		padding: 0;
		overflow-y: auto;
		font-size: var(--text-sm);
		list-style: none;
	}
	.below li {
		overflow-wrap: anywhere;
	}
</style>
