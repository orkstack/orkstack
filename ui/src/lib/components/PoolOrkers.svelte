<script lang="ts">
	// The Orkers serving one pool, and the cordon (docs/DEPLOY.md "Removing an Orker safely: cordon and drain").
	//
	// Orkers are deployment infrastructure: the same list whichever namespace
	// is selected. It is read from ActivityService.ListOrkers — the cluster's
	// poller records plus each Orker's own answer — on mount and on an
	// explicit Refresh, never cached. An Orker that could not be asked is
	// UNREACHABLE and none of its numbers are drawn: "not measured" rendered
	// as "idle" would send an operator to stop a host that may be mid-task.
	import { onMount } from 'svelte';
	import { errorMessage } from '$lib/client';
	import { activityClient } from '$lib/clients/activities';
	import { FilterOperator, filterOf } from '$lib/listFilter';
	import type { Orker } from '$lib/gen/orkstack/v1/activity_pb';
	import { withNamespace } from '$lib/namespace';
	import {
		cordonCommand,
		orkerCommand,
		orkerLoad,
		orkerPins,
		orkerState,
		orkerTasks
	} from '$lib/orkers';
	import { fmtTimestamp } from '$lib/status';
	import CliEquivalent from './CliEquivalent.svelte';
	import ConfirmDialog from './ConfirmDialog.svelte';
	import ErrorPanel from './ErrorPanel.svelte';

	interface Props {
		pool: string;
		/** The selected namespace, carried onto run links. */
		namespace: string;
	}

	let { pool, namespace }: Props = $props();

	let orkers = $state<Orker[]>([]);
	let loaded = $state(false);
	let loading = $state(false);
	let loadError = $state('');

	let cordoning = $state<Orker | null>(null);
	let reason = $state('');
	let cordonBusy = $state(false);
	let cordonError = $state('');
	let uncordonBusy = $state('');
	let actionError = $state('');

	async function load(): Promise<void> {
		loading = true;
		try {
			// The pool is this disclosure's scope: a fixed filter, no form.
			const res = await activityClient(fetch).listOrkers({ filters: [filterOf('pool', FilterOperator.EQUALS, [pool])] });
			orkers = res.orkers;
			loadError = '';
		} catch (err) {
			loadError = errorMessage(err);
		} finally {
			loading = false;
			loaded = true;
		}
	}

	onMount(() => {
		void load();
	});

	function openCordon(orker: Orker): void {
		cordoning = orker;
		reason = '';
		cordonError = '';
	}

	async function acceptCordon(): Promise<void> {
		if (!cordoning || !reason.trim()) return;
		cordonBusy = true;
		cordonError = '';
		try {
			await activityClient(fetch).cordonOrker({
				orkerId: cordoning.orkerId,
				cordoned: true,
				reason: reason.trim()
			});
			cordoning = null;
			await load();
		} catch (err) {
			// Shown in the dialog, never as a silently unchanged row; the list
			// is re-read so a host that is gone leaves it.
			cordonError = errorMessage(err);
			await load();
		} finally {
			cordonBusy = false;
		}
	}

	async function uncordon(orker: Orker): Promise<void> {
		uncordonBusy = orker.orkerId;
		actionError = '';
		try {
			await activityClient(fetch).cordonOrker({ orkerId: orker.orkerId, cordoned: false });
		} catch (err) {
			actionError = `Uncordon ${orker.orkerId}: ${errorMessage(err)}`;
		} finally {
			uncordonBusy = '';
			await load();
		}
	}

	const stateClass: Record<string, string> = {
		ready: 'border-ink text-ink',
		cordoned: 'border-dashed border-rule-strong text-ink-secondary',
		unreachable: 'border-red text-red-strong'
	};
</script>

<div class="mt-3 border-t border-rule pt-3" data-testid="pool-orkers-{pool}">
	<div class="mb-2 flex items-center justify-between gap-2">
		<p class="text-xs text-ink-muted" data-testid="pool-orkers-caption-{pool}">
			Orkers serving pool {pool} — deployment infrastructure, the same whichever namespace is selected.
		</p>
		<button
			type="button"
			data-testid="pool-orkers-refresh-{pool}"
			disabled={loading}
			onclick={() => void load()}
			class="btn btn-sm shrink-0"
		>
			{loading ? 'Refreshing…' : 'Refresh'}
		</button>
	</div>

	{#if loadError}
		<ErrorPanel
			testid="pool-orkers-error-{pool}"
			retryTestid="pool-orkers-retry-{pool}"
			subject="the Orkers of pool {pool}"
			message={loadError}
			busy={loading}
			onretry={() => void load()}
		/>
	{:else if !loaded}
		<p class="text-xs text-ink-muted" data-testid="pool-orkers-loading-{pool}">Asking the Orkers…</p>
	{:else if orkers.length === 0}
		<p class="text-xs text-ink-muted" data-testid="pool-orkers-empty-{pool}">
			No Orker answered for this pool. Orkers older than this release are counted above but cannot be
			listed here.
		</p>
	{:else}
		{#if actionError}
			<p
				class="mb-2 border border-red bg-red-tint px-2 py-1.5 text-xs text-red-strong"
				role="alert"
				data-testid="pool-orkers-action-error-{pool}"
			>
				{actionError}
			</p>
		{/if}
		<ul class="border-t-(length:--rule-region) border-ink">
			{#each orkers as orker (orker.orkerId)}
				{@const rowState = orkerState(orker)}
				<li
					class="border-b border-rule py-2 text-xs"
					data-testid="orker-row-{orker.orkerId}"
				>
					<div class="flex flex-wrap items-center justify-between gap-2">
						<span class="font-mono font-medium break-all text-ink">{orker.orkerId}</span>
						<span
							class="stamp {stateClass[rowState]}"
							data-testid="orker-state-{orker.orkerId}">{rowState}</span
						>
					</div>
					{#if rowState === 'unreachable'}
						<p class="mt-1 text-ink-secondary" data-testid="orker-unavailable-{orker.orkerId}">
							Could not be asked: {orker.unavailableReason}. Its load is <strong>unknown</strong>, not
							zero.
						</p>
					{:else}
						<dl class="mt-1 grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5 text-ink-secondary sm:grid-cols-[auto_1fr_auto_1fr_auto_1fr]">
							<dt class="caps text-ink-muted">Build</dt>
							<dd class="figure" data-testid="orker-build-{orker.orkerId}">{orker.buildId || '-'}</dd>
							<dt class="caps text-ink-muted">Tasks</dt>
							<dd class="figure" data-testid="orker-tasks-{orker.orkerId}">{orkerTasks(orker)}</dd>
							<dt class="caps text-ink-muted">Pins</dt>
							<dd class="figure" data-testid="orker-pins-{orker.orkerId}">{orkerPins(orker)}</dd>
						</dl>
						{#if orker.cordoned}
							<p class="mt-1 text-ink-secondary" data-testid="orker-reason-{orker.orkerId}">
								Cordoned since {fmtTimestamp(orker.cordonedAt)}: “{orker.cordonReason}”
							</p>
						{/if}
					{/if}
					<p class="mt-1 text-ink-muted">
						Last seen: <span data-testid="orker-last-seen-{orker.orkerId}"
							>{fmtTimestamp(orker.lastSeen)}</span
						>
					</p>
					{#if orker.pins.length > 0}
						<ul class="mt-1 space-y-0.5">
							{#each orker.pins as pin, i (pin.runId + ':' + i)}
								<li data-testid="orker-pin-{orker.orkerId}-{pin.runId}">
									pin {pin.pin || '-'} of run
									<a
										class="link font-mono"
										href={withNamespace(`/runs/${pin.runId}`, namespace)}>{pin.runId}</a
									>
									— expires {pin.expiresAt ? fmtTimestamp(pin.expiresAt) : '-'}
								</li>
							{/each}
						</ul>
					{/if}
					{#if rowState !== 'unreachable'}
						<div class="mt-2 flex items-center justify-end gap-2">
							{#if orker.cordoned}
								<button
									type="button"
									data-testid="orker-uncordon-{orker.orkerId}"
									disabled={uncordonBusy === orker.orkerId}
									onclick={() => void uncordon(orker)}
									class="btn btn-sm"
								>
									Uncordon
								</button>
							{:else}
								<button
									type="button"
									data-testid="orker-cordon-{orker.orkerId}"
									onclick={() => openCordon(orker)}
									class="btn btn-sm border-red text-red-strong"
								>
									Cordon
								</button>
							{/if}
							<CliEquivalent
								command={orkerCommand(orker)}
								testid="orker-cli-{orker.orkerId}"
								label="CLI equivalent for {orker.orkerId}"
							/>
						</div>
					{/if}
				</li>
			{/each}
		</ul>
	{/if}
</div>

{#if cordoning}
	<ConfirmDialog
		testid="orker-cordon-dialog"
		title="Cordon {cordoning.orkerId}?"
		acceptLabel="Cordon"
		acceptTestid="orker-cordon-accept"
		cancelTestid="orker-cordon-cancel"
		busy={cordonBusy}
		error={cordonError}
		acceptDisabled={!reason.trim()}
		onaccept={() => void acceptCordon()}
		oncancel={() => {
			cordoning = null;
			cordonError = '';
		}}
	>
		{#snippet body()}
			<p data-testid="orker-cordon-body">
				{cordoning?.orkerId}
				{cordoning ? orkerLoad(cordoning) : ''}; they finish where they are. It will take no new task
				and open no new pin until uncordoned or restarted.
			</p>
			<label class="mt-3 block text-sm font-medium text-ink" for="orker-cordon-reason"
				>Reason <span class="font-normal text-ink-muted">(required)</span></label
			>
			<input
				id="orker-cordon-reason"
				data-testid="orker-cordon-reason"
				bind:value={reason}
				maxlength="256"
				class="field mt-1 h-9 w-full px-3 text-sm"
				placeholder="Why is this Orker leaving the pool?"
			/>
			<p
				class="mt-3 overflow-x-auto border border-rule bg-paper-sunken p-2 font-mono text-xs text-ink-secondary"
				data-testid="orker-cordon-command"
			>
				{cordonCommand(cordoning?.orkerId ?? '', reason.trim() || '<reason>')}
			</p>
		{/snippet}
	</ConfirmDialog>
{/if}
