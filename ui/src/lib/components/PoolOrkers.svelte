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
	import { errorMessage } from '#lib/client.ts';
	import { activityClient } from '#lib/clients/activities.ts';
	import { FilterOperator, filterOf } from '#lib/listFilter.ts';
	import type { Orker } from '#lib/gen/orkstack/v1/activity_pb.ts';
	import { withNamespace } from '#lib/namespace.ts';
	import {
		orkerCommand,
		orkerLoad,
		orkerPins,
		orkerState,
		orkerTasks
	} from '#lib/orkers.ts';
	import { fmtTimestamp } from '#lib/status.ts';
	import CliPopover from './CliPopover.svelte';
	import Icon from './Icon.svelte';
	import { formatBytes } from '#lib/resources.ts';
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
		ready: 'bg-ok-tint text-ok mark-square',
		cordoned: 'pill-wait',
		unreachable: 'bg-fail-tint text-fail mark-tri'
	};
</script>

<div data-testid="pool-orkers-{pool}">
	<div class="orkers-note">
		<p class="text-dense text-ink-3" data-testid="pool-orkers-caption-{pool}">
			Deployment infrastructure: the same whichever namespace is selected.
		</p>
		<button type="button" data-testid="pool-orkers-refresh-{pool}" disabled={loading} onclick={() => void load()} class="btn btn-sm btn-quiet shrink-0">
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
		<p class="px-4 py-3 text-dense text-ink-3" data-testid="pool-orkers-loading-{pool}">Asking the Orkers…</p>
	{:else if orkers.length === 0}
		<p class="px-4 py-3 text-dense text-ink-3" data-testid="pool-orkers-empty-{pool}">
			No Orker answered for this pool. Orkers older than this release are counted above but cannot be
			listed here.
		</p>
	{:else}
		{#if actionError}
			<p class="mx-4 my-2 text-dense text-fail" role="alert" data-testid="pool-orkers-action-error-{pool}">{actionError}</p>
		{/if}
		<div class="overflow-x-auto scroll-panel">
			<!-- The phone layout makes rows of blocks: the roles keep it a table for assistive technology. -->
			<!-- svelte-ignore a11y_no_redundant_roles -->
			<table class="register orkers" role="table" aria-label="Orkers of pool {pool}">
				<thead>
					<tr>
						<th scope="col">Orker</th>
						<th scope="col">Status</th>
						<th scope="col">cpu</th>
						<th scope="col">Memory</th>
						<th scope="col">Running tasks</th>
						<th scope="col">Open pins</th>
						<th scope="col"><span class="sr">Actions</span></th>
					</tr>
				</thead>
				<tbody>
					{#each orkers as orker (orker.orkerId)}
						{@const rowState = orkerState(orker)}
						<!-- svelte-ignore a11y_no_redundant_roles -->
						<tr role="row" data-testid="orker-row-{orker.orkerId}">
							<th scope="row" role="rowheader">
								<span class="font-mono text-sm font-medium break-all text-ink">{orker.orkerId}</span>
								<small class="ork-sub">
									{#if rowState !== 'unreachable'}build <span data-testid="orker-build-{orker.orkerId}">{orker.buildId || '-'}</span>{' · '}{/if}last seen <span data-testid="orker-last-seen-{orker.orkerId}">{fmtTimestamp(orker.lastSeen)}</span>
								</small>
							</th>
							<td role="cell" data-label="Status">
								<span class="stamp {stateClass[rowState]} capitalize" data-testid="orker-state-{orker.orkerId}">{rowState}</span>
								{#if orker.cordoned}
									<small class="ork-sub" data-testid="orker-reason-{orker.orkerId}">Cordoned since {fmtTimestamp(orker.cordonedAt)}: “{orker.cordonReason}”</small>
								{/if}
							</td>
							{#if rowState === 'unreachable'}
								<td role="cell" colspan="4" class="text-dense text-ink-2" data-testid="orker-unavailable-{orker.orkerId}">
									Could not be asked: {orker.unavailableReason}. Its load is <strong>unknown</strong>, not zero.
								</td>
							{:else}
								<td role="cell" data-label="cpu" class="num">{(orker.capacity?.cpu ?? 0) > 0 ? `${orker.reserved?.cpu ?? 0} of ${orker.capacity?.cpu}` : '—'}</td>
								<td role="cell" data-label="Memory" class="num">{(orker.capacity?.memoryBytes ?? 0n) > 0n ? `${formatBytes(orker.reserved?.memoryBytes ?? 0n)} of ${formatBytes(orker.capacity?.memoryBytes ?? 0n)}` : '—'}</td>
								<td role="cell" data-label="Running tasks" class="num" data-testid="orker-tasks-{orker.orkerId}">{orkerTasks(orker)}</td>
								<td role="cell" data-label="Open pins" class="num" data-testid="orker-pins-{orker.orkerId}">{orkerPins(orker)}</td>
							{/if}
							<td role="cell" class="ork-actions">
								{#if rowState !== 'unreachable'}
									{#if orker.cordoned}
										<button type="button" data-testid="orker-uncordon-{orker.orkerId}" disabled={uncordonBusy === orker.orkerId} onclick={() => void uncordon(orker)} class="btn btn-sm">Uncordon</button>
									{:else}
										<button type="button" data-testid="orker-cordon-{orker.orkerId}" onclick={() => openCordon(orker)} class="btn btn-sm"><Icon name="pause" />Cordon…</button>
									{/if}
									<CliPopover testid="orker-cli-{orker.orkerId}" title="Command line: {orker.orkerId}" commands={[{ label: 'This Orker', command: orkerCommand(orker) }]} />
								{/if}
							</td>
						</tr>
						{#if orker.pins.length > 0}
							<tr>
								<td colspan="7" class="text-dense">
									<ul class="grid gap-0.5">
										{#each orker.pins as pin, i (pin.runId + ':' + i)}
											<li data-testid="orker-pin-{orker.orkerId}-{pin.runId}">
												pin {pin.pin || '-'} of run
												<a class="link font-mono" href={withNamespace(`/runs/${pin.runId}`, namespace)}>{pin.runId}</a>
												— expires {pin.expiresAt ? fmtTimestamp(pin.expiresAt) : '-'}
											</li>
										{/each}
									</ul>
								</td>
							</tr>
						{/if}
					{/each}
				</tbody>
			</table>
		</div>
	{/if}
</div>

{#if cordoning}
	<ConfirmDialog
		testid="orker-cordon-dialog"
		title="Cordon {cordoning.orkerId}?"
		acceptLabel="Cordon"
		cancelLabel="Back"
		glyph="pause"
		destructive
		acceptTestid="orker-cordon-accept"
		cancelTestid="orker-cordon-cancel"
		busy={cordonBusy}
		error={cordonError}
		acceptDisabled={!reason.trim()}
		acceptDisabledReason="Give a reason to cordon this Orker."
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
			<label class="text-dense font-semibold text-ink" for="orker-cordon-reason"
				>Reason <span class="font-normal text-ink-3">(required)</span></label
			>
			<input
				id="orker-cordon-reason"
				data-testid="orker-cordon-reason"
				bind:value={reason}
				maxlength="256"
				required
				aria-required="true"
				aria-invalid={reason.trim() ? undefined : 'true'}
				class="field h-9 w-full px-3 text-base"
				placeholder="Why is this Orker leaving the pool?"
			/>
		{/snippet}
	</ConfirmDialog>
{/if}

<style>
	.orkers-note {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		justify-content: space-between;
		gap: 4px 12px;
		padding: 8px 16px;
		border-bottom: 1px solid var(--line);
	}
	.ork-sub {
		display: block;
		margin-top: 2px;
		font-size: 12.5px;
		font-weight: 400;
		color: var(--ink-3);
		overflow-wrap: anywhere;
	}
	.ork-actions {
		white-space: nowrap;
		text-align: right;
	}
	.ork-actions > :global(*) {
		vertical-align: middle;
	}
	@media (max-width: 640px) {
		.orkers thead {
			display: none;
		}
		.orkers,
		.orkers tbody,
		.orkers td,
		.orkers th {
			display: block;
		}
		.orkers tr {
			display: grid;
			grid-template-columns: repeat(2, minmax(0, 1fr));
			gap: 10px 16px;
			padding: 14px 16px;
			border-bottom: 1px solid var(--line);
		}
		.orkers th {
			grid-column: 1 / -1;
		}
		.orkers td,
		.orkers th {
			padding: 0 !important;
			border: 0 !important;
		}
		.orkers td[data-label]::before {
			content: attr(data-label);
			display: block;
			margin-bottom: 3px;
			font-size: 12.5px;
			font-weight: 600;
			color: var(--ink-3);
		}
		.ork-actions {
			grid-column: 1 / -1;
			text-align: left;
		}
	}
</style>
