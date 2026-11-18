<script lang="ts">
	// The Orkers serving one pool, and the cordon (docs/DEPLOY.md "Removing an Orker safely: cordon and drain").
	//
	// Orkers are deployment infrastructure: the same list whichever namespace
	// is selected. It is read from ActivityService.ListOrkers — the cluster's
	// poller records plus each Orker's own answer — on mount and on an
	// explicit Refresh, never cached. An Orker that could not be asked is
	// UNREACHABLE and none of its numbers are drawn: "not measured" rendered
	// as "idle" would send an operator to stop a host that may be mid-task.
	import { tick, untrack } from 'svelte';
	import TimeAgo from './TimeAgo.svelte';
	import { errorMessage } from '#lib/client.ts';
	import { activityClient } from '#lib/clients/activities.ts';
	import { FilterOperator, filterOf } from '#lib/listFilter.ts';
	import type { Orker } from '#lib/gen/orkstack/v1/activity_pb.ts';
	import { withNamespace } from '#lib/namespace.ts';
	import {
		METADATA_REACHABLE_LABEL,
		METADATA_UNKNOWN_LABEL,
		metadataConcerns,
		metadataReachable,
		operatorCordoned,
		orkerCommand,
		orkerGpu,
		orkerLoad,
		orkerPins,
		orkerState,
		orkerStateLabel,
		orkerTasks,
		MAX_UNIT_CELLS,
		profileCause,
		profileCauseChip,
		profileCauseNote,
		profileCauses,
		profileKindLabel,
		profileState,
		profileUnits,
		profileUnitsText,
		STORE_OUTAGE_LABEL,
		stoppingNote
	} from '#lib/orkers.ts';
	import { fmtTimestamp } from '#lib/status.ts';
	import CliPopover from './CliPopover.svelte';
	import Icon from './Icon.svelte';
	import { formatBytes } from '#lib/resources.ts';
	import ConfirmDialog from './ConfirmDialog.svelte';
	import ErrorPanel from './ErrorPanel.svelte';

	interface Props {
		pool: string;
		/** Moves when the page's Refresh is pressed. */
		refresh?: number;
		/** The selected namespace, carried onto run links. */
		namespace: string;
		/** Called after a Cordon or Uncordon re-read the list, so the page reads its pool cards again. */
		onchange?: () => void;
	}

	let { pool, namespace, refresh = 0, onchange }: Props = $props();

	let orkers = $state<Orker[]>([]);
	// gpu units are a column only where some Orker of the pool reports them.
	const gpuColumn = $derived(orkers.some((o) => orkerGpu(o) !== null));
	let loaded = $state(false);
	let loading = $state(false);
	let loadError = $state('');

	let cordoning = $state<Orker | null>(null);
	let reason = $state('');
	let cordonBusy = $state(false);
	let cordonError = $state('');
	let uncordonBusy = $state('');
	let actionError = $state('');
	/** Said to a screen reader after Uncordon: the row can stay cordoned for another cause. */
	let notice = $state('');

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

	$effect(() => {
		void refresh;
		untrack(() => void load());
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
		// The list was read again, whatever the answer: the cards follow it.
		onchange?.();
	}

	async function uncordon(orker: Orker): Promise<void> {
		uncordonBusy = orker.orkerId;
		actionError = '';
		notice = '';
		let cleared = false;
		try {
			const res = await activityClient(fetch).cordonOrker({ orkerId: orker.orkerId, cordoned: false });
			const still = [
				res.orker?.storeOutage ? 'it cannot reach object storage' : '',
				(res.orker?.profileCordons.length ?? 0) > 0 ? 'a profile is out of service' : ''
			].filter(Boolean);
			notice =
				still.length > 0
					? `Cleared the operator's cordon on ${orker.orkerId}. It stays cordoned because ${still.join(' and ')}.`
					: `Uncordoned ${orker.orkerId}.`;
			cleared = true;
		} catch (err) {
			actionError = `Uncordon ${orker.orkerId}: ${errorMessage(err)}`;
		} finally {
			uncordonBusy = '';
			await load();
			// The button that had focus is gone: Cordon stands in its place.
			if (cleared) {
				await tick();
				document.querySelector<HTMLElement>(`[data-testid="orker-cordon-${orker.orkerId}"]`)?.focus();
			}
		}
		onchange?.();
	}

	const stateClass: Record<string, string> = {
		ready: 'bg-ok-tint text-ok mark-square',
		cordoned: 'pill-wait',
		'store-outage': 'bg-fail-tint text-fail mark-tri',
		stopping: 'pill-wait',
		unreachable: 'bg-fail-tint text-fail mark-tri',
		// A drift takes the pool out of service here until a restart: a failure.
		'profile-drift': 'bg-fail-tint text-fail mark-tri',
		// An unavailable profile clears itself and the pool stays live: a wait.
		'profile-unavailable': 'pill-wait',
		// A cause this release cannot read keeps the Orker out of its pool.
		'profile-out': 'bg-fail-tint text-fail mark-tri'
	};
</script>

<div data-testid="pool-orkers-{pool}">
	<p class="sr" data-testid="pool-orkers-caption-{pool}">Deployment infrastructure: the same whichever namespace is selected.</p>
	<p class="sr" role="status" data-testid="pool-orkers-notice-{pool}">{notice}</p>

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
		<!-- Positioned, so a visually hidden header scrolls with the table and never widens the page. -->
		<div class="relative overflow-x-auto scroll-panel">
			<!-- svelte-ignore a11y_no_redundant_roles -->
			<table class="register orkers" role="table" aria-label="Orkers of pool {pool}">
				<thead>
					<tr>
						<th scope="col">Orker</th>
						<th scope="col">Status</th>
						<th scope="col">Profiles</th>
						<th scope="col">cpu reserved</th>
						<th scope="col">memory reserved</th>
						{#if gpuColumn}<th scope="col">gpu reserved</th>{/if}
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
									{#if rowState !== 'unreachable' && orker.buildId}build <span data-testid="orker-build-{orker.orkerId}">{orker.buildId}</span>{' · '}{/if}last seen <span data-testid="orker-last-seen-{orker.orkerId}"><TimeAgo at={orker.lastSeen} /></span>
								</small>
							</th>
							<td role="cell" data-label="Status">
								<span class="stamp {stateClass[rowState]} capitalize" data-testid="orker-state-{orker.orkerId}">{orkerStateLabel(rowState)}</span>
								{#if rowState === 'cordoned'}
									{@const beside = profileState(orker)}
									{#if beside}
										<span class="sr">, </span>
										<span class="stamp {stateClass[beside]} capitalize" data-testid="orker-profile-stamp-{orker.orkerId}">{orkerStateLabel(beside)}</span>
									{/if}
								{/if}
								{#if rowState === 'cordoned' && orker.storeOutage}
									<span class="sr">, </span>
									<span class="stamp {stateClass['store-outage']} capitalize" data-testid="orker-outage-stamp-{orker.orkerId}">{STORE_OUTAGE_LABEL}</span>
								{/if}
								{#if orker.stopping}
									<small class="ork-sub ork-note" data-testid="orker-stopping-{orker.orkerId}">{stoppingNote(orker, fmtTimestamp)}</small>
								{/if}
								{#if operatorCordoned(orker)}
									<small class="ork-sub ork-note" data-testid="orker-reason-{orker.orkerId}">Cordoned since {fmtTimestamp(orker.cordonedAt)}: “{orker.cordonReason}”</small>
								{/if}
								{#if rowState !== 'unreachable'}
									{@const concerns = metadataConcerns(orker)}
									{#if concerns.length > 0}
										{@const reachable = metadataReachable(orker)}
										<span class="sr">, </span>
										<span class="stamp {reachable ? 'bg-fail-tint text-fail mark-tri' : 'pill-wait mark-ring'} capitalize" data-testid="orker-metadata-stamp-{orker.orkerId}">{reachable ? METADATA_REACHABLE_LABEL : METADATA_UNKNOWN_LABEL}</span>
										<small class="ork-sub ork-note" data-testid="orker-metadata-{orker.orkerId}">At boot, from a container on the default network: {concerns.join('; ')}.{#if reachable} A task here could ask for the host's role; close the endpoint on the host.{:else} Not read as safe.{/if}</small>
									{/if}
								{/if}
								{#each profileCauses(orker) as cause (cause.profile + ':' + cause.cause + ':' + cause.reason)}
									<small class="ork-sub ork-note" data-testid="orker-profile-cause-{orker.orkerId}-{cause.profile}">{profileCauseNote(cause, fmtTimestamp)}</small>
								{/each}
								{#if orker.storeOutage}
									<small class="ork-sub ork-note" data-testid="orker-outage-{orker.orkerId}">Cordoned itself since {fmtTimestamp(orker.storeOutage.since)}: {orker.storeOutage.reason}. Tasks wait for it without using an attempt. It resumes by itself once object storage answers{#if operatorCordoned(orker)}, and stays cordoned until an operator's cordon is cleared too{/if}.</small>
								{/if}
							</td>
							{#if rowState === 'unreachable'}
								<td role="cell" colspan={gpuColumn ? 6 : 5} class="text-dense text-ink-2" data-testid="orker-unavailable-{orker.orkerId}">
									Could not be asked: {orker.unavailableReason}. Its load is <strong>unknown</strong>, not zero.
								</td>
							{:else}
								<td role="cell" data-label="Profiles" data-testid="orker-profiles-{orker.orkerId}">
									{#if orker.profiles.length === 0}
										<span class="text-ink-3">—</span>
									{:else}
										<ul class="profiles">
											{#each orker.profiles as p (p.name)}
												{@const cause = profileCause(orker, p.name)}
												{@const units = profileUnits(p)}
												<li class="profile" data-testid="orker-profile-{orker.orkerId}-{p.name}">
													<span class="profile-name">{p.name}</span>
													<span class="profile-kind">{profileKindLabel(p.kind)}</span>
													{#if cause}
														{@const chip = profileCauseChip(cause)}
														<span class="sr">, </span>
														<span class="stamp stamp-sm {chip.failure ? 'bg-fail-tint text-fail mark-tri' : 'pill-wait'}" data-testid="orker-profile-state-{orker.orkerId}-{p.name}">{chip.word}</span>
													{/if}
													{#if units}
														<span class="units" role="img" aria-label={profileUnitsText(p)} data-testid="orker-profile-units-{orker.orkerId}-{p.name}">
															{#each { length: Math.min(units.total, MAX_UNIT_CELLS) } as _, i (i)}
																<span class="unit" class:unit-bound={i < units.bound} class:unit-bad={i >= units.bound + units.free}></span>
															{/each}
														</span>
														<small class="ork-sub" aria-hidden="true">{profileUnitsText(p)}</small>
													{/if}
												</li>
											{/each}
										</ul>
									{/if}
								</td>
								<td role="cell" data-label="cpu reserved" class="num">{(orker.capacity?.cpu ?? 0) > 0 ? `${orker.reserved?.cpu ?? 0} of ${orker.capacity?.cpu}` : '—'}</td>
								<td role="cell" data-label="memory reserved" class="num">{(orker.capacity?.memoryBytes ?? 0n) > 0n ? `${formatBytes(orker.reserved?.memoryBytes ?? 0n)} of ${formatBytes(orker.capacity?.memoryBytes ?? 0n)}` : '—'}</td>
								{#if gpuColumn}<td role="cell" data-label="gpu reserved" class="num" data-testid="orker-gpu-{orker.orkerId}">{orkerGpu(orker) ?? '—'}</td>{/if}
								<td role="cell" data-label="Running tasks" class="num" data-testid="orker-tasks-{orker.orkerId}">{orkerTasks(orker).replace(/^-$/, '—')}</td>
								<td role="cell" data-label="Open pins" class="num" data-testid="orker-pins-{orker.orkerId}">{orkerPins(orker).replace(/^-$/, '—')}</td>
							{/if}
							<td role="cell" class="ork-actions">
								{#if rowState !== 'unreachable'}
									{#if operatorCordoned(orker)}
										<button type="button" data-testid="orker-uncordon-{orker.orkerId}" disabled={uncordonBusy === orker.orkerId} onclick={() => void uncordon(orker)} class="btn btn-sm">Uncordon</button>
									{:else if !orker.stopping}
										<button type="button" data-testid="orker-cordon-{orker.orkerId}" onclick={() => openCordon(orker)} class="btn btn-sm"><Icon name="pause" />Cordon</button>
									{/if}
									<CliPopover testid="orker-cli-{orker.orkerId}" title="Command line: {orker.orkerId}" commands={[{ label: 'This Orker', command: orkerCommand(orker) }]} />
								{/if}
							</td>
						</tr>
						{#if orker.pins.length > 0}
							<tr>
								<td colspan={gpuColumn ? 9 : 8} class="text-dense">
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
	.ork-sub {
		display: block;
		margin-top: 2px;
		font-size: 12.5px;
		font-weight: 400;
		color: var(--ink-3);
		overflow-wrap: anywhere;
	}
	/* A long status sentence must not squeeze the id into a sliver, and the
	   headers give it the room, so every row's actions stay in the frame. */
	.orkers :global(tbody th) {
		min-width: 8.5rem;
	}
	.orkers :global(thead th) {
		white-space: normal;
	}
	/* A profile: its name, its kind, the cause that holds it and, for a gpu
	   profile, one cell per unit: bound filled, free outlined, unhealthy struck. */
	.profiles {
		display: grid;
		gap: 6px;
	}
	.profile {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 4px 8px;
	}
	.profile-name {
		font-family: var(--font-mono);
		font-size: var(--text-sm);
		color: var(--color-ink);
	}
	.profile-kind {
		font-size: var(--text-xs);
		color: var(--color-ink-3);
	}
	.units {
		display: inline-flex;
		flex-wrap: wrap;
		gap: 2px;
		max-width: 9rem;
	}
	.unit {
		width: 8px;
		height: 12px;
		border: var(--rule-hair) solid var(--color-line-strong);
		border-radius: 2px;
	}
	.unit-bound {
		border-color: var(--color-navy-text);
		background: var(--color-navy-text);
	}
	.unit-bad {
		border-color: var(--color-fail-mark);
		background: linear-gradient(to top right, transparent 45%, var(--color-fail-mark) 45% 55%, transparent 55%);
	}
	@media (forced-colors: active) {
		.unit-bound {
			background: CanvasText;
		}
		.unit-bad {
			background: linear-gradient(to top right, transparent 45%, GrayText 45% 55%, transparent 55%);
		}
	}
	.ork-actions {
		white-space: nowrap;
		text-align: right;
	}
	.ork-actions > :global(*) {
		vertical-align: middle;
	}
	/* A phone keeps one line per Orker, and the table scrolls sideways. */
	@media (max-width: 640px) {
		.orkers td,
		.orkers :global(tbody th) {
			white-space: nowrap;
		}
		.ork-sub {
			overflow-wrap: normal;
		}
		/* A sentence is not a figure: it wraps in its cell, so it reads without scrolling. */
		.ork-note {
			min-width: 14rem;
			max-width: 18rem;
			white-space: normal;
			overflow-wrap: anywhere;
		}
	}
</style>
