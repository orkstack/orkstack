<script lang="ts">
	// Admin → Releases (docs/DEPLOY.md §3): which build of OrkStack takes new
	// runs, the one write that changes it, and what still holds a release
	// that takes none — the parity of `orkctl release …`.
	//
	// Nothing here is stored or polled: the page is one ListReleases read,
	// re-read on Refresh (the parity of `status --watch`) and after every
	// write. Each action's dialog re-reads its release on open and shows THAT
	// read's blockers, so a decision is made on what is true now, and the
	// write carries that read's revision: a routing that changed in between
	// is refused by the engine, and the refusal is shown in the dialog.
	import { invalidateAll } from '$app/navigation';
	import { navigating, page } from '$app/state';
	import CliEquivalent from '$lib/components/CliEquivalent.svelte';
	import ConfirmDialog from '$lib/components/ConfirmDialog.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import { errorMessage, orkstack } from '$lib/client';
	import { ReleaseRole, type ListReleasesResponse, type Release } from '$lib/gen/orkstack/v1/service_pb';
	import { withNamespace } from '$lib/namespace';
	import {
		canPromote,
		canRamp,
		canRollBack,
		hasHolds,
		holdsSummary,
		noCurrentSentence,
		promoteResult,
		rampResult,
		releaseCommand,
		roleLabel,
		runsLabel,
		stateLabel,
		validRampPercent,
		versioningOffSentence
	} from '$lib/releases';
	import { shellQuote } from '$lib/runDetail';
	import { fmtTimestamp } from '$lib/status';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	let refreshing = $state(false);
	async function refresh() {
		refreshing = true;
		try {
			await invalidateAll();
		} finally {
			refreshing = false;
		}
	}
	const navigatingHere = $derived(
		navigating.to !== null && navigating.to.url.pathname === page.url.pathname
	);
	const busy = $derived(refreshing || navigatingHere);

	const resp = $derived(data.releases);
	const current = $derived(resp?.releases.find((r) => r.role === ReleaseRole.CURRENT));

	let expanded = $state<Record<string, boolean>>({});
	let result = $state('');

	// ── the dialog ──────────────────────────────────────────────────────
	type DialogKind = 'promote' | 'rollback' | 'ramp' | 'ramp-stop';
	let dialog = $state<{ kind: DialogKind; build: string } | null>(null);
	let dialogRead = $state<ListReleasesResponse | null>(null);
	let dialogLoading = $state(false);
	let dialogBusy = $state(false);
	let dialogError = $state('');
	let percent = $state('10');

	const dialogRelease = $derived(dialogRead?.releases.find((r) => r.buildId === dialog?.build));
	const dialogBlockers = $derived(dialogRelease?.blockers ?? []);
	const rampPercent = $derived(validRampPercent(percent));
	const judged = $derived(dialog?.kind !== 'ramp-stop');
	const acceptDisabled = $derived(
		dialogLoading ||
			!dialogRelease ||
			(judged && dialogBlockers.length > 0) ||
			(dialog?.kind === 'ramp' && rampPercent === null)
	);

	const titles: Record<DialogKind, string> = {
		promote: 'Promote release',
		rollback: 'Roll back to release',
		ramp: 'Ramp release',
		'ramp-stop': 'Stop the ramp of release'
	};
	const acceptLabels: Record<DialogKind, string> = {
		promote: 'Promote',
		rollback: 'Roll back',
		ramp: 'Ramp',
		'ramp-stop': 'Stop ramp'
	};

	// The command the dialog would run, or null while the typed ramp percent is
	// invalid: `--percent 0` there would be a stop-the-ramp, not what was typed.
	function dialogCommand(kind: DialogKind, build: string): string | null {
		switch (kind) {
			case 'promote':
				return releaseCommand('promote', build);
			case 'rollback':
				return releaseCommand('rollback', build);
			case 'ramp':
				return rampPercent === null ? null : releaseCommand('ramp', build, rampPercent);
			case 'ramp-stop':
				return releaseCommand('ramp', build, 0);
		}
	}

	async function openDialog(kind: DialogKind, build: string): Promise<void> {
		dialog = { kind, build };
		dialogRead = null;
		dialogError = '';
		percent = '10';
		dialogLoading = true;
		try {
			dialogRead = await orkstack(fetch).system.listReleases({ buildId: build });
			if (!dialogRead.releases.some((r) => r.buildId === build)) {
				dialogError = `no release ${build} is known to the cluster any more; refresh the page`;
			}
		} catch (err) {
			dialogError = errorMessage(err);
		} finally {
			dialogLoading = false;
		}
	}

	function closeDialog(): void {
		dialog = null;
		dialogRead = null;
		dialogError = '';
	}

	async function accept(): Promise<void> {
		if (!dialog || !dialogRead) return;
		const { kind, build } = dialog;
		const pageRead = resp;
		dialogBusy = true;
		dialogError = '';
		try {
			if (kind === 'promote' || kind === 'rollback') {
				const res = await orkstack(fetch).system.promoteRelease({
					buildId: build,
					expectedRevision: dialogRead.revision
				});
				result = promoteResult(pageRead, build, res, kind === 'rollback');
			} else {
				const p = kind === 'ramp' ? (rampPercent ?? 0) : 0;
				const res = await orkstack(fetch).system.rampRelease({
					buildId: build,
					percent: p,
					expectedRevision: dialogRead.revision
				});
				result = rampResult(pageRead, build, p, res);
			}
			closeDialog();
			await invalidateAll();
		} catch (err) {
			// Shown in the dialog, never as a silently unchanged table; the
			// table is re-read so what changed underneath is visible.
			dialogError = errorMessage(err);
			await invalidateAll();
		} finally {
			dialogBusy = false;
		}
	}

	const roleClass = (r: Release): string => {
		switch (r.role) {
			case ReleaseRole.CURRENT:
				return 'border-succeeded-border bg-succeeded-bg text-succeeded-fg';
			case ReleaseRole.RAMPING:
				return 'border-info-200 bg-info-50 text-info-700';
			case ReleaseRole.CANDIDATE:
				return 'border-border bg-surface text-fg';
			default:
				return 'border-border-subtle bg-surface-sunken text-fg-muted';
		}
	};

	const actionClass =
		'inline-flex h-7 items-center rounded-md border border-border bg-surface px-2.5 text-xs font-medium text-fg shadow-xs hover:bg-surface-hover disabled:opacity-50';

	function drainCommand(orker: string): string {
		return `orkctl orker drain ${shellQuote(orker)}`;
	}
</script>

<svelte:head><title>Admin — Releases — OrkStack</title></svelte:head>

<div class="mb-4 flex flex-wrap items-start justify-between gap-3">
	<p class="max-w-3xl text-sm text-fg-muted" data-testid="releases-scope">
		Releases of deployment <strong class="text-fg">{resp?.deploymentName || '—'}</strong>. A release
		is one build of the engine and Orkers, deployed together. Deployment-wide: the namespace switcher
		does not scope this page.
	</p>
	<div class="flex items-center gap-3">
		<span class="text-xs text-fg-muted" data-testid="releases-observed-at"
			>observed at {fmtTimestamp(resp?.observedAt)}</span
		>
		<button type="button" data-testid="releases-refresh" disabled={busy} onclick={() => void refresh()} class={actionClass}>
			{busy ? 'Refreshing…' : 'Refresh'}
		</button>
		<CliEquivalent command="orkctl release status" testid="releases-cli-status" />
	</div>
</div>

{#if result}
	<p
		class="mb-4 rounded-md border border-succeeded-border bg-succeeded-bg px-3 py-2 text-sm text-succeeded-fg"
		data-testid="releases-result"
	>
		{result}
	</p>
{/if}

{#if data.releasesError}
	<ErrorPanel
		testid="releases-error"
		retryTestid="releases-retry"
		subject="this deployment's releases"
		message={data.releasesError}
		busy={refreshing}
		onretry={() => void refresh()}
	/>
{:else if !resp}
	<p class="text-sm text-fg-muted" data-testid="releases-loading">Loading releases…</p>
{:else if !resp.versioned}
	<p
		class="rounded-md border border-border bg-surface px-4 py-3 text-sm text-fg-secondary"
		data-testid="releases-versioning-off"
	>
		{versioningOffSentence}
	</p>
{:else}
	{#if !current}
		<p
			class="mb-4 rounded-md border border-warning-200 bg-warning-50 px-4 py-3 text-sm text-warning-800"
			data-testid="releases-no-current"
		>
			No release is current: {noCurrentSentence(resp.unversionedEngineReplicas)}
		</p>
	{:else if current.engineReplicas === 0}
		<p
			class="mb-4 rounded-md border border-danger-200 bg-danger-50 px-4 py-3 text-sm text-danger-700"
			data-testid="releases-current-no-engine"
		>
			The current release has no running engine: new runs will not start.
		</p>
	{/if}

	<div class="overflow-x-auto rounded-lg border border-border bg-surface shadow-xs">
		<table class="w-full min-w-[960px] border-collapse text-left text-sm" data-testid="releases-table">
			<thead class="bg-surface-sunken">
				<tr>
					<th class="h-10 px-4 pl-5 text-xs font-medium text-fg-muted">Release</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">Role</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">Engines</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">Orkers</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">Pools served</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">Runs</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">State</th>
					<th class="h-10 px-4 pr-5 text-xs font-medium text-fg-muted">Actions</th>
				</tr>
			</thead>
			{#each resp.releases as release (release.buildId)}
				{@const build = release.buildId}
				<tbody class="border-t border-border-subtle">
					<tr data-testid="release-row-{build}">
						<td class="h-11 px-4 pl-5 align-middle font-mono text-xs font-medium text-fg">{build}</td>
						<td class="h-11 px-4 align-middle">
							<span
								class="rounded-full border px-2 py-0.5 text-xs font-medium {roleClass(release)}"
								data-testid="release-role-{build}">{roleLabel(release)}</span
							>
						</td>
						<td class="h-11 px-4 align-middle text-fg" data-testid="release-engine-{build}"
							>{release.engineReplicas}</td
						>
						<td class="h-11 px-4 align-middle text-fg" data-testid="release-orkers-{build}"
							>{release.orkers.length}</td
						>
						<td class="h-11 px-4 align-middle text-fg-secondary" data-testid="release-pools-{build}"
							>{release.poolsServed.length ? release.poolsServed.join(', ') : '—'}</td
						>
						<td
							class="h-11 px-4 align-middle text-fg"
							data-testid="release-runs-{build}"
							title={release.runsUnavailableReason || undefined}>{runsLabel(release)}</td
						>
						<td class="h-11 px-4 align-middle text-fg-secondary" data-testid="release-state-{build}"
							>{stateLabel(release)}</td
						>
						<td class="h-11 px-4 pr-5 align-middle">
							<div class="flex flex-wrap items-center gap-1.5">
								{#if canPromote(release)}
									<button type="button" class={actionClass} data-testid="release-promote-{build}" onclick={() => void openDialog('promote', build)}>Promote</button>
									<CliEquivalent command={releaseCommand('promote', build)} testid="release-cli-promote-{build}" />
								{/if}
								{#if canRollBack(release)}
									<button type="button" class={actionClass} data-testid="release-rollback-{build}" onclick={() => void openDialog('rollback', build)}>Roll back</button>
									<CliEquivalent command={releaseCommand('rollback', build)} testid="release-cli-rollback-{build}" />
								{/if}
								{#if canRamp(release)}
									<button type="button" class={actionClass} data-testid="release-ramp-{build}" onclick={() => void openDialog('ramp', build)}>Ramp…</button>
									<CliEquivalent command={releaseCommand('ramp', build, 10)} testid="release-cli-ramp-{build}" />
								{/if}
								{#if release.role === ReleaseRole.RAMPING}
									<button type="button" class={actionClass} data-testid="release-ramp-stop-{build}" onclick={() => void openDialog('ramp-stop', build)}>Stop ramp</button>
									<CliEquivalent command={releaseCommand('ramp', build, 0)} testid="release-cli-ramp-stop-{build}" />
								{/if}
							</div>
						</td>
					</tr>
					{#if (release.blockers.length > 0 && release.role !== ReleaseRole.CURRENT) || hasHolds(release) || release.runsUnavailableReason}
						<tr>
							<td colspan="8" class="px-5 pb-3">
								{#if release.blockers.length > 0 && release.role !== ReleaseRole.CURRENT}
									<div
										class="rounded-md border border-warning-200 bg-warning-50 px-3 py-2 text-xs text-warning-800"
										data-testid="release-blockers-{build}"
									>
										<p class="font-medium">
											{release.role === ReleaseRole.RAMPING
												? `${build} is ramping but cannot take more new runs — the runs it takes now are affected too:`
												: `${build} cannot take new runs yet:`}
										</p>
										<ul class="mt-1 list-disc space-y-0.5 pl-4">
											{#each release.blockers as blocker, i (i)}
												<li data-testid="release-blocker-{build}-{i}">{blocker.message}</li>
											{/each}
										</ul>
									</div>
								{/if}
								{#if release.runsUnavailableReason}
									<p class="mt-1 text-xs text-fg-muted" data-testid="release-runs-unknown-{build}">
										Runs in flight could not be counted: {release.runsUnavailableReason}
									</p>
								{/if}
								{#if hasHolds(release)}
									<button
										type="button"
										class="mt-2 text-xs font-medium text-brand-700 hover:underline"
										data-testid="release-holds-toggle-{build}"
										aria-expanded={!!expanded[build]}
										onclick={() => (expanded[build] = !expanded[build])}
									>
										{expanded[build] ? 'Hide' : 'Show'} what {build} is still finishing ({holdsSummary(release) || 'runs'})
									</button>
									{#if expanded[build]}
										<div class="mt-2 space-y-1 text-xs" data-testid="release-holds-{build}">
											{#if release.runs.length > 0}
												<ul class="space-y-0.5">
													{#each release.runs as run (run.id)}
														<li>
															<a
																class="font-mono text-brand-700 hover:underline"
																href={withNamespace('/runs/' + run.id, run.namespace)}
																data-testid="release-run-{build}-{run.id}">{run.id}</a
															>
															<span class="text-fg-secondary"
																>{run.namespace || 'default'}/{run.orkflowName}@{run.orkflowVersion}</span
															>
														</li>
													{/each}
												</ul>
												{#if release.runsInFlight > release.runs.length}
													<p class="text-fg-muted">and {release.runsInFlight - release.runs.length} more</p>
												{/if}
											{/if}
											{#each release.orkers as orker (orker.orkerId)}
												{#each orker.pins as pin, i (pin.runId + ':' + i)}
													{@const pinRun = release.runs.find((r) => r.id === pin.runId)}
													<div class="flex items-center gap-2" data-testid="release-pin-{build}-{orker.orkerId}-{pin.runId}">
														<span>
															pin {pin.pin || '-'} of run
															<!-- A pin names its run but not the run's namespace; only a run
															     listed for the release has one to link to. Any other
															     run id is text, never a link guessed into `default`. -->
															{#if pinRun}
																<a
																	class="font-mono text-brand-700 hover:underline"
																	href={withNamespace('/runs/' + pin.runId, pinRun.namespace)}
																	data-testid="release-pin-run-{pin.runId}">{pin.runId}</a
																>
															{:else}
																<span class="font-mono" data-testid="release-pin-run-{pin.runId}">{pin.runId}</span>
															{/if}
															on <span class="font-mono">{orker.orkerId}</span>, expires {pin.expiresAt
																? fmtTimestamp(pin.expiresAt)
																: '-'}
														</span>
														<CliEquivalent command={drainCommand(orker.orkerId)} testid="release-pin-cli-{orker.orkerId}" />
													</div>
												{/each}
											{/each}
											<p class="text-fg-muted">
												Keep one of its engines and one of its Orkers in every pool it serves running until it
												is drained, and stop no Orker that hosts an open pin.
											</p>
										</div>
									{/if}
								{/if}
							</td>
						</tr>
					{/if}
				</tbody>
			{/each}
		</table>
	</div>

	{#if resp.orkersUnavailableReason}
		<p
			class="mt-4 rounded-md border border-warning-200 bg-warning-50 px-3 py-2 text-sm text-warning-800"
			data-testid="releases-orkers-error"
		>
			{resp.orkersUnavailableReason}
		</p>
	{/if}

	{#if resp.unattributedOrkers.length > 0}
		<section class="mt-6" data-testid="releases-unattributed">
			<h2 class="mb-2 text-sm font-semibold text-fg">Orkers that belong to no release</h2>
			<ul class="space-y-1 text-sm text-fg-secondary">
				{#each resp.unattributedOrkers as orker (orker.orkerId)}
					<li data-testid="releases-unattributed-{orker.orkerId}">
						<span class="font-mono">{orker.orkerId}</span>:
						{#if orker.unavailableReason}
							did not answer ({orker.unavailableReason})
						{:else if !orker.buildId}
							pool {orker.pool || '—'}, started without ORK_BUILD_ID
						{:else}
							pool {orker.pool || '—'}, release {orker.buildId} is no longer known to the cluster
						{/if}
					</li>
				{/each}
			</ul>
		</section>
	{/if}
{/if}

{#if dialog}
	<ConfirmDialog
		title="{titles[dialog.kind]} {dialog.build}"
		testid="release-dialog"
		acceptTestid="release-dialog-accept"
		cancelTestid="release-dialog-cancel"
		acceptLabel={acceptLabels[dialog.kind]}
		busy={dialogBusy}
		error={dialogError}
		{acceptDisabled}
		onaccept={() => void accept()}
		oncancel={closeDialog}
	>
		{#snippet body()}
			{#if dialog}
				{#if dialog.kind === 'ramp'}
					<label class="mb-3 flex items-center gap-2">
						<span>Share of new runs (%)</span>
						<input
							data-testid="release-ramp-percent"
							bind:value={percent}
							inputmode="numeric"
							class="h-8 w-20 rounded-md border border-border bg-surface px-2 text-sm text-fg"
						/>
					</label>
					{#if rampPercent === null}
						<p class="mb-2 text-xs text-danger-700" data-testid="release-ramp-percent-error">
							A ramp is a whole percent from 1 to 100; stop a ramp with Stop ramp.
						</p>
					{/if}
				{/if}
				{#if dialogLoading}
					<p data-testid="release-dialog-loading">Reading the release as it is now…</p>
				{:else if dialog.kind === 'ramp-stop'}
					<p data-testid="release-dialog-blockers">
						Stopping a ramp only takes new runs away from a release, so nothing is checked. The runs it
						took finish on it.
					</p>
				{:else if dialogRelease}
					<div data-testid="release-dialog-blockers">
						{#if dialogBlockers.length === 0}
							<p>No blockers: this release can take new runs.</p>
						{:else}
							<p class="font-medium text-warning-800">{dialog.build} cannot take new runs yet:</p>
							<ul class="mt-1 list-disc space-y-0.5 pl-4 text-warning-800">
								{#each dialogBlockers as blocker, i (i)}
									<li>{blocker.message}</li>
								{/each}
							</ul>
						{/if}
					</div>
				{/if}
				{@const command = dialogCommand(dialog.kind, dialog.build)}
				{#if command}
					<div class="mt-3 flex items-center gap-2 text-xs text-fg-muted">
						<span>CLI equivalent</span>
						<CliEquivalent {command} testid="release-dialog-cli" />
					</div>
				{/if}
			{/if}
		{/snippet}
	</ConfirmDialog>
{/if}
