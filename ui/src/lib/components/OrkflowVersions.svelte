<script lang="ts">
	// The Versions tab: what is stored, what runs, what changed between two of
	// them, and the one control that changes which version runs.
	//
	// Nothing here is optimistic. A badge moves only after the engine says the
	// pointer moved, because the store arbitrates two promotions with a
	// compare-and-swap: a UI that moved the badge on click would show the loser
	// of that race a result that never happened.
	import { untrack } from 'svelte';
	import { goto, invalidateAll } from '$app/navigation';
	import ConfirmDialog from '$lib/components/ConfirmDialog.svelte';
	import DiffView from '$lib/components/DiffView.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import { errorMessage, orkstack } from '$lib/client';
	import { Status } from '$lib/gen/orkstack/v1/common_pb';
	import type { OrkflowVersion } from '$lib/gen/orkstack/v1/service_pb';
	import { namespaceLabel, withNamespace } from '$lib/namespace';
	import { fmtTimestamp } from '$lib/status';

	interface Props {
		name: string;
		namespace: string;
		versions: OrkflowVersion[];
		currentVersion: string;
		headVersion: string;
		error: string | null;
	}

	let { name, namespace, versions, currentVersion, headVersion, error }: Props = $props();

	// runsNow answers, for one row, "is this the version an unnamed run
	// executes". It is the ENGINE's answer (is_current), read rather than
	// recomputed: comparing against the head instead would agree until somebody
	// rolls back, and then quietly point at the wrong code — which is the one
	// case the badge exists for.
	const runsNow = (version: OrkflowVersion) => version.isCurrent;

	// resolved is that version's name, for the sentences that have to say it.
	const resolved = $derived(versions.find(runsNow)?.version ?? '');

	// editHref makes this table the entry point to editing: a row's Edit opens
	// THAT row's document, because the editor reads the version the link names.
	// Without it the Edit tab would always open the head — so the one edit an
	// operator most often wants, changing the version that RUNS, would show
	// somebody else's text. The namespace is stated like on every other
	// internal link.
	const editHref = (version: string) =>
		withNamespace(`/orkflows/${name}?version=${version}&tab=edit`, namespace);

	let reloading = $state(false);
	async function reload() {
		reloading = true;
		try {
			await Promise.all([invalidateAll(), checkDeletionSafety()]);
		} finally {
			// Whatever happened, the view stops waiting: either the data arrived
			// or the loader's own error is rendered below with this button again.
			reloading = false;
		}
	}

	// ── promote ────────────────────────────────────────────────────────────
	let pendingPromotion = $state('');
	let promoting = $state(false);
	let promoteError = $state('');

	function askToPromote(version: string) {
		pendingPromotion = version;
		promoteError = '';
	}

	async function promote() {
		promoting = true;
		promoteError = '';
		try {
			await orkstack(fetch).orkflows.setCurrentOrkflowVersion({
				name,
				version: pendingPromotion,
				namespace
			});
			pendingPromotion = '';
			await invalidateAll();
		} catch (err) {
			// A lost compare-and-swap lands here as FailedPrecondition. The dialog
			// stays open holding the engine's sentence, and the button is the
			// retry: re-reading and deciding again is the whole recovery.
			promoteError = errorMessage(err);
		} finally {
			promoting = false;
		}
	}

	// ── delete ────────────────────────────────────────────────────────────
	const deletionRunWindowMs = 24 * 60 * 60 * 1000;
	type DeleteSafety = 'checking' | 'safe' | 'blocked' | 'error';
	let deleteSafety = $state<DeleteSafety>('checking');
	let deleteSafetyReason = $state('Checking for runs that block deletion…');
	let deleteCheckSequence = 0;
	let pendingDelete = $state(false);
	let deleting = $state(false);
	let deleteError = $state('');

	function terminal(status: Status): boolean {
		return (
			status === Status.SUCCEEDED ||
			status === Status.FAILED ||
			status === Status.CANCELED ||
			status === Status.SKIPPED
		);
	}

	function timestampMs(timestamp: { seconds: bigint; nanos: number }): number {
		return Number(timestamp.seconds) * 1000 + Math.floor(timestamp.nanos / 1e6);
	}

	async function checkDeletionSafety() {
		const sequence = ++deleteCheckSequence;
		deleteSafety = 'checking';
		deleteSafetyReason = 'Checking for runs that block deletion…';
		try {
			// Active runs are queried separately BY STATUS, which is the whole
			// trick: the engine pushes the status into Temporal's visibility
			// query, so these two reads are answered over live executions
			// rather than over every run this orkflow ever had. An older live
			// run therefore cannot hide behind a page of newer completed ones,
			// and no read here costs a scan of the namespace.
			//
			// The ordinary page stays a BOUNDED sample, used only for the
			// 24-hour explanation. This check exists to disable the control
			// and say why; the server repeats the active-and-recent check
			// exhaustively at deletion time and is the authority that refuses.
			const [running, pending, recentPage] = await Promise.all([
				orkstack(fetch).runs.listRuns({ orkflowName: name, namespace, status: Status.RUNNING, pageSize: 1 }),
				orkstack(fetch).runs.listRuns({ orkflowName: name, namespace, status: Status.PENDING, pageSize: 1 }),
				orkstack(fetch).runs.listRuns({ orkflowName: name, namespace, pageSize: 200 })
			]);
			if (sequence !== deleteCheckSequence) return;
			const cutoff = Date.now() - deletionRunWindowMs;
			const active = running.runs.length + pending.runs.length;
			let recent = 0;
			for (const run of recentPage.runs) {
				if (terminal(run.status) && (!run.finishedAt || timestampMs(run.finishedAt) > cutoff)) {
					recent++;
				}
			}
			if (active + recent > 0) {
				deleteSafety = 'blocked';
				const lowerBound = active > 0 ? 'At least ' : '';
				deleteSafetyReason = `${lowerBound}${active + recent} run(s) block deletion (${active} active status(es), ${recent} finished within 24h or missing a finish time).`;
			} else {
				deleteSafety = 'safe';
				deleteSafetyReason = '';
			}
		} catch (err) {
			if (sequence !== deleteCheckSequence) return;
			deleteSafety = 'error';
			deleteSafetyReason = `Deletion is disabled because runs could not be checked: ${errorMessage(err)}`;
		}
	}

	$effect(() => {
		name;
		namespace;
		void checkDeletionSafety();
	});

	function askToDelete() {
		if (deleteSafety !== 'safe') return;
		pendingDelete = true;
		deleteError = '';
	}

	async function deleteOrkflow() {
		deleting = true;
		deleteError = '';
		try {
			await orkstack(fetch).orkflows.deleteOrkflow({ name, namespace });
			await goto(withNamespace('/orkflows', namespace), { invalidateAll: true });
		} catch (err) {
			// Keep the engine's refusal sentence intact in ConfirmDialog's error slot.
			deleteError = errorMessage(err);
		} finally {
			deleting = false;
		}
	}

	// ── diff ───────────────────────────────────────────────────────────────
	let diffFrom = $state('');
	let diffTo = $state('');
	let diffText = $state<string | null>(null);
	let diffPair = $state('');
	let diffing = $state(false);
	let diffError = $state('');

	// Seeded to the two newest versions — the comparison somebody opening this
	// tab almost always wants — and only while the user has chosen nothing.
	// untrack keeps the seeding out of its own dependencies: the effect watches
	// the version list, not the choice it makes from it.
	$effect(() => {
		const rows = versions;
		untrack(() => {
			if (!diffFrom && rows.length > 1) diffFrom = rows[1].version;
			if (!diffTo && rows.length > 0) diffTo = rows[0].version;
		});
	});

	async function runDiff() {
		if (!diffFrom || !diffTo) {
			diffError = 'Choose the two versions to compare.';
			return;
		}
		diffing = true;
		diffError = '';
		diffText = null;
		try {
			const res = await orkstack(fetch).orkflows.diffOrkflowVersions({
				name,
				fromVersion: diffFrom,
				toVersion: diffTo,
				namespace
			});
			diffText = res.unifiedDiff;
			diffPair = `${res.fromVersion} → ${res.toVersion}`;
		} catch (err) {
			diffError = errorMessage(err);
		} finally {
			diffing = false;
		}
	}
</script>

{#if error}
	<ErrorPanel
		testid="versions-error"
		retryTestid="versions-retry"
		subject="the versions of {name} in namespace {namespaceLabel(namespace)}"
		message={error}
		busy={reloading}
		onretry={reload}
	/>
{:else if reloading}
	<p data-testid="versions-loading" class="text-sm text-zinc-400">Loading versions…</p>
{:else if versions.length === 0}
	<p data-testid="versions-empty" class="text-sm text-zinc-400">
		No stored versions. Apply this orkflow from the Edit tab to create one.
	</p>
{:else}
	<div class="mb-4 flex items-start justify-between gap-4">
		<p class="text-sm text-zinc-400" data-testid="versions-summary">
			{versions.length}
			{versions.length === 1 ? 'version' : 'versions'}; runs execute
			<span class="font-medium text-zinc-200" data-testid="versions-resolved">{resolved}</span>
			{#if !currentVersion}
				<span class="text-zinc-500">(nobody has promoted this orkflow, so the newest applies)</span>
			{/if}
		</p>
		<div class="flex flex-col items-end gap-2">
			<div class="flex gap-2">
				<button
					type="button"
					data-testid="versions-reload"
					disabled={reloading}
					onclick={reload}
					class="rounded-md border border-zinc-700 px-3 py-1.5 text-sm text-zinc-300 hover:bg-zinc-800 disabled:opacity-50"
				>
					{reloading ? 'Reloading…' : 'Reload'}
				</button>
				<button
					type="button"
					data-testid="versions-delete"
					disabled={reloading || deleteSafety !== 'safe'}
					title={deleteSafetyReason || 'Permanently delete this orkflow and every version'}
					onclick={askToDelete}
					class="rounded-md border border-red-700 px-3 py-1.5 text-sm text-red-300 hover:bg-red-900/20 disabled:cursor-not-allowed disabled:opacity-50"
				>
					Delete Orkflow
				</button>
			</div>
			{#if deleteSafetyReason}
				<p data-testid="versions-delete-status" class="max-w-md text-right text-xs text-zinc-400">
					{deleteSafetyReason}
				</p>
			{/if}
		</div>
	</div>

	<div class="overflow-hidden rounded-lg border border-zinc-800">
		<table class="w-full text-left text-sm" data-testid="version-list">
			<thead class="bg-zinc-900 text-xs uppercase tracking-wide text-zinc-400">
				<tr>
					<th class="px-4 py-3">Version</th>
					<th class="px-4 py-3">Applied</th>
					<th class="px-4 py-3">Description</th>
					<th class="px-4 py-3 text-center">From</th>
					<th class="px-4 py-3 text-center">To</th>
					<th class="px-4 py-3"></th>
				</tr>
			</thead>
			<tbody class="divide-y divide-zinc-800">
				{#each versions as version (version.version)}
					<tr class="hover:bg-zinc-900/60" data-testid="version-row-{version.version}">
						<td class="px-4 py-3">
							<span class="font-medium text-zinc-100">{version.version}</span>
							{#if runsNow(version)}
								<span
									data-testid="version-current-{version.version}"
									class="ml-2 rounded-full bg-emerald-500/15 px-2 py-0.5 text-xs font-medium text-emerald-300"
									title="runs when no version is named">current</span
								>
							{/if}
							{#if version.version === headVersion}
								<span
									data-testid="version-head-{version.version}"
									class="ml-2 rounded-full bg-sky-500/15 px-2 py-0.5 text-xs font-medium text-sky-300"
									title="the most recently applied version — what an edit is derived from"
									>head</span
								>
							{/if}
						</td>
						<td class="px-4 py-3 text-zinc-400">{fmtTimestamp(version.appliedAt)}</td>
						<td class="max-w-md truncate px-4 py-3 text-zinc-400">{version.description || '—'}</td>
						<td class="px-4 py-3 text-center">
							<input
								type="radio"
								name="diff-from"
								aria-label="diff from {version.version}"
								data-testid="diff-from-{version.version}"
								value={version.version}
								bind:group={diffFrom}
								class="accent-emerald-500"
							/>
						</td>
						<td class="px-4 py-3 text-center">
							<input
								type="radio"
								name="diff-to"
								aria-label="diff to {version.version}"
								data-testid="diff-to-{version.version}"
								value={version.version}
								bind:group={diffTo}
								class="accent-emerald-500"
							/>
						</td>
						<td class="px-4 py-3 text-right">
							<div class="flex items-center justify-end gap-2">
								<a
									href={editHref(version.version)}
									data-testid="version-edit-{version.version}"
									title="edit this version's source and apply it as a new version"
									class="rounded-md border border-zinc-700 px-3 py-1.5 text-xs text-zinc-200 hover:bg-zinc-800"
								>
									Edit
								</a>
								{#if runsNow(version)}
									<span class="text-xs text-zinc-500">running</span>
								{:else}
									<button
										type="button"
										data-testid="promote-{version.version}"
										onclick={() => askToPromote(version.version)}
										class="rounded-md border border-zinc-700 px-3 py-1.5 text-xs text-zinc-200 hover:bg-zinc-800"
									>
										Promote
									</button>
								{/if}
							</div>
						</td>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>

	<div class="mt-6">
		<div class="mb-3 flex items-center gap-3">
			<h3 class="text-sm font-semibold text-zinc-200">Compare two versions</h3>
			<button
				type="button"
				data-testid="diff-run"
				disabled={diffing}
				onclick={runDiff}
				class="rounded-md bg-emerald-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-emerald-500 disabled:opacity-50"
			>
				{diffing ? 'Diffing…' : `Diff ${diffFrom || '…'} → ${diffTo || '…'}`}
			</button>
		</div>

		{#if diffing}
			<p data-testid="diff-loading" class="text-sm text-zinc-400">Asking the engine for the diff…</p>
		{:else if diffError}
			<ErrorPanel
				testid="diff-error"
				retryTestid="diff-retry"
				subject="the diff of {diffFrom} → {diffTo}"
				message={diffError}
				onretry={runDiff}
			/>
		{:else if diffText !== null}
			{#if diffText === ''}
				<p data-testid="diff-identical" class="text-sm text-zinc-400">
					{diffPair}: the two versions' source YAML is byte-identical.
				</p>
			{:else}
				<DiffView diff={diffText} testid="version-diff" />
			{/if}
		{:else}
			<p data-testid="diff-idle" class="text-sm text-zinc-500">
				Pick a From and a To above, then Diff. The comparison is the engine's own, the same text
				<code class="text-zinc-400">orkctl orkflow diff</code> prints.
			</p>
		{/if}
	</div>
{/if}

{#if pendingPromotion}
	<ConfirmDialog
		testid="promote-confirm"
		title="Promote {name} to {pendingPromotion}?"
		acceptLabel="Promote"
		acceptTestid="promote-accept"
		cancelTestid="promote-cancel"
		busy={promoting}
		error={promoteError}
		onaccept={promote}
		oncancel={() => {
			pendingPromotion = '';
			promoteError = '';
		}}
	>
		{#snippet body()}
			<p>
				Runs that name no version currently execute
				<span class="font-medium text-zinc-100">{resolved || 'the newest applied version'}</span>.
				After this they execute
				<span class="font-medium text-emerald-300">{pendingPromotion}</span>, in namespace
				<span class="font-medium text-zinc-100">{namespaceLabel(namespace)}</span>.
			</p>
			<p class="mt-2 text-zinc-400">
				Runs already executing keep the version they pinned. Nothing stored changes — only which
				version an unnamed one resolves to.
			</p>
		{/snippet}
	</ConfirmDialog>
{/if}

{#if pendingDelete}
	<ConfirmDialog
		testid="delete-confirm"
		title="Delete {name}?"
		acceptLabel="Delete"
		acceptTestid="delete-accept"
		cancelTestid="delete-cancel"
		busy={deleting}
		error={deleteError}
		onaccept={deleteOrkflow}
		oncancel={() => {
			pendingDelete = false;
			deleteError = '';
		}}
	>
		{#snippet body()}
			<p class="font-bold text-red-400">
				This action will delete all versions and cannot be reversed.
			</p>
			<p class="mt-3">
				Delete <span class="font-medium text-zinc-100">{name}</span> in namespace
				<span class="font-medium text-zinc-100">{namespaceLabel(namespace)}</span>?
			</p>
			<p class="mt-3 text-sm text-zinc-300">
				The engine will refuse deletion if a run is active or finished within the last 24 hours.
			</p>
		{/snippet}
	</ConfirmDialog>
{/if}
