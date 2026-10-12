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
	import CliEquivalent from '$lib/components/CliEquivalent.svelte';
	import ConfirmDialog from '$lib/components/ConfirmDialog.svelte';
	import DiffView from '$lib/components/DiffView.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import InterfaceFindings from '$lib/components/InterfaceFindings.svelte';
	import { errorMessage } from '$lib/client';
	import { orkflowClient } from '$lib/clients/orkflows';
	import { runClient } from '$lib/clients/runs';
	import {
		breakingCount,
		callerLabel,
		changeWord,
		compatCommand,
		composers,
		countNoun,
		followers,
		notCompatible,
		promoteBlocked,
		promoteCommand,
		unavailableSchedules,
		verdictChipClass,
		verdictWord
	} from '$lib/compat';
	import { Status } from '$lib/gen/orkstack/v1/common_pb';
	import { runWindowFilters } from '$lib/browsing';
	import {
		InterfaceVerdict,
		OrkflowCallerKind,
		type CompareInterfacesResponse,
		type OrkflowCaller,
		type OrkflowVersion
} from '$lib/gen/orkstack/v1/orkflow_service_pb';
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

	// The dialog's interface comparison and callers are INFORMATION: the gate
	// is the engine's, and it compares again when the promotion is sent. A
	// comparison that could not be read therefore never blocks the button; a
	// BREAKING one does until it is acknowledged (docs/UI_CLI_PARITY.md
	// "Interface compatibility").
	let promoteComparing = $state(false);
	let promoteCompare = $state<CompareInterfacesResponse | null>(null);
	let promoteCompareError = $state('');
	let promoteCallers = $state<OrkflowCaller[] | null>(null);
	let promoteCallersError = $state('');
	let acknowledged = $state(false);
	let promoteSequence = 0;

	async function checkPromotion(version: string) {
		const sequence = ++promoteSequence;
		promoteComparing = true;
		promoteCompare = null;
		promoteCompareError = '';
		promoteCallers = null;
		promoteCallersError = '';
		const client = orkflowClient(fetch);
		const comparison = client
			.compareInterfaces({ name, namespace, fromVersion: '', toVersion: version })
			.then(
				(res) => {
					if (sequence === promoteSequence) promoteCompare = res;
				},
				(err) => {
					if (sequence === promoteSequence) promoteCompareError = errorMessage(err);
				}
			)
			.finally(() => {
				if (sequence === promoteSequence) promoteComparing = false;
			});
		const callers = client.listOrkflowCallers({ name, namespace }).then(
			(res) => {
				if (sequence === promoteSequence) promoteCallers = res.callers;
			},
			(err) => {
				if (sequence === promoteSequence) promoteCallersError = errorMessage(err);
			}
		);
		await Promise.all([comparison, callers]);
	}

	function askToPromote(version: string) {
		pendingPromotion = version;
		promoteError = '';
		acknowledged = false;
		void checkPromotion(version);
	}

	const promoteVerdict = $derived(promoteCompare?.verdict);
	const promoteIsBreaking = $derived(promoteVerdict === InterfaceVerdict.BREAKING);

	async function promote() {
		promoting = true;
		promoteError = '';
		try {
			await orkflowClient(fetch).setCurrentOrkflowVersion({
				name,
				version: pendingPromotion,
				namespace,
				acknowledgeBreaking: acknowledged
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
				runClient(fetch).listRuns({ namespace, filters: runWindowFilters(name, 'RUNNING', '', ''), pageSize: 1 }),
				runClient(fetch).listRuns({ namespace, filters: runWindowFilters(name, 'PENDING', '', ''), pageSize: 1 }),
				runClient(fetch).listRuns({ namespace, filters: runWindowFilters(name, '', '', ''), pageSize: 200 })
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
			await orkflowClient(fetch).deleteOrkflow({ name, namespace });
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

	// ── compare interfaces ─────────────────────────────────────────────────
	// Independent of the Diff panel: neither clears the other.
	let compatResult = $state<CompareInterfacesResponse | null>(null);
	let comparing = $state(false);
	let compatError = $state('');

	async function runCompat() {
		if (!diffFrom || !diffTo) {
			compatError = 'Choose the two versions to compare.';
			return;
		}
		comparing = true;
		compatError = '';
		compatResult = null;
		try {
			compatResult = await orkflowClient(fetch).compareInterfaces({
				name,
				namespace,
				fromVersion: diffFrom,
				toVersion: diffTo
			});
		} catch (err) {
			compatError = errorMessage(err);
		} finally {
			comparing = false;
		}
	}

	async function runDiff() {
		if (!diffFrom || !diffTo) {
			diffError = 'Choose the two versions to compare.';
			return;
		}
		diffing = true;
		diffError = '';
		diffText = null;
		try {
			const res = await orkflowClient(fetch).diffOrkflowVersions({
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
	<p data-testid="versions-loading" class="text-sm text-fg-muted">Loading versions…</p>
{:else if versions.length === 0}
	<p data-testid="versions-empty" class="text-sm text-fg-muted">
		No stored versions. Apply this orkflow from the Edit tab to create one.
	</p>
{:else}
	<div class="mb-4 flex items-start justify-between gap-4">
		<p class="text-sm text-fg-secondary" data-testid="versions-summary">
			{versions.length}
			{versions.length === 1 ? 'version' : 'versions'}; runs execute
			<span class="font-medium text-fg" data-testid="versions-resolved">{resolved}</span>
			{#if !currentVersion}
				<span class="text-fg-muted">(nobody has promoted this orkflow, so the newest applies)</span>
			{/if}
		</p>
		<div class="flex flex-col items-end gap-2">
			<div class="flex gap-2">
				<button
					type="button"
					data-testid="versions-reload"
					disabled={reloading}
					onclick={reload}
					class="inline-flex h-9 items-center justify-center gap-1.5 rounded-md border border-border bg-surface px-3.5 text-sm font-medium text-fg shadow-xs transition-colors duration-150 hover:border-border-strong hover:bg-surface-hover disabled:pointer-events-none disabled:opacity-50"
				>
					{reloading ? 'Reloading…' : 'Reload'}
				</button>
				<button
					type="button"
					data-testid="versions-delete"
					disabled={reloading || deleteSafety !== 'safe'}
					title={deleteSafetyReason || 'Permanently delete this orkflow and every version'}
					onclick={askToDelete}
					class="inline-flex h-9 items-center justify-center gap-1.5 rounded-md border border-danger-200 bg-surface px-3.5 text-sm font-medium text-danger-700 shadow-xs transition-colors duration-150 hover:border-danger-300 hover:bg-danger-50 disabled:pointer-events-none disabled:cursor-not-allowed disabled:opacity-50"
				>
					Delete Orkflow
				</button>
			</div>
			{#if deleteSafetyReason}
				<p data-testid="versions-delete-status" class="max-w-md text-right text-xs text-fg-muted">
					{deleteSafetyReason}
				</p>
			{/if}
		</div>
	</div>

	<div class="overflow-x-auto rounded-lg border border-border bg-surface shadow-xs">
		<table class="w-full min-w-[640px] border-collapse text-left text-sm" data-testid="version-list">
			<thead class="bg-surface-sunken">
				<tr>
					<th class="h-10 px-4 pl-5 text-xs font-medium text-fg-muted">Version</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">Applied</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">Description</th>
					<th class="h-10 px-4 text-center text-xs font-medium text-fg-muted">From</th>
					<th class="h-10 px-4 text-center text-xs font-medium text-fg-muted">To</th>
					<th class="h-10 px-4 pr-5 text-xs font-medium text-fg-muted"></th>
				</tr>
			</thead>
			<tbody>
				{#each versions as version (version.version)}
					<tr
						class="border-t border-border-subtle transition-colors duration-150 hover:bg-surface-hover"
						data-testid="version-row-{version.version}"
					>
						<td class="h-11 px-4 pl-5 align-middle">
							<span class="font-medium text-fg">{version.version}</span>
							{#if runsNow(version)}
								<span
									data-testid="version-current-{version.version}"
									class="ml-2 inline-flex items-center rounded-full border border-succeeded-border bg-succeeded-bg px-2 py-0.5 text-[11px] font-medium text-succeeded-fg"
									title="runs when no version is named">current</span
								>
							{/if}
							{#if version.version === headVersion}
								<span
									data-testid="version-head-{version.version}"
									class="ml-2 inline-flex items-center rounded-full border border-info-200 bg-info-50 px-2 py-0.5 text-[11px] font-medium text-info-700"
									title="the most recently applied version — what an edit is derived from"
									>head</span
								>
							{/if}
							{#if version.interfaceVerdict !== InterfaceVerdict.UNSPECIFIED}
								<span
									data-testid="version-interface-{version.version}"
									class="ml-2 {verdictChipClass(version.interfaceVerdict)}"
									title="declared interface against {resolved}, the version runs execute now"
									>{verdictWord(version.interfaceVerdict)}</span
								>
							{/if}
						</td>
						<td class="h-11 px-4 align-middle tabular-nums text-fg-muted"
							>{fmtTimestamp(version.appliedAt)}</td
						>
						<td class="h-11 max-w-md truncate px-4 align-middle text-fg-secondary"
							>{version.description || '—'}</td
						>
						<td class="h-11 px-4 text-center align-middle">
							<input
								type="radio"
								name="diff-from"
								aria-label="diff from {version.version}"
								data-testid="diff-from-{version.version}"
								value={version.version}
								bind:group={diffFrom}
								class="accent-brand-600"
							/>
						</td>
						<td class="h-11 px-4 text-center align-middle">
							<input
								type="radio"
								name="diff-to"
								aria-label="diff to {version.version}"
								data-testid="diff-to-{version.version}"
								value={version.version}
								bind:group={diffTo}
								class="accent-brand-600"
							/>
						</td>
						<td class="h-11 px-4 pr-5 align-middle">
							<div class="flex items-center justify-end gap-2">
								<a
									href={editHref(version.version)}
									data-testid="version-edit-{version.version}"
									title="edit this version's source and apply it as a new version"
									class="inline-flex h-8 items-center rounded-md border border-border bg-surface px-3 text-xs font-medium text-fg shadow-xs transition-colors duration-150 hover:border-border-strong hover:bg-surface-hover"
								>
									Edit
								</a>
								{#if runsNow(version)}
									<span class="text-xs text-fg-muted">running</span>
								{:else}
									<button
										type="button"
										data-testid="promote-{version.version}"
										onclick={() => askToPromote(version.version)}
										class="inline-flex h-8 items-center rounded-md border border-border bg-surface px-3 text-xs font-medium text-fg shadow-xs transition-colors duration-150 hover:border-border-strong hover:bg-surface-hover"
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
			<h3 class="text-sm font-semibold text-fg">Compare two versions</h3>
			<button
				type="button"
				data-testid="diff-run"
				disabled={diffing}
				onclick={runDiff}
				class="inline-flex h-8 items-center justify-center gap-1.5 rounded-md bg-brand-600 px-3 text-xs font-medium text-white shadow-xs transition-colors duration-150 hover:bg-brand-700 active:bg-brand-800 disabled:pointer-events-none disabled:opacity-50"
			>
				{diffing ? 'Diffing…' : `Diff ${diffFrom || '…'} → ${diffTo || '…'}`}
			</button>
			<button
				type="button"
				data-testid="compat-run"
				disabled={comparing}
				onclick={runCompat}
				class="inline-flex h-8 items-center justify-center gap-1.5 rounded-md border border-border bg-surface px-3 text-xs font-medium text-fg shadow-xs transition-colors duration-150 hover:border-border-strong hover:bg-surface-hover disabled:pointer-events-none disabled:opacity-50"
			>
				{comparing ? 'Comparing…' : `Compare interfaces ${diffFrom || '…'} → ${diffTo || '…'}`}
			</button>
			<CliEquivalent
				command={compatCommand(namespace, name, diffFrom, diffTo)}
				testid="compat-cli"
				label="Compare the interfaces of {diffFrom} and {diffTo} from the CLI"
			/>
		</div>

		{#if comparing}
			<p data-testid="compat-loading" class="mb-4 text-sm text-fg-muted">
				Asking the engine to compare the declared interfaces…
			</p>
		{:else if compatError}
			<div class="mb-4">
				<ErrorPanel
					testid="compat-error"
					retryTestid="compat-retry"
					subject="the interface comparison of {diffFrom} → {diffTo}"
					message={compatError}
					onretry={runCompat}
				/>
			</div>
		{:else if compatResult}
			<div class="mb-4">
				<InterfaceFindings {name} response={compatResult} />
			</div>
		{/if}

		{#if diffing}
			<p data-testid="diff-loading" class="text-sm text-fg-muted">Asking the engine for the diff…</p>
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
				<p data-testid="diff-identical" class="text-sm text-fg-muted">
					{diffPair}: the two versions' source YAML is byte-identical.
				</p>
			{:else}
				<DiffView diff={diffText} testid="version-diff" />
			{/if}
		{:else}
			<p data-testid="diff-idle" class="text-sm text-fg-muted">
				Pick a From and a To above, then Diff. The comparison is the engine's own, the same text
				<code>orkctl orkflow diff</code> prints.
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
		acceptDisabled={promoteBlocked({
			comparing: promoteComparing,
			verdict: promoteVerdict,
			acknowledged
		})}
		onaccept={promote}
		oncancel={() => {
			pendingPromotion = '';
			promoteError = '';
			acknowledged = false;
			promoteSequence++;
		}}
	>
		{#snippet body()}
			<p>
				Runs that name no version currently execute
				<span class="font-medium text-fg">{resolved || 'the newest applied version'}</span>.
				After this they execute
				<span class="font-medium text-brand-700">{pendingPromotion}</span>, in namespace
				<span class="font-medium text-fg">{namespaceLabel(namespace)}</span>.
			</p>
			<p class="mt-2 text-fg-muted">
				Runs already executing keep the version they pinned. Nothing stored changes — only which
				version an unnamed one resolves to.
			</p>

			<div class="mt-3 rounded-md border border-border-subtle bg-surface-sunken px-3 py-2">
				{#if promoteComparing}
					<p data-testid="promote-interface-loading" class="text-fg-muted">
						Comparing the declared interface with {resolved || 'the version that runs now'}…
					</p>
				{:else if promoteCompareError}
					<p data-testid="promote-interface-error" class="text-fg-secondary">
						The interface comparison could not be read ({promoteCompareError}). The engine still checks
						it when you promote.
					</p>
				{:else if promoteCompare}
					<p
						data-testid="promote-interface-verdict"
						class={promoteIsBreaking ? 'font-medium text-danger-700' : 'text-fg-secondary'}
					>
						Declared interface against {promoteCompare.fromVersion}:
						{#if promoteIsBreaking}
							breaking — {countNoun(breakingCount(promoteCompare.findings), 'breaking change', 'breaking changes')}
						{:else if promoteVerdict === InterfaceVerdict.UNKNOWN}
							unknown — {notCompatible(promoteCompare.findings)[0]?.message ?? ''}
						{:else}
							compatible
						{/if}
					</p>
					{#if notCompatible(promoteCompare.findings).length > 0}
						<ul data-testid="promote-interface-findings" class="mt-2 space-y-1">
							{#each notCompatible(promoteCompare.findings) as finding, index (index)}
								<li
									data-testid="promote-finding-{finding.field}-{changeWord(finding.change)}"
									class="text-xs"
								>
									<span class="font-mono text-fg">{finding.field}</span>:
									<span class="text-fg-secondary">{finding.message}</span>
								</li>
							{/each}
						</ul>
					{/if}
				{/if}
			</div>

			{#if promoteCallersError}
				<p data-testid="promote-callers-error" class="mt-2 text-xs text-fg-muted">
					What starts or composes {name} could not be read ({promoteCallersError}); the promotion is
					still possible.
				</p>
			{:else if promoteCallers}
				<div data-testid="promote-followers" class="mt-2 text-xs text-fg-secondary">
					{#if followers(promoteCallers).length > 0}
						<p>
							Also moves:
							{followers(promoteCallers)
								.map(
									(c) =>
										`${c.kind === OrkflowCallerKind.TRIGGER ? 'trigger' : 'schedule'} ${callerLabel(c)}`
								)
								.join(', ')} — they name no version.
						</p>
					{:else}
						<p>No trigger or schedule follows the current version; runs submitted without a version do.</p>
					{/if}
					{#each unavailableSchedules(promoteCallers) as c, index (index)}
						<p>schedule {c.name}: version unavailable ({c.unavailableReason})</p>
					{/each}
				</div>
				{#if composers(promoteCallers).length > 0}
					<p data-testid="promote-pinned" class="mt-1 text-xs text-fg-muted">
						{composers(promoteCallers).length} composing Orkflow {composers(promoteCallers).length === 1
							? 'version pins'
							: 'versions pin'} a version of {name} and {composers(promoteCallers).length === 1
							? 'is'
							: 'are'} not moved by this promotion.
					</p>
				{/if}
			{/if}

			{#if promoteIsBreaking || promoteCompareError}
				<label class="mt-3 flex items-start gap-2 text-sm text-fg">
					<input
						type="checkbox"
						data-testid="promote-acknowledge"
						bind:checked={acknowledged}
						class="mt-0.5 accent-brand-600"
					/>
					<span>I have read the breaking changes; promote anyway</span>
				</label>
			{/if}

			<div class="mt-3 flex justify-end">
				<CliEquivalent
					command={promoteCommand(namespace, name, pendingPromotion, acknowledged)}
					testid="promote-cli"
					label="Promote {pendingPromotion} from the CLI"
				/>
			</div>
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
		destructive
		onaccept={deleteOrkflow}
		oncancel={() => {
			pendingDelete = false;
			deleteError = '';
		}}
	>
		{#snippet body()}
			<p class="font-semibold text-danger-700">
				This action will delete all versions and cannot be reversed.
			</p>
			<p class="mt-3">
				Delete <span class="font-medium text-fg">{name}</span> in namespace
				<span class="font-medium text-fg">{namespaceLabel(namespace)}</span>?
			</p>
			<p class="mt-3 text-sm text-fg-secondary">
				The engine will refuse deletion if a run is active or finished within the last 24 hours.
			</p>
		{/snippet}
	</ConfirmDialog>
{/if}
