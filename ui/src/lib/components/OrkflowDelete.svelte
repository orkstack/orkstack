<script lang="ts">
	// Delete Orkflow: it deletes the whole Orkflow and every version, so it lives
	// on the Orkflow's header, in More, on every tab. The trigger is neutral with
	// its glyph (ui/DESIGN.md, the No Hue For Destruction Rule). What blocks it is
	// read when the menu first opens, so a page view pays for none of it, and the
	// server repeats the check at deletion time and is the authority that refuses.
	import { goto } from '$app/navigation';
	import { errorMessage } from '#lib/client.ts';
	import { orkflowClient } from '#lib/clients/orkflows.ts';
	import { runClient } from '#lib/clients/runs.ts';
	import { runWindowFilters } from '#lib/browsing.ts';
	import { Status } from '#lib/gen/orkstack/v1/common_pb.ts';
	import { namespaceLabel, withNamespace } from '#lib/namespace.ts';
	import ConfirmDialog from './ConfirmDialog.svelte';
	import Icon from './Icon.svelte';
	import MoreMenu from './MoreMenu.svelte';

	let { name, namespace }: { name: string; namespace: string } = $props();

	const deletionRunWindowMs = 24 * 60 * 60 * 1000;
	type DeleteSafety = 'unread' | 'checking' | 'safe' | 'blocked' | 'error';
	let deleteSafety = $state<DeleteSafety>('unread');
	let deleteSafetyReason = $state('');
	let deleteCheckSequence = 0;
	let pendingDelete = $state(false);
	let deleting = $state(false);
	let deleteError = $state('');
	let typedName = $state('');

	function terminal(status: Status): boolean {
		return status === Status.SUCCEEDED || status === Status.FAILED || status === Status.CANCELED || status === Status.SKIPPED;
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

	// Another Orkflow reached from this page is a new question.
	$effect(() => {
		void name;
		void namespace;
		// A check still in flight was about the last one.
		deleteCheckSequence++;
		deleteSafety = 'unread';
		deleteSafetyReason = '';
	});

	function askToDelete(close: () => void) {
		if (deleteSafety !== 'safe') return;
		close();
		typedName = '';
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
</script>

<MoreMenu testid="orkflow-more" iconOnly bordered label="More about this Orkflow" onopen={() => void checkDeletionSafety()}>
	{#snippet items(close)}
		<button
			type="button"
			data-testid="orkflow-delete"
			disabled={deleteSafety !== 'safe'}
			aria-describedby={deleteSafetyReason ? 'orkflow-delete-status' : undefined}
			data-tip={deleteSafetyReason ? undefined : 'Permanently delete this Orkflow and every version'}
			onclick={() => askToDelete(close)}
			class="gap-2 disabled:cursor-not-allowed disabled:text-ink-3"><Icon name="trash" />Delete Orkflow</button
		>
		{#if deleteSafetyReason}
			<p id="orkflow-delete-status" data-testid="orkflow-delete-status" class="max-w-xs px-3 pt-1 pb-2 text-xs text-ink-3">{deleteSafetyReason}</p>
		{/if}
	{/snippet}
</MoreMenu>

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
		acceptDisabled={typedName !== name}
		acceptDisabledReason="Type the Orkflow's name to delete it"
		glyph="trash"
		onaccept={deleteOrkflow}
		oncancel={() => {
			pendingDelete = false;
			deleteError = '';
		}}
	>
		{#snippet body()}
			<p class="severity-error font-semibold">
				This action will delete all versions and cannot be reversed.
			</p>
			<p class="mt-3">
				Delete <span class="font-medium text-ink">{name}</span> in namespace
				<span class="font-medium text-ink">{namespaceLabel(namespace)}</span>?
			</p>
			<p class="mt-3 text-sm text-ink-secondary">
				The engine will refuse deletion if a run is active or finished within the last 24 hours.
			</p>
			<label class="mt-3 flex flex-col gap-1.5 text-sm">
				<span class="field-label mb-0">Type <span class="font-mono">{name}</span> to confirm</span>
				<input data-testid="delete-confirm-name" bind:value={typedName} autocomplete="off" spellcheck="false" class="field h-9 px-3 font-mono" />
			</label>
		{/snippet}
	</ConfirmDialog>
{/if}
