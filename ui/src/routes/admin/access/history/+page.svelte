<script lang="ts">
	// Access → History (UX35): every revision of the document, who made each
	// and what it changed; a diff of any two; and, for a revision written
	// around the API, its acknowledgement.
	import { invalidateAll } from '$app/navigation';
	import { toJsonString } from '@bufbuild/protobuf';
	import DiffView from '#lib/components/DiffView.svelte';
	import ErrorPanel from '#lib/components/ErrorPanel.svelte';
	import Principal from '#lib/components/Principal.svelte';
	import SheetHeader from '#lib/components/SheetHeader.svelte';
	import TimeAgo from '#lib/components/TimeAgo.svelte';
	import { errorMessage } from '#lib/client.ts';
	import { accessClient } from '#lib/clients/access.ts';
	import { AccessDocumentSchema, type AccessRevision } from '#lib/gen/orkstack/v1/access_pb.ts';
	import { unifiedDiff } from '#lib/lineDiff.ts';
	import { can, missing } from '#lib/permissions.ts';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();
	const canWrite = $derived(can(data.grants, 'access.write'));
	const around = $derived(data.access?.document?.writtenAroundApi ?? false);
	const head = $derived(data.revisions[0]);

	let from = $state<number | null>(null);
	let to = $state<number | null>(null);
	let diff = $state('');
	let diffBusy = $state(false);
	let diffError = $state('');

	async function show() {
		if (from === null || to === null) return;
		diffBusy = true;
		diffError = '';
		try {
			const c = accessClient(fetch);
			const [a, b] = await Promise.all([c.getAccessRevision({ revision: BigInt(from) }), c.getAccessRevision({ revision: BigInt(to) })]);
			const text = (r: typeof a) => (r.document ? toJsonString(AccessDocumentSchema, r.document, { prettySpaces: 2 }) : '');
			diff = unifiedDiff(text(a), text(b), `revision ${from}`, `revision ${to}`);
		} catch (err) {
			diffError = errorMessage(err);
		} finally {
			diffBusy = false;
		}
	}

	let ackBusy = $state(false);
	let ackError = $state('');
	async function acknowledge(r: AccessRevision) {
		ackBusy = true;
		ackError = '';
		try {
			await accessClient(fetch).acknowledgeAccessRevision({ revision: r.revision, sha256: r.sha256 });
			await invalidateAll();
		} catch (err) {
			ackError = errorMessage(err);
		} finally {
			ackBusy = false;
		}
	}
</script>

<svelte:head><title>History · Access · OrkStack</title></svelte:head>

<SheetHeader title="History" testid="history-title" commands={[{ label: 'List the revisions', command: 'orkctl admin access history' }]} />

{#if data.revisionsError}
	<ErrorPanel message={data.revisionsError} subject="the revisions" testid="history-error" onretry={() => invalidateAll()} />
{:else}
	{#if around && head}
		<div class="alert alert-fail mb-4" role="alert" data-testid="history-around">
			<div class="min-w-0 flex-1">
				<p>Revision <span class="tabular-nums">{head.revision}</span> was written around the API. If it is your own change, acknowledge it; the banner then clears on every engine.</p>
				{#if ackError}<p class="text-fail">{ackError}</p>{/if}
			</div>
			{#if canWrite}
				<button type="button" class="btn" onclick={() => acknowledge(head)} disabled={ackBusy} data-testid="history-acknowledge">Acknowledge revision {head.revision}</button>
			{:else}
				<p class="text-sm text-ink-3">{missing('access.write')}</p>
			{/if}
		</div>
	{/if}

	<section class="card mb-5" aria-labelledby="revisions-h">
		<div class="card-head"><h2 id="revisions-h">Revisions <span class="tag">{data.revisions.length}</span></h2></div>
		<div class="overflow-x-auto">
			<table class="register min-w-[760px]" data-testid="history-table">
				<thead><tr><th>Revision</th><th>Written</th><th>By</th><th>Change</th><th>From</th><th>To</th></tr></thead>
				<tbody>
					{#each data.revisions as r (r.revision)}
						{@const n = Number(r.revision)}
						<tr data-testid="revision-{n}">
							<td class="tabular-nums">{n}</td>
							<td><TimeAgo at={r.writtenAt} /></td>
							<td>{#if r.writtenBy}<Principal name={r.writtenBy} verified={r.writtenByVerified} />{:else}<span class="text-ink-3">Not through the API</span>{/if}</td>
							<td>{r.change}</td>
							<td><input type="radio" name="from" value={n} bind:group={from} aria-label="Compare from revision {n}" /></td>
							<td><input type="radio" name="to" value={n} bind:group={to} aria-label="Compare to revision {n}" /></td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
		<div class="flex justify-end p-4"><button type="button" class="btn" onclick={show} disabled={from === null || to === null || diffBusy} data-testid="history-compare">{diffBusy ? 'Reading…' : 'Compare'}</button></div>
	</section>
	{#if diffError}<p class="text-fail" role="alert">{diffError}</p>{/if}
	{#if diff}<DiffView {diff} testid="history-diff" />{/if}
{/if}
