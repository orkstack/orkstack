<script lang="ts">
	// Access → People (UX35): the principals this deployment knows of here —
	// signed in now, disabled, or named by a binding — with their sign-ins and
	// the bindings that name them; disable, enable, and end their sign-ins.
	import { invalidateAll } from '$app/navigation';
	import ConfirmDialog from '#lib/components/ConfirmDialog.svelte';
	import ErrorPanel from '#lib/components/ErrorPanel.svelte';
	import Icon from '#lib/components/Icon.svelte';
	import SheetHeader from '#lib/components/SheetHeader.svelte';
	import AccessTabs from '../AccessTabs.svelte';
	import TimeAgo from '#lib/components/TimeAgo.svelte';
	import { errorMessage } from '#lib/client.ts';
	import { accessClient } from '#lib/clients/access.ts';
	import { people, type PersonRow } from '#lib/accessPeople.ts';
	import { conflictWords, isConflict, isMe } from '#lib/accessSelf.ts';
	import { can, missing } from '#lib/permissions.ts';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();
	const doc = $derived(data.access?.document);
	const rows = $derived(doc ? people(doc, data.signins) : []);
	const canWrite = $derived(can(data.grants, 'access.write'));
	let q = $state('');
	const shown = $derived(rows.filter((r) => !q.trim() || r.id.toLowerCase().includes(q.trim().toLowerCase())));

	let acting = $state<{ kind: 'disable' | 'enable' | 'revoke'; row: PersonRow } | null>(null);
	let busy = $state(false);
	let error = $state('');
	async function act() {
		if (!acting || !doc) return;
		busy = true;
		error = '';
		try {
			const id = acting.row.id;
			if (acting.kind === 'revoke') await accessClient(fetch).revokeSignins({ principalId: id });
			else await accessClient(fetch).setPrincipalDisabled({ principalId: id, disabled: acting.kind === 'disable', etag: doc.etag });
			acting = null;
			await invalidateAll();
		} catch (err) {
			error = isConflict(err) ? conflictWords : errorMessage(err);
		} finally {
			busy = false;
		}
	}
</script>

<svelte:head><title>People · Access · OrkStack</title></svelte:head>

<SheetHeader title="People" testid="people-title" commands={[{ label: 'List the sign-ins', command: 'orkctl admin principal signins list' }]} />
<AccessTabs />

<p class="mb-4 max-w-prose text-sm text-ink-2">
	Who is signed in now, who is disabled, and who a binding names. A principal who reached the API with a token only, and is in no
	binding by name, is in the audit trail rather than here.
</p>

{#if data.accessError || data.signinsError}
	<ErrorPanel message={data.accessError ?? data.signinsError ?? ''} subject="the people of this deployment" testid="people-error" onretry={() => invalidateAll()} />
{:else}
	<section class="card" aria-labelledby="people-h">
		<div class="card-head">
			<h2 id="people-h">People <span class="tag">{shown.length}</span></h2>
			<div class="card-end">
				<label class="sr-only" for="people-q">Find a principal</label>
				<input id="people-q" class="search field" type="search" placeholder="Principal" bind:value={q} data-testid="people-filter" />
			</div>
		</div>
		<div class="overflow-x-auto">
			<table class="register register-stacked wide-reg" data-testid="people-table">
				<thead class="max-sm:sr-only"><tr><th>Principal</th><th>Sign-ins</th><th>Bound by</th><th><span class="sr-only">Actions</span></th></tr></thead>
				<tbody>
					{#each shown as r (r.id)}
						<tr data-testid="person-{r.id}">
							<td class="max-sm:col-span-2">
								<span class="font-mono">{r.id}</span>
								{#if isMe(data.me, r.id)}<span class="tag ml-2">You</span>{/if}
								{#if r.disabled}<span class="stamp stamp-sm pill-outline ml-2" data-testid="person-disabled-{r.id}">Disabled</span>{/if}
							</td>
							<td data-label="Sign-ins">
								{#if r.signins.length === 0}<span class="text-ink-3">None</span>{:else}
									<ul>{#each r.signins as s (s.publicId)}<li class="text-sm">Since <TimeAgo at={s.createdAt} />, until <TimeAgo at={s.expiresAt} clock /></li>{/each}</ul>
								{/if}
							</td>
							<td class="text-sm" data-label="Bound by">
								{#if r.bindings.length === 0}<span class="text-ink-3">By name, none</span>{:else}
									{#each r.bindings as b, i (b.name)}{i > 0 ? ', ' : ''}{b.role} at <span class="font-mono">{b.scope}</span>{/each}
								{/if}
							</td>
							<td class="row-acts text-right whitespace-nowrap max-sm:col-span-2">
								{#if canWrite}
									{#if r.signins.length > 0}<button type="button" class="btn btn-sm" onclick={() => { acting = { kind: 'revoke', row: r }; error = ''; }} data-testid="person-revoke-{r.id}">End sign-ins</button>{/if}
									{#if r.disabled}
										<button type="button" class="btn btn-sm" onclick={() => { acting = { kind: 'enable', row: r }; error = ''; }} data-testid="person-enable-{r.id}">Enable</button>
									{:else}
										<button type="button" class="btn btn-sm" onclick={() => { acting = { kind: 'disable', row: r }; error = ''; }} data-testid="person-disable-{r.id}"><Icon name="ban" />Disable</button>
									{/if}
								{:else}
									<span class="text-sm text-ink-3">{missing('access.write')}</span>
								{/if}
							</td>
						</tr>
					{:else}
						<tr><td colspan="4" class="text-ink-3">{q.trim() ? 'No principal matches.' : 'No one is signed in, disabled or named by a binding.'}</td></tr>
					{/each}
				</tbody>
			</table>
		</div>
	</section>
{/if}

{#if acting}
	<ConfirmDialog
		title={acting.kind === 'disable' ? `Disable ${acting.row.id}?` : acting.kind === 'enable' ? `Enable ${acting.row.id}?` : `End the sign-ins of ${acting.row.id}?`}
		testid="person-dialog"
		acceptLabel={acting.kind === 'disable' ? 'Disable' : acting.kind === 'enable' ? 'Enable' : 'End sign-ins'}
		cancelLabel="Leave it"
		acceptTestid="person-confirm"
		cancelTestid="person-cancel"
		destructive={acting.kind !== 'enable'}
		glyph={acting.kind === 'enable' ? 'apply' : 'ban'}
		{busy}
		{error}
		onaccept={act}
		oncancel={() => (acting = null)}
	>
		{#snippet body()}
			{#if acting!.kind !== 'enable' && isMe(data.me, acting!.row.id)}
				<p class="mb-3 font-semibold" data-testid="person-self">This is you. {acting!.kind === 'disable' ? 'You are signed out and refused until another administrator enables you.' : 'This browser is signed out too.'}</p>
			{/if}
			{#if acting!.kind === 'disable'}
				<p>Every request it makes is refused, whatever it presents, from each engine’s next refresh of the document; its sign-ins end at their next check.</p>
			{:else if acting!.kind === 'enable'}
				<p>It is decided by its bindings again from each engine’s next refresh of the document.</p>
			{:else}
				<p>Its browsers are signed out: at once on this engine, within 15 seconds on every other. Its identity provider can still sign it in again; disable it to stop that.</p>
			{/if}
		{/snippet}
	</ConfirmDialog>
{/if}

<style>
	/* Below 640 px the register stacks: a row's actions take its full width, from the left. */
	@media (max-width: 639.98px) {
		.row-acts {
			text-align: left;
		}
	}
	/* From 640 px it is a table, scrolling sideways in its frame below 760 px. */
	@media (min-width: 640px) {
		.wide-reg {
			min-width: 760px;
		}
	}
</style>
