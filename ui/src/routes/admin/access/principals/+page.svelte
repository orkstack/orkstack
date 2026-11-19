<script lang="ts">
	// Access → People (UX35): the principals this deployment knows of here —
	// seen to sign in over the window chosen, signed in now, disabled, or named
	// by a binding — with when they last signed in, their sign-ins and the
	// bindings that name them; disable, enable, and end their sign-ins.
	import { invalidateAll } from '$app/navigation';
	import ConfirmDialog from '#lib/components/ConfirmDialog.svelte';
	import ErrorPanel from '#lib/components/ErrorPanel.svelte';
	import Icon from '#lib/components/Icon.svelte';
	import SheetHeader from '#lib/components/SheetHeader.svelte';
	import AccessTabs from '../AccessTabs.svelte';
	import TimeAgo from '#lib/components/TimeAgo.svelte';
	import { errorMessage } from '#lib/client.ts';
	import { accessClient } from '#lib/clients/access.ts';
	import {
		PEOPLE_WINDOW_LABEL,
		PEOPLE_WINDOW_ORDER,
		PEOPLE_WINDOW_PHRASE,
		people,
		peopleCommand,
		steppedDownSentence,
		type PeopleWindow,
		type PersonRow
	} from '#lib/accessPeople.ts';
	import { conflictWords, isConflict, isMe } from '#lib/accessSelf.ts';
	import { can, missing } from '#lib/permissions.ts';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();
	const doc = $derived(data.access?.document);
	const read = $derived(data.seenRead);
	const rows = $derived(doc ? people(doc, data.signins, read.seen) : []);
	// The window shown, or the one asked for when none could be read.
	const span = $derived(read.shown ?? read.asked);
	const stepped = $derived(steppedDownSentence(read));
	// In asserted mode nothing records who signed in: no window was read.
	const reads = $derived(data.mode !== 'asserted');
	const windowHref = (w: PeopleWindow) => `?window=${w}`;
	// What the command line lists when even the shortest window is too large for the page: the last hour.
	const shortestCommand = peopleCommand(1);
	const canWrite = $derived(can(data.grants, 'access.write'));
	let q = $state('');
	const shown = $derived(
		rows.filter((r) => {
			const needle = q.trim().toLowerCase();
			return !needle || r.id.toLowerCase().includes(needle) || r.display.toLowerCase().includes(needle);
		})
	);

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

<SheetHeader title="People" testid="people-title" commands={reads ? [{ label: 'The principals seen to sign in', command: peopleCommand(span) }] : []} />
<AccessTabs />

<p class="mb-4 max-w-prose text-sm text-ink-2">
	{reads ? 'Who signed in over the window below, who' : 'Who'} is signed in now, who is disabled, and who a binding names. A principal who reached the API
	with a token only, and is in no binding by name, is in the audit trail rather than here.
</p>

{#if data.mode !== 'asserted'}
	<div class="mb-4 flex flex-wrap items-center gap-3" data-testid="people-window">
		<span class="caps text-ink-muted" aria-hidden="true">Signed in over</span>
		<div role="group" aria-label="Signed in over" class="inline-flex h-8 rounded-sm border border-rule-strong bg-paper">
			{#each PEOPLE_WINDOW_ORDER as w (w)}
				<a
					data-testid="people-window-{w}"
					data-sveltekit-noscroll
					aria-current={span === w ? 'page' : undefined}
					href={windowHref(w)}
					class="figure inline-flex items-center border-r border-rule-strong px-3 text-sm text-ink-secondary transition-colors last:border-r-0 hover:bg-hover hover:text-ink current:bg-blue-tint current:font-semibold current:text-blue-strong"
					>{PEOPLE_WINDOW_LABEL[w]}</a
				>
			{/each}
		</div>
	</div>

	{#if stepped}
		<!-- The window asked for was too large to read: a shorter one is shown, and said. -->
		<div class="alert mb-4" role="status" data-testid="people-seen-stepped">
			<span class="alert-ic"><Icon name="clock" /></span>
			<p class="min-w-0 flex-1 text-sm">{stepped}</p>
		</div>
	{/if}
	{#if read.error}
		<!-- The seen list failed on its own: everyone else is still listed below. -->
		<div class="alert alert-fail mb-4" role="alert" data-testid="people-seen-error">
			<span class="alert-ic"><Icon name="alert" /></span>
			<p class="min-w-0 flex-1 text-sm">
				{#if read.tooLong}
					Even {PEOPLE_WINDOW_PHRASE['1d']} examines more audit records than one request may, so who signed in is not listed. The command line
					lists a shorter window: <code class="font-mono" data-testid="people-seen-command">{shortestCommand}</code>.
				{:else}
					Who signed in could not be read: {read.error}
				{/if}
			</p>
		</div>
	{:else if read.truncated}
		<p class="mb-4 text-sm text-ink-3" data-testid="people-seen-truncated">
			More people signed in over {PEOPLE_WINDOW_PHRASE[span]} than this page lists; the command line lists them all:
			<code class="font-mono">{peopleCommand(span)}</code>.
		</p>
	{/if}
{/if}

{#if data.accessError || data.signinsError}
	<ErrorPanel message={data.accessError ?? data.signinsError ?? ''} subject="the people of this deployment" testid="people-error" onretry={() => invalidateAll()} />
{:else}
	<section class="card" aria-labelledby="people-h">
		<div class="card-head">
			<h2 id="people-h">People <span class="tag">{shown.length}</span></h2>
			<div class="card-end">
				<label class="sr-only" for="people-q">Find a principal</label>
				<input id="people-q" class="search field" type="search" placeholder="Principal or name" bind:value={q} data-testid="people-filter" />
			</div>
		</div>
		<div class="overflow-x-auto">
			<table class="register register-stacked wide-reg" data-testid="people-table">
				<thead class="max-sm:sr-only"><tr><th>Principal</th><th>Last signed in</th><th>Sign-ins</th><th>Bound by</th><th><span class="sr-only">Actions</span></th></tr></thead>
				<tbody>
					{#each shown as r (r.id)}
						<tr data-testid="person-{r.id}">
							<td class="max-sm:col-span-2">
								{#if r.display}<span class="block text-sm text-ink" data-testid="person-display-{r.id}">{r.display}</span>{/if}
								<span class="font-mono">{r.id}</span>
								{#if isMe(data.me, r.id)}<span class="tag ml-2">You</span>{/if}
								{#if r.disabled}<span class="stamp stamp-sm pill-outline ml-2" data-testid="person-disabled-{r.id}">Disabled</span>{/if}
							</td>
							<td class="text-sm" data-label="Last signed in" data-testid="person-seen-{r.id}">
								{#if r.lastSeen}<TimeAgo at={r.lastSeen} testid="person-seen-at-{r.id}" />{:else if !reads}<span class="text-ink-3">Not recorded</span>{:else if read.error}<span class="text-ink-3">Not read</span>{:else if read.truncated}<span class="text-ink-3">Not among those listed</span>{:else}<span class="text-ink-3">Not in {PEOPLE_WINDOW_PHRASE[span]}</span>{/if}
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
						<tr><td colspan="5" class="text-ink-3">{q.trim() ? 'No principal matches.' : !reads ? 'No one is signed in now, disabled or named by a binding.' : read.error ? 'No one is signed in now, disabled or named by a binding; who signed in before could not be read.' : `No one signed in over ${PEOPLE_WINDOW_PHRASE[span]}, is disabled or is named by a binding.`}</td></tr>
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
	/* From 640 px it is a table, scrolling sideways in its frame below 880 px. */
	@media (min-width: 640px) {
		.wide-reg {
			min-width: 880px;
		}
	}
</style>
