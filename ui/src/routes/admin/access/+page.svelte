<script lang="ts">
	// Access → Bindings (UX35): every binding, filterable by scope, role and
	// subject; create, edit, delete and pin again. Configuration's bindings
	// are shown read only. Banners say what makes the document's decisions
	// less than they look: a stale copy, a peer engine that does not enforce,
	// a mode record that went missing, Orkers whose storage credential reaches
	// the control plane.
	import { invalidateAll } from '$app/navigation';
	import ConfirmDialog from '#lib/components/ConfirmDialog.svelte';
	import Dialog from '#lib/components/Dialog.svelte';
	import ErrorPanel from '#lib/components/ErrorPanel.svelte';
	import Icon from '#lib/components/Icon.svelte';
	import SheetHeader from '#lib/components/SheetHeader.svelte';
	import AccessTabs from './AccessTabs.svelte';
	import TimeAgo from '#lib/components/TimeAgo.svelte';
	import { errorMessage } from '#lib/client.ts';
	import { accessClient } from '#lib/clients/access.ts';
	import type { Binding } from '#lib/gen/orkstack/v1/access_pb.ts';
	import { bindingEdit, keep, pinWords, savedBinding, sharedCredentialMark, type BindingEdit, type BindingFilter } from '#lib/accessBindings.ts';
	import { conflictWords, grantsMe, isConflict } from '#lib/accessSelf.ts';
	import { can, canAnywhere } from '#lib/permissions.ts';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();
	const doc = $derived(data.access?.document);
	const status = $derived(data.access?.status);
	const canWrite = $derived(can(data.grants, 'access.write'));
	// A holder of access.bind at a namespace creates and deletes bindings
	// there (docs/ACCESS.md, "Changing it"); the server decides each one.
	const canBind = $derived(canWrite || canAnywhere(data.grants, 'access.bind'));
	const writeReason = 'You need access.write on this deployment, or access.bind on a namespace.';
	const roles = $derived([...(doc?.builtInRoles ?? []), ...(doc?.roles ?? [])].map((r) => r.name));

	let filter = $state<BindingFilter>({ text: '', role: '', scopeKind: '' });
	const rows = $derived([...(doc?.configuredBindings ?? []), ...(doc?.bindings ?? [])].filter((b) => keep(b, filter)));
	const notEnforcing = $derived((status?.engines ?? []).filter((e) => e.mode !== 'enforce' || e.settingsDiffer.length > 0));

	// ── the edit dialog ─────────────────────────────────────────────────
	let editing = $state<({ creating: boolean } & BindingEdit) | null>(null);
	// The binding the dialog opened on, as it was stored.
	let editingFrom = $state.raw<Binding | null>(null);
	// The binding a save replaces: after a conflict the document was read again, and holds it as it is now.
	const replaced = $derived.by(() => {
		const name = editing?.name;
		return editing && !editing.creating ? ((doc?.bindings ?? []).find((b) => b.name === name) ?? editingFrom) : null;
	});
	const replacedMark = $derived(replaced ? sharedCredentialMark(replaced) : null);
	let saveBusy = $state(false);
	let saveError = $state('');
	// The document changed under the page: read it again and keep what the
	// dialog holds, so the person saves their change on top of the new one.
	let conflict = $state(false);
	async function readAgain() {
		await invalidateAll();
		// A read that failed leaves the page's copy as it was: say so.
		conflict = false;
		saveError = data.accessError ?? '';
		actError = data.accessError ?? '';
	}
	function openCreate() {
		editingFrom = null;
		editing = { creating: true, name: '', role: roles[0] ?? 'viewer', scope: 'deployment', subjects: '' };
		saveError = '';
		conflict = false;
	}
	function openEdit(b: Binding) {
		editingFrom = b;
		editing = { creating: false, ...bindingEdit(b) };
		saveError = '';
		conflict = false;
	}
	async function save() {
		if (!editing || !doc) return;
		saveBusy = true;
		saveError = '';
		conflict = false;
		const binding = savedBinding(editing, replaced);
		try {
			if (editing.creating) await accessClient(fetch).createBinding({ binding, etag: doc.etag });
			else await accessClient(fetch).updateBinding({ binding, etag: doc.etag });
			editing = null;
			await invalidateAll();
		} catch (err) {
			saveError = isConflict(err) ? conflictWords : errorMessage(err);
			conflict = isConflict(err);
		} finally {
			saveBusy = false;
		}
	}

	// ── delete and pin again ────────────────────────────────────────────
	let deleting = $state<Binding | null>(null);
	let repinning = $state<Binding | null>(null);
	let actBusy = $state(false);
	let actError = $state('');
	async function confirmDelete() {
		if (!deleting || !doc) return;
		actBusy = true;
		actError = '';
		conflict = false;
		try {
			await accessClient(fetch).deleteBinding({ name: deleting.name, etag: doc.etag });
			deleting = null;
			await invalidateAll();
		} catch (err) {
			actError = isConflict(err) ? conflictWords : errorMessage(err);
			conflict = isConflict(err);
		} finally {
			actBusy = false;
		}
	}
	async function confirmRepin() {
		if (!repinning || !doc) return;
		actBusy = true;
		actError = '';
		conflict = false;
		try {
			await accessClient(fetch).updateBinding({ binding: repinning, etag: doc.etag, repin: true });
			repinning = null;
			await invalidateAll();
		} catch (err) {
			actError = isConflict(err) ? conflictWords : errorMessage(err);
			conflict = isConflict(err);
		} finally {
			actBusy = false;
		}
	}
	const mine = (name: string) => grantsMe(data.me, name).map((g) => `${g.role} at ${g.scope}`).join(', ');
</script>

<svelte:head><title>Bindings · Access · OrkStack</title></svelte:head>

<SheetHeader title="Bindings" testid="access-title" commands={[{ label: 'List the bindings', command: 'orkctl admin binding list' }]}>
	{#snippet actions()}
		<button
			type="button"
			class="btn btn-primary"
			onclick={() => canBind && openCreate()}
			aria-disabled={!canBind}
			aria-describedby={canBind ? undefined : 'access-write-reason'}
			data-testid="binding-new"
			disabled={!doc}>New binding</button
		>
		{#if !canBind}<p id="access-write-reason" class="w-full text-ink-3 text-sm">{writeReason}</p>{/if}
	{/snippet}
</SheetHeader>
<AccessTabs />

<p class="mb-4 max-w-prose text-sm text-ink-2" data-testid="access-bucket-line">
	Whoever can write this deployment’s bucket with an engine’s credential can rewrite this document, and is an administrator
	of the deployment whatever it says.
</p>

{#if data.accessError}
	<ErrorPanel message={data.accessError} subject="the access document" testid="access-error" onretry={() => invalidateAll()} />
{:else if doc && status}
	<div class="mb-4 flex flex-col gap-3">
		{#if doc.writtenAroundApi}
			<div class="alert alert-fail" role="alert" data-testid="banner-around-api">
				<span class="alert-ic"><Icon name="alert" /></span>
				<p class="min-w-0 flex-1">
					The stored document was written around the API, not through it. Read what changed in
					<a class="link" href="/admin/access/history">History</a>, and acknowledge that revision there if it is yours.
				</p>
			</div>
		{/if}
		{#if status.documentStale}
			<div class="alert" role="status" data-testid="banner-stale">
				<span class="alert-ic"><Icon name="clock" /></span>
				<p class="min-w-0 flex-1">
					This engine decides from a copy of the document read <TimeAgo at={status.documentReadAt} />; the store has not
					answered since. Past its stale limit it serves configuration’s admins only.
				</p>
			</div>
		{/if}
		{#if status.modeRecordMissing}
			<div class="alert alert-fail" role="alert" data-testid="banner-mode-record">
				<span class="alert-ic"><Icon name="alert" /></span>
				<p class="min-w-0 flex-1">The deployment’s mode record was there and is gone. Engines refuse to start until it is written again.</p>
			</div>
		{/if}
		{#if notEnforcing.length > 0}
			<div class="alert alert-fail" role="alert" data-testid="banner-fleet">
				<span class="alert-ic"><Icon name="alert" /></span>
				<div class="min-w-0 flex-1">
					<p>Not every engine decides as this one does. A request one of these answers is not checked the same way:</p>
					<ul class="mt-1 font-mono text-sm">
						{#each notEnforcing as e (e.id)}
							<li>{e.id}{e.buildId ? ` (${e.buildId})` : ''}: {e.mode !== 'enforce' ? `runs ${e.mode}` : `holds another ${e.settingsDiffer.join(' and ')}`}</li>
						{/each}
					</ul>
				</div>
			</div>
		{/if}
		{#if status.orkersReachingControlPlane.length > 0 || status.orkersUnchecked.length > 0}
			<div class="alert alert-fail" role="alert" data-testid="banner-orkers">
				<span class="alert-ic"><Icon name="alert" /></span>
				<div class="min-w-0 flex-1">
					{#if status.orkersReachingControlPlane.length > 0}
						<p>These Orkers’ storage credential reaches this document and the engines’ own objects: <span class="font-mono">{status.orkersReachingControlPlane.join(', ')}</span>.</p>
					{/if}
					{#if status.orkersUnchecked.length > 0}
						<p>These Orkers’ storage credential was never checked: <span class="font-mono">{status.orkersUnchecked.join(', ')}</span>.</p>
					{/if}
				</div>
			</div>
		{/if}
	</div>

	<section class="card" aria-labelledby="bindings-h">
		<div class="card-head flex flex-wrap items-center gap-3">
			<h2 id="bindings-h">Bindings <span class="tag">{rows.length}</span></h2>
			<div class="card-end flex flex-wrap items-center gap-2">
				<label class="sr-only" for="binding-q">Find a binding</label>
				<input id="binding-q" class="search field" type="search" placeholder="Name, scope or subject" bind:value={filter.text} data-testid="binding-filter-text" />
				<label class="sr-only" for="binding-role">Role</label>
				<select id="binding-role" class="field" bind:value={filter.role} data-testid="binding-filter-role">
					<option value="">Any role</option>
					{#each roles as r (r)}<option value={r}>{r}</option>{/each}
				</select>
				<label class="sr-only" for="binding-kind">Scope</label>
				<select id="binding-kind" class="field" bind:value={filter.scopeKind} data-testid="binding-filter-scope">
					<option value="">Any scope</option>
					<option value="deployment">Deployment</option>
					<option value="namespace">A namespace</option>
					<option value="orkflow">An Orkflow</option>
					<option value="pool">A pool</option>
				</select>
			</div>
		</div>
		<div class="overflow-x-auto">
			<table class="register register-stacked register-pairs wide-reg" data-testid="binding-table">
				<thead class="max-sm:sr-only">
					<tr><th>Name</th><th>Role</th><th>Scope</th><th>Subjects</th><th><span class="sr-only">Actions</span></th></tr>
				</thead>
				<tbody>
					{#each rows as b ((b.fromConfiguration ? 'c:' : 's:') + b.name)}
						{@const pin = pinWords(b)}
						{@const shared = sharedCredentialMark(b)}
						<tr data-testid="binding-{b.name}">
							<td class="font-mono">{b.name}</td>
							<td data-label="Role">{b.role}</td>
							<td data-label="Scope">
								<span class="font-mono">{b.scope}</span>
								{#if pin}<span class="ml-2 {pin.stale ? 'stamp stamp-sm pill-outline' : 'tag'}" data-tip={pin.tip}>{pin.word}</span>{/if}
							</td>
							<td data-label="Subjects"><ul class="font-mono text-sm break-all">{#each b.subjects as s (s)}<li>{s}</li>{/each}</ul>
								{#if shared}<span class="tag mt-1.5" data-testid="binding-shared-{b.name}">{shared.word}</span><span class="mt-1 block text-sm text-ink-3">{shared.why}</span>{/if}</td>
							<td class="row-acts text-right whitespace-nowrap max-sm:col-span-2">
								{#if b.fromConfiguration}
									<span class="pill-plain stamp stamp-sm" data-tip="Made by the engine’s configuration (auth.admins, auth.orkers); change it there.">From configuration</span>
								{:else if canBind}
									{#if canWrite && pin?.stale}<button type="button" class="btn btn-sm" onclick={() => { repinning = b; actError = ''; conflict = false; }} data-testid="binding-repin-{b.name}">Pin again</button>{/if}
									{#if canWrite}<button type="button" class="btn btn-sm" onclick={() => openEdit(b)} data-testid="binding-edit-{b.name}">Edit</button>{/if}
									<button type="button" class="btn btn-sm" onclick={() => { deleting = b; actError = ''; conflict = false; }} data-testid="binding-delete-{b.name}"><Icon name="trash" />Delete</button>
								{/if}
							</td>
						</tr>
					{:else}
						<tr><td colspan="5" class="text-ink-3" data-testid="binding-none">No binding matches.</td></tr>
					{/each}
				</tbody>
			</table>
		</div>
	</section>
{/if}

{#if editing}
	<Dialog title={editing.creating ? 'New binding' : `Edit ${editing.name}`} testid="binding-dialog" onclose={() => (editing = null)} widthClass="max-w-xl" glyph="apply">
		{#snippet body()}
			<form class="flex flex-col gap-4" onsubmit={(e) => { e.preventDefault(); save(); }}>
				<label class="flex flex-col gap-1">
					<span class="text-sm font-semibold text-ink-2">Name</span>
					<input class="field font-mono" bind:value={editing!.name} required readonly={!editing!.creating} data-testid="binding-name" />
				</label>
				<label class="flex flex-col gap-1">
					<span class="text-sm font-semibold text-ink-2">Role</span>
					<select class="field" bind:value={editing!.role} data-testid="binding-role">{#each roles as r (r)}<option value={r}>{r}</option>{/each}</select>
				</label>
				<label class="flex flex-col gap-1">
					<span class="text-sm font-semibold text-ink-2">Scope</span>
					<input class="field font-mono" bind:value={editing!.scope} required data-testid="binding-scope" aria-describedby="binding-scope-hint" />
					<span id="binding-scope-hint" class="text-sm text-ink-3"><code>deployment</code>, <code>namespace:NAME</code>, <code>orkflow:NAMESPACE/NAME</code> or <code>pool:NAME</code></span>
				</label>
				<label class="flex flex-col gap-1">
					<span class="text-sm font-semibold text-ink-2">Subjects, one a line</span>
					<textarea class="field font-mono" rows="4" bind:value={editing!.subjects} required data-testid="binding-subjects" aria-describedby="binding-subjects-hint"></textarea>
					<span id="binding-subjects-hint" class="text-sm text-ink-3"><code>user:ISSUER:NAME</code>, <code>group:ISSUER:GROUP</code>, <code>serviceaccount:NAME</code>…</span>
				</label>
				{#if replacedMark}
					<p class="alert" data-testid="binding-shared-note"><span class="alert-ic"><Icon name="info" /></span><span class="min-w-0 flex-1">{editing!.role === 'orker' ? `${replacedMark.why} Saving keeps that.` : 'Its Orkers share one credential. Only the orker role may be bound that way, so saving it with another role ends that.'}</span></p>
				{/if}
				{#if !editing!.creating && mine(editing!.name)}
					<p class="alert" data-testid="binding-self"><span class="alert-ic"><Icon name="info" /></span><span class="min-w-0 flex-1">You hold <strong>{mine(editing!.name)}</strong> by this binding. A change to it changes what you can do.</span></p>
				{/if}
				{#if saveError}
					<div class="flex flex-wrap items-center gap-3" role="alert" data-testid="binding-error">
						<p class="text-fail">{saveError}</p>
						{#if conflict}<button type="button" class="btn btn-sm" onclick={readAgain} data-testid="binding-read-again">Read it again</button>{/if}
					</div>
				{/if}
				<div class="flex justify-end gap-2">
					<button type="button" class="btn" onclick={() => (editing = null)}>Keep as it was</button>
					<button type="submit" class="btn btn-primary" disabled={saveBusy} data-testid="binding-save"><Icon name="apply" />{saveBusy ? 'Saving…' : 'Save binding'}</button>
				</div>
			</form>
		{/snippet}
	</Dialog>
{/if}

{#if deleting}
	<ConfirmDialog
		title="Delete {deleting.name}?"
		testid="binding-delete-dialog"
		acceptLabel="Delete binding"
		cancelLabel="Keep it"
		acceptTestid="binding-delete-confirm"
		cancelTestid="binding-delete-cancel"
		destructive
		glyph="trash"
		busy={actBusy}
		error={actError}
		onaccept={confirmDelete}
		oncancel={() => (deleting = null)}
	>
		{#snippet body()}
			<p>Everyone it names loses <strong>{deleting!.role}</strong> at <code>{deleting!.scope}</code> within each engine’s refresh interval. Sources and objects saved through it pause or are refused one interval later unless saved again. To stop one person, disable them instead.</p>
			{#if mine(deleting!.name)}<p class="mt-3 font-semibold" data-testid="binding-delete-self">You hold {mine(deleting!.name)} by it: you lose that too, unless another binding grants it.</p>{/if}
			{#if conflict}<button type="button" class="btn btn-sm mt-3" onclick={readAgain} data-testid="binding-delete-read-again">Read it again</button>{/if}
		{/snippet}
	</ConfirmDialog>
{/if}

{#if repinning}
	<ConfirmDialog
		title="Pin {repinning.name} again?"
		testid="binding-repin-dialog"
		acceptLabel="Pin again"
		cancelLabel="Leave it stale"
		acceptTestid="binding-repin-confirm"
		cancelTestid="binding-repin-cancel"
		busy={actBusy}
		error={actError}
		glyph="apply"
		onaccept={confirmRepin}
		oncancel={() => (repinning = null)}
	>
		{#snippet body()}
			<p>It will grant <strong>{repinning!.role}</strong> on the <code>{repinning!.scope}</code> that exists now, which is not the one it was saved for.</p>
			{#if conflict}<button type="button" class="btn btn-sm mt-3" onclick={readAgain}>Read it again</button>{/if}
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
