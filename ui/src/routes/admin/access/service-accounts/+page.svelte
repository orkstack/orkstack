<script lang="ts">
	// Access → Service accounts (UX35): each account, its keys (id, added)
	// and the bindings that name it; make or delete an account, add or remove
	// a key. A key made here is a P-256 pair made in this browser: its private
	// half is offered as a file, as often as asked, until the person says it is
	// saved; only then is the public half stored. The private half is never
	// shown or kept.
	import { invalidateAll } from '$app/navigation';
	import { create } from '@bufbuild/protobuf';
	import ConfirmDialog from '#lib/components/ConfirmDialog.svelte';
	import Dialog from '#lib/components/Dialog.svelte';
	import ErrorPanel from '#lib/components/ErrorPanel.svelte';
	import Icon from '#lib/components/Icon.svelte';
	import SheetHeader from '#lib/components/SheetHeader.svelte';
	import AccessTabs from '../AccessTabs.svelte';
	import TimeAgo from '#lib/components/TimeAgo.svelte';
	import { errorMessage } from '#lib/client.ts';
	import { accessClient } from '#lib/clients/access.ts';
	import { ServiceAccountKeySchema, ServiceAccountSchema, type ServiceAccount } from '#lib/gen/orkstack/v1/access_pb.ts';
	import Plate from '#lib/components/Plate.svelte';
	import { conflictWords, isConflict } from '#lib/accessSelf.ts';
	import { generateKeyPair, keyFileName, offerDownload, type KeyPairPEM } from '#lib/keypair.ts';
	import { can, missing } from '#lib/permissions.ts';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();
	const doc = $derived(data.access?.document);
	const canWrite = $derived(can(data.grants, 'access.write'));
	const bindingsOf = (name: string) =>
		[...(doc?.configuredBindings ?? []), ...(doc?.bindings ?? [])].filter((b) => b.subjects.includes(`serviceaccount:${name}`));

	let creating = $state<{ name: string; description: string; withKey: boolean } | null>(null);
	let busy = $state(false);
	let error = $state('');
	let removing = $state<{ account: ServiceAccount; kid: string } | null>(null);
	let deleting = $state<ServiceAccount | null>(null);
	// What a stored key's private half was saved as; it stays until dismissed.
	let saved = $state('');

	// A key made here and not stored yet. Its private half lives only in this
	// state: the public half is stored once the person says they have saved
	// the file, and closing the dialog drops both, storing nothing.
	// It holds the account's name, never a copy of the account: the keys a
	// store adds to are read from the document as it is at that moment.
	let pending = $state<{ account: string; adding: boolean; description: string; pair: KeyPairPEM; downloaded: boolean; confirmed: boolean } | null>(null);
	let keyConflict = $state(false);
	function closeKey() {
		pending = null;
		error = '';
		keyConflict = false;
	}
	async function readAgain() {
		await invalidateAll();
		error = data.accessError ?? '';
		keyConflict = false;
	}
	const fileName = $derived(pending ? keyFileName(pending.account, pending.pair.kid) : '');

	async function createAccount() {
		if (!creating || !doc) return;
		busy = true;
		error = '';
		try {
			const name = creating.name.trim();
			if (!/^[a-z][a-z0-9-]*$/.test(name)) {
				error = 'A name is lower-case letters, digits and hyphens, beginning with a letter.';
				return;
			}
			if (doc.serviceAccounts.some((a) => a.name === name)) {
				error = `serviceaccount:${name} already exists.`;
				return;
			}
			if (creating.withKey) {
				pending = { account: name, adding: false, description: creating.description.trim(), pair: await generateKeyPair(), downloaded: false, confirmed: false };
				creating = null;
				return;
			}
			await accessClient(fetch).createServiceAccount({
				serviceAccount: create(ServiceAccountSchema, { name, description: creating.description.trim(), keys: [] }),
				etag: doc.etag
			});
			creating = null;
			await invalidateAll();
		} catch (err) {
			error = isConflict(err) ? conflictWords : errorMessage(err);
		} finally {
			busy = false;
		}
	}

	async function addKey(sa: ServiceAccount) {
		if (!doc || !canWrite) return;
		error = '';
		try {
			pending = { account: sa.name, adding: true, description: sa.description, pair: await generateKeyPair(), downloaded: false, confirmed: false };
		} catch (err) {
			// No Web Crypto (a page served over plain HTTP) or a refusal: say so.
			error = `No key could be made in this browser: ${errorMessage(err)}`;
		}
	}

	function download() {
		if (!pending) return;
		offerDownload(fileName, pending.pair.privatePem);
		pending.downloaded = true;
	}

	async function storeKey() {
		if (!pending || !pending.confirmed || !doc) return;
		busy = true;
		error = '';
		keyConflict = false;
		try {
			const key = create(ServiceAccountKeySchema, { publicKeyPem: pending.pair.publicPem });
			const now = doc.serviceAccounts.find((a) => a.name === pending!.account);
			if (pending.adding && !now) {
				error = `serviceaccount:${pending.account} is gone: nothing was stored.`;
				return;
			}
			if (!pending.adding && now) {
				error = `serviceaccount:${pending.account} was made meanwhile: nothing was stored.`;
				return;
			}
			if (now) {
				const next = create(ServiceAccountSchema, { ...now, keys: [...now.keys, key] });
				await accessClient(fetch).updateServiceAccount({ serviceAccount: next, etag: doc.etag });
			} else {
				await accessClient(fetch).createServiceAccount({
					serviceAccount: create(ServiceAccountSchema, { name: pending.account, description: pending.description, keys: [key] }),
					etag: doc.etag
				});
			}
			saved = `serviceaccount:${pending.account} has the key ${pending.pair.kid}. Its private half is the file ${fileName}; OrkStack did not keep it and cannot show it again.`;
			pending = null;
			await invalidateAll();
		} catch (err) {
			error = isConflict(err) ? conflictWords : errorMessage(err);
			keyConflict = isConflict(err);
		} finally {
			busy = false;
		}
	}

	async function confirmRemove() {
		if (!removing || !doc) return;
		busy = true;
		error = '';
		try {
			const sa = removing.account;
			const next = create(ServiceAccountSchema, { ...sa, keys: sa.keys.filter((k) => k.kid !== removing!.kid) });
			await accessClient(fetch).updateServiceAccount({ serviceAccount: next, etag: doc.etag });
			removing = null;
			await invalidateAll();
		} catch (err) {
			error = errorMessage(err);
		} finally {
			busy = false;
		}
	}

	async function confirmDelete() {
		if (!deleting || !doc) return;
		busy = true;
		error = '';
		try {
			await accessClient(fetch).deleteServiceAccount({ name: deleting.name, etag: doc.etag });
			deleting = null;
			await invalidateAll();
		} catch (err) {
			error = errorMessage(err);
		} finally {
			busy = false;
		}
	}
</script>

<svelte:head><title>Service accounts · Access · OrkStack</title></svelte:head>

<SheetHeader title="Service accounts" testid="sa-title" commands={[{ label: 'List the service accounts', command: 'orkctl admin service-account list' }]}>
	{#snippet actions()}
		<button type="button" class="btn btn-primary" onclick={() => canWrite && (creating = { name: '', description: '', withKey: true })} aria-disabled={!canWrite} aria-describedby={canWrite ? undefined : 'sa-write-reason'} disabled={!doc} data-testid="sa-new">New service account</button>
		{#if !canWrite}<p id="sa-write-reason" class="w-full text-sm text-ink-3">{missing('access.write')}</p>{/if}
	{/snippet}
</SheetHeader>
<AccessTabs />

{#if saved}
	<div class="alert mb-4" role="status" data-testid="sa-saved">
		<span class="alert-ic"><Icon name="download" /></span>
		<p class="min-w-0 flex-1 break-words">{saved}</p>
		<button type="button" class="btn btn-sm btn-quiet" onclick={() => (saved = '')} data-testid="sa-saved-dismiss">Dismiss</button>
	</div>
{/if}
{#if error && !creating && !removing && !deleting && !pending}<p class="mb-4 text-fail" role="alert">{error}</p>{/if}

{#if data.accessError}
	<ErrorPanel message={data.accessError} subject="the service accounts" testid="sa-error" onretry={() => invalidateAll()} />
{:else if doc}
	{#if doc.serviceAccounts.length === 0}
		<p class="text-ink-2" data-testid="sa-none">No service account yet. One lets a script or an Orker call the API with a key of its own.</p>
	{/if}
	<div class="flex flex-col gap-4">
		{#each doc.serviceAccounts as sa (sa.name)}
			{@const bound = bindingsOf(sa.name)}
			<section class="card" aria-labelledby="sa-{sa.name}" data-testid="sa-{sa.name}">
				<div class="card-head">
					<h2 id="sa-{sa.name}" class="font-mono">serviceaccount:{sa.name}</h2>
					{#if canWrite}
						<div class="card-end flex gap-2">
							<button type="button" class="btn btn-sm" onclick={() => addKey(sa)} disabled={busy || sa.keys.length >= 4} data-testid="sa-add-key-{sa.name}">Add a key</button>
							<button type="button" class="btn btn-sm" onclick={() => { deleting = sa; error = ''; }} data-testid="sa-delete-{sa.name}"><Icon name="trash" />Delete</button>
						</div>
					{/if}
				</div>
				<div class="flex flex-col gap-3 px-4 pb-4">
					{#if sa.description}<p class="text-ink-2">{sa.description}</p>{/if}
					{#if sa.orkerId}<p class="text-sm">Names the Orker <span class="font-mono">{sa.orkerId}</span></p>{/if}
					<table class="register">
						<thead><tr><th>Key</th><th>Added</th><th><span class="sr-only">Actions</span></th></tr></thead>
						<tbody>
							{#each sa.keys as k (k.kid)}
								<tr>
									<td class="font-mono text-sm break-all">{k.kid}</td>
									<td>{#if k.addedAt}<TimeAgo at={k.addedAt} />{:else}<span class="text-ink-3">Not recorded</span>{/if}</td>
									<td class="text-right">{#if canWrite}<button type="button" class="btn btn-sm" onclick={() => { removing = { account: sa, kid: k.kid }; error = ''; }} data-testid="sa-remove-key-{k.kid}"><Icon name="trash" />Remove</button>{/if}</td>
								</tr>
							{:else}
								<tr><td colspan="3" class="text-ink-3">No key: it cannot sign anything.</td></tr>
							{/each}
						</tbody>
					</table>
					<p class="text-sm text-ink-2">
						{#if bound.length === 0}Bound to nothing.{:else}Bound by {#each bound as b, i (b.name)}{i > 0 ? ', ' : ''}<span class="font-mono">{b.name}</span> ({b.role} at <span class="font-mono">{b.scope}</span>){/each}.{/if}
					</p>
				</div>
			</section>
		{/each}
	</div>
{/if}

{#if creating}
	<Dialog title="New service account" testid="sa-dialog" onclose={() => (creating = null)} glyph="apply">
		{#snippet body()}
			<form class="flex flex-col gap-4" onsubmit={(e) => { e.preventDefault(); createAccount(); }}>
				<label class="flex flex-col gap-1"><span class="text-sm font-semibold text-ink-2">Name</span><input class="field font-mono" bind:value={creating!.name} required data-testid="sa-name" /></label>
				<label class="flex flex-col gap-1"><span class="text-sm font-semibold text-ink-2">What it is for</span><input class="field" bind:value={creating!.description} data-testid="sa-description" /></label>
				<label class="flex items-start gap-2"><input type="checkbox" bind:checked={creating!.withKey} data-testid="sa-with-key" /><span>Make a key now. You save its private half as a file before the account is created; OrkStack keeps only the public half.</span></label>
				{#if error}<p class="text-fail" role="alert" data-testid="sa-create-error">{error}</p>{/if}
				<div class="flex justify-end gap-2">
					<button type="button" class="btn" onclick={() => (creating = null)}>Keep as it was</button>
					<button type="submit" class="btn btn-primary" disabled={busy} data-testid="sa-save">{#if creating!.withKey}Next: save the key{:else}<Icon name="apply" />{busy ? 'Saving…' : 'Create service account'}{/if}</button>
				</div>
			</form>
		{/snippet}
	</Dialog>
{/if}

{#if removing}
	<ConfirmDialog title="Remove this key?" testid="sa-remove-dialog" acceptLabel="Remove key" cancelLabel="Keep it" acceptTestid="sa-remove-confirm" cancelTestid="sa-remove-cancel" destructive glyph="trash" busy={busy} error={error} onaccept={confirmRemove} oncancel={() => (removing = null)}>
		{#snippet body()}
			<p>Whatever signs with <span class="font-mono break-all">{removing!.kid}</span> is refused from each engine’s next refresh of the document.</p>
			{#if removing!.account.keys.length === 1}<p class="mt-3 font-semibold" data-testid="sa-remove-last">It is the account’s only key: nothing can sign as serviceaccount:{removing!.account.name} until a key is added.</p>{/if}
		{/snippet}
	</ConfirmDialog>
{/if}

{#if deleting}
	<ConfirmDialog title="Delete serviceaccount:{deleting.name}?" testid="sa-delete-dialog" acceptLabel="Delete service account" cancelLabel="Keep it" acceptTestid="sa-delete-confirm" cancelTestid="sa-delete-cancel" destructive glyph="trash" busy={busy} error={error} onaccept={confirmDelete} oncancel={() => (deleting = null)}>
		{#snippet body()}<p>Everything that signs as it is refused from each engine’s next refresh of the document. Bindings that name it stay, and match nothing.</p>{/snippet}
	</ConfirmDialog>
{/if}

{#if pending}
	<Dialog title="Save the private key" testid="sa-key-dialog" onclose={closeKey} glyph="download">
		{#snippet body()}
			<div class="flex flex-col gap-4">
				<p>
					This is the only copy of the private half of a new key for <span class="font-mono">serviceaccount:{pending!.account}</span>. OrkStack
					stores the public half alone; close this without storing, and the key is gone and nothing changes.
				</p>
				<Plate key="Key" value={pending!.pair.kid} wide testid="sa-key-kid" />
				<div class="flex flex-wrap items-center gap-3">
					<button type="button" class="btn" onclick={download} data-testid="sa-key-download"><Icon name="download" />{pending!.downloaded ? 'Download again' : `Download ${fileName}`}</button>
					{#if pending!.downloaded}<span class="text-sm text-ink-2">Offered as <span class="font-mono">{fileName}</span></span>{/if}
				</div>
				<label class="flex items-start gap-2">
					<input type="checkbox" bind:checked={pending!.confirmed} disabled={!pending!.downloaded} aria-describedby={pending!.downloaded ? undefined : 'sa-key-confirm-hint'} data-testid="sa-key-confirm" />
					<span>I saved <span class="font-mono">{fileName}</span> where whatever signs as this account will read it.</span>
				</label>
				{#if !pending!.downloaded}<p id="sa-key-confirm-hint" class="text-sm text-ink-3">Download the file first.</p>{/if}
				{#if error}
					<div class="flex flex-wrap items-center gap-3" role="alert" data-testid="sa-key-error">
						<p class="text-fail">{error}</p>
						{#if keyConflict}<button type="button" class="btn btn-sm" onclick={readAgain} data-testid="sa-key-read-again">Read it again</button>{/if}
					</div>
				{/if}
				<div class="flex flex-wrap justify-end gap-2">
					<button type="button" class="btn" onclick={closeKey} data-testid="sa-key-discard">Discard this key</button>
					<button type="button" class="btn btn-primary" onclick={storeKey} disabled={busy || !pending!.confirmed} data-testid="sa-key-store"
						><Icon name="apply" />{busy ? 'Saving…' : pending!.adding ? 'Add the key' : 'Create service account'}</button
					>
				</div>
			</div>
		{/snippet}
	</Dialog>
{/if}
