<script lang="ts">
	// The Admin Namespaces view (design §6.1, §6.2): every namespace this
	// deployment has, with create/edit/delete against the real registry.
	//
	// Writes are honest (acceptance 2): an update sends the etag it read, a
	// conflict is a FailedPrecondition and is shown as one, a refused delete
	// names the orkflow count the server refused it with (verbatim — the
	// server already names them, so nothing here re-derives that sentence),
	// and `default`'s delete control is disabled BEFORE any click, with the
	// reason inline.
	import { goto, invalidateAll } from '$app/navigation';
	import { page } from '$app/state';
	import ConfirmDialog from '$lib/components/ConfirmDialog.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import { ConnectError, Code } from '@connectrpc/connect';
	import { errorMessage, orkstack } from '$lib/client';
	import type { Namespace } from '$lib/gen/orkstack/v1/namespace_pb';
	import { DEFAULT_NAMESPACE, namespaceLabel, rememberNamespace, withNamespace } from '$lib/namespace';
	import {
		DEFAULT_POOL_NAME,
		parsePoolList,
		validateNamespaceForm,
		type NamespaceFormErrors
	} from '$lib/namespaceRules';
	import { fmtTimestamp } from '$lib/status';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	let retrying = $state(false);
	async function retry() {
		retrying = true;
		try {
			await invalidateAll();
		} finally {
			retrying = false;
		}
	}

	// ── create ─────────────────────────────────────────────────────────────
	let createName = $state('');
	let createDescription = $state('');
	let createDefaultPool = $state('');
	let createAllowedPools = $state('');
	let createErrors = $state<NamespaceFormErrors>({});
	let createBusy = $state(false);
	let createServerError = $state('');
	let createSuccess = $state('');

	function resetCreateForm() {
		createName = '';
		createDescription = '';
		createDefaultPool = '';
		createAllowedPools = '';
	}

	async function submitCreate() {
		createServerError = '';
		createSuccess = '';
		const allowed = parsePoolList(createAllowedPools);
		createErrors = validateNamespaceForm(
			{ name: createName.trim(), defaultPool: createDefaultPool.trim(), allowedPools: allowed },
			{ requireName: true }
		);
		// Local validation blocks the request entirely — no RPC is made for a
		// slug the registry would refuse anyway (design §6.2, acceptance 6).
		if (Object.keys(createErrors).length > 0) return;

		createBusy = true;
		try {
			const res = await orkstack(fetch).namespaces.createNamespace({
				namespace: {
					name: createName.trim(),
					description: createDescription.trim(),
					defaultPool: createDefaultPool.trim(),
					allowedPools: allowed
				}
			});
			createSuccess = res.created
				? `Created ${res.namespace?.name}.`
				: `${res.namespace?.name} already exists with this exact spec — nothing changed.`;
			resetCreateForm();
			await invalidateAll();
		} catch (err) {
			createServerError = errorMessage(err);
		} finally {
			createBusy = false;
		}
	}

	// ── edit (etag round trip) ────────────────────────────────────────────
	let editingName = $state('');
	let editDescription = $state('');
	let editDefaultPool = $state('');
	let editAllowedPools = $state('');
	let editEtag = $state('');
	let editErrors = $state<NamespaceFormErrors>({});
	let editBusy = $state(false);
	let editServerError = $state('');
	// A conflict names both what was expected to still be there and that the
	// document has since moved — the wording UpdateNamespace's own refusal
	// uses (internal/api/namespaces.go).
	let editConflict = $state(false);

	function openEdit(ns: Namespace) {
		editingName = ns.name;
		editDescription = ns.description;
		editDefaultPool = ns.defaultPool;
		editAllowedPools = ns.allowedPools.join(', ');
		editEtag = ns.etag;
		editLabels = { ...ns.labels };
		editErrors = {};
		editServerError = '';
		editConflict = false;
	}

	function closeEdit() {
		editingName = '';
		editConflict = false;
		editServerError = '';
	}
	// editLabels carries a namespace's labels UNTOUCHED through an edit. This
	// form has no control for them — design §6.1's Admin feature list does
	// not ask for one — but UpdateNamespace is a full replace of the mutable
	// document (internal/api/namespaces.go), so a submit that omitted them
	// would silently erase whatever labels were there. They are read here and
	// sent back exactly as read; only the fields this form DOES expose can
	// change.
	let editLabels: Record<string, string> = {};

	async function submitEdit() {
		editServerError = '';
		editConflict = false;
		const allowed = parsePoolList(editAllowedPools);
		editErrors = validateNamespaceForm(
			{ name: editingName, defaultPool: editDefaultPool.trim(), allowedPools: allowed },
			{ requireName: false }
		);
		if (Object.keys(editErrors).length > 0) return;

		editBusy = true;
		try {
			await orkstack(fetch).namespaces.updateNamespace({
				namespace: {
					name: editingName,
					description: editDescription.trim(),
					defaultPool: editDefaultPool.trim(),
					allowedPools: allowed,
					labels: editLabels,
					etag: editEtag
				}
			});
			closeEdit();
			await invalidateAll();
		} catch (err) {
			// UpdateNamespace's compare-and-swap is FailedPrecondition when the
			// etag this form holds is no longer the stored one — a stale read,
			// shown as one. The form's values are left exactly where the user
			// left them: a conflict must never discard an edit.
			if (err instanceof ConnectError && err.code === Code.FailedPrecondition) {
				editConflict = true;
				editServerError = errorMessage(err);
			} else {
				editServerError = errorMessage(err);
			}
		} finally {
			editBusy = false;
		}
	}

	// reloadEtagAndRetry re-reads the namespace for its CURRENT etag and
	// re-submits this same edit against it — "the loser re-reads and
	// re-applies" (UpdateNamespace's own doc comment). The description/pool
	// fields are untouched: only the concurrency token moves.
	async function reloadEtagAndRetry() {
		editBusy = true;
		editServerError = '';
		try {
			const res = await orkstack(fetch).namespaces.getNamespace({ name: editingName });
			editEtag = res.namespace?.etag ?? '';
			editConflict = false;
			await submitEdit();
		} catch (err) {
			editServerError = errorMessage(err);
		} finally {
			editBusy = false;
		}
	}

	// ── delete ─────────────────────────────────────────────────────────────
	let pendingDelete = $state('');
	let deleteBusy = $state(false);
	let deleteError = $state('');

	function askToDelete(name: string) {
		pendingDelete = name;
		deleteError = '';
	}

	async function confirmDelete() {
		deleteBusy = true;
		deleteError = '';
		const deletedName = pendingDelete;
		try {
			await orkstack(fetch).namespaces.deleteNamespace({ name: deletedName });
			pendingDelete = '';
			// The namespace this page/app is currently SCOPED TO no longer
			// exists. Leaving `?ns=` pointed at it would carry a coordinate the
			// registry no longer answers for onto Pools, onto Orkflows, onto
			// every link this page's own layout builds — the exact confusion
			// acceptance 4 asks this area not to cause. Reset to `default`,
			// which always resolves (AGENTS.md §1.10), rather than leaving a
			// dangling selection for the next click to discover.
			if (namespaceLabel(data.namespace) === deletedName) {
				rememberNamespace(DEFAULT_NAMESPACE);
				await goto(withNamespace(page.url.pathname + page.url.search, DEFAULT_NAMESPACE), {
					replaceState: true,
					invalidateAll: true
				});
			} else {
				await invalidateAll();
			}
		} catch (err) {
			// The refusal is the server's own sentence, verbatim: it already
			// names the orkflow count and the orkflows in the way
			// (internal/api/namespaces.go DeleteNamespace) — nothing here
			// re-derives or paraphrases it.
			deleteError = errorMessage(err);
		} finally {
			deleteBusy = false;
		}
	}
</script>

<svelte:head><title>Admin — Namespaces — OrkStack</title></svelte:head>

<p class="mb-4 text-sm text-zinc-500" data-testid="namespaces-scope-caption">
	All namespaces this deployment has — the namespace selector above does not filter this view.
</p>

{#if data.namespacesError}
	<ErrorPanel
		testid="namespaces-error"
		retryTestid="namespaces-retry"
		subject="the namespace registry"
		message={data.namespacesError}
		busy={retrying}
		onretry={retry}
	/>
{:else}
	<section class="mb-8 rounded-lg border border-zinc-800 p-4">
		<h2 class="mb-3 text-sm font-semibold text-zinc-200">Create a namespace</h2>
		<form
			class="grid gap-3 sm:grid-cols-2"
			onsubmit={(e) => {
				e.preventDefault();
				void submitCreate();
			}}
		>
			<label class="flex flex-col gap-1 text-sm">
				<span class="text-zinc-400">Name</span>
				<input
					data-testid="ns-create-name"
					bind:value={createName}
					placeholder="payments"
					class="rounded-md border border-zinc-700 bg-zinc-900 px-2 py-1.5 text-zinc-100"
				/>
				{#if createErrors.name}
					<span class="text-xs text-rose-400" data-testid="ns-create-error-name"
						>{createErrors.name}</span
					>
				{/if}
			</label>
			<label class="flex flex-col gap-1 text-sm">
				<span class="text-zinc-400">Description</span>
				<input
					data-testid="ns-create-description"
					bind:value={createDescription}
					class="rounded-md border border-zinc-700 bg-zinc-900 px-2 py-1.5 text-zinc-100"
				/>
			</label>
			<label class="flex flex-col gap-1 text-sm">
				<span class="text-zinc-400">Default pool (blank = {DEFAULT_POOL_NAME})</span>
				<input
					data-testid="ns-create-default-pool"
					bind:value={createDefaultPool}
					placeholder={DEFAULT_POOL_NAME}
					class="rounded-md border border-zinc-700 bg-zinc-900 px-2 py-1.5 text-zinc-100"
				/>
				{#if createErrors.defaultPool}
					<span class="text-xs text-rose-400" data-testid="ns-create-error-default-pool"
						>{createErrors.defaultPool}</span
					>
				{/if}
			</label>
			<label class="flex flex-col gap-1 text-sm">
				<span class="text-zinc-400">Allowed pools (comma-separated, blank = unrestricted)</span>
				<input
					data-testid="ns-create-allowed-pools"
					bind:value={createAllowedPools}
					placeholder="cpu, gpu"
					class="rounded-md border border-zinc-700 bg-zinc-900 px-2 py-1.5 text-zinc-100"
				/>
				{#if createErrors.allowedPools}
					<span class="text-xs text-rose-400" data-testid="ns-create-error-allowed-pools"
						>{createErrors.allowedPools}</span
					>
				{/if}
			</label>
			<div class="sm:col-span-2">
				<button
					type="submit"
					data-testid="ns-create-submit"
					disabled={createBusy}
					class="rounded-md bg-emerald-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-emerald-500 disabled:opacity-50"
				>
					{createBusy ? 'Creating…' : 'Create namespace'}
				</button>
			</div>
		</form>
		{#if createServerError}
			<p
				class="mt-3 rounded-md border border-rose-500/40 bg-rose-500/10 px-3 py-2 text-sm text-rose-300"
				data-testid="ns-create-server-error"
			>
				{createServerError}
			</p>
		{/if}
		{#if createSuccess}
			<p class="mt-3 text-sm text-emerald-300" data-testid="ns-create-success">{createSuccess}</p>
		{/if}
	</section>

	{#if retrying}
		<p class="text-sm text-zinc-400" data-testid="namespaces-loading">Loading namespaces…</p>
	{:else}
		{#if data.namespacesList.length <= 1}
			<p class="mb-3 text-sm text-zinc-400" data-testid="namespaces-empty">
				No namespaces beyond <code class="text-zinc-300">default</code> yet — it is undeletable
				below because every orkflow that names no namespace is in it. Create one above.
			</p>
		{/if}
		<div class="overflow-hidden rounded-lg border border-zinc-800">
			<table class="w-full text-left text-sm" data-testid="namespace-list">
				<thead class="bg-zinc-900 text-xs uppercase tracking-wide text-zinc-400">
					<tr>
						<th class="px-4 py-3">Name</th>
						<th class="px-4 py-3">Description</th>
						<th class="px-4 py-3">Default pool</th>
						<th class="px-4 py-3">Allowed pools</th>
						<th class="px-4 py-3">Updated</th>
						<th class="px-4 py-3"></th>
					</tr>
				</thead>
				<tbody class="divide-y divide-zinc-800">
					{#each data.namespacesList as ns (ns.name)}
						<tr class="hover:bg-zinc-900/60" data-testid="namespace-row-{ns.name}">
							<td class="px-4 py-3 font-medium text-zinc-100">{ns.name}</td>
							<td class="max-w-xs truncate px-4 py-3 text-zinc-400">{ns.description || '—'}</td>
							<td class="px-4 py-3 text-zinc-300">{ns.defaultPool || DEFAULT_POOL_NAME}</td>
							<td class="px-4 py-3 text-zinc-400">
								{ns.allowedPools.length > 0 ? ns.allowedPools.join(', ') : 'unrestricted'}
							</td>
							<td class="px-4 py-3 text-zinc-400">{fmtTimestamp(ns.updatedAt)}</td>
							<td class="px-4 py-3 text-right">
								<div class="flex items-center justify-end gap-2">
									<button
										type="button"
										data-testid="namespace-edit-{ns.name}"
										onclick={() => openEdit(ns)}
										class="rounded-md border border-zinc-700 px-3 py-1.5 text-xs text-zinc-200 hover:bg-zinc-800"
									>
										Edit
									</button>
									{#if ns.name === 'default'}
										<span
											class="text-xs text-zinc-500"
											data-testid="namespace-delete-default-reason"
											title="every orkflow that names no namespace is in it"
										>
											undeletable
										</span>
										<button
											type="button"
											data-testid="namespace-delete-default"
											disabled
											title="default cannot be deleted: every orkflow that names no namespace is in it"
											class="cursor-not-allowed rounded-md border border-zinc-800 px-3 py-1.5 text-xs text-zinc-600"
										>
											Delete
										</button>
									{:else}
										<button
											type="button"
											data-testid="namespace-delete-{ns.name}"
											onclick={() => askToDelete(ns.name)}
											class="rounded-md border border-rose-800 px-3 py-1.5 text-xs text-rose-300 hover:bg-rose-500/10"
										>
											Delete
										</button>
									{/if}
								</div>
							</td>
						</tr>
						{#if editingName === ns.name}
							<tr>
								<td colspan="6" class="bg-zinc-900/40 px-4 py-4">
									<form
										data-testid="namespace-edit-form-{ns.name}"
										class="grid gap-3 sm:grid-cols-2"
										onsubmit={(e) => {
											e.preventDefault();
											void submitEdit();
										}}
									>
										<label class="flex flex-col gap-1 text-sm">
											<span class="text-zinc-400">Description</span>
											<input
												data-testid="ns-edit-description-{ns.name}"
												bind:value={editDescription}
												class="rounded-md border border-zinc-700 bg-zinc-950 px-2 py-1.5 text-zinc-100"
											/>
										</label>
										<label class="flex flex-col gap-1 text-sm">
											<span class="text-zinc-400">Default pool</span>
											<input
												data-testid="ns-edit-default-pool-{ns.name}"
												bind:value={editDefaultPool}
												class="rounded-md border border-zinc-700 bg-zinc-950 px-2 py-1.5 text-zinc-100"
											/>
											{#if editErrors.defaultPool}
												<span class="text-xs text-rose-400">{editErrors.defaultPool}</span>
											{/if}
										</label>
										<label class="flex flex-col gap-1 text-sm sm:col-span-2">
											<span class="text-zinc-400">Allowed pools (comma-separated)</span>
											<input
												data-testid="ns-edit-allowed-pools-{ns.name}"
												bind:value={editAllowedPools}
												class="rounded-md border border-zinc-700 bg-zinc-950 px-2 py-1.5 text-zinc-100"
											/>
											{#if editErrors.allowedPools}
												<span class="text-xs text-rose-400">{editErrors.allowedPools}</span>
											{/if}
										</label>
										<div class="flex items-center gap-2 sm:col-span-2">
											<button
												type="submit"
												data-testid="ns-edit-save-{ns.name}"
												disabled={editBusy}
												class="rounded-md bg-emerald-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-emerald-500 disabled:opacity-50"
											>
												{editBusy ? 'Saving…' : 'Save'}
											</button>
											<button
												type="button"
												data-testid="ns-edit-cancel-{ns.name}"
												disabled={editBusy}
												onclick={closeEdit}
												class="rounded-md border border-zinc-700 px-3 py-1.5 text-xs text-zinc-300 hover:bg-zinc-800"
											>
												Cancel
											</button>
										</div>
									</form>
									{#if editConflict}
										<div
											class="mt-3 rounded-md border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-sm text-amber-200"
											data-testid="namespace-edit-conflict-{ns.name}"
										>
											<p>{editServerError}</p>
											<p class="mt-1 text-xs text-amber-300/80">
												Namespace {ns.name} changed since it was read — reload and re-apply.
											</p>
											<button
												type="button"
												data-testid="namespace-edit-conflict-retry-{ns.name}"
												onclick={reloadEtagAndRetry}
												disabled={editBusy}
												class="mt-2 rounded-md border border-amber-500/50 px-3 py-1 text-xs text-amber-100 hover:bg-amber-500/20 disabled:opacity-50"
											>
												Reload latest and re-apply my edit
											</button>
										</div>
									{:else if editServerError}
										<p
											class="mt-3 rounded-md border border-rose-500/40 bg-rose-500/10 px-3 py-2 text-sm text-rose-300"
											data-testid="namespace-edit-error-{ns.name}"
										>
											{editServerError}
										</p>
									{/if}
								</td>
							</tr>
						{/if}
					{/each}
				</tbody>
			</table>
		</div>
	{/if}
{/if}

{#if pendingDelete}
	<ConfirmDialog
		testid="namespace-delete-confirm"
		title="Delete namespace {pendingDelete}?"
		acceptLabel="Delete"
		acceptTestid="namespace-delete-confirm-accept"
		cancelTestid="namespace-delete-confirm-cancel"
		busy={deleteBusy}
		error={deleteError}
		onaccept={confirmDelete}
		oncancel={() => {
			pendingDelete = '';
			deleteError = '';
		}}
	>
		{#snippet body()}
			<p>
				This removes the registry entry for <span class="font-medium text-zinc-100"
					>{pendingDelete}</span
				>. It is refused while any orkflow is still stored in it.
			</p>
		{/snippet}
	</ConfirmDialog>
{/if}
