<script lang="ts">
	// The Admin Namespaces view: every namespace this
	// deployment has, with create/edit/delete against the real registry.
	//
	// Writes are honest: an update sends the etag it read, a
	// conflict is a FailedPrecondition and is shown as one, a refused delete
	// names the orkflow count the server refused it with (verbatim — the
	// server already names them, so nothing here re-derives that sentence),
	// and `default`'s delete control is disabled BEFORE any click, with the
	// reason inline.
	import { goto, invalidateAll } from '$app/navigation';
	import { navigating, page } from '$app/state';
	import ConfirmDialog from '$lib/components/ConfirmDialog.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import { isStaleEtagConflict } from '$lib/adminErrors';
	import { errorMessage, orkstack } from '$lib/client';
	import type { Namespace } from '$lib/gen/orkstack/v1/namespace_pb';
	import { DEFAULT_NAMESPACE, namespaceLabel, rememberNamespace, withNamespace } from '$lib/namespace';
	import {
		DEFAULT_POOL_NAME,
		parsePoolList,
		validateNamespaceForm,
		type NamespaceFormErrors
	} from '$lib/namespaceRules';
	import { hoursToMs, parseRetentionHours } from '$lib/retentionForm';
	import { fmtDuration } from '$lib/stats';
	import { fmtTimestamp } from '$lib/status';
	import { durationFromMs } from '@bufbuild/protobuf/wkt';
	import type { Duration } from '@bufbuild/protobuf/wkt';
	import type { PageProps } from './$types';

	// runRetentionLabel renders an unset run_retention as what it means: this
	// namespace inherits the deployment maximum, never a dash that could be
	// mistaken for "no retention" (the same rule allowedPoolsLabel-style
	// renderers in this file already follow for allowed_pools).
	function runRetentionLabel(d: Duration | undefined): string {
		return d ? fmtDuration(d) : 'inherited (deployment maximum)';
	}

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

	// navigatingHere covers every re-entry into this same URL that isn't the
	// explicit Retry click above (which calls `invalidateAll` directly, never
	// `goto`, so `navigating` never turns true for it) — chiefly the
	// namespace switcher re-navigating `/admin/namespaces?ns=…` while this
	// page stays mounted with its stale list still on screen.
	const navigatingHere = $derived(
		navigating.to !== null && navigating.to.url.pathname === page.url.pathname
	);
	const busy = $derived(retrying || navigatingHere);


	// ── create ─────────────────────────────────────────────────────────────
	let createName = $state('');
	let createDescription = $state('');
	let createDefaultPool = $state('');
	let createAllowedPools = $state('');
	let createRunRetentionHours = $state('');
	let createErrors = $state<NamespaceFormErrors>({});
	let createBusy = $state(false);
	let createServerError = $state('');
	let createSuccess = $state('');

	function resetCreateForm() {
		createName = '';
		createDescription = '';
		createDefaultPool = '';
		createAllowedPools = '';
		createRunRetentionHours = '';
	}

	async function submitCreate() {
		createServerError = '';
		createSuccess = '';
		const allowed = parsePoolList(createAllowedPools);
		createErrors = validateNamespaceForm(
			{
				name: createName.trim(),
				defaultPool: createDefaultPool.trim(),
				allowedPools: allowed,
				runRetentionHours: createRunRetentionHours
			},
			{ requireName: true }
		);
		// Local validation blocks the request entirely — no RPC is made for a
		// slug the registry would refuse anyway.
		if (Object.keys(createErrors).length > 0) return;

		createBusy = true;
		try {
			const res = await orkstack(fetch).namespaces.createNamespace({
				namespace: {
					name: createName.trim(),
					description: createDescription.trim(),
					defaultPool: createDefaultPool.trim(),
					allowedPools: allowed,
					// Blank means inherit: omit the field entirely rather than send
					// a zero duration, which validNamespaceRetention would refuse
					// as non-positive.
					runRetention:
						createRunRetentionHours.trim() === ''
							? undefined
							: durationFromMs(hoursToMs(Number(createRunRetentionHours.trim())))
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
	let editRunRetentionHours = $state('');
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
		editRunRetentionHours = ns.runRetention ? String(Number(ns.runRetention.seconds) / 3600) : '';
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
	// form has no control for them, but UpdateNamespace is a full replace of
	// the mutable
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
			{
				name: editingName,
				defaultPool: editDefaultPool.trim(),
				allowedPools: allowed,
				runRetentionHours: editRunRetentionHours
			},
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
					etag: editEtag,
					// Unlike labels (which this form never edits directly and so
					// carries through untouched), this field IS user-editable —
					// openEdit prefilled it from what was stored — so a blank
					// here is a deliberate choice to inherit the deployment
					// maximum, not an omission to preserve.
					runRetention:
						editRunRetentionHours.trim() === ''
							? undefined
							: durationFromMs(hoursToMs(Number(editRunRetentionHours.trim())))
				}
			});
			closeEdit();
			await invalidateAll();
		} catch (err) {
			// UpdateNamespace's compare-and-swap is FailedPrecondition for TWO
			// unrelated reasons — a stale etag, and this engine having no
			// namespace registry at all (internal/api/namespaces.go's own
			// `registry()`) — and only the first is fixed by "reload and
			// re-apply". isStaleEtagConflict tells them apart by the server's
			// own words (see $lib/adminErrors.ts): a FailedPrecondition that
			// is NOT a stale etag is shown as an ordinary write error, with no
			// conflict-recovery button that would just repeat the failure.
			if (isStaleEtagConflict(err)) {
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
	// re-applies" (UpdateNamespace's own doc comment). The FORM's own
	// fields (description/pool) are left exactly as the user typed them —
	// that is the edit being re-applied. Everything this form does NOT
	// expose is instead refreshed from this fresh read, not just the etag:
	// the earlier version of this fix moved the concurrency token forward
	// while replaying `editLabels` captured at the ORIGINAL open, so a
	// concurrent label change made underneath this edit (the same kind of
	// out-of-band write the conflict itself proves happened) was silently
	// overwritten by the retry. Labels are the one such field this page
	// carries today; a future hidden field belongs here too.
	async function reloadEtagAndRetry() {
		editBusy = true;
		editServerError = '';
		try {
			const res = await orkstack(fetch).namespaces.getNamespace({ name: editingName });
			editEtag = res.namespace?.etag ?? '';
			editLabels = { ...res.namespace?.labels };
			editConflict = false;
			await submitEdit();
		} catch (err) {
			// The reread itself failed — a NotFound because the namespace was
			// deleted out from under this conflict, or any other error. Either
			// way `editConflict` must NOT survive: the "reload and re-apply"
			// panel promises a namespace to reload, and pointing that button
			// at one that may no longer exist is worse than the ordinary
			// error path below (`{:else if editServerError}`).
			editConflict = false;
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
			// this area must not cause. Reset to `default`,
			// which always resolves (AGENTS.md §7), rather than leaving a
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

<p class="mb-4 text-sm text-fg-muted" data-testid="namespaces-scope-caption">
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
	<section class="mb-8 rounded-lg border border-border bg-surface p-5 shadow-xs">
		<h2 class="mb-3 text-sm font-semibold text-fg">Create a namespace</h2>
		<form
			class="grid gap-3 sm:grid-cols-2"
			onsubmit={(e) => {
				e.preventDefault();
				void submitCreate();
			}}
		>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="font-medium text-fg">Name</span>
				<input
					data-testid="ns-create-name"
					bind:value={createName}
					placeholder="payments"
					class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
				/>
				{#if createErrors.name}
					<span class="text-xs text-danger-700" data-testid="ns-create-error-name"
						>{createErrors.name}</span
					>
				{/if}
			</label>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="font-medium text-fg">Description</span>
				<input
					data-testid="ns-create-description"
					bind:value={createDescription}
					class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
				/>
			</label>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="font-medium text-fg">Default pool (blank = {DEFAULT_POOL_NAME})</span>
				<input
					data-testid="ns-create-default-pool"
					bind:value={createDefaultPool}
					placeholder={DEFAULT_POOL_NAME}
					class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
				/>
				{#if createErrors.defaultPool}
					<span class="text-xs text-danger-700" data-testid="ns-create-error-default-pool"
						>{createErrors.defaultPool}</span
					>
				{/if}
			</label>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="font-medium text-fg">Allowed pools (comma-separated, blank = unrestricted)</span>
				<input
					data-testid="ns-create-allowed-pools"
					bind:value={createAllowedPools}
					placeholder="cpu, gpu"
					class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
				/>
				{#if createErrors.allowedPools}
					<span class="text-xs text-danger-700" data-testid="ns-create-error-allowed-pools"
						>{createErrors.allowedPools}</span
					>
				{/if}
			</label>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="font-medium text-fg">Run retention (hours, blank = inherit)</span>
				<input
					data-testid="ns-create-run-retention"
					bind:value={createRunRetentionHours}
					inputmode="decimal"
					placeholder="inherit"
					class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
				/>
				<span class="text-xs text-fg-muted">Blank inherits the deployment maximum.</span>
				{#if createErrors.runRetention}
					<span class="text-xs text-danger-700" data-testid="ns-create-error-run-retention"
						>{createErrors.runRetention}</span
					>
				{/if}
			</label>
			<div class="sm:col-span-2">
				<button
					type="submit"
					data-testid="ns-create-submit"
					disabled={createBusy}
					class="inline-flex h-9 items-center justify-center gap-1.5 rounded-md bg-brand-600 px-3.5 text-sm font-medium text-white shadow-xs transition-colors duration-150 hover:bg-brand-700 active:bg-brand-800 disabled:pointer-events-none disabled:opacity-50"
				>
					{createBusy ? 'Creating…' : 'Create namespace'}
				</button>
			</div>
		</form>
		{#if createServerError}
			<p
				class="mt-3 rounded-md border border-danger-200 bg-danger-50 px-3 py-2 text-sm text-danger-700"
				data-testid="ns-create-server-error"
			>
				{createServerError}
			</p>
		{/if}
		{#if createSuccess}
			<p class="mt-3 text-sm text-succeeded-fg" data-testid="ns-create-success">{createSuccess}</p>
		{/if}
	</section>

	{#if busy}
		<p class="text-sm text-fg-muted" data-testid="namespaces-loading">Loading namespaces…</p>
	{:else}
		{#if data.namespacesList.length <= 1}
			<p class="mb-3 text-sm text-fg-muted" data-testid="namespaces-empty">
				No namespaces beyond <code>default</code> yet — it is undeletable
				below because every orkflow that names no namespace is in it. Create one above.
			</p>
		{/if}
		<div class="overflow-x-auto rounded-lg border border-border bg-surface shadow-xs">
			<table class="w-full min-w-[720px] border-collapse text-left text-sm" data-testid="namespace-list">
				<thead class="bg-surface-sunken">
					<tr>
						<th class="h-10 px-4 pl-5 text-xs font-medium text-fg-muted">Name</th>
						<th class="h-10 px-4 text-xs font-medium text-fg-muted">Description</th>
						<th class="h-10 px-4 text-xs font-medium text-fg-muted">Default pool</th>
						<th class="h-10 px-4 text-xs font-medium text-fg-muted">Allowed pools</th>
						<th class="h-10 px-4 text-xs font-medium text-fg-muted">Run retention</th>
						<th class="h-10 px-4 text-xs font-medium text-fg-muted">Updated</th>
						<th class="h-10 px-4 pr-5 text-xs font-medium text-fg-muted"></th>
					</tr>
				</thead>
				<tbody>
					{#each data.namespacesList as ns (ns.name)}
						<tr
							class="border-t border-border-subtle transition-colors duration-150 hover:bg-surface-hover"
							data-testid="namespace-row-{ns.name}"
						>
							<td class="h-11 px-4 pl-5 align-middle font-medium text-fg">{ns.name}</td>
							<td class="h-11 max-w-xs truncate px-4 align-middle text-fg-secondary"
								>{ns.description || '—'}</td
							>
							<td class="h-11 px-4 align-middle text-fg-secondary"
								>{ns.defaultPool || DEFAULT_POOL_NAME}</td
							>
							<td class="h-11 px-4 align-middle text-fg-secondary">
								{ns.allowedPools.length > 0 ? ns.allowedPools.join(', ') : 'unrestricted'}
							</td>
							<td class="h-11 px-4 align-middle text-fg-secondary" data-testid="namespace-run-retention-{ns.name}"
								>{runRetentionLabel(ns.runRetention)}</td
							>
							<td class="h-11 px-4 align-middle text-fg-muted tabular-nums"
								>{fmtTimestamp(ns.updatedAt)}</td
							>
							<td class="h-11 px-4 pr-5 align-middle">
								<div class="flex items-center justify-end gap-2">
									<button
										type="button"
										data-testid="namespace-edit-{ns.name}"
										onclick={() => openEdit(ns)}
										class="inline-flex h-8 items-center rounded-md border border-border bg-surface px-3 text-xs font-medium text-fg shadow-xs transition-colors duration-150 hover:border-border-strong hover:bg-surface-hover"
									>
										Edit
									</button>
									{#if ns.name === 'default'}
										<span
											class="text-xs text-fg-muted"
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
											class="inline-flex h-8 cursor-not-allowed items-center rounded-md border border-border px-3 text-xs text-fg-muted opacity-50"
										>
											Delete
										</button>
									{:else}
										<button
											type="button"
											data-testid="namespace-delete-{ns.name}"
											onclick={() => askToDelete(ns.name)}
											class="inline-flex h-8 items-center rounded-md border border-danger-200 bg-surface px-3 text-xs font-medium text-danger-700 shadow-xs transition-colors duration-150 hover:border-danger-300 hover:bg-danger-50"
										>
											Delete
										</button>
									{/if}
								</div>
							</td>
						</tr>
						{#if editingName === ns.name}
							<tr>
								<td colspan="7" class="bg-surface-sunken px-4 py-4">
									<form
										data-testid="namespace-edit-form-{ns.name}"
										class="grid gap-3 sm:grid-cols-2"
										onsubmit={(e) => {
											e.preventDefault();
											void submitEdit();
										}}
									>
										<label class="flex flex-col gap-1.5 text-sm">
											<span class="font-medium text-fg">Description</span>
											<input
												data-testid="ns-edit-description-{ns.name}"
												bind:value={editDescription}
												class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
											/>
										</label>
										<label class="flex flex-col gap-1.5 text-sm">
											<span class="font-medium text-fg">Default pool</span>
											<input
												data-testid="ns-edit-default-pool-{ns.name}"
												bind:value={editDefaultPool}
												class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
											/>
											{#if editErrors.defaultPool}
												<span class="text-xs text-danger-700">{editErrors.defaultPool}</span>
											{/if}
										</label>
										<label class="flex flex-col gap-1.5 text-sm sm:col-span-2">
											<span class="font-medium text-fg">Allowed pools (comma-separated)</span>
											<input
												data-testid="ns-edit-allowed-pools-{ns.name}"
												bind:value={editAllowedPools}
												class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
											/>
											{#if editErrors.allowedPools}
												<span class="text-xs text-danger-700">{editErrors.allowedPools}</span>
											{/if}
										</label>
										<label class="flex flex-col gap-1.5 text-sm">
											<span class="font-medium text-fg">Run retention (hours, blank = inherit)</span>
											<input
												data-testid="ns-edit-run-retention-{ns.name}"
												bind:value={editRunRetentionHours}
												inputmode="decimal"
												placeholder="inherit"
												class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
											/>
											{#if editErrors.runRetention}
												<span class="text-xs text-danger-700">{editErrors.runRetention}</span>
											{/if}
										</label>
										<div class="flex items-center gap-2 sm:col-span-2">
											<button
												type="submit"
												data-testid="ns-edit-save-{ns.name}"
												disabled={editBusy}
												class="inline-flex h-8 items-center rounded-md bg-brand-600 px-3 text-xs font-medium text-white shadow-xs transition-colors duration-150 hover:bg-brand-700 disabled:pointer-events-none disabled:opacity-50"
											>
												{editBusy ? 'Saving…' : 'Save'}
											</button>
											<button
												type="button"
												data-testid="ns-edit-cancel-{ns.name}"
												disabled={editBusy}
												onclick={closeEdit}
												class="inline-flex h-8 items-center rounded-md border border-border bg-surface px-3 text-xs font-medium text-fg shadow-xs transition-colors duration-150 hover:bg-surface-hover disabled:pointer-events-none disabled:opacity-50"
											>
												Cancel
											</button>
										</div>
									</form>
									{#if editConflict}
										<div
											class="mt-3 rounded-md border border-warning-200 bg-warning-50 px-3 py-2 text-sm text-warning-800"
											data-testid="namespace-edit-conflict-{ns.name}"
										>
											<p>{editServerError}</p>
											<p class="mt-1 text-xs text-warning-700">
												Namespace {ns.name} changed since it was read — reload and re-apply.
											</p>
											<button
												type="button"
												data-testid="namespace-edit-conflict-retry-{ns.name}"
												onclick={reloadEtagAndRetry}
												disabled={editBusy}
												class="mt-2 inline-flex h-7 items-center rounded-md border border-warning-300 bg-surface px-3 text-xs font-medium text-warning-800 shadow-xs hover:bg-warning-50 disabled:pointer-events-none disabled:opacity-50"
											>
												Reload latest and re-apply my edit
											</button>
										</div>
									{:else if editServerError}
										<p
											class="mt-3 rounded-md border border-danger-200 bg-danger-50 px-3 py-2 text-sm text-danger-700"
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
		destructive
		onaccept={confirmDelete}
		oncancel={() => {
			pendingDelete = '';
			deleteError = '';
		}}
	>
		{#snippet body()}
			<p>
				This removes the registry entry for <span class="font-medium text-fg">{pendingDelete}</span
				>. It is refused while any orkflow is still stored in it.
			</p>
		{/snippet}
	</ConfirmDialog>
{/if}
