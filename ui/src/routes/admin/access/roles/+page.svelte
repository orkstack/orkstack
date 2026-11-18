<script lang="ts">
	// Access → Roles (UX35): built-in roles read only, each with its
	// permissions; custom roles made, changed and deleted with a picker of
	// the catalogue grouped by resource, each permission saying what it
	// allows and where it can be bound.
	import { invalidateAll } from '$app/navigation';
	import { create } from '@bufbuild/protobuf';
	import ConfirmDialog from '#lib/components/ConfirmDialog.svelte';
	import Dialog from '#lib/components/Dialog.svelte';
	import ErrorPanel from '#lib/components/ErrorPanel.svelte';
	import Icon from '#lib/components/Icon.svelte';
	import SheetHeader from '#lib/components/SheetHeader.svelte';
	import AccessTabs from '../AccessTabs.svelte';
	import { errorMessage } from '#lib/client.ts';
	import { accessClient } from '#lib/clients/access.ts';
	import { RoleSchema, type Role } from '#lib/gen/orkstack/v1/access_pb.ts';
	import { groupByResource, whereWords } from '#lib/accessRoles.ts';
	import { can, missing } from '#lib/permissions.ts';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();
	const doc = $derived(data.access?.document);
	const canWrite = $derived(can(data.grants, 'access.write'));
	const groups = $derived(groupByResource(data.permissions));
	const allows = $derived(new Map(data.permissions.map((p) => [p.name, p.allows])));

	let editing = $state<{ creating: boolean; name: string; description: string; chosen: Set<string> } | null>(null);
	let busy = $state(false);
	let error = $state('');
	let deleting = $state<Role | null>(null);

	function openCreate() {
		editing = { creating: true, name: '', description: '', chosen: new Set() };
		error = '';
	}
	function openEdit(r: Role) {
		editing = { creating: false, name: r.name, description: r.description, chosen: new Set(r.permissions) };
		error = '';
	}
	function toggle(name: string, on: boolean) {
		if (!editing) return;
		const next = new Set(editing.chosen);
		if (on) next.add(name);
		else next.delete(name);
		editing.chosen = next;
	}
	async function save() {
		if (!editing || !doc) return;
		busy = true;
		error = '';
		const role = create(RoleSchema, {
			name: editing.name.trim(),
			description: editing.description.trim(),
			permissions: data.permissions.map((p) => p.name).filter((n) => editing!.chosen.has(n))
		});
		try {
			if (editing.creating) await accessClient(fetch).createRole({ role, etag: doc.etag });
			else await accessClient(fetch).updateRole({ role, etag: doc.etag });
			editing = null;
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
			await accessClient(fetch).deleteRole({ name: deleting.name, etag: doc.etag });
			deleting = null;
			await invalidateAll();
		} catch (err) {
			error = errorMessage(err);
		} finally {
			busy = false;
		}
	}
</script>

<svelte:head><title>Roles · Access · OrkStack</title></svelte:head>

<SheetHeader title="Roles" testid="roles-title" commands={[{ label: 'List the roles', command: 'orkctl admin role list' }]}>
	{#snippet actions()}
		<button type="button" class="btn btn-primary" onclick={() => canWrite && openCreate()} aria-disabled={!canWrite} aria-describedby={canWrite ? undefined : 'roles-write-reason'} disabled={!doc} data-testid="role-new">New role</button>
		{#if !canWrite}<p id="roles-write-reason" class="w-full text-sm text-ink-3">{missing('access.write')}</p>{/if}
	{/snippet}
</SheetHeader>
<AccessTabs />

{#if data.accessError || data.permissionsError}
	<ErrorPanel message={data.accessError ?? data.permissionsError ?? ''} subject="the roles" testid="roles-error" onretry={() => invalidateAll()} />
{:else if doc}
	<div class="grid gap-4 lg:grid-cols-2">
		{#each [...doc.builtInRoles, ...doc.roles] as r (r.name)}
			<section class="card" aria-labelledby="role-{r.name}" data-testid="role-{r.name}">
				<div class="card-head">
					<h2 id="role-{r.name}" class="font-mono">{r.name}</h2>
					<div class="card-end flex items-center gap-2">
						{#if r.builtIn}
							<span class="stamp stamp-sm pill-plain">Built in</span>
						{:else if canWrite}
							<button type="button" class="btn btn-sm" onclick={() => openEdit(r)} data-testid="role-edit-{r.name}">Edit</button>
							<button type="button" class="btn btn-sm" onclick={() => { deleting = r; error = ''; }} data-testid="role-delete-{r.name}"><Icon name="trash" />Delete</button>
						{/if}
					</div>
				</div>
				<div class="px-4 pb-4">
					{#if r.description}<p class="mb-2 text-ink-2">{r.description}</p>{/if}
					<ul class="flex flex-wrap gap-1.5">
						{#each r.permissions as p (p)}<li class="tag font-mono" data-tip={allows.get(p) ?? ''}>{p}</li>{/each}
					</ul>
				</div>
			</section>
		{/each}
	</div>
{/if}

{#if editing}
	<Dialog title={editing.creating ? 'New role' : `Edit ${editing.name}`} testid="role-dialog" onclose={() => (editing = null)} widthClass="max-w-3xl" glyph="apply">
		{#snippet body()}
			<form class="flex flex-col gap-4" onsubmit={(e) => { e.preventDefault(); save(); }}>
				<label class="flex flex-col gap-1">
					<span class="text-sm font-semibold text-ink-2">Name</span>
					<input class="field font-mono" bind:value={editing!.name} required readonly={!editing!.creating} data-testid="role-name" />
				</label>
				<label class="flex flex-col gap-1">
					<span class="text-sm font-semibold text-ink-2">What it is for</span>
					<input class="field" bind:value={editing!.description} data-testid="role-description" />
				</label>
				<fieldset class="flex max-h-[50dvh] flex-col gap-4 overflow-y-auto">
					<legend class="text-sm font-semibold text-ink-2">Permissions <span class="tag">{editing!.chosen.size}</span></legend>
					{#each groups as g (g.resource)}
						<div>
							<p class="mb-1 font-semibold">{g.resource}</p>
							<ul class="flex flex-col gap-1">
								{#each g.permissions as p (p.name)}
									<li>
										<label class="flex items-start gap-2">
											<input type="checkbox" checked={editing!.chosen.has(p.name)} onchange={(e) => toggle(p.name, e.currentTarget.checked)} data-testid="role-perm-{p.name}" />
											<span><span class="font-mono">{p.name}</span> <span class="text-ink-2">{p.allows}</span> <span class="text-sm text-ink-3">{whereWords(p)}</span></span>
										</label>
									</li>
								{/each}
							</ul>
						</div>
					{/each}
				</fieldset>
				{#if error}<p class="text-fail" role="alert" data-testid="role-error">{error}</p>{/if}
				<div class="flex justify-end gap-2">
					<button type="button" class="btn" onclick={() => (editing = null)}>Keep as it was</button>
					<button type="submit" class="btn btn-primary" disabled={busy} data-testid="role-save"><Icon name="apply" />{busy ? 'Saving…' : 'Save role'}</button>
				</div>
			</form>
		{/snippet}
	</Dialog>
{/if}

{#if deleting}
	<ConfirmDialog title="Delete {deleting.name}?" testid="role-delete-dialog" acceptLabel="Delete role" cancelLabel="Keep it" acceptTestid="role-delete-confirm" cancelTestid="role-delete-cancel" destructive glyph="trash" busy={busy} error={error} onaccept={confirmDelete} oncancel={() => (deleting = null)}>
		{#snippet body()}<p>A role a binding still names cannot be deleted; delete or change those bindings first.</p>{/snippet}
	</ConfirmDialog>
{/if}
