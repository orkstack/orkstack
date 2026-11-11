<script lang="ts">
	// The Triggers tab: who outside this deployment may start THIS orkflow.
	//
	// It is the inbound mirror of the Admin Webhooks page, not a variant of it.
	// A webhook is OUTBOUND and deployment-wide — OrkStack calls a subscriber
	// when any run changes state. A trigger is INBOUND and bound to one
	// orkflow: a caller posts to /v1/triggers/{id} with the trigger's bearer
	// token and one run of this orkflow starts. The namespace, orkflow and
	// version come from the STORED trigger and never from the request
	// (docs/ORKFLOW_TRIGGERS.md), which is the fact the scope statement below
	// exists to say out loud rather than leave a reader to assume.
	//
	// It is a sibling of OrkflowVersions and OrkflowEditor and works the same
	// way: the loader reads the list, this component writes and then asks the
	// loader to read it again. Nothing here is optimistic — a row appears
	// because the store answered with it, not because a create returned.
	import { shortVersion } from '#lib/version.ts';
	import { goto, invalidateAll } from '$app/navigation';
	import { page } from '$app/state';
	import { untrack } from 'svelte';
	import { EDIT_PARAM, closedHref, editHref } from '#lib/editParam.ts';
	import CliEquivalent from '#lib/components/CliEquivalent.svelte';
	import ConfirmDialog from '#lib/components/ConfirmDialog.svelte';
	import Icon from '#lib/components/Icon.svelte';
	import Dialog from '#lib/components/Dialog.svelte';
	import ErrorPanel from '#lib/components/ErrorPanel.svelte';
	import Select from '#lib/components/Select.svelte';
	import { errorMessage } from '#lib/client.ts';
	import { triggerClient } from '#lib/clients/triggers.ts';
	import type { OrkflowVersion } from '#lib/gen/orkstack/v1/orkflow_service_pb.ts';
import type { Trigger } from '#lib/gen/orkstack/v1/trigger_service_pb.ts';
	import { namespaceLabel } from '#lib/namespace.ts';
	import { shellQuote } from '#lib/runDetail.ts';
	import { fmtTimestamp } from '#lib/status.ts';
	import { triggerUpdateCli } from '#lib/automation.ts';
	import { SCOPE_SLUG_PATTERN } from '#lib/webhooks.ts';

	interface Props {
		name: string;
		namespace: string;
		versions: OrkflowVersion[];
		currentVersion: string;
		triggers: Trigger[];
		error: string | null;
		/** The origin a caller posts to — this page's own; see +page.svelte. */
		invokeOrigin: string;
	}

	let { name, namespace, versions, currentVersion, triggers, error, invokeOrigin }: Props =
		$props();

	// CURRENT is the sentinel for "pin nothing", spelled the way RunSubmitPanel
	// already spells it: an empty option value reads as an unset control, and
	// "pinned" versus "follows the pointer" is the one choice on this form a
	// reader must not have to guess at. It becomes the empty wire value at
	// submit, which is what an unpinned trigger stores.
	const CURRENT = '__current__';

	let selectedVersion = $state(CURRENT);
	let description = $state('');
	// The trigger's optional handle: how an automation document addresses it
	// (docs/AUTOMATION.md). The pattern only catches a typo; the server is the
	// authority on the slug rule and on uniqueness.
	let triggerName = $state('');
	// The edit dialog: name, pin and description change in place; the id, URL
	// and token never do (UpdateTrigger).
	let editing = $state<Trigger | null>(null);
	let editName = $state('');
	let editVersion = $state(CURRENT);
	let editDescription = $state('');
	let editError = $state('');
	let saving = $state(false);
	let creating = $state(false);
	let createError = $state('');
	// The one-time disclosure. Held only in this component's memory: the store
	// keeps a salted digest, so nothing can rebuild this panel from a listing.
	let shownTrigger = $state<Trigger | null>(null);
	let shownSecret = $state('');
	let deleting = $state('');
	let deleteError = $state('');
	let removing = $state(false);
	let reloading = $state(false);

	// Which version an unpinned trigger resolves to, named the way the Versions
	// tab names it: the engine's own is_current answer, with the loader's
	// current pointer preferred because that is the field the tab is about.
	const currentLabel = $derived(
		shortVersion(currentVersion || versions.find((entry) => entry.isCurrent)?.version || '')
	);
	const versionOptions = $derived([
		{
			value: CURRENT,
			label: currentLabel
				? `Current (${currentLabel}) — resolved at each invoke`
				: 'Current — resolved at each invoke'
		},
		...versions.map((entry) => ({
			value: entry.version,
			label: `${shortVersion(entry.version)}${entry.isCurrent ? ' — current' : ''}${entry.description ? ` — ${entry.description}` : ''}`
		}))
	]);
	const chosenVersion = $derived(selectedVersion === CURRENT ? '' : selectedVersion);

	// An empty pin is the current pointer at invoke time, not "no version": a
	// cell reading "—" would suggest the trigger starts nothing.
	const versionLabel = (version: string) => shortVersion(version) || 'current';
	const scopeOf = (trigger: Trigger) =>
		`Runs ${trigger.orkflowName}@${versionLabel(trigger.orkflowVersion)}`;

	// The invoke route is plain HTTP on the engine's API port, not a Connect
	// method, so it is written out here rather than derived from a generated
	// client that never sees it.
	const invokeUrl = (id: string) => `${invokeOrigin}/v1/triggers/${id}`;

	function curl(id: string, authorization: string): string {
		return [
			`curl -X POST ${shellQuote(invokeUrl(id))} \\`,
			`  -H ${authorization} \\`,
			`  -H 'Content-Type: application/json' \\`,
			`  -d '{}'`,
			// Optional, and a comment so the command stays runnable as copied:
			// the header records a correlation reference on the started run.
			`# optional: -H 'Ork-Origin-Reference: <your CI job URL>'`
		].join('\n');
	}

	// A real token is single-quoted, which is what keeps whatever it contains
	// from being read as shell syntax. The placeholder is DOUBLE quoted for the
	// opposite reason: single quotes would also stop $ORKSTACK_TRIGGER_TOKEN
	// from expanding, leaving a command that copies cleanly and then 401s.
	const curlWithToken = (id: string, token: string) =>
		curl(id, shellQuote(`Authorization: Bearer ${token}`));
	const curlWithPlaceholder = (id: string) =>
		curl(id, '"Authorization: Bearer $ORKSTACK_TRIGGER_TOKEN"');

	async function create() {
		creating = true;
		createError = '';
		shownSecret = '';
		shownTrigger = null;
		try {
			const response = await triggerClient(fetch).createTrigger({
				namespace,
				orkflowName: name,
				orkflowVersion: chosenVersion,
				description,
				name: triggerName.trim()
			});
			shownTrigger = response.trigger ?? null;
			shownSecret = response.secret;
			description = '';
			triggerName = '';
			selectedVersion = CURRENT;
			await invalidateAll();
		} catch (err) {
			// The draft stays in the form: a refused create (an orkflow or version
			// the engine will not bind to) is re-submitted after a fix, not retyped.
			createError = errorMessage(err);
		} finally {
			creating = false;
		}
	}

	async function remove() {
		if (!deleting) return;
		removing = true;
		deleteError = '';
		const removed = deleting;
		try {
			await triggerClient(fetch).deleteTrigger({ id: removed, namespace });
			// A deleted trigger's token authenticates nothing, so a secret panel
			// still offering that id would be an invitation to run a command that
			// now answers 404.
			if (shownTrigger?.id === removed) {
				shownTrigger = null;
				shownSecret = '';
			}
			deleting = '';
			await invalidateAll();
		} catch (err) {
			deleteError = errorMessage(err);
		} finally {
			removing = false;
		}
	}

	function startEdit(trigger: Trigger) {
		editing = trigger;
		editName = trigger.name;
		editVersion = trigger.orkflowVersion || CURRENT;
		editDescription = trigger.description;
		editError = '';
	}

	const editPin = $derived(editVersion === CURRENT ? '' : editVersion);

	// A trigger's id links to its edit, and the address opens it.
	function closeEdit() {
		void goto(closedHref(page.url), { reset: false });
	}
	$effect(() => {
		const id = page.url.searchParams.get(EDIT_PARAM);
		const open = untrack(() => editing);
		if (!id) {
			if (open) editing = null;
			return;
		}
		if (open?.id === id) return;
		const listed = triggers.find((trigger) => trigger.id === id);
		if (listed) untrack(() => startEdit(listed));
	});
	const editMissing = $derived.by(() => {
		const id = page.url.searchParams.get(EDIT_PARAM);
		return id && !error && !triggers.some((trigger) => trigger.id === id) ? `Trigger ${id} is not one of ${name}'s.` : '';
	});

	async function saveEdit() {
		if (!editing) return;
		saving = true;
		editError = '';
		try {
			await triggerClient(fetch).updateTrigger({
				id: editing.id,
				namespace,
				name: editName.trim(),
				orkflowVersion: editPin,
				description: editDescription
			});
			await goto(closedHref(page.url), { invalidateAll: true, reset: false });
		} catch (err) {
			editError = errorMessage(err);
		} finally {
			saving = false;
		}
	}

	async function reload() {
		reloading = true;
		try {
			await invalidateAll();
		} finally {
			// The view never stays on "Retrying…": either the list arrived, or the
			// loader's error is rendered again with the same button.
			reloading = false;
		}
	}
</script>

<!--
	The scope, first, in the place and voice the Admin Webhooks page states its
	own. That page says "deployment-wide" because a reader would otherwise think
	the namespace picker filters it; this one says the opposite thing for the
	opposite reason — a token reaches ONE orkflow in ONE namespace and nothing
	else. The VERSION is deliberately absent from this sentence: each trigger
	pins its own at creation, so one blanket claim here would be false for every
	row that chose differently. The pin is stated per row instead.
-->
<p class="mb-2 max-w-prose text-sm text-ink-secondary" data-testid="triggers-scope">
	A trigger lets an outside caller start one run of
	<span class="font-medium text-ink">{name}</span>
	over plain HTTP, by posting to
	<code class="font-mono text-xs">/v1/triggers/&lt;id&gt;</code> with its bearer token. The Orkflow,
	the namespace and the version come from the trigger itself and never from the request, so a token
	cannot be redirected at anything else. Each trigger pins its own version when it is created — or
	follows the current pointer — and says which on its row below.
</p>
<p class="mb-5 max-w-prose text-xs text-ink-muted" data-testid="triggers-origin-note">
	The commands below address <span class="font-mono">{invokeOrigin}</span>, the address this page
	was opened at. If the engine's API is published somewhere else, invoke that address instead.
</p>

{#if editMissing}<div class="mb-3"><ErrorPanel testid="trigger-edit-missing" message={editMissing} /></div>{/if}

{#if error}
	<ErrorPanel
		testid="triggers-error"
		retryTestid="triggers-retry"
		subject="the triggers of {name}"
		message={error}
		busy={reloading}
		onretry={reload}
	/>
{:else if triggers.length === 0}
	<p data-testid="triggers-empty" class="text-sm text-ink-muted">
		No triggers start {name}. Create one below to give an external caller a way in.
	</p>
{:else}
	<!--
		Cards rather than a table: each row carries a CliEquivalent, whose panel
		is absolutely positioned, and the horizontally scrollable table wrapper
		this page's other lists use would clip it.
	-->
	<ul class="border-t-2 border-ink" data-testid="trigger-list">
		{#each triggers as trigger (trigger.id)}
			<li
				data-testid="trigger-{trigger.id}"
				class="border-b border-rule py-3"
			>
				<div class="flex items-start justify-between gap-4">
					<div class="min-w-0">
						<a class="link font-mono text-sm font-medium break-all" data-testid="edit-trigger-{trigger.id}" data-sveltekit-reset="false" href={editHref(page.url, trigger.id)}>{trigger.id}</a>
						<p class="mt-1 text-sm" data-testid="trigger-name-{trigger.id}">
							{#if trigger.name}<span class="font-mono font-medium text-ink">{trigger.name}</span
								>{:else}<span class="text-ink-muted">unnamed</span>{/if}
						</p>
						<p class="mt-1 text-sm text-ink-secondary" data-testid="trigger-scope-{trigger.id}">
							{scopeOf(trigger)}
						</p>
						{#if trigger.description}
							<p class="mt-1 text-sm text-ink-secondary">{trigger.description}</p>
						{/if}
						<p class="mt-1 text-xs text-ink-muted">Created {fmtTimestamp(trigger.createdAt)}</p>
					</div>
					<div class="flex shrink-0 items-center gap-2">
						<CliEquivalent
							command={curlWithPlaceholder(trigger.id)}
							testid="trigger-cli-{trigger.id}"
							label="Invocation of {trigger.id}"
						/>
						<button
							type="button"
							data-testid="delete-trigger-{trigger.id}"
							onclick={() => {
								deleting = trigger.id;
								deleteError = '';
							}}
							class="btn btn-sm"
						>
							<Icon name="trash" />Delete
						</button>
					</div>
				</div>
			</li>
		{/each}
	</ul>
{/if}

<section class="region mt-8 pt-3">
	<h2 class="text-md font-semibold text-ink">Create trigger</h2>
	<form
		class="mt-3 max-w-3xl space-y-4"
		data-testid="trigger-create-form"
		onsubmit={(event) => {
			event.preventDefault();
			void create();
		}}
	>
		<div class="flex flex-col gap-1.5">
			<label for="trigger-create-version" class="text-sm font-medium text-ink">Version</label>
			<Select
				id="trigger-create-version"
				testid="trigger-create-version"
				value={selectedVersion}
				options={versionOptions}
				disabled={creating}
				onchange={(next) => (selectedVersion = next)}
			/>
			<p class="text-xs text-ink-muted" data-testid="trigger-create-version-mode">
				{chosenVersion
					? `Pinned to ${chosenVersion}. It keeps starting that version after the current pointer moves.`
					: `Unpinned. Each invoke starts whichever version is current${currentLabel ? ` (${currentLabel} now)` : ''}.`}
			</p>
		</div>

		<div class="flex flex-col gap-1.5">
			<label for="trigger-create-name" class="text-sm font-medium text-ink">
				Name <span class="font-normal text-ink-muted">(optional — how an automation document refers to it)</span>
			</label>
			<input
				id="trigger-create-name"
				data-testid="trigger-create-name"
				bind:value={triggerName}
				pattern={SCOPE_SLUG_PATTERN}
				maxlength={200}
				data-tip="lowercase letters, digits and hyphens, starting with a letter"
				placeholder="github-deploy"
				class="field h-9 w-full px-3 text-sm"
			/>
		</div>

		<div class="flex flex-col gap-1.5">
			<label for="trigger-create-description" class="text-sm font-medium text-ink">
				Description <span class="font-normal text-ink-muted">(optional)</span>
			</label>
			<input
				id="trigger-create-description"
				data-testid="trigger-create-description"
				bind:value={description}
				placeholder="what calls this — nightly kickoff, deploy hook, …"
				class="field h-9 w-full px-3 text-sm"
			/>
		</div>

		{#if createError}
			<ErrorPanel testid="trigger-create-error" message={createError} />
		{/if}

		<button
			type="submit"
			data-testid="trigger-create-submit"
			disabled={creating}
			class="btn border-ink"
		>
			{creating ? 'Creating…' : 'Create trigger'}
		</button>
	</form>

	{#if shownSecret}
		<div
			data-testid="trigger-secret-once"
			data-cli-surface="panel"
			class="note mt-4 max-w-3xl border-ink p-3 text-sm"
		>
			<p class="font-semibold">Copy this bearer token now. It will not be shown again.</p>
			<p class="mt-1 text-xs text-ink-secondary">
				Only a salted digest of it is stored, so no listing and no support bundle can disclose it.
				Rotating a token is delete then create.
			</p>
			<code data-testid="trigger-secret-value" class="mt-2 block break-all font-mono text-xs"
				>{shownSecret}</code
			>
			{#if shownTrigger}
				<div class="mt-3 flex items-center justify-between gap-3">
					<p class="text-xs">
						<span data-testid="trigger-secret-id" class="font-mono">{shownTrigger.id}</span>
						<span class="block text-ink-secondary">{scopeOf(shownTrigger)}</span>
					</p>
					<CliEquivalent
						command={curlWithToken(shownTrigger.id, shownSecret)}
						testid="trigger-secret-cli"
						label="Runnable invocation of this trigger, with its token"
					/>
				</div>
			{/if}
		</div>
	{/if}
</section>

{#if deleting}
	<ConfirmDialog
		testid="delete-trigger-confirm"
		title="Delete trigger {deleting}?"
		acceptLabel="Delete"
		glyph="trash"
		acceptTestid="delete-trigger-accept"
		cancelTestid="delete-trigger-cancel"
		busy={removing}
		error={deleteError}
		destructive
		onaccept={remove}
		oncancel={() => {
			deleting = '';
			deleteError = '';
		}}
	>
		{#snippet body()}
			<p>
				A later invoke of <span class="font-mono text-ink">{deleting}</span> returns 404 on every
				replica, and its bearer token authenticates nothing afterwards.
			</p>
			<p class="mt-2 text-ink-muted">
				Runs it already started are untouched. There is no rotation: a caller that still needs a way
				in gets a new trigger, with a new id and a new token.
			</p>
		{/snippet}
	</ConfirmDialog>
{/if}

{#if editing}
	<Dialog title="Edit trigger {editing.id}" testid="trigger-edit-dialog" onclose={closeEdit} widthClass="max-w-xl">
		{#snippet body()}
			<p class="mb-3 text-sm text-ink-secondary">
				Editing keeps this trigger's URL and token. Its Orkflow cannot change.
			</p>
			<form
				class="space-y-3"
				onsubmit={(event) => {
					event.preventDefault();
					void saveEdit();
				}}
			>
				<div class="flex flex-col gap-1.5">
					<label for="trigger-edit-name" class="text-sm font-medium text-ink">
						Name <span class="font-normal text-ink-muted">(empty makes it unnamed)</span>
					</label>
					<input
						id="trigger-edit-name"
						data-testid="trigger-edit-name"
						bind:value={editName}
						pattern={SCOPE_SLUG_PATTERN}
						maxlength={200}
						data-tip="lowercase letters, digits and hyphens, starting with a letter"
						class="field h-9 w-full px-3 text-sm"
					/>
				</div>
				<div class="flex flex-col gap-1.5">
					<label for="trigger-edit-version" class="text-sm font-medium text-ink">Version</label>
					<Select
						id="trigger-edit-version"
						testid="trigger-edit-version"
						value={editVersion}
						options={versionOptions}
						disabled={saving}
						onchange={(next) => (editVersion = next)}
					/>
				</div>
				<div class="flex flex-col gap-1.5">
					<label for="trigger-edit-description" class="text-sm font-medium text-ink">Description</label>
					<input
						id="trigger-edit-description"
						data-testid="trigger-edit-description"
						bind:value={editDescription}
						class="field h-9 w-full px-3 text-sm"
					/>
				</div>
				{#if editError}
					<ErrorPanel testid="trigger-edit-error" message={editError} />
				{/if}
				<div class="flex items-center justify-between gap-3">
					<button
						type="submit"
						data-testid="trigger-edit-submit"
						disabled={saving}
						class="btn btn-primary"
					>
						{saving ? 'Saving…' : 'Save'}
					</button>
					<CliEquivalent
						command={triggerUpdateCli(editing?.id ?? "", namespaceLabel(namespace), {
							name: editName.trim(),
							version: editPin,
							description: editDescription
						})}
						testid="trigger-edit-cli"
					/>
				</div>
			</form>
		{/snippet}
	</Dialog>
{/if}
