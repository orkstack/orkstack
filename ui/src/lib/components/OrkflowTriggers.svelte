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
	import { invalidateAll } from '$app/navigation';
	import CliEquivalent from '$lib/components/CliEquivalent.svelte';
	import ConfirmDialog from '$lib/components/ConfirmDialog.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import Select from '$lib/components/Select.svelte';
	import { errorMessage, orkstack } from '$lib/client';
	import type { OrkflowVersion, Trigger } from '$lib/gen/orkstack/v1/service_pb';
	import { namespaceLabel } from '$lib/namespace';
	import { shellQuote } from '$lib/runDetail';
	import { fmtTimestamp } from '$lib/status';

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
		currentVersion || versions.find((entry) => entry.isCurrent)?.version || ''
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
			label: `${entry.version}${entry.isCurrent ? ' — current' : ''}${entry.description ? ` — ${entry.description}` : ''}`
		}))
	]);
	const chosenVersion = $derived(selectedVersion === CURRENT ? '' : selectedVersion);

	// An empty pin is the current pointer at invoke time, not "no version": a
	// cell reading "—" would suggest the trigger starts nothing.
	const versionLabel = (version: string) => version || 'current';
	const scopeOf = (trigger: Trigger) =>
		`Runs ${trigger.orkflowName}@${versionLabel(trigger.orkflowVersion)} in ${namespaceLabel(trigger.namespace)}`;

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
			const response = await orkstack(fetch).triggers.createTrigger({
				namespace,
				orkflowName: name,
				orkflowVersion: chosenVersion,
				description
			});
			shownTrigger = response.trigger ?? null;
			shownSecret = response.secret;
			description = '';
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
			await orkstack(fetch).triggers.deleteTrigger({ id: removed, namespace });
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
<p class="mb-2 text-sm text-fg-secondary" data-testid="triggers-scope">
	A trigger lets an outside caller start one run of
	<span class="font-medium text-fg">{name}</span>
	in namespace <span class="font-medium text-fg">{namespaceLabel(namespace)}</span> over plain
	HTTP, by posting to
	<code class="font-mono text-xs">/v1/triggers/&lt;id&gt;</code> with its bearer token. The orkflow,
	the namespace and the version come from the trigger itself and never from the request, so a token
	cannot be redirected at anything else. Each trigger pins its own version when it is created — or
	follows the current pointer — and says which on its row below.
</p>
<p class="mb-5 text-xs text-fg-muted" data-testid="triggers-origin-note">
	The commands below address <span class="font-mono">{invokeOrigin}</span>, the address this page
	was opened at. If the engine's API is published somewhere else, invoke that address instead.
</p>

<section class="mb-6 rounded-lg border border-border bg-surface p-4 shadow-xs">
	<h2 class="text-sm font-semibold text-fg">Create trigger</h2>
	<form
		class="mt-3 space-y-4"
		data-testid="trigger-create-form"
		onsubmit={(event) => {
			event.preventDefault();
			void create();
		}}
	>
		<div class="flex flex-col gap-1.5">
			<label for="trigger-create-version" class="text-sm font-medium text-fg">Version</label>
			<Select
				id="trigger-create-version"
				testid="trigger-create-version"
				value={selectedVersion}
				options={versionOptions}
				disabled={creating}
				onchange={(next) => (selectedVersion = next)}
			/>
			<p class="text-xs text-fg-muted" data-testid="trigger-create-version-mode">
				{chosenVersion
					? `Pinned to ${chosenVersion}. It keeps starting that version after the current pointer moves.`
					: `Unpinned. Each invoke starts whichever version is current${currentLabel ? ` (${currentLabel} now)` : ''}.`}
			</p>
		</div>

		<div class="flex flex-col gap-1.5">
			<label for="trigger-create-description" class="text-sm font-medium text-fg">
				Description <span class="font-normal text-fg-muted">(optional)</span>
			</label>
			<input
				id="trigger-create-description"
				data-testid="trigger-create-description"
				bind:value={description}
				placeholder="what calls this — nightly kickoff, deploy hook, …"
				class="h-9 w-full rounded-md border border-border bg-surface px-3 text-sm text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
			/>
		</div>

		{#if createError}
			<ErrorPanel testid="trigger-create-error" message={createError} />
		{/if}

		<button
			type="submit"
			data-testid="trigger-create-submit"
			disabled={creating}
			class="inline-flex h-9 items-center justify-center rounded-md bg-brand-600 px-3.5 text-sm font-medium text-white shadow-xs transition-colors duration-150 hover:bg-brand-700 active:bg-brand-800 focus-visible:ring-2 focus-visible:ring-brand-500/40 focus-visible:ring-offset-2 focus-visible:ring-offset-canvas focus-visible:outline-none disabled:pointer-events-none disabled:opacity-50"
		>
			{creating ? 'Creating…' : 'Create trigger'}
		</button>
	</form>

	{#if shownSecret}
		<div
			data-testid="trigger-secret-once"
			class="mt-4 rounded-md border border-warning-200 bg-warning-50 p-3 text-sm text-warning-800"
		>
			<p class="font-medium">Copy this bearer token now. It will not be shown again.</p>
			<p class="mt-1 text-xs">
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
						<span class="block text-warning-800/80">{scopeOf(shownTrigger)}</span>
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

{#if error}
	<ErrorPanel
		testid="triggers-error"
		retryTestid="triggers-retry"
		subject="the triggers of {name} in namespace {namespaceLabel(namespace)}"
		message={error}
		busy={reloading}
		onretry={reload}
	/>
{:else if triggers.length === 0}
	<p data-testid="triggers-empty" class="text-sm text-fg-muted">
		No triggers start {name} in namespace {namespaceLabel(namespace)}. Create one above to give an
		external caller a way in.
	</p>
{:else}
	<!--
		Cards rather than a table: each row carries a CliEquivalent, whose panel
		is absolutely positioned, and the horizontally scrollable table wrapper
		this page's other lists use would clip it.
	-->
	<ul class="space-y-3" data-testid="trigger-list">
		{#each triggers as trigger (trigger.id)}
			<li
				data-testid="trigger-{trigger.id}"
				class="rounded-lg border border-border bg-surface p-4 shadow-xs"
			>
				<div class="flex items-start justify-between gap-4">
					<div class="min-w-0">
						<p class="font-mono text-sm font-medium break-all text-fg">{trigger.id}</p>
						<p class="mt-1 text-sm text-fg-secondary" data-testid="trigger-scope-{trigger.id}">
							{scopeOf(trigger)}
						</p>
						{#if trigger.description}
							<p class="mt-1 text-sm text-fg-secondary">{trigger.description}</p>
						{/if}
						<p class="mt-1 text-xs text-fg-muted">Created {fmtTimestamp(trigger.createdAt)}</p>
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
							class="inline-flex h-8 items-center rounded-md border border-danger-200 bg-surface px-3 text-xs font-medium text-danger-700 shadow-xs transition-colors duration-150 hover:border-danger-300 hover:bg-danger-50 focus-visible:ring-2 focus-visible:ring-danger-500/40 focus-visible:ring-offset-2 focus-visible:ring-offset-canvas focus-visible:outline-none"
						>
							Delete
						</button>
					</div>
				</div>
			</li>
		{/each}
	</ul>
{/if}

{#if deleting}
	<ConfirmDialog
		testid="delete-trigger-confirm"
		title="Delete trigger {deleting}?"
		acceptLabel="Delete"
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
				A later invoke of <span class="font-mono text-fg">{deleting}</span> returns 404 on every
				replica, and its bearer token authenticates nothing afterwards.
			</p>
			<p class="mt-2 text-fg-muted">
				Runs it already started are untouched. There is no rotation: a caller that still needs a way
				in gets a new trigger, with a new id and a new token.
			</p>
		{/snippet}
	</ConfirmDialog>
{/if}
