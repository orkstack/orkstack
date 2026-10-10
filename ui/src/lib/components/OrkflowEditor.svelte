<script lang="ts">
	// The Edit tab: change ONE stored version's YAML and apply it as a new one.
	//
	// Three decisions are load-bearing here and none is cosmetic:
	//
	// 1. There is NO client-side validation. The compiler is the validator, and
	//    a second one in the browser would drift from it — telling somebody a
	//    document is fine that the engine then rejects, or worse the reverse.
	//    Every message in the error panel below is an ApplyOrkflow response.
	//
	// 2. Every apply carries TWO versions, because it states two facts
	//    (proto/orkstack/v1/service.proto, ApplyOrkflowRequest):
	//      base_version          — the stored version this edit was DERIVED
	//                              from. Any stored version is a valid base;
	//                              editing an old one forks forward from it.
	//      expected_head_version — the head this page OBSERVED when it fetched.
	//                              It is the concurrency token, and the only
	//                              thing a competing apply can invalidate.
	//
	// 3. A refusal never costs the user their work. A head that moved is
	//    REPORTED, with the diff of what arrived and a retarget that moves the
	//    token and leaves the text exactly as it was typed.
	import { untrack } from 'svelte';
	import { goto } from '$app/navigation';
	import { guardUnsaved } from '$lib/dirtyGuard';
	import { Code, ConnectError } from '@connectrpc/connect';
	import DiffView from '$lib/components/DiffView.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import { errorMessage, orkstack } from '$lib/client';
	import { ValidationSeverity, type ValidationIssue } from '$lib/gen/orkstack/v1/service_pb';
	import { namespaceLabel } from '$lib/namespace';

	interface Props {
		name: string;
		namespace: string;
		/** The stored YAML of the SELECTED version — what this edit is derived from. */
		sourceYaml: string;
		/** The selected version: this edit's provenance, sent as base_version. */
		baseVersion: string;
		/** The head at load: the concurrency token, sent as expected_head_version. */
		headVersion: string;
		/** Why the selected version or the head could not be read, if they could not. */
		error: string | null;
		/** Where to land after a successful apply. */
		versionsHref: string;
		onreload: () => Promise<void>;
	}

	let {
		name,
		namespace,
		sourceYaml,
		baseVersion,
		headVersion,
		error,
		versionsHref,
		onreload
	}: Props = $props();

	// Seeded from the first read that SUCCEEDS for a given version, whenever
	// that is — and re-seeded when the SELECTED version changes.
	//
	// It has to be seeded once per version and not again: a later reload of the
	// page data must not overwrite what somebody is typing — that is the loss
	// this whole tab exists to prevent. But "once" cannot mean "at
	// construction". When the first read FAILS this component is still built,
	// with `''` for both, and it keeps its place in the template, so a
	// SUCCESSFUL retry re-renders the same instance holding the same two empty
	// strings: an empty box over a base version it does not have, and an apply
	// that answers "reload before applying". That is a retry in name only.
	//
	// And "once, ever" cannot be it either. Selecting another version navigates
	// within this route, so the same instance is handed a new document; a seed
	// that closed for good would keep showing the old one under the new
	// version's name.
	//
	// So the seed waits for a document to exist, and closes over the version it
	// took — `seededFrom`, compared against the selection.
	let yaml = $state(untrack(() => sourceYaml));
	let cleanYaml = $state(untrack(() => sourceYaml));
	let base = $state(untrack(() => baseVersion));
	let expectedHead = $state(untrack(() => headVersion));
	let seededFrom = $state<string | null>(
		untrack(() => (error === null && baseVersion !== '' ? baseVersion : null))
	);

	let applying = $state(false);
	let validating = $state(false);
	let reloading = $state(false);
	let applyError = $state('');
	let warnings = $state<string[]>([]);
	let issues = $state<ValidationIssue[]>([]);
	let applied = $state<{ name: string; version: string } | null>(null);
	// The no-op report: an apply that stored nothing, said out loud. Silence
	// here would be indistinguishable from success, and navigating away would
	// be worse — it would show a version list nobody changed as the result of
	// an apply.
	let noop = $state('');

	// The stale-head report. It holds both tokens this editor sent, the head the
	// engine has now, and the diff between the two heads once asked for — the
	// "re-read + diff path" the design requires instead of a silent overwrite.
	let conflict = $state<{ base: string; expectedHead: string; message: string } | null>(null);
	let conflictHead = $state('');
	let conflictHeadError = $state('');
	let conflictDiff = $state<string | null>(null);
	let conflictDiffError = $state('');
	let conflictBusy = $state(false);

	// The seed itself. It lives below every piece of state it clears: selecting
	// another version starts a fresh edit, and a message about the previous one
	// left standing over the new document would describe neither.
	$effect(() => {
		// Tracked: the two things that say a real document has arrived, and the
		// version it belongs to.
		if (error !== null || baseVersion === '') return;
		if (untrack(() => seededFrom) === baseVersion) return;
		yaml = sourceYaml;
		base = baseVersion;
		cleanYaml = sourceYaml;
		expectedHead = headVersion;
		seededFrom = baseVersion;
		noop = '';
		applyError = '';
		warnings = [];
		conflict = null;
	});

	const client = () => orkstack(fetch);
	const dirty = $derived(yaml !== cleanYaml);

	guardUnsaved(() => dirty, 'Discard unsaved orkflow YAML?');

	async function validate(): Promise<boolean> {
		applyError = '';
		issues = [];
		if (!yaml.trim()) { applyError = 'Paste an orkflow YAML document first.'; return false; }
		const validatedYaml = yaml;
		const validatedNamespace = namespace;
		validating = true;
		try {
			const res = await client().orkflows.validateOrkflow({ yaml: validatedYaml, namespace: validatedNamespace });
			if (yaml !== validatedYaml || namespace !== validatedNamespace) return false;
			issues = res.issues;
			return !res.issues.some((issue) => issue.severity === ValidationSeverity.ERROR);
		} catch (err) {
			applyError = errorMessage(err);
			return false;
		} finally { validating = false; }
	}

	async function reload() {
		reloading = true;
		try {
			await onreload();
		} finally {
			reloading = false;
		}
	}

	async function apply() {
		applyError = '';
		warnings = [];
		applied = null;
		noop = '';
		conflict = null;
		conflictDiff = null;
		conflictDiffError = '';
		if (!base || !expectedHead) {
			applyError =
				'This orkflow’s versions could not be read, so an edit can be checked against ' +
				'neither the version it came from nor the newest one. Reload before applying — ' +
				'applying blind is how a newer version gets buried.';
			return;
		}
		// The unchanged document, answered where it was typed. An apply that
		// stores nothing is not a failure and must not read as a success: no
		// RPC, no navigation, and a sentence naming the version.
		if (yaml === sourceYaml) {
			noop = `The document is unchanged from ${name}@${base}.`;
			return;
		}
		applying = true;
		const submittedYaml = yaml;
		const submittedNamespace = namespace;
		try {
			const res = await client().orkflows.applyOrkflow({
				yaml: submittedYaml,
				namespace: submittedNamespace,
				baseVersion: base,
				expectedHeadVersion: expectedHead
			});
			warnings = res.warnings;
			// created=false is the same answer from the other side: the version
			// is already stored with byte-identical source. Rendered the same
			// way, and no navigation — nothing moved.
			if (!res.created) {
				noop =
					`${name}@${res.orkflow?.version ?? base} is already stored with this exact ` +
					'source.';
				return;
			}
			if (res.warnings.length > 0 && res.orkflow) {
				applied = { name: res.orkflow.name, version: res.orkflow.version };
				if (yaml === submittedYaml) cleanYaml = submittedYaml;
				return;
			}
			if (yaml === submittedYaml) cleanYaml = submittedYaml;
			await goto(versionsHref, { invalidateAll: true });
		} catch (err) {
			if (err instanceof ConnectError && err.code === Code.FailedPrecondition) {
				conflict = { base, expectedHead, message: err.message };
				await readHead();
			} else {
				applyError = errorMessage(err);
			}
		} finally {
			applying = false;
		}
	}

	async function importYaml(event: Event) {
		const input = event.currentTarget as HTMLInputElement;
		const file = input.files?.[0];
		if (!file) return;
		yaml = await file.text();
		input.value = '';
	}
	function exportYaml() {
		const link = document.createElement('a');
		link.href = URL.createObjectURL(new Blob([yaml], { type: 'application/yaml' }));
		link.download = `${name}.yaml`;
		link.click();
		URL.revokeObjectURL(link.href);
	}

	// readHead answers the only question a conflict raises: what is there now.
	async function readHead() {
		conflictBusy = true;
		conflictHeadError = '';
		try {
			const res = await client().orkflows.listOrkflowVersions({ name, namespace });
			conflictHead = res.headVersion;
		} catch (err) {
			conflictHead = '';
			conflictHeadError = errorMessage(err);
		} finally {
			conflictBusy = false;
		}
	}

	// The diff a conflict calls for is between the two HEADS — the head this
	// editor was opened against and the one that arrived. The base is where the
	// edit came from and is not what changed, so diffing from it would answer a
	// question nobody asked.
	async function showConflictDiff() {
		conflictBusy = true;
		conflictDiffError = '';
		conflictDiff = null;
		try {
			const res = await client().orkflows.diffOrkflowVersions({
				name,
				fromVersion: conflict?.expectedHead ?? '',
				toVersion: conflictHead,
				namespace
			});
			conflictDiff = res.unifiedDiff;
		} catch (err) {
			conflictDiffError = errorMessage(err);
		} finally {
			conflictBusy = false;
		}
	}

	// Retargeting is a decision the user makes, never one the page makes for
	// them: it moves the TOKEN forward after they have seen what arrived, and it
	// leaves their text — and the version they derived it from — exactly as they
	// were. Moving the base instead would rewrite the edit's provenance to a
	// document it was never derived from.
	function retarget() {
		expectedHead = conflictHead;
		conflict = null;
		conflictDiff = null;
		conflictDiffError = '';
	}
</script>

{#if error}
	<ErrorPanel
		testid="editor-error-load"
		retryTestid="editor-retry-load"
		subject={baseVersion
			? `version ${baseVersion} of ${name} in namespace ${namespaceLabel(namespace)}`
			: `the stored YAML of ${name} in namespace ${namespaceLabel(namespace)}`}
		message={error}
		busy={reloading}
		onretry={reload}
	/>
{:else if reloading}
	<p data-testid="editor-loading" class="text-sm text-fg-muted">Reloading the stored document…</p>
{:else}
	<form
		class="space-y-4"
		data-testid="editor-form"
		onsubmit={(e) => {
			e.preventDefault();
			void apply();
		}}
	>
		<div class="flex flex-wrap items-baseline gap-x-3 gap-y-1 text-sm text-fg-secondary">
			<span>
				Editing <span class="font-medium text-fg">{name}</span> in namespace
				<span class="font-medium text-fg">{namespaceLabel(namespace)}</span>, derived from
				version
				<span class="font-medium text-brand-700" data-testid="editor-base-version">{base}</span>.
			</span>
			<span class="text-xs text-fg-muted">
				The newest applied version is
				<span class="font-medium text-fg-secondary" data-testid="editor-head-version"
					>{expectedHead}</span
				>{#if base === expectedHead}, which is what this edit is derived from.{:else}<span
						data-testid="editor-fork-notice"
					>&nbsp;— applying forks forward from {base} and becomes the new head. The version that
						runs is unchanged until somebody promotes.</span
					>{/if}
			</span>
			<span class="text-xs text-fg-muted">
				Applying stores a NEW version — change <code>version:</code> before you apply. Re-applying
				the same version with different content is refused.
			</span>
		</div>

		<textarea
			id="editor-yaml"
			data-testid="editor-yaml"
			aria-label="Orkflow YAML"
			bind:value={yaml}
			oninput={() => (noop = '')}
			rows="24"
			spellcheck="false"
			class="min-h-[360px] w-full rounded-md border border-border bg-surface px-3 py-2 font-mono text-[13px] leading-5 text-fg shadow-xs [font-variant-ligatures:none] [tab-size:2] whitespace-pre focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
		></textarea>
		<div class="flex flex-wrap gap-3 text-sm">
			<label class="cursor-pointer font-medium text-brand-700">Import YAML<input data-testid="editor-import" type="file" accept=".yaml,.yml,text/yaml,application/yaml" class="sr-only" onchange={importYaml} /></label>
			<button type="button" data-testid="editor-export" onclick={exportYaml} class="font-medium text-brand-700">Export YAML</button>
			{#if dirty}<span data-testid="editor-unsaved" class="text-warning-700">Unsaved changes</span>{/if}
		</div>

		{#if applyError}
			<ErrorPanel testid="editor-error" message={applyError} />
		{/if}
		{#if noop}
			<p
				data-testid="editor-noop"
				role="status"
				class="rounded-md border border-border bg-surface-sunken px-4 py-2 text-sm text-fg-secondary"
			>
				{noop} Nothing was applied, and no new version was created.
			</p>
		{/if}
		{#each warnings as warning, i (i)}
			<div class="rounded-md border border-warning-200 bg-warning-50 px-4 py-2 text-sm text-warning-700">
				{warning}
			</div>
		{/each}
		{#if issues.length > 0}
			<div data-testid="editor-validation-issues" class="space-y-2 rounded-md border border-border bg-surface-sunken p-3 text-sm">
				<p class="font-medium text-fg">Validation results</p>
				{#each issues as issue, i (`${issue.field}-${i}`)}
					<p class={issue.severity === ValidationSeverity.ERROR ? 'text-danger-700' : 'text-warning-700'}><span class="font-medium">{issue.field || 'document'}</span>: {issue.message}</p>
				{/each}
			</div>
		{/if}
		{#if applied}
			<p data-testid="editor-apply-success" class="rounded-md border border-success-200 bg-success-50 px-4 py-2 text-sm text-success-700">Applied {applied.name}@{applied.version}. Warnings remain visible above. <a class="font-medium underline" href={versionsHref}>Open versions</a></p>
		{/if}

		{#if conflict}
			<div
				data-testid="editor-conflict"
				role="alert"
				class="space-y-3 rounded-md border border-warning-200 bg-warning-50 px-4 py-3 text-sm text-warning-800"
			>
				<p class="font-medium">
					{name} changed while this edit was open. Nothing was overwritten.
				</p>
				<p class="whitespace-pre-wrap text-warning-700">{conflict.message}</p>
				<p>
					This edit is derived from
					<span class="font-medium" data-testid="editor-conflict-base">{conflict.base}</span>; the
					newest applied version was
					<span class="font-medium" data-testid="editor-conflict-expected"
						>{conflict.expectedHead}</span
					>
					when this document was fetched
					{#if conflictHead}
						and is now
						<span class="font-medium" data-testid="editor-conflict-head">{conflictHead}</span>.
					{:else if conflictBusy}
						— <span data-testid="editor-conflict-loading">reading the newest version…</span>
					{/if}
				</p>

				{#if conflictHeadError}
					<ErrorPanel
						testid="editor-conflict-head-error"
						retryTestid="editor-conflict-head-retry"
						subject="the newest version of {name}"
						message={conflictHeadError}
						busy={conflictBusy}
						onretry={readHead}
					/>
				{/if}

				<div class="flex flex-wrap gap-3">
					<button
						type="button"
						data-testid="editor-conflict-diff"
						disabled={conflictBusy || !conflictHead}
						onclick={showConflictDiff}
						class="inline-flex h-8 items-center rounded-md border border-warning-300 bg-surface px-3 text-xs font-medium text-warning-800 shadow-xs hover:bg-warning-50 disabled:pointer-events-none disabled:opacity-50"
					>
						{conflictBusy
							? 'Working…'
							: `Show what changed (${conflict.expectedHead} → ${conflictHead})`}
					</button>
					<button
						type="button"
						data-testid="editor-conflict-rebase"
						disabled={conflictBusy || !conflictHead}
						onclick={retarget}
						class="inline-flex h-8 items-center rounded-md border border-border bg-surface px-3 text-xs font-medium text-fg shadow-xs hover:bg-surface-hover disabled:pointer-events-none disabled:opacity-50"
					>
						Keep my edit and retarget head {conflictHead || '…'}
					</button>
				</div>

				{#if conflictDiffError}
					<ErrorPanel
						testid="conflict-diff-error"
						subject="the diff of {conflict.expectedHead} → {conflictHead}"
						message={conflictDiffError}
						busy={conflictBusy}
						onretry={showConflictDiff}
					/>
				{:else if conflictDiff !== null}
					{#if conflictDiff === ''}
						<p data-testid="conflict-diff-identical" class="text-sm text-fg-secondary">
							The two versions' source YAML is byte-identical.
						</p>
					{:else}
						<DiffView diff={conflictDiff} testid="conflict-diff" />
					{/if}
				{/if}
			</div>
		{/if}

		<button type="button" data-testid="editor-validate" disabled={validating || applying} onclick={() => void validate()} class="mr-3 inline-flex h-9 items-center justify-center rounded-md border border-border bg-surface px-3.5 text-sm font-medium text-fg disabled:opacity-50">{validating ? 'Validating…' : 'Validate'}</button>
		<button
			type="submit"
			data-testid="editor-apply"
			disabled={applying || validating}
			class="inline-flex h-9 items-center justify-center gap-1.5 rounded-md bg-brand-600 px-3.5 text-sm font-medium text-white shadow-xs transition-colors duration-150 hover:bg-brand-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500/40 focus-visible:ring-offset-2 focus-visible:ring-offset-canvas active:bg-brand-800 disabled:pointer-events-none disabled:opacity-50"
		>
			{applying ? 'Applying…' : `Apply as a new version of ${name}`}
		</button>
	</form>
{/if}
