<script lang="ts">
	import type { Snippet } from 'svelte';
	import { onMount } from 'svelte';
	import CliPopover from './CliPopover.svelte';
	import EditorDocsRail from './EditorDocsRail.svelte';
	import Icon from './Icon.svelte';
	import { shellQuote } from '#lib/runDetail.ts';
	// The Edit tab: change ONE stored version's YAML and apply it as a new one.
	//
	// Three decisions are load-bearing here and none is cosmetic:
	//
	// 1. The engine is the validator. The editor checks YAML syntax and the
	//    generated schema as the document is typed (src/lib/yaml/), and asks
	//    the engine — ValidateOrkflow, apply's own preparation — once typing
	//    pauses; only that answer ever says the document has no problems, and
	//    nothing the editor finds on its own ever disables Apply. Every message
	//    in the error panel below is an ApplyOrkflow response.
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
	import { guardUnsaved } from '#lib/dirtyGuard.ts';
	import { Code, ConnectError } from '@connectrpc/connect';
	import DiffView from '#lib/components/DiffView.svelte';
	import ErrorPanel from '#lib/components/ErrorPanel.svelte';
	import YamlField from '#lib/yaml/YamlField.svelte';
	import YamlProblems from '#lib/yaml/YamlProblems.svelte';
	import { YamlSession } from '#lib/yaml/session.svelte.ts';
	import { errorMessage } from '#lib/client.ts';
	import { orkflowClient } from '#lib/clients/orkflows.ts';
	import { ValidationSeverity } from '#lib/gen/orkstack/v1/orkflow_service_pb.ts';
	import { namespaceLabel } from '#lib/namespace.ts';

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
		/** Hands the page its header's actions: Validate, Apply version and the command line. */
		onactions?: (actions: Snippet | undefined) => void;
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
		onreload,
		onactions
	}: Props = $props();
	const uid = $props.id();
	const formId = `${uid}-form`;
	$effect(() => {
		onactions?.(error || reloading ? undefined : headerActions);
		return () => onactions?.(undefined);
	});

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
	// The version this draft would store, as its own top-level line says: only ever a label.
	const draftVersion = $derived(/^version:[ \t]*["']?([^\s"'#]+)/m.exec(yaml)?.[1] ?? '');
	let validating = $state(false);
	let reloading = $state(false);
	let applyError = $state('');
	let warnings = $state<string[]>([]);
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

	const dirty = $derived(yaml !== cleanYaml);

	guardUnsaved(() => dirty, 'Discard unsaved Orkflow YAML?');

	// The live check (src/lib/yaml/session.svelte.ts): ValidateOrkflow, asked
	// once typing pauses and by the Validate button.
	const session = new YamlSession(async (text, ns, signal) => {
		const res = await orkflowClient(fetch).validateOrkflow({ yaml: text, namespace: ns }, { signal });
		return res.issues;
	});

	async function validate(): Promise<boolean> {
		applyError = '';
		if (!yaml.trim()) { applyError = 'Paste an Orkflow YAML document first.'; return false; }
		const validatedYaml = yaml;
		const validatedNamespace = namespace;
		validating = true;
		try {
			const issues = await session.validateNow(validatedYaml, validatedNamespace);
			if (issues === null || yaml !== validatedYaml || namespace !== validatedNamespace) return false;
			return !issues.some((issue) => issue.severity === ValidationSeverity.ERROR);
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
				'This Orkflow’s versions could not be read, so an edit can be checked against ' +
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
			const res = await orkflowClient(fetch).applyOrkflow({
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
			const res = await orkflowClient(fetch).listOrkflowVersions({ name, namespace });
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
			const res = await orkflowClient(fetch).diffOrkflowVersions({
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

	// ---- Layout C: the problems chip, the docs rail, the More disclosure ----
	const allFindings = $derived([...session.engineFindings, ...session.localFindings]);
	const errorCount = $derived(allFindings.filter((f) => f.severity === 'error').length);
	const warningCount = $derived(allFindings.filter((f) => f.severity === 'warning').length);
	const problemCount = $derived(errorCount + warningCount);
	// Apply is held only by the engine's own verdict on this exact text, never
	// by what the editor finds on its own.
	const engineCurrent = $derived(session.engine.kind === 'answered' && session.engine.text === yaml);
	const engineErrors = $derived(engineCurrent ? session.engineFindings.filter((f) => f.severity === 'error').length : 0);
	const applyHeld = $derived(engineErrors > 0);
	// The list is open while there are problems; the chip folds it away.
	let problemsOpen = $state(true);
	let moreOpen = $state(false);
	let railOpen = $state(true);
	onMount(() => {
		railOpen = matchMedia('(min-width: 1280px)').matches;
	});
	const atCaret = $derived(session.caretTask);
</script>

{#snippet headerActions()}
				<button type="button" data-testid="editor-validate" aria-disabled={validating || applying ? 'true' : undefined} onclick={() => { if (!validating && !applying) void validate(); }} class="btn">{validating ? 'Validating…' : 'Validate'}</button>
				<button
					type="submit"
					form={formId}
					data-tip="Applying stores a new version: change version: first"
					data-testid="editor-apply"
					aria-disabled={applyHeld || applying || validating ? 'true' : undefined}
					aria-describedby={applyHeld ? 'editor-apply-why' : undefined}
					onclick={(e) => {
						if (applyHeld || applying || validating) e.preventDefault();
					}}
					class="btn btn-primary"
				>
					<Icon name="apply" />{applying ? 'Applying…' : `Apply version ${draftVersion}`.trim()}
				</button>
				<CliPopover
					testid="editor-cli"
					title="Command line: this Orkflow"
					commands={[
						{ label: 'Check a file without storing it', command: `orkctl -n ${shellQuote(namespaceLabel(namespace))} orkflow validate ${name}.ork.yaml` },
						{ label: 'Apply it as a new version', command: `orkctl -n ${shellQuote(namespaceLabel(namespace))} apply ${name}.ork.yaml` },
						{ label: 'Edit it in your own editor', command: `orkctl -n ${shellQuote(namespaceLabel(namespace))} orkflow edit ${name}` }
					]}
				/>
			{#if applyHeld}
				<p id="editor-apply-why" class="apply-why" data-testid="editor-apply-why">Fix {engineErrors} {engineErrors === 1 ? 'problem' : 'problems'} to apply</p>
			{/if}
{/snippet}

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
	<p data-testid="editor-loading" class="text-sm text-ink-muted">Reloading the stored document…</p>
{:else}
	<form
		id={formId}
		class="grid gap-4"
		data-testid="editor-form"
		onsubmit={(e) => {
			e.preventDefault();
			void apply();
		}}
	>
		<div class="ed-grid" class:rail-closed={!railOpen}>
			<section class="card edcard" aria-labelledby="editor-yaml-label">
				<div class="card-head">
					<h2 id="editor-yaml-label">Edit</h2>
					<span class="card-sub">a new version from <span class="figure" data-testid="editor-base-version">{base}</span><span class={base === expectedHead ? 'sr' : ''}>; the newest is <span class="figure" data-testid="editor-head-version">{expectedHead}</span></span></span>
					{#if problemCount > 0}
						<button
							type="button"
							class="chip"
							aria-expanded={problemsOpen}
							aria-controls="editor-problems"
							data-testid="editor-problems-chip"
							onclick={() => (problemsOpen = !problemsOpen)}
							><span>Problems:</span><b
								>{errorCount} {errorCount === 1 ? 'error' : 'errors'}{#if warningCount}, {warningCount} {warningCount === 1 ? 'warning' : 'warnings'}{/if}</b
							><Icon name="chev" /></button
						>
					{/if}
					<div class="card-end">
						{#if dirty}<span data-testid="editor-unsaved" class="text-sm font-semibold text-ink-3">Unsaved changes</span>{/if}
						<span class="ie">
							<label class="btn btn-sm btn-quiet cursor-pointer">Import<input data-testid="editor-import" type="file" accept=".yaml,.yml,text/yaml,application/yaml" class="sr-only" onchange={importYaml} /></label>
							<button type="button" data-testid="editor-export" onclick={exportYaml} class="btn btn-sm btn-quiet">Export</button>
						</span>
						<!-- A plain disclosure: Escape and focus leaving it close it. -->
						<!-- svelte-ignore a11y_no_static_element_interactions -->
						<span
							class="more-wrap"
							onkeydown={(e) => {
								if (e.key === 'Escape' && moreOpen) {
									e.stopPropagation();
									moreOpen = false;
									(e.currentTarget.querySelector('[data-testid="editor-more"]') as HTMLElement | null)?.focus();
								}
							}}
						>
							<button
								type="button"
								class="btn btn-sm btn-icon"
								aria-label="More actions"
								aria-expanded={moreOpen}
								aria-controls="editor-more"
								data-testid="editor-more"
								onclick={() => (moreOpen = !moreOpen)}
								onfocusout={(e) => {
									const next = e.relatedTarget as Node | null;
									if (!next || !(e.currentTarget.parentElement?.contains(next))) moreOpen = false;
								}}><Icon name="more" /></button
							>
							{#if moreOpen}
								<span id="editor-more" class="more-pop pop" data-testid="editor-more-menu">
									<label class="more-item cursor-pointer">Import<input type="file" accept=".yaml,.yml,text/yaml,application/yaml" class="sr-only" onchange={(e) => { moreOpen = false; importYaml(e); }} /></label>
									<button type="button" class="more-item" onclick={() => { moreOpen = false; exportYaml(); }}>Export</button>
								</span>
							{/if}
						</span>
					</div>
				</div>
				{#if base !== expectedHead}
					<p class="ed-fork" data-testid="editor-fork-notice">Applying forks forward from {base} and becomes the new head; the version that runs is unchanged until somebody promotes.</p>
				{/if}
				<div class="px-0">
					<YamlField
						bind:value={yaml}
						kind="Orkflow"
						{namespace}
						{session}
						testid="editor-yaml"
						label="Orkflow YAML"
						labelledby="editor-yaml-label"
						rows={24}
						oninput={() => (noop = '')}
					/>
				</div>
				{#if problemsOpen && problemCount > 0}
					<section id="editor-problems" class="plist scroll-panel" aria-label="Problems" data-testid="editor-problems">
						<YamlProblems {session} listTestid="editor-validation-issues" issueTestid="editor-validation-issue" testid="editor-yaml" level={3} />
					</section>
				{/if}
			</section>
			<EditorDocsRail task={atCaret} open={railOpen} ontoggle={() => (railOpen = !railOpen)} />
		</div>

		{#if applyError}
			<ErrorPanel testid="editor-error" message={applyError} />
		{/if}
		{#if noop}
			<p
				data-testid="editor-noop"
				role="status"
				class="note flex gap-3 px-4 py-2 text-sm"
			>
				<span class="caps text-ink-muted">Note</span>
				<span>{noop} Nothing was applied, and no new version was created.</span>
			</p>
		{/if}
		{#each warnings as warning, i (i)}
			<div class="severity-warning border border-red px-4 py-2 text-sm">
				{warning}
			</div>
		{/each}
		<!-- A live region that is always there, so the apply's one confirmation is announced. -->
		<div role="status">
			{#if applied}
				<p data-testid="editor-apply-success" class="confirm border border-rule-strong px-4 py-2 text-sm text-ink">Applied {applied.name}@{applied.version}. Warnings remain visible above. <a class="link font-semibold" href={versionsHref}>Open versions</a></p>
			{/if}
		</div>

		{#if conflict}
			<div
				data-testid="editor-conflict"
				role="alert"
				class="space-y-3 border border-red bg-red-tint px-4 py-3 text-sm text-ink"
			>
				<p class="severity-error font-semibold">
					{name} changed while this edit was open. Nothing was overwritten.
				</p>
				<p class="whitespace-pre-wrap text-ink-secondary">{conflict.message}</p>
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
						class="btn btn-sm"
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
						class="btn btn-sm"
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
						<p data-testid="conflict-diff-identical" class="text-sm text-ink-secondary">
							The two versions' source YAML is byte-identical.
						</p>
					{:else}
						<DiffView diff={conflictDiff} testid="conflict-diff" lang="yaml" />
					{/if}
				{/if}
			</div>
		{/if}

	</form>
{/if}

<style>
	.ed-fork {
		margin: 0;
		padding: 8px 16px;
		border-bottom: 1px solid var(--line);
		font-size: 13.5px;
		color: var(--ink-2);
	}
	.apply-why {
		flex-basis: 100%;
		margin: 0;
		font-size: 13px;
		font-weight: 600;
		color: var(--fail);
	}
	.ed-grid {
		display: grid;
		grid-template-columns: minmax(0, 1fr) 320px;
		gap: 16px;
		align-items: start;
	}
	.edcard {
		position: sticky;
		top: calc(var(--spacing-bar) + 16px);
		display: flex;
		flex-direction: column;
		overflow: hidden;
	}
	.edcard :global(.cm-editor) {
		height: max(420px, calc(100dvh - var(--spacing-bar) - 300px)) !important;
		min-height: 0 !important;
		max-height: none !important;
		border: 0 !important;
		border-radius: 0;
	}
	.edcard :global(.yaml-field > div:last-child) {
		margin: 0;
		padding: 8px 16px;
		border-top: 1px solid var(--line);
		background: var(--surface-2);
	}
	.plist {
		max-height: 196px;
		overflow-y: auto;
		scrollbar-gutter: stable;
		padding: 8px 16px;
		border-top: 1px solid var(--line);
	}
	.more-wrap {
		position: relative;
		display: none;
	}
	.more-pop {
		right: 0;
		top: calc(100% + 4px);
		display: grid;
		min-width: 140px;
		padding: 4px;
	}
	.more-item {
		display: block;
		padding: 8px 10px;
		border: 0;
		border-radius: 4px;
		background: transparent;
		color: var(--ink);
		font: inherit;
		font-size: 14px;
		text-align: left;
		cursor: pointer;
	}
	.more-item:hover,
	.more-item:focus-within,
	.more-item:focus-visible {
		background: var(--hover);
	}
	@media (max-width: 1359px) {
		.ed-grid {
			grid-template-columns: minmax(0, 1fr) 296px;
		}
	}
	@media (min-width: 1181px) and (max-width: 1279px) {
		.ed-grid.rail-closed {
			grid-template-columns: minmax(0, 1fr) 44px;
		}
		.ed-grid.rail-closed :global(.rail .rail-head > p) {
			display: none;
		}
		.ed-grid.rail-closed :global(.rail .rail-head) {
			padding: 8px 2px;
		}
	}
	@media (min-width: 1280px) {
		.ed-grid.rail-closed {
			grid-template-columns: minmax(0, 1fr) 44px;
		}
		.ed-grid.rail-closed :global(.rail .rail-head > p) {
			display: none;
		}
		.ed-grid.rail-closed :global(.rail .rail-head) {
			padding: 8px 2px;
		}
	}
	@media (max-width: 1180px) {
		.ed-grid {
			grid-template-columns: minmax(0, 1fr);
		}
		.edcard {
			position: static;
		}
		.edcard :global(.cm-editor) {
			height: clamp(380px, 58dvh, 680px) !important;
		}
		.ed-grid.rail-closed :global(.rail) {
			position: sticky;
			bottom: 0;
			z-index: 10;
		}
		:global(html:has(.ed-grid.rail-closed)) {
			scroll-padding-bottom: 80px;
		}
	}
	@media (max-width: 640px) {
		.ed-grid.rail-closed :global(.rail) {
			position: static;
		}
		:global(html:has(.ed-grid.rail-closed)) {
			scroll-padding-bottom: 0;
		}
	}
	@media (max-width: 400px) {
		.ie {
			display: none;
		}
		.more-wrap {
			display: inline-block;
		}
	}
</style>
