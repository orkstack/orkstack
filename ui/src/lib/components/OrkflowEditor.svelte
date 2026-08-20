<script lang="ts">
	// The Edit tab: change the stored YAML and apply it as a new version.
	//
	// Two decisions from the design (v003-design.md §1.9, M10.7) are load-bearing
	// here and neither is cosmetic:
	//
	// 1. There is NO client-side validation. The compiler is the validator, and
	//    a second one in the browser would drift from it — telling somebody a
	//    document is fine that the engine then rejects, or worse the reverse.
	//    Every message in the error panel below is an ApplyOrkflow response.
	//
	// 2. Every apply carries base_version: the head version this editor was
	//    opened on. The engine compares it with the head it holds now and
	//    refuses (FailedPrecondition) when somebody else has applied in the
	//    meantime, so an edit made from a stale copy is REPORTED instead of
	//    landing quietly beside a newer version nobody looked at.
	import { untrack } from 'svelte';
	import { goto } from '$app/navigation';
	import { Code, ConnectError } from '@connectrpc/connect';
	import DiffView from '$lib/components/DiffView.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import { errorMessage, orkstack } from '$lib/client';
	import { namespaceLabel } from '$lib/namespace';

	interface Props {
		name: string;
		namespace: string;
		/** The stored YAML of the head version — what this edit is derived from. */
		sourceYaml: string;
		/** The head version at load: the optimistic-concurrency token. */
		headVersion: string;
		/** Why the head could not be read, if it could not. */
		error: string | null;
		/** Where to land after a successful apply. */
		versionsHref: string;
		onreload: () => Promise<void>;
	}

	let { name, namespace, sourceYaml, headVersion, error, versionsHref, onreload }: Props =
		$props();

	// Seeded once. A later reload of the page data must not overwrite what
	// somebody is typing — that is the loss this whole tab exists to prevent.
	let yaml = $state(untrack(() => sourceYaml));
	let base = $state(untrack(() => headVersion));

	let applying = $state(false);
	let reloading = $state(false);
	let applyError = $state('');
	let warnings = $state<string[]>([]);

	// The stale-edit report. It holds the base this editor sent, the head the
	// engine has now, and the diff between them once asked for — the "re-read +
	// diff path" the design requires instead of a silent overwrite.
	let conflict = $state<{ base: string; message: string } | null>(null);
	let conflictHead = $state('');
	let conflictHeadError = $state('');
	let conflictDiff = $state<string | null>(null);
	let conflictDiffError = $state('');
	let conflictBusy = $state(false);

	const client = () => orkstack(fetch);

	async function reload() {
		reloading = true;
		try {
			await onreload();
		} finally {
			reloading = false;
		}
	}

	async function apply() {
		if (!base) {
			applyError =
				'This orkflow’s head version could not be read, so an edit cannot be checked ' +
				'against it. Reload before applying — applying blind is how a newer version ' +
				'gets buried.';
			return;
		}
		applying = true;
		applyError = '';
		warnings = [];
		conflict = null;
		conflictDiff = null;
		conflictDiffError = '';
		try {
			const res = await client().orkflows.applyOrkflow({ yaml, namespace, baseVersion: base });
			warnings = res.warnings;
			await goto(versionsHref, { invalidateAll: true });
		} catch (err) {
			if (err instanceof ConnectError && err.code === Code.FailedPrecondition) {
				conflict = { base, message: err.message };
				await readHead();
			} else {
				applyError = errorMessage(err);
			}
		} finally {
			applying = false;
		}
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

	async function showConflictDiff() {
		conflictBusy = true;
		conflictDiffError = '';
		conflictDiff = null;
		try {
			const res = await client().orkflows.diffOrkflowVersions({
				name,
				fromVersion: conflict?.base ?? '',
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

	// Rebasing is a decision the user makes, never one the page makes for them:
	// it moves the token forward after they have seen what arrived, and it
	// leaves their text exactly as they typed it.
	function rebase() {
		base = conflictHead;
		conflict = null;
		conflictDiff = null;
		conflictDiffError = '';
	}
</script>

{#if error}
	<ErrorPanel
		testid="editor-error-load"
		retryTestid="editor-retry-load"
		subject="the stored YAML of {name} in namespace {namespaceLabel(namespace)}"
		message={error}
		busy={reloading}
		onretry={reload}
	/>
{:else if reloading}
	<p data-testid="editor-loading" class="text-sm text-zinc-400">Reloading the stored document…</p>
{:else}
	<form
		class="space-y-4"
		data-testid="editor-form"
		onsubmit={(e) => {
			e.preventDefault();
			void apply();
		}}
	>
		<div class="flex flex-wrap items-baseline gap-x-3 gap-y-1 text-sm text-zinc-400">
			<span>
				Editing <span class="font-medium text-zinc-200">{name}</span> in namespace
				<span class="font-medium text-zinc-200">{namespaceLabel(namespace)}</span>, derived from
				version
				<span class="font-medium text-emerald-300" data-testid="editor-base-version">{base}</span>.
			</span>
			<span class="text-xs text-zinc-500">
				Applying stores a NEW version — change <code>version:</code> before you apply. Re-applying
				the same version with different content is refused.
			</span>
		</div>

		<textarea
			id="editor-yaml"
			data-testid="editor-yaml"
			aria-label="Orkflow YAML"
			bind:value={yaml}
			rows="24"
			spellcheck="false"
			class="w-full rounded-md border border-zinc-700 bg-zinc-950 px-3 py-2 font-mono text-sm text-zinc-100"
		></textarea>

		{#if applyError}
			<ErrorPanel testid="editor-error" message={applyError} />
		{/if}
		{#each warnings as warning, i (i)}
			<div
				class="rounded-md border border-amber-500/40 bg-amber-500/10 px-4 py-2 text-sm text-amber-300"
			>
				{warning}
			</div>
		{/each}

		{#if conflict}
			<div
				data-testid="editor-conflict"
				role="alert"
				class="space-y-3 rounded-md border border-amber-500/50 bg-amber-500/10 px-4 py-3 text-sm text-amber-200"
			>
				<p class="font-medium">{name} changed underneath this edit. Nothing was overwritten.</p>
				<p class="whitespace-pre-wrap text-amber-200/90">{conflict.message}</p>
				<p>
					This editor is derived from
					<span class="font-medium" data-testid="editor-conflict-base">{conflict.base}</span>;
					{#if conflictHead}
						the newest applied version is now
						<span class="font-medium" data-testid="editor-conflict-head">{conflictHead}</span>.
					{:else if conflictBusy}
						<span data-testid="editor-conflict-loading">reading the newest version…</span>
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
						class="rounded-md border border-amber-400/50 px-3 py-1.5 text-sm font-medium text-amber-100 hover:bg-amber-500/20 disabled:opacity-50"
					>
						{conflictBusy ? 'Working…' : `Show what changed (${conflict.base} → ${conflictHead})`}
					</button>
					<button
						type="button"
						data-testid="editor-conflict-rebase"
						disabled={conflictBusy || !conflictHead}
						onclick={rebase}
						class="rounded-md border border-zinc-600 px-3 py-1.5 text-sm text-zinc-200 hover:bg-zinc-800 disabled:opacity-50"
					>
						Apply on top of {conflictHead || '…'}
					</button>
				</div>

				{#if conflictDiffError}
					<ErrorPanel
						testid="conflict-diff-error"
						subject="the diff of {conflict.base} → {conflictHead}"
						message={conflictDiffError}
						busy={conflictBusy}
						onretry={showConflictDiff}
					/>
				{:else if conflictDiff !== null}
					{#if conflictDiff === ''}
						<p data-testid="conflict-diff-identical">
							The two versions' source YAML is byte-identical.
						</p>
					{:else}
						<DiffView diff={conflictDiff} testid="conflict-diff" />
					{/if}
				{/if}
			</div>
		{/if}

		<button
			type="submit"
			data-testid="editor-apply"
			disabled={applying}
			class="rounded-md bg-emerald-600 px-4 py-2 text-sm font-medium text-white hover:bg-emerald-500 disabled:opacity-50"
		>
			{applying ? 'Applying…' : `Apply as a new version of ${name}`}
		</button>
	</form>
{/if}
