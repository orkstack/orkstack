<script lang="ts">
	// The Evidence dialog: what a run's evidence bundle WOULD contain, shown
	// before a byte of it is fetched, then the bundle itself.
	//
	// The manifest comes first and is the same document `orkctl run evidence
	// export --json` prints; the download is the same request the CLI makes, so
	// the browser and the terminal hand an auditor the same file. Nothing is
	// shown from a partial stream: a manifest, or a bundle, exists only once the
	// engine's final `last` chunk has arrived (docs/RUN_EVIDENCE.md).
	import { onDestroy, untrack } from 'svelte';
	import CliEquivalent from './CliEquivalent.svelte';
	import Dialog from './Dialog.svelte';
	import ErrorPanel from './ErrorPanel.svelte';
	import { errorMessage } from '$lib/client';
	import { runClient } from '$lib/clients/runs';
	import {
		absentMembers,
		collectEvidence,
		definitionLine,
		evidenceBlob,
		evidenceCommand,
		evidenceFilename,
		evidenceSummary,
		evidenceTooLarge,
		parseManifest,
		type Manifest
	} from '$lib/evidence';

	interface Props {
		runId: string;
		namespace: string;
		onclose: () => void;
	}

	let { runId, namespace, onclose }: Props = $props();

	let includeLogs = $state(true);
	let includeArtifacts = $state(true);
	let manifest = $state<Manifest | undefined>(undefined);
	let manifestText = $state('');
	let loading = $state(false);
	let error = $state('');
	let showManifest = $state(false);
	let downloading = $state(false);

	let manifestAbort: AbortController | undefined;
	let downloadAbort: AbortController | undefined;
	onDestroy(() => {
		manifestAbort?.abort();
		downloadAbort?.abort();
	});

	const absent = $derived(manifest ? absentMembers(manifest) : []);
	const overLimit = $derived(
		manifest ? evidenceTooLarge(manifest, namespace, runId, { logs: includeLogs, artifacts: includeArtifacts }) : null
	);
	const tooLarge = $derived(overLimit !== null);
	const command = $derived(evidenceCommand(namespace, runId, { logs: includeLogs, artifacts: includeArtifacts }));

	function request(manifestOnly: boolean) {
		return {
			runId,
			namespace,
			excludeLogs: !includeLogs,
			excludeArtifacts: !includeArtifacts,
			manifestOnly
		};
	}

	// Collects one stream; resolves only when `last` arrived ($lib/evidence).
	function collect(manifestOnly: boolean, signal: AbortSignal): Promise<Uint8Array[]> {
		return collectEvidence(runClient(fetch).exportRunEvidence(request(manifestOnly), { signal }), manifestOnly);
	}

	async function loadManifest(): Promise<void> {
		manifestAbort?.abort();
		const abort = new AbortController();
		manifestAbort = abort;
		loading = true;
		error = '';
		manifest = undefined;
		manifestText = '';
		try {
			const parts = await collect(true, abort.signal);
			const bytes = new Uint8Array(parts.reduce((n, part) => n + part.length, 0));
			let offset = 0;
			for (const part of parts) {
				bytes.set(part, offset);
				offset += part.length;
			}
			if (abort.signal.aborted) return;
			manifest = parseManifest(bytes);
			manifestText = new TextDecoder().decode(bytes);
		} catch (err) {
			if (!abort.signal.aborted) error = errorMessage(err);
		} finally {
			if (manifestAbort === abort) {
				manifestAbort = undefined;
				loading = false;
			}
		}
	}

	// Fetched when the dialog opens, and again whenever a toggle changes:
	// the manifest is per request, and the request is what the toggles say.
	$effect(() => {
		void includeLogs;
		void includeArtifacts;
		untrack(() => void loadManifest());
	});

	async function download(): Promise<void> {
		if (!manifest || tooLarge) return;
		downloadAbort?.abort();
		const abort = new AbortController();
		downloadAbort = abort;
		downloading = true;
		error = '';
		try {
			// evidenceBlob empties `parts`: from here the Blob is the only copy.
			const blob = evidenceBlob(await collect(false, abort.signal));
			const url = URL.createObjectURL(blob);
			const link = document.createElement('a');
			link.href = url;
			link.download = evidenceFilename(runId);
			link.click();
			URL.revokeObjectURL(url);
		} catch (err) {
			if (!abort.signal.aborted) error = errorMessage(err);
		} finally {
			if (downloadAbort === abort) downloadAbort = undefined;
			downloading = false;
		}
	}
</script>

<Dialog title="Evidence bundle of {runId}" testid="run-evidence-dialog" {onclose} widthClass="max-w-2xl">
	{#snippet body()}
		<div class="grid grid-cols-1 gap-4">
			<p class="text-sm text-ink-secondary">
				One tar.gz of everything this run recorded — the archived run and events, the pinned definition,
				and per task its commit, values, artifacts, attempt records and logs — with a MANIFEST.json naming
				each member's sha256. Verify it later, offline, with <code class="font-mono text-xs"
					>orkctl run evidence verify</code
				>.
			</p>
			<div class="flex flex-wrap gap-4">
				<label class="flex items-center gap-1.5 text-sm text-ink">
					<input
						type="checkbox"
						data-testid="run-evidence-logs"
						bind:checked={includeLogs}
						disabled={downloading}
					/>Include logs
				</label>
				<label class="flex items-center gap-1.5 text-sm text-ink">
					<input
						type="checkbox"
						data-testid="run-evidence-artifacts"
						bind:checked={includeArtifacts}
						disabled={downloading}
					/>Include artifacts
				</label>
			</div>

			{#if error}
				<ErrorPanel
					testid="run-evidence-error"
					retryTestid="run-evidence-retry"
					subject="the evidence of {runId}"
					message={error}
					onretry={() => void loadManifest()}
				/>
			{/if}

			{#if loading}
				<p class="text-sm text-ink-muted" data-testid="run-evidence-loading">Reading the manifest…</p>
			{:else if manifest}
				<div class="grid grid-cols-1 gap-1.5">
					<p class="text-sm font-medium text-ink" data-testid="run-evidence-summary">{evidenceSummary(manifest)}</p>
					<p class="font-mono text-xs break-all text-ink-muted" data-testid="run-evidence-definition">
						{definitionLine(manifest)}
					</p>
				</div>
				{#if absent.length}
					<div>
						<p class="text-xs font-medium text-ink-muted">Not in the bundle, and why</p>
						<ul class="mt-1 grid grid-cols-1 gap-1" data-testid="run-evidence-absent">
							{#each absent as member (member.path)}
								<li class="text-xs">
									<span class="font-mono text-ink">{member.path}</span>
									<span class="text-ink-muted">— {member.state}: {member.reason}</span>
								</li>
							{/each}
						</ul>
					</div>
				{/if}
				<div>
					<button
						type="button"
						data-testid="run-evidence-manifest-toggle"
						aria-expanded={showManifest}
						aria-controls="run-evidence-manifest"
						onclick={() => (showManifest = !showManifest)}
						class="text-xs font-medium text-blue hover:underline"
					>{showManifest ? 'Hide manifest' : 'View manifest'}</button>
					{#if showManifest}
						<pre
							id="run-evidence-manifest"
							data-testid="run-evidence-manifest"
							class="mt-2 max-h-64 overflow-auto rounded-sm bg-paper-sunken p-3 font-mono text-xs text-ink-secondary">{manifestText}</pre>
					{/if}
				</div>
			{/if}

			{#if overLimit}
				<div
					data-testid="run-evidence-too-large"
					class="rounded-sm border border-rule-strong bg-paper-sunken px-3 py-2 text-sm text-ink-secondary"
				>
					<p>{overLimit.message}</p>
					<div class="mt-2 flex items-start gap-2">
						<pre
							data-testid="run-evidence-too-large-command"
							class="min-w-0 flex-1 overflow-x-auto rounded-sm bg-paper-sunken p-2 font-mono text-xs text-ink">{overLimit.command}</pre>
						<CliEquivalent command={overLimit.command} testid="run-evidence-too-large-cli" label="Copy the export command" />
					</div>
				</div>
			{/if}

			<div class="flex items-center justify-end gap-2">
				<CliEquivalent {command} testid="run-evidence-cli" label="Copy CLI" />
				<button
					type="button"
					data-testid="run-evidence-download"
					disabled={!manifest || loading || downloading || tooLarge}
					onclick={() => void download()}
					class="btn btn-primary"
				>{downloading
						? 'Downloading…'
						: tooLarge
							? 'Too large for a browser download — use the CLI'
							: 'Download bundle'}</button>
			</div>
		</div>
	{/snippet}
</Dialog>
