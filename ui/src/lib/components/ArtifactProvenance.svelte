<script lang="ts">
	// The Provenance disclosure on one recorded artifact: whether its bytes are
	// still in the object store, this run's own standing in the object index,
	// and every archived run of this namespace that recorded the same object.
	//
	// It is on demand, like Inspect: nothing is fetched until the reader opens
	// it, and every open asks again — an operator who just ran a lifecycle rule
	// wants a fresh answer, not the one from before it. The panel renders the
	// same fields in the same words as `orkctl run artifact provenance`, and a
	// failed read is an error, never an empty list.
	import { errorMessage } from '$lib/client';
	import { runClient } from '$lib/clients/runs';
	import { ArtifactKind, type GetArtifactProvenanceResponse } from '$lib/gen/orkstack/v1/run_service_pb';
	import { withNamespace } from '$lib/namespace';
	import {
		BYTES_MISSING,
		BYTES_PRESENT,
		NO_REFERENCES,
		provenanceCommand,
		referenceStateText,
		rfc3339,
		unreadableText
	} from '$lib/provenance';
	import CliEquivalent from './CliEquivalent.svelte';
	import ErrorPanel from './ErrorPanel.svelte';
	import StatusPill from './StatusPill.svelte';

	interface Props {
		runId: string;
		namespace: string;
		taskId: string;
		kind: ArtifactKind;
		name: string;
		/** The item of a fan-out task that recorded the artifact; undefined for a task's own. */
		item?: number;
	}

	let { runId, namespace, taskId, kind, name, item }: Props = $props();

	let open = $state(false);
	let loading = $state(false);
	let error = $state('');
	let answer = $state<GetArtifactProvenanceResponse | undefined>(undefined);
	// Each open is one request; a slower earlier answer must not overwrite a
	// newer one, so only the latest request may land.
	let request = 0;

	const key = $derived(`${taskId}${item !== undefined ? `-item-${item}` : ''}-${name}`);
	const base = $derived(`artifact-provenance-${key}`);
	const kindName = $derived(refKind(kind));

	async function load(): Promise<void> {
		const mine = ++request;
		loading = true;
		error = '';
		answer = undefined;
		try {
			const res = await runClient(fetch).getArtifactProvenance({ runId, namespace, taskId, kind, name, item });
			if (mine === request) answer = res;
		} catch (err) {
			if (mine === request) error = errorMessage(err);
		} finally {
			if (mine === request) loading = false;
		}
	}

	function toggle(): void {
		open = !open;
		if (open) void load();
		else request++;
	}

	function refKind(k: ArtifactKind): string {
		return ArtifactKind[k] ?? 'UNSPECIFIED';
	}
</script>

<button
	type="button"
	data-testid="artifact-provenance-toggle-{key}"
	aria-expanded={open}
	aria-controls={base}
	onclick={toggle}
	class="font-medium text-brand-700 hover:underline"
>
	{open ? 'Hide provenance' : 'Provenance'}
</button>
{#if open}
	<div
		id={base}
		data-testid={base}
		class="w-full min-w-0 rounded-md border border-border-subtle bg-surface-sunken px-3 py-2 text-xs"
	>
		<div class="flex items-start justify-between gap-3">
			<p class="font-medium text-fg">Provenance of <span class="font-mono">{name}</span> <span class="text-fg-muted">({kindName})</span></p>
			<CliEquivalent
				command={provenanceCommand(namespace, runId, taskId, kind, name, item)}
				testid="artifact-provenance-cli-{key}"
				label="CLI equivalent of this provenance check"
			/>
		</div>
		{#if loading}
			<p class="mt-2 text-fg-muted" data-testid="{base}-loading">Loading…</p>
		{:else if error}
			<div class="mt-2">
				<ErrorPanel
					message={error}
					subject="the provenance of {name}"
					testid="{base}-error"
					retryTestid="{base}-retry"
					onretry={() => void load()}
				/>
			</div>
		{:else if answer}
			<dl class="mt-2 grid grid-cols-[auto_1fr] gap-x-3 gap-y-1">
				<dt class="text-fg-muted uppercase">Bytes</dt>
				<dd
					data-testid="{base}-bytes"
					class={answer.bytesPresent ? 'text-fg-secondary' : 'font-medium text-danger-700'}
				>
					{answer.bytesPresent ? BYTES_PRESENT : BYTES_MISSING}
				</dd>
				<dt class="text-fg-muted uppercase">Reference</dt>
				<dd data-testid="{base}-reference" class="text-fg-secondary">
					{referenceStateText(answer.ownReference, answer.releasedAt)}
				</dd>
			</dl>
			<div class="mt-2" data-testid="{base}-references">
				{#if answer.references.length === 0}
					<p class="text-fg-muted" data-testid="{base}-references-empty">{NO_REFERENCES}</p>
				{:else}
					<p class="text-fg-muted uppercase">Referenced by</p>
					<ul class="mt-1 space-y-1">
						{#each answer.references as ref, i (ref.runId + ':' + ref.taskId + ':' + ref.kind + ':' + i)}
							<li
								data-testid="{base}-reference-row"
								class="flex flex-wrap items-center gap-x-3 gap-y-1 rounded border border-border-subtle bg-surface px-2 py-1"
							>
								<a
									href={withNamespace(`/runs/${encodeURIComponent(ref.runId)}`, namespace)}
									data-testid="{base}-reference-link"
									class="font-mono break-all text-brand-700 hover:underline">{ref.runId}</a
								>
								{#if ref.runId === runId}
									<span
										data-testid="{base}-this-run"
										class="rounded-full border border-border px-1.5 text-[11px] text-fg-muted">this run</span
									>
								{/if}
								<span class="text-fg-secondary">{ref.orkflowName}@{ref.orkflowVersion}</span>
								<span class="font-mono text-fg-secondary">{ref.taskId}</span>
								<span class="text-fg-muted">{refKind(ref.kind)}</span>
								<StatusPill status={ref.runStatus} />
								<span class="text-fg-muted">{rfc3339(ref.runCreatedAt)}</span>
							</li>
						{/each}
					</ul>
				{/if}
			</div>
			{#if unreadableText(answer.referencesUnreadable)}
				<p class="mt-2 text-fg-muted" data-testid="{base}-unreadable">{unreadableText(answer.referencesUnreadable)}</p>
			{/if}
		{/if}
	</div>
{/if}
