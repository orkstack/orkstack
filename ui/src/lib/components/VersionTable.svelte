<script lang="ts">
	// The versions of the Orkflow a run pinned, beside the run's tasks: which
	// one this run is, which one is current, which one is the head. It reads
	// ListOrkflowVersions EXACTLY ONCE, after the page has rendered — never on
	// a live event or a poll; only again when the reader presses Retry after a
	// failure — and says when that read has settled (`data-loaded`), the point
	// awaited-tasks.spec.ts waits for before it holds the Orkflow page's own
	// read of the same list.
	//
	// It shows what the listing carries and nothing more: a version, when it
	// was applied, and its marks. A version's digest or run count is not on
	// the listing, so it is not invented here; the run's own digest is in the
	// title block.
	import { onMount } from 'svelte';
	import { errorMessage } from '$lib/client';
	import { orkflowClient } from '$lib/clients/orkflows';
	import type { OrkflowVersion } from '$lib/gen/orkstack/v1/orkflow_service_pb';
	import { Status } from '$lib/gen/orkstack/v1/common_pb';
	import { withNamespace } from '$lib/namespace';
	import { fmtTimestamp } from '$lib/status';

	interface Props {
		namespace: string;
		orkflowName: string;
		/** The version this run pinned. */
		runVersion: string;
		runStatus: Status;
		/** At most this many rows; this run's own row is always among them. */
		limit?: number;
	}

	let { namespace, orkflowName, runVersion, runStatus, limit = 5 }: Props = $props();

	let versions = $state<OrkflowVersion[] | null>(null);
	let head = $state('');
	let error = $state('');
	let loading = $state(false);

	async function read(): Promise<void> {
		loading = true;
		error = '';
		try {
			const res = await orkflowClient(fetch).listOrkflowVersions({ name: orkflowName, namespace });
			versions = res.versions;
			head = res.headVersion;
		} catch (err) {
			versions = null;
			error = errorMessage(err);
		} finally {
			loading = false;
		}
	}

	// Once, after the first render: onMount runs after the page has drawn.
	onMount(() => {
		void read();
	});

	const shown = $derived.by(() => {
		if (!versions) return [];
		const first = versions.slice(0, limit);
		if (first.some((v) => v.version === runVersion)) return first;
		const own = versions.find((v) => v.version === runVersion);
		return own ? [...first.slice(0, limit - 1), own] : first;
	});

	function rowGround(version: string): string {
		if (version !== runVersion) return '';
		if (runStatus === Status.FAILED) return 'bg-red-tint';
		if (runStatus === Status.RUNNING || runStatus === Status.PENDING) return 'bg-blue-tint';
		return 'bg-paper-sunken';
	}

	const allHref = $derived(withNamespace(`/orkflows/${encodeURIComponent(orkflowName)}?tab=versions`, namespace));
</script>

<section aria-labelledby="run-versions-heading">
	<h2 id="run-versions-heading" class="caps mb-2 text-ink-muted">Versions</h2>
	{#if error}
		<div data-testid="run-versions-error" role="alert" class="border border-red px-3 py-2 text-sm">
			<p class="severity-error">Error: the versions of {orkflowName} could not be read: {error}</p>
			<button type="button" data-testid="run-versions-retry" class="btn btn-sm mt-2" disabled={loading} onclick={() => void read()}
				>{loading ? 'Retrying…' : 'Retry'}</button
			>
		</div>
	{:else}
		<table data-testid="run-versions" data-loaded={versions ? 'true' : undefined} aria-busy={loading} class="register">
			<thead>
				<tr>
					<th scope="col">Version</th>
					<th scope="col">Applied</th>
					<th scope="col"><span class="sr-only">Marks</span></th>
				</tr>
			</thead>
			<tbody>
				{#if !versions}
					<tr><td colspan="3" class="text-ink-muted">Reading the versions…</td></tr>
				{:else}
					{#each shown as v (v.version)}
						<tr data-testid="run-version-{v.version}" class={rowGround(v.version)}>
							<td class="figure text-ink">{v.version}</td>
							<td class="figure text-xs text-ink-secondary">{fmtTimestamp(v.appliedAt)}</td>
							<td class="text-right">
								<div class="flex flex-wrap justify-end gap-1">
									{#if v.version === runVersion}<span class="stamp border-ink text-ink" data-testid="run-version-{v.version}-this">This run</span>{/if}
									{#if v.isCurrent}<span class="stamp border-blue text-blue" data-testid="run-version-{v.version}-current">Current</span>{/if}
									{#if v.version === head}<span class="stamp border-rule-strong text-ink-secondary" data-testid="run-version-{v.version}-head">Head</span>{/if}
								</div>
							</td>
						</tr>
					{:else}
						<tr><td colspan="3" class="text-ink-muted">No versions are listed for {orkflowName}.</td></tr>
					{/each}
				{/if}
			</tbody>
		</table>
		{#if versions && versions.length > shown.length}
			<a href={allHref} data-testid="run-versions-all" class="link mt-2 inline-block text-sm">All {versions.length} versions</a>
		{/if}
	{/if}
</section>
