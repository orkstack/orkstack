<script lang="ts">
	import { invalidateAll } from '$app/navigation';
	import { navigating, page } from '$app/state';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import { withNamespace } from '$lib/namespace';
	import SheetHeader from '$lib/components/SheetHeader.svelte';
	import { OPERATE_PAGES, operateScope } from '$lib/navigation';
	import { fmtTimestamp } from '$lib/status';
	import type { PageProps } from './$types';

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

	// navigatingHere covers a re-entry into this same URL a delayed RPC
	// could stall on (the Retry click above sets `retrying` itself, since it
	// calls `invalidateAll` directly rather than `goto`).
	const navigatingHere = $derived(
		navigating.to !== null && navigating.to.url.pathname === page.url.pathname
	);
	const busy = $derived(retrying || navigatingHere);

	// The Operate pages as an index: what each governs and how far its figures
	// reach. Every page is one link, in the strip's order.
	const governs = [
		{ id: 'namespaces', testid: 'admin-card-namespaces', governs: 'Create, edit and delete namespaces — every one this deployment has, not just the one selected above.' },
		{ id: 'pools', testid: 'admin-card-pools', governs: 'Live Orker counts for the pools the selected namespace may dispatch onto.' },
		{ id: 'releases', testid: 'admin-card-releases', governs: 'Which release takes new runs, a ramp or promotion, and what still holds a previous release.' },
		{ id: 'activities', testid: 'admin-card-activities', governs: 'Every activity this deployment can run, each pinned to the image digest it resolved to.' },
		{ id: 'webhooks', testid: 'admin-card-webhooks', governs: 'Where run events are delivered, and what each delivery answered.' },
		{ id: 'automation', testid: 'admin-card-automation', governs: 'Plan, export and apply the selected namespace’s automation document.' },
		{ id: 'retention', testid: 'admin-card-retention', governs: 'The longest any namespace may keep a finished run; each namespace may choose less.' },
		{ id: 'usage', testid: 'admin-card-usage', governs: 'What each namespace reserved over a window, beside its budget.' },
		{ id: 'cache', testid: 'admin-card-cache', governs: 'How cached tasks fared, the memos stored now, and purging them.' },
		{ id: 'image-policy', testid: 'admin-card-image-policy', governs: 'The floor every image must meet: digests, registries and signing keys.' },
		{ id: 'audit', testid: 'admin-card-audit', governs: 'Every recorded write, with the principal its gateway asserted.' }
	];
	const index = governs.map((entry) => ({ ...entry, page: OPERATE_PAGES.find((p) => p.id === entry.id)! }));
</script>

<svelte:head><title>Admin overview — OrkStack</title></svelte:head>

<SheetHeader title="Operate">
	{#snippet meta()}Manage namespaces, watch pool health, and read what this deployment is made of.{/snippet}
</SheetHeader>

{#if data.systemError}
	<ErrorPanel
		testid="system-error"
		retryTestid="system-retry"
		subject="this deployment's system information"
		message={data.systemError}
		busy={retrying}
		onretry={retry}
	/>
{:else if busy}
	<p class="text-sm text-ink-muted" data-testid="system-loading">Loading system information…</p>
{:else if !data.system}
	<p class="text-sm text-ink-muted" data-testid="system-empty">
		The engine returned no system information.
	</p>
{:else}
	<section class="mb-10" data-testid="system-info" aria-labelledby="system-info-heading">
		<div class="mb-2 flex flex-wrap items-baseline justify-between gap-x-4">
			<h2 id="system-info-heading" class="text-md font-semibold text-ink">This deployment</h2>
			<span class="text-xs text-ink-muted">describes the replica that answered</span>
		</div>
		<!-- The deployment's facts in the title block's anatomy: a caps label
			over its value, cells divided by hairlines inside an ink frame. -->
		<dl class="grid grid-cols-1 border-t-(length:--rule-frame) border-b border-ink sm:grid-cols-2 lg:grid-cols-4">
			<div class="min-w-0 border-b border-rule px-3 py-2 sm:border-r">
				<dt class="caps text-ink-muted">Engine version</dt>
				<dd class="figure mt-0.5 text-sm font-semibold break-words text-ink" data-testid="system-engine-version">{data.system.engineVersion || '—'}</dd>
			</div>
			<div class="min-w-0 border-b border-rule px-3 py-2 sm:border-r">
				<dt class="caps text-ink-muted">Go version</dt>
				<dd class="figure mt-0.5 text-sm font-semibold break-words text-ink" data-testid="system-go-version">{data.system.goVersion || '—'}</dd>
			</div>
			<div class="min-w-0 border-b border-rule px-3 py-2 sm:border-r">
				<dt class="caps text-ink-muted">Deployment</dt>
				<dd class="mt-0.5 text-md font-semibold break-words text-ink" data-testid="system-deployment">{data.system.deploymentName || '—'}</dd>
			</div>
			<div class="min-w-0 border-b border-rule px-3 py-2 sm:border-r">
				<dt class="caps text-ink-muted">Versioning</dt>
				<dd class="mt-0.5 text-md font-semibold break-words text-ink" data-testid="system-versioning">{data.system.versioningActive ? `on (build ${data.system.buildId})` : 'off'}</dd>
			</div>
			<div class="min-w-0 border-b border-rule px-3 py-2 sm:border-r">
				<dt class="caps text-ink-muted">Artifacts</dt>
				<dd class="figure mt-0.5 text-sm font-semibold break-words text-ink" data-testid="system-artifact-backend">{data.system.artifactLocation || '—'} ({data.system.artifactBackend || '—'})</dd>
			</div>
			<div class="min-w-0 border-b border-rule px-3 py-2 sm:border-r">
				<dt class="caps text-ink-muted">Temporal address</dt>
				<dd class="figure mt-0.5 text-sm font-semibold break-words text-ink" data-testid="system-temporal-address">{data.system.temporalAddress || '—'}</dd>
			</div>
			<div class="min-w-0 border-b border-rule px-3 py-2 sm:border-r">
				<dt class="caps text-ink-muted">Server time</dt>
				<dd class="figure mt-0.5 text-sm font-semibold break-words text-ink" data-testid="system-server-time">{fmtTimestamp(data.system.serverTime)}</dd>
			</div>
		</dl>
		<p class="mt-2 max-w-prose text-xs text-ink-muted">
			Everything above except the server time is this replica's own configuration and build, not a
			measurement of the cluster — see <code>orkctl admin info</code>.
		</p>
	</section>
{/if}

<section aria-labelledby="operate-index-heading">
	<h2 id="operate-index-heading" class="mb-2 text-md font-semibold text-ink">Manage</h2>
	<div class="overflow-x-auto border-t-2 border-ink">
		<table class="register min-w-[640px]" data-testid="operate-index">
			<thead>
				<tr>
					<th scope="col" class="w-40">Page</th>
					<th scope="col">What it governs</th>
					<th scope="col" class="w-40">Scope</th>
				</tr>
			</thead>
			<tbody>
				{#each index as entry (entry.id)}
					<tr class="transition-colors hover:bg-hover">
						<td>
							<a href={withNamespace(entry.page.href, data.namespace)} data-testid={entry.testid} class="link font-semibold">{entry.page.label}</a>
						</td>
						<td class="text-ink-secondary">{entry.governs}</td>
						<td class="text-ink-secondary">{operateScope(entry.id)}</td>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
</section>
