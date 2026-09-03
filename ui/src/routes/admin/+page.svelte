<script lang="ts">
	import { invalidateAll } from '$app/navigation';
	import { navigating, page } from '$app/state';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import { withNamespace } from '$lib/namespace';
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

	const cards = [
		{
			href: '/admin/namespaces',
			testid: 'admin-card-namespaces',
			title: 'Namespaces',
			body: 'Create, edit and delete namespaces — every one this deployment has, not just the one selected above.'
		},
		{
			href: '/admin/pools',
			testid: 'admin-card-pools',
			title: 'Pools',
			body: 'Live Orker counts for the pools the SELECTED namespace may dispatch onto.'
		},
		{
			href: '/admin/activities',
			testid: 'admin-card-activities',
			title: 'Activities',
			body: 'Every activity this deployment can run, each pinned to the image digest it resolved to.'
		},
		{
			href: '/admin/retention',
			testid: 'admin-card-retention',
			title: 'Maximum run retention',
			body: 'The longest any namespace may keep a finished run; each namespace may choose less.'
		}
	];
</script>

<svelte:head><title>Admin overview — OrkStack</title></svelte:head>

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
	<p class="text-sm text-fg-muted" data-testid="system-loading">Loading system information…</p>
{:else if !data.system}
	<p class="text-sm text-fg-muted" data-testid="system-empty">
		The engine returned no system information.
	</p>
{:else}
	<section class="mb-8" data-testid="system-info">
		<div class="mb-3 flex items-baseline justify-between">
			<h2 class="text-sm font-semibold text-fg">This deployment</h2>
			<span class="text-xs text-fg-muted">describes the replica that answered</span>
		</div>
		<div class="overflow-hidden rounded-lg border border-border bg-surface shadow-xs">
			<dl class="divide-y divide-border-subtle text-sm">
				<div class="grid grid-cols-[10rem_1fr] px-4 py-2.5">
					<dt class="text-fg-muted">Engine version</dt>
					<dd class="text-fg" data-testid="system-engine-version">
						{data.system.engineVersion || '—'}
					</dd>
				</div>
				<div class="grid grid-cols-[10rem_1fr] px-4 py-2.5">
					<dt class="text-fg-muted">Go version</dt>
					<dd class="text-fg" data-testid="system-go-version">
						{data.system.goVersion || '—'}
					</dd>
				</div>
				<div class="grid grid-cols-[10rem_1fr] px-4 py-2.5">
					<dt class="text-fg-muted">Deployment</dt>
					<dd class="text-fg" data-testid="system-deployment">
						{data.system.deploymentName || '—'}
					</dd>
				</div>
				<div class="grid grid-cols-[10rem_1fr] px-4 py-2.5">
					<dt class="text-fg-muted">Versioning</dt>
					<dd class="text-fg" data-testid="system-versioning">
						{#if data.system.versioningActive}
							on (build {data.system.buildId})
						{:else}
							off
						{/if}
					</dd>
				</div>
				<div class="grid grid-cols-[10rem_1fr] px-4 py-2.5">
					<dt class="text-fg-muted">Artifacts</dt>
					<dd class="text-fg" data-testid="system-artifact-backend">
						{data.system.artifactLocation || '—'} ({data.system.artifactBackend || '—'})
					</dd>
				</div>
				<div class="grid grid-cols-[10rem_1fr] px-4 py-2.5">
					<dt class="text-fg-muted">Temporal address</dt>
					<dd class="text-fg" data-testid="system-temporal-address">
						{data.system.temporalAddress || '—'}
					</dd>
				</div>
				<div class="grid grid-cols-[10rem_1fr] px-4 py-2.5">
					<dt class="text-fg-muted">Server time</dt>
					<dd class="text-fg" data-testid="system-server-time">
						{fmtTimestamp(data.system.serverTime)}
					</dd>
				</div>
			</dl>
		</div>
		<p class="mt-2 text-xs text-fg-muted">
			Everything above except the server time is this replica's own configuration and build, not a
			measurement of the cluster — see <code>orkctl admin info</code>.
		</p>
	</section>
{/if}

<section>
	<h2 class="mb-3 text-sm font-semibold text-fg">Manage</h2>
	<div class="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
		{#each cards as card (card.testid)}
			<a
				href={withNamespace(card.href, data.namespace)}
				data-testid={card.testid}
				class="rounded-lg border border-border bg-surface p-4 shadow-xs transition-colors duration-150 hover:border-border-strong hover:bg-surface-hover"
			>
				<h3 class="font-medium text-fg">{card.title}</h3>
				<p class="mt-1 text-sm text-fg-muted">{card.body}</p>
			</a>
		{/each}
	</div>
</section>
