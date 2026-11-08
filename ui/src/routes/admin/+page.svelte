<script lang="ts">
	import { invalidateAll } from '$app/navigation';
	import { navigating, page } from '$app/state';
	import ErrorPanel from '#lib/components/ErrorPanel.svelte';
	import SheetHeader from '#lib/components/SheetHeader.svelte';
	import { fmtTimestamp } from '#lib/status.ts';
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
</script>

<svelte:head><title>Admin overview — OrkStack</title></svelte:head>

<SheetHeader title="Operate" commands={[{ label: "This replica's configuration and build", command: 'orkctl admin info' }]} />

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
	<section class="card" data-testid="system-info" aria-labelledby="system-info-heading">
		<div class="card-head">
			<h2 id="system-info-heading">This deployment</h2>
		</div>
		<div class="grid gap-3 p-4">
			<!-- Two strips, so a wrapped row never starts indented behind a rule. -->
			<dl class="fstrip">
				<div><dt>Engine version</dt><dd class="font-mono" data-testid="system-engine-version">{data.system.engineVersion || '—'}</dd></div>
				<div><dt>Go version</dt><dd class="font-mono" data-testid="system-go-version">{data.system.goVersion || '—'}</dd></div>
				<div><dt>Deployment</dt><dd data-testid="system-deployment">{data.system.deploymentName || '—'}</dd></div>
				<div><dt>Versioning</dt><dd data-testid="system-versioning">{data.system.versioningActive ? `on (build ${data.system.buildId})` : 'off'}</dd></div>
			</dl>
			<dl class="fstrip">
				<div><dt>Artifacts</dt><dd class="font-mono" data-testid="system-artifact-backend">{data.system.artifactLocation || '—'} ({data.system.artifactBackend || '—'})</dd></div>
				<div><dt>Temporal address</dt><dd class="font-mono" data-testid="system-temporal-address">{data.system.temporalAddress || '—'}</dd></div>
				<div><dt>Server time</dt><dd data-testid="system-server-time">{fmtTimestamp(data.system.serverTime)}</dd></div>
			</dl>
			<p class="text-sm text-ink-3">
				The replica that answered: its own configuration and build, not a measurement of the cluster.
			</p>
		</div>
	</section>
{/if}
