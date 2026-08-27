<script lang="ts">
	// The activity catalog (design §4.2): what this deployment can run.
	//
	// This page replaces the plugin inventory. The difference is not cosmetic:
	// a plugin was a Go type compiled into an Orker and the inventory was a
	// question asked OF the Orkers, so it could disagree between pools and
	// arrive partially filled. An activity is an IMAGE, and which images exist
	// is engine configuration read in one place, so every row is complete or
	// the whole read failed. There is no "partial row" state here because the
	// wire can no longer produce one.
	//
	// ROW ORDER IS THE RESPONSE'S OWN. §2.3 makes ListActivitiesResponse's
	// ordering part of the contract, in as many words: "A stable order is part
	// of the contract, not an implementation detail: the Admin page renders
	// it." So this page renders `data.activities` in the order it arrived and
	// sorts nothing. A client-side sort would silently take that guarantee
	// over from the server, and the next reader would not know which of the
	// two was in force.
	import { invalidateAll } from '$app/navigation';
	import { navigating, page } from '$app/state';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import { shortDigest } from '$lib/activity';
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

	// navigatingHere covers a re-entry into this same URL a delayed RPC could
	// stall on (the Retry click sets `retrying` itself, since it calls
	// invalidateAll directly rather than goto).
	const navigatingHere = $derived(
		navigating.to !== null && navigating.to.url.pathname === page.url.pathname
	);
	const busy = $derived(retrying || navigatingHere);
</script>

<svelte:head><title>Admin — Activities — OrkStack</title></svelte:head>

<p class="mb-4 text-sm text-zinc-500" data-testid="activities-scope-caption">
	Every activity this deployment can run, as configured on the engine — deployment-wide, not scoped
	by the namespace selector above. An activity is an OCI image; a task names one with
	<code class="rounded bg-zinc-800 px-1 py-0.5 text-xs text-zinc-300">uses:</code>.
</p>

<!--
	ORKER STATUS. Presented the way the Pools page presents liveness: an
	unanswered probe is its OWN state, never drawn as a zero or an absence.
	DescribeOrker is dispatched onto the default pool's queue (§6.5), so
	"no answer" means no Orker is serving that pool right now — which is
	fixed by starting one, not by asking again.
-->
<section class="mb-6" data-testid="orker-section">
	<h2 class="mb-2 text-sm font-semibold uppercase tracking-wide text-zinc-400">Orker</h2>
	{#if data.orkerError}
		<div
			class="rounded-lg border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-300"
			data-testid="orker-unavailable"
		>
			<p class="font-medium text-amber-200">No Orker answered the default pool.</p>
			<p class="mt-1 break-words text-amber-300/80">{data.orkerError}</p>
			<p class="mt-1 text-xs text-amber-300/60">
				Nothing was measured. Start an Orker for this deployment's default pool — this is not a
				report that zero Orkers are healthy.
			</p>
		</div>
	{:else if !data.orker}
		<p class="text-sm text-zinc-400" data-testid="orker-empty">
			The probe returned no Orker description.
		</p>
	{:else}
		{@const orker = data.orker}
		<div
			class="grid grid-cols-2 gap-4 rounded-lg border border-zinc-800 p-4 text-sm sm:grid-cols-4"
			data-testid="orker-card"
		>
			<div>
				<div class="text-xs uppercase text-zinc-500">Orker</div>
				<div class="mt-1 font-mono text-zinc-200" data-testid="orker-id">{orker.orkerId || '—'}</div>
			</div>
			<div>
				<div class="text-xs uppercase text-zinc-500">Pool</div>
				<div class="mt-1 text-zinc-200" data-testid="orker-pool">{orker.pool || '—'}</div>
			</div>
			<div>
				<div class="text-xs uppercase text-zinc-500">Runtime</div>
				<div class="mt-1 text-zinc-200" data-testid="orker-runtime">
					{orker.runtimeName || '—'}{orker.runtimeVersion ? ` ${orker.runtimeVersion}` : ''}
				</div>
			</div>
			<div>
				<div class="text-xs uppercase text-zinc-500">Build</div>
				<div class="mt-1 font-mono text-xs text-zinc-300" data-testid="orker-build">
					{orker.buildId || 'unversioned'}
				</div>
			</div>
			<div>
				<div class="text-xs uppercase text-zinc-500">Security profile</div>
				<div class="mt-1 text-zinc-200" data-testid="orker-security-profile">
					{orker.securityProfile || '—'}
				</div>
			</div>
			<div>
				<div class="text-xs uppercase text-zinc-500">Max concurrent</div>
				<div class="mt-1 text-zinc-200" data-testid="orker-max-concurrent">
					{orker.maxConcurrent}
				</div>
			</div>
			<div>
				<div class="text-xs uppercase text-zinc-500">Rootless</div>
				<div class="mt-1 text-zinc-200" data-testid="orker-rootless">
					{orker.rootless ? 'yes' : 'no'}
				</div>
			</div>
		</div>
	{/if}
</section>

<h2 class="mb-2 text-sm font-semibold uppercase tracking-wide text-zinc-400">Catalog</h2>

{#if data.activitiesError}
	<ErrorPanel
		testid="activities-error"
		retryTestid="activities-retry"
		subject="the activity catalog"
		message={data.activitiesError}
		busy={retrying}
		onretry={retry}
	/>
{:else if busy}
	<p class="text-sm text-zinc-400" data-testid="activities-loading">Loading activities…</p>
{:else if data.activities.length === 0}
	<p class="text-sm text-zinc-400" data-testid="activities-empty">
		This deployment has no activities configured. A task can still name any registry reference
		directly with <code class="rounded bg-zinc-800 px-1 py-0.5 text-xs">uses:</code>.
	</p>
{:else}
	<div class="overflow-hidden rounded-lg border border-zinc-800" data-testid="activity-list">
		<table class="w-full text-left text-sm">
			<thead class="bg-zinc-900 text-xs uppercase tracking-wide text-zinc-400">
				<tr>
					<th class="px-4 py-3">Name</th>
					<th class="px-4 py-3">Reference</th>
					<th class="px-4 py-3">Index digest</th>
					<th class="px-4 py-3">Media type</th>
					<th class="px-4 py-3">Platforms</th>
					<th class="px-4 py-3">Resolved</th>
				</tr>
			</thead>
			<tbody class="divide-y divide-zinc-800">
				{#each data.activities as activity (activity.name)}
					<tr class="hover:bg-zinc-900/60" data-testid="activity-row-{activity.name}">
						<td class="px-4 py-3 font-medium text-zinc-100" data-testid="activity-name-{activity.name}">
							{activity.name}
						</td>
						<td
							class="px-4 py-3 font-mono text-xs text-zinc-300"
							data-testid="activity-reference-{activity.name}"
						>
							{activity.reference || '—'}
						</td>
						<!--
							The digest is the PIN, and the pin is the point: it is
							what makes a run explainable after a tag moves. It is
							abbreviated for the eye but carried in full in `title`
							and in a data attribute, so nothing here is the only
							copy of the value.
						-->
						<td class="px-4 py-3">
							{#if activity.indexDigest}
								<span
									class="font-mono text-xs text-emerald-300"
									title={activity.indexDigest}
									data-digest={activity.indexDigest}
									data-testid="activity-digest-{activity.name}">{shortDigest(activity.indexDigest)}</span
								>
							{:else}
								<span
									class="text-xs text-zinc-600"
									data-testid="activity-digest-unresolved-{activity.name}"
									title="this reference has not been resolved to an index digest">unresolved</span
								>
							{/if}
						</td>
						<td
							class="px-4 py-3 font-mono text-xs text-zinc-400"
							data-testid="activity-media-type-{activity.name}"
						>
							{activity.mediaType || '—'}
						</td>
						<td class="px-4 py-3 text-xs text-zinc-400" data-testid="activity-platforms-{activity.name}">
							{#if activity.platforms.length > 0}
								{activity.platforms.join(', ')}
							{:else}
								<span class="text-zinc-600" title="a single-manifest reference advertises no platform list"
									>single manifest</span
								>
							{/if}
						</td>
						<td class="px-4 py-3 text-xs text-zinc-500" data-testid="activity-resolved-at-{activity.name}">
							{fmtTimestamp(activity.resolvedAt)}
						</td>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
{/if}
