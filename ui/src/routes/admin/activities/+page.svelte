<script lang="ts">
	import { filterFlags } from '#lib/listFilter.ts';
	// The activity catalog (§4.2): what this deployment can run.
	//
	// An activity is an image, and which images exist is engine configuration
	// read in one place (§4.2), so every row is complete or the whole read
	// failed. There is no "partial row" state here because the wire cannot
	// produce one.
	//
	// ROW ORDER IS THE RESPONSE'S OWN. §2.3 makes ListActivitiesResponse's
	// ordering part of the contract, in as many words: "A stable order is part
	// of the contract, not an implementation detail: the Admin page renders
	// it." So this page renders `data.activities` in the order it arrived and
	// sorts nothing. A client-side sort would silently take that guarantee
	// over from the server, and the next reader would not know which of the
	// two was in force.
	import { copyText, MANUAL_COPY_MESSAGE, selectByTestid } from '#lib/clipboard.ts';
	import { goto, invalidateAll } from '$app/navigation';
	import { navigating, page } from '$app/state';
	import ErrorPanel from '#lib/components/ErrorPanel.svelte';
	import ActivityIcon from '#lib/components/ActivityIcon.svelte';
	import Icon from '#lib/components/Icon.svelte';
	import CliPopover from '#lib/components/CliPopover.svelte';
	import Plate from '#lib/components/Plate.svelte';
	import DocText from '#lib/components/DocText.svelte';
	import ListFilter from '#lib/components/ListFilter.svelte';
	import { listHref } from '#lib/browsing.ts';
	import type { FieldFilter } from '#lib/listFilter.ts';
	import { namespaceLabel } from '#lib/namespace.ts';
	import { ListActivitiesRequestSchema } from '#lib/gen/orkstack/v1/activity_service_pb.ts';
	import ActivityInterface from '#lib/components/ActivityInterface.svelte';
	import { inspectCommand, interfaceSummary, pinnedReference } from '#lib/activity.ts';
	import { shortDigestWords } from '#lib/runDetail.ts';
	import { errorMessage } from '#lib/client.ts';
	import { activityClient } from '#lib/clients/activities.ts';
	import type { ActivityInfo } from '#lib/gen/orkstack/v1/activity_service_pb.ts';
	import { fmtTimestamp } from '#lib/status.ts';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	// The catalog's filters (ListFilter; docs/UI_CLI_PARITY.md "Filtering
	// lists"). `ns` rides along for links out of this page; it does not scope
	// the catalog.
	function applyFilters(filters: FieldFilter[]) {
		void goto(listHref('/admin/activities', { ns: namespaceLabel(data.namespace) }, { filter: filters }), { reset: false });
	}
	const clearHref = $derived(listHref('/admin/activities', { ns: namespaceLabel(data.namespace) }));

	// Which catalog rows have their interface disclosed. Keyed by name, which
	// the catalog guarantees unique.
	// The icons this catalog declares, for the page's key: the rest draw the default.
	const declaredKinds = $derived(data.activities.filter((activity) => activity.icon && !activity.iconError));
	let disclosed = $state<Record<string, boolean>>({});

	// Inspect a reference: InspectActivity resolves one reference on demand,
	// exactly as `orkctl activity inspect` does. An error is the server's own
	// message, never a silently empty result.
	let inspectReference = $state('');
	let inspecting = $state(false);
	let inspected = $state<ActivityInfo | null>(null);
	let inspectError = $state('');
	const inspectCli = $derived(inspectCommand(inspectReference.trim() || '<name-or-reference>'));
	async function inspect(event: SubmitEvent) {
		event.preventDefault();
		const reference = inspectReference.trim();
		inspecting = true;
		inspectError = '';
		inspected = null;
		try {
			const res = await activityClient(fetch).inspectActivity({ reference });
			inspected = res.activity ?? null;
			if (!inspected) inspectError = 'the engine answered with no activity';
		} catch (err) {
			inspectError = errorMessage(err);
		} finally {
			inspecting = false;
		}
	}

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

<header class="mb-4 flex flex-wrap items-start gap-3">
	<div class="min-w-0 flex-1">
		<h1 class="page-h1">Activities</h1>
		<p class="mt-1 text-dense text-ink-2" data-testid="activities-scope-caption">The images tasks name in <code>uses:</code>, resolved to a digest. Deployment-wide: the same whichever namespace is selected.</p>
	</div>
	<button type="button" class="btn" data-testid="activities-refresh" disabled={busy} onclick={() => void retry()}>Refresh</button>
	<CliPopover
		testid="activities-cli"
		title="Command line: activities"
		commands={[
			{ label: 'The catalog', command: ['orkctl', 'activity', 'list', ...filterFlags(data.applied)].join(' ') },
			{ label: 'Build an image, on your machine', command: 'orkctl activity build ./images/my-activity' },
			{ label: 'Run the activity contract test', command: 'orkctl activity test my-image' },
			{ label: 'Inspect a reference, with its interface', command: inspectCli }
		]}
	/>
</header>




<h2 class="sr">Catalog</h2>

<ListFilter request={ListActivitiesRequestSchema} applied={data.applied} testid="activities" command={['orkctl', 'activity', 'list']} onapply={applyFilters} {clearHref} cli={false} />

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
	<p class="text-sm text-ink-muted" data-testid="activities-loading">Loading activities…</p>
{:else if data.activities.length === 0 && data.applied.length > 0}
	<p class="text-sm text-ink-muted" data-testid="activities-empty">No activities match these filters.</p>
{:else if data.activities.length === 0}
	<p class="text-sm text-ink-muted" data-testid="activities-empty">
		This deployment has no activities configured. A task can still name any registry reference
		directly with <code>uses:</code>.
	</p>
{:else}
	<div class="card overflow-x-auto scroll-panel" data-testid="activity-list">
		<!-- The phone layout makes rows of blocks: the roles keep it a table for assistive technology. -->
		<!-- svelte-ignore a11y_no_redundant_roles -->
		<table class="register acts" role="table" aria-label="Activities">
			<thead>
				<tr>
					<th scope="col">Activity</th>
					<th scope="col">Image</th>
					<th scope="col">Digest and platforms</th>
					<th scope="col">Ops</th>
					<th scope="col">Interface</th>
				</tr>
			</thead>
			<tbody>
				{#each data.activities as activity (activity.name)}
					<!-- svelte-ignore a11y_no_redundant_roles -->
					<tr role="row" data-testid="activity-row-{activity.name}">
						<th scope="row" role="rowheader">
							<span class="inline-flex items-center gap-2 font-semibold text-ink" data-testid="activity-name-{activity.name}"
								><ActivityIcon icon={activity.icon} class="size-4 text-navy-text" />{activity.name}</span
							>
							{#if activity.docs?.description}
								<span class="mt-1 block text-dense font-normal text-ink-2"><DocText text={activity.docs.description} limit={160} subject={activity.name} testid="activity-summary-{activity.name}" /></span>
							{/if}
							{#if activity.iconError}
								<span class="mt-1 block text-dense font-normal text-ink-3" data-testid="activity-icon-error-{activity.name}">Its icon was refused: {activity.iconError}</span>
							{/if}
							{#if activity.docsError}
								<span class="mt-1 block text-dense font-normal text-ink-3" data-testid="activity-docs-error-{activity.name}">Its description was refused: {activity.docsError}</span>
							{/if}
						</th>
						<td role="cell" data-label="Image" class="font-mono text-sm text-ink-2" data-testid="activity-reference-{activity.name}">
							{#if activity.reference}{#each activity.reference.split(/(?<=\/)|(?=[:@])/) as part, i (i)}{part}<wbr />{/each}{:else}—{/if}
						</td>
						<td role="cell" data-label="Digest and platforms">
							{#if activity.indexDigest}
								<span data-digest={activity.indexDigest} data-testid="activity-digest-{activity.name}"
									><Plate key="sha256" value={shortDigestWords(activity.indexDigest)} tip={activity.indexDigest} small /></span
								>
							{:else}
								<span class="text-sm text-ink-3" data-testid="activity-digest-unresolved-{activity.name}">unresolved</span>
							{/if}
							<span class="mt-1 block font-mono text-xs text-ink-3" data-testid="activity-platforms-{activity.name}"
								>{activity.platforms.filter((p) => p !== 'unknown/unknown').join(', ') || 'single manifest'}</span
							>
							<span class="sr" data-testid="activity-media-type-{activity.name}">{activity.mediaType || '—'}</span>
							<span class="sr" data-testid="activity-resolved-at-{activity.name}">{fmtTimestamp(activity.resolvedAt)}</span>
						</td>
						<td role="cell" data-label="Ops" class="font-mono text-sm text-ink-2">
							{#if activity.interface && Object.keys(activity.interface.ops).length > 0}
								<span class="flex flex-wrap gap-1">{#each Object.keys(activity.interface.ops).sort() as op (op)}<span class="tag" data-tip={activity.docs?.ops[op]?.description || undefined}>{op}</span>{/each}</span>
							{:else}
								<span class="text-ink-3">—</span>
							{/if}
						</td>
						<td role="cell" data-label="Interface" class="text-sm">
							{#if activity.indexDigest}
								<button
									type="button"
									class="btn btn-sm btn-quiet btn-disclosure"
									aria-expanded={!!disclosed[activity.name]}
									aria-controls={disclosed[activity.name] ? `activity-interface-panel-${activity.name}` : undefined}
									data-testid="activity-interface-{activity.name}"
									onclick={() => (disclosed[activity.name] = !disclosed[activity.name])}
								>
									{#if activity.interface && !activity.interfaceError}<span class="tag">Declared</span>{' '}{/if}{interfaceSummary(activity) === 'none' ? 'None declared' : interfaceSummary(activity)}
								</button>
							{:else}
								<span class="text-ink-3" data-testid="activity-interface-{activity.name}">{interfaceSummary(activity)}</span>
							{/if}
						</td>
					</tr>
					{#if disclosed[activity.name]}
						<tr class="bg-surface-2">
							<td colspan="5" class="px-5 py-3" id="activity-interface-panel-{activity.name}" data-testid="activity-interface-panel-{activity.name}">
								<ActivityInterface info={activity} />
							</td>
						</tr>
					{/if}
				{/each}
			</tbody>
		</table>
	</div>
{/if}

<section class="card kinds mt-4" aria-labelledby="kinds-heading" data-testid="activity-kinds">
	<h2 id="kinds-heading" class="kinds-h">Task kinds</h2>
	<div class="kinds-key">
		<span><Icon name="a-default" class="size-4 text-ink-3" />default</span>
		{#each declaredKinds as activity (activity.name)}<span><ActivityIcon icon={activity.icon} class="size-4 text-ink-3" />{activity.name}</span>{/each}
		<span><Icon name="a-orkflow" class="size-4 text-ink-3" />composed task</span>
		<span><Icon name="a-await" class="size-4 text-ink-3" />awaited task</span>
	</div>
	<p class="kinds-note">An image that declares no icon or summary shows the default icon and no summary.</p>
</section>

<!--
	INSPECT A REFERENCE. Any reference `uses:` would accept — a catalog name or
	a registry reference — resolved on demand by the engine against the
	registry, uncached, writing nothing. Namespace-agnostic like the rest of
	this page.
-->
<section class="region mt-10 pt-3" data-testid="activity-inspect">
	<h2 class="mb-2 text-md font-semibold text-ink">Inspect a reference</h2>
	<form class="flex flex-wrap items-center gap-2" data-testid="activity-inspect-form" onsubmit={inspect}>
		<label class="sr-only" for="activity-inspect-reference">Catalog name or registry reference</label>
		<input
			id="activity-inspect-reference"
			type="text"
			bind:value={inspectReference}
			placeholder="file, or ghcr.io/acme/migrate:v3"
			autocomplete="off"
			spellcheck="false"
			class="field h-9 min-w-0 flex-1 px-3 font-mono text-sm sm:max-w-md"
			data-testid="activity-inspect-reference"
		/>
		<button
			type="submit"
			disabled={inspecting || inspectReference.trim() === ''}
			class="btn"
			data-testid="activity-inspect-submit">{inspecting ? 'Inspecting…' : 'Inspect'}</button
		>
	</form>
	{#if inspectError}
		<div class="mt-3">
			<ErrorPanel testid="activity-inspect-error" subject="that reference" message={inspectError} />
		</div>
	{:else if inspected}
		<div
			class="mt-3 space-y-3 border-t-(length:--rule-region) border-b border-ink py-3 text-sm"
			data-testid="activity-inspect-result"
		>
			<dl class="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1 text-xs">
				<dt>Activity</dt>
				<dd class="text-ink" data-testid="activity-inspect-activity">
					{inspected.name ? `${inspected.name} (catalog)` : `${inspected.reference} (reference)`}
				</dd>
				<dt>Reference</dt>
				<dd class="font-mono break-all text-ink-secondary">{inspected.reference}</dd>
				<dt>Pinned</dt>
				<dd class="font-mono break-all text-ink" data-testid="activity-inspect-pinned">{pinnedReference(inspected)}</dd>
				<dt>Media type</dt>
				<dd class="font-mono text-ink-secondary">{inspected.mediaType || '-'}</dd>
				<dt>Platforms</dt>
				<dd class="text-ink-secondary">{inspected.platforms.length > 0 ? inspected.platforms.join(', ') : '-'}</dd>
				<dt>Resolved</dt>
				<dd class="text-ink-secondary">{fmtTimestamp(inspected.resolvedAt)}</dd>
			</dl>
			<ActivityInterface info={inspected} />
		</div>
	{/if}
</section>

<!--
	ORKER STATUS. Presented the way the Pools page presents liveness: an
	unanswered probe is its OWN state, never drawn as a zero or an absence.
	DescribeOrker is dispatched onto the default pool's own channel (§6.5), so
	"no answer" means no Orker is serving that pool right now — which is
	fixed by starting one, not by asking again.
-->
<section class="mt-10" data-testid="orker-section" aria-labelledby="orker-heading">
	<h2 id="orker-heading" class="mb-2 text-md font-semibold text-ink">Orker</h2>
	{#if data.orkerError}
		<div
			class="border border-red px-4 py-3 text-sm"
			data-testid="orker-unavailable"
		>
			<p class="severity-warning font-semibold">No Orker answered the default pool.</p>
			<p class="mt-1 break-words text-ink-secondary">{data.orkerError}</p>
			<p class="mt-1 text-xs text-ink-secondary">
				Nothing was measured. Start an Orker for this deployment's default pool — this is not a
				report that zero Orkers are healthy.
			</p>
		</div>
	{:else if !data.orker}
		<p class="text-sm text-ink-muted" data-testid="orker-empty">
			The probe returned no Orker description.
		</p>
	{:else}
		{@const orker = data.orker}
		<!-- The Orker's facts in the title block's anatomy. -->
		<dl
			class="fstrip card p-4"
			data-testid="orker-card"
		>
			<div class="min-w-0">
				<dt>Orker</dt>
				<dd class="font-mono text-sm break-words" data-testid="orker-id">{orker.orkerId || '—'}</dd>
			</div>
			<div class="min-w-0">
				<dt>Pool</dt>
				<dd class="font-mono text-sm break-words" data-testid="orker-pool">{orker.pool || '—'}</dd>
			</div>
			<div class="min-w-0">
				<dt>Runtime</dt>
				<dd class="break-words" data-testid="orker-runtime">{orker.runtimeName || '—'}{orker.runtimeVersion ? ` ${orker.runtimeVersion}` : ''}</dd>
			</div>
			<div class="min-w-0">
				<dt>Build</dt>
				<dd class="font-mono text-sm break-words" data-testid="orker-build">{orker.buildId || 'unversioned'}</dd>
			</div>
			<div class="min-w-0">
				<dt>Security profile</dt>
				<dd class="[overflow-wrap:anywhere]" data-testid="orker-security-profile">{orker.securityProfile || '—'}</dd>
			</div>
			<div class="min-w-0">
				<dt>Max concurrent</dt>
				<dd class="font-mono text-sm break-words" data-testid="orker-max-concurrent">{String(orker.maxConcurrent)}</dd>
			</div>
			<div class="min-w-0">
				<dt>Rootless</dt>
				<dd class="break-words" data-testid="orker-rootless">{orker.rootless ? 'yes' : 'no'}</dd>
			</div>
		</dl>
	{/if}
</section>

<style>
	.kinds {
		display: grid;
		gap: 10px;
		padding: 14px 16px;
	}
	.kinds-h {
		margin: 0;
		font-size: 15px;
		font-weight: 650;
	}
	.kinds-key {
		display: flex;
		flex-wrap: wrap;
		gap: 4px 14px;
		font-size: 13px;
		color: var(--ink-2);
	}
	.kinds-key span {
		display: inline-flex;
		align-items: center;
		gap: 8px;
	}
	.kinds-note {
		margin: 0;
		font-size: 13px;
		color: var(--ink-3);
	}
	@media (max-width: 640px) {
		.acts thead {
			display: none;
		}
		.acts,
		.acts tbody,
		.acts tr,
		.acts td,
		.acts th {
			display: block;
		}
		.acts tr {
			display: grid;
			grid-template-columns: minmax(0, 1fr);
			gap: 10px;
			padding: 14px 16px;
			border-bottom: 1px solid var(--line);
		}
		.acts td,
		.acts th {
			padding: 0 !important;
			border: 0 !important;
		}
		.acts td[data-label]::before {
			content: attr(data-label);
			display: block;
			margin-bottom: 3px;
			font-family: var(--font-ui);
			font-size: 12.5px;
			font-weight: 600;
			color: var(--ink-3);
		}
	}
</style>
