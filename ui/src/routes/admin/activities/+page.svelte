<script lang="ts">
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
	import { goto, invalidateAll } from '$app/navigation';
	import { navigating, page } from '$app/state';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import ListFilter from '$lib/components/ListFilter.svelte';
	import { listHref } from '$lib/browsing';
	import type { FieldFilter } from '$lib/listFilter';
	import { namespaceLabel } from '$lib/namespace';
	import { ListActivitiesRequestSchema } from '$lib/gen/orkstack/v1/service_pb';
	import ActivityInterface from '$lib/components/ActivityInterface.svelte';
	import CliEquivalent from '$lib/components/CliEquivalent.svelte';
	import { inspectCommand, interfaceSummary, pinnedReference, shortDigest } from '$lib/activity';
	import { errorMessage, orkstack } from '$lib/client';
	import type { ActivityInfo } from '$lib/gen/orkstack/v1/service_pb';
	import { fmtTimestamp } from '$lib/status';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	// The catalog's filters (ListFilter; docs/UI_CLI_PARITY.md "Filtering
	// lists"). `ns` rides along for links out of this page; it does not scope
	// the catalog.
	function applyFilters(filters: FieldFilter[]) {
		void goto(listHref('/admin/activities', { ns: namespaceLabel(data.namespace) }, { filter: filters }), { keepFocus: true, noScroll: true });
	}
	const clearHref = $derived(listHref('/admin/activities', { ns: namespaceLabel(data.namespace) }));

	// Which catalog rows have their interface disclosed. Keyed by name, which
	// the catalog guarantees unique.
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
			const res = await orkstack(fetch).activities.inspectActivity({ reference });
			inspected = res.activity ?? null;
			if (!inspected) inspectError = 'the engine answered with no activity';
		} catch (err) {
			inspectError = errorMessage(err);
		} finally {
			inspecting = false;
		}
	}

	let retrying = $state(false);
	let copiedLocalTool = $state('');
	function copyLocalTool(command: string, label: string) {
		void navigator.clipboard?.writeText(command).then(() => (copiedLocalTool = label));
	}
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

<p class="mb-4 text-sm text-fg-muted" data-testid="activities-scope-caption">
	Every activity this deployment can run, as configured on the engine — deployment-wide, not scoped
	by the namespace selector above. An activity is an OCI image; a task names one with
	<code>uses:</code>.
</p>

<!--
	ORKER STATUS. Presented the way the Pools page presents liveness: an
	unanswered probe is its OWN state, never drawn as a zero or an absence.
	DescribeOrker is dispatched onto the default pool's own channel (§6.5), so
	"no answer" means no Orker is serving that pool right now — which is
	fixed by starting one, not by asking again.
-->
<section class="mb-6" data-testid="orker-section">
	<h2 class="mb-2 text-[11px] font-medium tracking-wider text-fg-muted uppercase">Orker</h2>
	{#if data.orkerError}
		<div
			class="rounded-md border border-warning-200 bg-warning-50 px-4 py-3 text-sm"
			data-testid="orker-unavailable"
		>
			<p class="font-medium text-warning-800">No Orker answered the default pool.</p>
			<p class="mt-1 break-words text-warning-700">{data.orkerError}</p>
			<p class="mt-1 text-xs text-warning-700/80">
				Nothing was measured. Start an Orker for this deployment's default pool — this is not a
				report that zero Orkers are healthy.
			</p>
		</div>
	{:else if !data.orker}
		<p class="text-sm text-fg-muted" data-testid="orker-empty">
			The probe returned no Orker description.
		</p>
	{:else}
		{@const orker = data.orker}
		<div
			class="grid grid-cols-2 gap-4 rounded-lg border border-border bg-surface p-4 text-sm shadow-xs sm:grid-cols-4"
			data-testid="orker-card"
		>
			<div>
				<div class="text-xs font-medium text-fg-muted">Orker</div>
				<div class="mt-1 font-mono text-fg-secondary" data-testid="orker-id">{orker.orkerId || '—'}</div>
			</div>
			<div>
				<div class="text-xs font-medium text-fg-muted">Pool</div>
				<div class="mt-1 text-fg-secondary" data-testid="orker-pool">{orker.pool || '—'}</div>
			</div>
			<div>
				<div class="text-xs font-medium text-fg-muted">Runtime</div>
				<div class="mt-1 text-fg-secondary" data-testid="orker-runtime">
					{orker.runtimeName || '—'}{orker.runtimeVersion ? ` ${orker.runtimeVersion}` : ''}
				</div>
			</div>
			<div>
				<div class="text-xs font-medium text-fg-muted">Build</div>
				<div class="mt-1 font-mono text-xs text-fg-secondary" data-testid="orker-build">
					{orker.buildId || 'unversioned'}
				</div>
			</div>
			<div>
				<div class="text-xs font-medium text-fg-muted">Security profile</div>
				<div class="mt-1 text-fg-secondary" data-testid="orker-security-profile">
					{orker.securityProfile || '—'}
				</div>
			</div>
			<div>
				<div class="text-xs font-medium text-fg-muted">Max concurrent</div>
				<div class="mt-1 text-fg-secondary" data-testid="orker-max-concurrent">
					{orker.maxConcurrent}
				</div>
			</div>
			<div>
				<div class="text-xs font-medium text-fg-muted">Rootless</div>
				<div class="mt-1 text-fg-secondary" data-testid="orker-rootless">
					{orker.rootless ? 'yes' : 'no'}
				</div>
			</div>
		</div>
	{/if}
</section>

<section class="mb-6 rounded-lg border border-border bg-surface p-4 shadow-xs" data-testid="activity-local-tools">
	<h2 class="text-sm font-semibold text-fg">Local activity tools</h2>
	<p class="mt-1 text-sm text-fg-muted">
		Builds and contract tests run on your machine with its OCI runtime. They are intentionally not browser actions.
	</p>
	<p class="mt-2 text-xs text-fg-muted">Prerequisites: a local build context, Docker or another supported OCI runtime, and access to the image registry when pushing.</p>
	<div class="mt-3 grid gap-3 lg:grid-cols-2">
		<div class="rounded-md bg-surface-sunken p-3"><p class="text-xs font-medium text-fg">Build an image</p><code data-testid="activity-build-command" class="mt-1 block break-all font-mono text-xs text-fg-secondary">orkctl activity build ./images/my-activity</code><button type="button" onclick={() => copyLocalTool('orkctl activity build ./images/my-activity', 'Build command copied')} class="mt-2 text-xs font-medium text-brand-700 hover:underline">Copy build command</button></div>
		<div class="rounded-md bg-surface-sunken p-3"><p class="text-xs font-medium text-fg">Run the activity contract test</p><code data-testid="activity-test-command" class="mt-1 block break-all font-mono text-xs text-fg-secondary">orkctl activity test my-image</code><button type="button" onclick={() => copyLocalTool('orkctl activity test my-image', 'Test command copied')} class="mt-2 text-xs font-medium text-brand-700 hover:underline">Copy test command</button></div>
	</div>
	{#if copiedLocalTool}<p data-testid="activity-local-tool-copied" class="mt-2 text-xs text-fg-muted">{copiedLocalTool}</p>{/if}
</section>

<h2 class="mb-2 text-[11px] font-medium tracking-wider text-fg-muted uppercase">Catalog</h2>

<ListFilter request={ListActivitiesRequestSchema} applied={data.applied} testid="activities" command={['orkctl', 'activity', 'list']} onapply={applyFilters} {clearHref} />

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
	<p class="text-sm text-fg-muted" data-testid="activities-loading">Loading activities…</p>
{:else if data.activities.length === 0 && data.applied.length > 0}
	<p class="text-sm text-fg-muted" data-testid="activities-empty">No activities match these filters.</p>
{:else if data.activities.length === 0}
	<p class="text-sm text-fg-muted" data-testid="activities-empty">
		This deployment has no activities configured. A task can still name any registry reference
		directly with <code>uses:</code>.
	</p>
{:else}
	<div class="overflow-x-auto rounded-lg border border-border bg-surface shadow-xs" data-testid="activity-list">
		<table class="w-full min-w-[960px] border-collapse text-left text-sm">
			<thead class="bg-surface-sunken">
				<tr>
					<th class="h-10 px-4 pl-5 text-xs font-medium text-fg-muted">Name</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">Reference</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">Index digest</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">Media type</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">Platforms</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">Interface</th>
					<th class="h-10 px-4 pr-5 text-xs font-medium text-fg-muted">Resolved</th>
				</tr>
			</thead>
			<tbody>
				{#each data.activities as activity (activity.name)}
					<tr
						class="border-t border-border-subtle transition-colors duration-150 hover:bg-surface-hover"
						data-testid="activity-row-{activity.name}"
					>
						<td class="h-11 px-4 pl-5 align-middle font-medium text-fg" data-testid="activity-name-{activity.name}">
							{activity.name}
						</td>
						<td
							class="h-11 px-4 align-middle font-mono text-xs text-fg-secondary"
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
						<td class="h-11 px-4 align-middle">
							{#if activity.indexDigest}
								<span
									class="font-mono text-xs text-brand-700"
									title={activity.indexDigest}
									data-digest={activity.indexDigest}
									data-testid="activity-digest-{activity.name}">{shortDigest(activity.indexDigest)}</span
								>
							{:else}
								<span
									class="text-xs text-fg-muted"
									data-testid="activity-digest-unresolved-{activity.name}"
									title="this reference has not been resolved to an index digest">unresolved</span
								>
							{/if}
						</td>
						<td
							class="h-11 px-4 align-middle font-mono text-xs text-fg-muted"
							data-testid="activity-media-type-{activity.name}"
						>
							{activity.mediaType || '—'}
						</td>
						<td class="h-11 px-4 align-middle text-xs text-fg-muted" data-testid="activity-platforms-{activity.name}">
							{#if activity.platforms.length > 0}
								{activity.platforms.join(', ')}
							{:else}
								<span title="a single-manifest reference advertises no platform list"
									>single manifest</span
								>
							{/if}
						</td>
						<!--
							What the image declares about its with: block. The cell
							is a disclosure: the full table opens in a row beneath,
							so the catalog stays one line per activity.
						-->
						<td class="h-11 px-4 align-middle text-xs">
							{#if activity.indexDigest}
								<button
									type="button"
									class="inline-flex items-center gap-1 rounded text-brand-700 hover:underline focus-visible:ring-2 focus-visible:ring-brand-500/40 focus-visible:outline-none"
									aria-expanded={!!disclosed[activity.name]}
									aria-controls="activity-interface-panel-{activity.name}"
									data-testid="activity-interface-{activity.name}"
									onclick={() => (disclosed[activity.name] = !disclosed[activity.name])}
								>
									<span aria-hidden="true">{disclosed[activity.name] ? '▾' : '▸'}</span>{interfaceSummary(activity)}
								</button>
							{:else}
								<span class="text-fg-muted" data-testid="activity-interface-{activity.name}">{interfaceSummary(activity)}</span>
							{/if}
						</td>
						<td class="h-11 px-4 pr-5 align-middle text-xs text-fg-muted" data-testid="activity-resolved-at-{activity.name}">
							{fmtTimestamp(activity.resolvedAt)}
						</td>
					</tr>
					{#if disclosed[activity.name]}
						<tr class="border-t border-border-subtle bg-surface-sunken/40">
							<td
								colspan="7"
								class="px-5 py-3"
								id="activity-interface-panel-{activity.name}"
								data-testid="activity-interface-panel-{activity.name}"
							>
								<ActivityInterface info={activity} />
							</td>
						</tr>
					{/if}
				{/each}
			</tbody>
		</table>
	</div>
{/if}

<!--
	INSPECT A REFERENCE. Any reference `uses:` would accept — a catalog name or
	a registry reference — resolved on demand by the engine against the
	registry, uncached, writing nothing. Namespace-agnostic like the rest of
	this page.
-->
<section class="mt-6" data-testid="activity-inspect">
	<h2 class="mb-2 text-[11px] font-medium tracking-wider text-fg-muted uppercase">Inspect a reference</h2>
	<form class="flex flex-wrap items-center gap-2" data-testid="activity-inspect-form" onsubmit={inspect}>
		<label class="sr-only" for="activity-inspect-reference">Catalog name or registry reference</label>
		<input
			id="activity-inspect-reference"
			type="text"
			bind:value={inspectReference}
			placeholder="file, or ghcr.io/acme/migrate:v3"
			autocomplete="off"
			spellcheck="false"
			class="h-9 min-w-0 flex-1 rounded-md border border-border bg-surface px-3 font-mono text-sm text-fg shadow-xs focus-visible:ring-2 focus-visible:ring-brand-500/40 focus-visible:outline-none sm:max-w-md"
			data-testid="activity-inspect-reference"
		/>
		<button
			type="submit"
			disabled={inspecting || inspectReference.trim() === ''}
			class="h-9 rounded-md bg-brand-600 px-3 text-sm font-medium text-white shadow-xs hover:bg-brand-700 disabled:cursor-not-allowed disabled:opacity-50"
			data-testid="activity-inspect-submit">{inspecting ? 'Inspecting…' : 'Inspect'}</button
		>
		<CliEquivalent testid="activity-inspect-cli" command={inspectCli} label="orkctl activity inspect for this reference" />
	</form>
	{#if inspectError}
		<div class="mt-3">
			<ErrorPanel testid="activity-inspect-error" subject="that reference" message={inspectError} />
		</div>
	{:else if inspected}
		<div
			class="mt-3 space-y-3 rounded-lg border border-border bg-surface p-4 text-sm shadow-xs"
			data-testid="activity-inspect-result"
		>
			<dl class="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1 text-xs">
				<dt class="text-fg-muted">Activity</dt>
				<dd class="text-fg" data-testid="activity-inspect-activity">
					{inspected.name ? `${inspected.name} (catalog)` : `${inspected.reference} (reference)`}
				</dd>
				<dt class="text-fg-muted">Reference</dt>
				<dd class="font-mono break-all text-fg-secondary">{inspected.reference}</dd>
				<dt class="text-fg-muted">Pinned</dt>
				<dd class="font-mono break-all text-brand-700" data-testid="activity-inspect-pinned">{pinnedReference(inspected)}</dd>
				<dt class="text-fg-muted">Media type</dt>
				<dd class="font-mono text-fg-secondary">{inspected.mediaType || '-'}</dd>
				<dt class="text-fg-muted">Platforms</dt>
				<dd class="text-fg-secondary">{inspected.platforms.length > 0 ? inspected.platforms.join(', ') : '-'}</dd>
				<dt class="text-fg-muted">Resolved</dt>
				<dd class="text-fg-secondary">{fmtTimestamp(inspected.resolvedAt)}</dd>
			</dl>
			<ActivityInterface info={inspected} />
		</div>
	{/if}
</section>
