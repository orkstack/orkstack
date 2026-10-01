<script lang="ts">
	import { goto, invalidateAll } from '$app/navigation';
	import { page } from '$app/state';
	import DagGraph from '$lib/components/DagGraph.svelte';
	import Dialog from '$lib/components/Dialog.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import OrkflowEditor from '$lib/components/OrkflowEditor.svelte';
	import OrkflowSchedules from '$lib/components/OrkflowSchedules.svelte';
	import OrkflowTriggers from '$lib/components/OrkflowTriggers.svelte';
	import OrkflowVersions from '$lib/components/OrkflowVersions.svelte';
	import RunSubmitPanel from '$lib/components/RunSubmitPanel.svelte';
	import { NAMESPACE_PARAM, namespaceLabel, withNamespace } from '$lib/namespace';
	import { fmtTimestamp } from '$lib/status';
	import { activityLabel, shortDigest } from '$lib/activity';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();
	const wf = $derived(data.orkflow);

	// The tab is a coordinate of the page, so it lives in the URL beside the
	// namespace and the version: a link to "the versions of deploy in payments"
	// is a link, not an instruction to click twice. The namespace is always
	// stated, `default` included — a tab link that dropped it would hand the
	// next reader's remembered namespace a page about somebody else's orkflow.
	//
	// The VERSION travels with the two tabs that render one version — the
	// Overview's DAG and the editor's document — and not with the two that list
	// across them. Carrying it to the Versions tab would also carry it back out:
	// the Edit link after an apply would reopen the version the fork came FROM
	// rather than the head it just created. The Triggers tab is the same kind of
	// listing: each trigger pins its own version, so the page's `?version=`
	// names none of them and would only leak back into the tabs that obey it.
	// The Schedules tab is the same kind of listing, for the same reason.
	function tabHref(tab: string): string {
		const params = new URLSearchParams();
		params.set(NAMESPACE_PARAM, namespaceLabel(data.namespace));
		if (data.version && tab !== 'versions' && tab !== 'triggers' && tab !== 'schedules')
			params.set('version', data.version);
		if (tab !== 'overview') params.set('tab', tab);
		return `/orkflows/${data.name}?${params.toString()}`;
	}

	const tabs = [
		{ id: 'overview', label: 'Overview' },
		{ id: 'versions', label: 'Versions' },
		{ id: 'triggers', label: 'Triggers' },
		{ id: 'schedules', label: 'Schedules' },
		{ id: 'edit', label: 'Edit' }
	];

	// Submitting is a dialog on this page rather than a trip to a page that
	// would ask again which orkflow it is looking at. The coordinates are read
	// off `wf` — the version this page is CURRENTLY showing, which is `?version=`
	// when the URL names one and the current pointer when it does not — so the
	// draft opens pinned to exactly the DAG above it. They are derived rather
	// than narrowed inside the dialog because a snippet is its own function: the
	// `{#if wf}` around it does not travel into the body.
	let submitDialogOpen = $state(false);
	const submitName = $derived(wf?.name ?? data.name);
	const submitVersion = $derived(wf?.version ?? '');

	// The origin an external caller posts a trigger to. Read from `page.url`
	// rather than `window.location` so a server render answers it too, and it is
	// this page's own address because that is the only one the browser can know:
	// the engine's API publishes /v1/triggers/{id} itself, and the Triggers tab
	// says so beside the commands it builds.
	const invokeOrigin = $derived(page.url.origin);

	let reloading = $state(false);
	async function reload() {
		reloading = true;
		try {
			await invalidateAll();
		} finally {
			// The view never stays on "Retrying…": either the data arrived, or the
			// loader's error is rendered again with the same button.
			reloading = false;
		}
	}
</script>

<svelte:head><title>{data.name} — OrkStack</title></svelte:head>

<div class="mb-6">
	<div class="flex items-baseline gap-3">
		<h1 class="text-xl font-semibold tracking-tight text-fg" data-testid="orkflow-name">
			{wf?.name ?? data.name}
		</h1>
		{#if wf}
			<span class="text-sm text-fg-muted" data-testid="orkflow-version">v{wf.version}</span>
		{/if}
	</div>
	{#if wf?.description}
		<p class="mt-1 text-sm text-fg-muted">{wf.description}</p>
	{/if}
	{#if wf}
		<p class="mt-1 text-xs text-fg-muted">Applied {fmtTimestamp(wf.createdAt)}</p>
	{/if}
</div>

<nav class="mb-6 flex gap-1 border-b border-border" aria-label="Orkflow views">
	{#each tabs as tab (tab.id)}
		<a
			href={tabHref(tab.id)}
			data-testid="tab-{tab.id}"
			aria-current={data.tab === tab.id ? 'page' : undefined}
			class="-mb-px border-b-2 border-transparent px-3 py-2 text-sm font-medium text-fg-muted transition-colors duration-150 hover:text-fg current:border-brand-600 current:text-fg"
		>
			{tab.label}
		</a>
	{/each}
</nav>

{#if data.tab === 'overview'}
	{#if data.orkflowError}
		<ErrorPanel
			testid="engine-error"
			retryTestid="overview-retry"
			subject="orkflow {data.name}"
			message={data.orkflowError}
			busy={reloading}
			onretry={reload}
		/>
	{:else if reloading}
		<p data-testid="overview-loading" class="text-sm text-fg-muted">Loading the orkflow…</p>
	{:else if !wf}
		<p data-testid="overview-empty" class="text-sm text-fg-muted">
			The engine returned no document for {data.name}.
		</p>
	{:else}
		<section class="mb-8">
			<h2 class="mb-3 text-sm font-semibold text-fg">Task DAG</h2>
			<DagGraph tasks={wf.tasks} />
		</section>

		<section class="mb-8">
			<h2 class="mb-3 text-sm font-semibold text-fg">Tasks ({wf.tasks.length})</h2>
			<div class="overflow-x-auto rounded-lg border border-border bg-surface shadow-xs">
				<table class="w-full min-w-[720px] border-collapse text-left text-sm">
					<thead class="bg-surface-sunken">
						<tr>
							<th class="h-10 px-4 pl-5 text-xs font-medium text-fg-muted">ID</th>
							<th class="h-10 px-4 text-xs font-medium text-fg-muted">Activity</th>
							<th class="h-10 px-4 text-xs font-medium text-fg-muted">Image digest</th>
							<th class="h-10 px-4 text-xs font-medium text-fg-muted">Pool</th>
							<th class="h-10 px-4 text-xs font-medium text-fg-muted">Needs</th>
							<th class="h-10 px-4 pr-5 text-xs font-medium text-fg-muted">Timeout</th>
						</tr>
					</thead>
					<tbody>
						{#each wf.tasks as task (task.id)}
							<tr
								class="border-t border-border-subtle transition-colors duration-150 hover:bg-surface-hover"
								data-testid="task-row-{task.id}"
							>
								<td class="h-11 px-4 pl-5 align-middle font-medium text-fg">{task.id}</td>
								<td class="h-11 px-4 align-middle text-fg-secondary" data-testid="task-activity-{task.id}">
									{activityLabel(task) || '—'}
								</td>
								<!--
									The resolved index digest is what makes this version
									explainable after a tag moves: the definition pins the
									image, so the digest belongs beside the name it
									resolved from, not hidden a click away.
								-->
								<td class="h-11 px-4 align-middle">
									{#if task.image?.indexDigest}
										<span
											class="font-mono text-xs text-fg-secondary"
											title={task.image.indexDigest}
											data-digest={task.image.indexDigest}
											data-testid="task-image-digest-{task.id}"
											>{shortDigest(task.image.indexDigest)}</span
										>
									{:else}
										<span
											class="text-xs text-fg-muted"
											data-testid="task-image-unpinned-{task.id}"
											title="this task's image was not resolved to a digest">unpinned</span
										>
									{/if}
								</td>
								<td class="h-11 px-4 align-middle text-fg-secondary" data-testid="task-pool-{task.id}">
									{task.pool || 'main'}
								</td>
								<td class="h-11 px-4 align-middle text-fg-secondary" data-testid="task-needs-{task.id}">
									{task.needs.length > 0 ? task.needs.join(', ') : '—'}
								</td>
								<td class="h-11 px-4 pr-5 align-middle text-fg-secondary">{task.timeoutSeconds}s</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		</section>

		<div class="flex gap-3">
			<button
				type="button"
				data-testid="orkflow-submit-run"
				onclick={() => (submitDialogOpen = true)}
				class="inline-flex h-9 items-center justify-center gap-1.5 rounded-md bg-brand-600 px-3.5 text-sm font-medium text-white shadow-xs transition-colors duration-150 hover:bg-brand-700 active:bg-brand-800 focus-visible:ring-2 focus-visible:ring-brand-500/40 focus-visible:ring-offset-2 focus-visible:ring-offset-canvas focus-visible:outline-none"
			>
				Submit a run
			</button>
			<a
				href={withNamespace(`/runs?orkflow=${wf.name}`, data.namespace)}
				class="inline-flex h-9 items-center justify-center gap-1.5 rounded-md border border-border bg-surface px-3.5 text-sm font-medium text-fg shadow-xs transition-colors duration-150 hover:border-border-strong hover:bg-surface-hover"
			>
				View runs
			</a>
		</div>
	{/if}
{:else if data.tab === 'versions'}
	<OrkflowVersions
		name={data.name}
		namespace={data.namespace}
		versions={data.versions}
		currentVersion={data.currentVersion}
		headVersion={data.headVersion}
		error={data.versionsError}
	/>
{:else if data.tab === 'triggers'}
	<OrkflowTriggers
		name={data.name}
		namespace={data.namespace}
		versions={data.versions}
		currentVersion={data.currentVersion}
		triggers={data.triggers}
		error={data.triggersError}
		{invokeOrigin}
	/>
{:else if data.tab === 'schedules'}
	<OrkflowSchedules
		name={data.name}
		namespace={data.namespace}
		versions={data.versions}
		currentVersion={data.currentVersion}
		schedules={data.schedules}
		error={data.schedulesError}
	/>
{:else}
	<OrkflowEditor
		name={data.name}
		namespace={data.namespace}
		sourceYaml={data.editYaml}
		baseVersion={data.editVersion}
		headVersion={data.headVersion}
		error={data.editError}
		versionsHref={tabHref('versions')}
		onreload={reload}
	/>
{/if}

{#if submitDialogOpen}
	<Dialog
		testid="orkflow-submit-dialog"
		title="Submit a run of {submitName}"
		widthClass="max-w-2xl"
		onclose={() => (submitDialogOpen = false)}
	>
		{#snippet body()}
			<RunSubmitPanel
				namespace={data.namespace}
				orkflowName={submitName}
				initialVersion={submitVersion}
				onsubmitted={(runId) => {
					// The panel reports the run and stops there; leaving is this
					// page's decision, and it is the same one the old link made:
					// the operator lands on the run they just started.
					void goto(withNamespace(`/runs/${runId}`, data.namespace));
				}}
			/>
		{/snippet}
	</Dialog>
{/if}
