<script lang="ts">
	import { goto, invalidateAll } from '$app/navigation';
	import { page } from '$app/state';
	import { rememberRecent } from '$lib/palette/recent';
	import DrawingFrame from '$lib/components/DrawingFrame.svelte';
	import Dialog from '$lib/components/Dialog.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import OrkflowEditor from '$lib/components/OrkflowEditor.svelte';
	import OrkflowSchedules from '$lib/components/OrkflowSchedules.svelte';
	import OrkflowTriggers from '$lib/components/OrkflowTriggers.svelte';
	import OrkflowVersions from '$lib/components/OrkflowVersions.svelte';
	import OrkflowCallers from '$lib/components/OrkflowCallers.svelte';
	import RunSubmitPanel from '$lib/components/RunSubmitPanel.svelte';
	import SheetHeader from '$lib/components/SheetHeader.svelte';
	import { registerProvider } from '$lib/palette/context';
	import { orkflowPageCommands } from '$lib/palette/orkflowPage';
	import { setTitleBlock } from '$lib/sheet.svelte';
	import { listHref, runWindowFilters } from '$lib/browsing';
	import { NAMESPACE_PARAM, namespaceLabel, withNamespace } from '$lib/namespace';
	import { fmtTimestamp } from '$lib/status';
	import { activityLabel, shortDigest } from '$lib/activity';
	import { cacheSpecLabel } from '$lib/cache';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();
	const wf = $derived(data.orkflow);

	// The palette's RECENT group: this browser remembers the Orkflow's name.
	$effect(() => {
		if (data.orkflow?.name) rememberRecent({ kind: 'orkflow', id: data.orkflow.name, namespace: namespaceLabel(data.namespace) });
	});

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

	// The title block: what this page shows — the version on screen, its
	// digest, and the marks the Versions tab gives it (current, head), or —
	// when it is neither. No word is invented for a version that is neither.
	const shownVersion = $derived(wf?.version ?? data.version);
	// Current is the engine's own answer (is_current), read and never
	// recomputed: the version an unnamed run executes.
	const resolvedVersion = $derived(data.versions.find((v) => v.isCurrent)?.version ?? '');
	const marks = $derived(
		[shownVersion && shownVersion === resolvedVersion ? 'current' : '', shownVersion && shownVersion === data.headVersion ? 'head' : '']
			.filter(Boolean)
			.join(' · ')
	);
	setTitleBlock(() => {
		const digest = wf?.definitionDigest ?? '';
		const hex = digest.includes(':') ? digest.slice(digest.indexOf(':') + 1) : digest;
		return [
			{ label: 'ORKFLOW', value: wf?.name ?? data.name },
			{ label: 'VERSION', value: shownVersion || '—', mono: true },
			{ label: 'DIGEST', value: hex.slice(0, 8) || '—', mono: true, title: digest || undefined, copy: digest || undefined },
			{ label: 'NAMESPACE', value: namespaceLabel(data.namespace) },
			{ label: 'STATUS', value: marks || '—' }
		];
	});

	// The versions beside the DAG: the listing this page already read, at
	// most five rows, the shown one always among them.
	const ASIDE_ROWS = 5;
	const asideVersions = $derived.by(() => {
		const first = data.versions.slice(0, ASIDE_ROWS);
		if (!shownVersion || first.some((v) => v.version === shownVersion)) return first;
		const own = data.versions.find((v) => v.version === shownVersion);
		return own ? [...first.slice(0, ASIDE_ROWS - 1), own] : first;
	});
	function versionHref(version: string): string {
		return `/orkflows/${data.name}?${new URLSearchParams({ [NAMESPACE_PARAM]: namespaceLabel(data.namespace), version }).toString()}`;
	}

	// What this page adds to the palette while it is open: Submit a run…
	// opens this page's own dialog; the other tabs; a task id jumps to its
	// row in the task register. Registering builds nothing ($lib/palette).
	$effect(() =>
		registerProvider({
			id: 'orkflow-page',
			commands: (query) =>
				orkflowPageCommands(
					{
						name: data.name,
						tab: data.tab,
						tasks: () => (wf?.tasks ?? []).map((t) => t.id),
						hrefOf: tabHref,
						canSubmit: !!wf,
						openSubmit: () => (submitDialogOpen = true)
					},
					query
				)
		})
	);

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

<SheetHeader title={wf?.name ?? data.name} testid="orkflow-name">
	{#snippet meta()}
		{#if wf}
			<p class="flex flex-wrap items-baseline gap-x-3 gap-y-1">
				<span class="figure font-semibold text-ink" data-testid="orkflow-version">v{wf.version}</span>
				<span class="text-xs text-ink-muted">Applied {fmtTimestamp(wf.createdAt)}</span>
			</p>
			{#if wf.description}<p class="mt-1 max-w-prose">{wf.description}</p>{/if}
		{/if}
	{/snippet}
	{#snippet actions()}
		<a
			href={listHref('/runs', { ns: namespaceLabel(data.namespace) }, { filter: runWindowFilters(wf?.name ?? data.name, '', '', '') })}
			class="btn">View runs</a
		>
		{#if wf}
			<button type="button" data-testid="orkflow-submit-run" onclick={() => (submitDialogOpen = true)} class="btn btn-primary">
				Submit a run
			</button>
		{/if}
	{/snippet}
</SheetHeader>

<nav class="mb-6 flex gap-1 overflow-x-auto border-b border-rule" aria-label="Orkflow views">
	{#each tabs as tab (tab.id)}
		<a
			href={tabHref(tab.id)}
			data-testid="tab-{tab.id}"
			aria-current={data.tab === tab.id ? 'page' : undefined}
			class="caps -mb-px shrink-0 border-b-2 border-transparent px-3 py-2.5 text-ink-muted transition-colors hover:text-ink current:border-blue current:text-ink"
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
			subject="Orkflow {data.name}"
			message={data.orkflowError}
			busy={reloading}
			onretry={reload}
		/>
	{:else if reloading}
		<p data-testid="overview-loading" class="text-sm text-ink-muted">Loading the Orkflow…</p>
	{:else if !wf}
		<p data-testid="overview-empty" class="text-sm text-ink-muted">
			The engine returned no document for {data.name}.
		</p>
	{:else}
		<!-- The DAG beside the versions: the drawing and which version of it
			this is. Below 1280 px the versions follow the drawing. -->
		<div class="mb-8 grid gap-6 xl:grid-cols-[minmax(0,1fr)_var(--spacing-aside)]">
			<section class="min-w-0" aria-label="Task DAG">
				<DrawingFrame
					testid="orkflow-drawing"
					caption="{wf.name} @ {wf.version} · {wf.tasks.length} {wf.tasks.length === 1 ? 'task' : 'tasks'}"
					tasks={wf.tasks}
					orkflowLabel="{wf.name}@{wf.version}"
				/>
			</section>
			<aside class="min-w-0" aria-labelledby="orkflow-sheet-versions-heading">
				<h2 id="orkflow-sheet-versions-heading" class="caps mb-2 text-ink-muted">Versions</h2>
				{#if data.versionsError}
					<p data-testid="orkflow-sheet-versions-error" class="severity-error text-sm">
						Error: the versions of {data.name} could not be read: {data.versionsError}
					</p>
				{:else}
					<table class="register" data-testid="orkflow-sheet-versions">
						<thead>
							<tr>
								<th scope="col">Version</th>
								<th scope="col">Applied</th>
								<th scope="col"><span class="sr-only">Marks</span></th>
							</tr>
						</thead>
						<tbody>
							{#each asideVersions as v (v.version)}
								<tr data-testid="orkflow-sheet-version-{v.version}" class={v.version === shownVersion ? 'bg-blue-tint' : ''}>
									<td class="figure">
										{#if v.version === shownVersion}
											<span class="font-semibold text-ink" aria-current="true">{v.version}</span>
										{:else}
											<a class="link" href={versionHref(v.version)}>{v.version}</a>
										{/if}
									</td>
									<td class="figure text-xs text-ink-secondary">{fmtTimestamp(v.appliedAt)}</td>
									<td class="text-right">
										<div class="flex flex-wrap justify-end gap-1">
											{#if v.version === shownVersion}<span class="stamp border-ink text-ink" data-testid="orkflow-sheet-version-{v.version}-shown">Shown</span>{/if}
											{#if v.isCurrent}<span class="stamp border-blue text-blue" data-testid="orkflow-sheet-version-{v.version}-current">Current</span>{/if}
											{#if v.version === data.headVersion}<span class="stamp border-rule-strong text-ink-secondary" data-testid="orkflow-sheet-version-{v.version}-head">Head</span>{/if}
										</div>
									</td>
								</tr>
							{:else}
								<tr><td colspan="3" class="text-ink-muted">No versions are listed for {data.name}.</td></tr>
							{/each}
						</tbody>
					</table>
					{#if data.versions.length > asideVersions.length}
						<a href={tabHref('versions')} data-testid="orkflow-sheet-versions-all" class="link mt-2 inline-block text-sm"
							>All {data.versions.length} versions</a
						>
					{/if}
				{/if}
			</aside>
		</div>

		<section class="region mb-8 pt-3" aria-labelledby="orkflow-tasks-heading">
			<div class="mb-3 flex flex-wrap items-baseline justify-between gap-2">
				<h2 id="orkflow-tasks-heading" class="text-md font-semibold text-ink">Tasks ({wf.tasks.length})</h2>
				{#if wf.tasks.some((task) => task.cache)}
					<a
						data-testid="orkflow-cache-link"
						href={withNamespace(`/admin/cache?orkflow=${encodeURIComponent(wf.name)}`, data.namespace)}
						class="link text-sm font-semibold">Cache hit rate and memos</a
					>
				{/if}
			</div>
			<div class="overflow-x-auto">
				<table class="register min-w-[720px]">
					<thead>
						<tr>
							<th scope="col">ID</th>
							<th scope="col">Activity</th>
							<th scope="col">Image digest</th>
							<th scope="col">Pool</th>
							<th scope="col">Needs</th>
							<th scope="col">Cache</th>
							<th scope="col">Timeout</th>
						</tr>
					</thead>
					<tbody>
						{#each wf.tasks as task (task.id)}
							<tr
								id="task-row-{task.id}"
								class="transition-colors hover:bg-hover target:bg-blue-tint"
								data-testid="task-row-{task.id}"
							>
								<td class="font-mono font-semibold text-ink">{task.id}</td>
								<td class="text-ink-secondary" data-testid="task-activity-{task.id}">
									{#if task.orkflow}
										<!-- A composed task: a link to the child Orkflow at the version it pins. -->
										<a
											href={withNamespace(`/orkflows/${encodeURIComponent(task.orkflow.name)}?version=${encodeURIComponent(task.orkflow.version)}`, data.namespace)}
											data-testid="task-child-link-{task.id}"
											class="link font-mono text-xs">orkflow {task.orkflow.name}@{task.orkflow.version}</a
										>
									{:else}
										{activityLabel(task) || '—'}
									{/if}
								</td>
								<!--
									The resolved index digest is what makes this version
									explainable after a tag moves: the definition pins the
									image, so the digest belongs beside the name it
									resolved from, not hidden a click away.
								-->
								<td>
									{#if task.orkflow}
										<!-- The child's definition digest: what this version pins it as. -->
										<span
											class="font-mono text-xs text-ink-secondary"
											title={task.orkflow.definitionDigest}
											data-digest={task.orkflow.definitionDigest}
											data-testid="task-child-digest-{task.id}"
											>{shortDigest(task.orkflow.definitionDigest) || '—'}</span
										>
									{:else if task.image?.indexDigest}
										<span
											class="font-mono text-xs text-ink-secondary"
											title={task.image.indexDigest}
											data-digest={task.image.indexDigest}
											data-testid="task-image-digest-{task.id}"
											>{shortDigest(task.image.indexDigest)}</span
										>
									{:else}
										<span
											class="text-xs text-ink-muted"
											data-testid="task-image-unpinned-{task.id}"
											title="this task's image was not resolved to a digest">unpinned</span
										>
									{/if}
								</td>
								<td class="text-ink-secondary" data-testid="task-pool-{task.id}">
									{task.orkflow ? '—' : task.pool || 'main'}
								</td>
								<td class="font-mono text-xs text-ink-secondary" data-testid="task-needs-{task.id}">
									{task.needs.length > 0 ? task.needs.join(', ') : '—'}
								</td>
								<td class="text-ink-secondary" data-testid="task-cache-{task.id}">
									{cacheSpecLabel(task.cache)}
								</td>
								<td class="figure text-ink-secondary">{task.orkflow ? '—' : `${task.timeoutSeconds}s`}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		</section>

		<!--
			OUTPUTS: the Orkflow's declared return contract. When declared, a
			run's output IS this map rendered, and it is all an Orkflow that
			composes this one can read; when not, a run's output is every leaf
			task's values and a composing Orkflow reads nothing.
		-->
		<section class="region mb-8 pt-3" data-testid="orkflow-outputs" aria-labelledby="orkflow-outputs-heading">
			<h2 id="orkflow-outputs-heading" class="mb-3 text-md font-semibold text-ink">Outputs</h2>
			{#if Object.keys(wf.outputs ?? {}).length > 0}
				<div class="overflow-x-auto">
					<table class="register">
						<thead>
							<tr>
								<th scope="col">Name</th>
								<th scope="col">Template</th>
							</tr>
						</thead>
						<tbody>
							{#each Object.keys(wf.outputs).sort() as key (key)}
								<tr data-testid="orkflow-output-{key}">
									<td class="font-mono text-xs text-ink">{key}</td>
									<td class="font-mono text-xs break-all text-ink-secondary">{wf.outputs[key]}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{:else}
				<p class="text-sm text-ink-muted" data-testid="orkflow-outputs-none">
					This Orkflow declares no outputs; a run's output is every leaf task's values, and an Orkflow that
					composes it receives none.
				</p>
			{/if}
		</section>
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
	<OrkflowCallers name={data.name} namespace={data.namespace} />
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
