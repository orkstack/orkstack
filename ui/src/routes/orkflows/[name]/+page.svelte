<script lang="ts">
	import { invalidateAll } from '$app/navigation';
	import DagGraph from '$lib/components/DagGraph.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import OrkflowEditor from '$lib/components/OrkflowEditor.svelte';
	import OrkflowVersions from '$lib/components/OrkflowVersions.svelte';
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
	// Overview's DAG and the editor's document — and not with the Versions tab,
	// which lists them all. Carrying it there would also carry it back out: the
	// Edit link after an apply would reopen the version the fork came FROM
	// rather than the head it just created.
	function tabHref(tab: string): string {
		const params = new URLSearchParams();
		params.set(NAMESPACE_PARAM, namespaceLabel(data.namespace));
		if (data.version && tab !== 'versions') params.set('version', data.version);
		if (tab !== 'overview') params.set('tab', tab);
		return `/orkflows/${data.name}?${params.toString()}`;
	}

	const tabs = [
		{ id: 'overview', label: 'Overview' },
		{ id: 'versions', label: 'Versions' },
		{ id: 'edit', label: 'Edit' }
	];

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
		<h1 class="text-2xl font-bold" data-testid="orkflow-name">{wf?.name ?? data.name}</h1>
		{#if wf}
			<span class="text-sm text-zinc-400" data-testid="orkflow-version">v{wf.version}</span>
		{/if}
	</div>
	{#if wf?.description}
		<p class="mt-1 text-sm text-zinc-400">{wf.description}</p>
	{/if}
	{#if wf}
		<p class="mt-1 text-xs text-zinc-500">Applied {fmtTimestamp(wf.createdAt)}</p>
	{/if}
</div>

<nav class="mb-6 flex gap-1 border-b border-zinc-800" aria-label="Orkflow views">
	{#each tabs as tab (tab.id)}
		<a
			href={tabHref(tab.id)}
			data-testid="tab-{tab.id}"
			aria-current={data.tab === tab.id ? 'page' : undefined}
			class="-mb-px border-b-2 px-4 py-2 text-sm {data.tab === tab.id
				? 'border-emerald-400 font-medium text-emerald-300'
				: 'border-transparent text-zinc-400 hover:text-zinc-200'}"
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
		<p data-testid="overview-loading" class="text-sm text-zinc-400">Loading the orkflow…</p>
	{:else if !wf}
		<p data-testid="overview-empty" class="text-sm text-zinc-400">
			The engine returned no document for {data.name}.
		</p>
	{:else}
		<section class="mb-8">
			<h2 class="mb-3 text-lg font-semibold">Task DAG</h2>
			<DagGraph tasks={wf.tasks} />
		</section>

		<section class="mb-8">
			<h2 class="mb-3 text-lg font-semibold">Tasks ({wf.tasks.length})</h2>
			<div class="overflow-hidden rounded-lg border border-zinc-800">
				<table class="w-full text-left text-sm">
					<thead class="bg-zinc-900 text-xs uppercase tracking-wide text-zinc-400">
						<tr>
							<th class="px-4 py-3">ID</th>
							<th class="px-4 py-3">Activity</th>
							<th class="px-4 py-3">Image digest</th>
							<th class="px-4 py-3">Pool</th>
							<th class="px-4 py-3">Needs</th>
							<th class="px-4 py-3">Timeout</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-zinc-800">
						{#each wf.tasks as task (task.id)}
							<tr data-testid="task-row-{task.id}">
								<td class="px-4 py-2.5 font-medium text-zinc-100">{task.id}</td>
								<td class="px-4 py-2.5 text-zinc-300" data-testid="task-activity-{task.id}">
									{activityLabel(task) || '—'}
								</td>
								<!--
									The resolved index digest is what makes this version
									explainable after a tag moves: the definition pins the
									image, so the digest belongs beside the name it
									resolved from, not hidden a click away.
								-->
								<td class="px-4 py-2.5">
									{#if task.image?.indexDigest}
										<span
											class="font-mono text-xs text-emerald-300"
											title={task.image.indexDigest}
											data-digest={task.image.indexDigest}
											data-testid="task-image-digest-{task.id}"
											>{shortDigest(task.image.indexDigest)}</span
										>
									{:else}
										<span
											class="text-xs text-zinc-600"
											data-testid="task-image-unpinned-{task.id}"
											title="this task's image was not resolved to a digest">unpinned</span
										>
									{/if}
								</td>
								<td class="px-4 py-2.5 text-zinc-400" data-testid="task-pool-{task.id}">
									{task.pool || 'main'}
								</td>
								<td class="px-4 py-2.5 text-zinc-400" data-testid="task-needs-{task.id}">
									{task.needs.length > 0 ? task.needs.join(', ') : '—'}
								</td>
								<td class="px-4 py-2.5 text-zinc-400">{task.timeoutSeconds}s</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		</section>

		<div class="flex gap-3">
			<a
				href={withNamespace(`/submit?orkflow=${wf.name}`, data.namespace)}
				class="rounded-md bg-emerald-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-emerald-500"
			>
				Submit a run
			</a>
			<a
				href={withNamespace(`/runs?orkflow=${wf.name}`, data.namespace)}
				class="rounded-md border border-zinc-700 px-3 py-1.5 text-sm text-zinc-300 hover:bg-zinc-800"
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
