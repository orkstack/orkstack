<script lang="ts">
	// Admin → Cache: how one namespace's cached tasks fared over one window,
	// and the memos stored now — the browser form of `orkctl cache stats` —
	// with the purge `orkctl cache purge` performs. A memo is advisory:
	// purging changes no result, only whether the next run of a cached task
	// runs its container (docs/DSL.md "Caching").
	import { goto, invalidateAll } from '$app/navigation';
	import CliEquivalent from '$lib/components/CliEquivalent.svelte';
	import Dialog from '$lib/components/Dialog.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import ScopeNote from '$lib/components/ScopeNote.svelte';
	import SheetHeader from '$lib/components/SheetHeader.svelte';
	import Select from '$lib/components/Select.svelte';
	import { errorMessage } from '$lib/client';
	import { cacheClient } from '$lib/clients/cache';
	import {
		CACHE_WINDOWS,
		cacheStatsCli,
		hitRateLabel,
		inventoryRowId,
		parseGoDuration,
		purgeCommand,
		purgeSelectorError,
		reusedLabel,
		scopeName,
		type PurgeSelector
	} from '$lib/cache';
	import { namespaceLabel, withNamespace } from '$lib/namespace';
	import { fmtInstant } from '$lib/stats';
	import { durationFromMs } from '@bufbuild/protobuf/wkt';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	function windowHref(since: string): string {
		const params = new URLSearchParams({ since });
		if (data.orkflow) params.set('orkflow', data.orkflow);
		return withNamespace(`/admin/cache?${params.toString()}`, data.namespace);
	}

	let customSince = $state('');
	function applyCustom(event: SubmitEvent): void {
		event.preventDefault();
		if (Number.isNaN(Date.parse(customSince))) return;
		void goto(windowHref(customSince));
	}

	let retrying = $state(false);
	async function retry(): Promise<void> {
		retrying = true;
		try {
			await invalidateAll();
		} finally {
			retrying = false;
		}
	}

	// The purge dialog: exactly one selector, never a default scope.
	let purgeOpen = $state(false);
	let selector = $state<'orkflow' | 'namespace-scoped' | 'all'>('orkflow');
	let purgeOrkflow = $state('');
	let purgeTask = $state('');
	let purgeOlderThan = $state('');
	let purging = $state(false);
	let purgeError = $state('');
	let purgeResult = $state('');

	const purge = $derived<PurgeSelector>({
		namespace: data.namespace,
		...(selector === 'orkflow' ? { orkflow: purgeOrkflow } : {}),
		...(selector === 'namespace-scoped' ? { namespaceScoped: true } : {}),
		...(selector === 'all' ? { all: true } : {}),
		...(selector !== 'all' && purgeTask ? { task: purgeTask } : {}),
		...(purgeOlderThan ? { olderThan: purgeOlderThan } : {})
	});
	const purgeInvalid = $derived(
		purgeSelectorError(purge) ||
			(purgeOlderThan && parseGoDuration(purgeOlderThan) === null ? 'older than takes a duration such as 72h' : '')
	);

	function openPurge(): void {
		purgeOrkflow = data.orkflow || data.orkflowNames[0] || '';
		selector = 'orkflow';
		purgeTask = '';
		purgeOlderThan = '';
		purgeError = '';
		purgeResult = '';
		purgeOpen = true;
	}

	async function confirmPurge(): Promise<void> {
		if (purgeInvalid) return;
		purging = true;
		purgeError = '';
		let deleted = 0n;
		let kept = 0n;
		try {
			const olderThanSeconds = purgeOlderThan ? parseGoDuration(purgeOlderThan) : null;
			for (;;) {
				const resp = await cacheClient(fetch).purgeCache({
					namespace: data.namespace,
					orkflowName: purge.orkflow ?? '',
					namespaceScoped: purge.namespaceScoped ?? false,
					all: purge.all ?? false,
					taskId: purge.task ?? '',
					...(olderThanSeconds ? { olderThan: durationFromMs(olderThanSeconds * 1000) } : {})
				});
				deleted += resp.deleted;
				kept = resp.kept;
				if (!resp.more) break;
			}
			purgeResult = `Purged ${deleted} memo${deleted === 1n ? '' : 's'}${olderThanSeconds ? `; kept ${kept} newer` : ''}.`;
			await invalidateAll();
		} catch (err) {
			purgeError = deleted > 0n ? `Purged ${deleted} memos, then: ${errorMessage(err)}` : errorMessage(err);
		} finally {
			purging = false;
		}
	}
</script>

<svelte:head><title>Admin — Cache — OrkStack</title></svelte:head>

<SheetHeader title="Cache">
	{#snippet meta()}Namespace <span class="font-mono font-semibold text-ink">{namespaceLabel(data.namespace)}</span>{/snippet}
	{#snippet actions()}
		<CliEquivalent command={cacheStatsCli(data.namespace, data.window, data.orkflow)} testid="cache-stats-cli" />
		<button
			type="button"
			data-testid="cache-purge"
			onclick={openPurge}
			class="btn border-red text-red-strong hover:bg-red-tint"
			>Purge memos</button
		>
	{/snippet}
</SheetHeader>

<ScopeNote testid="cache-scope">
	How cached tasks (<span class="font-mono">cache:</span>) fared over the window, and the memos stored now.
	Memos are advisory: purging them changes no result, only whether the next run of a cached task runs its
	container.
</ScopeNote>

<section class="mb-6 flex flex-wrap items-center gap-3" data-testid="cache-window">
	<span class="caps text-ink-muted" aria-hidden="true">Window</span>
	<div role="group" aria-label="Cache window" class="inline-flex h-8 rounded-sm border border-rule-strong bg-paper">
		{#each CACHE_WINDOWS as preset (preset)}
			<a
				data-testid="cache-window-{preset}"
				aria-current={data.window.window === preset ? 'page' : undefined}
				href={windowHref(preset)}
				class="figure inline-flex items-center border-r border-rule-strong px-3 text-sm text-ink-secondary transition-colors last:border-r-0 hover:bg-hover hover:text-ink current:bg-blue-tint current:font-semibold current:text-blue-strong"
				>{preset}</a
			>
		{/each}
	</div>
	<form class="flex items-center gap-2" onsubmit={applyCustom}>
		<label class="text-sm text-ink-secondary" for="cache-window-since">since</label>
		<input
			id="cache-window-since"
			data-testid="cache-window-since"
			bind:value={customSince}
			placeholder={data.window.window === 'custom' ? data.window.since : '2026-09-20T00:00:00Z'}
			class="field h-8 w-56 px-3 font-mono text-xs"
		/>
		<button type="submit" data-testid="cache-window-apply" class="btn btn-sm">Apply</button>
	</form>
	{#if data.orkflow}
		<span class="text-sm text-ink-muted" data-testid="cache-orkflow-filter">
			Orkflow <span class="font-mono text-ink-secondary">{data.orkflow}</span>
			<a class="link ml-1" href={withNamespace(`/admin/cache?since=${encodeURIComponent(data.window.window === 'custom' ? (data.window.since ?? '') : data.window.window)}`, data.namespace)}>clear</a>
		</span>
	{/if}
</section>

{#if data.statsError}
	<ErrorPanel
		testid="cache-error"
		retryTestid="cache-retry"
		subject="the cache statistics"
		message={data.statsError}
		busy={retrying}
		onretry={retry}
	/>
{:else if data.stats}
	{@const stats = data.stats}
	<p class="figure mb-3 text-xs text-ink-muted" data-testid="cache-computed-at">
		{fmtInstant(stats.windowSince)} → {fmtInstant(stats.windowUntil)} · computed {fmtInstant(stats.computedAt)}
	</p>
	<section class="mb-10">
		<h2 class="mb-2 text-md font-semibold text-ink">Cached tasks</h2>
		{#if stats.usage.length === 0}
			<p class="text-sm text-ink-muted" data-testid="cache-usage-empty">No cached task finished in this window.</p>
		{:else}
			<div class="overflow-x-auto border-t-2 border-ink">
				<table class="register min-w-[720px]" data-testid="cache-usage">
					<thead>
						<tr>
							<th scope="col">Orkflow</th>
							<th scope="col">Task</th>
							<th scope="col" class="text-right">Hits</th>
							<th scope="col" class="text-right">Misses</th>
							<th scope="col" class="text-right">Bypassed</th>
							<th scope="col" class="text-right">Hit rate</th>
							<th scope="col" class="text-right" title="the producing attempts' duration the hits did not spend again">Reused</th>
						</tr>
					</thead>
					<tbody>
						{#each stats.usage as row (`${row.orkflowName}/${row.taskId}`)}
							<tr class="transition-colors hover:bg-hover" data-testid="cache-usage-{row.orkflowName}-{row.taskId}">
								<td class="font-semibold text-ink">{row.orkflowName}</td>
								<td class="font-mono text-ink-secondary">{row.taskId}</td>
								<td class="figure text-right" data-testid="cache-usage-{row.orkflowName}-{row.taskId}-hits">{row.hits.toString()}</td>
								<td class="figure text-right" data-testid="cache-usage-{row.orkflowName}-{row.taskId}-misses">{row.misses.toString()}</td>
								<td class="figure text-right">{row.bypassed.toString()}</td>
								<td class="figure text-right" data-testid="cache-usage-{row.orkflowName}-{row.taskId}-rate">{hitRateLabel(row)}</td>
								<td class="figure text-right">{reusedLabel(row.reused)}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
		<p class="mt-2 text-xs text-ink-muted" data-testid="cache-scan-note">
			scanned {stats.runsScanned.toString()} of {stats.runsInWindow.toString()} finished runs. Reused is the producing
			attempts' duration the hits did not spend again.
		</p>
	</section>

	<section class="region mb-10 pt-3">
		<h2 class="mb-2 text-md font-semibold text-ink">Memos</h2>
		{#if stats.inventory.length === 0}
			<p class="text-sm text-ink-muted" data-testid="cache-memos-empty">No memos stored.</p>
		{:else}
			<div class="overflow-x-auto border-t-2 border-ink">
				<table class="register min-w-[720px]" data-testid="cache-memos">
					<thead>
						<tr>
							<th scope="col">Scope</th>
							<th scope="col">Orkflow</th>
							<th scope="col">Task</th>
							<th scope="col" class="text-right">Memos</th>
							<th scope="col">Oldest</th>
							<th scope="col">Newest</th>
						</tr>
					</thead>
					<tbody>
						{#each stats.inventory as row (inventoryRowId(row))}
							<tr class="transition-colors hover:bg-hover" data-testid="cache-memos-{inventoryRowId(row)}">
								<td class="text-ink-secondary">{scopeName(row.scope)}</td>
								<td class="text-ink">{row.orkflowName || '—'}</td>
								<td class="font-mono text-ink-secondary">{row.taskId}</td>
								<td class="figure text-right">{row.memos.toString()}</td>
								<td class="figure text-xs text-ink-muted">{fmtInstant(row.oldest)}</td>
								<td class="figure text-xs text-ink-muted">{fmtInstant(row.newest)}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	</section>
{/if}

{#if purgeResult && !purgeOpen}
	<p class="confirm text-sm text-ink" role="status" data-testid="cache-purge-result">{purgeResult}</p>
{/if}

{#if purgeOpen}
	<Dialog title="Purge memos" testid="cache-purge-dialog" onclose={() => (purgeOpen = false)} widthClass="max-w-xl">
		{#snippet body()}
			<form
				class="mt-4 flex flex-col gap-4 text-sm"
				onsubmit={(event) => {
					event.preventDefault();
					void confirmPurge();
				}}
			>
				<fieldset class="flex flex-col gap-2">
					<legend class="field-label">Which memos</legend>
					<label class="flex items-center gap-2">
						<input type="radio" name="cache-purge-selector" value="orkflow" bind:group={selector} data-testid="cache-purge-orkflow"  />
						One Orkflow's memos
					</label>
					{#if selector === 'orkflow'}
						<div class="ml-6">
							<Select
								testid="cache-purge-orkflow-select"
								label="Orkflow"
								value={purgeOrkflow}
								options={[...new Set([...(data.orkflow ? [data.orkflow] : []), ...data.orkflowNames])].map((name) => ({ value: name, label: name }))}
								onchange={(next) => (purgeOrkflow = next)}
							/>
						</div>
					{/if}
					<label class="flex items-center gap-2">
						<input type="radio" name="cache-purge-selector" value="namespace-scoped" bind:group={selector} data-testid="cache-purge-namespace-scoped"  />
						The namespace-scoped memos
					</label>
					<label class="flex items-center gap-2">
						<input type="radio" name="cache-purge-selector" value="all" bind:group={selector} data-testid="cache-purge-all"  />
						Every memo of the namespace
					</label>
				</fieldset>
				{#if selector !== 'all'}
					<label class="flex flex-col gap-1">
						<span class="field-label mb-0">Task <span class="field-hint">(optional)</span></span>
						<input
							data-testid="cache-purge-task"
							bind:value={purgeTask}
							placeholder="featurize"
							class="field h-9 px-3 font-mono text-sm"
						/>
					</label>
				{/if}
				<label class="flex flex-col gap-1">
					<span class="field-label mb-0">Only memos older than <span class="field-hint">(optional, e.g. 72h)</span></span>
					<input
						data-testid="cache-purge-older-than"
						bind:value={purgeOlderThan}
						placeholder="72h"
						class="field figure h-9 px-3 text-sm"
					/>
				</label>
				<p class="text-ink-muted">No run, commit or artifact is touched; the next run of these tasks runs its container.</p>
				{#if purgeInvalid}
					<p class="severity-error" data-testid="cache-purge-invalid">{purgeInvalid}</p>
				{/if}
				{#if purgeError}
					<p class="severity-error" role="alert" data-testid="cache-purge-error">{purgeError}</p>
				{/if}
				{#if purgeResult}
					<p class="confirm text-ink" role="status" data-testid="cache-purge-result">{purgeResult}</p>
				{/if}
				<div class="flex items-center justify-between gap-3">
					<CliEquivalent command={purgeCommand(purge)} testid="cache-purge-cli" />
					<div class="flex gap-2">
						<button
							type="button"
							onclick={() => (purgeOpen = false)}
							class="btn"
							>Close</button
						>
						<button
							type="submit"
							data-testid="cache-purge-confirm"
							disabled={purging || Boolean(purgeInvalid)}
							class="btn btn-danger"
							>{purging ? 'Purging…' : 'Purge'}</button
						>
					</div>
				</div>
			</form>
		{/snippet}
	</Dialog>
{/if}
