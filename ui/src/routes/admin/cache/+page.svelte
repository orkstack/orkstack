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
	import { withNamespace } from '$lib/namespace';
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

<div class="mb-4 flex flex-wrap items-center gap-3">
	<h2 class="text-base font-semibold text-fg">Cache</h2>
	<p class="text-sm text-fg-muted">
		How cached tasks (<span class="font-mono">cache:</span>) fared over the window, and the memos stored now.
		Memos are advisory: purging them changes no result, only whether the next run of a cached task runs its
		container.
	</p>
</div>

<section class="mb-4 flex flex-wrap items-center gap-3" data-testid="cache-window">
	<span class="text-sm font-medium text-fg">Window</span>
	<div role="group" aria-label="Cache window" class="inline-flex h-9 rounded-md border border-border bg-surface">
		{#each CACHE_WINDOWS as preset (preset)}
			<a
				data-testid="cache-window-{preset}"
				aria-current={data.window.window === preset ? 'page' : undefined}
				href={windowHref(preset)}
				class="inline-flex items-center border-r border-border px-3 text-sm font-medium text-fg-muted transition-colors first:rounded-l-md last:rounded-r-md last:border-r-0 hover:bg-surface-hover hover:text-fg current:bg-brand-50 current:text-brand-700"
				>{preset}</a
			>
		{/each}
	</div>
	<form class="flex items-center gap-2" onsubmit={applyCustom}>
		<label class="text-sm text-fg-muted" for="cache-window-since">since</label>
		<input
			id="cache-window-since"
			data-testid="cache-window-since"
			bind:value={customSince}
			placeholder={data.window.window === 'custom' ? data.window.since : '2026-09-20T00:00:00Z'}
			class="h-9 w-56 rounded-md border border-border bg-surface px-3 text-sm text-fg placeholder:text-fg-placeholder focus:border-brand-500"
		/>
		<button
			type="submit"
			data-testid="cache-window-apply"
			class="h-9 rounded-md border border-border bg-surface px-3 text-sm font-medium text-fg hover:bg-surface-hover">Apply</button
		>
	</form>
	{#if data.orkflow}
		<span class="text-sm text-fg-muted" data-testid="cache-orkflow-filter">
			orkflow <span class="font-mono text-fg-secondary">{data.orkflow}</span>
			<a class="ml-1 text-brand-700 hover:underline" href={withNamespace(`/admin/cache?since=${encodeURIComponent(data.window.window === 'custom' ? (data.window.since ?? '') : data.window.window)}`, data.namespace)}>clear</a>
		</span>
	{/if}
	<div class="ml-auto flex items-center gap-2">
		<CliEquivalent command={cacheStatsCli(data.namespace, data.window, data.orkflow)} testid="cache-stats-cli" />
		<button
			type="button"
			data-testid="cache-purge"
			onclick={openPurge}
			class="h-9 rounded-md border border-danger-200 bg-surface px-3 text-sm font-medium text-danger-700 hover:bg-danger-50"
			>Purge memos</button
		>
	</div>
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
	<p class="mb-3 text-xs text-fg-muted" data-testid="cache-computed-at">
		{fmtInstant(stats.windowSince)} → {fmtInstant(stats.windowUntil)} · computed {fmtInstant(stats.computedAt)}
	</p>
	<section class="mb-6">
		<h3 class="mb-2 text-sm font-semibold text-fg">Cached tasks</h3>
		{#if stats.usage.length === 0}
			<p class="text-sm text-fg-muted" data-testid="cache-usage-empty">No cached task finished in this window.</p>
		{:else}
			<div class="overflow-x-auto rounded-md border border-border bg-surface">
				<table class="w-full min-w-[720px] border-collapse text-left text-sm" data-testid="cache-usage">
					<thead class="bg-surface-sunken">
						<tr>
							<th class="h-10 px-4 pl-5 text-xs font-medium text-fg-muted">Orkflow</th>
							<th class="h-10 px-4 text-xs font-medium text-fg-muted">Task</th>
							<th class="h-10 px-4 text-right text-xs font-medium text-fg-muted">Hits</th>
							<th class="h-10 px-4 text-right text-xs font-medium text-fg-muted">Misses</th>
							<th class="h-10 px-4 text-right text-xs font-medium text-fg-muted">Bypassed</th>
							<th class="h-10 px-4 text-right text-xs font-medium text-fg-muted">Hit rate</th>
							<th class="h-10 px-4 pr-5 text-right text-xs font-medium text-fg-muted" title="the producing attempts' duration the hits did not spend again">Reused</th>
						</tr>
					</thead>
					<tbody>
						{#each stats.usage as row (`${row.orkflowName}/${row.taskId}`)}
							<tr class="border-t border-border-subtle" data-testid="cache-usage-{row.orkflowName}-{row.taskId}">
								<td class="h-11 px-4 pl-5 align-middle font-medium text-fg">{row.orkflowName}</td>
								<td class="h-11 px-4 align-middle font-mono text-fg-secondary">{row.taskId}</td>
								<td class="h-11 px-4 text-right align-middle tabular-nums" data-testid="cache-usage-{row.orkflowName}-{row.taskId}-hits">{row.hits.toString()}</td>
								<td class="h-11 px-4 text-right align-middle tabular-nums" data-testid="cache-usage-{row.orkflowName}-{row.taskId}-misses">{row.misses.toString()}</td>
								<td class="h-11 px-4 text-right align-middle tabular-nums">{row.bypassed.toString()}</td>
								<td class="h-11 px-4 text-right align-middle tabular-nums" data-testid="cache-usage-{row.orkflowName}-{row.taskId}-rate">{hitRateLabel(row)}</td>
								<td class="h-11 px-4 pr-5 text-right align-middle tabular-nums">{reusedLabel(row.reused)}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
		<p class="mt-2 text-xs text-fg-muted" data-testid="cache-scan-note">
			scanned {stats.runsScanned.toString()} of {stats.runsInWindow.toString()} finished runs. Reused is the producing
			attempts' duration the hits did not spend again.
		</p>
	</section>

	<section class="mb-6">
		<h3 class="mb-2 text-sm font-semibold text-fg">Memos</h3>
		{#if stats.inventory.length === 0}
			<p class="text-sm text-fg-muted" data-testid="cache-memos-empty">No memos stored.</p>
		{:else}
			<div class="overflow-x-auto rounded-md border border-border bg-surface">
				<table class="w-full min-w-[720px] border-collapse text-left text-sm" data-testid="cache-memos">
					<thead class="bg-surface-sunken">
						<tr>
							<th class="h-10 px-4 pl-5 text-xs font-medium text-fg-muted">Scope</th>
							<th class="h-10 px-4 text-xs font-medium text-fg-muted">Orkflow</th>
							<th class="h-10 px-4 text-xs font-medium text-fg-muted">Task</th>
							<th class="h-10 px-4 text-right text-xs font-medium text-fg-muted">Memos</th>
							<th class="h-10 px-4 text-xs font-medium text-fg-muted">Oldest</th>
							<th class="h-10 px-4 pr-5 text-xs font-medium text-fg-muted">Newest</th>
						</tr>
					</thead>
					<tbody>
						{#each stats.inventory as row (inventoryRowId(row))}
							<tr class="border-t border-border-subtle" data-testid="cache-memos-{inventoryRowId(row)}">
								<td class="h-11 px-4 pl-5 align-middle text-fg-secondary">{scopeName(row.scope)}</td>
								<td class="h-11 px-4 align-middle text-fg">{row.orkflowName || '—'}</td>
								<td class="h-11 px-4 align-middle font-mono text-fg-secondary">{row.taskId}</td>
								<td class="h-11 px-4 text-right align-middle tabular-nums">{row.memos.toString()}</td>
								<td class="h-11 px-4 align-middle text-fg-muted">{fmtInstant(row.oldest)}</td>
								<td class="h-11 px-4 pr-5 align-middle text-fg-muted">{fmtInstant(row.newest)}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	</section>
{/if}

{#if purgeResult && !purgeOpen}
	<p class="text-sm text-fg-secondary" data-testid="cache-purge-result">{purgeResult}</p>
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
					<legend class="mb-1 font-medium text-fg">Which memos</legend>
					<label class="flex items-center gap-2">
						<input type="radio" name="cache-purge-selector" value="orkflow" bind:group={selector} data-testid="cache-purge-orkflow" class="accent-brand-600" />
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
						<input type="radio" name="cache-purge-selector" value="namespace-scoped" bind:group={selector} data-testid="cache-purge-namespace-scoped" class="accent-brand-600" />
						The namespace-scoped memos
					</label>
					<label class="flex items-center gap-2">
						<input type="radio" name="cache-purge-selector" value="all" bind:group={selector} data-testid="cache-purge-all" class="accent-brand-600" />
						Every memo of the namespace
					</label>
				</fieldset>
				{#if selector !== 'all'}
					<label class="flex flex-col gap-1">
						<span class="text-fg">Task <span class="text-fg-muted">(optional)</span></span>
						<input
							data-testid="cache-purge-task"
							bind:value={purgeTask}
							placeholder="featurize"
							class="h-9 rounded-md border border-border bg-surface px-3 text-sm text-fg placeholder:text-fg-placeholder focus:border-brand-500"
						/>
					</label>
				{/if}
				<label class="flex flex-col gap-1">
					<span class="text-fg">Only memos older than <span class="text-fg-muted">(optional, e.g. 72h)</span></span>
					<input
						data-testid="cache-purge-older-than"
						bind:value={purgeOlderThan}
						placeholder="72h"
						class="h-9 rounded-md border border-border bg-surface px-3 text-sm text-fg placeholder:text-fg-placeholder focus:border-brand-500"
					/>
				</label>
				<p class="text-fg-muted">No run, commit or artifact is touched; the next run of these tasks runs its container.</p>
				{#if purgeInvalid}
					<p class="text-danger-700" data-testid="cache-purge-invalid">{purgeInvalid}</p>
				{/if}
				{#if purgeError}
					<p class="text-danger-700" data-testid="cache-purge-error">{purgeError}</p>
				{/if}
				{#if purgeResult}
					<p class="text-fg-secondary" data-testid="cache-purge-result">{purgeResult}</p>
				{/if}
				<div class="flex items-center justify-between gap-3">
					<CliEquivalent command={purgeCommand(purge)} testid="cache-purge-cli" />
					<div class="flex gap-2">
						<button
							type="button"
							onclick={() => (purgeOpen = false)}
							class="h-9 rounded-md border border-border bg-surface px-3 text-sm font-medium text-fg hover:bg-surface-hover"
							>Close</button
						>
						<button
							type="submit"
							data-testid="cache-purge-confirm"
							disabled={purging || Boolean(purgeInvalid)}
							class="h-9 rounded-md bg-danger-600 px-3 text-sm font-medium text-on-ink hover:bg-danger-700 disabled:opacity-50"
							>{purging ? 'Purging…' : 'Purge'}</button
						>
					</div>
				</div>
			</form>
		{/snippet}
	</Dialog>
{/if}
