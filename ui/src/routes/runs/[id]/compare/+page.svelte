<script lang="ts">
	// Two runs of one namespace, side by side: what differs between them.
	//
	// The page renders RunService.DiffRuns and nothing else. The engine decides
	// every value and the order of the rows; `orkctl run diff` prints the same
	// strings from the same response, and the CliEquivalent below is the
	// command that does. Nothing here is stored, and the URL carries two ids
	// (and an optional field list) — never a run's input.
	import { invalidateAll } from '$app/navigation';
	import CliEquivalent from '$lib/components/CliEquivalent.svelte';
	import DiffView from '$lib/components/DiffView.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import RunDiffTable from '$lib/components/RunDiffTable.svelte';
	import StatusPill from '$lib/components/StatusPill.svelte';
	import { withNamespace } from '$lib/namespace';
	import { runCommand } from '$lib/runDetail';
	import { diffSummary, isNarrowed, unavailableLines } from '$lib/runDiff';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	let retrying = $state(false);

	const diff = $derived(data.diff);
	const runHref = (id: string) => withNamespace(`/runs/${encodeURIComponent(id)}`, data.namespace);
	const onlyQuery = $derived(data.only.length ? `&only=${encodeURIComponent(data.only.join(','))}` : '');
	const swapHref = $derived(
		withNamespace(
			`/runs/${encodeURIComponent(data.toId)}/compare?to=${encodeURIComponent(data.fromId)}${onlyQuery}`,
			data.namespace
		)
	);
	const cli = $derived(
		runCommand(data.namespace, [
			'run',
			'diff',
			data.fromId,
			data.toId,
			...(data.only.length ? ['--only', data.only.join(',')] : [])
		])
	);
	const compared = (field: string) => diff?.fieldsCompared.includes(field) ?? false;
	const definitionRow = $derived(diff?.differences.find((row) => row.taskId === '' && row.field === 'definition'));
	const definitionIdentical = $derived(!!diff && diff.sameDefinitionDigest && !definitionRow);

	async function retry(): Promise<void> {
		retrying = true;
		try {
			await invalidateAll();
		} finally {
			retrying = false;
		}
	}

	const chip = 'inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-medium whitespace-nowrap';
	const sameChip = `${chip} border-border bg-surface-sunken text-fg-muted`;
	const differentChip = `${chip} border-warning-200 bg-warning-50 text-warning-700`;
</script>

<svelte:head><title>Compare {data.fromId} — OrkStack</title></svelte:head>

<div class="mb-4">
	<a href={runHref(data.fromId)} class="text-sm font-medium text-brand-700 hover:underline" data-testid="run-diff-back"
		>← Back to {data.fromId}</a
	>
	<h1 class="mt-2 text-xl font-semibold text-fg">Compare runs</h1>
</div>

{#if data.error}
	<ErrorPanel
		testid="run-diff-error"
		retryTestid="run-diff-retry"
		subject="the comparison of {data.fromId} with {data.toId}"
		message={data.error}
		busy={retrying}
		onretry={retry}
	/>
{:else if !data.toId}
	<p class="text-sm text-fg-muted" data-testid="run-diff-choose">
		Choose a run to compare with: open
		<a href={runHref(data.fromId)} class="font-medium text-brand-700 hover:underline">{data.fromId}</a>
		and use <span class="font-medium text-fg">Compare</span>.
	</p>
{:else if diff}
	<div class="mb-6 flex flex-wrap items-start gap-4">
		<div class="grid min-w-0 gap-2 text-sm">
			<div class="flex flex-wrap items-center gap-2">
				<span class="w-10 text-fg-muted">From</span>
				<a
					href={runHref(diff.from?.id ?? data.fromId)}
					data-testid="run-diff-from"
					class="font-mono font-medium break-all text-brand-700 hover:underline">{diff.from?.id}</a
				>
				<StatusPill status={diff.from?.status} testid="run-diff-from-status" />
				<span class="text-fg-muted">{diff.from?.orkflowName}@{diff.from?.orkflowVersion}</span>
			</div>
			<div class="flex flex-wrap items-center gap-2">
				<span class="w-10 text-fg-muted">To</span>
				<a
					href={runHref(diff.to?.id ?? data.toId)}
					data-testid="run-diff-to"
					class="font-mono font-medium break-all text-brand-700 hover:underline">{diff.to?.id}</a
				>
				<StatusPill status={diff.to?.status} testid="run-diff-to-status" />
				<span class="text-fg-muted">{diff.to?.orkflowName}@{diff.to?.orkflowVersion}</span>
			</div>
			<div class="flex flex-wrap items-center gap-2">
				<span data-testid="run-diff-orkflow" class={diff.sameOrkflow ? sameChip : differentChip}
					>{diff.sameOrkflow ? 'Same orkflow' : 'Different orkflows'}</span
				>
				{#if compared('definition')}
					<span
						data-testid="run-diff-definition"
						class={definitionIdentical ? sameChip : differentChip}
						title={definitionRow ? `${definitionRow.fromValue} → ${definitionRow.toValue}` : diff.from?.orkflowDefinitionDigest}
						>{definitionIdentical ? 'Definition identical' : 'Definition differs'}</span
					>
				{:else}
					<span data-testid="run-diff-definition" class={sameChip}>Definition not compared</span>
				{/if}
			</div>
		</div>
		<div class="ml-auto flex items-center gap-2">
			<a
				href={swapHref}
				data-testid="run-diff-swap"
				class="inline-flex h-9 items-center rounded-md border border-border bg-surface px-3 text-sm font-medium text-fg hover:bg-surface-hover"
				>Swap</a
			>
			<CliEquivalent command={cli} testid="run-diff-cli" />
		</div>
	</div>

	{#if isNarrowed(diff)}
		<p class="mb-4 font-mono text-xs text-fg-muted" data-testid="run-diff-fields">
			Compared: {diff.fieldsCompared.join(', ')}
		</p>
	{/if}

	{#if definitionRow}
		<p class="mb-4 font-mono text-xs break-all text-fg-secondary" data-testid="run-diff-definition-values">
			{definitionRow.fromValue} → {definitionRow.toValue}
		</p>
	{/if}

	<section class="mb-6">
		<h2 class="mb-2 text-sm font-semibold text-fg">Input</h2>
		{#if !compared('input')}
			<p class="text-sm text-fg-muted" data-testid="run-diff-input-not-compared">Input not compared</p>
		{:else if diff.inputUnifiedDiff === ''}
			<p class="text-sm text-fg-muted" data-testid="run-diff-input-identical">Inputs are identical</p>
		{:else}
			<DiffView diff={diff.inputUnifiedDiff} testid="run-diff-input" />
		{/if}
	</section>

	<section class="mb-4">
		<h2 class="mb-2 text-sm font-semibold text-fg">Tasks</h2>
		<RunDiffTable rows={diff.differences} />
	</section>

	<p class="text-sm text-fg" data-testid="run-diff-summary">{diffSummary(diff)}</p>
	{#if unavailableLines(diff).length > 0}
		<div class="mt-2 space-y-0.5 font-mono text-xs break-all text-fg-muted" data-testid="run-diff-unavailable">
			{#each unavailableLines(diff) as line, i (i)}
				<p>{line}</p>
			{/each}
		</div>
	{/if}
{/if}
