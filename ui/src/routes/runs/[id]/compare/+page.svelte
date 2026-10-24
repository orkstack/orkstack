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
	import Icon from '$lib/components/Icon.svelte';
	import { withNamespace } from '$lib/namespace';
	import { runCommand } from '$lib/runDetail';
	import { diffSummary, isNarrowed, unavailableLines } from '$lib/runDiff';
	import type { PageProps } from './$types';
	import SheetHeader from '$lib/components/SheetHeader.svelte';
	import { namespaceLabel } from '$lib/namespace';
	import { setTitleBlock } from '$lib/sheet.svelte';
	import { statusLabel } from '$lib/status';
	import { Status } from '$lib/gen/orkstack/v1/common_pb';

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

	// SAME is said quietly; DIFFERENT in the drafting blue, the colour of
	// "what changed" on this page (red stays for failure).
	const sameChip = 'stamp border-rule-strong bg-paper text-ink-secondary';
	const differentChip = 'stamp border-blue bg-blue-tint text-blue-strong';

	// The title block: the two runs by their status (never their ids — those
	// are the page's own links), what they ran, and the namespace.
	setTitleBlock(() => {
		const d = data.diff;
		const status = (s: Status | undefined) => ({ value: statusLabel(s), status: s ?? Status.UNSPECIFIED });
		return [
			{ label: 'FROM', ...status(d?.from?.status) },
			{ label: 'TO', ...status(d?.to?.status) },
			{ label: 'ORKFLOW', value: d ? (d.sameOrkflow ? (d.from?.orkflowName ?? '—') : 'different') : '—' },
			{
				label: 'DEFINITION',
				value: !d ? '—' : !d.fieldsCompared.includes('definition') ? 'not compared' : definitionIdentical ? 'same' : 'different'
			},
			{ label: 'NAMESPACE', value: namespaceLabel(data.namespace) }
		];
	});
</script>

<svelte:head><title>Compare {data.fromId} — OrkStack</title></svelte:head>

<a href={runHref(data.fromId)} class="link mb-3 inline-flex items-center gap-1 text-sm" data-testid="run-diff-back"
	><Icon name="chevronLeft" class="size-3.5" />Back to {data.fromId}</a
>
<SheetHeader title="Compare runs">
	{#snippet actions()}
		{#if diff}
			<a href={swapHref} data-testid="run-diff-swap" class="btn">Swap</a>
			<CliEquivalent command={cli} testid="run-diff-cli" />
		{/if}
	{/snippet}
</SheetHeader>

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
	<p class="text-sm text-ink-muted" data-testid="run-diff-choose">
		Choose a run to compare with: open
		<a href={runHref(data.fromId)} class="font-medium text-blue hover:underline">{data.fromId}</a>
		and use <span class="font-medium text-ink">Compare</span>.
	</p>
{:else if diff}
	{#snippet side(which: 'from' | 'to', label: string, id: string, run: typeof diff.from)}
		<dl class="min-w-0 border-t-2 border-ink pt-2">
			<dt class="caps text-ink-muted">{label}</dt>
			<dd class="mt-1">
				<a href={runHref(run?.id ?? id)} data-testid={which === 'from' ? 'run-diff-from' : 'run-diff-to'} class="link font-mono text-sm font-semibold break-all">{run?.id}</a>
			</dd>
			<dd class="mt-1.5 flex flex-wrap items-center gap-2 text-sm text-ink-secondary">
				<StatusPill status={run?.status} testid={which === 'from' ? 'run-diff-from-status' : 'run-diff-to-status'} />
				<span>{run?.orkflowName}@{run?.orkflowVersion}</span>
			</dd>
			{#if run?.orkflowDefinitionDigest}
				<dd class="mt-1 font-mono text-xs break-all text-ink-muted" title={run.orkflowDefinitionDigest}>
					{run.orkflowDefinitionDigest.slice(run.orkflowDefinitionDigest.indexOf(':') + 1, run.orkflowDefinitionDigest.indexOf(':') + 13)}
				</dd>
			{/if}
		</dl>
	{/snippet}
	<div class="mb-4 grid gap-6 sm:grid-cols-2">
		{@render side('from', 'From', data.fromId, diff.from)}
		{@render side('to', 'To', data.toId, diff.to)}
	</div>
	<div class="mb-6 flex flex-wrap items-center gap-2">
		<span data-testid="run-diff-orkflow" class={diff.sameOrkflow ? sameChip : differentChip}
			>{diff.sameOrkflow ? 'Same Orkflow' : 'Different Orkflows'}</span
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

	{#if isNarrowed(diff)}
		<p class="mb-4 font-mono text-xs text-ink-muted" data-testid="run-diff-fields">
			Compared: {diff.fieldsCompared.join(', ')}
		</p>
	{/if}

	{#if definitionRow}
		<p class="mb-4 font-mono text-xs break-all text-ink-secondary" data-testid="run-diff-definition-values">
			{definitionRow.fromValue} → {definitionRow.toValue}
		</p>
	{/if}

	<section class="region mb-6 pt-3">
		<h2 class="caps mb-2 text-ink-muted">Input</h2>
		{#if !compared('input')}
			<p class="text-sm text-ink-muted" data-testid="run-diff-input-not-compared">Input not compared</p>
		{:else if diff.inputUnifiedDiff === ''}
			<p class="text-sm text-ink-muted" data-testid="run-diff-input-identical">Inputs are identical</p>
		{:else}
			<DiffView diff={diff.inputUnifiedDiff} testid="run-diff-input" />
		{/if}
	</section>

	<section class="region mb-4 pt-3">
		<h2 class="caps mb-2 text-ink-muted">Tasks</h2>
		<RunDiffTable rows={diff.differences} />
	</section>

	<p class="text-sm text-ink" data-testid="run-diff-summary">{diffSummary(diff)}</p>
	{#if unavailableLines(diff).length > 0}
		<div class="mt-2 space-y-0.5 font-mono text-xs break-all text-ink-muted" data-testid="run-diff-unavailable">
			{#each unavailableLines(diff) as line, i (i)}
				<p>{line}</p>
			{/each}
		</div>
	{/if}
{/if}
