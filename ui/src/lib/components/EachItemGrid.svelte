<script lang="ts">
	// A fan-out's card body (docs/DSL.md "Fan-out"): one summary line, one
	// cell per item, and — for the item a reader selects — that item's own
	// panel: its facts, its failure, its attempts, its log, its artifacts and
	// the CLI that reads each of them. It renders in place of the task's log
	// pane: a fan-out's own row has no log; every item has one.
	//
	// Nothing here reads on its own. The page owns the run's state — the item
	// views patched from item events, the attempt timelines keyed convert[97],
	// the live map — and this component draws it and reports what the reader
	// chose.
	import type { EachSpec } from '$lib/gen/orkstack/v1/orkflow_pb';
	import type { TaskAttempt, TaskEach } from '$lib/gen/orkstack/v1/run_pb';
	import type { LiveAttempt, RunArtifact } from '$lib/gen/orkstack/v1/run_service_pb';
	import { Status } from '$lib/gen/orkstack/v1/common_pb';
	import { eachSummary, isCachedItem, itemAriaLabel, itemCli, itemLabel, itemStatusText, type ItemView } from '$lib/each';
	import CacheBadge from './CacheBadge.svelte';
	import { STATUS_CLASSES, fmtTimestamp, isTerminal } from '$lib/status';
	import { attemptsSeen } from '$lib/attempts';
	import { formatDuration } from '$lib/runDetail';
	import { liveLine } from '$lib/live';
	import AttemptTimeline from './AttemptTimeline.svelte';
	import CliEquivalent from './CliEquivalent.svelte';
	import FailurePanel from './FailurePanel.svelte';
	import TaskLogPane from './TaskLogPane.svelte';
	import ArtifactProvenance from './ArtifactProvenance.svelte';

	interface Props {
		runId: string;
		/** Wire value: empty is the default namespace. */
		namespace: string;
		taskId: string;
		each?: TaskEach;
		/** The definition's `each:`, for the not-yet-expanded summary. */
		spec?: EachSpec;
		items: ItemView[];
		/** The selected item, or undefined for none. */
		selected?: number;
		onSelect: (item: number | undefined) => void;
		/** One item's attempt timeline, as the page holds it. */
		attemptsOf: (item: number) => TaskAttempt[];
		/** This task's recorded artifacts; an item's carry its index. */
		artifacts: RunArtifact[];
		downloading: string;
		onDownload: (record: RunArtifact) => void;
		/** The live heartbeat of a running item, keyed by index. */
		liveOf: (item: number) => LiveAttempt | undefined;
		liveNowMs: number;
		/** The attempt the URL named for the selected item, 0 for latest. */
		attempt?: number;
		deepLinked?: boolean;
		pinRequest?: { attempt: number; nonce: number };
		onAttemptChange?: (attempt: number | null) => void;
		onAttemptSeen?: (attempt: number) => void;
		onViewLogs: (attempt: number) => void;
	}

	let {
		runId,
		namespace,
		taskId,
		each,
		spec,
		items,
		selected,
		onSelect,
		attemptsOf,
		artifacts,
		downloading,
		onDownload,
		liveOf,
		liveNowMs,
		attempt = 0,
		deepLinked = false,
		pinRequest,
		onAttemptChange,
		onAttemptSeen,
		onViewLogs
	}: Props = $props();

	const summary = $derived(eachSummary(each?.expanded ?? false, items, spec));
	const current = $derived(selected !== undefined ? items[selected] : undefined);
	const currentAttempts = $derived(current ? attemptsOf(current.index) : []);
	const currentArtifacts = $derived(
		current ? artifacts.filter((record) => record.item === current.index) : []
	);

	let grid = $state<HTMLDivElement | undefined>();

	// The grid is one tab stop's worth of buttons a keyboard can walk: the
	// arrows move between cells, Home and End jump to either end. Every cell
	// is still a real button, so Tab and Enter work without any of this.
	function onGridKey(event: KeyboardEvent): void {
		const cells = grid ? Array.from(grid.querySelectorAll<HTMLButtonElement>('button[data-item]')) : [];
		const at = cells.indexOf(document.activeElement as HTMLButtonElement);
		if (at < 0) return;
		let next = at;
		switch (event.key) {
			case 'ArrowRight':
			case 'ArrowDown':
				next = Math.min(at + 1, cells.length - 1);
				break;
			case 'ArrowLeft':
			case 'ArrowUp':
				next = Math.max(at - 1, 0);
				break;
			case 'Home':
				next = 0;
				break;
			case 'End':
				next = cells.length - 1;
				break;
			default:
				return;
		}
		event.preventDefault();
		cells[next]?.focus();
	}

	function cli(command: string, index: number): string {
		return itemCli({ namespace, runId, taskId, item: index, command });
	}
</script>

<div class="mt-3" data-testid="task-{taskId}-each">
	<p class="text-xs text-ink-secondary" data-testid="task-{taskId}-each-summary">{summary}</p>
	{#if items.length > 0}
		<!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
		<div
			bind:this={grid}
			role="group"
			aria-label="Items of {taskId}"
			class="mt-2 flex flex-wrap gap-1"
			data-testid="task-{taskId}-item-grid"
			onkeydown={onGridKey}
		>
			{#each items as item (item.index)}
				<button
					type="button"
					data-item={item.index}
					data-testid="task-{taskId}-item-{item.index}"
					data-status={itemStatusText(item)}
					data-cached={isCachedItem(item) ? 'true' : undefined}
					aria-label={itemAriaLabel(item)}
					aria-pressed={selected === item.index}
					title={itemAriaLabel(item)}
					onclick={() => onSelect(selected === item.index ? undefined : item.index)}
					class="relative inline-flex h-7 min-w-7 items-center justify-center rounded-sm border px-1 font-mono text-2xs leading-none {STATUS_CLASSES[
						item.status
					]} {selected === item.index ? 'ring-2 ring-blue' : ''}"
				>{item.index}{#if isCachedItem(item)}<span
							aria-hidden="true"
							class="absolute top-0 right-0 size-1.5 rounded-bl-sm bg-ink"
						></span>{/if}</button>
			{/each}
		</div>
	{/if}

	{#if current}
		{@const run = current.run}
		{@const live = current.status === Status.RUNNING ? liveOf(current.index) : undefined}
		<div class="mt-3 rounded-sm border border-rule-faint bg-paper-sunken p-3" data-testid="task-{taskId}-item-panel">
			<div class="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-ink-muted">
				<span class="font-mono font-medium text-ink" data-testid="task-{taskId}-item-panel-label">{itemLabel(taskId, current.index)}</span>
				<span data-testid="task-{taskId}-item-panel-status">{itemStatusText(current)}</span>
				<span data-testid="task-{taskId}-item-panel-attempt">Attempt {current.attempt}</span>
				<CacheBadge cache={run?.cache} {namespace} testid="task-{taskId}-item-panel-cache" />
				{#if run?.orkerId}<span data-testid="task-{taskId}-item-panel-orker">Orker {run.orkerId}</span>{/if}
				{#if run?.orkerId && isTerminal(current.status)}<span data-testid="task-{taskId}-item-panel-exit">Exit {run.exitCode}</span>{/if}
				{#if run?.scheduledAt}<span>Scheduled {fmtTimestamp(run.scheduledAt)}</span>{/if}
				{#if run?.startedAt}<span>Started {fmtTimestamp(run.startedAt)}</span>{/if}
				{#if run?.finishedAt}<span>Finished {fmtTimestamp(run.finishedAt)} ({formatDuration(run.startedAt ?? run.scheduledAt, run.finishedAt)})</span>{/if}
				{#if live}
					{@const line = liveLine(live, liveNowMs)}
					<span
						data-testid="task-{taskId}-item-panel-live"
						data-live={line.kind}
						data-stale={line.stale}
						title={line.title}
						class={line.stale ? 'font-medium text-ink-secondary' : ''}>{line.text}</span
					>
				{/if}
			</div>
			{#if current.message && !run?.error?.message}
				<p class="mt-1 text-xs break-words text-ink-secondary">{current.message}</p>
			{/if}
			<div class="mt-2 flex flex-wrap items-center gap-2" data-testid="task-{taskId}-item-panel-cli">
				<CliEquivalent command={cli('logs', current.index)} testid="task-{taskId}-item-panel-cli-logs" label="Copy this item's logs CLI" />
				<CliEquivalent command={cli('artifacts', current.index)} testid="task-{taskId}-item-panel-cli-artifacts" label="Copy this item's artifacts CLI" />
				<CliEquivalent command={cli('attempts', current.index)} testid="task-{taskId}-item-panel-cli-attempts" label="Copy this item's attempts CLI" />
			</div>
			{#if run?.error?.message}
				<div class="mt-3">
					<FailurePanel
						error={run.error}
						testid="task-{taskId}-item-{current.index}-error"
						compact
						retryState={run.retryState}
						attemptCount={attemptsSeen(currentAttempts)}
					/>
				</div>
			{/if}
			{#if currentAttempts.length > 0}
				<AttemptTimeline
					{runId}
					{taskId}
					{namespace}
					item={current.index}
					attempts={currentAttempts}
					pool={run?.pool ?? ''}
					decidedOn={current.attempt}
					onViewLogs={onViewLogs}
				/>
			{/if}
			{#key current.index}
				<TaskLogPane
					{runId}
					{taskId}
					{namespace}
					item={current.index}
					status={current.status}
					scheduledAt={run?.scheduledAt ?? run?.startedAt}
					{attempt}
					{deepLinked}
					{pinRequest}
					{onAttemptChange}
					{onAttemptSeen}
				/>
			{/key}
			{#if currentArtifacts.length > 0}
				<ul class="mt-3 space-y-1 text-xs" data-testid="task-{taskId}-item-panel-artifacts">
					{#each currentArtifacts as record (`${record.kind}:${record.artifact?.name}`)}
						{#if record.artifact}
							{@const key = `${record.taskId}:${record.item}:${record.kind}:${record.artifact.name}`}
							<li class="flex flex-wrap items-center gap-2">
								<span class="font-mono break-all text-ink">{record.artifact.name}</span>
								<span class="text-ink-muted">{record.artifact.sizeBytes} bytes</span>
								<ArtifactProvenance {runId} {namespace} {taskId} kind={record.kind} name={record.artifact.name} item={current.index} />
								<button
									type="button"
									data-testid="download-artifact-{taskId}-item-{current.index}-{record.artifact.name}"
									onclick={() => onDownload(record)}
									disabled={downloading === key}
									class="ml-auto font-medium text-blue hover:underline disabled:opacity-50"
									>{downloading === key ? 'Downloading…' : 'Download'}</button
								>
							</li>
						{/if}
					{/each}
				</ul>
			{/if}
		</div>
	{/if}
</div>
