<script lang="ts">
	// EVERY TRY, INCLUDING THE ONES THAT LOST.
	//
	// A task's snapshot describes its FINAL attempt and nothing else: Temporal
	// discards an intermediate failure the moment it schedules the next try, so
	// "it failed twice with a connection reset, then passed" is a sentence
	// nothing on this page could say. `ListTaskAttempts` reads back the records
	// the Orker filed per attempt, and this is where they are rendered.
	//
	// Three rules the timeline holds to:
	//
	//   1. THE ORDER IS THE SERVER'S. Rows are ascending by attempt and gapless
	//      from 1, and are drawn exactly as they arrive — no sort, no regroup.
	//   2. AN UNREPORTED ROW IS MISSING EVIDENCE, NOT A FAILURE. It is dashed
	//      and grey, never rose, and it says why it is empty rather than
	//      leaving a reader to think the page dropped something.
	//   3. NOTHING IS INVENTED. A field the record does not carry is not drawn,
	//      so a row with no timing shows no timing rather than a zero.
	import CliEquivalent from '$lib/components/CliEquivalent.svelte';
	import FailurePanel from '$lib/components/FailurePanel.svelte';
	import { AttemptOutcome, type TaskAttempt } from '$lib/gen/orkstack/v1/run_pb';
	import {
		UNREPORTED_CAPTION,
		attemptOutcomeClasses,
		attemptOutcomeLabel,
		isUnreported
	} from '$lib/attempts';
	import { hasError } from '$lib/activity';
	import { formatDuration, runCommand } from '$lib/runDetail';
	import { fmtTime } from '$lib/status';
	import { attemptPhases, formatQuantity, phasesLine, type TaskTiming } from '$lib/timing';
	import { attemptCacheLine, keyComponentRows } from '$lib/cache';

	interface Props {
		runId: string;
		taskId: string;
		/** Wire value: empty is the default namespace. */
		namespace: string;
		/** This task's rows, in the order ListTaskAttempts returned them. */
		attempts: TaskAttempt[];
		/** Pins the task's log pane to one attempt — the same control its selector uses. */
		onViewLogs: (attempt: number) => void;
		/** The pool the task was dispatched to, for the wait-for-an-Orker line. */
		pool?: string;
		/** The attempt that decided the task (TaskRun.attempt). */
		decidedOn?: number;
		/** The task's derived timing ($lib/timing), for the wait for an Orker. */
		timing?: TaskTiming;
		/** One item of a fan-out: the timeline is that item's, and its CLI says --item. */
		item?: number;
	}

	let {
		runId,
		taskId,
		namespace,
		attempts,
		onViewLogs,
		pool = '',
		decidedOn = 0,
		timing,
		item
	}: Props = $props();

	// Open by default: this disclosure already lives INSIDE the card's Inspect,
	// and a reader who opened Inspect on a task that retried is asking exactly
	// this question. The toggle is here so a long timeline can be folded away
	// again, not to make the answer cost two clicks.
	let open = $state(true);
	// Which failed attempts have their error expanded, keyed by attempt number.
	let errorOpen = $state<Record<number, boolean>>({});

	/**
	 * measuredExit keeps proto3's missing field presence from being drawn as a
	 * measurement, exactly as the task card does: a succeeded attempt really
	 * measured 0, any non-zero value was really measured, and a 0 on an attempt
	 * whose container never started is the generated zero value and not
	 * evidence of a clean exit.
	 */
	function measuredExit(attempt: TaskAttempt): number | undefined {
		if (isUnreported(attempt)) return undefined;
		if (attempt.outcome === AttemptOutcome.SUCCEEDED || attempt.exitCode !== 0) {
			return attempt.exitCode;
		}
		return undefined;
	}

	/** The Orker and the pool it answered for, as one cell; empty when unrecorded. */
	function orkerCell(attempt: TaskAttempt): string {
		if (attempt.orkerId && attempt.pool) return `${attempt.orkerId} / ${attempt.pool}`;
		return attempt.orkerId || attempt.pool;
	}
</script>

<div class="mt-3 border-t border-border-subtle pt-3" data-testid="task-{taskId}-attempts">
	<div class="flex flex-wrap items-center justify-between gap-2">
		<button
			type="button"
			data-testid="task-{taskId}-attempts-toggle"
			aria-expanded={open}
			onclick={() => (open = !open)}
			class="inline-flex items-center gap-1.5 text-xs font-medium text-fg uppercase"
		>
			<span class="text-fg-muted">{open ? '▾' : '▸'}</span>
			Attempts ({attempts.length})
		</button>
		{#if attempts.length > 0}
			<CliEquivalent
				command={runCommand(namespace, ['run', 'attempts', runId, '--task', taskId, ...(item !== undefined ? ['--item', String(item)] : [])])}
				testid="task-{taskId}-attempts-cli"
				label="Copy CLI"
			/>
		{/if}
	</div>

	{#if open}
		<!--
			THE WAIT FOR AN ORKER, only for a task decided on a retry. For a task
			decided on its first attempt the card's Wait segment already IS that wait,
			and saying it twice would suggest two different numbers. For a retried
			task Wait includes the earlier attempts and the backoff between them,
			so the time before attempt 1 started is stated here, off attempt 1's
			own record — the same rule and reasons as `orkctl run timing`.
		-->
		{#if timing && decidedOn > 1 && timing.queueWait.kind !== 'na'}
			<p class="mt-2 text-xs text-fg-secondary" data-testid="attempt-{taskId}-orker-wait">
				{#if timing.queueWait.kind === 'value'}
					Waited for an Orker {formatQuantity(timing.queueWait)}{pool ? ` on pool ${pool}` : ''}
				{:else}
					Wait for an Orker: unavailable ({timing.queueWait.reason})
				{/if}
			</p>
		{/if}
		{#if attempts.length === 0}
			<!--
				An empty list is a valid answer, not an error — and it is not the
				same statement as an UNREPORTED row. Records live on the same
				lifecycle as logs, so a run past cleanup has none at all.
			-->
			<p class="mt-2 text-xs text-fg-muted" data-testid="task-{taskId}-attempts-empty">
				No attempt records for this task. They are written as each attempt ends and are
				reclaimed with the run's logs.
			</p>
		{:else}
			<ul class="mt-2 space-y-1.5">
				{#each attempts as attempt (attempt.attempt)}
					{@const unreported = isUnreported(attempt)}
					{@const exitCode = measuredExit(attempt)}
					{@const orker = orkerCell(attempt)}
					{@const failure = hasError(attempt.error) ? attempt.error : undefined}
					<li
						data-testid="task-{taskId}-attempt-{attempt.attempt}"
						data-outcome={attemptOutcomeLabel(attempt.outcome)}
						class="rounded-md border px-3 py-2 {unreported
							? 'border-dashed border-border-subtle bg-transparent opacity-70'
							: 'border-border-subtle bg-surface'}"
					>
						<div class="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-fg-muted">
							<span class="font-mono font-medium text-fg-secondary">#{attempt.attempt}</span>
							<span
								data-testid="task-{taskId}-attempt-{attempt.attempt}-outcome"
								class="inline-flex items-center rounded-full border px-2 py-0.5 text-2xs font-medium tracking-caps whitespace-nowrap uppercase leading-4 {attemptOutcomeClasses(
									attempt.outcome
								)}"
							>
								{attemptOutcomeLabel(attempt.outcome)}
							</span>
							{#if attempt.phase}<span title="the phase the attempt ended in">{attempt.phase}</span>{/if}
							{#if attempt.startedAt}<span>{fmtTime(attempt.startedAt)}</span>{/if}
							{#if attempt.startedAt && attempt.finishedAt}
								<span>{formatDuration(attempt.startedAt, attempt.finishedAt)}</span>
							{/if}
							{#if orker}<span class="font-mono break-all">{orker}</span>{/if}
							{#if exitCode !== undefined}
								<span class={exitCode === 0 ? '' : 'text-failed-fg'}>exit {exitCode}</span>
							{/if}
							{#if attempt.commitAdopted}
								<span title="this attempt found an existing commit and returned it without running the container">adopted</span>
							{/if}
							<span class="ml-auto flex items-center gap-2">
								{#if failure}
									<button
										type="button"
										data-testid="task-{taskId}-attempt-{attempt.attempt}-error-toggle"
										aria-expanded={errorOpen[attempt.attempt] ?? false}
										onclick={() =>
											(errorOpen[attempt.attempt] = !errorOpen[attempt.attempt])}
										class="font-medium text-brand-700 hover:underline"
									>
										{errorOpen[attempt.attempt] ? 'Hide error' : 'Show error'}
									</button>
								{/if}
								<!--
									The same pin the pane's own attempt selector sets, which is
									URL state: the address ends up naming this task and this
									try, so what a reader is looking at is a link they can send.
									An UNREPORTED attempt keeps the action — the log store is
									often the very reason we know that attempt happened.
								-->
								<button
									type="button"
									data-testid="task-{taskId}-attempt-{attempt.attempt}-logs"
									onclick={() => onViewLogs(attempt.attempt)}
									class="font-medium text-brand-700 hover:underline">View logs</button
								>
							</span>
						</div>
						{#if unreported}
							<p
								class="mt-1 text-2xs text-fg-muted"
								data-testid="task-{taskId}-attempt-{attempt.attempt}-unreported"
							>
								{UNREPORTED_CAPTION}
							</p>
						{:else}
							<!--
								Every §7.1 phase the attempt passed through, in order,
								with how long each took — the Orker's own record. An
								UNREPORTED row has nothing here (rule 3); a record
								written before phases were recorded says so.
							-->
							{@const phases = attemptPhases(attempt)}
							<p
								class="mt-1 font-mono text-2xs break-words text-fg-muted"
								data-testid="attempt-{taskId}-{attempt.attempt}-phases"
							>
								{#if phases.kind === 'phases'}
									{phasesLine(phases.phases)}
								{:else}
									phases unavailable ({phases.reason})
								{/if}
							</p>
						{/if}
						{#if attempt.cache}
							<!--
								This attempt's memo decision (a cached task), and the
								digests its key was computed from: what tells a reader
								which input changed between a miss and the run whose
								memo it no longer matches.
							-->
							<div class="mt-1 text-2xs text-fg-muted" data-testid="task-{taskId}-attempt-{attempt.attempt}-cache">
								<span>{attemptCacheLine(attempt.cache)}</span>
								{#if keyComponentRows(attempt.cache.keyComponents).length > 0}
									<details class="mt-1" data-testid="task-{taskId}-attempt-{attempt.attempt}-cache-components">
										<summary class="cursor-pointer font-medium text-brand-700">Key components</summary>
										<dl class="mt-1 grid grid-cols-[max-content_1fr] gap-x-3 gap-y-0.5 font-mono">
											{#each keyComponentRows(attempt.cache.keyComponents) as [name, value] (name)}
												<dt>{name}</dt>
												<dd class="break-all text-fg-secondary">{value}</dd>
											{/each}
										</dl>
									</details>
								{/if}
							</div>
						{/if}
						{#if failure && errorOpen[attempt.attempt]}
							<div class="mt-2">
								<FailurePanel
									error={failure}
									testid="task-{taskId}-attempt-{attempt.attempt}-error"
									compact
								/>
							</div>
						{/if}
					</li>
				{/each}
			</ul>
		{/if}
	{/if}
</div>
