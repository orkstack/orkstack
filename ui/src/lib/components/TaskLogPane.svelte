<script lang="ts">
	import { onDestroy, untrack } from 'svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import Select from '$lib/components/Select.svelte';
	import type { Status } from '$lib/gen/orkstack/v1/common_pb';
	import type { Timestamp } from '@bufbuild/protobuf/wkt';
	import { taskDispatched } from '$lib/status';
	import { followTaskLogs, type LogLine, type LogSocketState } from '$lib/taskLogs';

	// One task's output, followed live.
	//
	// The pane is collapsed until it is opened, and the socket exists only
	// while it is open: a run of thirty tasks would otherwise hold thirty
	// sockets the moment somebody glanced at the page.

	interface Props {
		runId: string;
		taskId: string;
		/** Wire value: empty is the default namespace. */
		namespace: string;
		status: Status;
		/**
		 * scheduledAt is the engine's dispatch stamp, and the only thing that
		 * tells a CANCELED task that RAN from one the run ended before it ever
		 * reached an Orker. Without it this pane opened a socket for an
		 * attempt that does not exist and the engine answered NotFound.
		 */
		scheduledAt?: Timestamp;
		/**
		 * attempt is the try to SHOW, and 0 means "the latest that wrote
		 * anything" — which follows the task onto its retries. It is the
		 * pane's initial coordinate; the selector below pins it from here on.
		 */
		attempt?: number;
		/** A task/attempt named in the URL opens this pane without changing normal browsing. */
		deepLinked?: boolean;
		/**
		 * pinRequest is the PAGE asking this pane to open on one attempt — the
		 * attempt timeline's "View logs", today.
		 *
		 * It is a request and not a coordinate because the two things a caller
		 * wants are not otherwise expressible: `attempt` alone cannot open a
		 * closed pane, and a reader who already chose an attempt from the
		 * selector below has a `pinned` value that would win over any prop. So
		 * the request carries a nonce, is served at most once, and lands as if
		 * the reader had opened the pane and picked that try themselves —
		 * leaving the selector showing what is on screen rather than
		 * disagreeing with it.
		 */
		pinRequest?: { attempt: number; nonce: number };
		onAttemptChange?: (attempt: number | null) => void;
		/**
		 * onAttemptSeen fires when this pane learns of a HIGHER attempt than it
		 * had seen before — the engine's own answer on the log stream, which is
		 * the earliest live signal anywhere in the browser that a task retried.
		 *
		 * It is a notification, not a control: nothing in this pane depends on
		 * what the listener does with it. The run page uses it to re-read the
		 * task's attempt records so the retry badge and the attempt timeline
		 * move with the run instead of waiting for its terminal snapshot.
		 */
		onAttemptSeen?: (attempt: number) => void;
		/**
		 * scrollback caps the lines KEPT AND RENDERED. A task may write far
		 * more than a browser can hold as DOM; past this many lines the oldest
		 * are dropped from the pane and counted, and the count is shown — the
		 * whole output stays in the artifact store and `orkctl run logs` reads
		 * it. Dropping silently is the one thing a log view must not do.
		 */
		scrollback?: number;
		/**
		 * item names one item of a fan-out task: the pane then follows that
		 * item's own log (its attempts, its chunks). Undefined for a task's own.
		 */
		item?: number;
	}

	let {
		runId,
		taskId,
		namespace,
		status,
		scheduledAt,
		attempt = 0,
		deepLinked = false,
		pinRequest,
		onAttemptChange,
		onAttemptSeen,
		scrollback = 2000,
		item
	}: Props = $props();

	let open = $state(false);
	// $state.raw: this array is replaced wholesale on every batch and can hold
	// thousands of entries. A deeply reactive proxy per line is the difference
	// between a chatty task rendering and a chatty task melting the tab.
	let lines = $state.raw<LogLine[]>([]);
	let dropped = $state(0);
	let truncated = $state(false);
	let ended = $state(false);
	let socketState = $state<LogSocketState>('connecting');
	// failure is set only by the TERMINAL state: the follow gave up, and this
	// is what it says went wrong. A view that cannot say why it stopped leaves
	// a reader watching a spinner for ever.
	let failure = $state('');
	let following = $state(true);
	let box: HTMLDivElement | undefined = $state(undefined);
	// The try being shown, as the ENGINE resolved it — 0 until the first chunk
	// says. A task that retries is followed onto its new attempt, and a pane
	// that did not say so would show an empty live stream where the reader
	// expects the output of the try that is actually running.
	let shownAttempt = $state(0);
	let retried = $state(false);
	// retryNonce is the reader asking again after a failure. It is part of the
	// subscription's identity below, so bumping it tears the dead follow down
	// and starts a fresh one — the same recovery ErrorPanel gives every other
	// view on this site, without reloading the page.
	let retryNonce = $state(0);
	let downloading = $state(false);
	let downloadError = $state('');
	let downloadStop: (() => void) | undefined;
	let search = $state('');

	// pinned is the reader's CHOICE of attempt, and null means they have not
	// made one: the pane shows whatever `attempt` says, which is the latest
	// try and follows the task onto its retries. A number pins the pane to
	// that try — the engine serves a finished attempt from the store, so the
	// output of the try that failed is readable while the one that replaced it
	// is still running.
	let pinned = $state<number | null>(null);
	const viewedAttempt = $derived(pinned ?? attempt);
	let openedForLink = $state(false);
	$effect(() => {
		if (deepLinked && !openedForLink) {
			open = true;
			openedForLink = true;
		}
	});

	// The page's pin request, served at most once per nonce.
	//
	// A nonce and not a value comparison: asking for the SAME attempt again is
	// a real ask — a reader who closed the pane and clicked "View logs" on that
	// row again means it, and an effect keyed on the attempt number would sit
	// there having already seen it. The address is written by the caller;
	// `page.url` does not move on a shallow replaceState, so the pane cannot
	// learn about the pin from the URL and is told instead.
	let servedPin = $state(0);
	$effect(() => {
		const request = pinRequest;
		if (!request || request.nonce <= untrack(() => servedPin)) return;
		servedPin = request.nonce;
		open = true;
		following = true;
		pinned = request.attempt > 0 ? request.attempt : null;
	});

	// maxAttempt LATCHES the highest try anybody has named, and in this tree
	// exactly two can name one: the engine's answer on THIS pane's stream,
	// which arrives with the first chunk and is the only source of the attempt
	// count, and the reader's own selection. `TaskRun.attempt`
	// (proto/orkstack/v1/run.proto) is not a third — nothing assigns it:
	// internal/engine/workflow.go builds every TaskRun without it and no later
	// write touches it, so a pane that read the run's record would be reading
	// 0 and calling it a count of tries. maxAttempt only grows, and the
	// subscription below deliberately does not reset it — pinning attempt 1
	// must not make the control that pinned it disappear, and a task that
	// retried does not un-retry.
	let maxAttempt = $state(0);
	$effect(() => {
		const seen = Math.max(shownAttempt, pinned ?? 0, attempt);
		if (seen > maxAttempt) {
			maxAttempt = seen;
			// Told, not asked: the latch only ever grows, so this fires once per
			// newly observed try and cannot loop. What the listener does with it
			// is none of this pane's business — see onAttemptSeen above.
			onAttemptSeen?.(seen);
		}
	});

	// stderr is shown in red, and the legend appears only once a red line has
	// actually arrived: a colour nobody can see needs no key.
	const hasStderr = $derived(lines.some((line) => line.stream === 'stderr'));
	const filteredLines = $derived.by(() => {
		const needle = search.trim().toLocaleLowerCase();
		return needle ? lines.filter((line) => line.text.toLocaleLowerCase().includes(needle)) : lines;
	});

	onDestroy(() => downloadStop?.());

	// A task with no attempt on record has no log prefix to read, and the
	// engine answers NotFound for it rather than an empty pane. Wait for it to
	// be dispatched; the run's event stream flips this the moment it is.
	//
	// CANCELED is the case a status alone cannot answer, which is why
	// taskDispatched reads the dispatch stamp too: a task killed mid-run has
	// output worth reading, and one the run canceled before it started has no
	// attempt at all.
	const started = $derived(taskDispatched(status, scheduledAt));

	// dispatched latches `started`: a task that has been dispatched once has
	// output to read for ever, so a later status reading as not-started must
	// not take this pane's subscription away. It changes once, false to true.
	let dispatched = $state(false);
	$effect(() => {
		if (started) dispatched = true;
	});

	const stateLabel = $derived.by(() => {
		if (ended) return 'ENDED';
		switch (socketState) {
			case 'open':
				return 'LIVE';
			case 'ended':
				return 'ENDED';
			case 'failed':
				return 'FAILED';
			case 'error':
			case 'closed':
				return 'RECONNECTING';
			default:
				return 'CONNECTING';
		}
	});

	// The coordinates this subscription is FOR, as one value that changes only
	// when one of them actually changes.
	//
	// It is load-bearing, and it was measured. These props are read through a
	// keyed each-item on the run page, so anything that invalidates the item
	// invalidates them — even when every value is identical. The subscription
	// below therefore re-ran on the task's own terminal status event: it tore
	// the socket down, opened a second one at from_seq=0 and replayed the
	// whole attempt, every line downloaded and rendered again (up to the
	// ORK_ORKER_LOG_MAX_BYTES an attempt may hold) — the exact opposite of the
	// resume `from_seq` exists for. And since a new subscription empties the
	// buffer, a reader parked mid-pane was thrown to the tail at the instant
	// the last line landed: PAUSED scrollTop=500 -> FOLLOWING scrollTop=976,
	// measured in a browser on the task that was ending.
	//
	// A $derived propagates only when its VALUE changes, so an each-item
	// replaced by an equal one stops here. The run page also patches its tasks
	// in place rather than replacing them; either one alone closes this, and a
	// log socket is worth both.
	const coordinates = $derived(
		[runId, taskId, namespace, viewedAttempt, retryNonce, item ?? ''].join('\u0000')
	);

	$effect(() => {
		if (!open || !dispatched) return;
		coordinates; // the ONE reactive dependency of this subscription
		// Untracked on purpose: reading a prop here would put the each-item
		// back in the dependency set and undo the paragraph above.
		const target = untrack(() => ({ runId, taskId, namespace, attempt: viewedAttempt, item }));
		// A subscription replays this attempt from its first chunk, so the pane
		// it feeds starts empty. Keeping what a previous subscription left
		// would double every line, and the numbering it is keyed by would
		// repeat. This now runs only for a genuinely different run, task,
		// namespace or attempt — which is why the follow is reset with it: a
		// different subject is a new read, and a new read starts at the tail.
		lines = [];
		dropped = 0;
		truncated = false;
		ended = false;
		following = true;
		shownAttempt = 0;
		retried = false;
		failure = '';
		socketState = 'connecting';
		return followTaskLogs({
			...target,
			onState: (s, detail) => {
				socketState = s;
				if (s === 'failed') failure = detail ?? 'The log stream stopped.';
			},
			onLines: (delivery) => {
				if (delivery.restarted) {
					// The task retried, and the engine moved this same socket
					// onto the new try. Its numbering starts again at 1, so
					// what is on screen cannot be kept: two attempts appended
					// would repeat every key and read as one impossible run.
					// The output of the try that failed is still in the
					// artifact store; the notice below says how to read it.
					lines = [];
					dropped = 0;
					truncated = false;
					retried = true;
				}
				if (delivery.attempt > 0) shownAttempt = delivery.attempt;
				if (delivery.lines.length > 0) {
					const next = lines.concat(delivery.lines);
					if (next.length > scrollback) {
						dropped += next.length - scrollback;
						lines = next.slice(next.length - scrollback);
					} else {
						lines = next;
					}
				}
				if (delivery.truncated) truncated = true;
				if (delivery.ended) ended = true;
			}
		});
	});

	// Follow the tail while following. Svelte runs this after the DOM is
	// updated, so scrollHeight already includes the batch just rendered.
	$effect(() => {
		lines; // track
		if (!box || !following) return;
		box.scrollTop = box.scrollHeight;
	});

	// Scrolling up pauses the follow, which is the whole point of scrollback:
	// output arriving must not yank the reader away from the line they are
	// reading. Scrolling back to the bottom resumes it, and so does the button.
	function onScroll() {
		if (!box) return;
		// A box with nothing to scroll cannot carry a reader's gesture: its
		// scrollTop is 0 and its distance to the bottom is 0, which is exactly
		// what "the reader is at the tail" looks like. Read as a gesture, that
		// silently resumes the follow whenever the content shrinks below one
		// screen — after a buffer reset, or on a task that wrote two lines —
		// and the reader who had scrolled up is returned to the tail by the
		// next line to arrive. A reset is not a gesture, so it is not read as
		// one.
		if (box.scrollHeight - box.clientHeight <= 8) return;
		const distance = box.scrollHeight - box.scrollTop - box.clientHeight;
		following = distance <= 8;
	}

	function jumpToLatest() {
		following = true;
		if (box) box.scrollTop = box.scrollHeight;
	}

	// retry re-opens the stream after it gave up. The follow starts again from
	// the beginning of the attempt, so the pane is cleared with it — a resume
	// is what the socket does by itself, and this is the case where there is
	// no socket left to resume.
	function retry() {
		retryNonce += 1;
	}

	// selectAttempt pins the pane to one try, or hands it back to `latest`.
	//
	// It needs no socket mechanics of its own: the selection is part of this
	// pane's coordinates, so changing it tears the follow down and opens a new
	// one on the try that was chosen, with an empty pane — which is the same
	// teardown a different run or task already got.
	function selectAttempt(value: string) {
		const next = Number(value);
		pinned = Number.isFinite(next) && next > 0 ? next : null;
		onAttemptChange?.(pinned);
	}

	// `latest` is 0 — the same wire value the old <option value="0"> carried,
	// so what `selectAttempt` reads has not changed.
	const attemptOptions = $derived([
		{ value: '0', label: 'latest' },
		...Array.from({ length: maxAttempt }, (_, i) => ({
			value: String(i + 1),
			label: String(i + 1)
		}))
	]);

	function toggle() {
		open = !open;
		if (open) following = true;
	}

	function downloadRetained(): void {
		if (downloading) return;
		downloading = true;
		downloadError = '';
		const parts: Uint8Array[] = [];
		const target = untrack(() => ({ runId, taskId, namespace, attempt: viewedAttempt, item }));
		let resolvedAttempt = target.attempt;
		const decoder = new TextDecoder();
		downloadStop = followTaskLogs({
			...target,
			follow: false,
			onChunk: (bytes) => parts.push(bytes.slice()),
			onLines: (delivery) => { if (delivery.attempt > 0) resolvedAttempt = delivery.attempt; },
			onState: (state, detail) => {
				if (state === 'ended') {
					const text = parts.map((part) => decoder.decode(part, { stream: true })).join('') + decoder.decode();
					const blob = new Blob([text], { type: 'text/plain;charset=utf-8' });
					const url = URL.createObjectURL(blob);
					const link = document.createElement('a');
					link.href = url;
					link.download = `${target.runId}-${target.taskId}${target.item !== undefined ? `-item-${target.item}` : ''}-attempt-${resolvedAttempt || 'latest'}.log`;
					link.click();
					URL.revokeObjectURL(url);
					downloading = false;
					downloadStop = undefined;
				}
				if (state === 'failed') {
					downloadError = detail ?? 'Could not replay the retained logs.';
					downloading = false;
					downloadStop = undefined;
				}
			}
		});
	}
</script>

<div class="mt-2">
	<button
		type="button"
		data-testid="task-log-toggle-{taskId}"
		onclick={toggle}
		aria-expanded={open}
		class="inline-flex h-8 items-center justify-center gap-1.5 rounded-md border border-border bg-surface px-3 text-xs font-medium text-fg transition-colors hover:border-border-strong hover:bg-surface-hover"
	>
		<span class="text-fg-muted">{open ? '▾' : '▸'}</span>
		{open ? 'Hide output' : 'Show output'}
	</button>

	{#if open}
		<div class="mt-2 overflow-hidden rounded-md border border-console-border bg-console-bg">
			<div
				class="flex flex-wrap items-center gap-3 border-b border-console-border bg-console-header px-3 py-2 font-mono text-2xs tracking-caps uppercase"
			>
				<span
					data-testid="task-log-state-{taskId}"
					class="inline-flex items-center gap-1.5 {stateLabel === 'LIVE'
						? 'text-console-live'
						: stateLabel === 'ENDED'
							? 'text-console-muted'
							: stateLabel === 'FAILED'
								? 'text-console-stderr'
								: 'text-console-notice'}"
				>
					{#if stateLabel === 'LIVE'}
						<span
							class="size-1.5 rounded-full bg-console-live animate-live-dot motion-reduce:animate-none"
							aria-hidden="true"
						></span>
					{/if}
					{stateLabel}</span
				>
				<span data-testid="task-log-follow-{taskId}" class="text-console-muted">
					{following ? 'FOLLOWING' : 'PAUSED'}
				</span>
				{#if maxAttempt > 1}
					<!--
						The task retried, so WHICH try is a question the reader
						can now answer. `latest` (0) is the default and today's
						behavior: it follows the task onto its next attempt.
						A number pins the pane to a finished try, which the
						engine serves from the store.
					-->
					<label class="flex items-center gap-1.5 text-console-muted normal-case">
						Attempt:
						<Select
							testid="task-log-attempt-select-{taskId}"
							variant="console"
							value={String(pinned ?? 0)}
							options={attemptOptions}
							onchange={selectAttempt}
						/>
					</label>
				{/if}
				{#if shownAttempt > 1 || pinned !== null}
					<span
						data-testid="task-log-attempt-{taskId}"
						class="rounded-full border border-warning-600/50 bg-warning-600/15 px-2 text-console-notice normal-case tracking-normal"
						>attempt {shownAttempt || pinned}</span
					>
				{/if}
				{#if hasStderr}
					<span
						data-testid="task-log-stderr-legend-{taskId}"
						class="text-console-stderr/80 normal-case tracking-normal"
					>
						stderr shown in red
					</span>
				{/if}
				{#if !following}
					<button
						type="button"
						data-testid="task-log-jump-{taskId}"
						onclick={jumpToLatest}
						class="h-6 rounded-sm border border-console-live/40 px-2 text-console-live normal-case tracking-normal hover:bg-console-live/10"
					>
						Jump to latest
					</button>
				{/if}
				<span class="ml-auto text-console-muted normal-case tracking-normal" data-testid="task-log-count-{taskId}">
					{lines.length} lines
				</span>
				<button
					type="button"
					data-testid="task-log-download-{taskId}"
					disabled={downloading || !dispatched}
					onclick={downloadRetained}
					class="h-6 rounded-sm border border-console-border px-2 text-console-fg normal-case tracking-normal hover:bg-ink/5 disabled:opacity-50"
				>
					{downloading ? 'Preparing download…' : 'Download retained logs'}
				</button>
				<label class="flex items-center gap-1 text-console-muted normal-case tracking-normal">
					Search
					<input data-testid="task-log-search-{taskId}" bind:value={search} placeholder="loaded lines" class="h-6 w-28 rounded-sm border border-console-border bg-console-bg px-1.5 text-console-fg placeholder:text-console-muted" />
				</label>
			</div>
			{#if downloadError}
				<p data-testid="task-log-download-error-{taskId}" class="border-b border-console-border px-3 py-1.5 font-mono text-xs text-console-stderr">
					{downloadError}
				</p>
			{/if}

			{#if dropped > 0}
				<p
					data-testid="task-log-dropped-{taskId}"
					class="border-b border-console-border px-3 py-1.5 font-mono text-xs text-console-notice"
				>
					{dropped} earlier lines are no longer shown here (scrollback is {scrollback} lines).
					The whole output is in the artifact store: <code class="text-console-fg"
						>orkctl run logs {runId} --task {taskId}{item !== undefined ? ` --item ${item}` : ''}</code
					>
				</p>
			{/if}

			{#if retried}
				<p
					data-testid="task-log-retried-{taskId}"
					class="border-b border-console-border px-3 py-1.5 font-mono text-xs text-console-notice"
				>
					This task retried, and this pane is now following attempt {shownAttempt}. The
					earlier attempt's output is still in the artifact store:
					<code class="text-console-fg"
						>orkctl run logs {runId} --task {taskId} --attempt {shownAttempt - 1}</code
					>
				</p>
			{/if}

			{#if socketState === 'failed'}
				<div class="border-b border-console-border bg-surface px-3 py-2">
					<ErrorPanel
						testid="task-log-error-{taskId}"
						retryTestid="task-log-retry-{taskId}"
						retryLabel="Reconnect"
						subject="the output of task {taskId}"
						message={failure}
						onretry={retry}
					/>
				</div>
			{/if}

			<div
				bind:this={box}
				onscroll={onScroll}
				data-testid="task-log-{taskId}"
				class="max-h-80 overflow-y-auto px-3 py-2 font-mono text-xs leading-5 [font-variant-ligatures:none] lg:max-h-[480px]"
			>
				{#if !started}
					<p data-testid="task-log-waiting-{taskId}" class="text-console-muted">
						This task has not been dispatched yet. Its output appears here when it starts.
					</p>
				{:else if lines.length === 0}
					<p data-testid="task-log-empty-{taskId}" class="text-console-muted">
						{#if ended}
							This task wrote nothing.
						{:else if socketState === 'failed'}
							Nothing was received. The stream stopped for the reason above.
						{:else}
							Waiting for output…
						{/if}
					</p>
				{:else if filteredLines.length === 0}
					<p data-testid="task-log-no-match-{taskId}" class="text-console-muted">No loaded lines match this search.</p>
				{:else}
					{#each filteredLines as line (line.n)}
						<!--
							Three renderings, in priority order: an ORKER notice
							(amber) is about the log rather than in it, stderr
							(red) is the task's other file descriptor as the
							Orker marked it, and everything else is stdout.
							data-stream carries the decoded origin, so a test
							asserts the fact rather than the colour.
						-->
						<div
							data-testid="task-log-line"
							data-line={line.n}
							data-stream={line.stream}
							class="whitespace-pre-wrap break-all {line.notice
								? 'text-console-notice'
								: line.stream === 'stderr'
									? 'text-console-stderr'
									: 'text-console-fg'}"
						>
							{line.text}
						</div>
					{/each}
				{/if}
			</div>

			{#if truncated}
				<p
					data-testid="task-log-truncated-{taskId}"
					class="border-t border-console-border px-3 py-1.5 font-mono text-xs text-console-notice"
				>
					This task's output passed its cap and was truncated by the Orker. Nothing beyond the
					notice above was stored.
				</p>
			{/if}
		</div>
	{/if}
</div>
