<script lang="ts">
	import { untrack } from 'svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import { Status } from '$lib/gen/orkstack/v1/common_pb';
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
		 * attempt is the try to SHOW, and 0 means "the latest that wrote
		 * anything" — which follows the task onto its retries. It is the
		 * pane's initial coordinate; the selector below pins it from here on.
		 */
		attempt?: number;
		/**
		 * scrollback caps the lines KEPT AND RENDERED. A task may write far
		 * more than a browser can hold as DOM; past this many lines the oldest
		 * are dropped from the pane and counted, and the count is shown — the
		 * whole output stays in the artifact store and `orkctl run logs` reads
		 * it. Dropping silently is the one thing a log view must not do.
		 */
		scrollback?: number;
	}

	let {
		runId,
		taskId,
		namespace,
		status,
		attempt = 0,
		scrollback = 2000
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

	// pinned is the reader's CHOICE of attempt, and null means they have not
	// made one: the pane shows whatever `attempt` says, which is the latest
	// try and follows the task onto its retries. A number pins the pane to
	// that try — the engine serves a finished attempt from the store, so the
	// output of the try that failed is readable while the one that replaced it
	// is still running.
	let pinned = $state<number | null>(null);
	const viewedAttempt = $derived(pinned ?? attempt);

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
		const seen = Math.max(shownAttempt, pinned ?? 0);
		if (seen > maxAttempt) maxAttempt = seen;
	});

	// stderr is shown in red, and the legend appears only once a red line has
	// actually arrived: a colour nobody can see needs no key.
	const hasStderr = $derived(lines.some((line) => line.stream === 'stderr'));

	// A task with no attempt on record has no log prefix to read, and the
	// engine answers NotFound for it rather than an empty pane. Wait for it to
	// be dispatched; the run's event stream flips this the moment it is.
	const started = $derived(
		status !== Status.PENDING && status !== Status.UNSPECIFIED && status !== Status.SKIPPED
	);

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
		[runId, taskId, namespace, viewedAttempt, retryNonce].join('\u0000')
	);

	$effect(() => {
		if (!open || !dispatched) return;
		coordinates; // the ONE reactive dependency of this subscription
		// Untracked on purpose: reading a prop here would put the each-item
		// back in the dependency set and undo the paragraph above.
		const target = untrack(() => ({ runId, taskId, namespace, attempt: viewedAttempt }));
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
	}

	function toggle() {
		open = !open;
		if (open) following = true;
	}
</script>

<div class="mt-2">
	<button
		type="button"
		data-testid="task-log-toggle-{taskId}"
		onclick={toggle}
		aria-expanded={open}
		class="inline-flex items-center gap-1.5 rounded-md border border-zinc-700 px-2 py-1 text-xs text-zinc-300 hover:bg-zinc-800"
	>
		<span class="text-zinc-500">{open ? '▾' : '▸'}</span>
		{open ? 'Hide output' : 'Show output'}
	</button>

	{#if open}
		<div class="mt-2 rounded-lg border border-zinc-800 bg-zinc-950/60">
			<div class="flex flex-wrap items-center gap-3 border-b border-zinc-800 px-3 py-2 text-xs">
				<span
					data-testid="task-log-state-{taskId}"
					class={stateLabel === 'LIVE'
						? 'text-emerald-300'
						: stateLabel === 'ENDED'
							? 'text-zinc-400'
							: stateLabel === 'FAILED'
								? 'text-rose-300'
								: 'text-amber-300'}>{stateLabel}</span
				>
				<span data-testid="task-log-follow-{taskId}" class="text-zinc-500">
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
					<label class="flex items-center gap-1.5 text-zinc-500">
						Attempt:
						<select
							data-testid="task-log-attempt-select-{taskId}"
							value={String(pinned ?? 0)}
							onchange={(e) => selectAttempt(e.currentTarget.value)}
							class="rounded-md border border-zinc-700 bg-zinc-900 px-1.5 py-0.5 text-zinc-200"
						>
							<option value="0">latest</option>
							{#each Array.from({ length: maxAttempt }, (_, i) => i + 1) as n (n)}
								<option value={String(n)}>{n}</option>
							{/each}
						</select>
					</label>
				{/if}
				{#if shownAttempt > 1 || pinned !== null}
					<span
						data-testid="task-log-attempt-{taskId}"
						class="rounded-full bg-amber-500/15 px-2 py-0.5 text-amber-300 ring-1 ring-amber-400/40"
						>attempt {shownAttempt || pinned}</span
					>
				{/if}
				{#if hasStderr}
					<span data-testid="task-log-stderr-legend-{taskId}" class="text-rose-300/90">
						stderr shown in red
					</span>
				{/if}
				{#if !following}
					<button
						type="button"
						data-testid="task-log-jump-{taskId}"
						onclick={jumpToLatest}
						class="rounded-md border border-emerald-500/40 px-2 py-0.5 text-emerald-300 hover:bg-emerald-500/10"
					>
						Jump to latest
					</button>
				{/if}
				<span class="ml-auto text-zinc-600" data-testid="task-log-count-{taskId}">
					{lines.length} lines
				</span>
			</div>

			{#if dropped > 0}
				<p data-testid="task-log-dropped-{taskId}" class="border-b border-zinc-800 px-3 py-1.5 text-xs text-amber-300">
					{dropped} earlier lines are no longer shown here (scrollback is {scrollback} lines).
					The whole output is in the artifact store: <code class="text-amber-200"
						>orkctl run logs {runId} --task {taskId}</code
					>
				</p>
			{/if}

			{#if retried}
				<p
					data-testid="task-log-retried-{taskId}"
					class="border-b border-zinc-800 px-3 py-1.5 text-xs text-amber-300"
				>
					This task retried, and this pane is now following attempt {shownAttempt}. The
					earlier attempt's output is still in the artifact store:
					<code class="text-amber-200"
						>orkctl run logs {runId} --task {taskId} --attempt {shownAttempt - 1}</code
					>
				</p>
			{/if}

			{#if socketState === 'failed'}
				<div class="border-b border-zinc-800 px-3 py-2">
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
				class="max-h-80 overflow-y-auto px-3 py-2 font-mono text-xs leading-5"
			>
				{#if !started}
					<p data-testid="task-log-waiting-{taskId}" class="text-zinc-500">
						This task has not been dispatched yet. Its output appears here when it starts.
					</p>
				{:else if lines.length === 0}
					<p data-testid="task-log-empty-{taskId}" class="text-zinc-500">
						{#if ended}
							This task wrote nothing.
						{:else if socketState === 'failed'}
							Nothing was received. The stream stopped for the reason above.
						{:else}
							Waiting for output…
						{/if}
					</p>
				{:else}
					{#each lines as line (line.n)}
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
								? 'text-amber-300'
								: line.stream === 'stderr'
									? 'text-rose-300/90'
									: 'text-zinc-300'}"
						>
							{line.text}
						</div>
					{/each}
				{/if}
			</div>

			{#if truncated}
				<p data-testid="task-log-truncated-{taskId}" class="border-t border-zinc-800 px-3 py-1.5 text-xs text-amber-300">
					This task's output passed its cap and was truncated by the Orker. Nothing beyond the
					notice above was stored.
				</p>
			{/if}
		</div>
	{/if}
</div>
