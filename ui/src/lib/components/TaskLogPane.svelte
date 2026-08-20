<script lang="ts">
	import { untrack } from 'svelte';
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

	let { runId, taskId, namespace, status, attempt = 0, scrollback = 2000 }: Props = $props();

	let open = $state(false);
	// $state.raw: this array is replaced wholesale on every batch and can hold
	// thousands of entries. A deeply reactive proxy per line is the difference
	// between a chatty task rendering and a chatty task melting the tab.
	let lines = $state.raw<LogLine[]>([]);
	let dropped = $state(0);
	let truncated = $state(false);
	let ended = $state(false);
	let socketState = $state<LogSocketState>('connecting');
	let following = $state(true);
	let box: HTMLDivElement | undefined = $state(undefined);

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
	const coordinates = $derived([runId, taskId, namespace, attempt].join('\u0000'));

	$effect(() => {
		if (!open || !dispatched) return;
		coordinates; // the ONE reactive dependency of this subscription
		// Untracked on purpose: reading a prop here would put the each-item
		// back in the dependency set and undo the paragraph above.
		const target = untrack(() => ({ runId, taskId, namespace, attempt }));
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
		return followTaskLogs({
			...target,
			onState: (s) => (socketState = s),
			onLines: (delivery) => {
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
							: 'text-amber-300'}>{stateLabel}</span
				>
				<span data-testid="task-log-follow-{taskId}" class="text-zinc-500">
					{following ? 'FOLLOWING' : 'PAUSED'}
				</span>
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
						{ended ? 'This task wrote nothing.' : 'Waiting for output…'}
					</p>
				{:else}
					{#each lines as line (line.n)}
						<div
							data-testid="task-log-line"
							data-line={line.n}
							class="whitespace-pre-wrap break-all {line.notice
								? 'text-amber-300'
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
