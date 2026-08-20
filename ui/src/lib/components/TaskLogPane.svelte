<script lang="ts">
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

	// dispatched latches `started` so that the subscription below depends on a
	// value that changes ONCE, from false to true.
	//
	// This is not a micro-optimisation, it is a correctness fix, and it was
	// measured: subscribing on `started` re-subscribed when the task moved
	// RUNNING -> SUCCEEDED. A new subscription replays the attempt from its
	// first chunk, so the pane received line 1 again on top of the fifty-five
	// it already had, two entries claimed the same key, and Svelte threw
	// `each_key_duplicate` — which stops the component updating at all. The
	// symptom was a pane frozen on LIVE over a finished task, and the cause
	// was a subscription that had no business being replaced.
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

	$effect(() => {
		if (!open || !dispatched) return;
		// Read the coordinates synchronously so a changed run, task or attempt
		// tears this subscription down and opens the right one.
		const target = { runId, taskId, namespace, attempt };
		// A subscription replays this attempt from its first chunk, so the pane
		// it feeds starts empty. Keeping what a previous subscription left
		// would double every line, and the numbering it is keyed by would
		// repeat.
		lines = [];
		dropped = 0;
		truncated = false;
		ended = false;
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
