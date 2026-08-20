<script lang="ts">
	import { untrack } from 'svelte';
	import { invalidateAll } from '$app/navigation';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import StatusPill from '$lib/components/StatusPill.svelte';
	import TaskLogPane from '$lib/components/TaskLogPane.svelte';
	import { Status } from '$lib/gen/orkstack/v1/common_pb';
	import { EventType, type Event } from '$lib/gen/orkstack/v1/run_pb';
	import { watchRunEvents, type RunSocketState } from '$lib/runEvents';
	import { withNamespace } from '$lib/namespace';
	import { eventTypeLabel, fmtTime, fmtTimestamp, isTerminal, statusLabel } from '$lib/status';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	// A run that could not be read is a failed request. The retry re-issues it;
	// nothing here waits forever on a spinner.
	let retrying = $state(false);
	async function retry() {
		retrying = true;
		try {
			await invalidateAll();
		} finally {
			retrying = false;
		}
	}

	interface TaskView {
		id: string;
		plugin: string;
		status: Status;
		attempt: number;
		message: string;
	}

	const run = $derived(data.run);

	// Seed from the GetRun snapshot, then let the WebSocket stream drive every
	// subsequent change in place (no page refresh). The seed is deliberately a
	// one-shot read of `data`, hence untrack.
	let runStatus = $state<Status>(untrack(() => data.run?.status) ?? Status.UNSPECIFIED);
	let runError = $state<string>(untrack(() => data.run?.error?.message) ?? '');
	let socketState = $state<RunSocketState>('connecting');
	let streamDelivered = $state(false);
	// The socket is only 'live' once it has actually delivered something.
	// 'error' or a reconnect loop that never delivers means the browser is
	// not receiving this run, whatever the run itself is doing.
	const streamBroken = $derived(!streamDelivered && (socketState === 'error' || socketState === 'closed'));
	let events = $state<Event[]>([]);
	let tasks = $state<TaskView[]>(untrack(seedTasks));
	let finishedAt = $state(untrack(() => data.run?.finishedAt));
	let startedAt = $state(untrack(() => data.run?.startedAt));
	// The run's status timeline, built purely from the streamed run-level
	// events. The bridge replays from sequence 0, so the trail always shows
	// every transition the page received — even when two events land in the
	// same frame and the pill only ever paints the newer one.
	let statusTrail = $state<Status[]>([]);

	const RUN_LEVEL = new Set<EventType>([
		EventType.RUN_CREATED,
		EventType.RUN_STARTED,
		EventType.RUN_SUCCEEDED,
		EventType.RUN_FAILED,
		EventType.RUN_CANCELED
	]);

	function seedTasks(): TaskView[] {
		const views = new Map<string, TaskView>();
		for (const task of data.orkflow?.tasks ?? []) {
			views.set(task.id, { id: task.id, plugin: task.plugin, status: Status.PENDING, attempt: 0, message: '' });
		}
		for (const tr of data.run?.taskRuns ?? []) {
			const existing = views.get(tr.taskId);
			views.set(tr.taskId, {
				id: tr.taskId,
				plugin: tr.plugin || existing?.plugin || '',
				status: tr.status,
				attempt: tr.attempt,
				message: tr.error?.message ?? ''
			});
		}
		return [...views.values()];
	}

	// Patch the view IN PLACE. Replacing `tasks[idx]` carries the same data,
	// but a keyed `{#each}` hands each item to its block as ONE value: replace
	// it and every reader of `task.id` is invalidated, including the props
	// that carry a log pane's subscription coordinates. Measured: a task
	// turning SUCCEEDED tore that pane's socket down and replayed the whole
	// attempt from its first chunk, emptying the pane under a reader who had
	// scrolled up. A field-by-field patch touches only the fields that moved.
	function upsertTask(taskId: string, patch: Partial<TaskView>) {
		const existing = tasks.find((t) => t.id === taskId);
		if (existing) {
			Object.assign(existing, patch);
			return;
		}
		tasks.push({ id: taskId, plugin: '', status: Status.PENDING, attempt: 0, message: '', ...patch });
	}

	function onEvent(event: Event) {
		events.push(event);
		const status = event.status === Status.UNSPECIFIED ? undefined : event.status;
		switch (event.type) {
			case EventType.RUN_CREATED:
				runStatus = status ?? Status.PENDING;
				break;
			case EventType.RUN_STARTED:
				runStatus = status ?? Status.RUNNING;
				startedAt = event.timestamp;
				break;
			case EventType.RUN_SUCCEEDED:
				runStatus = status ?? Status.SUCCEEDED;
				finishedAt = event.timestamp;
				break;
			case EventType.RUN_FAILED:
				runStatus = status ?? Status.FAILED;
				finishedAt = event.timestamp;
				if (event.message) runError = event.message;
				break;
			case EventType.RUN_CANCELED:
				runStatus = status ?? Status.CANCELED;
				finishedAt = event.timestamp;
				break;
			case EventType.TASK_SCHEDULED:
				upsertTask(event.taskId, { status: status ?? Status.PENDING });
				break;
			case EventType.TASK_STARTED:
				upsertTask(event.taskId, { status: status ?? Status.RUNNING });
				break;
			case EventType.TASK_SUCCEEDED:
				upsertTask(event.taskId, { status: status ?? Status.SUCCEEDED });
				break;
			case EventType.TASK_FAILED:
				upsertTask(event.taskId, {
					status: status ?? Status.FAILED,
					message: event.message
				});
				break;
			case EventType.TASK_LOG:
				// A run event, not the task's output. EVENT_TYPE_TASK_LOG carries
				// the engine's own notes about a task — a skip and why — and the
				// run's history is deliberately not where output lives. Bytes
				// travel from the Orker to the artifact store and reach this page
				// over the log socket instead (TaskLogPane), which is what keeps
				// a chatty task out of Temporal history for ever.
				if (event.message) upsertTask(event.taskId, { message: event.message });
				break;
		}
		if (RUN_LEVEL.has(event.type) && statusTrail[statusTrail.length - 1] !== runStatus) {
			statusTrail.push(runStatus);
		}
	}

	// A socket that never opens is the failure this page must not hide. It
	// looked identical to a quiet run: an amber CONNECTING badge and an empty
	// event log reading "Waiting for events…", while the run had in fact
	// finished. Track whether the stream ever delivered anything so the page
	// can say "not connected" instead of implying "nothing has happened".
	$effect(() => {
		if (!run) return;
		const stop = watchRunEvents(
			run.id,
			data.namespace,
			(event) => {
				streamDelivered = true;
				onEvent(event);
			},
			(s) => (socketState = s)
		);
		return stop;
	});

	let logEl: HTMLDivElement | undefined = $state(undefined);
	$effect(() => {
		events.length; // track
		if (logEl) logEl.scrollTop = logEl.scrollHeight;
	});
</script>

<svelte:head><title>Run {run?.id ?? ''} — OrkStack</title></svelte:head>

{#if data.error}
	<ErrorPanel
		testid="engine-error"
		retryTestid="run-retry"
		subject="run {data.runId}"
		message={data.error}
		busy={retrying}
		onretry={retry}
	/>
{:else if retrying}
	<p class="text-sm text-zinc-400" data-testid="run-loading">Loading the run…</p>
{:else if run}
	<div class="mb-6 flex flex-wrap items-center gap-4">
		<div>
			<h1 class="font-mono text-xl font-bold" data-testid="run-id">{run.id}</h1>
			<p class="text-sm text-zinc-400">
				<a href={withNamespace(`/orkflows/${run.orkflowName}`, data.namespace)} class="text-emerald-300 hover:underline"
					>{run.orkflowName}</a
				>@{run.orkflowVersion}
			</p>
			{#if statusTrail.length > 0}
				<p class="mt-1 font-mono text-xs text-zinc-500" data-testid="run-status-trail">
					{statusTrail.map(statusLabel).join(' -> ')}
				</p>
			{/if}
		</div>
		<div class="flex items-center gap-3">
			<StatusPill status={runStatus} testid="run-status" />
			<span
				data-testid="live-badge"
				class="inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-xs ring-1 {isTerminal(
					runStatus
				)
					? 'bg-zinc-700/40 text-zinc-400 ring-zinc-500/40'
					: socketState === 'open'
						? 'bg-emerald-500/15 text-emerald-300 ring-emerald-400/40'
						: 'bg-amber-500/15 text-amber-300 ring-amber-400/40'}"
			>
				<span
					class="h-1.5 w-1.5 rounded-full {isTerminal(runStatus)
						? 'bg-zinc-500'
						: socketState === 'open'
							? 'bg-emerald-400'
							: 'bg-amber-400'}"
				></span>
				{isTerminal(runStatus)
					? 'ENDED'
					: socketState === 'open'
						? 'LIVE'
						: streamBroken
							? 'NOT CONNECTED'
							: 'CONNECTING'}
			</span>
		</div>
	</div>

	{#if runError}
		<div
			data-testid="run-error"
			class="mb-6 rounded-md border border-rose-500/40 bg-rose-500/10 px-4 py-3 text-sm text-rose-300"
		>
			{runError}
		</div>
	{/if}

	<div class="mb-6 grid grid-cols-2 gap-4 text-sm sm:grid-cols-4">
		<div class="rounded-lg border border-zinc-800 p-3">
			<div class="text-xs uppercase text-zinc-500">Created</div>
			<div class="mt-1 text-zinc-200">{fmtTimestamp(run.createdAt)}</div>
		</div>
		<div class="rounded-lg border border-zinc-800 p-3">
			<div class="text-xs uppercase text-zinc-500">Started</div>
			<div class="mt-1 text-zinc-200">{fmtTimestamp(startedAt ?? run.startedAt)}</div>
		</div>
		<div class="rounded-lg border border-zinc-800 p-3">
			<div class="text-xs uppercase text-zinc-500">Finished</div>
			<div class="mt-1 text-zinc-200">{fmtTimestamp(finishedAt ?? run.finishedAt)}</div>
		</div>
		<div class="rounded-lg border border-zinc-800 p-3">
			<div class="text-xs uppercase text-zinc-500">Tasks</div>
			<div class="mt-1 text-zinc-200" data-testid="task-summary">
				{tasks.filter((t) => t.status === Status.SUCCEEDED).length}/{tasks.length} succeeded
			</div>
		</div>
	</div>

	<div class="space-y-6">
		<section>
			<h2 class="mb-3 text-lg font-semibold">Tasks</h2>
			<div class="space-y-2" data-testid="task-list">
				{#each tasks as task (task.id)}
					<div
						class="rounded-lg border border-zinc-800 bg-zinc-900/40 px-4 py-3"
						data-testid="task-{task.id}"
					>
						<div class="flex items-center justify-between gap-4">
							<div>
								<div class="font-medium text-zinc-100">{task.id}</div>
								<div class="text-xs text-zinc-500">
									plugin: {task.plugin}{#if task.message}
										· <span class="text-zinc-400">{task.message}</span>{/if}
								</div>
							</div>
							<StatusPill status={task.status} testid="task-{task.id}-status" />
						</div>
						<TaskLogPane
							runId={run.id}
							taskId={task.id}
							namespace={data.namespace}
							status={task.status}
							attempt={0}
						/>
					</div>
				{/each}
			</div>
		</section>

		<section>
			<h2 class="mb-3 text-lg font-semibold">Event log</h2>
			<div
				bind:this={logEl}
				data-testid="event-log"
				class="max-h-[28rem] overflow-y-auto rounded-lg border border-zinc-800 bg-zinc-950/60 p-3 font-mono text-xs"
			>
				{#if events.length === 0 && streamBroken}
					<p data-testid="event-log-offline" class="text-amber-300">
						Not receiving events. The engine refused or dropped this page's
						websocket, so this run cannot update live — reload to see its current
						state. If the engine serves the UI from another origin, it needs
						<code class="text-amber-200">ORK_WS_ALLOWED_ORIGINS</code> to include it.
					</p>
				{:else if events.length === 0}
					<p class="text-zinc-500">Waiting for events…</p>
				{:else}
					{#each events as event (event.sequence.toString())}
						<div class="flex gap-2 py-0.5" data-testid="event-entry">
							<span class="shrink-0 text-zinc-600">{fmtTime(event.timestamp)}</span>
							<span class="shrink-0 text-zinc-500">#{event.sequence.toString()}</span>
							<span
								class="shrink-0 {event.type === EventType.RUN_FAILED ||
								event.type === EventType.TASK_FAILED
									? 'text-rose-400'
									: event.type === EventType.RUN_SUCCEEDED ||
										  event.type === EventType.TASK_SUCCEEDED
										? 'text-emerald-400'
										: 'text-sky-300'}">{eventTypeLabel(event.type)}</span
							>
							{#if event.taskId}<span class="shrink-0 text-zinc-400">[{event.taskId}]</span>{/if}
							<span class="break-all text-zinc-300">{event.message}</span>
						</div>
					{/each}
				{/if}
			</div>
		</section>
	</div>
{/if}
