<script lang="ts">
	import { untrack } from 'svelte';
	import { invalidateAll } from '$app/navigation';
	import { page } from '$app/state';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import EventInspector from '$lib/components/EventInspector.svelte';
	import StatusPill from '$lib/components/StatusPill.svelte';
	import TaskLogPane from '$lib/components/TaskLogPane.svelte';
	import FailurePanel from '$lib/components/FailurePanel.svelte';
	import { EVENT_BUFFER_CAP, appendEvent, emptyEventBuffer } from '$lib/eventBuffer';
	import { Status } from '$lib/gen/orkstack/v1/common_pb';
	import { EventType, type Event, type TaskRun } from '$lib/gen/orkstack/v1/run_pb';
	import { watchRunEvents, type RunSocketState } from '$lib/runEvents';
	import { NAMESPACE_PARAM, namespaceLabel, withNamespace } from '$lib/namespace';
	import { fmtTimestamp, isTerminal, statusLabel } from '$lib/status';
	import { activityLabel, hasError, pinnedOrReference, shortDigest } from '$lib/activity';
	import type { Error as OrkError } from '$lib/gen/orkstack/v1/common_pb';
	import type { CommitRef } from '$lib/gen/orkstack/v1/activity_pb';
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
		// activity is what the task NAMES — its `uses:`, or the reference its
		// image resolved from.
		activity: string;
		status: Status;
		attempt: number;
		message: string;
		// error is the WHOLE terminal error, not a flattened string, so the
		// page can render its §8.1 category and §8.2 reason code rather than
		// only the sentence. Absent while a task has not failed.
		error?: OrkError;
		// exitCode has presence HERE even though proto3 int32 does not. A task
		// that has not completed has no measured exit code; 0 is a real result
		// only once a task succeeded. A failed task's non-zero value is likewise
		// measured. Keeping the absence out of the generated zero value prevents
		// the page from drawing "0" before the terminal snapshot arrives.
		exitCode?: number;
		commit?: CommitRef;
	}

	const run = $derived(data.run);

	// runError is the STRUCTURED terminal error from the GetRun snapshot — the
	// only source that carries a §8.1 category. It is $derived rather than
	// $state because a reload or a retry should re-read it; the socket's own
	// message lives separately in runErrorMessage.
	const runError = $derived(hasError(data.run?.error) ? data.run?.error : undefined);

	// taskDefs is the pinned DEFINITION for each task, keyed by id: what the
	// version this run pinned says the task runs. It is kept out of TaskView
	// on purpose — TaskView is patched field-by-field by the event reducer,
	// and the definition never changes for the life of a run, so folding it
	// in would churn a value that cannot move.
	const taskDefs = $derived(
		new Map((data.orkflow?.tasks ?? []).map((t) => [t.id, t]))
	);

	// Seed from the GetRun snapshot, then let the WebSocket stream drive live
	// changes. Terminal per-attempt facts are the exception: events carry only
	// status/message, so a terminal RUN event triggers one snapshot re-read
	// below. The seed is deliberately a one-shot read of `data`, hence untrack.
	let runStatus = $state<Status>(untrack(() => data.run?.status) ?? Status.UNSPECIFIED);
	// The streamed RUN_FAILED event carries a message and nothing else, so the
	// live path and the snapshot path are kept apart: `runErrorMessage` is what
	// the socket said, and `runError` below is the STRUCTURED error from the
	// snapshot, which is the only one of the two that carries a category.
	let runErrorMessage = $state<string>(untrack(() => data.run?.error?.message) ?? '');
	let socketState = $state<RunSocketState>('connecting');
	let streamDelivered = $state(false);
	// The socket is only 'live' once it has actually delivered something.
	// 'error' or a reconnect loop that never delivers means the browser is
	// not receiving this run, whatever the run itself is doing.
	const streamBroken = $derived(!streamDelivered && (socketState === 'error' || socketState === 'closed'));
	// The event stream, BOUNDED. The socket delivers every event whether or
	// not the inspector is open — the backlog has to be there when it opens —
	// so this is what a long run grows, and appendEvent caps it at
	// EVENT_BUFFER_CAP and counts what it drops. $state.raw because the buffer
	// is replaced wholesale and a deeply reactive proxy per event is what made
	// a chatty run's page heavy.
	let buffer = $state.raw(emptyEventBuffer());
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
			views.set(task.id, {
				id: task.id,
				activity: activityLabel(task),
				status: Status.PENDING,
				attempt: 0,
				message: ''
			});
		}
		for (const tr of data.run?.taskRuns ?? []) {
			const existing = views.get(tr.taskId);
			views.set(tr.taskId, {
				id: tr.taskId,
				activity: existing?.activity ?? '',
				status: tr.status,
				attempt: tr.attempt,
				message: tr.error?.message ?? '',
				error: hasError(tr.error) ? tr.error : undefined,
				exitCode: measuredExitCode(tr),
				commit: tr.commit
			});
		}
		return [...views.values()];
	}

	// Proto3's int32 has no field presence. Preserve the distinction the UI
	// actually knows: success necessarily measured exit 0, and any non-zero
	// value is measured; a zero on a pending/running/failed infrastructure
	// snapshot is not evidence that a container exited 0.
	function measuredExitCode(tr: TaskRun): number | undefined {
		if (tr.status === Status.SUCCEEDED || tr.exitCode !== 0) return tr.exitCode;
		return undefined;
	}

	function mergeTerminalFacts() {
		for (const tr of data.run?.taskRuns ?? []) {
			upsertTask(tr.taskId, {
				status: tr.status,
				attempt: tr.attempt,
				message: tr.error?.message ?? '',
				error: hasError(tr.error) ? tr.error : undefined,
				exitCode: measuredExitCode(tr),
				commit: tr.commit
			});
		}
	}

	let terminalRefresh: Promise<void> | undefined;
	function refreshTerminalFacts() {
		if (terminalRefresh) return;
		terminalRefresh = (async () => {
			// Events intentionally remain compact and carry no ActivityFailureDetail.
			// Re-read exactly once at the run's terminal transition instead of
			// inventing a polling loop or pretending the event message is structured.
			await invalidateAll();
			mergeTerminalFacts();
		})();
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
		tasks.push({
			id: taskId,
			activity: '',
			status: Status.PENDING,
			attempt: 0,
			message: '',
			...patch
		});
	}

	function onEvent(event: Event) {
		buffer = appendEvent(buffer, event);
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
				refreshTerminalFacts();
				break;
			case EventType.RUN_FAILED:
				runStatus = status ?? Status.FAILED;
				finishedAt = event.timestamp;
				if (event.message) runErrorMessage = event.message;
				refreshTerminalFacts();
				break;
			case EventType.RUN_CANCELED:
				runStatus = status ?? Status.CANCELED;
				finishedAt = event.timestamp;
				refreshTerminalFacts();
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

	// ONE SOCKET, TWO CONSUMERS, AND ONLY THE RENDERING IS ON DEMAND.
	//
	// This is the page's single subscription to the run's event stream, and
	// its callback feeds two things that are NOT the same thing:
	//
	//   1. the reducer above (`onEvent`'s switch) — the run's status, its
	//      status trail, and every task's status and message. It runs on every
	//      event, always, whatever the page is showing.
	//   2. the capped event buffer — what the inspector renders when it is
	//      open. It is filled always too, so opening the inspector later shows
	//      the backlog rather than only what arrived after the click.
	//
	// Only the LIST RENDERING is conditional (`?events=1`, below). Closing the
	// inspector removes rows from the DOM and nothing else: the socket is not
	// touched, so task cards keep updating with it closed, and reopening it
	// needs no second replay path. Subscribing only while it was open was
	// rejected for exactly that — the reducer needs these events anyway.
	//
	// The effect's dependencies are two DERIVED PRIMITIVES rather than `data`
	// or `run`: a $derived propagates only when its value changes, so a
	// re-render or a loader that hands back an equal run cannot tear this
	// socket down. A teardown here is not free — the stream replays from
	// sequence 0, and a replay into a buffer that already holds those events
	// would duplicate every row the inspector renders.
	//
	// A socket that never opens is the failure this page must not hide. It
	// looked identical to a quiet run: an amber CONNECTING badge and an empty
	// event log reading "Waiting for events…", while the run had in fact
	// finished. Track whether the stream ever delivered anything so the page
	// can say "not connected" instead of implying "nothing has happened".
	const socketRunId = $derived(data.run?.id ?? '');
	const socketNamespace = $derived(data.namespace);
	$effect(() => {
		const runId = socketRunId;
		const namespace = socketNamespace;
		if (!runId) return;
		const stop = watchRunEvents(
			runId,
			namespace,
			(event) => {
				streamDelivered = true;
				onEvent(event);
			},
			(s) => (socketState = s)
		);
		return stop;
	});

	// The inspector's open/closed state is a coordinate of the page, so it
	// lives in the URL beside the namespace — the doctrine this repo's other
	// pages already follow. A pasted link to "this run with the inspector
	// open" works, a reload keeps it, the back button closes it, and a test
	// can deep-link it. A local `let open = $state(false)` has none of those.
	//
	// The navigation is a plain link on the same route. The loader reads only
	// `?ns`, and SvelteKit re-runs a load only for the search params it
	// actually read, so toggling `?events` re-fetches nothing and rebuilds no
	// component: the socket above, the task panes and their own sockets all
	// survive the toggle.
	const EVENTS_PARAM = 'events';
	const eventsOpen = $derived(page.url.searchParams.get(EVENTS_PARAM) === '1');
	function eventsHref(open: boolean): string {
		const params = new URLSearchParams();
		params.set(NAMESPACE_PARAM, namespaceLabel(data.namespace));
		if (open) params.set(EVENTS_PARAM, '1');
		return `/runs/${encodeURIComponent(data.runId)}?${params.toString()}`;
	}
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
	<p class="text-sm text-fg-muted" data-testid="run-loading">Loading the run…</p>
{:else if run}
	<div class="mb-6 flex flex-wrap items-center gap-4">
		<div>
			<h1 class="font-mono text-xl font-semibold text-fg" data-testid="run-id">{run.id}</h1>
			<p class="text-sm text-fg-muted">
				<a
					href={withNamespace(`/orkflows/${run.orkflowName}`, data.namespace)}
					class="font-medium text-brand-700 hover:underline">{run.orkflowName}</a
				>@{run.orkflowVersion}
			</p>
			{#if statusTrail.length > 0}
				<p class="mt-1 font-mono text-xs text-fg-muted" data-testid="run-status-trail">
					{statusTrail.map(statusLabel).join(' -> ')}
				</p>
			{/if}
		</div>
		<div class="flex items-center gap-3">
			<StatusPill status={runStatus} testid="run-status" />
			<span
				data-testid="live-badge"
				class="inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium {isTerminal(
					runStatus
				)
					? 'border-border bg-surface-sunken text-fg-muted'
					: socketState === 'open'
						? 'border-succeeded-border bg-succeeded-bg text-succeeded-fg'
						: 'border-warning-200 bg-warning-50 text-warning-700'}"
			>
				<span
					class="h-1.5 w-1.5 rounded-full {isTerminal(runStatus)
						? 'bg-fg-muted'
						: socketState === 'open'
							? 'bg-succeeded-solid animate-pulse-dot motion-reduce:animate-none'
							: 'bg-warning-600'}"
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

	<!--
		The run's terminal error. The snapshot's Error carries the §8.1
		category, the §8.2 reason code and the retryable flag, so when it is
		present it is rendered in full rather than as a bare sentence. A run
		whose failure arrived only over the socket
		has a message and no category, and says exactly that much.
	-->
	{#if runError}
		<div class="mb-6"><FailurePanel error={runError} testid="run-error" /></div>
	{:else if runErrorMessage}
		<div
			data-testid="run-error"
			class="mb-6 rounded-md border border-danger-200 bg-danger-50 px-4 py-3 text-sm text-danger-700"
		>
			{runErrorMessage}
		</div>
	{/if}

	<div class="mb-6 grid grid-cols-2 gap-4 text-sm sm:grid-cols-4">
		<div class="rounded-lg border border-border bg-surface p-3 shadow-xs">
			<div class="text-xs font-medium text-fg-muted">Created</div>
			<div class="mt-1 text-fg-secondary">{fmtTimestamp(run.createdAt)}</div>
		</div>
		<div class="rounded-lg border border-border bg-surface p-3 shadow-xs">
			<div class="text-xs font-medium text-fg-muted">Started</div>
			<div class="mt-1 text-fg-secondary">{fmtTimestamp(startedAt ?? run.startedAt)}</div>
		</div>
		<div class="rounded-lg border border-border bg-surface p-3 shadow-xs">
			<div class="text-xs font-medium text-fg-muted">Finished</div>
			<div class="mt-1 text-fg-secondary">{fmtTimestamp(finishedAt ?? run.finishedAt)}</div>
		</div>
		<div class="rounded-lg border border-border bg-surface p-3 shadow-xs">
			<div class="text-xs font-medium text-fg-muted">Tasks</div>
			<div class="mt-1 text-fg-secondary" data-testid="task-summary">
				{tasks.filter((t) => t.status === Status.SUCCEEDED).length}/{tasks.length} succeeded
			</div>
		</div>
	</div>

	<div class="space-y-6">
		<section>
			<h2 class="mb-3 text-sm font-semibold text-fg">Tasks</h2>
			<div class="space-y-2" data-testid="task-list">
				{#each tasks as task (task.id)}
					{@const def = taskDefs.get(task.id)}
					<div
						class="rounded-lg border border-border bg-surface px-4 py-3 shadow-xs"
						data-testid="task-{task.id}"
					>
						<div class="flex items-center justify-between gap-4">
							<div class="min-w-0">
								<div class="font-medium text-fg">{task.id}</div>
								<div class="text-xs text-fg-muted">
									<span data-testid="task-{task.id}-activity"
										>{task.activity || 'activity not yet known'}</span
									>{#if task.message}
										· <span class="text-fg-secondary">{task.message}</span>{/if}
								</div>
							</div>
							<StatusPill status={task.status} testid="task-{task.id}-status" />
						</div>

						<!--
							WHAT RAN, AND WHAT IT PRODUCED.

							The definition half (image, command, env, secret NAMES)
							comes from the Orkflow this run pinned; the attempt half
							(exit code, commit) comes from the TaskRun. They are shown
							together because a reader debugging a task needs both, and
							the pin — the resolved index digest — is the point: it is
							what makes this run explainable after a tag moves.

							SECRET VALUES ARE NEVER HERE. A SecretRef carries a name
							and a SOURCE; the value is resolved on the Orker, into the
							container, and never travels to the engine or this page.
							Only names are rendered, and nothing renders `source`.
						-->
						{#if def || task.exitCode !== undefined || task.commit}
							<dl
								class="mt-3 grid grid-cols-1 gap-x-6 gap-y-2 border-t border-border-subtle pt-3 text-xs sm:grid-cols-2"
								data-testid="task-{task.id}-detail"
							>
								{#if def?.image}
									<div class="sm:col-span-2">
										<dt class="text-fg-muted uppercase">Image</dt>
										<dd
											class="mt-0.5 font-mono break-all text-fg-secondary"
											data-testid="task-{task.id}-image"
										>
											{pinnedOrReference(def.image) || '—'}
										</dd>
										{#if def.image.indexDigest}
											<dd class="mt-0.5">
												<span
													class="font-mono text-brand-700"
													title={def.image.indexDigest}
													data-digest={def.image.indexDigest}
													data-testid="task-{task.id}-image-digest"
													>{shortDigest(def.image.indexDigest)}</span
												>
											</dd>
										{/if}
									</div>
								{/if}
								{#if def && def.command.length > 0}
									<div class="sm:col-span-2">
										<dt class="text-fg-muted uppercase">Command</dt>
										<dd
											class="mt-0.5 font-mono break-all text-fg-secondary"
											data-testid="task-{task.id}-command"
										>
											{JSON.stringify(def.command)}
										</dd>
									</div>
								{/if}
								{#if def && Object.keys(def.env).length > 0}
									<div class="sm:col-span-2">
										<dt class="text-fg-muted uppercase">Env</dt>
										<!--
											Env is shown as NAME=VALUE. It is
											workload-supplied, non-secret configuration by
											construction: the platform puts nothing here
											(Task.env's own contract), and anything that
											must not be read travels as a `secrets:` entry,
											whose value is resolved on the Orker and never
											reaches the engine at all.
										-->
										<dd
											class="mt-0.5 font-mono break-all text-fg-secondary"
											data-testid="task-{task.id}-env"
										>
											{Object.entries(def.env)
												.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
												.map(([k, v]) => `${k}=${v}`)
												.join('  ')}
										</dd>
									</div>
								{/if}
								{#if def && def.secrets.length > 0}
									<div>
										<dt class="text-fg-muted uppercase">Secrets</dt>
										<dd
											class="mt-0.5 font-mono text-fg-secondary"
											title="names only — a secret's value is resolved on the Orker and never reaches this page"
											data-testid="task-{task.id}-secrets"
										>
											{def.secrets.map((s) => s.name).join(', ')}
										</dd>
									</div>
								{/if}
								<div>
									<dt class="text-fg-muted uppercase">Exit code</dt>
									<dd
										class="mt-0.5 font-mono {task.exitCode === undefined
											? 'text-fg-muted'
											: task.exitCode === 0
												? 'text-fg-secondary'
												: 'text-failed-fg'}"
										data-state={task.exitCode === undefined ? 'unknown' : 'measured'}
										data-testid="task-{task.id}-exit-code"
									>
										{task.exitCode === undefined ? 'Not measured' : task.exitCode}
									</dd>
								</div>
								{#if task.commit?.uri}
									<div class="sm:col-span-2">
										<dt class="text-fg-muted uppercase">Commit</dt>
										<dd
											class="mt-0.5 font-mono break-all text-fg-muted"
											data-testid="task-{task.id}-commit"
										>
											{task.commit.uri}
										</dd>
									</div>
								{/if}
							</dl>
						{/if}

						{#if task.error}
							<div class="mt-3">
								<FailurePanel error={task.error} testid="task-{task.id}-error" compact />
							</div>
						{/if}
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

		<!--
			THE EVENT STREAM IS AN INSPECTION VIEW, NOT THE PAGE.

			What a reader comes here for is the tasks and their output, so the
			page loads with no event list in the DOM at all. This link is the
			way in, and its `?events=1` is the whole of the state: see the
			socket effect above for why opening and closing it cannot cost a
			live update.
		-->
		<section>
			<div class="mb-3 flex flex-wrap items-center gap-3">
				<a
					href={eventsHref(!eventsOpen)}
					data-testid="events-toggle"
					data-sveltekit-noscroll
					aria-expanded={eventsOpen}
					class="inline-flex h-8 items-center gap-1.5 rounded-md border border-border bg-surface px-3 text-sm font-medium text-fg shadow-xs transition-colors duration-150 hover:border-border-strong hover:bg-surface-hover"
				>
					<span class="text-fg-muted">{eventsOpen ? '▾' : '▸'}</span>
					{eventsOpen ? 'Hide events' : 'Inspect events'}
				</a>
				{#if !eventsOpen}
					<span class="text-xs text-fg-muted" data-testid="events-teaser">
						{buffer.total}
						{buffer.total === 1 ? 'event' : 'events'} received on this page
					</span>
				{/if}
			</div>

			{#if eventsOpen}
				<EventInspector
					events={buffer.events}
					dropped={buffer.dropped}
					total={buffer.total}
					{streamBroken}
					runId={run.id}
					cap={EVENT_BUFFER_CAP}
				/>
			{/if}
		</section>
	</div>
{/if}
