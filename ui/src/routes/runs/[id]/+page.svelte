<script lang="ts">
	import { onDestroy, untrack } from 'svelte';
	import { goto, invalidateAll, replaceState } from '$app/navigation';
	import { page } from '$app/state';
	import CliEquivalent from '$lib/components/CliEquivalent.svelte';
	import Dialog from '$lib/components/Dialog.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import EventInspector from '$lib/components/EventInspector.svelte';
	import RunSubmitPanel from '$lib/components/RunSubmitPanel.svelte';
	import StatusPill from '$lib/components/StatusPill.svelte';
	import TaskLogPane from '$lib/components/TaskLogPane.svelte';
	import FailurePanel from '$lib/components/FailurePanel.svelte';
	import ConfirmDialog from '$lib/components/ConfirmDialog.svelte';
	import { errorMessage, orkstack } from '$lib/client';
	import { artifactMetadata, downloadFilename, formatDuration, payloadJson, runCommand, timestampJson } from '$lib/runDetail';
	import { EVENT_BUFFER_CAP, appendEvent, emptyEventBuffer } from '$lib/eventBuffer';
	import { Status, type ArtifactRef, type Payload } from '$lib/gen/orkstack/v1/common_pb';
	import { EventType, type Event, type TaskRun } from '$lib/gen/orkstack/v1/run_pb';
	import { watchRunEvents, type RunSocketState } from '$lib/runEvents';
	import { NAMESPACE_PARAM, namespaceLabel, withNamespace } from '$lib/namespace';
	import { fmtTimestamp, isTerminal, statusLabel, taskDispatched } from '$lib/status';
	import { activityLabel, hasError, pinnedOrReference, shortDigest } from '$lib/activity';
	import type { Error as OrkError } from '$lib/gen/orkstack/v1/common_pb';
	import type { CommitRef } from '$lib/gen/orkstack/v1/activity_pb';
	import { ArtifactKind, type RunArtifact } from '$lib/gen/orkstack/v1/service_pb';
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
		output?: Payload;
		artifacts: ArtifactRef[];
		valuesRef?: ArtifactRef;
		systemArtifacts: ArtifactRef[];
		startedAt?: TaskRun['startedAt'];
		finishedAt?: TaskRun['finishedAt'];
		pool: string;
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
	const artifactsByTask = $derived.by(() => {
		const grouped = new Map<string, RunArtifact[]>();
		for (const record of data.artifacts) {
			const records = grouped.get(record.taskId) ?? [];
			records.push(record);
			grouped.set(record.taskId, records);
		}
		return grouped;
	});

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
	let cancelDialog = $state(false);
	// "Run again" is a DIALOG, not a route. The draft it opens is seeded from
	// the run this page already loaded — its pinned version and its exact
	// input — so there is no second GetRun and, deliberately, no URL that
	// carries an input payload for someone to paste into a chat window.
	let runAgainDialogOpen = $state(false);
	let cancelReason = $state('');
	let cancelBusy = $state(false);
	let cancelError = $state('');
	let cancellationRequested = $state(false);
	// Which task cards have their Inspect open, keyed by task id. It is the
	// same shape as a log pane's own `open`: component-lifetime state for a
	// disclosure inside one card, deliberately NOT in the URL. A per-card
	// toggle is not a view somebody sends to somebody else, and putting it in
	// the address would make a link's meaning depend on which cards the sender
	// happened to have expanded.
	let inspectOpenByTask = $state<Record<string, boolean>>({});
	let copied = $state('');
	let downloadingArtifact = $state('');
	let artifactDownloadError = $state('');
	let artifactDownloadAbort: AbortController | undefined;
	onDestroy(() => artifactDownloadAbort?.abort());
	let wallNow = $state(Date.now());
	$effect(() => {
		const timer = setInterval(() => (wallNow = Date.now()), 1000);
		return () => clearInterval(timer);
	});
	const currentTimestamp = $derived({
		seconds: BigInt(Math.floor(wallNow / 1000)),
		nanos: (wallNow % 1000) * 1_000_000
	});

	function seedTasks(): TaskView[] {
		const views = new Map<string, TaskView>();
		for (const task of data.orkflow?.tasks ?? []) {
			views.set(task.id, {
				id: task.id,
				activity: activityLabel(task),
				status: Status.PENDING,
				attempt: 0,
				message: '',
				artifacts: [],
				systemArtifacts: [],
				pool: ''
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
				commit: tr.commit,
				output: tr.output,
				artifacts: tr.artifacts,
				valuesRef: tr.valuesRef,
				systemArtifacts: tr.systemArtifacts,
				startedAt: tr.startedAt,
				finishedAt: tr.finishedAt,
				pool: tr.pool
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
				commit: tr.commit,
				output: tr.output,
				artifacts: tr.artifacts,
				valuesRef: tr.valuesRef,
				systemArtifacts: tr.systemArtifacts,
				startedAt: tr.startedAt,
				finishedAt: tr.finishedAt,
				pool: tr.pool
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
			artifacts: [],
			systemArtifacts: [],
			pool: '',
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
			// TASK_SCHEDULED and TASK_STARTED are the live "this task reached an
			// Orker" signal, and the page needs it as a TIMESTAMP, not only a
			// status: CANCELED alone cannot say whether a task ever ran, and
			// `startedAt` is what the snapshot uses to answer that. The engine
			// stamps TaskRun.started_at from the same workflow.Now it emits
			// TASK_SCHEDULED with, so the value here agrees with the one the
			// terminal re-read brings back. Written only when the task has none,
			// so a retry's later event cannot move a task's first start.
			case EventType.TASK_SCHEDULED:
			case EventType.TASK_STARTED: {
				const fallback =
					event.type === EventType.TASK_SCHEDULED ? Status.PENDING : Status.RUNNING;
				const known = tasks.find((t) => t.id === event.taskId);
				upsertTask(event.taskId, {
					status: status ?? fallback,
					...(known?.startedAt ? {} : { startedAt: event.timestamp })
				});
				break;
			}
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
				//
				// The status is applied too, but ONLY for the two the engine
				// actually resolves a task with here: markSkipped and
				// markCanceled are its only TASK_LOG emitters, and there is no
				// TASK_SKIPPED/TASK_CANCELED event type for them to use
				// instead. Gating on those two means a future informational
				// TASK_LOG cannot silently reset a task's status. Without this
				// a skipped or never-started task sat at PENDING until the
				// terminal snapshot re-read landed.
				{
					const patch: Partial<TaskView> = {};
					if (event.message) patch.message = event.message;
					if (status === Status.SKIPPED || status === Status.CANCELED) patch.status = status;
					if (Object.keys(patch).length > 0) upsertTask(event.taskId, patch);
				}
				break;
		}
	}

	// ONE SOCKET, TWO CONSUMERS, AND ONLY THE RENDERING IS ON DEMAND.
	//
	// This is the page's single subscription to the run's event stream, and
	// its callback feeds two things that are NOT the same thing:
	//
	//   1. the reducer above (`onEvent`'s switch) — the run's status and every
	//      task's status and message. It runs on every event, always, whatever
	//      the page is showing.
	//   2. the capped event buffer — what the event list renders when it is
	//      open. It is filled always too, so opening the list later shows the
	//      backlog rather than only what arrived after the click.
	//
	// Only the LIST RENDERING is conditional (`?events=1`, below). Closing the
	// event list removes rows from the DOM and nothing else: the socket is not
	// touched, so task cards keep updating with it closed, and reopening it
	// needs no second replay path. Subscribing only while it was open was
	// rejected for exactly that — the reducer needs these events anyway.
	//
	// The effect's dependencies are two DERIVED PRIMITIVES rather than `data`
	// or `run`: a $derived propagates only when its value changes, so a
	// re-render or a loader that hands back an equal run cannot tear this
	// socket down. A teardown here is not free — the stream replays from
	// sequence 0, and a replay into a buffer that already holds those events
	// would duplicate every row the event list renders.
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

	// The event list is URL state — `?events=1`, and nothing else — independent
	// of the live subscription above. There is no containing panel to open
	// first and no second parameter gating it: the header's link IS the list's
	// state, so it can be sent to someone else and it survives a reload. What
	// each task card discloses about ITSELF is deliberately not here; that is
	// `inspectOpenByTask`, component state, and a link carries none of it.
	const eventsOpen = $derived(page.url.searchParams.get('events') === '1');
	function eventsHref(open: boolean): string {
		const params = new URLSearchParams(page.url.searchParams);
		params.set(NAMESPACE_PARAM, namespaceLabel(data.namespace));
		params.delete('events');
		if (open) params.set('events', '1');
		return `/runs/${encodeURIComponent(data.runId)}?${params.toString()}`;
	}

	const deepTask = $derived(page.url.searchParams.get('task') ?? '');
	const deepAttempt = $derived(Number(page.url.searchParams.get('attempt') ?? '0'));
	// WHAT A CARD SAYS BEFORE ITS TASK HAS RUN.
	//
	// A task waiting on its dependencies is not "PENDING" in any sense a
	// reader can act on — nothing is queued, no Orker has seen it — and a task
	// the run resolved without ever dispatching carries STATUS_CANCELED, which
	// on its own reads as "this was running and got killed". Neither deserves
	// a status pill. So a card that has not been dispatched draws a muted
	// caption in the pill's place saying what is actually true: which of its
	// needs has not finished yet, or that the run ended before it ever
	// started. `taskDispatched` is the test, and it leans on the engine
	// stamping started_at ONLY at real dispatch.
	function notStartedCaption(task: TaskView): string {
		if (task.status === Status.CANCELED || isTerminal(runStatus)) return 'Never started';
		const blocking = (taskDefs.get(task.id)?.needs ?? []).filter(
			(need) => !isTerminal(tasks.find((t) => t.id === need)?.status)
		);
		return blocking.length > 0 ? `Waiting for ${blocking.join(', ')}` : 'Waiting to start';
	}

	function taskHref(taskId: string, attempt = 0): string {
		const params = new URLSearchParams(page.url.searchParams);
		params.set(NAMESPACE_PARAM, namespaceLabel(data.namespace));
		params.set('task', taskId);
		if (attempt > 0) params.set('attempt', String(attempt));
		else params.delete('attempt');
		return `/runs/${encodeURIComponent(data.runId)}?${params.toString()}`;
	}
	function setTaskAttempt(taskId: string, attempt: number | null): void {
		replaceState(taskHref(taskId, attempt ?? 0), page.state);
	}
	function copy(value: string, label: string): void {
		void navigator.clipboard?.writeText(value).then(
			() => (copied = label),
			() => (copied = 'Copy failed')
		);
	}
	function downloadJson(name: string, value: unknown): void {
		const blob = new Blob([JSON.stringify(value, null, 2)], { type: 'application/json' });
		const url = URL.createObjectURL(blob);
		const link = document.createElement('a');
		link.href = url;
		link.download = name;
		link.click();
		URL.revokeObjectURL(url);
	}
	function taskExport(task: TaskView) {
		return {
			id: task.id, status: statusLabel(task.status), attempt: task.attempt,
			pool: task.pool || undefined, startedAt: timestampJson(task.startedAt), finishedAt: timestampJson(task.finishedAt),
			output: task.output?.data, values: task.valuesRef ? artifactMetadata(task.valuesRef) : undefined,
			artifacts: task.artifacts.map(artifactMetadata),
			systemArtifacts: task.systemArtifacts.map(artifactMetadata), error: task.error
		};
	}
	function runExport() {
		return {
			id: run?.id, namespace: namespaceLabel(data.namespace),
			orkflow: { name: run?.orkflowName, version: run?.orkflowVersion, definitionDigest: run?.orkflowDefinitionDigest },
			status: statusLabel(runStatus), createdAt: timestampJson(run?.createdAt), startedAt: timestampJson(startedAt ?? run?.startedAt), finishedAt: timestampJson(finishedAt ?? run?.finishedAt),
			input: run?.input?.data, output: run?.output?.data, error: runError, tasks: tasks.map(taskExport)
		};
	}
	async function cancelRun(): Promise<void> {
		if (!run) return;
		cancelBusy = true;
		cancelError = '';
		try {
			await orkstack(fetch).runs.cancelRun({ runId: run.id, namespace: data.namespace, reason: cancelReason.trim() });
			cancellationRequested = true;
			cancelDialog = false;
			await invalidateAll();
		} catch (err) {
			cancelError = errorMessage(err);
		} finally {
			cancelBusy = false;
		}
	}
	function artifactKindLabel(kind: ArtifactKind): string {
		return ArtifactKind[kind] ?? 'UNSPECIFIED';
	}
	async function downloadArtifact(record: RunArtifact): Promise<void> {
		if (!run || !record.artifact) return;
		const key = `${record.taskId}:${record.kind}:${record.artifact.name}`;
		downloadingArtifact = key;
		artifactDownloadError = '';
		artifactDownloadAbort?.abort();
		const abort = new AbortController();
		artifactDownloadAbort = abort;
		try {
			const parts: Uint8Array[] = [];
			let receivedName = record.artifact.name;
			let received = 0n;
			let complete = false;
			for await (const chunk of orkstack(fetch).runs.readArtifact({
				runId: run.id, namespace: data.namespace, taskId: record.taskId, kind: record.kind, name: record.artifact.name
			}, { signal: abort.signal })) {
				if (chunk.artifact?.name) receivedName = chunk.artifact.name;
				if (chunk.data.length) {
					parts.push(chunk.data.slice());
					received += BigInt(chunk.data.length);
				}
				if (chunk.last) complete = true;
			}
			if (!complete) throw new Error('Artifact stream ended before the engine confirmed a complete artifact.');
			if (received !== record.artifact.sizeBytes) throw new Error(`Artifact size changed during download (expected ${record.artifact.sizeBytes} bytes, received ${received}).`);
			const blob = new Blob(parts.map((part) => part.slice().buffer as ArrayBuffer), {
				type: record.artifact.contentType || 'application/octet-stream'
			});
			if (record.artifact.sha256 && globalThis.crypto?.subtle) {
				const digest = await crypto.subtle.digest('SHA-256', await blob.arrayBuffer());
				const actual = Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, '0')).join('');
				if (actual !== record.artifact.sha256.toLowerCase()) throw new Error('Artifact checksum did not match the recorded SHA-256.');
			}
			const url = URL.createObjectURL(blob);
			const link = document.createElement('a');
			link.href = url;
			link.download = downloadFilename(receivedName);
			link.click();
			URL.revokeObjectURL(url);
		} catch (err) {
			artifactDownloadError = errorMessage(err);
		} finally {
			if (artifactDownloadAbort === abort) artifactDownloadAbort = undefined;
			downloadingArtifact = '';
		}
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
			<h1 class="font-mono text-xl font-semibold break-all text-fg" data-testid="run-id">{run.id}</h1>
			<p class="text-sm break-all text-fg-muted">
				<a
					href={withNamespace(`/orkflows/${encodeURIComponent(run.orkflowName)}?version=${encodeURIComponent(run.orkflowVersion)}`, data.namespace)}
					data-testid="pinned-orkflow-link"
					class="font-medium text-brand-700 hover:underline">{run.orkflowName}</a
				>@{run.orkflowVersion}
			</p>
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
		<div class="ml-auto flex flex-wrap items-center justify-end gap-2">
			<button
				type="button"
				data-testid="run-again"
				onclick={() => (runAgainDialogOpen = true)}
				class="inline-flex h-9 items-center rounded-md border border-border bg-surface px-3 text-sm font-medium text-fg shadow-xs hover:bg-surface-hover"
			>Run again</button>
			<CliEquivalent
				command={runCommand(data.namespace, ['run', 'get', run.id])}
				testid="run-inspect-cli"
				label="Copy CLI"
			/>
			<button
				type="button"
				data-testid="export-run-json"
				onclick={() => downloadJson(`${run.id}.json`, runExport())}
				class="inline-flex h-9 items-center rounded-md border border-border bg-surface px-3 text-sm font-medium text-fg shadow-xs hover:bg-surface-hover"
			>Export JSON</button>
			{#if !isTerminal(runStatus)}
				<button
					type="button"
					data-testid="cancel-run"
					disabled={cancellationRequested}
					onclick={() => { cancelDialog = true; cancelError = ''; }}
					class="inline-flex h-9 items-center rounded-md bg-danger-600 px-3 text-sm font-medium text-white shadow-xs hover:bg-danger-700 disabled:opacity-50"
				>{cancellationRequested ? 'Cancellation requested' : 'Cancel run'}</button>
			{/if}
			<a
				href={eventsHref(!eventsOpen)}
				data-testid="events-toggle"
				data-sveltekit-noscroll
				aria-expanded={eventsOpen}
				aria-controls="run-events"
				class="inline-flex h-9 items-center rounded-md border border-border bg-surface px-3 text-sm font-medium text-fg shadow-xs hover:bg-surface-hover focus-visible:outline-2 focus-visible:outline-brand-700"
			>{eventsOpen ? 'Hide events' : 'Show events'}</a>
		</div>
	</div>
	{#if copied}
		<p data-testid="copy-status" class="-mt-4 mb-4 text-xs text-fg-muted">{copied}</p>
	{/if}
	{#if cancellationRequested && !isTerminal(runStatus)}
		<p data-testid="cancellation-requested" class="mb-4 rounded-md border border-warning-200 bg-warning-50 px-3 py-2 text-sm text-warning-700">
			Cancellation requested. Waiting for the run to stop.
		</p>
	{/if}

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

	<div class="mb-6 grid grid-cols-2 gap-4 text-sm sm:grid-cols-5">
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
			<div class="text-xs font-medium text-fg-muted">Elapsed</div>
			<div class="mt-1 text-fg-secondary" data-testid="run-elapsed">{formatDuration(startedAt ?? run.startedAt, finishedAt ?? run.finishedAt ?? (isTerminal(runStatus) ? undefined : currentTimestamp))}</div>
		</div>
		<div class="rounded-lg border border-border bg-surface p-3 shadow-xs">
			<div class="text-xs font-medium text-fg-muted">Tasks</div>
			<div class="mt-1 text-fg-secondary" data-testid="task-summary">
				{tasks.filter((t) => t.status === Status.SUCCEEDED).length}/{tasks.length} succeeded
			</div>
		</div>
	</div>

	<section class="mb-6 grid gap-4 lg:grid-cols-2" aria-label="Run values">
		<div class="min-w-0 rounded-lg border border-border bg-surface p-4 shadow-xs">
			<div class="flex items-center justify-between gap-3">
				<h2 class="text-sm font-semibold text-fg">Inputs</h2>
				{#if run.input}
					<button type="button" data-testid="copy-run-input" onclick={() => copy(payloadJson(run.input), 'Inputs copied')} class="text-xs font-medium text-brand-700 hover:underline">Copy JSON</button>
				{/if}
			</div>
			<pre data-testid="run-input" class="mt-2 max-h-56 overflow-auto rounded-md bg-surface-sunken p-3 font-mono text-xs text-fg-secondary">{run.input ? payloadJson(run.input) : 'No input values are recorded for this run.'}</pre>
		</div>
		<div class="min-w-0 rounded-lg border border-border bg-surface p-4 shadow-xs">
			<div class="flex items-center justify-between gap-3">
				<h2 class="text-sm font-semibold text-fg">Run output</h2>
				{#if run.output}
					<button type="button" data-testid="copy-run-output" onclick={() => copy(payloadJson(run.output), 'Run output copied')} class="text-xs font-medium text-brand-700 hover:underline">Copy JSON</button>
				{/if}
			</div>
			<pre data-testid="run-output" class="mt-2 max-h-56 overflow-auto rounded-md bg-surface-sunken p-3 font-mono text-xs text-fg-secondary">{run.output ? payloadJson(run.output) : isTerminal(runStatus) ? 'This run finished without aggregated output values.' : 'Run output is not available yet.'}</pre>
		</div>
	</section>

	<div class="space-y-6">
		<!--
			The run's history reads BEFORE the tasks it produced. It is the
			run-level account of what happened, so it belongs beside the run's
			own header rather than under a task list a reader has to scroll
			past — and it is what subsumed the old status trail: every row
			carries a sequence number and a timestamp the trail never had.
		-->
		{#if eventsOpen}
			<section id="run-events" aria-label="Run events" data-testid="run-events">
				<EventInspector
					events={buffer.events}
					dropped={buffer.dropped}
					total={buffer.total}
					{streamBroken}
					runId={run.id}
					cap={EVENT_BUFFER_CAP}
				/>
			</section>
		{/if}
		<section>
			<h2 class="mb-3 text-sm font-semibold text-fg">Tasks</h2>
			{#if data.artifactsError}
				<p data-testid="artifacts-unavailable" class="mb-3 rounded-md border border-warning-200 bg-warning-50 px-3 py-2 text-xs text-warning-700">
					Artifact metadata is unavailable: {data.artifactsError}. Downloads require the engine's recorded-artifact read API and are never linked directly to storage URIs.
				</p>
			{/if}
			{#if artifactDownloadError}
				<p data-testid="artifact-download-error" class="mb-3 rounded-md border border-danger-200 bg-danger-50 px-3 py-2 text-xs text-danger-700">Artifact download failed: {artifactDownloadError}</p>
			{/if}
			<div class="space-y-2" data-testid="task-list">
				{#each tasks as task (task.id)}
					{@const dispatched = taskDispatched(task.status, task.startedAt)}
					<div
						class="rounded-lg border bg-surface px-4 py-3 shadow-xs {dispatched
							? 'border-border'
							: 'border-dashed border-border-subtle opacity-60'}"
						data-testid="task-{task.id}"
						data-dispatched={dispatched}
					>
						<div class="flex items-center justify-between gap-4">
							<div class="min-w-0">
								<a href={taskHref(task.id, task.attempt)} class="font-medium break-all text-brand-700 hover:underline" data-testid="task-deep-link-{task.id}">{task.id}</a>
								{#if task.message && !task.error}
									<p class="text-xs break-words text-fg-secondary">{task.message}</p>
	{/if}
							</div>
							<!--
								SKIPPED keeps its pill: a skip is a DAG decision the reader
								should see, not the absence of one. Everything else that was
								never dispatched gets the caption instead of a pill — and
								keeps the same data-testid, because "what does this task say"
								is one question whichever of the two answers it.
							-->
							{#if dispatched || task.status === Status.SKIPPED}
								<StatusPill status={task.status} testid="task-{task.id}-status" />
							{:else}
								<span
									data-testid="task-{task.id}-status"
									data-state="not-started"
									class="text-[11px] whitespace-nowrap text-fg-muted"
								>{notStartedCaption(task)}</span>
							{/if}
						</div>
						<div class="mt-2 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-fg-muted">
							{#if dispatched}
								<span data-testid="task-{task.id}-elapsed">Elapsed {formatDuration(task.startedAt, task.finishedAt ?? (task.status === Status.RUNNING ? currentTimestamp : undefined))}</span>
							{/if}
							{#if task.pool}<span>Pool {task.pool}</span>{/if}
							<CliEquivalent
								command={runCommand(data.namespace, ['run', 'logs', run.id, '--task', task.id, ...(task.attempt > 0 ? ['--attempt', String(task.attempt)] : [])])}
								testid="run-task-{task.id}-logs-cli"
								label="Copy logs CLI"
							/>
						</div>

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
							startedAt={task.startedAt}
							attempt={deepTask === task.id && Number.isFinite(deepAttempt) ? deepAttempt : 0}
							deepLinked={deepTask === task.id}
							onAttemptChange={(attempt) => setTaskAttempt(task.id, attempt)}
						/>
						<div class="mt-2">
							<button
								type="button"
								data-testid="task-inspect-toggle-{task.id}"
								aria-expanded={inspectOpenByTask[task.id] ?? false}
								onclick={() => (inspectOpenByTask[task.id] = !inspectOpenByTask[task.id])}
								class="inline-flex h-8 items-center justify-center gap-1.5 rounded-md border border-border bg-surface px-3 text-xs font-medium text-fg shadow-xs transition-colors duration-150 hover:border-border-strong hover:bg-surface-hover focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500/40 focus-visible:ring-offset-2 focus-visible:ring-offset-canvas"
							>
								<span class="text-fg-muted">{inspectOpenByTask[task.id] ? '▾' : '▸'}</span>
								{inspectOpenByTask[task.id] ? 'Hide inspect' : 'Inspect'}
							</button>
						</div>
						{#if inspectOpenByTask[task.id]}
							{@const def = taskDefs.get(task.id)}
							{@const taskArtifacts = artifactsByTask.get(task.id) ?? []}
							<div
								class="mt-2 rounded-md border border-border-subtle bg-surface-sunken p-3"
								data-testid="task-{task.id}-inspect"
							>
								<p class="text-xs break-all text-fg-muted" data-testid="task-{task.id}-activity">{task.activity || 'activity not yet known'}</p>
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
												<dt class="text-fg-muted uppercase">Environment</dt>
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
								<!--
									Gated on their own presence, not on the definition
									block above: a task that produced artifacts and has
									no pinned definition still lists them.
								-->
								{#if taskArtifacts.length > 0}
									<div class="mt-3 border-t border-border-subtle pt-3" data-testid="task-artifacts-{task.id}">
										<h4 class="text-xs text-fg-muted uppercase">Artifacts</h4>
										{#each taskArtifacts as record (record.kind + ':' + (record.artifact?.name ?? ''))}
											{#if record.artifact}
												{@const key = `${record.taskId}:${record.kind}:${record.artifact.name}`}
												<div class="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1 rounded-md border border-border-subtle px-3 py-2 text-xs">
													<span class="font-medium text-fg">{artifactKindLabel(record.kind)}</span>
													<span class="font-mono break-all text-fg-secondary">{record.artifact.name}</span>
													<span class="text-fg-muted">{record.artifact.contentType || 'unknown type'} · {record.artifact.sizeBytes.toString()} bytes</span>
													<button type="button" data-testid="download-artifact-{task.id}-{record.artifact.name}" onclick={() => downloadArtifact(record)} disabled={downloadingArtifact === key} class="ml-auto font-medium text-brand-700 hover:underline disabled:opacity-50">{downloadingArtifact === key ? 'Downloading…' : 'Download'}</button>
													<details class="w-full min-w-0 text-fg-muted"><summary>Metadata</summary><pre class="mt-1 overflow-auto font-mono">{JSON.stringify(artifactMetadata(record.artifact), null, 2)}</pre></details>
												</div>
											{/if}
										{/each}
									</div>
								{/if}
							</div>
						{/if}
						{#if task.output || task.valuesRef}
							<div class="mt-3 rounded-md border border-border-subtle bg-surface-sunken p-3">
								<div class="flex items-center justify-between gap-3">
									<span class="text-xs font-medium text-fg">Task values</span>
									{#if task.output}<button type="button" data-testid="copy-task-values-{task.id}" onclick={() => copy(payloadJson(task.output), 'Task values copied')} class="text-xs font-medium text-brand-700 hover:underline">Copy JSON</button>{/if}
								</div>
								<pre data-testid="task-values-{task.id}" class="mt-2 max-h-40 overflow-auto font-mono text-xs text-fg-secondary">{task.output ? payloadJson(task.output) : isTerminal(task.status) ? 'This task finished without inline values. Its recorded values artifact, if present, is listed under Inspect.' : 'Task values are not available yet.'}</pre>
							</div>
						{/if}
					</div>
				{/each}
			</div>
		</section>
		{#if cancelDialog && run}
			<ConfirmDialog
				testid="cancel-run-confirm"
				title="Cancel run {run.id}?"
				acceptLabel="Request cancellation"
				acceptTestid="cancel-run-accept"
				cancelTestid="cancel-run-dismiss"
				busy={cancelBusy}
				error={cancelError}
				destructive
				onaccept={cancelRun}
				oncancel={() => { cancelDialog = false; cancelError = ''; }}
			>
				{#snippet body()}
					<p>The request is sent to the durable execution service. This page keeps the current status until that service records a transition.</p>
					<label class="mt-3 block text-sm font-medium text-fg" for="cancel-reason">Reason <span class="font-normal text-fg-muted">(optional)</span></label>
					<textarea id="cancel-reason" data-testid="cancel-reason" bind:value={cancelReason} rows="3" class="mt-1 w-full rounded-md border border-border bg-surface px-3 py-2 text-sm text-fg" placeholder="Why should this run stop?"></textarea>
					<!--
						Deliberately NOT a CliEquivalent popover. This is a live preview of
						the exact destructive command this dialog is about to run, and it
						stays visible while the reason field is typed into. Styled to match
						the popover's command block; not hidden behind its icon.
					-->
					<p class="mt-3 overflow-x-auto rounded bg-surface-sunken p-2 font-mono text-xs text-fg-muted">{runCommand(data.namespace, ['run', 'cancel', run.id, ...(cancelReason.trim() ? ['--reason', cancelReason.trim()] : [])])}</p>
				{/snippet}
			</ConfirmDialog>
		{/if}
	</div>

	{#if runAgainDialogOpen}
		<Dialog
			testid="run-again-dialog"
			title="Run again from {run.id}"
			widthClass="max-w-2xl"
			onclose={() => (runAgainDialogOpen = false)}
		>
			{#snippet body()}
				<RunSubmitPanel
					namespace={data.namespace}
					orkflowName={run.orkflowName}
					initialVersion={run.orkflowVersion}
					initialInput={run.input?.data ?? {}}
					replayFromRunId={run.id}
					onsubmitted={(newRunId) => goto(withNamespace(`/runs/${newRunId}`, data.namespace))}
				/>
			{/snippet}
		</Dialog>
	{/if}
{/if}
