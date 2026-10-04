<script lang="ts">
	import { onDestroy, untrack } from 'svelte';
	import { goto, invalidateAll, replaceState } from '$app/navigation';
	import { navigating, page } from '$app/state';
	import ArtifactProvenance from '$lib/components/ArtifactProvenance.svelte';
	import AwaitPanel from '$lib/components/AwaitPanel.svelte';
	import CliEquivalent from '$lib/components/CliEquivalent.svelte';
	import Dialog from '$lib/components/Dialog.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import EventInspector from '$lib/components/EventInspector.svelte';
	import RunEvidenceDialog from '$lib/components/RunEvidenceDialog.svelte';
	import RunSubmitPanel from '$lib/components/RunSubmitPanel.svelte';
	import StatusPill from '$lib/components/StatusPill.svelte';
	import TaskLogPane from '$lib/components/TaskLogPane.svelte';
	import EachItemGrid from '$lib/components/EachItemGrid.svelte';
	import {
		applyItemEvent,
		itemFromUrl,
		itemsFromTaskRun,
		mergeItems,
		retriedItemsLabel,
		type ItemView
	} from '$lib/each';
	import FailurePanel from '$lib/components/FailurePanel.svelte';
	import RetryBadge from '$lib/components/RetryBadge.svelte';
	import AttemptTimeline from '$lib/components/AttemptTimeline.svelte';
	import TimingBar from '$lib/components/TimingBar.svelte';
	import { formatQuantity, taskTiming } from '$lib/timing';
	import ConfirmDialog from '$lib/components/ConfirmDialog.svelte';
	import Select from '$lib/components/Select.svelte';
	import { defaultCompareTarget } from '$lib/runDiff';
	import { errorMessage, orkstack } from '$lib/client';
	import { artifactMetadata, downloadFilename, formatDuration, payloadJson, pinAccent, runCommand, taskRunCommand, timestampJson } from '$lib/runDetail';
	import { EVENT_BUFFER_CAP, appendEvent, emptyEventBuffer } from '$lib/eventBuffer';
	import { Status, type ArtifactRef, type Payload } from '$lib/gen/orkstack/v1/common_pb';
	import {
		EventType,
		TaskAwaitSchema,
		type Event,
		type RetryState,
		type Run,
		type TaskAttempt,
		type TaskAwait,
		type TaskEach,
		type TaskRun
	} from '$lib/gen/orkstack/v1/run_pb';
	import { create } from '@bufbuild/protobuf';
	import { attemptsKey, attemptsSeen, groupAttemptsByTask, runRetryCount } from '$lib/attempts';
	import { durationFromMs } from '@bufbuild/protobuf/wkt';
	import { FailureGroupBy, type FlakeGroup, type GetFlakeIndexResponse } from '$lib/gen/orkstack/v1/stats_pb';
	import { flakesCli } from '$lib/flakes';
	import { watchRunEvents, type RunSocketState } from '$lib/runEvents';
	import { NAMESPACE_PARAM, namespaceLabel, withNamespace } from '$lib/namespace';
	import {
		PRINCIPAL_CAPTION,
		UNRECORDED,
		UNRECORDED_CAPTION,
		originKindSlug,
		originLinkPath,
		originReferenceURL
	} from '$lib/origin';
	import { fmtTimestamp, isTerminal, statusLabel, taskDispatched } from '$lib/status';
	import { activityLabel, hasError, pinnedOrReference, shortDigest } from '$lib/activity';
	import type { Error as OrkError } from '$lib/gen/orkstack/v1/common_pb';
	import type { CommitRef } from '$lib/gen/orkstack/v1/activity_pb';
	import { ArtifactKind, type LiveAttempt, type RunArtifact } from '$lib/gen/orkstack/v1/service_pb';
	import { liveLine } from '$lib/live';
	import { afterNavigation } from '$lib/settle';
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
		// retryState is why the platform stopped trying, straight off the
		// snapshot (Temporal's own ActivityError.RetryState). It qualifies the
		// error above and is absent for everything that did not fail.
		retryState?: RetryState;
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
		// scheduledAt is the engine's dispatch instant (TASK_SCHEDULED's own
		// timestamp); startedAt is when the Orker began the attempt that
		// decided the task, and reaches the page only with a snapshot.
		scheduledAt?: TaskRun['scheduledAt'];
		startedAt?: TaskRun['startedAt'];
		finishedAt?: TaskRun['finishedAt'];
		pool: string;
		// pin is the group of tasks this one was held on ONE Orker with, as its
		// author named it, and is empty for an unpinned task. Unlike pool it is
		// seeded from the DEFINITION and then confirmed by the snapshot: a pin
		// is declared before anything runs, so the grouping can be drawn on a
		// pending task, whereas which Orker it landed on cannot.
		pin: string;
		// orkerId is the Orker that produced this task's outcome. It is set for
		// every task that reached one, pinned or not, so empty here means "never
		// reached an Orker" — not "not pinned". Read beside pin it is the proof a
		// pin held: one pin, one orker across its members.
		orkerId: string;
		// await is set for an awaited task — from the definition until the
		// snapshot's own (with its deadline and resolver) replaces it. Its
		// presence is what draws the await panel instead of a log pane: an
		// awaited task runs no container and writes no logs.
		await?: TaskAwait;
		// each is set for a fan-out (spec each-fan-out §10.1): whether its
		// array was counted, and into how many items. items are those items,
		// seeded from the snapshot and moved one cell at a time by item
		// events; a fan-out's own row has no attempt, log or artifact of its
		// own — every item has.
		each?: TaskEach;
		items: ItemView[];
	}

	const run = $derived(data.run);
	// What started this run, read off the run it is: nothing here infers it.
	const origin = $derived(run?.origin);
	const originSlug = $derived(origin ? originKindSlug(origin.kind) : '');
	const originHref = $derived.by(() => {
		const path = originLinkPath(origin, run?.orkflowName ?? '');
		return path ? withNamespace(path, data.namespace) : null;
	});
	const originReference = $derived(origin?.reference ? originReferenceURL(origin.reference) : null);

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
	// EVERY TRY OF EVERY TASK, KEPT PER TASK.
	//
	// The loader reads the whole run's timeline in one call, and from then on
	// it is REPLACED PER TASK by the refetch below, because that is the grain
	// the live triggers have: a task reached its terminal status, or its log
	// pane saw a higher attempt. $state.raw and a fresh Map per write — the
	// value is swapped wholesale, and a deep proxy over every attempt record of
	// a thirty-task run buys nothing.
	let attemptsByTask = $state.raw<Map<string, TaskAttempt[]>>(
		untrack(() => groupAttemptsByTask(data.attempts))
	);
	// What the timeline could not be read for, if anything. It starts as the
	// loader's tolerant answer and is updated by the refetches, so a store that
	// came back reachable stops apologising.
	let attemptsError = $state<string | null>(untrack(() => data.attemptsError));
	// The rows this page last seeded from, by reference. Every later loader
	// answer — a retry, a cancel, the terminal re-read — comes back through
	// `data`, and re-seeding on a CHANGED reference keeps the timeline fresh
	// without a second request and without discarding a live refetch on a
	// re-render that changed nothing.
	let seededAttempts = $state.raw<TaskAttempt[]>(untrack(() => data.attempts));
	$effect(() => {
		const rows = data.attempts;
		if (rows === untrack(() => seededAttempts)) return;
		seededAttempts = rows;
		attemptsByTask = groupAttemptsByTask(rows);
		attemptsError = data.attemptsError;
	});
	// Refetches in flight, so a task whose terminal event and whose log pane
	// speak in the same tick asks once. Deliberately a plain Set: it is
	// bookkeeping for the request, not something the page renders.
	const attemptsInFlight = new Set<string>();

	/**
	 * refreshAttempts re-reads ONE task's attempt records.
	 *
	 * Attempt records are written by the Orker, outside the run's event stream
	 * (event sequence numbers are the workflow's to assign, so an Orker cannot
	 * add to it). There is therefore nothing to subscribe to — but there is no
	 * need for a polling loop either: the page already learns the two moments a
	 * task's timeline can change, and asks then.
	 */
	async function refreshAttempts(taskId: string, item?: number): Promise<void> {
		// An item's timeline is its own (keyed convert[97]); a fan-out's
		// terminal event never lands here, because the whole node's records
		// are every item's — an item's load when its panel opens.
		const key = attemptsKey(taskId, item);
		if (!run || !taskId || attemptsInFlight.has(key)) return;
		attemptsInFlight.add(key);
		try {
			const res = await orkstack(fetch).runs.listTaskAttempts({
				runId: run.id,
				namespace: data.namespace,
				taskId,
				item
			});
			const next = new Map(attemptsByTask);
			next.set(key, res.attempts);
			attemptsByTask = next;
			attemptsError = null;
		} catch (err) {
			// Never fatal, exactly like the loader's read: the rest of this page
			// does not depend on knowing what a task's earlier tries did.
			attemptsError = errorMessage(err);
		} finally {
			attemptsInFlight.delete(key);
		}
	}

	/**
	 * attemptSeen is the log pane reporting the highest try IT has been served.
	 * That is the earliest live evidence in the browser that a task retried —
	 * the engine resolves the log stream onto the new attempt while the run is
	 * still going — but it is only worth a request when it names a try the
	 * timeline does not already hold.
	 */
	function attemptSeen(taskId: string, attempt: number, item?: number): void {
		if (attempt <= attemptsSeen(attemptsByTask.get(attemptsKey(taskId, item)) ?? [])) return;
		void refreshAttempts(taskId, item);
	}

	// The run header's chip: how many times anything in this run had to be
	// tried again. Zero draws nothing — a run that never retried should not
	// have to say so.
	const runRetries = $derived(runRetryCount([...attemptsByTask.values()].flat()));

	// RETRY HISTORY, on request. Whether THIS run retried a task is on its
	// badge already; whether the task retries in this orkflow's other runs is
	// a question about the archive (GetFlakeIndex), and it is asked only when
	// a reader asks it — the page's own loads stay what they were. The open
	// state is remembered per viewer, so a reader who wants history on every
	// run page gets it without clicking, and one who never asks never costs
	// the scan.
	const HISTORY_KEY = 'orkstack.run.stability';
	const HISTORY_WINDOW = '7 days';
	const HISTORY_LOOKBACK_MS = 7 * 24 * 60 * 60_000;
	let historyOpen = $state(false);
	let history = $state<GetFlakeIndexResponse | null>(null);
	let historyError = $state<string | null>(null);
	let historyLoading = $state(false);
	// Which run the answer in hand was asked for; a new run page asks again.
	let historyFor = '';
	let historyGeneration = 0;
	$effect(() => {
		try {
			historyOpen = globalThis.localStorage?.getItem(HISTORY_KEY) === 'open';
		} catch {
			// Storage is blocked; the default (closed) stands.
		}
	});
	const flakesByTask = $derived(
		new Map<string, FlakeGroup>((historyOpen && history ? history.groups : []).map((group) => [group.taskId, group]))
	);
	async function loadHistory(namespace: string, orkflowName: string) {
		const mine = ++historyGeneration;
		historyLoading = true;
		try {
			const resp = await orkstack(fetch).stats.getFlakeIndex({
				namespace,
				orkflowName,
				lookback: durationFromMs(HISTORY_LOOKBACK_MS),
				groupBy: [FailureGroupBy.TASK]
			});
			if (mine !== historyGeneration) return;
			history = resp;
			historyError = null;
		} catch (err) {
			if (mine !== historyGeneration) return;
			// Tolerant: the badges stay exactly as they were, and the reason is
			// said beside the button rather than swallowed.
			history = null;
			historyError = errorMessage(err);
		} finally {
			if (mine === historyGeneration) historyLoading = false;
		}
	}
	$effect(() => {
		const key = `${data.namespace}/${run?.id ?? ''}`;
		if (!historyOpen || !run?.orkflowName) return;
		if (historyFor === key) return;
		historyFor = key;
		history = null;
		historyError = null;
		void loadHistory(data.namespace, run.orkflowName);
	});
	function toggleHistory() {
		historyOpen = !historyOpen;
		if (!historyOpen) {
			historyFor = '';
			history = null;
			historyError = null;
		}
		try {
			globalThis.localStorage?.setItem(HISTORY_KEY, historyOpen ? 'open' : 'closed');
		} catch {
			// Storage is blocked; the state lives for this page only.
		}
	}
	const historyNote = $derived.by(() => {
		if (!history) return '';
		const runs = history.runsScanned;
		let note = `${runs} finished ${runs === 1n ? 'run' : 'runs'} of this orkflow in the last ${HISTORY_WINDOW}; attempt records complete for ${history.runsEvidenceComplete}, missing for ${history.runsEvidenceMissing}`;
		if (history.runsEvidenceMissing > 0n) note += ' (excluded from every rate, never counted as clean)';
		return note;
	});
	let finishedAt = $state(untrack(() => data.run?.finishedAt));
	let startedAt = $state(untrack(() => data.run?.startedAt));
	let cancelDialog = $state(false);
	// "Run again" is a DIALOG, not a route. The draft it opens is seeded from
	// the run this page already loaded — its pinned version and its exact
	// input — so there is no second GetRun and, deliberately, no URL that
	// carries an input payload for someone to paste into a chat window.
	let runAgainDialogOpen = $state(false);
	// "Compare" is a dialog too, and it reads nothing but ids and statuses: the
	// comparison page it opens carries two run ids in its URL and nothing else.
	// Candidates are fetched when the dialog OPENS, not with the page — most
	// visits never compare, and ListRuns is not free.
	let compareDialogOpen = $state(false);
	// "Evidence" is a dialog that exists only while open: the manifest it
	// shows is fetched per opening and kept nowhere (docs/RUN_EVIDENCE.md).
	let evidenceDialogOpen = $state(false);
	let compareCandidates = $state<Run[]>([]);
	let compareSelected = $state('');
	let comparePasted = $state('');
	let compareLoading = $state(false);
	let compareError = $state('');
	// A pasted id — any run of this namespace, of any orkflow — wins over the
	// list, so the dialog never stands between an operator and a run it lacks.
	const compareTarget = $derived(comparePasted.trim() || compareSelected);
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

	// WHAT EACH RUNNING TASK IS DOING RIGHT NOW. InspectRun reads the
	// heartbeat detail the cluster already holds for every pending attempt;
	// nothing is stored and there is no stream, so the page polls the unary
	// read while something runs and stops the moment nothing does, when the
	// run is terminal, and while the tab is hidden. Each answer replaces the
	// map whole. A failed poll keeps the last answer and is never fatal (the
	// refreshAttempts posture). Ages are measured against the engine's
	// computed_at plus the wall time since the answer arrived — never the
	// browser's clock alone.
	const LIVE_POLL_MS = 5000;
	let liveByTask = $state.raw(new Map<string, LiveAttempt>());
	let liveError = $state<string | null>(null);
	let liveComputedAtMs = $state(0);
	let liveReceivedAtMs = $state(0);
	let liveInFlight = false;
	const liveNowMs = $derived(liveComputedAtMs + (wallNow - liveReceivedAtMs));
	const liveNowTimestamp = $derived({
		seconds: BigInt(Math.floor(liveNowMs / 1000)),
		nanos: (liveNowMs % 1000) * 1_000_000
	});
	const anyTaskRunning = $derived(tasks.some((task) => task.status === Status.RUNNING));

	async function pollLive(): Promise<void> {
		if (!run || liveInFlight) return;
		liveInFlight = true;
		try {
			const res = await orkstack(fetch).runs.inspectRun({ runId: run.id, namespace: data.namespace });
			const next = new Map<string, LiveAttempt>();
			for (const attempt of res.attempts) next.set(attemptsKey(attempt.taskId, attempt.item), attempt);
			liveByTask = next;
			const computed = res.computedAt;
			liveReceivedAtMs = Date.now();
			liveComputedAtMs = computed
				? Number(computed.seconds) * 1000 + Math.floor(computed.nanos / 1e6)
				: liveReceivedAtMs;
			liveError = null;
		} catch (err) {
			liveError = errorMessage(err);
		} finally {
			liveInFlight = false;
		}
	}

	$effect(() => {
		if (!run || isTerminal(runStatus) || !anyTaskRunning) return;
		let timer: ReturnType<typeof setInterval> | undefined;
		const start = () => {
			if (timer !== undefined) return;
			void pollLive();
			timer = setInterval(() => void pollLive(), LIVE_POLL_MS);
		};
		const stop = () => {
			if (timer === undefined) return;
			clearInterval(timer);
			timer = undefined;
		};
		const onVisibility = () => (document.visibilityState === 'hidden' ? stop() : start());
		document.addEventListener('visibilitychange', onVisibility);
		if (document.visibilityState !== 'hidden') start();
		return () => {
			stop();
			document.removeEventListener('visibilitychange', onVisibility);
		};
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
				pool: '',
				// The pin is seeded from the DEFINITION, the way activity is: a
				// pin is declared, not discovered, so the grouping is legible from
				// page load rather than appearing only once tasks dispatch. The
				// snapshot below overwrites it with what the run recorded.
				pin: task.pin,
				orkerId: '',
				items: [],
				...(task.await ? { await: create(TaskAwaitSchema, { callback: task.await.callback }) } : {})
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
				retryState: tr.retryState,
				exitCode: measuredExitCode(tr),
				commit: tr.commit,
				output: tr.output,
				artifacts: tr.artifacts,
				valuesRef: tr.valuesRef,
				systemArtifacts: tr.systemArtifacts,
				scheduledAt: tr.scheduledAt,
				startedAt: tr.startedAt,
				finishedAt: tr.finishedAt,
				pool: tr.pool,
				// The snapshot is authoritative, but a run recorded before the
				// engine copied pin onto TaskRun still has a definition that
				// declares it — so keep the seeded value rather than dropping the
				// grouping. Same fall-through as activity above.
				pin: tr.pin || existing?.pin || '',
				orkerId: tr.orkerId,
				...(tr.await ? { await: tr.await } : existing?.await ? { await: existing.await } : {}),
				...(tr.each ? { each: tr.each } : {}),
				items: itemsFromTaskRun(tr)
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
				retryState: tr.retryState,
				exitCode: measuredExitCode(tr),
				commit: tr.commit,
				output: tr.output,
				artifacts: tr.artifacts,
				valuesRef: tr.valuesRef,
				systemArtifacts: tr.systemArtifacts,
				scheduledAt: tr.scheduledAt,
				startedAt: tr.startedAt,
				finishedAt: tr.finishedAt,
				pool: tr.pool,
				orkerId: tr.orkerId,
				// The await's own state — deadline, resolver — copied like pool
				// and orkerId; absent for a container task.
				...(tr.await ? { await: tr.await } : {}),
				// Patched only when the snapshot carries one: upsertTask assigns
				// every key present in the patch, so an empty pin here would
				// erase the pin the definition already seeded.
				...(tr.pin ? { pin: tr.pin } : {}),
				// A fan-out's expansion and its items, never moving an item an
				// event already settled back to unsettled.
				...(tr.each
					? {
							each: tr.each,
							items: mergeItems(tasks.find((t) => t.id === tr.taskId)?.items ?? [], itemsFromTaskRun(tr))
						}
					: {})
			});
		}
	}

	// Whether the page holds the run's TERMINAL snapshot: true when it loaded
	// an already-finished run, and once the terminal re-read below has been
	// merged. Until then a task's missing startedAt means "not read yet", not
	// "no Orker reported a start" — the Orker's start arrives only with that
	// snapshot — so the timing bar waits for it rather than briefly naming a
	// reason that is not true.
	let terminalFactsRead = $state(untrack(() => isTerminal(data.run?.status ?? Status.UNSPECIFIED)));

	// The two re-reads a live event starts on its own. Neither may take the
	// router from a navigation the reader already began — a link clicked in
	// the instant the run finished used to be silently dropped — so each waits
	// that navigation out, and is skipped when it left this page (see
	// $lib/settle).
	let mounted = true;
	onDestroy(() => (mounted = false));
	function rereadOnEvent(): Promise<boolean> {
		return afterNavigation(
			() => navigating.complete,
			() => mounted,
			invalidateAll
		);
	}

	let terminalRefresh: Promise<void> | undefined;
	function refreshTerminalFacts() {
		if (terminalRefresh) return;
		terminalRefresh = (async () => {
			// Events intentionally remain compact and carry no ActivityFailureDetail.
			// Re-read exactly once at the run's terminal transition instead of
			// inventing a polling loop or pretending the event message is structured.
			if (!(await rereadOnEvent())) return;
			mergeTerminalFacts();
			terminalFactsRead = true;
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
			pin: '',
			orkerId: '',
			items: [],
			...patch
		});
	}

	// isFanOut answers from the definition first: a fan-out is declared, so
	// the page knows before its first snapshot row says so.
	function isFanOut(taskId: string): boolean {
		return taskDefs.get(taskId)?.each !== undefined || tasks.find((t) => t.id === taskId)?.each !== undefined;
	}

	// One event-triggered snapshot read, merged: what a fan-out's expansion
	// (its item count) and its end (every item's facts) need, since events
	// carry neither. Never a poll.
	function rereadAndMerge(): void {
		void (async () => {
			if (await rereadOnEvent()) mergeTerminalFacts();
		})();
	}

	function onEvent(event: Event) {
		buffer = appendEvent(buffer, event);
		// AN ITEM'S EVENT MOVES THAT ITEM'S CELL AND NOTHING ELSE. It is a
		// TASK_LOG carrying `item`; the task's own status is the node's, and
		// is never set from one of its items.
		if (event.item !== undefined) {
			const known = tasks.find((t) => t.id === event.taskId);
			upsertTask(event.taskId, { items: applyItemEvent(known?.items ?? [], event) });
			if (isTerminal(event.status) && selectedItemByTask[event.taskId] === event.item) {
				void refreshAttempts(event.taskId, event.item);
			}
			return;
		}
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
			// `scheduledAt` is what the snapshot uses to answer that. The engine
			// stamps TaskRun.scheduled_at from the same workflow.Now it emits
			// TASK_SCHEDULED with, so the value here agrees with the one the
			// terminal re-read brings back. Written only when the task has none,
			// so a retry's later event cannot move a task's dispatch.
			//
			// startedAt is NOT written here: it is the Orker's own start of the
			// attempt that decided the task, which no event carries. It reaches
			// the page only through a snapshot — the terminal re-read
			// (refreshTerminalFacts) — never from the workflow's clock.
			case EventType.TASK_SCHEDULED:
			case EventType.TASK_STARTED: {
				const fallback =
					event.type === EventType.TASK_SCHEDULED ? Status.PENDING : Status.RUNNING;
				const known = tasks.find((t) => t.id === event.taskId);
				upsertTask(event.taskId, {
					status: status ?? fallback,
					...(known?.scheduledAt ? {} : { scheduledAt: event.timestamp })
				});
				// A fan-out schedules once, when its array was counted: read
				// the expansion so the grid has its cells.
				if (event.type === EventType.TASK_SCHEDULED && isFanOut(event.taskId)) rereadAndMerge();
				break;
			}
			// A task's terminal transition is the moment its LAST attempt record
			// exists — the Orker files one as every attempt ends, including the
			// one that ended the task. Re-read that task's timeline here so the
			// badge and the timeline are right the instant the card turns, not
			// only after the run's own terminal snapshot lands.
			case EventType.TASK_SUCCEEDED:
				upsertTask(event.taskId, { status: status ?? Status.SUCCEEDED });
				if (isFanOut(event.taskId)) rereadAndMerge();
				else void refreshAttempts(event.taskId);
				break;
			case EventType.TASK_FAILED:
				upsertTask(event.taskId, {
					status: status ?? Status.FAILED,
					message: event.message
				});
				if (isFanOut(event.taskId)) rereadAndMerge();
				else void refreshAttempts(event.taskId);
				break;
			// An awaited task started waiting. Its deadline is TASK_AWAITING's
			// own timestamp plus the definition's timeout — the engine stamps both
			// from one workflow.Now — so the event's timestamp is recorded as the
			// open instant until a snapshot brings the deadline itself.
			case EventType.TASK_AWAITING: {
				const known = tasks.find((t) => t.id === event.taskId);
				upsertTask(event.taskId, {
					status: status ?? Status.AWAITING,
					message: event.message,
					...(known?.startedAt ? {} : { startedAt: event.timestamp }),
					...(known?.scheduledAt ? {} : { scheduledAt: event.timestamp })
				});
				break;
			}
			// An awaited task was resolved. The event says so in words; who
			// resolved it and through which door arrive with ONE re-read — an
			// event-triggered read, never a poll.
			case EventType.TASK_RESOLVED:
				if (event.message) upsertTask(event.taskId, { message: event.message });
				void (async () => {
					if (await rereadOnEvent()) mergeTerminalFacts();
				})();
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

	// The item a fan-out card has open, keyed by task. A deep link's
	// `?task=convert&item=97` seeds it, so a reload restores the selection.
	const deepItem = $derived(itemFromUrl(page.url));
	let selectedItemByTask = $state<Record<string, number | undefined>>(
		untrack(() => {
			const task = page.url.searchParams.get('task') ?? '';
			const item = itemFromUrl(page.url);
			return task && item !== undefined ? { [task]: item } : {};
		})
	);
	function selectItem(taskId: string, item: number | undefined): void {
		selectedItemByTask[taskId] = item;
		replaceState(taskHref(taskId, 0, item), page.state);
		if (item !== undefined) void refreshAttempts(taskId, item);
	}
	// A deep-linked item's timeline is read once, as if its panel had been
	// opened by hand.
	$effect(() => {
		const task = untrack(() => deepTask);
		const item = untrack(() => deepItem);
		if (task && item !== undefined && isFanOut(task)) void refreshAttempts(task, item);
	});

	function taskHref(taskId: string, attempt = 0, item?: number): string {
		const params = new URLSearchParams(page.url.searchParams);
		params.set(NAMESPACE_PARAM, namespaceLabel(data.namespace));
		params.set('task', taskId);
		if (attempt > 0) params.set('attempt', String(attempt));
		else params.delete('attempt');
		if (item !== undefined) params.set('item', String(item));
		else params.delete('item');
		return `/runs/${encodeURIComponent(data.runId)}?${params.toString()}`;
	}
	function setTaskAttempt(taskId: string, attempt: number | null, item?: number): void {
		replaceState(taskHref(taskId, attempt ?? 0, item), page.state);
	}
	// The pin requests handed to each task's log pane, newest nonce last.
	//
	// `setTaskAttempt` alone cannot do this job: a shallow replaceState writes
	// the ADDRESS — which is what makes the view sendable — but it deliberately
	// does not re-run the loader or move `page.url`, so nothing downstream of
	// it hears anything. The pane is therefore asked directly, and the address
	// is written beside it so the two agree.
	let logPinByTask = $state<Record<string, { attempt: number; nonce: number }>>({});
	let pinNonce = 0;
	function viewTaskAttemptLogs(taskId: string, attempt: number, item?: number): void {
		pinNonce += 1;
		logPinByTask[attemptsKey(taskId, item)] = { attempt, nonce: pinNonce };
		setTaskAttempt(taskId, attempt, item);
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
			pool: task.pool || undefined, pin: task.pin || undefined, orkerId: task.orkerId || undefined,
			scheduledAt: timestampJson(task.scheduledAt), startedAt: timestampJson(task.startedAt), finishedAt: timestampJson(task.finishedAt),
			output: task.output?.data, values: task.valuesRef ? artifactMetadata(task.valuesRef) : undefined,
			artifacts: task.artifacts.map(artifactMetadata),
			systemArtifacts: task.systemArtifacts.map(artifactMetadata), error: task.error,
			each: task.each ? { expanded: task.each.expanded, count: task.each.count } : undefined,
			items: task.each
				? task.items.map((item) => ({ item: item.index, status: statusLabel(item.status), attempt: item.attempt, error: item.run?.error }))
				: undefined
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
		const key = record.item === undefined
			? `${record.taskId}:${record.kind}:${record.artifact.name}`
			: `${record.taskId}:${record.item}:${record.kind}:${record.artifact.name}`;
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
				runId: run.id, namespace: data.namespace, taskId: record.taskId, kind: record.kind, name: record.artifact.name,
				item: record.item
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


	async function openCompare(): Promise<void> {
		if (!run) return;
		compareDialogOpen = true;
		compareError = '';
		comparePasted = '';
		compareLoading = true;
		const current = run;
		try {
			const res = await orkstack(fetch).runs.listRuns({
				namespace: data.namespace,
				orkflowName: current.orkflowName,
				pageSize: 20
			});
			compareCandidates = res.runs.filter((candidate) => candidate.id !== current.id);
			compareSelected = defaultCompareTarget(res.runs, current.id);
		} catch (err) {
			compareCandidates = [];
			compareSelected = '';
			compareError = errorMessage(err);
		} finally {
			compareLoading = false;
		}
	}

	function openComparison(): void {
		if (!run || compareTarget === '') return;
		void goto(
			withNamespace(
				`/runs/${encodeURIComponent(run.id)}/compare?to=${encodeURIComponent(compareTarget)}`,
				data.namespace
			)
		);
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
			<!--
				WHAT STARTED IT. The kind is a recorded fact, never an inference:
				a run recorded before origins existed says "unrecorded", not
				"submit". The reference is caller-supplied text and is a link only
				when it provably is an http(s) URL; the principal is the gateway's
				assertion and says so.
			-->
			<p class="mt-1 flex flex-wrap items-center gap-x-2 gap-y-1 text-sm text-fg-muted" data-testid="run-origin">
				{#if originSlug}
					<span
						data-testid="run-origin-kind"
						class="inline-flex items-center rounded-full border border-border bg-surface-sunken px-2 py-0.5 text-xs font-medium text-fg-secondary"
						>{originSlug}</span
					>
					{#if origin?.id}
						{#if originHref}
							<a
								href={originHref}
								data-testid="run-origin-link"
								class="font-mono text-xs text-brand-700 hover:underline">{origin.id}</a
							>
						{:else}
							<span data-testid="run-origin-id" class="font-mono text-xs text-fg-secondary">{origin.id}</span>
						{/if}
					{/if}
					{#if origin?.description}
						<span data-testid="run-origin-description">({origin.description})</span>
					{/if}
					{#if origin?.reference}
						{#if originReference}
							<a
								href={originReference}
								target="_blank"
								rel="noopener noreferrer"
								data-testid="run-origin-reference"
								class="break-all text-brand-700 hover:underline">{origin.reference}</a
							>
						{:else}
							<span data-testid="run-origin-reference" class="font-mono text-xs break-all text-fg-secondary"
								>{origin.reference}</span
							>
						{/if}
					{/if}
					{#if origin?.principal}
						<span data-testid="run-origin-principal" title={PRINCIPAL_CAPTION}
							>asserted by {origin.principal}</span
						>
					{/if}
				{:else}
					<span data-testid="run-origin-kind" class="italic" title={UNRECORDED_CAPTION}>{UNRECORDED}</span>
				{/if}
			</p>
		</div>
		<div class="flex items-center gap-3">
			<StatusPill status={runStatus} testid="run-status" />
			<!--
				RETRIES, WHEN THERE WERE ANY. A run whose tasks all passed first
				time says nothing here; one that got there by trying again says
				how many times, because "it succeeded" and "it succeeded on the
				fourth go" are different facts about a deployment.
			-->
			{#if runRetries > 0}
				<span
					data-testid="run-retries"
					title="Total task retries in this run. Each task's own attempts are under its Inspect."
					class="inline-flex items-center rounded-full border border-warning-200 bg-warning-50 px-2.5 py-0.5 text-xs font-medium whitespace-nowrap text-warning-700"
				>
					Retries: {runRetries}
				</span>
			{/if}
			<button
				type="button"
				data-testid="run-stability-toggle"
				aria-pressed={historyOpen}
				onclick={toggleHistory}
				class="inline-flex h-7 items-center rounded-full border px-2.5 text-xs font-medium whitespace-nowrap transition-colors duration-150 {historyOpen
					? 'border-brand-200 bg-brand-50 text-brand-700'
					: 'border-border bg-surface text-fg-muted hover:bg-surface-hover'}"
			>Retry history ({HISTORY_WINDOW})</button>
			{#if historyOpen}
				<CliEquivalent
					command={flakesCli(data.namespace, '168h', [], run.orkflowName)}
					testid="run-stability-cli"
				/>
				{#if historyLoading}
					<span class="text-xs text-fg-muted" data-testid="run-stability-loading">loading…</span>
				{:else if historyError}
					<span class="text-xs text-failed-fg" data-testid="run-stability-error"
						>retry history unavailable: {historyError}</span
					>
				{:else if history}
					<span class="text-xs text-fg-muted" data-testid="run-stability-note">{historyNote}</span>
				{/if}
			{/if}
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
			{#if isTerminal(runStatus)}
				<!-- The engine exports evidence for finished runs only, so a live
				     run is not offered it. -->
				<button
					type="button"
					data-testid="run-evidence"
					onclick={() => (evidenceDialogOpen = true)}
					class="inline-flex h-9 items-center rounded-md border border-border bg-surface px-3 text-sm font-medium text-fg shadow-xs hover:bg-surface-hover"
				>Evidence</button>
			{/if}
			<button
				type="button"
				data-testid="compare-run"
				onclick={() => void openCompare()}
				class="inline-flex h-9 items-center rounded-md border border-border bg-surface px-3 text-sm font-medium text-fg shadow-xs hover:bg-surface-hover"
			>Compare</button>
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
				{tasks.filter((t) => t.status === Status.SUCCEEDED).length}/{tasks.length} succeeded{#if tasks.some((t) => t.status === Status.AWAITING)}, {tasks.filter((t) => t.status === Status.AWAITING).length} awaiting{/if}
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
			<!--
				The attempt store is not the run: a deployment that has none, or
				one whose store is unreachable, still renders every status, every
				log and every artifact on this page. Say what is missing and what
				it costs — nothing else changes.
			-->
			{#if attemptsError}
				<p data-testid="attempts-unavailable" class="mb-3 rounded-md border border-warning-200 bg-warning-50 px-3 py-2 text-xs text-warning-700">
					Attempt history is not available: {attemptsError}. Task statuses, output and artifacts
					are unaffected; what each earlier try of a retried task did cannot be shown.
				</p>
			{/if}
			{#if artifactDownloadError}
				<p data-testid="artifact-download-error" class="mb-3 rounded-md border border-danger-200 bg-danger-50 px-3 py-2 text-xs text-danger-700">Artifact download failed: {artifactDownloadError}</p>
			{/if}
			<div class="space-y-2" data-testid="task-list">
				{#each tasks as task (task.id)}
					{@const dispatched = taskDispatched(task.status, task.scheduledAt ?? task.startedAt)}
					{@const taskAttempts = attemptsByTask.get(task.id) ?? []}
					{@const isAwait = task.await !== undefined || taskDefs.get(task.id)?.await !== undefined}
					{@const timing = taskTiming({ ...task, awaited: isAwait }, data.attemptStoreAbsent ? undefined : taskAttempts, currentTimestamp)}
					{@const accent = pinAccent(task.pin)}
					{@const fanOut = task.each !== undefined || taskDefs.get(task.id)?.each !== undefined}
					<!--
						A pinned task's card carries its pin's accent as a thicker
						left edge and a faint fill, so the members of one pin read as
						a block in the list without being re-sorted — the DAG order
						is the order, and regrouping the list would hide it.

						The accent is applied as an inline style rather than a class
						because the token is chosen at runtime: Tailwind's scanner
						only sees literal class names in source, so a composed
						`border-l-{accent}` would emit no CSS at all. The tokens it
						reads are therefore declared in a plain `:root` in app.css
						and NOT in `@theme`, which drops variables no utility class
						references — see the note there.

						It is decoration on top of the status pill, never a
						substitute for it, and the `Pin <name>` label below says the
						same thing in words.
					-->
					<div
						class="rounded-lg border bg-surface px-4 py-3 shadow-xs {dispatched
							? 'border-border'
							: 'border-dashed border-border-subtle opacity-60'}"
						style={accent
							? `border-left-width:3px;border-left-style:solid;border-left-color:var(--color-${accent}-line);background-color:var(--color-${accent}-fill)`
							: undefined}
						data-testid="task-{task.id}"
						data-dispatched={dispatched}
						data-pin={task.pin || undefined}
						data-pin-accent={accent}
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
							<div class="flex flex-wrap items-center justify-end gap-2">
								{#if dispatched || task.status === Status.SKIPPED}
									<StatusPill status={task.status} testid="task-{task.id}-status" />
								{:else}
									<span
										data-testid="task-{task.id}-status"
										data-state="not-started"
										class="text-[11px] whitespace-nowrap text-fg-muted"
									>{notStartedCaption(task)}</span>
								{/if}
								<!--
									A retry is invisible in a status — SUCCEEDED is SUCCEEDED
									whether it took one try or four — so it is said beside the
									pill rather than folded into it.
								-->
								{#if fanOut}
									<!--
										A fan-out retries per item: the badge counts the items
										that took more than one try, never a sum of their tries.
									-->
									{@const retried = retriedItemsLabel(task.items)}
									{#if retried}
										<span
											data-testid="task-{task.id}-retries"
											title="Temporal retried these items. Open an item for the outcome and error of every attempt."
											class="inline-flex items-center rounded-full border border-warning-200 bg-warning-50 px-2 py-0.5 text-[11px] font-medium whitespace-nowrap text-warning-700 leading-4"
										>{retried}</span>
									{/if}
								{:else}
								<RetryBadge
									attempts={taskAttempts}
									terminal={isTerminal(task.status)}
									testid="task-{task.id}-retries"
									history={flakesByTask.get(task.id)}
									historyWindow={HISTORY_WINDOW}
								/>
								{/if}
							</div>
						</div>
						<div class="mt-2 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-fg-muted">
							<!--
								WAITING OR SLOW. While the task has not finished, the only
								honest number is how long ago it was scheduled — no
								per-phase or wait/run split exists yet. Once it has, the
								bar splits its time into the wait for an Orker and the
								run on one. The Orker's start reaches the page with the
								run's terminal snapshot, so until that snapshot has been
								read (terminalFactsRead) a finished task shows its
								scheduled-to-finished span rather than a bar that would
								call the start unknown.
							-->
							{#if isAwait}
								{#if timing.run.kind === 'value'}
									<span data-testid="task-{task.id}-elapsed">Awaited {formatQuantity(timing.run)}</span>
								{/if}
							{:else if dispatched && !isTerminal(task.status)}
								<span data-testid="task-{task.id}-elapsed">Since scheduled {formatDuration(task.scheduledAt ?? task.startedAt, currentTimestamp)}</span>
							{:else if timing.scheduled && isTerminal(task.status) && (task.startedAt || terminalFactsRead)}
								<TimingBar taskId={task.id} {timing} />
							{:else if timing.scheduled && isTerminal(task.status)}
								<span data-testid="task-{task.id}-elapsed">Elapsed {formatDuration(task.scheduledAt ?? task.startedAt, task.finishedAt)}</span>
							{/if}
							<!--
								LIVE. While the task runs, what its pending attempt is doing
								now, from the heartbeat the cluster holds: its own running
								time, its phase, bytes and heartbeat age — or why that is
								unavailable. Nothing about the attempt's PAST is drawn from
								here; that is AttemptTimeline's. The Orker the heartbeat
								names is a live claim, so it has its own testid beside the
								snapshot's proof a pin held.
							-->
							{#if task.status === Status.RUNNING}
								{@const live = liveByTask.get(task.id)}
								{#if live?.attemptStartedAt}
									<span data-testid="task-{task.id}-running-for">Running for {formatDuration(live.attemptStartedAt, liveNowTimestamp)}</span>
								{/if}
								{#if live}
									{@const line = liveLine(live, liveNowMs)}
									<span
										data-testid="task-{task.id}-live"
										data-live={line.kind}
										data-stale={line.stale}
										title={line.title}
										class={line.stale ? 'font-medium text-warning-700' : ''}>{line.text}</span
									>
									{#if live.heartbeatAvailable && live.orkerId && !task.orkerId}
										<span data-testid="task-{task.id}-live-orker">Orker {live.orkerId}</span>
									{/if}
								{:else if liveError}
									<span data-testid="task-{task.id}-live" data-live="unavailable" data-stale="false" title={liveError}>Live detail unavailable</span>
								{/if}
							{/if}
							{#if task.pool}<span>Pool {task.pool}</span>{/if}
							<!--
								Pin and Orker are stated the same way Pool is: when the
								run knows one. Orker is filled for every task that
								reached one, pinned or not, so it is NOT gated on the
								pin — its absence means the task never got to an Orker.
								Together they are the proof a pin held, which is why the
								pin's own accent dot is drawn beside its name.
							-->
							{#if task.pin}
								<span data-testid="task-{task.id}-pin" class="inline-flex items-center gap-1.5">
									<span
										class="inline-block size-2 shrink-0 rounded-full"
										style="background-color:var(--color-{pinAccent(task.pin)}-line)"
									></span>
									Pin {task.pin}
								</span>
							{/if}
							{#if task.orkerId}
								<span data-testid="task-{task.id}-orker">Orker {task.orkerId}</span>
							{/if}
							{#if !isAwait && !fanOut}
							<CliEquivalent
								command={runCommand(data.namespace, ['run', 'logs', run.id, '--task', task.id, ...(task.attempt > 0 ? ['--attempt', String(task.attempt)] : [])])}
								testid="run-task-{task.id}-logs-cli"
								label="Copy logs CLI"
							/>
							{#if task.status === Status.FAILED}
								<!--
									A local tool, handed over the way activity build/test are:
									the page copies the command and runs nothing. It carries the
									namespace, the run and the task — no input, no secret, no
									path; a task that declares secrets makes the CLI name the
									exact --secret flags to add.
								-->
								<CliEquivalent
									command={taskRunCommand(data.namespace, run.id, task.id)}
									testid="run-task-{task.id}-replay-cli"
									label="Copy the command that runs this task locally"
								/>
							{/if}
							{/if}
							{#if task.status === Status.RUNNING}
								<CliEquivalent
									command={runCommand(data.namespace, ['run', 'inspect', run.id, '--task', task.id])}
									testid="run-task-{task.id}-inspect-cli"
									label="Copy inspect CLI"
								/>
							{/if}
							{#if timing.scheduled}
								<CliEquivalent
									command={runCommand(data.namespace, ['run', 'timing', run.id, '--task', task.id])}
									testid="run-task-{task.id}-timing-cli"
									label="Copy timing CLI"
								/>
							{/if}
						</div>

						{#if task.error}
							<div class="mt-3">
								<FailurePanel
									error={task.error}
									testid="task-{task.id}-error"
									compact
									retryState={task.retryState}
									attemptCount={attemptsSeen(taskAttempts)}
								/>
							</div>
						{/if}
						{#if isAwait}
							<!--
								An awaited task runs no container: no log pane, and no image
								to inspect. The panel says what it waits for, until when, who
								resolved it — and resolves it.
							-->
							<AwaitPanel
								runId={run.id}
								namespace={data.namespace}
								task={{ id: task.id, status: task.status, await: task.await, error: task.error, startedAt: task.startedAt }}
								spec={taskDefs.get(task.id)?.await}
								runTerminal={isTerminal(runStatus)}
								now={currentTimestamp}
								origin={page.url.origin}
							/>
						{:else if fanOut}
							{@const selected = selectedItemByTask[task.id]}
							<EachItemGrid
								runId={run.id}
								namespace={data.namespace}
								taskId={task.id}
								each={task.each}
								spec={taskDefs.get(task.id)?.each}
								items={task.items}
								{selected}
								onSelect={(item) => selectItem(task.id, item)}
								attemptsOf={(item) => attemptsByTask.get(attemptsKey(task.id, item)) ?? []}
								artifacts={artifactsByTask.get(task.id) ?? []}
								downloading={downloadingArtifact}
								onDownload={(record) => void downloadArtifact(record)}
								liveOf={(item) => liveByTask.get(attemptsKey(task.id, item))}
								{liveNowMs}
								attempt={deepTask === task.id && deepItem === selected && Number.isFinite(deepAttempt) ? deepAttempt : 0}
								deepLinked={deepTask === task.id && deepItem !== undefined && deepItem === selected}
								pinRequest={selected !== undefined ? logPinByTask[attemptsKey(task.id, selected)] : undefined}
								onAttemptChange={(attempt) => setTaskAttempt(task.id, attempt, selected)}
								onAttemptSeen={(attempt) => attemptSeen(task.id, attempt, selected)}
								onViewLogs={(attempt) => viewTaskAttemptLogs(task.id, attempt, selected)}
							/>
						{:else}
						<TaskLogPane
							runId={run.id}
							taskId={task.id}
							namespace={data.namespace}
							status={task.status}
							scheduledAt={task.scheduledAt ?? task.startedAt}
							attempt={deepTask === task.id && Number.isFinite(deepAttempt) ? deepAttempt : 0}
							deepLinked={deepTask === task.id}
							pinRequest={logPinByTask[task.id]}
							onAttemptChange={(attempt) => setTaskAttempt(task.id, attempt)}
							onAttemptSeen={(attempt) => attemptSeen(task.id, attempt)}
						/>
						{/if}
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
							{@const taskArtifacts = (artifactsByTask.get(task.id) ?? []).filter((record) => record.item === undefined)}
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
								{#if !isAwait && (def || task.exitCode !== undefined || task.commit)}
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
									WHAT EVERY TRY DID. The block above describes the
									task's FINAL attempt — its exit code, its commit —
									because that is all a TaskRun carries. The timeline
									is the rest of the story: the tries that failed, why
									they failed, and which Orker was holding each one.
									It is drawn only when the store had something to
									say; a deployment with no attempt store shows the
									page-level notice instead of an empty disclosure in
									every card.
								-->
								{#if !fanOut && (!data.attemptsError || taskAttempts.length > 0)}
									<AttemptTimeline
										runId={run.id}
										taskId={task.id}
										namespace={data.namespace}
										attempts={taskAttempts}
										pool={task.pool}
										decidedOn={task.attempt}
										{timing}
										onViewLogs={(attempt) => viewTaskAttemptLogs(task.id, attempt)}
									/>
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
													<ArtifactProvenance runId={run.id} namespace={data.namespace} taskId={task.id} kind={record.kind} name={record.artifact.name} />
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
					onsubmitted={(newRunId) => {
						// The new run opens on this same route, so this component
						// survives the navigation: close the dialog first, or the
						// new run's page opens under a stale "Submitting…" dialog.
						runAgainDialogOpen = false;
						void goto(withNamespace(`/runs/${newRunId}`, data.namespace));
					}}
				/>
			{/snippet}
		</Dialog>
	{/if}

	{#if evidenceDialogOpen}
		<RunEvidenceDialog runId={run.id} namespace={data.namespace} onclose={() => (evidenceDialogOpen = false)} />
	{/if}
	{#if compareDialogOpen}
		<Dialog
			testid="compare-dialog"
			title="Compare this run with another"
			widthClass="max-w-xl"
			onclose={() => (compareDialogOpen = false)}
		>
			{#snippet body()}
				<div class="grid grid-cols-1 gap-4">
					<div class="grid grid-cols-1 gap-1.5">
						<label for="compare-target" class="text-xs font-medium text-fg-muted"
							>A recent run of {run.orkflowName}</label
						>
						{#if compareLoading}
							<p class="text-sm text-fg-muted" data-testid="compare-loading">Loading recent runs…</p>
						{:else if compareError}
							<ErrorPanel
								testid="compare-error"
								retryTestid="compare-retry"
								subject="recent runs of {run.orkflowName}"
								message={compareError}
								onretry={() => void openCompare()}
							/>
						{:else if compareCandidates.length === 0}
							<p class="text-sm text-fg-muted" data-testid="compare-no-candidates">
								No other run of {run.orkflowName} in this namespace. Paste a run id below.
							</p>
						{:else}
							<Select
								id="compare-target"
								testid="compare-target"
								value={compareSelected}
								options={compareCandidates.map((candidate) => ({
									value: candidate.id,
									label: `${candidate.id} · ${statusLabel(candidate.status)} · ${fmtTimestamp(candidate.createdAt)}`
								}))}
								onchange={(value) => (compareSelected = value)}
							/>
						{/if}
					</div>
					<div class="grid grid-cols-1 gap-1.5">
						<label for="compare-target-id" class="text-xs font-medium text-fg-muted"
							>Or any run id (of any orkflow)</label
						>
						<input
							id="compare-target-id"
							data-testid="compare-target-id"
							type="text"
							bind:value={comparePasted}
							placeholder="ork-…"
							autocomplete="off"
							spellcheck="false"
							class="h-9 rounded-md border border-border bg-surface px-3 font-mono text-sm text-fg shadow-xs focus-visible:border-brand-500 focus-visible:ring-2 focus-visible:ring-brand-500/25 focus-visible:outline-none"
						/>
					</div>
					<div class="flex justify-end">
						<button
							type="button"
							data-testid="compare-open"
							disabled={compareTarget === ''}
							onclick={openComparison}
							class="inline-flex h-9 items-center rounded-md bg-brand-600 px-3 text-sm font-medium text-white shadow-xs hover:bg-brand-700 disabled:opacity-50"
						>Open comparison</button>
					</div>
				</div>
			{/snippet}
		</Dialog>
	{/if}
{/if}
