<script lang="ts">
	import { copyText, MANUAL_COPY_MESSAGE, selectByTestid } from '$lib/clipboard';
	import { pollWhileVisible } from '$lib/visibility';
	import { rememberRecent } from '$lib/palette/recent';
	import { onDestroy, untrack } from 'svelte';
	import { goto, invalidateAll, replaceState } from '$app/navigation';
	import { navigating, page } from '$app/state';
	import ArtifactProvenance from '$lib/components/ArtifactProvenance.svelte';
	import AwaitPanel from '$lib/components/AwaitPanel.svelte';
	import ChildRunPanel from '$lib/components/ChildRunPanel.svelte';
	import { childLabel, childrenListHref } from '$lib/compose';
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
		applyItemEvents,
		itemFromUrl,
		itemsFromTaskRun,
		mergeItems,
		retriedItemsLabel,
		type ItemView
	} from '$lib/each';
	import FailurePanel from '$lib/components/FailurePanel.svelte';
	import { plainFailure } from '$lib/failureText';
	import Icon from '$lib/components/Icon.svelte';
	import RetryBadge from '$lib/components/RetryBadge.svelte';
	import CacheBadge from '$lib/components/CacheBadge.svelte';
	import type { CacheResult } from '$lib/gen/orkstack/v1/activity_pb';
	import { CacheOutcome, JoinPolicy } from '$lib/gen/orkstack/v1/activity_pb';
	import {
		failingBannerText,
		isFailingNote,
		joinTitle,
		joinWord,
		outstandingHandlers,
		runFailing
	} from '$lib/join';
	import {
		cacheOutcomeLabel,
		cacheSourceHref,
		cacheSpecLabel,
		cachedCount,
		memoWriteSentence,
		missReasonSentence,
		sourceAttemptDuration
	} from '$lib/cache';
	import AttemptTimeline from '$lib/components/AttemptTimeline.svelte';
	import TimingBar from '$lib/components/TimingBar.svelte';
	import { formatQuantity, taskTiming, timelinePlacement } from '$lib/timing';
	import { timestampMs } from '$lib/stats';
	import ConfirmDialog from '$lib/components/ConfirmDialog.svelte';
	import Select from '$lib/components/Select.svelte';
	import { defaultCompareTarget } from '$lib/runDiff';
	import { errorMessage } from '$lib/client';
	import { runClient } from '$lib/clients/runs';
	import { statsClient } from '$lib/clients/stats';
	import { artifactMetadata, commandWords, downloadFilename, emptyPayload, formatDuration, inlinePayload, payloadJson, pinAccent, runCommand, shortRunId, taskRunCommand, timestampJson } from '$lib/runDetail';
	import { EVENT_BUFFER_CAP, appendEvents, emptyEventBuffer } from '$lib/eventBuffer';
	import { eventBatcher, slicedApply } from '$lib/eventBatch';
	import { Status, type ArtifactRef, type Payload } from '$lib/gen/orkstack/v1/common_pb';
	import {
		EventType,
		TaskAwaitSchema,
		TaskChildSchema,
		type Event,
		type RetryState,
		type Run,
		type TaskAttempt,
		type TaskAwait,
		type TaskChild,
		type TaskEach,
		type TaskRun
	} from '$lib/gen/orkstack/v1/run_pb';
	import { create } from '@bufbuild/protobuf';
	import {
		attemptsKey,
		attemptsSeen,
		groupAttemptsByTask,
		readAttemptSeed,
		runRetryCount,
		seedStillCurrent,
		type FailureTries,
		type SeedKey
	} from '$lib/attempts';
	import { Code, ConnectError } from '@connectrpc/connect';
	import { durationFromMs } from '@bufbuild/protobuf/wkt';
	import { FailureGroupBy, type FlakeGroup, type GetFlakeIndexResponse } from '$lib/gen/orkstack/v1/stats_pb';
	import { flakesCli } from '$lib/flakes';
	import { watchRunEvents, type RunSocketState } from '$lib/runEvents';
	import { runWindowFilters } from '$lib/browsing';
	import { NAMESPACE_PARAM, namespaceLabel, withNamespace } from '$lib/namespace';
	import {
		PRINCIPAL_CAPTION,
		UNRECORDED,
		UNRECORDED_CAPTION,
		originKindSlug,
		originLinkPath,
		originReferenceURL
	} from '$lib/origin';
	import { fmtTimestamp, isTerminal, isTerminalEventType, keepsEventMessage, statusLabel, taskDispatched } from '$lib/status';
	import { activityLabel, hasError, pinnedOrReference, shortDigest } from '$lib/activity';
	import type { Error as OrkError } from '$lib/gen/orkstack/v1/common_pb';
	import type { CommitRef } from '$lib/gen/orkstack/v1/activity_pb';
	import { ArtifactKind, type LiveAttempt, type RunArtifact } from '$lib/gen/orkstack/v1/run_service_pb';
	import { liveLine } from '$lib/live';
	import { afterNavigation } from '$lib/settle';
	import DrawingFrame, { type DrawingView } from '$lib/components/DrawingFrame.svelte';
	import RevisionCloud from '$lib/components/RevisionCloud.svelte';
	import SheetHeader from '$lib/components/SheetHeader.svelte';
	import MoreMenu from '$lib/components/MoreMenu.svelte';
	import TaskMap from '$lib/components/TaskMap.svelte';
	import TaskMapLegend from '$lib/components/TaskMapLegend.svelte';
	import VersionTable from '$lib/components/VersionTable.svelte';
	import { DAG_DRAW_LIMIT, type NodeState } from '$lib/dag';
	import { itemLabel } from '$lib/each';
	import { isFailureHandler } from '$lib/join';
	import { recordSeen } from '$lib/heldRuns';
	import { registerProvider } from '$lib/palette/context';
	import { commandId, type Command } from '$lib/palette/commands';
	import { match } from '$lib/palette/rank';
	import { setTitleBlock } from '$lib/sheet.svelte';
	import { scrollToCard, twoFrames, type MapTask } from '$lib/taskMap';
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
		// each is set for a fan-out (docs/DSL.md "Fan-out"): whether its
		// array was counted, and into how many items. items are those items,
		// seeded from the snapshot and moved one cell at a time by item
		// events; a fan-out's own row has no attempt, log or artifact of its
		// own — every item has.
		each?: TaskEach;
		items: ItemView[];
		// child is set for a composed task (`uses: orkflow:`): the child
		// Orkflow it pins — from the definition, so a pending card already says
		// what it will run — and, once the start is confirmed, the child run's
		// id from the snapshot. A composed task reaches no Orker and writes no
		// logs: its card shows the child instead.
		child?: TaskChild;
		// cache is the memo decision of the attempt that produced this task's
		// outcome (TaskRun.cache, docs/DSL.md "Caching") — set only for a
		// cached task that succeeded, from the snapshot the task's terminal
		// event brings.
		cache?: CacheResult;
		// factsRead is true once a snapshot that shows this task terminal has
		// been merged: until then an absent cache means "not read yet", never
		// "the Orker reported none".
		factsRead?: boolean;
		// join is the task's join policy (docs/DSL.md "Joins and failure
		// handlers"): seeded from the definition, copied from TaskRun.join
		// only when set — the rule pin follows. Absent for all_succeeded.
		join?: JoinPolicy;
	}

	const run = $derived(data.run);

	// The palette's RECENT group: this browser remembers the run's id, its
	// namespace and its Orkflow's name — never its inputs — after the page has
	// drawn, off the path that draws the header and the task cards.
	$effect(() => {
		const id = data.run?.id;
		if (!id) return;
		const entry = { kind: 'run' as const, id, namespace: namespaceLabel(data.namespace), orkflow: data.run?.orkflowName };
		const t = setTimeout(() => rememberRecent(entry), 0);
		return () => clearTimeout(t);
	});
	// Whether the pinned definition declares `outputs:` — then Run.output IS
	// those, and otherwise it is every leaf task's values.
	const declaresOutputs = $derived(Object.keys(data.orkflow?.outputs ?? {}).length > 0);
	// An input of {} is no input: it is said in the run's values NOTE, not drawn as an empty box.
	const hasRunInput = $derived(!emptyPayload(data.run?.input));
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
	// failingNote is the text of the failing note — the one TASK_LOG with no
	// task id a run with failure handlers emits when it starts failing. It is
	// what makes the Failing banner appear live, before the re-read it starts
	// brings the structured Run.error; '' until it arrives.
	let failingNote = $state('');
	let socketState = $state<RunSocketState>('connecting');
	let streamDelivered = $state(false);
	// Why the stream stopped for good ('failed'), in watchRunEvents' words.
	let streamFailure = $state('');
	// The socket is only 'live' once it has actually delivered something.
	// 'error' or a reconnect loop that never delivers means the browser is
	// not receiving this run, whatever the run itself is doing.
	const streamBroken = $derived(
		socketState === 'failed' || (!streamDelivered && (socketState === 'error' || socketState === 'closed'))
	);
	// The event stream, BOUNDED. The socket delivers every event whether or
	// not the inspector is open — the backlog has to be there when it opens —
	// so this is what a long run grows, and appendEvents caps it at
	// EVENT_BUFFER_CAP and counts what it drops. $state.raw because the buffer
	// is replaced wholesale and a deeply reactive proxy per event is what made
	// a chatty run's page heavy.
	let buffer = $state.raw(emptyEventBuffer());
	let tasks = $state<TaskView[]>(untrack(seedTasks));
	// taskById indexes `tasks` by id, so an event, a card or a derived value
	// finds its task in one lookup instead of a scan of every card: at the
	// task ceiling (thousands of tasks) a scan per event — and one per task
	// inside the failing-handlers banner — kept a live page busy for tens of
	// seconds. It holds the same reactive objects `tasks` does; `tasks` is
	// only ever seeded and pushed to, so a length that moved is the one
	// change that rebuilds it.
	const taskById = new Map<string, TaskView>();
	function findTask(id: string): TaskView | undefined {
		if (taskById.size !== tasks.length) {
			taskById.clear();
			for (const task of tasks) taskById.set(task.id, task);
		}
		return taskById.get(id);
	}
	// The tasks the loaded snapshot already holds as finished. Their attempts
	// arrive with the whole run's timeline read after the page renders, so a
	// replayed terminal event of one of them asks for nothing more: a live
	// page opened on a run thousands of tasks into its life would otherwise
	// send one attempts read per finished task.
	const finishedAtLoad = $derived(
		new Set((data.run?.taskRuns ?? []).filter((tr) => isTerminal(tr.status)).map((tr) => tr.taskId))
	);
	// The header's "· N cached": reused results, fan-out items included.
	const cachedTotal = $derived(
		cachedCount(
			tasks.map((t) => ({
				cache: t.cache,
				items: t.items.map((i) => i.run).filter((r): r is TaskRun => r !== undefined)
			}))
		)
	);
	// EVERY TRY OF EVERY TASK, KEPT PER TASK.
	//
	// The whole run's timeline is read in one call AFTER the page has
	// rendered — the run, its statuses and its logs need only the loader's
	// GetRun — and again on every later loader answer (a retry, a cancel, the
	// terminal re-read), keyed on the run snapshot's reference so a re-render
	// that changed nothing asks nothing. From then on it is REPLACED PER TASK
	// by the refetch below, because that is the grain the live triggers have:
	// a task reached its terminal status, or its log pane saw a higher
	// attempt. $state.raw and a fresh Map per write — the value is swapped
	// wholesale, and a deep proxy over every attempt record of a thirty-task
	// run buys nothing.
	let attemptsByTask = $state.raw<Map<string, TaskAttempt[]>>(new Map());
	// What the timeline could not be read for, if anything. It is the seed's
	// tolerant answer, updated by the refetches, so a store that came back
	// reachable stops apologising.
	let attemptsError = $state<string | null>(null);
	// No attempt store at all (FailedPrecondition): timing names it.
	let attemptStoreAbsent = $state(false);
	// Until the seed lands, nothing is said about earlier tries: an empty map
	// is "not read yet", which must not read as "no record".
	let attemptsSeeded = $state(false);
	let seedGeneration = 0;
	// What the last seed read was issued for: a finished run's later loader
	// answers (its replayed terminal event re-reads the run) keep it.
	let seededFor: SeedKey | null = null;
	$effect(() => {
		const snapshot = data.run;
		if (!snapshot) return;
		const terminal = isTerminal(snapshot.status);
		if (seedStillCurrent(seededFor, snapshot.id, terminal)) return;
		const key: SeedKey = { runId: snapshot.id, terminal };
		seededFor = key;
		const mine = ++seedGeneration;
		const namespace = untrack(() => data.namespace);
		void readAttemptSeed(
			runClient(fetch),
			snapshot.id,
			namespace,
			errorMessage,
			(err) => err instanceof ConnectError && err.code === Code.FailedPrecondition
		).then((seed) => {
			if (mine !== seedGeneration) return;
			// A read that failed is not kept: the next loader answer asks again.
			if (seed.error !== null && seededFor === key) seededFor = null;
			attemptsByTask = groupAttemptsByTask(seed.attempts);
			attemptsError = seed.error;
			attemptStoreAbsent = seed.storeAbsent;
			attemptsSeeded = true;
		});
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
			const res = await runClient(fetch).listTaskAttempts({
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
			const resp = await statsClient(fetch).getFlakeIndex({
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
		let note = `${runs} finished ${runs === 1n ? 'run' : 'runs'} of this Orkflow in the last ${HISTORY_WINDOW}; attempt records complete for ${history.runsEvidenceComplete}, missing for ${history.runsEvidenceMissing}`;
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
	// Which tasks' output panes are open: the toggle sits in the row's
	// actions, the pane below it (TaskLogPane binds it).
	let logOpenByTask = $state<Record<string, boolean>>({});
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
	// The run's span, the timeline every task row shares: started to finished,
	// or to now while it runs.
	const runSpan = $derived({
		start: timestampMs(startedAt ?? data.run?.startedAt ?? data.run?.createdAt),
		end: timestampMs(finishedAt ?? data.run?.finishedAt) ?? (isTerminal(runStatus) ? null : wallNow)
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
			const res = await runClient(fetch).inspectRun({ runId: run.id, namespace: data.namespace });
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
		return pollWhileVisible(() => void pollLive(), LIVE_POLL_MS, { immediate: true });
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
				...(task.join ? { join: task.join } : {}),
				...(task.await ? { await: create(TaskAwaitSchema, { callback: task.await.callback }) } : {}),
				...(task.orkflow
					? {
							child: create(TaskChildSchema, {
								orkflowName: task.orkflow.name,
								orkflowVersion: task.orkflow.version,
								orkflowDefinitionDigest: task.orkflow.definitionDigest
							})
						}
					: {})
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
				...(tr.child ? { child: tr.child } : existing?.child ? { child: existing.child } : {}),
				...(tr.cache ? { cache: tr.cache } : {}),
				...(tr.join ? { join: tr.join } : existing?.join ? { join: existing.join } : {}),
				factsRead: isTerminal(tr.status),
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
		mapRevision++;
		for (const tr of data.run?.taskRuns ?? []) {
			const existing = findTask(tr.taskId);
			upsertTask(tr.taskId, {
				status: tr.status,
				attempt: tr.attempt,
				// A SKIPPED or CANCELED row with no error is explained only by
				// its TASK_LOG sentence (a join's skip, a fail-fast cancel),
				// which the snapshot has no field for: keep it.
				message:
					tr.error?.message ??
					(keepsEventMessage(tr.status) ? (existing?.message ?? '') : ''),
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
				// The child run's id arrives with the snapshot that follows its
				// confirmed start; copied like await.
				...(tr.child ? { child: tr.child } : {}),
				// The memo decision arrives with the terminal snapshot; copied
				// like child.
				...(tr.cache ? { cache: tr.cache } : {}),
				...(isTerminal(tr.status) ? { factsRead: true } : {}),
				// Patched only when the snapshot carries one: upsertTask assigns
				// every key present in the patch, so an empty pin here would
				// erase the pin the definition already seeded.
				...(tr.pin ? { pin: tr.pin } : {}),
				// The join, on pin's rule: patched only when set.
				...(tr.join ? { join: tr.join } : {}),
				// A fan-out's expansion and its items, never moving an item an
				// event already settled back to unsettled.
				...(tr.each
					? {
							each: tr.each,
							items: mergeItems(findTask(tr.taskId)?.items ?? [], itemsFromTaskRun(tr))
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
		const existing = findTask(taskId);
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
		return taskDefs.get(taskId)?.each !== undefined || findTask(taskId)?.each !== undefined;
	}

	// isComposed answers from the definition first too: a composed task is
	// declared (`uses: orkflow:`), whether or not its child has started.
	function isComposed(taskId: string): boolean {
		return taskDefs.get(taskId)?.orkflow !== undefined || findTask(taskId)?.child !== undefined;
	}
	// Whether this run's definition composes anything: the header's
	// "Child runs" link is drawn only then.
	const composesAny = $derived(
		(data.orkflow?.tasks ?? []).some((t) => t.orkflow !== undefined) ||
			(data.run?.taskRuns ?? []).some((tr) => tr.child !== undefined)
	);

	// One event-triggered snapshot read, merged: what a fan-out's expansion
	// (its item count) and its end (every item's facts) need, since events
	// carry neither. Never a poll.
	function rereadAndMerge(): void {
		void (async () => {
			if (await rereadOnEvent()) mergeTerminalFacts();
		})();
	}

	// applyEvents takes one frame's worth of socket events ($lib/eventBatch),
	// in delivery order: ONE buffer append for the batch, ONE item-array copy
	// and write per fan-out it touches, and the reducer below for every other
	// event.
	function applyEvents(events: Event[]) {
		mapRevision++;
		buffer = appendEvents(buffer, events);
		// AN ITEM'S EVENT MOVES THAT ITEM'S CELL AND NOTHING ELSE. It is a
		// TASK_LOG carrying `item`; the task's own status is the node's, and
		// is never set from one of its items.
		const itemEvents = new Map<string, Event[]>();
		for (const event of events) {
			if (event.item !== undefined) {
				const list = itemEvents.get(event.taskId);
				if (list) list.push(event);
				else itemEvents.set(event.taskId, [event]);
				continue;
			}
			onEvent(event);
		}
		for (const [taskId, list] of itemEvents) {
			const known = findTask(taskId);
			upsertTask(taskId, { items: applyItemEvents(known?.items ?? [], list) });
			for (const event of list) {
				if (isTerminal(event.status) && selectedItemByTask[taskId] === event.item) {
					void refreshAttempts(taskId, event.item);
				}
			}
		}
	}

	// onEvent reduces one event that is not an item's into the run's and its
	// tasks' state.
	function onEvent(event: Event) {
		const status = event.status === Status.UNSPECIFIED ? undefined : event.status;
		switch (event.type) {
			// A run never leaves a terminal status. The socket replays a run's
			// stream from its first event, so on a page opened on a finished
			// run its RUN_CREATED and RUN_STARTED arrive after the snapshot
			// said how it ended — and for a run of thousands of tasks, its own
			// end arrives many frames later. They move nothing back.
			case EventType.RUN_CREATED:
				if (!isTerminal(runStatus)) runStatus = status ?? Status.PENDING;
				break;
			case EventType.RUN_STARTED:
				if (!isTerminal(runStatus)) runStatus = status ?? Status.RUNNING;
				startedAt = event.timestamp;
				break;
			// The terminal re-read is for a page that watched the run end; one
			// opened on a finished run already holds its terminal snapshot.
			case EventType.RUN_SUCCEEDED:
				runStatus = status ?? Status.SUCCEEDED;
				finishedAt = event.timestamp;
				if (!terminalFactsRead) refreshTerminalFacts();
				break;
			case EventType.RUN_FAILED:
				if (!isTerminal(runStatus) && loadedLive) failedWhileWatched = true;
				runStatus = status ?? Status.FAILED;
				finishedAt = event.timestamp;
				if (event.message) runErrorMessage = event.message;
				if (!terminalFactsRead) refreshTerminalFacts();
				break;
			case EventType.RUN_CANCELED:
				runStatus = status ?? Status.CANCELED;
				finishedAt = event.timestamp;
				if (!terminalFactsRead) refreshTerminalFacts();
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
				const known = findTask(event.taskId);
				// A finished task's replayed dispatch moves its status nowhere:
				// a task never leaves a terminal status either.
				upsertTask(event.taskId, {
					...(isTerminal(known?.status) ? {} : { status: status ?? fallback }),
					...(known?.scheduledAt ? {} : { scheduledAt: event.timestamp })
				});
				// A fan-out schedules once, when its array was counted: read
				// the expansion so the grid has its cells.
				if (event.type === EventType.TASK_SCHEDULED && isFanOut(event.taskId)) rereadAndMerge();
				// A composed task's TASK_STARTED is its child run's confirmed
				// start: one re-read brings child.runId the moment the child
				// exists — an event-triggered read, never a poll.
				if (event.type === EventType.TASK_STARTED && isComposed(event.taskId)) rereadAndMerge();
				break;
			}
			// A task's terminal transition is the moment its LAST attempt record
			// exists — the Orker files one as every attempt ends, including the
			// one that ended the task. Re-read that task's timeline here so the
			// badge and the timeline are right the instant the card turns, not
			// only after the run's own terminal snapshot lands.
			case EventType.TASK_SUCCEEDED:
				upsertTask(event.taskId, { status: status ?? Status.SUCCEEDED });
				if (isFanOut(event.taskId) || isComposed(event.taskId)) rereadAndMerge();
				else if (!finishedAtLoad.has(event.taskId)) {
					void refreshAttempts(event.taskId);
					// A cached task's memo decision travels on its snapshot row,
					// never on the event: one event-triggered read brings it.
					if (taskDefs.get(event.taskId)?.cache !== undefined) rereadAndMerge();
				}
				break;
			case EventType.TASK_FAILED:
				upsertTask(event.taskId, {
					status: status ?? Status.FAILED,
					message: event.message
				});
				if (isFanOut(event.taskId) || isComposed(event.taskId)) rereadAndMerge();
				else if (!finishedAtLoad.has(event.taskId)) void refreshAttempts(event.taskId);
				break;
			// An awaited task started waiting. Its deadline is TASK_AWAITING's
			// own timestamp plus the definition's timeout — the engine stamps both
			// from one workflow.Now — so the event's timestamp is recorded as the
			// open instant until a snapshot brings the deadline itself.
			case EventType.TASK_AWAITING: {
				const known = findTask(event.taskId);
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
				// The failing note is about the RUN: it carries no task id, so
				// it patches no card (an empty id would become a card of its
				// own). It shows the Failing banner and starts ONE re-read for
				// the structured Run.error — never a poll; task cards keep
				// moving from their own events.
				if (isFailingNote(event)) {
					failingNote = event.message;
					void rereadOnEvent();
					break;
				}
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
	//
	// A stream that stopped for good ('failed': six reconnects in a row that
	// delivered nothing) says so, with the reason, and offers Retry. Retry
	// bumps `socketEpoch`, which restarts this effect from `lastSequence` — a
	// plain variable, read once per start, never a dependency — so the events
	// already applied are not replayed into the reducer or the buffer.
	// How long a finished run's replayed events gather before they are
	// applied (see the socket effect below).
	const REPLAY_BATCH_MS = 250;
	// The most replayed events applied in one idle slice.
	const REPLAY_SLICE = 100;
	const socketRunId = $derived(data.run?.id ?? '');
	const socketNamespace = $derived(data.namespace);
	let socketEpoch = $state(0);
	let lastSequence = 0n;
	let lastSequenceRun = '';
	$effect(() => {
		const runId = socketRunId;
		const namespace = socketNamespace;
		void socketEpoch;
		if (!runId) return;
		// A cursor belongs to one run: another run starts from its beginning.
		if (runId !== lastSequenceRun) {
			lastSequence = 0n;
			lastSequenceRun = runId;
		}
		streamFailure = '';
		// A run the page opened finished is replayed, not watched: its stream
		// arrives as fast as the socket delivers it and changes nothing a
		// reader is waiting on, so it is applied in coarser batches — at a run
		// of thousands of tasks, a frame-by-frame replay re-laid out the page
		// a hundred times before it answered a click. A live run keeps its
		// per-frame batches.
		const replayed = untrack(() => isTerminal(data.run?.status));
		// ...and a slice of it at a time, so a click is answered between
		// slices rather than after the whole replay.
		const slices = replayed ? slicedApply(applyEvents, REPLAY_SLICE, whenIdle) : undefined;
		const batch = eventBatcher(slices ? slices.apply : applyEvents, replayed ? replayBatches : undefined);
		const stop = watchRunEvents(
			runId,
			namespace,
			(event) => {
				streamDelivered = true;
				if (event.sequence > lastSequence) lastSequence = event.sequence;
				batch.push(event);
				// The run's own end is applied at once, not a frame later.
				if (isTerminalEventType(event.type)) batch.flush();
			},
			(s, detail) => {
				socketState = s;
				if (s === 'failed') streamFailure = detail ?? 'the event stream stopped';
			},
			{ fromSequence: lastSequence }
		);
		return () => {
			stop();
			// What arrived for THIS run is applied, never dropped: lastSequence
			// already counts it, so a restart would not deliver it again. A
			// page that moved on to another run drops it instead.
			if (untrack(() => socketRunId) === runId) {
				batch.flush();
				slices?.drain();
			} else {
				batch.cancel();
				slices?.cancel();
			}
		};
	});
	// whenIdle runs a replay slice when the browser has nothing better to do,
	// so a reader's click is always handled first; a browser without
	// requestIdleCallback takes the next task.
	function whenIdle(run: () => void): () => void {
		if (typeof requestIdleCallback === 'function') {
			const handle = requestIdleCallback(run, { timeout: 1000 });
			return () => cancelIdleCallback(handle);
		}
		const timer = setTimeout(run, 0);
		return () => clearTimeout(timer);
	}
	function replayBatches(flush: () => void): () => void {
		const timer = setTimeout(flush, REPLAY_BATCH_MS);
		return () => clearTimeout(timer);
	}
	function retryStream(): void {
		socketEpoch += 1;
	}

	// The event list is URL state — `?events=1`, and nothing else — independent
	// of the live subscription above. There is no containing panel to open
	// first and no second parameter gating it: the header's link IS the list's
	// state, so it can be sent to someone else and it survives a reload. What
	// each task card discloses about ITSELF is deliberately not here; that is
	// `inspectOpenByTask`, component state, and a link carries none of it.
	const eventsOpen = $derived(page.url.searchParams.get('events') === '1');
	function eventsHref(open: boolean): string {
		void shallowTick;
		const params = new URLSearchParams(typeof location === 'undefined' ? page.url.searchParams : location.search);
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
	// A task's join: the pinned definition's, else the snapshot's.
	function joinOf(id: string): JoinPolicy | undefined {
		return taskDefs.get(id)?.join || findTask(id)?.join;
	}
	// The failure handlers still to finish, from the LIVE task views: one
	// drops out of the banner the moment its own terminal event arrives.
	const failingHandlers = $derived(
		outstandingHandlers(
			tasks.map((t) => ({ id: t.id, status: t.status })),
			joinOf
		)
	);

	// A task the run ended before it reached an Orker. It is stamped with its
	// lifecycle status (CANCELED, as the wire says) and the words "never
	// started" — on its row, its DAG node and the task map's key alike — so
	// no view says a different thing about it.
	function neverStarted(task: TaskView): boolean {
		return (
			task.status !== Status.SKIPPED &&
			!taskDispatched(task.status, task.scheduledAt ?? task.startedAt) &&
			(task.status === Status.CANCELED || isTerminal(runStatus))
		);
	}

	// A task of a live run that no Orker has seen yet: what it waits for.
	function notStartedCaption(task: TaskView): string {
		const blocking = (taskDefs.get(task.id)?.needs ?? []).filter(
			(need) => !isTerminal(findTask(need)?.status)
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
		selectedTask = taskId;
		shallow(writeHref({ task: taskId, attempt: 0, item }));
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
		shallow(writeHref({ task: taskId, attempt: attempt ?? 0, item }));
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
	// `shownAt` is the element showing the same text: on an origin with no
	// clipboard it is selected, and the status says to copy it by hand.
	async function copy(value: string, label: string, shownAt: string): Promise<void> {
		if ((await copyText(value)) === 'copied') {
			copied = label;
			return;
		}
		selectByTestid(shownAt);
		copied = MANUAL_COPY_MESSAGE;
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
			await runClient(fetch).cancelRun({ runId: run.id, namespace: data.namespace, reason: cancelReason.trim() });
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
			for await (const chunk of runClient(fetch).readArtifact({
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
			const res = await runClient(fetch).listRuns({
				namespace: data.namespace,
				filters: runWindowFilters(current.orkflowName, '', '', ''),
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

	// ---------------------------------------------------------------------
	// THE SHEET: its title block, its drawing, the task a reader selected,
	// what the palette offers on this page, and the changed-run mark.
	// ---------------------------------------------------------------------

	// A shallow write moves the address bar and NOT page.url (that is what
	// makes it shallow), so every write after the first builds on the address
	// bar itself; `shallowTick` lets the one link built from it (Show events)
	// follow.
	let shallowTick = $state(0);
	function shallow(href: string): void {
		replaceState(href, page.state);
		shallowTick++;
	}
	function writeHref(next: { task?: string; attempt?: number; item?: number; view?: DrawingView }): string {
		const params = new URLSearchParams(location.search);
		params.set(NAMESPACE_PARAM, namespaceLabel(data.namespace));
		if (next.task !== undefined) {
			params.set('task', next.task);
			if (next.attempt) params.set('attempt', String(next.attempt));
			else params.delete('attempt');
			if (next.item !== undefined) params.set('item', String(next.item));
			else params.delete('item');
		}
		if (next.view !== undefined) params.set('view', next.view);
		return `/runs/${encodeURIComponent(data.runId)}?${params.toString()}`;
	}

	// The title block: what this page is, along the bottom edge. Never the
	// run id — that is this page's heading.
	setTitleBlock(() => {
		const r = data.run;
		const namespace = namespaceLabel(data.namespace);
		if (!r) return [{ label: 'PAGE', value: 'Run' }, { label: 'NAMESPACE', value: namespace }];
		const digest = r.orkflowDefinitionDigest ?? '';
		const hex = digest.includes(':') ? digest.slice(digest.indexOf(':') + 1) : digest;
		return [
			{
				label: 'ORKFLOW',
				value: r.orkflowName,
				href: withNamespace(`/orkflows/${encodeURIComponent(r.orkflowName)}?version=${encodeURIComponent(r.orkflowVersion)}`, data.namespace)
			},
			{ label: 'VERSION', value: r.orkflowVersion },
			{ label: 'DIGEST', value: hex.slice(0, 8) || '—', mono: true, title: digest || undefined, copy: digest || undefined },
			{ label: 'NAMESPACE', value: namespace },
			{ label: 'STATUS', value: statusLabel(runStatus), status: runStatus }
		];
	});

	// Did the page open on a run still going? Then a failure that arrives is
	// one the reader watched happen, and the cloud draws itself in.
	const loadedLive = untrack(() => !isTerminal(data.run?.status ?? Status.UNSPECIFIED));
	let failedWhileWatched = $state(false);

	// The drawing's view: the URL's `view=` if it names one, else the task map
	// for a run of more than DAG_DRAW_LIMIT tasks or with no readable
	// definition (the DAG needs its needs), else the DAG.
	const loadedTasks = untrack(() => Math.max(data.orkflow?.tasks.length ?? 0, data.run?.taskRuns.length ?? 0));
	const urlView = untrack(() => page.url.searchParams.get('view'));
	let view = $state<DrawingView>(
		urlView === 'dag' || urlView === 'map'
			? urlView
			: !untrack(() => data.orkflow) || loadedTasks > DAG_DRAW_LIMIT
				? 'map'
				: 'dag'
	);
	function setView(next: DrawingView): void {
		view = next;
		shallow(writeHref({ view: next }));
	}
	const drawingViews = [
		{ id: 'dag' as const, label: 'DAG', testid: 'run-view-dag' },
		{ id: 'map' as const, label: 'Task map', testid: 'run-view-map' }
	];

	// The task a reader selected — from the map, the DAG or the palette — and
	// the item of a fan-out. A deep link seeds it and does not scroll.
	let selectedTask = $state(untrack(() => page.url.searchParams.get('task') ?? ''));
	const selectedCell = $derived(
		selectedTask ? { taskId: selectedTask, item: selectedItemByTask[selectedTask] } : null
	);
	function selectTask(taskId: string, item?: number): void {
		if (item !== undefined && isFanOut(taskId)) selectItem(taskId, item);
		else {
			selectedTask = taskId;
			shallow(writeHref({ task: taskId, attempt: 0 }));
		}
		void bringIntoView(taskId);
	}
	function cardOf(taskId: string): HTMLElement | null {
		return document.querySelector(`[data-testid="task-${CSS.escape(taskId)}"]`);
	}
	async function bringIntoView(taskId: string): Promise<void> {
		const card = cardOf(taskId);
		if (!card) return;
		await scrollToCard({
			rect: () => card.getBoundingClientRect(),
			scroll: () => card.scrollIntoView({ block: 'center' }),
			viewport: () => ({
				top: document.querySelector('header')?.getBoundingClientRect().bottom ?? 0,
				bottom: document.querySelector('[data-testid="title-block"]')?.getBoundingClientRect().top ?? innerHeight
			}),
			frames: twoFrames
		});
	}
	// The selected card is marked imperatively, on two elements, so a new
	// selection never re-renders thousands of cards.
	$effect(() => {
		const id = selectedTask;
		const card = id ? cardOf(id) : null;
		card?.setAttribute('data-selected', 'true');
		return () => card?.removeAttribute('data-selected');
	});

	// The map reads the tasks only in idle time and once per `mapRevision`
	// (one per event batch, one per merged snapshot).
	let mapRevision = $state(0);
	function mapSource(): MapTask[] {
		return tasks.map((t) => ({
			id: t.id,
			status: t.status,
			needs: taskDefs.get(t.id)?.needs ?? [],
			items: t.each !== undefined || taskDefs.get(t.id)?.each !== undefined ? t.items.map((i) => ({ index: i.index, status: i.status })) : undefined
		}));
	}
	let legend = $state.raw(new Map<Status, number>());
	// Of each status the map's key counts, the cells that never started.
	const legendNever = $derived.by(() => {
		const out = new Map<Status, number>();
		const add = (s: Status) => out.set(s, (out.get(s) ?? 0) + 1);
		for (const t of tasks) {
			// A fan-out's own cell is not one the key counts; its items are.
			if (isFanOut(t.id)) {
				for (const item of t.items) if (item.status === Status.CANCELED && !item.run?.scheduledAt) add(item.status);
			} else if (neverStarted(t)) add(t.status);
		}
		return out;
	});
	let taskMap = $state<{ focusStatus: (status: Status) => void } | undefined>(undefined);

	function attemptWords(task: TaskView): string {
		if (task.attempt <= 0) return '';
		const max = taskDefs.get(task.id)?.retry?.maximumAttempts ?? 0;
		return `attempt ${task.attempt} of ${Math.max(max, 1, task.attempt)}`;
	}
	// The readout's words for a cell.
	function describeCell(taskId: string, item?: number): string {
		const task = findTask(taskId);
		if (!task) return taskId;
		if (item !== undefined) {
			const it = task.items.find((i) => i.index === item);
			return [itemLabel(taskId, item), statusLabel(it?.status), it?.run?.error?.code, it?.attempt ? `attempt ${it.attempt}` : '']
				.filter(Boolean)
				.join(' · ');
		}
		return [task.id, statusLabel(task.status), neverStarted(task) ? 'never started' : '', task.error?.code, attemptWords(task)]
			.filter(Boolean)
			.join(' · ');
	}

	// What a DAG node says on this page: its status and a line under its id.
	function nodeState(id: string): NodeState | undefined {
		const task = findTask(id);
		if (!task) return undefined;
		const dispatched = taskDispatched(task.status, task.scheduledAt ?? task.startedAt);
		if (!dispatched && task.status !== Status.SKIPPED) {
			const never = neverStarted(task);
			return { status: task.status, line: never ? 'never started' : 'waiting', neverStarted: never };
		}
		const took = isTerminal(task.status) ? formatDuration(task.startedAt ?? task.scheduledAt, task.finishedAt) : '';
		const word =
			task.status === Status.SUCCEEDED
				? 'ok'
				: task.status === Status.FAILED
					? task.error?.code || 'failed'
					: statusLabel(task.status).toLowerCase();
		return { status: task.status, line: took && took !== '—' ? `${word} · ${took}` : word };
	}

	// A failed task's tries, as every failure surface on this page states
	// them: the task's own panel and the run's cloud read the same facts.
	function triesOf(task: TaskView): FailureTries {
		return {
			retryable: task.error?.retryable ?? false,
			retryState: task.retryState,
			attempts: attemptsSeen(attemptsByTask.get(task.id) ?? [])
		};
	}

	// The run's failure, for the cloud: the first task that failed, and its try.
	const failedTask = $derived(tasks.find((t) => t.status === Status.FAILED && !isFailureHandler(joinOf(t.id))));
	const cloudTag = $derived(
		`Version ${run?.orkflowVersion ?? ''} · ${failedTask ? `${failedTask.id} failed` : 'run failed'}`
	);
	const failedImage = $derived(failedTask ? pinnedOrReference(taskDefs.get(failedTask.id)?.image) : '');
	const getCommand = $derived(run ? runCommand(data.namespace, ['run', 'get', run.id, '--json']) : '');
	const againCommand = $derived(run ? runCommand(data.namespace, ['run', 'again', run.id]) : '');

	// Watching a run is seeing it: the runs list stops holding its changed
	// mark (this browser only, $lib/heldRuns), now and on every status change
	// while the page is visible. Off the path that draws the page.
	$effect(() => {
		const id = data.run?.id;
		const status = statusLabel(runStatus);
		const orkflow = data.run?.orkflowName ?? '';
		const namespace = namespaceLabel(data.namespace);
		if (!id) return;
		const t = setTimeout(() => {
			if (document.visibilityState === 'visible') recordSeen(namespace, id, status, orkflow);
		}, 0);
		return () => clearTimeout(t);
	});

	// What this page adds to the palette. Registering builds nothing: the
	// commands are made only while the palette is open, per query, and the
	// task jumps are the best eight ids by the palette's own ranking.
	function taskJumps(query: string): Command[] {
		const q = query.trim();
		if (!q) return [];
		const best: { id: string; grade: number; span: number }[] = [];
		for (const t of tasks) {
			const m = match(t.id, q);
			if (!m) continue;
			best.push({ id: t.id, grade: m.grade, span: m.span });
			if (best.length > 64) {
				best.sort((a, b) => b.grade - a.grade || a.span - b.span);
				best.length = 8;
			}
		}
		best.sort((a, b) => b.grade - a.grade || a.span - b.span);
		return best.slice(0, 8).map((b) => ({
			id: commandId('task', b.id),
			group: 'Tasks' as const,
			label: `Jump to task ${b.id}`,
			context: statusLabel(findTask(b.id)?.status),
			keywords: b.id,
			action: { kind: 'page' as const, run: () => selectTask(b.id) }
		}));
	}
	function pageCommands(): Command[] {
		if (!run) return [];
		const out: Command[] = [];
		const nextFailed = tasks.find((t) => t.status === Status.FAILED);
		if (nextFailed)
			out.push({
				id: 'run-next-failed',
				group: 'Tasks',
				label: 'Jump to the next failed task',
				context: nextFailed.id,
				action: { kind: 'page', run: () => selectTask(nextFailed.id) }
			});
		out.push({ id: 'run-again', group: 'Actions', label: 'Run again…', action: { kind: 'page', run: () => (runAgainDialogOpen = true) } });
		if (!isTerminal(runStatus) && !cancellationRequested)
			out.push({
				id: 'run-cancel',
				group: 'Actions',
				label: 'Cancel run…',
				action: {
					kind: 'page',
					run: () => {
						cancelDialog = true;
						cancelError = '';
					}
				}
			});
		out.push({ id: 'run-compare', group: 'Actions', label: 'Compare with…', action: { kind: 'page', run: () => void openCompare() } });
		if (isTerminal(runStatus))
			out.push({ id: 'run-evidence', group: 'Actions', label: 'Open evidence…', action: { kind: 'page', run: () => (evidenceDialogOpen = true) } });
		out.push({
			id: 'run-export',
			group: 'Actions',
			label: 'Export run JSON',
			action: { kind: 'page', run: () => downloadJson(`${run.id}.json`, runExport()) }
		});
		const href = eventsHref(!eventsOpen);
		out.push({
			id: 'run-events',
			group: 'Actions',
			label: eventsOpen ? 'Hide events' : 'Show events',
			action: { kind: 'navigate', href }
		});
		out.push({ id: 'run-copy-id', group: 'Actions', label: 'Copy run id', action: { kind: 'copy', text: run.id, what: 'the run id' } });
		out.push({
			id: 'run-copy-get',
			group: 'Actions',
			label: 'Copy orkctl run get --json',
			context: 'command',
			action: { kind: 'copy', text: getCommand, what: 'the command' }
		});
		return out;
	}
	// A provider offers at most eight: the page's own commands that match
	// come first (with nothing typed, all of them — never more than eight),
	// then the best task ids.
	$effect(() =>
		registerProvider({
			id: 'run-page',
			commands: (query) => {
				const own = query.trim() ? pageCommands().filter((c) => match(c.label, query)) : pageCommands();
				return [...own, ...taskJumps(query)].slice(0, 8);
			}
		})
	);
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
	<p class="text-sm text-ink-muted" data-testid="run-loading">Loading the run…</p>
{:else if run}
	<SheetHeader title={run.id} short={shortRunId(run.id)} testid="run-id" mono>
		{#snippet meta()}
			<!--
				ONE LINE OF FACTS: the pinned Orkflow and version, what started the
				run, and its status. The origin's kind is a recorded fact, never an
				inference: a run recorded before origins existed says "unrecorded",
				not "submit". The reference is caller-supplied text and is a link
				only when it provably is an http(s) URL; the principal is the
				gateway's assertion and says so.
			-->
			<p class="flex flex-wrap items-center gap-x-2 gap-y-1 text-sm text-ink-muted" data-testid="run-origin">
				<span class="break-all"
					><a
						href={withNamespace(`/orkflows/${encodeURIComponent(run.orkflowName)}?version=${encodeURIComponent(run.orkflowVersion)}`, data.namespace)}
						data-testid="pinned-orkflow-link"
						class="link font-semibold">{run.orkflowName}</a
					><span class="font-mono text-ink-secondary">@{run.orkflowVersion}</span></span
				>
				<span aria-hidden="true">·</span>
				{#if originSlug}
					<span>origin <span data-testid="run-origin-kind" class="text-ink-secondary">{originSlug}</span></span>
					{#if origin?.id}
						{#if originHref}
							<a href={originHref} data-testid="run-origin-link" class="link font-mono text-xs">{origin.id}</a>
						{:else}
							<span data-testid="run-origin-id" class="font-mono text-xs text-ink-secondary">{origin.id}</span>
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
								class="link break-all">{origin.reference}</a
							>
						{:else}
							<span data-testid="run-origin-reference" class="font-mono text-xs break-all text-ink-secondary"
								>{origin.reference}</span
							>
						{/if}
					{/if}
					{#if origin?.principal}
						<span data-testid="run-origin-principal" title={PRINCIPAL_CAPTION}>asserted by {origin.principal}</span>
					{/if}
				{:else}
					<span>origin <span data-testid="run-origin-kind" class="italic" title={UNRECORDED_CAPTION}>{UNRECORDED}</span></span>
				{/if}
				<span aria-hidden="true">·</span>
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
						class="stamp border-rule-strong bg-paper text-ink-secondary"
					>
						Retries: {runRetries}
					</span>
				{/if}
				<!--
					LIVE, while the run is: whether this page is still receiving its
					events. A finished run says nothing here — its status already
					says it ended.
				-->
				{#if !isTerminal(runStatus)}
					<span
						data-testid="live-badge"
						class="stamp {socketState === 'open' ? 'border-green bg-green-tint text-green-strong' : 'border-red bg-paper text-red-strong'}"
					>
						<span
							aria-hidden="true"
							class="size-1.5 rounded-full {socketState === 'open'
								? 'bg-green animate-live-dot motion-reduce:animate-none'
								: 'border border-red'}"
						></span>
						{socketState === 'open'
							? 'LIVE'
							: socketState === 'failed'
								? 'STOPPED'
								: streamBroken
									? 'NOT CONNECTED'
									: 'CONNECTING'}
					</span>
				{/if}
				<button
					type="button"
					data-testid="run-stability-toggle"
					aria-pressed={historyOpen}
					onclick={toggleHistory}
					class="btn btn-sm max-lg:hidden {historyOpen ? 'border-blue bg-blue-tint text-blue-strong' : 'text-ink-secondary'}"
				>Retry history ({HISTORY_WINDOW})</button>
				{#if historyOpen}
					<CliEquivalent
						command={flakesCli(data.namespace, '168h', [], run.orkflowName)}
						testid="run-stability-cli"
					/>
					{#if historyLoading}
						<span class="text-xs text-ink-muted" data-testid="run-stability-loading">loading…</span>
					{:else if historyError}
						<span class="text-xs text-failed-fg" data-testid="run-stability-error"
							>retry history unavailable: {historyError}</span
						>
					{:else if history}
						<span class="text-xs text-ink-muted" data-testid="run-stability-note">{historyNote}</span>
					{/if}
				{/if}
			</p>
		{/snippet}
		{#snippet actions()}
			<!--
				From 1024 px the secondary actions are buttons; below it they fold
				into one "More" beside Run again, so the drawing starts within the
				first screen of a phone. The buttons stay in the page either way
				(hidden below 1024 px), and the list's items carry their own ids.
			-->
			{#if isTerminal(runStatus)}
				<!-- The engine exports evidence for finished runs only, so a live
				     run is not offered it. -->
				<button type="button" data-testid="run-evidence" onclick={() => (evidenceDialogOpen = true)} class="btn max-lg:hidden">Evidence</button>
			{/if}
			{#if composesAny}
				<a href={childrenListHref(data.namespace, run.id)} data-testid="run-children-link" class="btn">Child runs</a>
				<CliEquivalent
					command={runCommand(data.namespace, ['run', 'list', '--filter', 'origin.kind=PARENT_RUN', '--filter', `origin.id=${run.id}`])}
					testid="run-children-cli"
					label="Copy the command that lists this run's child runs"
				/>
			{/if}
			<button type="button" data-testid="compare-run" onclick={() => void openCompare()} class="btn max-lg:hidden">Compare</button>
			<button type="button" data-testid="export-run-json" onclick={() => downloadJson(`${run.id}.json`, runExport())} class="btn max-lg:hidden"
				>Export JSON</button
			>
			<a
				href={eventsHref(!eventsOpen)}
				data-testid="events-toggle"
				data-sveltekit-noscroll
				aria-expanded={eventsOpen}
				aria-controls="run-events"
				class="btn max-lg:hidden">{eventsOpen ? 'Hide events' : 'Show events'}</a
			>
			<span class="max-lg:hidden"
				><CliEquivalent command={runCommand(data.namespace, ['run', 'get', run.id])} testid="run-inspect-cli" label="Copy CLI" /></span
			>
			<MoreMenu testid="run-more" class="lg:hidden">
				{#snippet items(close)}
					{#if isTerminal(runStatus)}
						<button type="button" data-testid="run-more-evidence" onclick={() => { close(); evidenceDialogOpen = true; }}>Evidence</button>
					{/if}
					<button type="button" data-testid="run-more-compare" onclick={() => { close(); void openCompare(); }}>Compare</button>
					<button type="button" data-testid="run-more-export" onclick={() => { close(); downloadJson(`${run.id}.json`, runExport()); }}>Export JSON</button>
					<a href={eventsHref(!eventsOpen)} data-testid="run-more-events" data-sveltekit-noscroll onclick={close}>{eventsOpen ? 'Hide events' : 'Show events'}</a>
					<button type="button" data-testid="run-more-stability" aria-pressed={historyOpen} onclick={() => { close(); toggleHistory(); }}
						>Retry history ({HISTORY_WINDOW})</button
					>
					<button type="button" data-testid="run-more-copy-get" onclick={() => { close(); void copy(getCommand, 'Command copied', 'run-reproduce-get'); }}
						>Copy orkctl run get</button
					>
				{/snippet}
			</MoreMenu>
			{#if !isTerminal(runStatus)}
				<button
					type="button"
					data-testid="cancel-run"
					disabled={cancellationRequested}
					onclick={() => {
						cancelDialog = true;
						cancelError = '';
					}}
					class="btn border-red text-red-strong hover:bg-red-tint">{cancellationRequested ? 'Cancellation requested' : 'Cancel run'}</button
				>
			{/if}
			<button
				type="button"
				data-testid="run-again"
				onclick={() => (runAgainDialogOpen = true)}
				class="btn {isTerminal(runStatus) ? 'btn-primary' : ''}">Run again</button
			>
		{/snippet}
	</SheetHeader>
	{#if copied}
		<p data-testid="copy-status" role="status" class="-mt-4 mb-4 text-xs text-ink-muted">{copied}</p>
	{/if}
	{#if socketState === 'failed' && !isTerminal(runStatus)}
		<div class="mb-4">
			<ErrorPanel
				testid="live-stopped"
				retryTestid="live-retry"
				retryLabel="Retry live updates"
				message="Live updates stopped: {streamFailure}. The statuses on this page are as of the last event received."
				onretry={retryStream}
			/>
		</div>
	{/if}
	{#if cancellationRequested && !isTerminal(runStatus)}
		<p data-testid="cancellation-requested" class="note mb-4 flex gap-3 px-3 py-2 text-sm">
			<span class="caps text-ink-muted">Note</span>
			<span>Cancellation requested. Waiting for the run to stop.</span>
		</p>
	{/if}
	{#if runFailing(runStatus, runError, failingNote)}
		<!--
			A failing run: its outcome is decided FAILED and its failure
			handlers still run. Keyed on the LIVE status, so the terminal event
			— RUN_FAILED, or a terminated run's RUN_CANCELED — clears it.
		-->
		<p
			data-testid="run-failing"
			data-handlers={failingHandlers.map((h) => h.id).join(',')}
			role="status"
			class="severity-warning mb-4 border border-red px-3 py-2 text-sm"
		>
			{failingBannerText(failingHandlers)}
		</p>
	{/if}

	<!--
		THE DRAWING beside its aside: the run's tasks as a DAG or a task map,
		and what a reader asks next — which version this is, why it failed, on
		which image, and the command that reproduces it. Below 1280 px the
		aside follows the drawing.
	-->
	<div class="mb-6 grid gap-6 xl:grid-cols-[minmax(0,1fr)_var(--spacing-aside)]">
		<div class="min-w-0">
			<DrawingFrame
				testid="run-drawing"
				caption="{run.orkflowName} @ {run.orkflowVersion} · {tasks.length} {tasks.length === 1 ? 'task' : 'tasks'}"
				views={drawingViews}
				{view}
				onview={setView}
				tasks={data.orkflow?.tasks ?? null}
				unavailable={data.orkflowError ?? 'the definition was not found'}
				orkflowLabel="{run.orkflowName}@{run.orkflowVersion}"
				on="run"
				dagLabel="Task DAG of this run"
				stateOf={nodeState}
				selected={selectedTask}
				hrefOf={(id) => taskHref(id, 0)}
				onselect={(id) => selectTask(id)}
				neighbourhoodWhenLarge
			>
				{#snippet map()}
					<TaskMap
						bind:this={taskMap}
						source={mapSource}
						definition={data.orkflow !== null}
						revision={mapRevision}
						selected={selectedCell}
						onselect={selectTask}
						describe={describeCell}
						onlegend={(counts) => (legend = counts)}
					/>
				{/snippet}
			</DrawingFrame>
			<div class="mt-2 min-h-12 sm:min-h-6">
				{#if view === 'map'}
					<TaskMapLegend counts={legend} neverStarted={legendNever} onpick={(status) => taskMap?.focusStatus(status)} />
				{:else}
					<p class="text-xs text-ink-muted">A dashed outline is a failure handler; a faded node never started.</p>
				{/if}
			</div>
		</div>
		<aside class="min-w-0 space-y-6" aria-label="About this run">
			<VersionTable namespace={data.namespace} orkflowName={run.orkflowName} runVersion={run.orkflowVersion} />
			{#if runError || runErrorMessage}
				<!--
					The run's terminal error. The snapshot's Error carries the §8.1
					category, the §8.2 reason code and the retryable flag, so when it
					is present it is rendered in full rather than as a bare sentence.
					A run whose failure arrived only over the socket has a message
					and no category, and says exactly that much.
				-->
				{#if runStatus === Status.FAILED}
					<RevisionCloud tag={cloudTag} attempts={failedTask ? attemptWords(failedTask) : ''} animate={failedWhileWatched}>
						{#if runError}
							<FailurePanel error={runError} testid="run-error" bare tries={failedTask ? triesOf(failedTask) : undefined} />
						{:else}
							{@const plain = plainFailure(runErrorMessage)}
							<div data-testid="run-error" role="alert" class="text-sm text-ink">
								{#if plain.code}<p class="severity-error font-mono font-semibold">{plain.code}</p>{/if}
								<p class="mt-1.5 max-w-prose break-words">{plain.text}</p>
							</div>
						{/if}
					</RevisionCloud>
				{:else if runError}
					<!-- A run that ended without failing (canceled): its reason in ink, a NOTE — never a failure's cloud or red. -->
					<FailurePanel error={runError} testid="run-error" tone="note" />
				{:else}
					<div data-testid="run-error" role="status" class="note px-3 py-2 text-sm text-ink">{plainFailure(runErrorMessage).text}</div>
				{/if}
			{/if}
			<!--
				WHEN, AND HOW FAR: the run's own clock and its tasks, as a register
				of the aside rather than a second band across the page. Elapsed is
				start to finish (the runs register's DURATION starts at submission).
			-->
			<section aria-labelledby="run-timing-heading">
				<h2 id="run-timing-heading" class="caps mb-1 text-ink-muted">Timing</h2>
				<dl class="grid grid-cols-[auto_minmax(0,1fr)] border-t border-rule text-sm" aria-label="Timing and tasks">
					{#each [{ label: 'Created', at: run.createdAt }, { label: 'Started', at: startedAt ?? run.startedAt }, { label: 'Finished', at: finishedAt ?? run.finishedAt }] as row (row.label)}
						<dt class="caps border-b border-rule py-1.5 pr-4 text-ink-muted">{row.label}</dt>
						<dd class="figure border-b border-rule py-1.5 text-xs text-ink-secondary">{fmtTimestamp(row.at)}</dd>
					{/each}
					<dt class="caps border-b border-rule py-1.5 pr-4 text-ink-muted">Elapsed</dt>
					<dd class="figure border-b border-rule py-1.5 text-ink" data-testid="run-elapsed">{formatDuration(startedAt ?? run.startedAt, finishedAt ?? run.finishedAt ?? (isTerminal(runStatus) ? undefined : currentTimestamp))}</dd>
					<dt class="caps border-b border-rule py-1.5 pr-4 text-ink-muted">Tasks</dt>
					<dd class="border-b border-rule py-1.5 text-ink" data-testid="task-summary">
						{tasks.filter((t) => t.status === Status.SUCCEEDED).length}/{tasks.length} succeeded{#if tasks.some((t) => t.status === Status.AWAITING)}, {tasks.filter((t) => t.status === Status.AWAITING).length} awaiting{/if}{#if cachedTotal > 0}<span data-testid="task-summary-cached"> · {cachedTotal} cached</span>{/if}
					</dd>
				</dl>
			</section>
			{#if failedImage}
				<section aria-labelledby="run-image-heading">
					<h2 id="run-image-heading" class="caps mb-1 text-ink-muted">Image</h2>
					<p class="font-mono text-xs break-all text-ink" data-testid="run-failed-image">{failedImage}</p>
				</section>
			{/if}
			<section aria-labelledby="run-reproduce-heading">
				<h2 id="run-reproduce-heading" class="caps mb-1 text-ink-muted">Reproduce</h2>
				<ul class="space-y-1.5">
					{#each [{ key: 'get', command: getCommand, what: 'Read this run' }, { key: 'again', command: againCommand, what: 'Run it again' }] as line (line.key)}
						<li class="flex items-start gap-2">
							<code class="min-w-0 flex-1 overflow-x-auto font-mono text-xs text-ink" data-testid="run-reproduce-{line.key}"
									>{#each commandWords(line.command) as word, i (i)}{#if i > 0}{' '}{/if}<span class="whitespace-nowrap">{word}</span>{/each}</code
								>
							<button
								type="button"
								class="btn-icon -mt-1.5 inline-flex size-7 shrink-0 items-center justify-center"
								aria-label="Copy: {line.what}"
								title="Copy: {line.what}"
								onclick={() => void copy(line.command, 'Command copied', `run-reproduce-${line.key}`)}><Icon name="copy" /></button
							>
						</li>
					{/each}
				</ul>
			</section>
		</aside>
	</div>

	<!--
		THE RUN'S VALUES. A value is shown in its own block; what a run has
		none of (no input, no aggregated output yet or at all) is said once, in
		one NOTE, rather than as an empty box beside a sentence in another.
	-->
	<section class="mb-6 space-y-4" aria-label="Run values">
		{#if hasRunInput || run.output}
			<div class="grid gap-6 lg:grid-cols-2">
				{#if hasRunInput}
					<div class="min-w-0">
						<div class="flex items-center justify-between gap-3">
							<h2 class="caps text-ink-muted">Inputs</h2>
							<button type="button" data-testid="copy-run-input" onclick={() => void copy(payloadJson(run.input), 'Inputs copied', 'run-input')} class="link text-xs">Copy JSON</button>
						</div>
						<pre data-testid="run-input" class="mt-1.5 max-h-56 overflow-auto rounded-sm border border-rule-faint bg-paper-sunken p-3 font-mono text-xs text-ink-secondary">{payloadJson(run.input)}</pre>
					</div>
				{/if}
				{#if run.output}
					<div class="min-w-0">
						<div class="flex items-center justify-between gap-3">
							<h2 class="caps text-ink-muted" data-testid="run-output-kind">
								{declaresOutputs ? 'Declared outputs' : 'Leaf task values'}
							</h2>
							<button type="button" data-testid="copy-run-output" onclick={() => void copy(payloadJson(run.output), 'Run output copied', 'run-output')} class="link text-xs">Copy JSON</button>
						</div>
						<pre data-testid="run-output" class="mt-1.5 max-h-56 overflow-auto rounded-sm border border-rule-faint bg-paper-sunken p-3 font-mono text-xs text-ink-secondary">{payloadJson(run.output)}</pre>
					</div>
				{/if}
			</div>
		{/if}
		{#if !hasRunInput || !run.output}
			<p data-testid="run-values-note" class="note flex max-w-prose gap-3 px-3 py-2 text-sm">
				<span class="caps text-ink-muted">Note</span>
				<span>
					{#if !hasRunInput}<span data-testid="run-input">This run took no input values.</span>{/if}
					{#if !run.output}<span data-testid="run-output"
							>{isTerminal(runStatus)
								? `It finished without ${declaresOutputs ? 'its declared outputs' : 'aggregated output values'}.`
								: `Its ${declaresOutputs ? 'declared outputs are' : 'output is'} not available yet.`}</span
						>{/if}
				</span>
			</p>
		{/if}
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
		<section class="region pt-4" aria-labelledby="run-tasks-heading">
			<h2 id="run-tasks-heading" class="caps mb-3 text-ink-muted">Tasks</h2>
			{#if data.artifactsError}
				<p data-testid="artifacts-unavailable" class="note mb-3 flex gap-3 px-3 py-2 text-xs">
					<span class="caps text-ink-muted">Note</span>
					<span>Artifact metadata is unavailable: {data.artifactsError}. Downloads require the engine's recorded-artifact read API and are never linked directly to storage URIs.</span>
				</p>
			{/if}
			<!--
				The attempt store is not the run: a deployment that has none, or
				one whose store is unreachable, still renders every status, every
				log and every artifact on this page. Say what is missing and what
				it costs — nothing else changes.
			-->
			{#if attemptsError}
				<p data-testid="attempts-unavailable" class="note mb-3 flex gap-3 px-3 py-2 text-xs">
					<span class="caps text-ink-muted">Note</span>
					<span>Attempt history is not available: {attemptsError}. Task statuses, output and artifacts
					are unaffected; what each earlier try of a retried task did cannot be shown.</span>
				</p>
			{/if}
			{#if artifactDownloadError}
				<p data-testid="artifact-download-error" role="alert" class="severity-error mb-3 border border-red px-3 py-2 text-xs">Error: artifact download failed: {artifactDownloadError}</p>
			{/if}
			<!-- One hairline-ruled row per task, on clean ground: no box round a row. -->
			<div class="border-t border-rule" data-testid="task-list">
				{#each tasks as task (task.id)}
					{@const dispatched = taskDispatched(task.status, task.scheduledAt ?? task.startedAt)}
					{@const taskAttempts = attemptsByTask.get(task.id) ?? []}
					{@const isAwait = task.await !== undefined || taskDefs.get(task.id)?.await !== undefined}
					{@const timing = taskTiming({ ...task, awaited: isAwait }, attemptStoreAbsent ? undefined : taskAttempts, currentTimestamp)}
					{@const accent = pinAccent(task.pin)}
					{@const fanOut = task.each !== undefined || taskDefs.get(task.id)?.each !== undefined}
					{@const composed = task.child !== undefined || taskDefs.get(task.id)?.orkflow !== undefined}
					<!--
						A pinned task's row is the one row with an outline: its pin's
						line, 1 px all round, and a faint fill, so the members of one
						pin read as a block in the list without being re-sorted — the
						DAG order is the order, and regrouping the list would hide it.

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
					<!--
						content-visibility: auto lets the browser skip laying out and
						painting a card while it is off screen, and the intrinsic size
						keeps the list's height (and its scrollbar) honest meanwhile:
						on a run of thousands of tasks every scroll and focus otherwise
						re-laid out every card, seconds before the page answered. It
						also contains the card's painting, so a card that can hold an
						overlay — an await's resolve and reject dialogs, a composed
						task's panel, an open Inspect with its command popover — is
						never skipped, and its overlay is never clipped to it.
					-->
					<div
						class="task-card px-3 py-3 {isAwait || composed || inspectOpenByTask[task.id]
							? ''
							: '[content-visibility:auto] [contain-intrinsic-size:auto_5rem]'} {accent
							? 'relative -mt-px rounded-sm border'
							: 'border-b border-rule'}"
						style={accent ? `border-color:var(--color-${accent}-line);background-color:var(--color-${accent}-fill)` : undefined}
						data-testid="task-{task.id}"
						data-dispatched={dispatched}
						data-pin={task.pin || undefined}
						data-pin-accent={accent}
					>
						<div class="flex items-center justify-between gap-4">
							<div class="min-w-0">
								<a href={taskHref(task.id, task.attempt)} class="font-semibold break-all text-ink hover:text-blue hover:underline" data-testid="task-deep-link-{task.id}">{task.id}</a>
								{#if task.message && !task.error}
									<p class="text-xs break-words text-ink-secondary">{task.message}</p>
								{/if}
								<!-- Short values are a fact of the row, said on it in the mono. -->
								{#if task.output && inlinePayload(task.output) !== null}
									<p class="mt-0.5 flex min-w-0 items-baseline gap-2 text-xs">
										<span class="caps text-ink-muted">Values</span>
										<code data-testid="task-values-{task.id}" class="font-mono break-all text-ink-secondary">{inlinePayload(task.output)}</code>
									</p>
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
								{#if task.join}
									<span
										data-testid="task-{task.id}-join"
										title={joinTitle(task.join, taskDefs.get(task.id)?.needs ?? [])}
										class="inline-flex items-center rounded-sm border border-rule-strong px-1.5 font-mono text-2xs leading-4 whitespace-nowrap text-ink-secondary"
									>join {joinWord(task.join)}</span>
								{/if}
								{#if dispatched || task.status === Status.SKIPPED}
									<StatusPill status={task.status} testid="task-{task.id}-status" />
								{:else if neverStarted(task)}
									<span class="inline-flex flex-col items-end gap-0.5">
										<StatusPill status={task.status} testid="task-{task.id}-status" />
										<span data-testid="task-{task.id}-status-note" class="text-2xs whitespace-nowrap text-ink-muted">never started</span>
									</span>
								{:else}
									<span
										data-testid="task-{task.id}-status"
										data-state="not-started"
										class="text-2xs whitespace-nowrap text-ink-muted"
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
											class="stamp border-rule-strong bg-paper text-ink-secondary"
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
								{#if !fanOut}
									<CacheBadge
										cache={task.cache}
										declared={taskDefs.get(task.id)?.cache !== undefined}
										succeeded={task.status === Status.SUCCEEDED && task.factsRead === true}
										namespace={data.namespace}
										testid="task-{task.id}-cache"
									/>
								{/if}
							</div>
						</div>
						<div class="mt-2 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-ink-muted">
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
								<TimingBar
									taskId={task.id}
									{timing}
									placement={timelinePlacement(runSpan, {
										scheduled: timestampMs(task.scheduledAt),
										started: timestampMs(task.startedAt),
										finished: timestampMs(task.finishedAt)
									})}
								/>
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
										class={line.stale ? 'font-medium text-ink-secondary' : ''}>{line.text}</span
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
							{#if !isAwait && !fanOut && !composed && !neverStarted(task)}
							<CliEquivalent
								command={runCommand(data.namespace, ['run', 'logs', run.id, '--task', task.id, ...(task.attempt > 0 ? ['--attempt', String(task.attempt)] : [])])}
								testid="run-task-{task.id}-logs-cli"
								label="Copy logs CLI"
								text="run logs"
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
									text="task run"
								/>
							{/if}
							{/if}
							{#if task.cache && task.cache.outcome !== CacheOutcome.HIT}
								<!--
									Where a miss's key components are: the attempt
									records, which say which input changed.
								-->
								<CliEquivalent
									command={runCommand(data.namespace, ['run', 'attempts', run.id, '--task', task.id])}
									testid="task-{task.id}-cache-cli"
									label="Copy the command that lists this task's attempts and their cache key components"
									text="run attempts"
								/>
							{/if}
							{#if task.status === Status.RUNNING}
								<CliEquivalent
									command={runCommand(data.namespace, ['run', 'inspect', run.id, '--task', task.id])}
									testid="run-task-{task.id}-inspect-cli"
									label="Copy inspect CLI"
									text="run inspect"
								/>
							{/if}
							{#if timing.scheduled}
								<CliEquivalent
									command={runCommand(data.namespace, ['run', 'timing', run.id, '--task', task.id])}
									testid="run-task-{task.id}-timing-cli"
									label="Copy timing CLI"
									text="run timing"
								/>
							{/if}
							<!-- The row's own actions, inline: its output and what ran. -->
							<span class="ml-auto flex flex-wrap items-center gap-2">
								{#if !isAwait && !composed && !fanOut}
									<button
										type="button"
										data-testid="task-log-toggle-{task.id}"
										aria-expanded={logOpenByTask[task.id] ?? false}
										onclick={() => (logOpenByTask[task.id] = !logOpenByTask[task.id])}
										class="btn btn-sm btn-disclosure"
									>
										{logOpenByTask[task.id] ? 'Hide output' : 'Show output'}
									</button>
								{/if}
								<button
									type="button"
									data-testid="task-inspect-toggle-{task.id}"
									aria-expanded={inspectOpenByTask[task.id] ?? false}
									onclick={() => (inspectOpenByTask[task.id] = !inspectOpenByTask[task.id])}
									class="btn btn-sm btn-disclosure"
								>
									{inspectOpenByTask[task.id] ? 'Hide inspect' : 'Inspect'}
								</button>
							</span>
						</div>

						{#if task.error}
							<div class="mt-3">
								<FailurePanel
									error={task.error}
									testid="task-{task.id}-error"
									compact
									tries={triesOf(task)}
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
						{:else if composed}
							<!--
								A composed task starts a child run: no log pane, no image.
								The panel names the pinned child, links its run, and rolls
								its tasks up live from the child run's own stream.
							-->
							<ChildRunPanel
								runId={run.id}
								namespace={data.namespace}
								taskId={task.id}
								child={task.child}
								status={task.status}
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
							toggle={false}
							bind:open={() => logOpenByTask[task.id] ?? false, (open) => (logOpenByTask[task.id] = open)}
						/>
						{/if}
						{#if inspectOpenByTask[task.id]}
							{@const def = taskDefs.get(task.id)}
							{@const taskArtifacts = (artifactsByTask.get(task.id) ?? []).filter((record) => record.item === undefined)}
							<div
								class="mt-3 rounded-sm bg-paper-sunken p-3"
								data-testid="task-{task.id}-inspect"
							>
								<p class="text-xs break-all text-ink-muted" data-testid="task-{task.id}-activity">{task.activity || 'activity not yet known'}</p>
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
								{#if composed}
									<dl
										class="mt-3 grid grid-cols-1 gap-x-6 gap-y-2 border-t border-rule-faint pt-3 text-xs"
										data-testid="task-{task.id}-detail"
									>
										<div>
											<dt class="caps text-ink-muted">Child Orkflow</dt>
											<dd class="mt-0.5 font-mono break-all text-ink-secondary" data-testid="task-{task.id}-child-ref">
												{childLabel(task.child)}
											</dd>
										</div>
										<div>
											<dt class="caps text-ink-muted">Definition digest</dt>
											<dd class="mt-0.5 font-mono break-all text-ink-secondary" data-testid="task-{task.id}-child-digest">
												{task.child?.orkflowDefinitionDigest || '—'}
											</dd>
										</div>
									</dl>
								{:else if !isAwait && (def || task.exitCode !== undefined || task.commit)}
									<dl
										class="mt-3 grid grid-cols-1 gap-x-6 gap-y-2 border-t border-rule-faint pt-3 text-xs sm:grid-cols-2"
										data-testid="task-{task.id}-detail"
									>
										{#if def?.image}
											<div class="sm:col-span-2">
												<dt class="caps text-ink-muted">Image</dt>
												<dd
													class="mt-0.5 font-mono break-all text-ink-secondary"
													data-testid="task-{task.id}-image"
												>
													{pinnedOrReference(def.image) || '—'}
												</dd>
												{#if def.image.indexDigest}
													<dd class="mt-0.5">
														<span
															class="font-mono text-blue"
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
												<dt class="caps text-ink-muted">Command</dt>
												<dd
													class="mt-0.5 font-mono break-all text-ink-secondary"
													data-testid="task-{task.id}-command"
												>
													{JSON.stringify(def.command)}
												</dd>
											</div>
										{/if}
										{#if def && Object.keys(def.env).length > 0}
											<div class="sm:col-span-2">
												<dt class="caps text-ink-muted">Environment</dt>
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
													class="mt-0.5 font-mono break-all text-ink-secondary"
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
												<dt class="caps text-ink-muted">Secrets</dt>
												<dd
													class="mt-0.5 font-mono text-ink-secondary"
													title="names only — a secret's value is resolved on the Orker and never reaches this page"
													data-testid="task-{task.id}-secrets"
												>
													{def.secrets.map((s) => s.name).join(', ')}
												</dd>
											</div>
										{/if}
										{#if def?.cache}
											<div>
												<dt class="caps text-ink-muted">Cache</dt>
												<dd class="mt-0.5 text-ink-secondary" data-testid="task-{task.id}-cache-spec">
													{cacheSpecLabel(def.cache)}
												</dd>
											</div>
										{/if}
										{#if task.cache}
											<div class="sm:col-span-2" data-testid="task-{task.id}-cache-detail">
												<dt class="caps text-ink-muted">Cache decision</dt>
												<dd class="mt-0.5 text-ink-secondary">{cacheOutcomeLabel(task.cache.outcome)}</dd>
												{#if task.cache.key}
													<dd class="mt-0.5 font-mono break-all text-ink-muted" data-testid="task-{task.id}-cache-key">
														{task.cache.key}
													</dd>
												{/if}
												{#if task.cache.outcome === CacheOutcome.HIT}
													<dd class="mt-0.5">
														Reused the result of run
														<a
															class="link font-mono"
															href={cacheSourceHref(data.namespace, task.cache.source)}
															data-testid="task-{task.id}-cache-detail-source">{task.cache.source?.runId}</a
														>, produced {fmtTimestamp(task.cache.source?.finishedAt)}{#if sourceAttemptDuration(task.cache.source)}
															(its attempt took {sourceAttemptDuration(task.cache.source)}){/if}.
													</dd>
													<dd
														class="mt-0.5 font-mono break-all text-ink-muted"
														data-testid="task-{task.id}-cache-source-commit"
													>
														{task.cache.source?.commitUri}
													</dd>
													<dd class="mt-0.5 text-ink-muted">
														stdout.log and stderr.log below are the producing run's; this
														attempt's own log is the cache line in the log pane.
													</dd>
												{:else}
													{#if task.cache.outcome === CacheOutcome.MISS}
														<dd class="mt-0.5" data-testid="task-{task.id}-cache-reason">
															{missReasonSentence(task.cache.missReason, task.cache.missDetail)}
														</dd>
													{/if}
													{#if memoWriteSentence(task.cache)}
														<dd class="mt-0.5 text-ink-muted" data-testid="task-{task.id}-cache-memo">
															{memoWriteSentence(task.cache)}
														</dd>
													{/if}
												{/if}
											</div>
										{/if}
										<div>
											<dt class="caps text-ink-muted">Exit code</dt>
											<dd
												class="mt-0.5 font-mono {task.exitCode === undefined
													? 'text-ink-muted'
													: task.exitCode === 0
														? 'text-ink-secondary'
														: 'text-failed-fg'}"
												data-state={task.exitCode === undefined ? 'unknown' : 'measured'}
												data-testid="task-{task.id}-exit-code"
											>
												{task.exitCode === undefined ? 'Not measured' : task.exitCode}
											</dd>
										</div>
										{#if task.commit?.uri}
											<div class="sm:col-span-2">
												<dt class="caps text-ink-muted">Commit</dt>
												<dd
													class="mt-0.5 font-mono break-all text-ink-muted"
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
								{#if !fanOut && attemptsSeeded && (!attemptsError || taskAttempts.length > 0)}
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
									<div class="mt-3 border-t border-rule-faint pt-3" data-testid="task-artifacts-{task.id}">
										<h4 class="caps text-ink-muted">Artifacts</h4>
										{#each taskArtifacts as record (record.kind + ':' + (record.artifact?.name ?? ''))}
											{#if record.artifact}
												{@const key = `${record.taskId}:${record.kind}:${record.artifact.name}`}
												<div class="flex flex-wrap items-center gap-x-3 gap-y-1 border-b border-rule-faint py-2 text-xs last:border-b-0">
													<span class="font-medium text-ink">{artifactKindLabel(record.kind)}</span>
													<span class="font-mono break-all text-ink-secondary">{record.artifact.name}</span>
													<span class="text-ink-muted">{record.artifact.contentType || 'unknown type'} · {record.artifact.sizeBytes.toString()} bytes</span>
													<button type="button" data-testid="download-artifact-{task.id}-{record.artifact.name}" onclick={() => downloadArtifact(record)} disabled={downloadingArtifact === key} class="link ml-auto font-medium disabled:opacity-50">{downloadingArtifact === key ? 'Downloading…' : 'Download'}</button>
													<ArtifactProvenance runId={run.id} namespace={data.namespace} taskId={task.id} kind={record.kind} name={record.artifact.name} />
													<details class="w-full min-w-0 text-ink-muted"><summary>Metadata</summary><pre class="mt-1 overflow-auto font-mono">{JSON.stringify(artifactMetadata(record.artifact), null, 2)}</pre></details>
												</div>
											{/if}
										{/each}
									</div>
								{/if}
							</div>
						{/if}
						{#if (task.output && inlinePayload(task.output) === null) || (!task.output && task.valuesRef)}
							<div class="mt-3 rounded-sm bg-paper-sunken p-3">
								<div class="flex items-center justify-between gap-3">
									<span class="text-xs font-medium text-ink">Task values</span>
									{#if task.output}<button type="button" data-testid="copy-task-values-{task.id}" onclick={() => void copy(payloadJson(task.output), 'Task values copied', `task-values-${task.id}`)} class="link text-xs">Copy JSON</button>{/if}
								</div>
								<pre data-testid="task-values-{task.id}" class="mt-2 max-h-40 overflow-auto font-mono text-xs text-ink-secondary">{task.output ? payloadJson(task.output) : isTerminal(task.status) ? 'This task finished without inline values. Its recorded values artifact, if present, is listed under Inspect.' : 'Task values are not available yet.'}</pre>
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
					<label class="mt-3 block text-sm font-medium text-ink" for="cancel-reason">Reason <span class="font-normal text-ink-muted">(optional)</span></label>
					<textarea id="cancel-reason" data-testid="cancel-reason" bind:value={cancelReason} rows="3" class="field mt-1 w-full px-3 py-2 text-sm" placeholder="Why should this run stop?"></textarea>
					<!--
						Deliberately NOT a CliEquivalent popover. This is a live preview of
						the exact destructive command this dialog is about to run, and it
						stays visible while the reason field is typed into. Styled to match
						the popover's command block; not hidden behind its icon.
					-->
					<p class="mt-3 overflow-x-auto rounded-sm bg-paper-sunken p-2 font-mono text-xs text-ink-muted">{runCommand(data.namespace, ['run', 'cancel', run.id, ...(cancelReason.trim() ? ['--reason', cancelReason.trim()] : [])])}</p>
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
						<label for="compare-target" class="text-xs font-medium text-ink-muted"
							>A recent run of {run.orkflowName}</label
						>
						{#if compareLoading}
							<p class="text-sm text-ink-muted" data-testid="compare-loading">Loading recent runs…</p>
						{:else if compareError}
							<ErrorPanel
								testid="compare-error"
								retryTestid="compare-retry"
								subject="recent runs of {run.orkflowName}"
								message={compareError}
								onretry={() => void openCompare()}
							/>
						{:else if compareCandidates.length === 0}
							<p class="text-sm text-ink-muted" data-testid="compare-no-candidates">
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
						<label for="compare-target-id" class="text-xs font-medium text-ink-muted"
							>Or any run id (of any Orkflow)</label
						>
						<input
							id="compare-target-id"
							data-testid="compare-target-id"
							type="text"
							bind:value={comparePasted}
							placeholder="ork-…"
							autocomplete="off"
							spellcheck="false"
							class="field h-9 px-3 font-mono text-sm"
						/>
					</div>
					<div class="flex justify-end">
						<button
							type="button"
							data-testid="compare-open"
							disabled={compareTarget === ''}
							onclick={openComparison}
							class="btn btn-primary"
						>Open comparison</button>
					</div>
				</div>
			{/snippet}
		</Dialog>
	{/if}
{/if}
