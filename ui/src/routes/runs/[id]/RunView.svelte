<script lang="ts">
	import { shortVersion, versionTip } from '#lib/version.ts';
	import { copyText, MANUAL_COPY_MESSAGE, selectByTestid } from '#lib/clipboard.ts';
	import { pollWhileVisible } from '#lib/visibility.ts';
	import { rememberRecent } from '#lib/palette/recent.ts';
	import { onDestroy, tick, untrack } from 'svelte';
	import { goto, invalidateAll, replaceState } from '$app/navigation';
	import { navigating, page } from '$app/state';
	import { chainListHref, childLabel, childrenListHref } from '#lib/compose.ts';
	import ItemCard from '#lib/components/ItemCard.svelte';
	import SeenBefore from '#lib/components/SeenBefore.svelte';
	import SinceLastRun from '#lib/components/SinceLastRun.svelte';
	import WaitsFor from '#lib/components/WaitsFor.svelte';
	import { taskTimelineBars, timelineAxis } from '#lib/taskTimeline.ts';
	import { reasonWord } from '#lib/taskReason.ts';
	import OutputsMark from '#lib/components/OutputsMark.svelte';
	import { rowOrkers } from '#lib/taskOrker.ts';
	import { activityClient } from '#lib/clients/activities.ts';
	import { catalogName, loadActivities } from '#lib/activityMeta.ts';
	import type { ActivityInfo } from '#lib/gen/orkstack/v1/activity_service_pb.ts';
	import { fmtQuantity, reservedOf } from '#lib/resources.ts';
	import { attemptPhases, phaseGroups } from '#lib/timing.ts';
	import Dialog from '#lib/components/Dialog.svelte';
	import ErrorPanel from '#lib/components/ErrorPanel.svelte';
	import StatusPill from '#lib/components/StatusPill.svelte';
	import TaskLogPane from '#lib/components/TaskLogPane.svelte';
	import EachItemGrid from '#lib/components/EachItemGrid.svelte';
	import {
		applyItemEvents,
		applyLiveItems,
		itemCli,
		itemFromUrl,
		itemStatusText,
		itemsFromTaskRun,
		mergeItems,
		retriedItemsLabel,
		type ItemView
	} from '#lib/each.ts';
	import FailurePanel from '#lib/components/FailurePanel.svelte';
	import { humanFailure, plainFailure } from '#lib/failureText.ts';
	import Icon from '#lib/components/Icon.svelte';
	import RetryBadge from '#lib/components/RetryBadge.svelte';
	import CacheBadge from '#lib/components/CacheBadge.svelte';
	import type { CacheResult } from '#lib/gen/orkstack/v1/activity_pb.ts';
	import { CacheOutcome, JoinPolicy } from '#lib/gen/orkstack/v1/activity_pb.ts';
	import {
		failingBannerText,
		isFailingNote,
		joinTitle,
		joinWords,
		outstandingHandlers,
		runFailing
	} from '#lib/join.ts';
	import { cachedCount } from '#lib/cache.ts';
	import { taskTiming } from '#lib/timing.ts';
	import { timestampMs } from '#lib/stats.ts';
	import ConfirmDialog from '#lib/components/ConfirmDialog.svelte';
	import CliPopover from '#lib/components/CliPopover.svelte';
	import FactsStrip, { type Fact } from '#lib/components/FactsStrip.svelte';
	import LiveControl from '#lib/components/LiveControl.svelte';
	import Plate from '#lib/components/Plate.svelte';
	import Principal from '#lib/components/Principal.svelte';
	import ProgressRing from '#lib/components/ProgressRing.svelte';
	import TimeAgo from '#lib/components/TimeAgo.svelte';
	import { runProgress } from '#lib/progress.ts';
	import Select from '#lib/components/Select.svelte';
	import { defaultCompareTarget } from '#lib/runDiff.ts';
	import { errorMessage } from '#lib/client.ts';
	import { runClient } from '#lib/clients/runs.ts';
	import { statsClient } from '#lib/clients/stats.ts';
	import { artifactMetadata, downloadFilename, emptyPayload, formatDuration, payloadJson, pinAccent, runCommand, shortDigestWords, shortRunId, taskRunCommand, timestampJson } from '#lib/runDetail.ts';
	import { EVENT_BUFFER_CAP, appendEvents, emptyEventBuffer } from '#lib/eventBuffer.ts';
	import { eventBatcher, slicedApply } from '#lib/eventBatch.ts';
	import { Status, type ArtifactRef, type Payload } from '#lib/gen/orkstack/v1/common_pb.ts';
	import {
		AttemptOutcome,
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
	} from '#lib/gen/orkstack/v1/run_pb.ts';
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
	} from '#lib/attempts.ts';
	import { Code, ConnectError } from '@connectrpc/connect';
	import { durationFromMs, timestampDate } from '@bufbuild/protobuf/wkt';
	import { finishedWords } from '#lib/when.ts';
	import { FailureGroupBy, type FlakeGroup, type GetFlakeIndexResponse } from '#lib/gen/orkstack/v1/stats_pb.ts';
	import { flakesCli } from '#lib/flakes.ts';
	import { watchRunEvents, type RunSocketState } from '#lib/runEvents.ts';
	import { runWindowFilters } from '#lib/browsing.ts';
	import { NAMESPACE_PARAM, namespaceLabel, withNamespace } from '#lib/namespace.ts';
	import {
		PRINCIPAL_CAPTION,
		UNRECORDED,
		UNRECORDED_CAPTION,
		originKindSlug,
		originLinkPath,
		originRootRunPath,
		originReferenceURL
	} from '#lib/origin.ts';
	import { stampClass } from '#lib/statusMarks.ts';
	import { digestHead } from '#lib/runDetail.ts';
	import { SvelteSet } from 'svelte/reactivity';
	import { fmtTimestamp, isTerminal, isTerminalEventType, keepsEventMessage, statusLabel, taskDispatched } from '#lib/status.ts';
	import { statusMark } from '#lib/statusMarks.ts';
	import { activityLabel, hasError, pinnedOrReference, shortDigest } from '#lib/activity.ts';
	import type { Error as OrkError } from '#lib/gen/orkstack/v1/common_pb.ts';
	import type { CommitRef } from '#lib/gen/orkstack/v1/activity_pb.ts';
	import { LiveAttemptState, type LiveAttempt, type RunArtifact } from '#lib/gen/orkstack/v1/run_service_pb.ts';
	import { liveLine } from '#lib/live.ts';
	import { afterNavigation } from '#lib/settle.ts';
	import DrawingFrame from '#lib/components/DrawingFrame.svelte';
	import ActivityIcon from '#lib/components/ActivityIcon.svelte';
	import MoreMenu from '#lib/components/MoreMenu.svelte';
	import ActivitySymbol from '#lib/components/ActivitySymbol.svelte';
	import { safeIcon } from '#lib/activityIcon.ts';
	import { activityKind } from '#lib/activityIcon.ts';
	import TaskMap from '#lib/components/TaskMap.svelte';
	import TaskMapLegend from '#lib/components/TaskMapLegend.svelte';
	import { dagDirectionParam, type NodeState } from '#lib/dag.ts';
	import { itemLabel } from '#lib/each.ts';
	import { isFailureHandler } from '#lib/join.ts';
	import { recordSeen } from '#lib/heldRuns.ts';
	import { registerProvider } from '#lib/palette/context.ts';
	import { commandId, type Command } from '#lib/palette/commands.ts';
	import { match } from '#lib/palette/rank.ts';
	import { STAGE_ROWS_MAX, mapGeometry, mapSectionsOf, revealBy, scrollToCard, twoFrames, type MapTask } from '#lib/taskMap.ts';
	import AttemptTimeline from '#lib/components/AttemptTimeline.svelte';
	import type { PageProps } from './$types';

	let { data }: { data: PageProps['data'] } = $props();

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
	// those, and otherwise it is every end task's values.
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
	const originRootHref = $derived.by(() => {
		const path = originRootRunPath(origin);
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
	// The artifact list as last read: a new read keeps the old list until it lands.
	let artifacts = $state.raw<RunArtifact[]>([]);
	let artifactsError = $state<string | null>(null);
	$effect(() => {
		let current = true;
		void data.artifactsRead.then((read) => {
			if (!current) return;
			artifacts = read.artifacts;
			artifactsError = read.error;
		});
		return () => {
			current = false;
		};
	});
	const artifactsByTask = $derived.by(() => {
		const grouped = new Map<string, RunArtifact[]>();
		for (const record of artifacts) {
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
	// $state.raw, and a task's view is replaced rather than edited: at thousands
	// of tasks a deeply reactive proxy per task, with a signal per field a row
	// reads, was most of what opening the page cost. A row re-renders only when
	// its own task is a new object; what a log pane subscribes by is its own
	// derived coordinates, so a replaced task never touches its socket.
	let tasks = $state.raw<TaskView[]>(untrack(seedTasks));
	// While a batch of changes is being made, the list they are made to.
	let tasksDraft: TaskView[] | null = null;
	// taskAt indexes `tasks` by id, so an event, a card or a derived value
	// finds its task in one lookup instead of a scan of every card: at the
	// task ceiling (thousands of tasks) a scan per event — and one per task
	// inside the failing-handlers banner — kept a live page busy for tens of
	// seconds. A task keeps its place, and tasks are only ever added at the
	// end, so a length that moved is the one change that rebuilds it.
	const taskAt = new Map<string, number>();
	function findTask(id: string): TaskView | undefined {
		const live = tasks;
		const list = tasksDraft ?? live;
		if (taskAt.size !== list.length) {
			taskAt.clear();
			list.forEach((task, at) => taskAt.set(task.id, at));
		}
		const at = taskAt.get(id);
		return at === undefined ? undefined : list[at];
	}
	// Many changes, one new list: a batch of events or a whole snapshot.
	function batchTasks(change: () => void): void {
		if (tasksDraft) return change();
		untrack(() => {
			const draft = [...tasks];
			tasksDraft = draft;
			try {
				change();
			} finally {
				tasksDraft = null;
				if (draft.length !== tasks.length || draft.some((task, at) => task !== tasks[at])) tasks = draft;
			}
		});
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

	// Every row reads this one map, so writing it costs every row again. A long
	// list takes a burst of answers (one per task ending) as one write.
	const ATTEMPTS_WRITE_MS = 500;
	let attemptsDue = new Map<string, TaskAttempt[]>();
	let attemptsWrite: ReturnType<typeof setTimeout> | undefined;
	function writeAttempts(): void {
		attemptsWrite = undefined;
		const next = new Map(attemptsByTask);
		for (const [key, attempts] of attemptsDue) next.set(key, attempts);
		attemptsDue = new Map();
		attemptsByTask = next;
	}
	function keepAttempts(key: string, attempts: TaskAttempt[]): void {
		attemptsDue.set(key, attempts);
		if (tasks.length <= ROW_CHUNK || (activeTask && key === attemptsKey(activeTask, activeItem))) {
			clearTimeout(attemptsWrite);
			writeAttempts();
		} else attemptsWrite ??= setTimeout(writeAttempts, ATTEMPTS_WRITE_MS);
	}
	onDestroy(() => clearTimeout(attemptsWrite));

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
			keepAttempts(key, res.attempts);
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
	// A row names its Orker when it is pinned; every row does once asked. A per
	// viewer preference, like the retry history above, and read here rather than
	// in an effect so a long list is not drawn twice to honour it.
	const ORKERS_KEY = 'orkstack.run.orkers';
	function storedOrkers(): boolean {
		try {
			return globalThis.localStorage?.getItem(ORKERS_KEY) === 'shown';
		} catch {
			return false;
		}
	}
	let orkersShown = $state(storedOrkers());
	function toggleOrkers() {
		orkersShown = !orkersShown;
		try {
			globalThis.localStorage?.setItem(ORKERS_KEY, orkersShown ? 'shown' : 'hidden');
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
	let copied = $state('');
	let downloadingArtifact = $state('');
	let artifactDownloadError = $state('');
	let artifactDownloadAbort: AbortController | undefined;
	onDestroy(() => artifactDownloadAbort?.abort());
	let wallNow = $state(Date.now());
	$effect(() => {
		if (isTerminal(runStatus)) return;
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
			const res = await runClient(fetch).inspectRun({ runId: run.id, namespace: data.namespace });
			const next = new Map<string, LiveAttempt>();
			for (const attempt of res.attempts) next.set(attemptsKey(attempt.taskId, attempt.item), attempt);
			liveByTask = next;
			// No event says a fan-out's item has started: this read does.
			const byTask = new Map<string, LiveAttempt[]>();
			for (const attempt of res.attempts) {
				if (attempt.item === undefined) continue;
				const list = byTask.get(attempt.taskId);
				if (list) list.push(attempt);
				else byTask.set(attempt.taskId, [attempt]);
			}
			batchTasks(() => {
				for (const [taskId, attempts] of byTask) {
					const task = findTask(taskId);
					if (!task) continue;
					const items = applyLiveItems(task.items, attempts);
					if (items === task.items) continue;
					upsertTask(taskId, { items });
					mapRevision++;
				}
			});
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
		batchTasks(mergeTerminalRows);
	}
	function mergeTerminalRows() {
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
	// #lib/settle.ts).
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

	// A patch that moves nothing changes nothing: replayed events arrive by the
	// thousand on a page opened on a finished run, and none may re-render a row.
	function upsertTask(taskId: string, patch: Partial<TaskView>) {
		batchTasks(() => {
			const existing = findTask(taskId);
			if (existing && (Object.keys(patch) as (keyof TaskView)[]).every((key) => patch[key] === existing[key])) return;
			const list = tasksDraft!;
			list[existing ? taskAt.get(taskId)! : list.length] = existing
				? { ...existing, ...patch }
				: { id: taskId, activity: '', status: Status.PENDING, attempt: 0, message: '', artifacts: [], systemArtifacts: [], pool: '', pin: '', orkerId: '', items: [], ...patch };
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

	// applyEvents takes one frame's worth of socket events (#lib/eventBatch.ts),
	// in delivery order: ONE buffer append for the batch, ONE item-array copy
	// and write per fan-out it touches, and the reducer below for every other
	// event.
	// The live control: while paused, events are held, never dropped, and
	// applied in order on resume, so the page catches up to the run.
	let paused = $state(false);
	let heldEvents: Event[] = [];
	function applyOrHold(events: Event[]): void {
		if (untrack(() => paused)) {
			heldEvents.push(...events);
			return;
		}
		applyEvents(events);
	}
	function toggleLive(): void {
		paused = !paused;
		if (!paused && heldEvents.length) {
			const due = heldEvents;
			heldEvents = [];
			applyEvents(due);
		}
	}

	function applyEvents(events: Event[]) {
		mapRevision++;
		buffer = appendEvents(buffer, events);
		batchTasks(() => applyEventsToTasks(events));
	}
	function applyEventsToTasks(events: Event[]) {
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
			const shown = untrack(() => (activeTask === taskId ? activeItem : undefined));
			upsertTask(taskId, { items: applyItemEvents(known?.items ?? [], list) });
			for (const event of list) {
				if (isTerminal(event.status) && shown === event.item) void refreshAttempts(taskId, event.item);
			}
		}
	}

	// When a task ended, as its event says, until a snapshot brings the record's
	// own: a live task's Finished does not wait for the run's terminal re-read.
	function finishOf(event: Event): Pick<TaskView, 'finishedAt'> | object {
		return findTask(event.taskId)?.finishedAt ? {} : { finishedAt: event.timestamp };
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
				upsertTask(event.taskId, { status: status ?? Status.SUCCEEDED, ...finishOf(event) });
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
					message: event.message,
					...finishOf(event)
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
					if (status === Status.SKIPPED || status === Status.CANCELED) Object.assign(patch, { status }, finishOf(event));
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
		const slices = replayed ? slicedApply(applyOrHold, REPLAY_SLICE, whenIdle) : undefined;
		const batch = eventBatcher(slices ? slices.apply : applyOrHold, replayed ? replayBatches : undefined);
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
		const params = new URLSearchParams(typeof location === 'undefined' ? page.url.search : location.search);
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
	// A number is the item chosen, null that a reader deselected it, absent no choice yet.
	let selectedItemByTask = $state<Record<string, number | null | undefined>>(
		untrack(() => {
			const task = page.url.searchParams.get('task') ?? '';
			const item = itemFromUrl(page.url);
			return task && item !== undefined ? { [task]: item } : {};
		})
	);
	function selectItem(taskId: string, item: number | undefined): void {
		selectedItemByTask[taskId] = item ?? null;
		selectedTask = taskId;
		selectionCleared = false;
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
		const params = new URLSearchParams(page.url.search);
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
			id: task.id, status: Status[task.status ?? Status.UNSPECIFIED], attempt: task.attempt,
			pool: task.pool || undefined, pin: task.pin || undefined, orkerId: task.orkerId || undefined,
			scheduledAt: timestampJson(task.scheduledAt), startedAt: timestampJson(task.startedAt), finishedAt: timestampJson(task.finishedAt),
			output: task.output?.data, values: task.valuesRef ? artifactMetadata(task.valuesRef) : undefined,
			artifacts: task.artifacts.map(artifactMetadata),
			systemArtifacts: task.systemArtifacts.map(artifactMetadata), error: task.error,
			each: task.each ? { expanded: task.each.expanded, count: task.each.count } : undefined,
			items: task.each
				? task.items.map((item) => ({ item: item.index, status: Status[item.status ?? Status.UNSPECIFIED], attempt: item.attempt, error: item.run?.error }))
				: undefined
		};
	}
	function runExport() {
		return {
			id: run?.id, namespace: namespaceLabel(data.namespace),
			orkflow: { name: run?.orkflowName, version: run?.orkflowVersion, definitionDigest: run?.orkflowDefinitionDigest },
			status: Status[runStatus ?? Status.UNSPECIFIED], createdAt: timestampJson(run?.createdAt), startedAt: timestampJson(startedAt ?? run?.startedAt), finishedAt: timestampJson(finishedAt ?? run?.finishedAt),
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
	// follow. replaceState lands its write asynchronously, so the tick moves
	// once the address bar has.
	let shallowTick = $state(0);
	function shallow(href: string): void {
		void replaceState(href, page.state).then(() => {
			shallowTick++;
		});
	}
	function writeHref(next: { task?: string; attempt?: number; item?: number; clear?: boolean }): string {
		const params = new URLSearchParams(location.search);
		params.set(NAMESPACE_PARAM, namespaceLabel(data.namespace));
		if (next.clear) for (const key of ['task', 'attempt', 'item']) params.delete(key);
		if (next.task !== undefined) {
			params.set('task', next.task);
			if (next.attempt) params.set('attempt', String(next.attempt));
			else params.delete('attempt');
			if (next.item !== undefined) params.set('item', String(next.item));
			else params.delete('item');
		}
		return `/runs/${encodeURIComponent(data.runId)}?${params.toString()}`;
	}

	// Did the page open on a run still going? Then a failure that arrives is
	// one the reader watched happen, and the cloud draws itself in.
	const loadedLive = untrack(() => !isTerminal(data.run?.status ?? Status.UNSPECIFIED));
	let failedWhileWatched = $state(false);

	// THE SELECTION. A row's name, its graph node, a task map cell and the
	// palette all select the same task. With no choice made it is the task the
	// page is about: the failed one on a failed run, else the first one running
	// or waiting. A deep link seeds it. Clear selection stands until the next
	// choice.
	let selectedTask = $state(untrack(() => page.url.searchParams.get('task') ?? ''));
	let selectionCleared = $state(false);
	let followedItem: { taskId: string; index: number } | null = null;
	// The failed task; else the one followed until it ends, so its log does not
	// jump; else the next live one; and when nothing is live the last one stays,
	// so a log does not vanish under its reader as the run ends.
	let followedTask = '';
	const autoTask = $derived.by(() => {
		const failed = tasks.find((t) => t.status === Status.FAILED && !isFailureHandler(joinOf(t.id)));
		if (failed) return (followedTask = failed.id);
		const kept = followedTask ? findTask(followedTask) : undefined;
		if (kept && (kept.status === Status.RUNNING || kept.status === Status.AWAITING)) return followedTask;
		const next = tasks.find((t) => t.status === Status.RUNNING) ?? tasks.find((t) => t.status === Status.AWAITING);
		if (next) followedTask = next.id;
		return followedTask;
	});
	// An id from the address this run does not have is no choice at all.
	const chosenTask = $derived(selectedTask && findTask(selectedTask) ? selectedTask : '');
	const activeTask = $derived(chosenTask || (selectionCleared ? '' : autoTask));
	const activeView = $derived(activeTask ? findTask(activeTask) : undefined);
	const activeItem = $derived.by((): number | undefined => {
		const task = activeView;
		if (!task || !isFanOut(task.id)) return undefined;
		const chosen = selectedItemByTask[task.id];
		if (chosen !== undefined) return chosen ?? undefined;
		// The page follows one item until it ends, so its card and log do not jump under a reader.
		if (followedItem?.taskId === task.id) {
			const kept = task.items[followedItem.index];
			if (kept && (kept.status === Status.RUNNING || kept.status === Status.FAILED)) return followedItem.index;
		}
		const next = (
			task.items.find((i) => i.status === Status.FAILED) ??
			task.items.find((i) => i.status === Status.RUNNING) ??
			(isTerminal(task.status) ? undefined : task.items.find((i) => !isTerminal(i.status)))
		)?.index;
		followedItem = next === undefined ? null : { taskId: task.id, index: next };
		return next;
	});
	const selectedCell = $derived(activeTask ? { taskId: activeTask, item: activeItem } : null);
	const selectionWords = $derived.by(() => {
		const task = activeView;
		if (!task) return '';
		const item = activeItem !== undefined ? task.items[activeItem] : undefined;
		const name = item ? itemLabel(task.id, item.index) : task.id;
		return `${name} is selected, ${statusLabel(item?.status ?? task.status).toLowerCase()}; its details are under its row`;
	});
	function selectTask(taskId: string, item?: number): void {
		if (item !== undefined && isFanOut(taskId)) selectItem(taskId, item);
		else {
			selectedTask = taskId;
			selectionCleared = false;
			shallow(writeHref({ task: taskId, attempt: 0 }));
		}
		void bringIntoView(taskId);
	}
	function clearSelection(): void {
		selectedTask = '';
		selectionCleared = true;
		shallow(writeHref({ clear: true }));
	}
	// A row's name: pressing the chosen task lets it go; pressing the task the
	// page chose by itself makes it the reader's choice.
	function toggleRow(taskId: string): void {
		// The panel leaving a row above this one must not move this one under the pointer.
		const before = cardOf(taskId)?.getBoundingClientRect().top;
		if (selectedTask === taskId) clearSelection();
		else {
			selectedTask = taskId;
			selectionCleared = false;
			shallow(writeHref({ task: taskId, attempt: 0 }));
		}
		void tick().then(() => {
			const after = cardOf(taskId)?.getBoundingClientRect().top;
			if (before !== undefined && after !== undefined && Math.abs(after - before) >= 1) window.scrollBy(0, after - before);
			if (activeTask === taskId) void bringIntoView(taskId);
		});
	}
	function toggleNode(taskId: string): void {
		if (selectedTask === taskId) clearSelection();
		else selectTask(taskId);
	}
	// An item chosen by the page has its timeline read as if a reader had pressed it.
	$effect(() => {
		const [task, item] = [activeTask, activeItem];
		if (task && item !== undefined && !untrack(() => attemptsByTask.has(attemptsKey(task, item)))) void refreshAttempts(task, item);
	});
	function cardOf(taskId: string): HTMLElement | null {
		return document.querySelector(`[data-testid="task-${CSS.escape(taskId)}"]`);
	}
	// The latest selection owns the scroll: an earlier one still following its card stops.
	let scrollRequest = 0;
	async function bringIntoView(taskId: string): Promise<void> {
		const request = ++scrollRequest;
		// The selection's panel moves in this flush: it is measured where it lands.
		await tick();
		const card = cardOf(taskId);
		if (!card || request !== scrollRequest) return;
		const viewport = () => ({
			top: document.querySelector('header')?.getBoundingClientRect().bottom ?? 0,
			bottom: innerHeight
		});
		const rect = () => rowAndPanel(card, viewport());
		await scrollToCard({
			rect,
			scroll: () => window.scrollBy(0, revealBy(rect(), viewport())),
			viewport,
			position: () => window.scrollY,
			now: () => performance.now(),
			frames: twoFrames,
			cancelled: () => request !== scrollRequest
		});
	}
	const ROW_CHUNK = 200;
	// A running row's clock: every second for a small run, every few at hundreds of
	// rows, because each change writes every running row's text, on screen or not.
	const LONG_LIST_TICK_S = 5n;
	const rowsNow = $derived.by(() => {
		const seconds = currentTimestamp.seconds;
		return { seconds: tasks.length > ROW_CHUNK ? seconds - (seconds % LONG_LIST_TICK_S) : seconds, nanos: 0 };
	});
	// A row and what is open under it, as far as they fit between the bar and the
	// viewport's bottom edge: the box a selection has to bring into view.
	function rowAndPanel(card: HTMLElement, view: { top: number; bottom: number }): { top: number; bottom: number } {
		const { top, bottom } = card.getBoundingClientRect();
		let last: Element = card;
		while (last.nextElementSibling?.matches('[data-row-open], [data-testid="task-panel"]')) last = last.nextElementSibling;
		return { top, bottom: Math.min(Math.max(bottom, last.getBoundingClientRect().bottom), top + view.bottom - view.top) };
	}
	// The task panel is one element, and this puts it under the selected row, after
	// what that row already has open. The row's chunk then stops skipping its
	// paint, which would otherwise clip the panel's menus at the chunk's edge.
	const chunked = $derived(tasks.length > ROW_CHUNK);
	function underSelectedRow(panel: HTMLElement) {
		void chunked;
		const row = activeTask ? cardOf(activeTask) : null;
		if (!row) return;
		let last: Element = row;
		while (last.nextElementSibling?.hasAttribute('data-row-open')) last = last.nextElementSibling;
		if (last.nextElementSibling !== panel) last.after(panel);
		const chunk = panel.closest('.rows-chunk');
		chunk?.setAttribute('data-open', 'true');
		return () => chunk?.removeAttribute('data-open');
	}
	// The selected row is marked imperatively, so a new selection never
	// re-renders thousands of rows.
	$effect(() => {
		const id = activeTask;
		const card = id ? cardOf(id) : null;
		const name = card?.querySelector('[aria-pressed]');
		card?.setAttribute('data-selected', 'true');
		name?.setAttribute('aria-pressed', 'true');
		return () => {
			card?.removeAttribute('data-selected');
			name?.setAttribute('aria-pressed', 'false');
		};
	});

	// What each task's image declares: the default icon until it is known.
	let activityByTask = $state.raw(new Map<string, ActivityInfo>());
	$effect(() => {
		const defs = data.orkflow?.tasks;
		if (!defs?.length) return;
		let live = true;
		void loadActivities(defs, activityClient(fetch)).then((answer) => {
			if (live) activityByTask = answer;
		});
		return () => {
			live = false;
		};
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
	// The map's sections as its frame is sized by: every task, then each
	// fan-out's items (the same sections the map builds, #lib/taskMap.ts).
	const mapSections = $derived(
		mapSectionsOf(
			tasks.map((t) => ({ id: t.id, needs: taskDefs.get(t.id)?.needs ?? [], itemCount: isFanOut(t.id) ? t.items.length : 0 })),
			data.orkflow !== null
		)
	);
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
			return { status: task.status, line: never ? 'never started' : 'Pending', neverStarted: never };
		}
		const fanOut = isFanOut(id);
		if (fanOut && task.items.length > 0 && !isTerminal(task.status)) {
			const succeeded = task.items.filter((i) => i.status === Status.SUCCEEDED).length;
			const running = task.items.filter((i) => i.status === Status.RUNNING).length;
			return { status: task.status, line: `Running · ${succeeded} of ${task.items.length}`, items: { succeeded, running, total: task.items.length } };
		}
		const took = isTerminal(task.status) ? formatDuration(task.startedAt ?? task.scheduledAt, task.finishedAt) : '';
		const shown = took && took !== '—' ? took : '';
		switch (task.status) {
			case Status.SUCCEEDED:
				if (task.cache?.outcome === CacheOutcome.HIT) return { status: task.status, line: 'From the cache' };
				if (isComposed(id)) return { status: task.status, line: shown ? `Child run · ${shown}` : 'Child run' };
				return { status: task.status, line: shown ? `Succeeded ${shown}` : 'Succeeded' };
			case Status.FAILED:
				return { status: task.status, line: task.exitCode ? `Failed · exit ${task.exitCode}` : task.error?.code || 'Failed' };
			default:
				return { status: task.status, line: statusLabel(task.status) };
		}
	}

	// A failed task's tries, as every failure surface on this page states
	// them: the task's own panel and the run's cloud read the same facts.
	function triesOf(task: TaskView): FailureTries {
		const attempts = attemptsSeen(attemptsByTask.get(task.id) ?? []);
		// A task that declares no retry policy and ran once did not exhaust anything:
		// its attempt line already says "no retry policy".
		if (attempts <= 1 && !taskDefs.get(task.id)?.retry) return { retryable: true, attempts };
		return { retryable: task.error?.retryable ?? false, retryState: task.retryState, attempts };
	}

	// The run's failure: the first task that failed, and its try.
	const failedTask = $derived(tasks.find((t) => t.status === Status.FAILED && !isFailureHandler(joinOf(t.id))));
	// The first line the failed task wrote to stderr, as its log pane reads it.
	let firstStderr = $state<{ taskId: string; text: string } | null>(null);
	const getCommand = $derived(run ? runCommand(data.namespace, ['run', 'get', run.id, '--json']) : '');
	const againCommand = $derived(run ? runCommand(data.namespace, ['run', 'again', run.id]) : '');

	// Watching a run is seeing it: the runs list stops holding its changed
	// mark (this browser only, #lib/heldRuns.ts), now and on every status change
	// while the page is visible. Off the path that draws the page.
	$effect(() => {
		const id = data.run?.id;
		// By its enum name, as the list keeps it: the word can change with the design.
		const status = Status[runStatus] ?? 'UNSPECIFIED';
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
		out.push({ id: 'run-again', group: 'Actions', label: 'Run again', action: { kind: 'page', run: () => (runAgainDialogOpen = true) } });
		if (!isTerminal(runStatus) && !cancellationRequested)
			out.push({
				id: 'run-cancel',
				group: 'Actions',
				label: 'Cancel run',
				action: {
					kind: 'page',
					run: () => {
						cancelDialog = true;
						cancelError = '';
					}
				}
			});
		out.push({ id: 'run-compare', group: 'Actions', label: 'Compare with', action: { kind: 'page', run: () => void openCompare() } });
		if (isTerminal(runStatus))
			out.push({ id: 'run-evidence', group: 'Actions', label: 'Open evidence', action: { kind: 'page', run: () => (evidenceDialogOpen = true) } });
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

	// ---- Harbour's header: progress, how long, the screen's commands ----
	const progress = $derived(runProgress(tasks));
	let clock = $state(Date.now());
	$effect(() => {
		if (isTerminal(runStatus)) return;
		const t = setInterval(() => (clock = Date.now()), 1000);
		return () => clearInterval(t);
	});
	const lasted = $derived.by(() => {
		const from = startedAt ?? run?.startedAt ?? run?.createdAt;
		if (!from) return '—';
		const end = finishedAt ?? run?.finishedAt;
		return end ? formatDuration(from, end) : formatDuration(from, { seconds: BigInt(Math.floor(clock / 1000)), nanos: (clock % 1000) * 1e6 });
	});
	// ---- The Tasks card ----
	const timelineEnd = $derived.by(() => {
		// The axis grows with a live run: every second for a small one, every
		// half minute at thousands of rows.
		const step = tasks.length > 200 ? 30_000 : 1000;
		return timestampMs(finishedAt ?? run?.finishedAt) ?? (isTerminal(runStatus) ? null : Math.ceil(wallNow / step) * step);
	});
	const timelineSpan = $derived({ start: timestampMs(startedAt ?? run?.startedAt ?? run?.createdAt), end: timelineEnd });
	// The axis holds the marks its own width holds: a narrow timeline has fewer, never crowded ones.
	const AXIS_MARK_PX = 48;
	let axisWidth = $state(0);
	function observeAxis(el: HTMLElement) {
		const observer = new ResizeObserver(([entry]) => (axisWidth = entry.contentRect.width));
		observer.observe(el);
		return () => observer.disconnect();
	}
	const axis = $derived(timelineSpan.start !== null && timelineSpan.end !== null ? timelineAxis(timelineSpan.end - timelineSpan.start, axisWidth > 0 ? Math.max(2, Math.floor(axisWidth / AXIS_MARK_PX)) : 5) : []);
	function latestItemEnd(items: TaskView['items']): number | null {
		let latest: number | null = null;
		for (const item of items) {
			const end = timestampMs(item.run?.finishedAt);
			if (end !== null && (latest === null || end > latest)) latest = end;
		}
		return latest;
	}
	// The key says "waited" only when a bar shows one. A long list is not scanned for it.
	const anyWait = $derived(tasks.length > ROW_CHUNK || tasks.some((task) => barsOf(task).bars.some((bar) => bar.kind === 'wait')));
	function barsOf(task: TaskView) {
		const rows = attemptsByTask.get(task.id) ?? [];
		return taskTimelineBars({
			run: timelineSpan,
			scheduled: timestampMs(task.scheduledAt),
			// A fan-out, a composed or an awaited task has no attempt of its own: it runs from when it began.
			started: timestampMs(task.startedAt ?? (!task_isContainer(task) ? task.scheduledAt : task.status === Status.RUNNING ? liveByTask.get(task.id)?.attemptStartedAt : undefined)),
			finished: timestampMs(task.finishedAt),
			running: task.status === Status.RUNNING,
			now: timelineEnd ?? wallNow,
			failed: task.status === Status.FAILED,
			doneUntil: task.status === Status.RUNNING && task.items.length > 0 ? latestItemEnd(task.items) : null,
			attempts: rows.map((row) => ({
				attempt: row.attempt,
				failed: row.outcome === AttemptOutcome.FAILED,
				started: timestampMs(row.startedAt),
				finished: timestampMs(row.finishedAt)
			}))
		});
	}
	// When the selected task fails its log moves under its row: focus inside the
	// old pane would fall to the body, so it goes to the task's name instead.
	let logFocusMoved = false;
	$effect.pre(() => {
		// A fan-out's log stays where it is: only a plain task's moves under its row.
		if (activeView?.status === Status.FAILED && !isFanOut(activeView.id) && document.activeElement?.closest('[data-run-log]')) logFocusMoved = true;
	});
	$effect(() => {
		const id = activeView?.status === Status.FAILED ? activeView.id : '';
		if (!id || !logFocusMoved) return;
		logFocusMoved = false;
		document.querySelector<HTMLElement>(`[data-testid="task-deep-link-${CSS.escape(id)}"]`)?.focus();
	});

	// A running attempt no Orker has started, for want of room.
	function waitingFor(live: LiveAttempt | undefined): boolean {
		return live?.state === LiveAttemptState.SCHEDULED && live.nextAttemptAt === undefined;
	}
	/** The word a task's image goes by: its catalog name, or the last part of its reference. */
	function imageWord(taskId: string): string {
		const def = taskDefs.get(taskId);
		const info = activityByTask.get(taskId);
		if (info?.name) return info.name;
		const uses = def?.uses ?? findTask(taskId)?.activity ?? '';
		const tail = uses.split('/').pop() ?? uses;
		return catalogName(tail);
	}
	const NO_ATTEMPTS: TaskAttempt[] = [];
	// What a row says that its own task and definition decide: one derived a row.
	function rowKind(task: TaskView) {
		const def = taskDefs.get(task.id);
		const isAwait = task.await !== undefined || def?.await !== undefined;
		const composed = task.child !== undefined || def?.orkflow !== undefined;
		const dispatched = taskDispatched(task.status, task.scheduledAt ?? task.startedAt);
		const never = neverStarted(task);
		const waiting = !dispatched && !never && task.status !== Status.SKIPPED;
		return {
			pill:
				task.status === Status.RUNNING && waitingFor(liveByTask.get(task.id))
					? { cls: 'stamp pill-wait', mark: 'mark-wait', word: liveByTask.get(task.id)?.storeOutageWait ? 'Waiting for object storage' : 'Waiting for capacity' }
					: waiting
						? { cls: 'stamp pill-outline text-ink-2', mark: '', word: 'Pending' }
						: {
								cls: stampClass(task.status),
								mark: task.status === Status.RUNNING ? 'mark-dot' : task.status === Status.AWAITING ? 'mark-diamond' : '',
								word: statusLabel(task.status)
							},
			def,
			isAwait,
			composed,
			dispatched,
			never,
			fanOut: task.each !== undefined || def?.each !== undefined,
			accent: pinAccent(task.pin),
			cached: def?.cache !== undefined,
			// A task of a live run that no Orker has seen yet.
			waiting,
			word: composed && task.child ? childLabel(task.child) : isAwait ? 'awaited task' : imageWord(task.id)
		};
	}
	type RowKind = ReturnType<typeof rowKind>;
	// A row's word for why it ended; the whole sentence is on its panel.
	function rowReason(task: TaskView, kind: RowKind): string {
		return task.message && !task.error && !kind.never ? reasonWord(task.status, task.message) : '';
	}
	// Whether a row has any of the rarer words of its second line.
	// Where a task ran, from the same places its panel reads: what the snapshot
	// says, then the heartbeat of what is running, then its last attempt's record.
	function liveOrker(key: string): string {
		const live = liveByTask.get(key);
		return live?.heartbeatAvailable ? live.orkerId : '';
	}
	function orkersOfRow(task: TaskView) {
		if (task.items.length > 0) return rowOrkers(task.items.map((i) => i.run?.orkerId || liveOrker(attemptsKey(task.id, i.index))));
		const attempts = attemptsByTask.get(task.id) ?? NO_ATTEMPTS;
		return rowOrkers([task.orkerId || liveOrker(task.id) || attempts[attempts.length - 1]?.orkerId || '']);
	}
	function rowHasExtras(task: TaskView, kind: RowKind): boolean {
		return (orkersShown && orkersOfRow(task) !== null) || rowHasOwnExtras(task, kind);
	}
	// A row whose second line would say only its activity and its Orker keeps that
	// line single: the id clips, and its tip holds the whole of it. Any other word
	// on the line wraps as it always has, so no fact is ever clipped for an id.
	function rowOrkerClips(task: TaskView, kind: RowKind): boolean {
		return orkersShown && !kind.never && !kind.waiting && !rowHasOwnExtras(task, kind);
	}
	function rowHasOwnExtras(task: TaskView, kind: RowKind): boolean {
		return (
			kind.fanOut ||
			!!task.child?.runId ||
			!!task.join ||
			!!task.pin ||
			!!task.cache ||
			(kind.cached && task.status === Status.SUCCEEDED) ||
			!!rowReason(task, kind) ||
			flakesByTask.has(task.id) ||
			attemptsSeen(attemptsByTask.get(task.id) ?? NO_ATTEMPTS) > 1 ||
			(task.status === Status.RUNNING && liveByTask.has(task.id))
		);
	}
	// One drawing per declared icon in the page's own sprite, however many tasks run the image.
	const declaredSprites = $derived.by(() => {
		const byIcon = new Map<ActivityInfo['icon'], string>();
		for (const info of activityByTask.values()) {
			if (info.icon && !byIcon.has(info.icon) && safeIcon(info.icon)) byIcon.set(info.icon, `i-declared-${byIcon.size}`);
		}
		return byIcon;
	});
	function iconHref(id: string, kind: RowKind): string {
		if (kind.composed) return '#i-a-orkflow';
		if (kind.isAwait) return '#i-a-await';
		const declared = declaredSprites.get(activityByTask.get(id)?.icon);
		return declared ? `#${declared}` : '#i-a-default';
	}
	// A <use> costs a row a shadow tree of its own, on screen or not, and again each
	// time its icon changes: a long list's rows draw theirs from when their chunk
	// first comes near the screen.
	const litChunks = new SvelteSet<number>();
	const lightChunk = (chunk: number) => (el: HTMLElement) => {
		const observer = new IntersectionObserver(
			([entry]) => {
				if (!entry.isIntersecting) return;
				litChunks.add(chunk);
				observer.disconnect();
			},
			{ rootMargin: '800px' }
		);
		observer.observe(el);
		return () => observer.disconnect();
	};
	const rowChunks = $derived.by(() => {
		if (tasks.length <= ROW_CHUNK) return null;
		const chunks: TaskView[][] = [];
		for (let i = 0; i < tasks.length; i += ROW_CHUNK) chunks.push(tasks.slice(i, i + ROW_CHUNK));
		return chunks;
	});
	function itemHasArtifacts(taskId: string, item: number): boolean {
		return (artifactsByTask.get(taskId) ?? []).some((record) => record.item === item);
	}
	function fanOutWords(task: TaskView): string {
		const spec = taskDefs.get(task.id)?.each;
		const count = task.each?.expanded ? task.items.length : 0;
		const parts = [count ? `${count} ${count === 1 ? 'item' : 'items'}` : 'a fan-out'];
		if (spec?.maxParallel) parts.push(`${spec.maxParallel} at a time`);
		return parts.join(', ');
	}
	function rowDuration(task: TaskView): string {
		if (isTerminal(task.status)) {
			const took = formatDuration(task.startedAt ?? task.scheduledAt, task.finishedAt);
			return took === '—' ? '' : took;
		}
		if (task.status === Status.RUNNING || task.status === Status.AWAITING) {
			return formatDuration(task.scheduledAt ?? task.startedAt, rowsNow);
		}
		return '';
	}
	let expandAll = $state(false);
	const hasFanOut = $derived((data.orkflow?.tasks ?? []).some((t) => t.each !== undefined));
	const graphSub = $derived.by(() => {
		const count = `${tasks.length} ${tasks.length === 1 ? 'task' : 'tasks'}`;
		const fan = tasks.filter((t) => t.items.length > 0);
		if (fan.length === 1) return `${count}; ${fan[0].id} has ${fan[0].items.length} items`;
		return count;
	});
	// The graph's key: the statuses it shows, and the activities drawn.
	const LEGEND_ORDER = [Status.SUCCEEDED, Status.RUNNING, Status.AWAITING, Status.FAILED, Status.SKIPPED, Status.CANCELED, Status.PENDING];
	const legendStatuses = $derived.by(() => {
		// A long run's key reads the task map's counts rather than scanning every task on every event.
		const present = rowChunks ? new Set(legend.keys()) : new Set(tasks.map((t) => t.status));
		return LEGEND_ORDER.filter((s) => present.has(s));
	});
	const legendActivities = $derived.by(() => {
		const seen = new Map<string, { word: string; icon?: ActivityInfo['icon']; kind: 'default' | 'orkflow' | 'await' }>();
		for (const def of data.orkflow?.tasks ?? []) {
			const kind = def.orkflow ? 'orkflow' : def.await ? 'await' : 'default';
			const word = kind === 'orkflow' ? 'composed task' : kind === 'await' ? 'awaited task' : imageWord(def.id);
			if (!seen.has(word)) seen.set(word, { word, kind, icon: activityByTask.get(def.id)?.icon });
		}
		return [...seen.values()].slice(0, 8);
	});
	const anyHandlers = $derived(tasks.some((t) => isFailureHandler(joinOf(t.id))));

	// ---- The capacity note and What X waits for ----
	const capacityWaiters = $derived([...liveByTask.values()].filter((live) => waitingFor(live) && live.capacityWait));
	const firstWaiter = $derived(capacityWaiters[0]);
	const outageWaiters = $derived([...liveByTask.values()].filter((live) => waitingFor(live) && live.storeOutageWait));
	const firstOutageWaiter = $derived(outageWaiters[0]);
	const waiterLabel = (live: LiveAttempt) => (live.item !== undefined ? itemLabel(live.taskId, live.item) : live.taskId);
	const waiterSeconds = (live: LiveAttempt): string => {
		const from = timestampMs(live.attemptScheduledAt);
		return from === null ? '' : `${Math.max(0, Math.round((liveNowMs - from) / 1000))}s`;
	};
	// The selected task or item, when it is the one waiting.
	const activeWaiter = $derived.by(() => {
		if (!activeTask) return undefined;
		const live = liveByTask.get(attemptsKey(activeTask, activeItem));
		return waitingFor(live) ? live : undefined;
	});
	// How this run's own work on that pool is going, for "what frees it".
	const poolPeers = $derived.by(() => {
		const pool = activeWaiter?.capacityWait?.pool;
		if (!pool) return { running: 0, took: '' };
		let running = 0;
		const spans: number[] = [];
		const count = (status: Status, r: { pool: string; startedAt?: TaskRun['startedAt']; finishedAt?: TaskRun['finishedAt'] } | undefined) => {
			if (r?.pool !== pool) return;
			if (status === Status.RUNNING) running++;
			const [from, to] = [timestampMs(r.startedAt), timestampMs(r.finishedAt)];
			if (status === Status.SUCCEEDED && from !== null && to !== null) spans.push(to - from);
		};
		for (const task of tasks) {
			if (task.items.length) for (const item of task.items) count(item.status, item.run);
			else count(task.status, task);
		}
		if (spans.length === 0) return { running, took: '' };
		spans.sort((a, b) => a - b);
		const s = (ms: number) => `${Math.round(ms / 1000)}s`;
		const median = spans[Math.floor(spans.length / 2)];
		return { running, took: spans.length === 1 ? s(median) : `${s(spans[0])} to ${s(spans[spans.length - 1])}, ${s(median)} median` };
	});

	// ---- How it failed: the chain of records that led here ----
	const failedNeed = $derived.by(() => {
		if (!failedTask) return '';
		return (taskDefs.get(failedTask.id)?.needs ?? []).find((need) => findTask(need)?.status === Status.SUCCEEDED) ?? '';
	});
	const skippedTasks = $derived(tasks.filter((t) => t.status === Status.SKIPPED).map((t) => t.id));
	const neverTasks = $derived(tasks.filter((t) => neverStarted(t)).map((t) => t.id));
	const handlerTasks = $derived(tasks.filter((t) => isFailureHandler(joinOf(t.id)) && t.status !== Status.SKIPPED && !neverStarted(t)).map((t) => t.id));
	function failureSentence(message: string): string {
		const parts = [humanFailure(plainFailure(message).text).replace(/\.?$/, '.')];
		if (skippedTasks.length) parts.push(`${listWords(skippedTasks)} ${skippedTasks.length === 1 ? 'was' : 'were'} skipped.`);
		if (neverTasks.length) parts.push(`${listWords(neverTasks)} never started.`);
		if (handlerTasks.length) parts.push(`${listWords(handlerTasks)} ran as failure ${handlerTasks.length === 1 ? 'handler' : 'handlers'}; the run still ends Failed with this reason.`);
		return parts.join(' ');
	}
	const listWords = (ids: string[]) => (ids.length <= 1 ? ids.join('') : `${ids.slice(0, -1).join(', ')} and ${ids[ids.length - 1]}`);

	function viewStderr(event: MouseEvent, task: TaskView): void {
		event.preventDefault();
		selectedTask = task.id;
		selectionCleared = false;
		shallow(writeHref({ task: task.id, attempt: task.attempt }));
		void tick().then(() => {
			const log = document.querySelector<HTMLElement>(`[data-testid="task-log-${CSS.escape(task.id)}"]`);
			log?.scrollIntoView({ block: 'center' });
			log?.focus({ preventScroll: true });
		});
	}

	let inputsAsJson = $state(false);
	// Which tasks have their Inspect open: a disclosure of one task, never in the address.
	let inspectOpenByTask = $state<Record<string, boolean>>({});
	const inputEntries = $derived.by(() => {
		const raw = run?.input?.data;
		if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return [];
		return Object.entries(raw as Record<string, unknown>).map(([key, value]) => [key, typeof value === 'string' ? value : JSON.stringify(value)] as const);
	});

	const task_isAwait = (task: TaskView) => task.await !== undefined || taskDefs.get(task.id)?.await !== undefined;
	const task_isComposed = (task: TaskView) => task.child !== undefined || taskDefs.get(task.id)?.orkflow !== undefined;
	const task_isFanOut = (task: TaskView) => task.each !== undefined || taskDefs.get(task.id)?.each !== undefined;
	const task_isContainer = (task: TaskView) => !task_isAwait(task) && !task_isComposed(task) && !task_isFanOut(task);
	// A comparison in passing reads every task's attempt records on the engine:
	// past this many tasks the card offers it and leaves the reading to the page.
	const DIFF_IN_PASSING = 300;
	// The task map's box: as tall as the map at the card's width, within a bound.
	let mapWidth = $state(0);
	const MAP_MAX_HEIGHT = 320;
	// TaskMap's own padding on each side of the box, 8 px.
	const MAP_PADDING = 16;
	// Observed, not bound: a bound width is read during the render, which lays out
	// the whole page in the middle of drawing it.
	let mapBox = $state<HTMLDivElement | undefined>();
	$effect(() => {
		if (!mapBox || typeof ResizeObserver === 'undefined') return;
		const observer = new ResizeObserver(([entry]) => (mapWidth = entry.contentRect.width));
		observer.observe(mapBox);
		return () => observer.disconnect();
	});
	const mapBoxHeight = $derived.by(() => {
		if (mapWidth <= 0) return 60;
		const g = mapGeometry(mapSections, mapWidth - MAP_PADDING, MAP_MAX_HEIGHT - MAP_PADDING);
		return g.scroll > 0 ? MAP_MAX_HEIGHT : Math.max(g.height + MAP_PADDING, 36);
	});
	// A task's commands, behind its terminal icon.
	function taskCliOf(task: TaskView) {
		if (!run) return [];
		const canLog = task_isContainer(task) && !neverStarted(task);
		const rows = attemptsByTask.get(task.id) ?? [];
		return [
			...(canLog ? [{ label: 'Its log', command: runCommand(data.namespace, ['run', 'logs', run.id, '--task', task.id, ...(task.attempt > 0 ? ['--attempt', String(task.attempt)] : [])]) }] : []),
			...(canLog && task.status === Status.FAILED ? [{ label: 'Run it here, on your machine', command: taskRunCommand(data.namespace, run.id, task.id) }] : []),
			...((task.cache && task.cache.outcome !== CacheOutcome.HIT) || rows.length > 1 ? [{ label: 'Its attempts and cache key', command: runCommand(data.namespace, ['run', 'attempts', run.id, '--task', task.id]) }] : []),
			...(task.status === Status.RUNNING ? [{ label: 'What it is doing', command: runCommand(data.namespace, ['run', 'inspect', run.id, '--task', task.id]) }] : []),
			...(task.scheduledAt || task.startedAt ? [{ label: 'Where its time went', command: runCommand(data.namespace, ['run', 'timing', run.id, '--task', task.id]) }] : [])
		];
	}
	// The selected task's or item's commands: the Item card's terminal icon.
	const activeCli = $derived.by(() => {
		const task = activeView;
		if (!run || !task) return [];
		const item = task_isFanOut(task) ? activeItem : undefined;
		if (item !== undefined) {
			const words = ['logs', 'artifacts', 'attempts'];
			return words.map((command) => ({ label: `This item's ${command}`, command: itemCli({ namespace: data.namespace, runId: run.id, taskId: task.id, item, command }) }));
		}
		return taskCliOf(task);
	});

	const runCli = $derived(
		run
			? [
					...(isTerminal(runStatus) ? [] : [{ label: 'Watch this run', command: runCommand(data.namespace, ['run', 'watch', run.id]) }]),
					{ label: 'What this run is doing', command: runCommand(data.namespace, ['run', 'inspect', run.id]) },
					{ label: 'Where its time went', command: runCommand(data.namespace, ['run', 'timing', run.id]) },
					{ label: 'The run, as JSON', command: getCommand },
					{ label: 'Run it again', command: againCommand },
					...(isTerminal(runStatus) ? [] : [{ label: 'Cancel it', command: runCommand(data.namespace, ['run', 'cancel', run.id]) }])
				]
			: []
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
	{#snippet startedBy()}
		<span data-testid="run-origin" class="inline-flex flex-wrap items-center gap-x-1.5">
			{#if originSlug}
				<span data-testid="run-origin-kind" class={originSlug === 'submit' ? 'sr' : ''}>{originSlug === 'run-again' ? 'Run again' : originSlug === 'parent-run' ? 'Parent run' : originSlug[0].toUpperCase() + originSlug.slice(1)}</span>
				{#if origin?.id}
					{#if originHref}
						<a href={originHref} data-testid="run-origin-link" class="font-mono text-sm font-semibold">{origin.id.startsWith('ork-') ? shortRunId(origin.id) : origin.id}</a>
					{:else}
						<b data-testid="run-origin-id" class="font-semibold">{origin.id}</b>
					{/if}
				{/if}
				{#if originRootHref && origin}
					<span data-testid="run-origin-root-kind">Root run</span>
					<a href={originRootHref} data-testid="run-origin-root-link" class="font-mono text-sm font-semibold">{shortRunId(origin.rootRunId)}</a>
				{/if}
				{#if origin?.description}<span data-testid="run-origin-description" class="text-ink-3">({origin.description})</span>{/if}
				{#if origin?.reference}
					{#if originReference}
						<a href={originReference} target="_blank" rel="noopener noreferrer" data-testid="run-origin-reference" class="break-all">{origin.reference}</a>
					{:else}
						<span data-testid="run-origin-reference" class="font-mono text-sm break-all text-ink-2">{origin.reference}</span>
					{/if}
				{/if}
				{#if origin?.principal}<span data-testid="run-origin-principal"><span class={originSlug === 'submit' ? 'sr' : ''}>{'by '}</span><Principal name={origin.principal} /></span>{:else if originSlug === 'submit'}<span class="text-ink-3" data-testid="run-origin-unasserted">no principal asserted</span>{/if}
			{:else}
				<span data-testid="run-origin-kind" class="text-ink-3" data-tip={UNRECORDED_CAPTION}>{UNRECORDED}</span>
			{/if}
		</span>
	{/snippet}
	{#snippet startedWhen()}<TimeAgo clock at={startedAt ?? run?.startedAt ?? run?.createdAt} testid="run-started" />{/snippet}
	{#snippet finishedWhen()}<TimeAgo at={finishedAt ?? run?.finishedAt} since={startedAt ?? run?.startedAt ?? run?.createdAt} testid="run-finished" />{/snippet}
	{#snippet lastedFor()}<b class="num" data-testid="run-lasted">{lasted}</b>{/snippet}
	{#snippet progressFact()}<ProgressRing {progress} status={runStatus} testid="run-progress" />{#if isTerminal(runStatus) && (progress.skipped > 0 || neverTasks.length > 0)}<span class="text-sm text-ink-3" data-testid="run-progress-words">{[`${progress.ran} ran`, progress.skipped ? `${progress.skipped} skipped` : '', neverTasks.length ? `${neverTasks.length} never started` : ''].filter(Boolean).join(' · ')}</span>{/if}{/snippet}
	{#snippet canceledBy()}{#if run?.canceledBy}<span data-testid="run-canceled-by">by <Principal name={run.canceledBy} /></span>{/if}{/snippet}

	<header class="rh" data-testid="run-header">
		<h1 class="page-h1 rh-title">
			<a
				href={withNamespace(`/orkflows/${encodeURIComponent(run.orkflowName)}?version=${encodeURIComponent(run.orkflowVersion)}`, data.namespace)}
				data-testid="pinned-orkflow-link"
				class="text-ink no-underline hover:underline">{run.orkflowName}</a
			><StatusPill status={runStatus} testid="run-status" />
		</h1>
		<div class="rh-actions">
			<button type="button" data-testid="run-evidence" aria-disabled={isTerminal(runStatus) ? undefined : 'true'} aria-describedby={isTerminal(runStatus) ? undefined : 'run-evidence-why'} data-tip={isTerminal(runStatus) ? undefined : 'A bundle is for a finished run'} onclick={() => { if (isTerminal(runStatus)) evidenceDialogOpen = true; }} class="btn btn-quiet">Evidence</button>
			{#if !isTerminal(runStatus)}<span class="sr" id="run-evidence-why">A bundle is for a finished run</span>{/if}
			<button type="button" data-testid="compare-run" onclick={() => void openCompare()} class="btn btn-quiet">Compare</button>
			<a
				href={eventsHref(!eventsOpen)}
				data-testid="events-toggle"
				data-sveltekit-reset="false"
				aria-expanded={eventsOpen}
				aria-controls="run-events"
				class="btn btn-quiet">Events</a
			>
			{#if composesAny}
				<a href={childrenListHref(data.namespace, run.id)} data-testid="run-children-link" class="btn btn-quiet">Child runs</a>
				{#if originSlug !== 'parent-run'}
					<a href={chainListHref(data.namespace, run.id)} data-testid="run-chain-link" class="btn btn-quiet">Chain runs</a>
				{/if}
			{/if}
			<a class="btn" href={withNamespace(`/orkflows/${encodeURIComponent(run.orkflowName)}?tab=edit&version=${encodeURIComponent(run.orkflowVersion)}`, data.namespace)} data-testid="run-edit" data-tip="Edit from {run.orkflowVersion}"><Icon name="edit" />Edit</a>
			{#if !isTerminal(runStatus)}
				<span class="act-gap act-break" aria-hidden="true"></span>
				<button
					type="button"
					data-testid="cancel-run"
					aria-disabled={cancellationRequested ? 'true' : undefined}
					onclick={() => {
						if (cancellationRequested) return;
						cancelDialog = true;
						cancelError = '';
					}}
					class="btn"><Icon name="ban" />{cancellationRequested ? 'Cancellation requested' : 'Cancel run'}</button
				>
			{/if}
			<button type="button" data-testid="run-again" onclick={() => (runAgainDialogOpen = true)} class="btn btn-primary"><Icon name="play" />Run again</button>
			<CliPopover testid="run-inspect-cli" title="Command line: this run" commands={[...runCli, ...activeCli]} />
		</div>
		<div class="rh-plates">
			<Plate key="run" value={run.id} short={shortRunId(run.id)} copyLabel="Copy run id" testid="run-id" />
			<a class="plate-open" data-testid="run-version-link" href={withNamespace(`/orkflows/${encodeURIComponent(run.orkflowName)}?version=${encodeURIComponent(run.orkflowVersion)}`, data.namespace)} aria-label="Version {shortVersion(run.orkflowVersion)}{versionTip(run.orkflowVersion) ? ` (${run.orkflowVersion})` : ''} of {run.orkflowName}" data-tip="Open this version"><Plate key="version" value={run.orkflowVersion} testid="run-version" inLink /></a>
		</div>
		<div class="rh-facts">
			<FactsStrip
				facts={[
					{ label: originSlug === 'submit' ? 'Submitted by' : 'Started by', value: startedBy },
					{ label: 'Started', value: startedWhen },
					...(isTerminal(runStatus) && (finishedAt ?? run.finishedAt) ? [{ label: 'Finished', value: finishedWhen }] : []),
					{ label: isTerminal(runStatus) ? 'Ran for' : 'Running for', value: lastedFor },
					{ label: 'Progress', value: progressFact },
					...(runStatus === Status.CANCELED && run.canceledBy ? [{ label: 'Canceled', value: canceledBy }] : [])
				] satisfies Fact[]}
			/>
			{#if !isTerminal(runStatus)}
				<LiveControl {paused} ontoggle={toggleLive} testid="run-live" />
				{#if socketState !== 'open'}
					<span data-testid="live-badge" class="text-sm font-semibold {socketState === 'failed' ? 'text-fail' : 'text-ink-3'}"
						>{socketState === 'failed' ? 'STOPPED' : streamBroken ? 'NOT CONNECTED' : 'CONNECTING'}</span
					>
				{:else}
					<span data-testid="live-badge" class="sr">LIVE</span>
				{/if}
			{/if}
			{#if historyOpen}
				{#if historyLoading}
					<span class="text-sm text-ink-3" data-testid="run-stability-loading">Reading the retry history…</span>
				{:else if historyError}
					<span class="text-sm text-fail" data-testid="run-stability-error">Retry history unavailable: {historyError}</span>
				{:else if history}
					<span class="text-sm text-ink-3" data-testid="run-stability-note">{historyNote}</span>
				{/if}
			{/if}
			{#if runRetries > 0}
				<span data-testid="run-retries" class="text-sm text-ink-3">{runRetries} task {runRetries === 1 ? 'retry' : 'retries'}</span>
			{/if}
		</div>
	</header>
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
	{#if firstWaiter?.capacityWait}
		<!-- Why something that should be running is not: one amber line above the graph. -->
		{@const wait = firstWaiter.capacityWait}
		{@const reserved = reservedOf(wait.reserved, wait.capacity)}
		<div class="capline mb-4" data-testid="run-capacity-note">
			<span class="mark-wait" aria-hidden="true"></span>
			<!-- The live region is this sentence alone: the clock beside it must not be read out every second. -->
			<b role="status" class="font-semibold text-wait">{waiterLabel(firstWaiter)} is waiting for capacity{#if capacityWaiters.length > 1}, and {capacityWaiters.length - 1} more{/if}</b>
			<span class="min-w-0 text-ink-2">every Orker of pool {wait.pool} is full{#if reserved}, {reserved} reserved{/if} · {waiterSeconds(firstWaiter)}</span>
			<a class="ml-auto font-semibold whitespace-nowrap" data-testid="run-capacity-pool" href={withNamespace(`/admin/pools#pool-${encodeURIComponent(wait.pool)}`, data.namespace)}>View pool {wait.pool}</a>
		</div>
	{/if}
	{#if firstOutageWaiter?.storeOutageWait}
		<!-- Why something that should be running is not: the same amber line, naming the outage. -->
		{@const outage = firstOutageWaiter.storeOutageWait}
		<div class="capline mb-4" data-testid="run-store-outage-note">
			<span class="mark-wait" aria-hidden="true"></span>
			<b role="status" class="font-semibold text-wait">{waiterLabel(firstOutageWaiter)} is waiting for object storage{#if outageWaiters.length > 1}, and {outageWaiters.length - 1} more{/if}</b>
			<span class="min-w-0 text-ink-2">the Orkers of pool {outage.pool} cannot reach it, so no attempt is being used · {waiterSeconds(firstOutageWaiter)}</span>
			<a class="ml-auto font-semibold whitespace-nowrap" data-testid="run-store-outage-pool" href={withNamespace(`/admin/pools#pool-${encodeURIComponent(outage.pool)}`, data.namespace)}>View pool {outage.pool}</a>
		</div>
	{/if}
	{#if runStatus === Status.FAILED && failedTask}
		<!-- The failure, said once above the graph: which task, its reason code and its plain sentence. -->
		<div class="alert alert-fail mb-4" role="status" data-testid="run-failure-banner">
			<span class="alert-ic"><Icon name="alert" /></span>
			<div class="min-w-0 flex-[1_1_15rem]">
				<b>{failedTask.id} failed{#if failedTask.error?.code}: <code>{failedTask.error.code}</code>{/if}</b>
				{#if failedTask.error?.message}<p class="mt-0.5 text-ink-2" data-testid="run-failure-sentence">{failureSentence(failedTask.error.message)}</p>{/if}
			</div>
			<a class="btn btn-sm ml-auto" href={taskHref(failedTask.id, failedTask.attempt)} onclick={(e) => viewStderr(e, failedTask!)} data-testid="run-failure-view-stderr">View stderr</a>
		</div>
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
			class="capline mb-4 text-sm"
		>
			{failingBannerText(failingHandlers)}
		</p>
	{/if}

	<!-- THE GRAPH: the run's tasks, what is selected and what it touches. -->
	<div class="mb-5 min-w-0">
		<DrawingFrame
			testid="run-drawing"
			title="Graph"
			sub={graphSub}
			caption="{run.orkflowName} @ {shortVersion(run.orkflowVersion)} · {tasks.length} {tasks.length === 1 ? 'task' : 'tasks'}"
			tasks={data.orkflow?.tasks ?? null}
			unavailable={data.orkflowError ?? 'the definition was not found'}
			orkflowLabel="{run.orkflowName}@{shortVersion(run.orkflowVersion)}"
			on="run"
			dagLabel="Graph of {run.orkflowName}.{activeTask ? ` Selected: ${activeTask}.` : ' No task selected.'}"
			stateOf={nodeState}
			selected={activeTask}
			hrefOf={(id) => taskHref(id, 0)}
			onselect={toggleNode}
			neighbourhoodWhenLarge
			direction={page.url.searchParams.has('dag') ? dagDirectionParam(page.url) : 'LR'}
			iconOf={(id) => activityByTask.get(id)?.icon}
		>
			{#snippet actions()}
				<button type="button" class="btn btn-sm btn-quiet" data-testid="run-dag-clear" aria-disabled={!activeTask} onclick={() => activeTask && clearSelection()}>Clear selection</button>
			{/snippet}
			{#snippet footer()}
				<div class="legend" data-testid="run-dag-key">
					{#each legendStatuses as status (status)}
						{@const mark = statusMark(status)}
						<span class="legend-i"
							><span class="stamp border-transparent bg-transparent p-0 {mark.inner ? '' : mark.className}" aria-hidden="true">{#if mark.inner}<span class={mark.className}></span>{/if}</span>{statusLabel(status)}</span
						>
					{/each}
					{#each legendActivities as activity (activity.word)}
						<span class="legend-i"><ActivityIcon kind={activity.kind} icon={activity.icon} class="size-3.5 text-ink-3" />{activity.word}</span>
					{/each}
					<span class="legend-note text-ink-3">
						{#if anyHandlers}Failure handlers run only in a failing run and never rescue it.{:else if activeTask}Greyed tasks are outside the selection.{/if}
					</span>
				</div>
			{/snippet}
		</DrawingFrame>
	</div>

	<!-- What the run took and what it returned, between its drawing and its timeline. -->
	{#if !isTerminal(runStatus) || hasRunInput || run.output}
		<div class="run-values">
			{#if !isTerminal(runStatus) || hasRunInput || run.output?.data}
			<section class="card" aria-labelledby="run-inputs-heading" data-testid="run-inputs">
				<div class="card-head">
					<h2 id="run-inputs-heading">Inputs</h2>
					{#if hasRunInput}
						<div class="card-end">
							<button type="button" class="btn btn-sm btn-quiet" data-testid="run-inputs-json" aria-pressed={inputsAsJson} onclick={() => (inputsAsJson = !inputsAsJson)}>JSON</button>
							<button type="button" class="btn btn-sm btn-quiet" data-testid="copy-run-input" onclick={() => void copy(payloadJson(run.input), 'Inputs copied', 'run-input')}>Copy</button>
						</div>
					{/if}
				</div>
				{#if hasRunInput && (inputsAsJson || inputEntries.length === 0)}
					<pre data-testid="run-input" class="valbox">{payloadJson(run.input)}</pre>
				{:else if hasRunInput}
					<dl class="kv kv-stack" data-testid="run-input">
						{#each inputEntries as [key, value] (key)}
							<dt>{key}</dt>
							<dd><code>{value}</code></dd>
						{/each}
					</dl>
				{/if}
				{#if !hasRunInput || !run.output}
					<p data-testid="run-values-note" class="pad text-ink-2">
						{#if !hasRunInput}<span data-testid="run-input">This run took no input values.</span>{/if}
						{#if !run.output}<span data-testid="run-output"
								>{isTerminal(runStatus)
									? `It finished without ${declaresOutputs ? 'its declared outputs' : 'aggregated output values'}.`
									: `Its ${declaresOutputs ? 'declared outputs are' : 'output is'} not available yet.`}</span
							>{/if}
					</p>
				{/if}
			</section>
			{/if}
			{#if run.output}
				<section class="card" aria-labelledby="run-output-kind" data-testid="run-outputs">
					<div class="card-head">
						<h2 id="run-output-kind" data-testid="run-output-kind">Outputs</h2>
						<OutputsMark declared={declaresOutputs} />
						<div class="card-end"><button type="button" class="btn btn-sm btn-quiet" data-testid="copy-run-output" onclick={() => void copy(payloadJson(run.output), 'Run output copied', 'run-output')}>Copy</button></div>
					</div>
					<pre data-testid="run-output" class="valbox">{payloadJson(run.output)}</pre>
				</section>
			{/if}
		</div>
	{/if}

	<p class="sr" role="status" data-testid="item-card-status">{selectionWords}</p>
	{#if declaredSprites.size > 0}
		<svg class="sr" aria-hidden="true" focusable="false"><defs>{#each [...declaredSprites] as [icon, id] (id)}<ActivitySymbol {id} {icon} />{/each}</defs></svg>
	{/if}
	<div class="run-grid">
		<div class="run-col">
			<!--
				The run's history reads BEFORE the tasks it produced. It is the
				run-level account of what happened, so it belongs beside the run's
				own header rather than under a task list a reader has to scroll past.
			-->
			{#if eventsOpen}
				<section id="run-events" aria-label="Run events" data-testid="run-events">
					{#await import('#lib/components/EventInspector.svelte') then { default: EventInspector }}
						<EventInspector
							events={buffer.events}
							dropped={buffer.dropped}
							total={buffer.total}
							{streamBroken}
							runId={run.id}
							cap={EVENT_BUFFER_CAP}
						/>
					{/await}
				</section>
			{/if}
			<section class="card tasks" aria-labelledby="run-tasks-heading">
				<div class="card-head">
					<h2 id="run-tasks-heading">Tasks</h2>
					<span class="tlkey" aria-hidden="true"
						><span><i class="k-r"></i>ran</span>{#if !isTerminal(runStatus)}<span><i class="k-run"></i>running</span>{/if}{#if anyWait}<span><i class="k-w"></i>waited</span>{/if}{#if runRetries > 0 || runStatus === Status.FAILED}<span><i class="k-c"></i>failed attempt</span>{/if}</span
					>
					<div class="card-end">
						{#if hasFanOut}
							<button type="button" class="btn btn-sm btn-quiet" data-testid="run-expand-all" aria-pressed={expandAll} onclick={() => (expandAll = !expandAll)}>{expandAll ? 'Collapse' : 'Expand all'}</button>
						{/if}
						<MoreMenu testid="run-tasks-more" iconOnly label="More about these tasks">
							{#snippet items(close)}
								<button type="button" data-testid="run-orkers-toggle" aria-pressed={orkersShown} data-tip="Which Orker ran each task, on its row" onclick={() => { close(); toggleOrkers(); }}>{orkersShown ? 'Hide Orkers' : 'Show Orkers'}</button>
								<button type="button" data-testid="run-stability-toggle" aria-pressed={historyOpen} data-tip="Whether each task retries in this Orkflow's other runs, over {HISTORY_WINDOW}" onclick={() => { close(); toggleHistory(); }}>{historyOpen ? 'Hide retry history' : 'Retry history'}</button>
								<button type="button" data-testid="export-run-json" onclick={() => { close(); downloadJson(`${run.id}.json`, runExport()); }}>Export JSON</button>
							{/snippet}
						</MoreMenu>
					</div>
				</div>
				<p class="sr" data-testid="task-summary"><span>{tasks.filter((t) => t.status === Status.SUCCEEDED).length}/{tasks.length}</span> succeeded{#if tasks.some((t) => t.status === Status.AWAITING)}, {tasks.filter((t) => t.status === Status.AWAITING).length} awaiting{/if}{#if cachedTotal > 0}<span data-testid="task-summary-cached"> · {cachedTotal} cached</span>{/if}</p>
				{#if !isTerminal(runStatus)}
					{@const fan = tasks.filter((t) => t.items.length > 0)}
					<div class="guide">
						<span>Tasks <b>1–{tasks.length}</b> of <b>{tasks.length}</b>{#if fan.length === 1}{' · '}items <b>{itemLabel(fan[0].id, 0)}–{itemLabel(fan[0].id, fan[0].items.length - 1)}</b>{/if}</span>
						<span>Showing all</span>
					</div>
				{/if}
				{#if artifactsError}
					<p data-testid="artifacts-unavailable" class="tasks-note">Artifact metadata is unavailable: {artifactsError}. Downloads require the engine's recorded-artifact read API and are never linked directly to storage URIs.</p>
				{/if}
				<!--
					The attempt store is not the run: a deployment that has none, or
					one whose store is unreachable, still renders every status, every
					log and every artifact on this page. Say what is missing and what
					it costs — nothing else changes.
				-->
				{#if attemptsError}
					<p data-testid="attempts-unavailable" class="tasks-note">Attempt history is not available: {attemptsError}. Task statuses, output and artifacts are unaffected; what each earlier try of a retried task did cannot be shown.</p>
				{/if}
				{#if artifactDownloadError}
					<p data-testid="artifact-download-error" role="alert" class="tasks-note text-fail">Error: artifact download failed: {artifactDownloadError}</p>
				{/if}
				<div class="trow thead" aria-hidden="true">
					<span>Task</span>
					<span class="tl-axis" {@attach observeAxis}>{#each axis as mark, i (i)}<span style:left="{mark.at * 100}%" class:first={i === 0}>{mark.label}</span>{/each}</span>
					<span class="text-right">Status</span>
				</div>
					{#snippet rowExtras(task: TaskView, kind: RowKind, line: ReturnType<typeof barsOf>)}
						{#if task.child?.runId}{' · '}child run <a class="font-mono" data-testid="task-{task.id}-child-run" href={withNamespace(`/runs/${encodeURIComponent(task.child.runId)}`, data.namespace)}>{shortRunId(task.child.runId)}</a>{/if}
						{#if kind.fanOut}<span>{' · '}{fanOutWords(task)}</span>{/if}
						{#if task.join && joinWords(task.join)}{' · '}<span data-testid="task-{task.id}-join" data-tip={joinTitle(task.join, kind.def?.needs ?? [])}>{joinWords(task.join)}</span>{/if}
						{#if task.pin}{' · '}<span data-testid="task-{task.id}-pin"><i class="pin-dot" style="background-color:var(--color-{kind.accent}-line)"></i>Pin {task.pin}</span>{/if}
						{#if task.pin || orkersShown}
							{@const orkers = orkersOfRow(task)}
							{#if orkers}{' · '}<span class="torker" data-testid="task-{task.id}-orker" data-tip={orkers.tip ?? orkers.word}>{#if !orkers.tip}<span class="sr">{'Orker '}</span>{/if}{orkers.word}</span>{/if}
						{/if}
						{#if rowReason(task, kind)}{' · '}<span data-testid="task-{task.id}-reason-word">{rowReason(task, kind)}</span>{/if}
						{#if task.status === Status.RUNNING}
							{@const live = liveByTask.get(task.id)}
							{#if live && !live.heartbeatAvailable && line.words === ''}<span class="sr">{liveLine(live, liveNowMs).text}</span>{/if}
						{/if}
						{#if kind.fanOut}
							<!-- A fan-out retries per item: the mark counts the items that took more than one try. -->
							{@const retried = retriedItemsLabel(task.items)}
							{#if retried}{' · '}<span data-testid="task-{task.id}-retries" data-tip="These items were retried. Open an item for the outcome and error of every attempt.">{retried}</span>{/if}
						{:else}
							<RetryBadge attempts={attemptsByTask.get(task.id) ?? NO_ATTEMPTS} terminal={isTerminal(task.status)} testid="task-{task.id}-retries" history={flakesByTask.get(task.id)} historyWindow={HISTORY_WINDOW} />
							<CacheBadge cache={task.cache} declared={kind.cached} succeeded={task.status === Status.SUCCEEDED && task.factsRead === true} namespace={data.namespace} testid="task-{task.id}-cache" />
						{/if}
					{/snippet}
					<!--
						The rows are this page's cost at thousands of tasks, so a row is as few
						reactive parts as it can be: one derived for what its definition says,
						one for its bars, the rare words behind one {#if} and the rare
						expansions behind another.

						A pinned task's row carries its pin's line and a faint fill, so the
						members of one pin read as a block without being re-sorted: the accent
						is an inline style because the token is chosen at run time, and the
						words `Pin <name>` say the same thing.
					-->
					{#snippet taskRows(list: TaskView[], lit: boolean)}
						{#each list as task (task.id)}
							{@const kind = rowKind(task)}
							{@const line = barsOf(task)}
							<div
								class="trow"
								class:pinned={!!kind.accent}
								style={kind.accent ? `border-color:var(--color-${kind.accent}-line);background-color:var(--color-${kind.accent}-fill)` : undefined}
								data-testid="task-{task.id}"
								data-dispatched={kind.dispatched}
								data-pin={task.pin || undefined}
								data-pin-accent={kind.accent}
								><span class="tname-col"
									><button type="button" class="tname" aria-pressed="false" data-testid="task-deep-link-{task.id}" onclick={() => toggleRow(task.id)}
										><svg viewBox="0 0 16 16" aria-hidden="true" focusable="false" class="rico"><use href={lit ? iconHref(task.id, kind) : undefined} /></svg>{task.id}</button
									><small class="tsub" class:clips={rowOrkerClips(task, kind)}
										>{kind.word}{#if kind.never || kind.waiting}{' · '}<span data-testid="task-{task.id}-status-note" data-tip={kind.never ? task.message || undefined : undefined}>{kind.never ? 'never started' : notStartedCaption(task)}</span>{/if}{#if rowHasExtras(task, kind)}{@render rowExtras(task, kind, line)}{/if}</small
									></span
								><span class="tl" role={line.bars.length ? 'img' : undefined} aria-label={line.bars.length ? line.words : undefined} data-testid="task-{task.id}-timing"
									>{#each line.bars as bar, i (i)}<i class="tl-{bar.kind}" data-testid="task-{task.id}-{bar.kind === 'wait' ? 'wait' : 'run'}-segment" style:left="{bar.left * 100}%" style:width="{bar.width * 100}%"></i>{/each}</span
								><span class="stc"
									><span data-testid="task-{task.id}-status" data-state={kind.waiting ? 'not-started' : undefined} class={kind.pill.cls}
										>{#if kind.pill.mark}<span class={kind.pill.mark} aria-hidden="true"></span>{/if}{kind.pill.word}</span
									><span class="num tdur" data-testid="task-{task.id}-elapsed"
										>{#if task.status === Status.RUNNING || task.status === Status.AWAITING}<span class="sr">{kind.isAwait ? 'Awaited ' : 'Since scheduled '}</span>{/if}{rowDuration(task)}</span
									></span
								></div
							>
							{#if task.status === Status.FAILED || kind.isAwait || kind.composed || kind.fanOut}
								{@const def = kind.def}
								{@const isAwait = kind.isAwait}
								{@const fanOut = kind.fanOut}
								{@const composed = kind.composed}
								{@const taskAttempts = attemptsByTask.get(task.id) ?? NO_ATTEMPTS}
						{#if task.status === Status.FAILED && (task.id === failedTask?.id || task.id === activeTask)}
							<!-- The run's failed task says everything about its failure under its row; another failed task, when it is selected. -->
							{@const shownAttempts = taskAttempts}
							{@const lastAttempt = shownAttempts[shownAttempts.length - 1]}
							{@const phases = lastAttempt ? attemptPhases(lastAttempt) : null}
							{@const groups = phases?.kind === 'phases' ? phaseGroups(phases.phases) : []}
							{@const groupTotal = groups.reduce((sum, g) => sum + g.ms, 0)}
							{@const image = lastAttempt?.image ?? def?.image}
							{@const digest = image?.indexDigest ?? ''}
							{@const timing = taskTiming({ ...task, awaited: isAwait }, attemptStoreAbsent ? undefined : taskAttempts)}
							<div class="xpand" data-row-open data-testid="task-{task.id}-failure">
								<p class="xhead">
									<b>{task.id}</b>
									<span class="text-ink-3">{[
										task.attempt > 0 || shownAttempts.length > 0 ? `attempt ${Math.max(task.attempt, shownAttempts.length)} of ${Math.max(def?.retry?.maximumAttempts ?? 1, task.attempt, shownAttempts.length)}` : '',
										task.exitCode !== undefined ? `exit code ${task.exitCode}` : '',
										def && !def.retry ? 'no retry policy' : ''
									].filter(Boolean).join(' · ')}</span>
								</p>
<div class="xbody">
								{#if !composed && !isAwait && !fanOut}
									{#snippet wokerWord()}<span class="font-mono text-xs">{task.orkerId || lastAttempt?.orkerId || '—'}</span>{/snippet}
									{#snippet imagePlate()}<span data-testid={task.id === failedTask?.id ? 'run-failed-image' : undefined}><span class="sr">{pinnedOrReference(image)}</span>{#if digest}<Plate key="sha256" value={digestHead(digest)} tip={digest} small />{:else}<span class="font-mono text-xs break-all">{pinnedOrReference(image) || '—'}</span>{/if}</span>{/snippet}
									<FactsStrip
										small
										label="Attempt of {task.id}"
										facts={[
											{ label: 'Orker', value: wokerWord },
											{ label: 'Pool', value: task.pool || lastAttempt?.pool || '—' },
											...(lastAttempt?.resources && fmtQuantity(lastAttempt.resources) ? [{ label: 'Reserved', value: fmtQuantity(lastAttempt.resources) }] : []),
											{ label: 'Waited to start', value: timing.wait.kind === 'value' ? formatDuration(timing.wait.from, timing.wait.to) : '—' },
											...(task.finishedAt ? [{ label: 'Finished', testid: `task-${task.id}-finished`, value: finishedWords(timestampDate(task.finishedAt), task.startedAt ? timestampDate(task.startedAt) : undefined) }] : []),
											{ label: 'Image', value: imagePlate }
										] satisfies Fact[]}
									/>
								{/if}
								{#if task.error}
									<FailurePanel error={task.error} testid="task-{task.id}-error" flat tries={triesOf(task)} />
								{/if}
								{#if groupTotal > 0}
									<div class="phline">
										<div class="pbar pbar-sm" role="img" aria-label="Phases of attempt {lastAttempt?.attempt}: {groups.map((g) => `${g.label} ${(g.ms / 1000).toFixed(1)} seconds`).join(', ')}">
											{#each groups as group (group.key)}<i class="ph-{group.key}" style:width="{(100 * group.ms) / groupTotal}%"></i>{/each}
										</div>
										<span class="pkey-in" aria-hidden="true">{#each groups as group (group.key)}<span><i class="ph-{group.key}"></i>{group.label.toLowerCase()} {(group.ms / 1000).toFixed(1)}s</span>{/each}</span>
									</div>
								{/if}
</div>
								{#if !composed && !isAwait && !fanOut && (task.id === failedTask?.id || task.id === activeTask)}
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
										onFirstStderr={task.id === failedTask?.id ? (text) => (firstStderr = { taskId: task.id, text }) : undefined}
										toggle={false}
										open
										embedded
										label={task.id}
									/>
								{/if}
								{#if !composed && !isAwait && !fanOut}
									<div class="xfoot"><button type="button" class="btn btn-sm btn-quiet btn-disclosure" data-testid="task-inspect-toggle-{task.id}" aria-expanded={inspectOpenByTask[task.id] ?? false} onclick={() => (inspectOpenByTask[task.id] = !inspectOpenByTask[task.id])}>{inspectOpenByTask[task.id] ? 'Hide inspect' : 'Inspect'}</button></div>
								{/if}
								{#if !composed && !isAwait && !fanOut && inspectOpenByTask[task.id]}
									{#await import('#lib/components/TaskInspect.svelte') then { default: TaskInspect }}
										<TaskInspect
											runId={run.id}
											namespace={data.namespace}
											{task}
											{def}
											isAwait={false}
											isComposed={false}
											attempts={taskAttempts}
											{attemptsSeeded}
											{attemptsError}
											{timing}
											artifacts={(artifactsByTask.get(task.id) ?? []).filter((record) => record.item === undefined)}
											downloading={downloadingArtifact}
											onDownload={(record) => void downloadArtifact(record)}
											onViewLogs={(attempt) => viewTaskAttemptLogs(task.id, attempt)}
											onCopyValues={() => void copy(payloadJson(task.output), 'Task values copied', `task-values-${task.id}`)}
										/>
									{/await}
								{/if}
								{#if runStatus === Status.FAILED && task.id === failedTask?.id && task.error?.code}
									<SeenBefore namespace={data.namespace} orkflow={run.orkflowName} taskId={task.id} code={task.error.code} runId={run.id} />
								{/if}
							</div>
						{/if}
						{#if isAwait && (!isTerminal(task.status) || task.id === activeTask)}
							<!--
								An awaited task runs no container: no log pane, and no image
								to inspect. The panel says what it waits for, until when, who
								resolved it — and resolves it.
							-->
							<div class="xpand" data-row-open>
								{#await import('#lib/components/AwaitPanel.svelte') then { default: AwaitPanel }}
									<AwaitPanel
										runId={run.id}
										namespace={data.namespace}
										task={{ id: task.id, status: task.status, await: task.await, error: task.error, startedAt: task.startedAt }}
										spec={def?.await}
										runTerminal={isTerminal(runStatus)}
										now={currentTimestamp}
										origin={page.url.origin}
									/>
								{/await}
							</div>
						{:else if composed && (!isTerminal(task.status) || task.id === activeTask)}
							<!-- A composed task starts a child run: its panel names the pinned child and rolls its tasks up live. -->
							<div class="xpand" data-row-open>
								{#await import('#lib/components/ChildRunPanel.svelte') then { default: ChildRunPanel }}
									<ChildRunPanel runId={run.id} namespace={data.namespace} taskId={task.id} child={task.child} status={task.status} />
								{/await}
							</div>
						{:else if fanOut && (expandAll || task.id === activeTask || !isTerminal(task.status) || task.status === Status.FAILED)}
							<div data-row-open><EachItemGrid
								taskId={task.id}
								each={task.each}
								spec={def?.each}
								items={task.items}
								selected={task.id === activeTask ? activeItem : undefined}
								onSelect={(item) => selectItem(task.id, item)}
								waitingOf={(item) => waitingFor(liveByTask.get(attemptsKey(task.id, item)))}
								waitingWord={task.items.some((i) => liveByTask.get(attemptsKey(task.id, i.index))?.storeOutageWait) ? 'waiting for object storage' : undefined}
							/></div>
						{/if}
							{/if}
						{/each}
					{/snippet}
					<div data-testid="task-list">
						{#if rowChunks}
							<!-- Cut into chunks a long list's layout can skip whole, not row by row. -->
							{#each rowChunks as chunk, c (c)}<div class="rows-chunk" {@attach lightChunk(c)}>{@render taskRows(chunk, litChunks.has(c))}</div>{/each}
						{:else}
							{@render taskRows(tasks, true)}
						{/if}
					</div>
				<!-- THE LOG: one pane that follows the selection, inside the task panel. -->
				{#snippet selectedLog()}
					{#if activeView && !(task_isAwait(activeView) || task_isComposed(activeView))}
						{@const fanned = task_isFanOut(activeView)}
						<div data-run-log>
						{#if fanned && activeItem === undefined}
							<p class="log-none" data-testid="run-log-pick-item">Select an item of {activeView.id} to see its log.</p>
						{:else if fanned && activeItem !== undefined}
							{@const current = activeView.items[activeItem]}
							{#key `${activeView.id}[${activeItem}]`}
								<TaskLogPane
									runId={run.id}
									taskId={activeView.id}
									namespace={data.namespace}
									item={activeItem}
									status={current?.status ?? Status.PENDING}
									scheduledAt={current?.run?.scheduledAt ?? current?.run?.startedAt ?? liveByTask.get(attemptsKey(activeView.id, activeItem))?.attemptScheduledAt}
									attempt={deepTask === activeView.id && deepItem === activeItem && Number.isFinite(deepAttempt) ? deepAttempt : 0}
									deepLinked={deepTask === activeView.id && deepItem === activeItem}
									pinRequest={logPinByTask[attemptsKey(activeView.id, activeItem)]}
									onAttemptChange={(attempt) => setTaskAttempt(activeView!.id, attempt, activeItem)}
									onAttemptSeen={(attempt) => attemptSeen(activeView!.id, attempt, activeItem)}
									toggle={false}
									open
									label={itemLabel(activeView.id, activeItem)}
								/>
							{/key}
						{:else}
							{#key activeView.id}
								<TaskLogPane
									runId={run.id}
									taskId={activeView.id}
									namespace={data.namespace}
									status={activeView.status}
									scheduledAt={activeView.scheduledAt ?? activeView.startedAt}
									attempt={deepTask === activeView.id && Number.isFinite(deepAttempt) ? deepAttempt : 0}
									deepLinked={deepTask === activeView.id}
									pinRequest={logPinByTask[activeView.id]}
									onAttemptChange={(attempt) => setTaskAttempt(activeView!.id, attempt)}
									onAttemptSeen={(attempt) => attemptSeen(activeView!.id, attempt)}
									toggle={false}
									open
									label={activeView.id}
								/>
							{/key}
						{/if}
						</div>
					{/if}
				{/snippet}
				<!--
					THE TASK PANEL: what the selected task or item is, under its own row. It is
					one element the selection moves, so a new selection re-renders no row and a
					row costs nothing for being selectable. A failed container task says all of
					this in its own expansion instead.
				-->
			{#if activeView && !(activeView.status === Status.FAILED && task_isContainer(activeView))}
				{@const fanned = task_isFanOut(activeView)}
				{@const current = fanned && activeItem !== undefined ? activeView.items[activeItem] : undefined}
				{@const itemRun = current?.run}
				{@const key = attemptsKey(activeView.id, current?.index)}
				{@const facts = attemptsByTask.get(key) ?? []}
				{@const panel = current ? `task-${activeView.id}-item-panel` : `task-${activeView.id}`}
				{@const liveNow = liveByTask.get(key)}
				{@const def = taskDefs.get(activeView.id)}
				{@const timing = current ? undefined : taskTiming({ ...activeView, awaited: task_isAwait(activeView) }, attemptStoreAbsent ? undefined : facts, currentTimestamp)}
				<div class="xpand tpanel" data-testid="task-panel" {@attach underSelectedRow}>
<ItemCard
					title={current ? `Item ${itemLabel(activeView.id, current.index)}` : `Task ${activeView.id}`}
					prefix={panel}
					panelTestid={current ? panel : 'item-card'}
					status={current ? (liveNow?.state === LiveAttemptState.STARTED && !isTerminal(current.status) ? Status.RUNNING : current.status) : activeView.status}
					waiting={waitingFor(liveNow)}
					waitingFor={liveNow?.storeOutageWait ? 'Waiting for object storage' : undefined}
					attempts={facts}
					attempt={current ? current.attempt : activeView.attempt}
					orkerId={current ? (itemRun?.orkerId ?? '') : activeView.orkerId}
					pool={current ? itemRun?.pool || activeView.pool : activeView.pool}
					requested={def?.resources}
					scheduledAt={current ? (itemRun?.scheduledAt ?? liveNow?.attemptScheduledAt) : (activeView.scheduledAt ?? activeView.startedAt)}
					startedAt={current ? itemRun?.startedAt : activeView.startedAt}
					finishedAt={current ? (itemRun?.finishedAt ?? current.finishedAt) : activeView.finishedAt}
					live={liveNow}
					liveError={liveError}
					{liveNowMs}
					now={currentTimestamp}
					{timing}
					image={def?.image}
					reason={current || activeView.error ? '' : activeView.message}
					resolved={!current && task_isAwait(activeView) && activeView.status === Status.SUCCEEDED ? { runId: run.id, namespace: data.namespace, taskId: activeView.id, output: activeView.output, valuesRef: activeView.valuesRef } : undefined}
					imageReference={activityByTask.get(activeView.id)?.reference || def?.image?.reference}
					note={fanned && !current ? 'Select an item to see its attempts and log.' : ''}
				>
					{#if activeWaiter?.storeOutageWait}
						<WaitsFor label={waiterLabel(activeWaiter)} outage={activeWaiter.storeOutageWait} namespace={data.namespace} />
					{:else if activeWaiter?.capacityWait}
						<WaitsFor label={waiterLabel(activeWaiter)} wait={activeWaiter.capacityWait} namespace={data.namespace} ownRunning={poolPeers.running} took={poolPeers.took} />
					{/if}
					{#if current}
						{#if itemRun?.cache}<div class="insp-row"><CacheBadge cache={itemRun.cache} namespace={data.namespace} testid="{panel}-cache" /></div>{/if}
						{#if itemRun?.orkerId && isTerminal(current.status)}<span class="sr" data-testid="{panel}-exit">Exit {itemRun.exitCode}</span>{/if}
						<span class="sr" data-testid="{panel}-status">{itemStatusText(current)}</span>
						{#if current.message && !itemRun?.error?.message}<p class="insp-row text-dense text-ink-2">{current.message}</p>{/if}
						{#if itemRun?.error?.message}
							<div class="insp-row">
								<FailurePanel error={itemRun.error} testid="task-{activeView.id}-item-{current.index}-error" flat tries={{ retryable: itemRun.error.retryable, retryState: itemRun.retryState, attempts: attemptsSeen(facts) }} />
							</div>
						{/if}
						{@render selectedLog()}
						{#if facts.length > 1 || (facts.length > 0 && isTerminal(current.status)) || itemHasArtifacts(activeView.id, current.index)}
							<div class="insp-more">
								<button type="button" class="btn btn-sm btn-quiet btn-disclosure" data-testid="{panel}-inspect-toggle" aria-expanded={inspectOpenByTask[key] ?? false} onclick={() => (inspectOpenByTask[key] = !inspectOpenByTask[key])}>{inspectOpenByTask[key] ? 'Hide inspect' : 'Inspect'}</button>
							</div>
						{/if}
						{#if inspectOpenByTask[key] && (facts.length > 1 || (facts.length > 0 && isTerminal(current.status)))}
							<div class="insp-row">
								<AttemptTimeline runId={run.id} taskId={activeView.id} namespace={data.namespace} item={current.index} attempts={facts} pool={itemRun?.pool ?? ''} decidedOn={current.attempt} onViewLogs={(attempt) => viewTaskAttemptLogs(activeView!.id, attempt, current.index)} />
							</div>
						{/if}
						{@const itemArtifacts = (artifactsByTask.get(activeView.id) ?? []).filter((record) => record.item === current.index)}
						{#if inspectOpenByTask[key] && itemArtifacts.length > 0}
							<ul class="insp-row grid gap-1 text-xs" data-testid="{panel}-artifacts">
								{#each itemArtifacts as record (`${record.kind}:${record.artifact?.name}`)}
									{#if record.artifact}
										{@const dl = `${record.taskId}:${record.item}:${record.kind}:${record.artifact.name}`}
										<li class="flex flex-wrap items-center gap-2">
											<span class="font-mono break-all text-ink">{record.artifact.name}</span>
											<span class="text-ink-3">{record.artifact.sizeBytes} bytes</span>
											<button type="button" data-testid="download-artifact-{activeView.id}-item-{current.index}-{record.artifact.name}" onclick={() => void downloadArtifact(record)} disabled={downloadingArtifact === dl} class="link ml-auto font-medium disabled:opacity-50">{downloadingArtifact === dl ? 'Downloading…' : 'Download'}</button>
										</li>
									{/if}
								{/each}
							</ul>
						{/if}
					{:else if !fanned}
						{@render selectedLog()}
						<div class="insp-more">
							<button type="button" class="btn btn-sm btn-quiet btn-disclosure" data-testid="task-inspect-toggle-{activeView.id}" aria-expanded={inspectOpenByTask[activeView.id] ?? false} onclick={() => (inspectOpenByTask[activeView!.id] = !inspectOpenByTask[activeView!.id])}>{inspectOpenByTask[activeView.id] ? 'Hide inspect' : 'Inspect'}</button>
						</div>
						{#if inspectOpenByTask[activeView.id] && timing}
							{#await import('#lib/components/TaskInspect.svelte') then { default: TaskInspect }}
								<TaskInspect
									runId={run.id}
									namespace={data.namespace}
									task={activeView}
									{def}
									isAwait={task_isAwait(activeView)}
									isComposed={task_isComposed(activeView)}
									attempts={facts}
									{attemptsSeeded}
									{attemptsError}
									{timing}
									artifacts={(artifactsByTask.get(activeView.id) ?? []).filter((record) => record.item === undefined)}
									downloading={downloadingArtifact}
									onDownload={(record) => void downloadArtifact(record)}
									onViewLogs={(attempt) => viewTaskAttemptLogs(activeView!.id, attempt)}
									onCopyValues={() => void copy(payloadJson(activeView!.output), 'Task values copied', `task-values-${activeView!.id}`)}
								/>
							{/await}
						{/if}
					{:else}
						{@render selectedLog()}
					{/if}
				</ItemCard>
</div>
			{/if}
			</section>
			{#if cancelDialog && run}
				<ConfirmDialog
					testid="cancel-run-confirm"
					title="Cancel run {shortRunId(run.id)}?"
					acceptLabel="Cancel run"
					cancelLabel="Keep running"
					glyph="ban"
					acceptTestid="cancel-run-accept"
					cancelTestid="cancel-run-dismiss"
					busy={cancelBusy}
					error={cancelError}
					destructive
					onaccept={cancelRun}
					oncancel={() => { cancelDialog = false; cancelError = ''; }}
				>
					{#snippet body()}
						<p>Running tasks get a stop signal and the run ends Canceled once they have stopped. Tasks that have not started never will. This page keeps the current status until the run records the change.</p>
						<label class="text-dense font-semibold text-ink" for="cancel-reason">Reason <span class="font-normal text-ink-3">(optional)</span></label>
						<textarea id="cancel-reason" data-testid="cancel-reason" bind:value={cancelReason} rows="3" class="field w-full px-2.5 py-2 text-base" placeholder="Why should this run stop?"></textarea>

					{/snippet}
				</ConfirmDialog>
			{/if}
		</div>

		<aside class="run-aside" aria-label="Run details">
			{#if runError || runErrorMessage}
				<!--
					WHY IT ENDED: the run's terminal error, in full when the snapshot
					carries it (§8.1 category, §8.2 reason code, retryable), else the
					message the socket delivered. Recorded facts only, never a guess.
				-->
				<section class="card" aria-labelledby="run-why-heading" data-testid="run-why">
					<div class="card-head"><h2 id="run-why-heading">{runStatus === Status.FAILED ? 'Why it failed' : 'How it ended'}</h2></div>
					<div class="diag">
						{#if runStatus === Status.FAILED}
							{#if runError}
								<!-- The banner announces a failure with a task to blame; only a run with none is announced here. -->
								<FailurePanel error={runError} testid="run-error" bare compact silent={!!failedTask} tries={failedTask ? triesOf(failedTask) : undefined} emphasis={failedTask?.id} then={failedTask && firstStderr?.taskId === failedTask.id ? 'Its first stderr line says why:' : undefined} />
							{:else}
								{@const plain = plainFailure(runErrorMessage)}
								<div data-testid="run-error" role="alert" class="text-dense text-ink">
									{#if plain.code}<p class="severity-error font-mono font-semibold">{plain.code}</p>{/if}
									<p class="mt-1.5 max-w-prose break-words">{plain.text}</p>
								</div>
							{/if}
							{#if failedTask && firstStderr?.taskId === failedTask.id}
								<code class="why-line" data-testid="run-why-stderr">{firstStderr.text}</code>
							{/if}
							{#if failedTask}
								<div class="flex flex-wrap gap-2">
									<a class="btn btn-sm" href={taskHref(failedTask.id, failedTask.attempt)} onclick={(e) => viewStderr(e, failedTask!)} data-testid="run-why-view-stderr">View stderr</a>
									<a class="btn btn-sm btn-quiet" href={withNamespace(`/orkflows/${encodeURIComponent(run.orkflowName)}?tab=edit`, data.namespace)} data-testid="run-why-open-orkflow">Open {run.orkflowName}</a>
								</div>
							{/if}
						{:else if runError}
							<FailurePanel error={runError} testid="run-error" tone="note" />
						{:else}
							<div data-testid="run-error" role="status" class="text-dense text-ink">{plainFailure(runErrorMessage).text}</div>
						{/if}
					</div>
				</section>
			{/if}
			{#if runStatus === Status.FAILED && failedTask}
				<section class="card" aria-labelledby="run-how-heading" data-testid="run-how-failed">
					<div class="card-head"><h2 id="run-how-heading">How it failed</h2><span class="card-sub">from this run's records</span></div>
					<div class="diag">
						<ol class="chain">
							{#if failedNeed}<li><b>{failedNeed}</b> succeeded</li>{/if}
							<li class="bad"><b>{failedTask.id}</b>{#if failedNeed}, which needs it,{/if} {failedTask.exitCode ? `exited ${failedTask.exitCode}` : 'failed'}{#if failedTask.error?.code}<small>{failedTask.error.code}</small>{/if}</li>
							{#if skippedTasks.length}<li class="pend"><b>{listWords(skippedTasks)}</b> {skippedTasks.length === 1 ? 'was' : 'were'} skipped<small>{skippedTasks.length === 1 ? 'its need' : 'their needs'} did not succeed</small></li>{/if}
							{#if neverTasks.length}<li class="pend"><b>{listWords(neverTasks)}</b> never started<small>the run was already failing</small></li>{/if}
						</ol>
					</div>
				</section>
			{/if}
			{#if runStatus === Status.FAILED}
				<SinceLastRun namespace={data.namespace} runId={run.id} orkflow={run.orkflowName} createdAt={timestampMs(run.createdAt)} sinceSuccess read={tasks.length <= DIFF_IN_PASSING} finished />
			{/if}
			<section class="card" aria-labelledby="run-map-heading" data-testid="run-map-card">
				<div class="card-head"><h2 id="run-map-heading">Task map</h2><span class="card-sub">{hasFanOut ? 'every task and item, in the order they can run' : 'every task, row by row in the order they can run'}</span></div>
				<div class="tmap" style:height="{mapBoxHeight}px" bind:this={mapBox}>
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
				</div>
				{#if !(isTerminal(runStatus) && tasks.length <= STAGE_ROWS_MAX)}<div class="tmap-key"><TaskMapLegend compact={tasks.length <= STAGE_ROWS_MAX} counts={legend} neverStarted={legendNever} onpick={(status) => taskMap?.focusStatus(status)} /></div>{/if}
			</section>
			{#if runStatus !== Status.FAILED}
				<SinceLastRun namespace={data.namespace} runId={run.id} orkflow={run.orkflowName} createdAt={timestampMs(run.createdAt)} sinceSuccess={false} read={tasks.length <= DIFF_IN_PASSING} finished={isTerminal(runStatus)} />
			{/if}
		</aside>
	</div>

	{#if runAgainDialogOpen}
		<Dialog
			testid="run-again-dialog"
			title="Run again from {run.id}"
			widthClass="max-w-2xl"
			onclose={() => (runAgainDialogOpen = false)}
		>
			{#snippet body()}
				<!-- Most visits never submit: the form's code loads when it opens. -->
				{#await import('#lib/components/RunSubmitPanel.svelte')}
					<p class="text-sm text-ink-3" data-testid="run-submit-loading">Loading the form…</p>
				{:then { default: RunSubmitPanel }}
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
				{/await}
			{/snippet}
		</Dialog>
	{/if}

	{#if evidenceDialogOpen}
		{#await import('#lib/components/RunEvidenceDialog.svelte') then { default: RunEvidenceDialog }}
			<RunEvidenceDialog runId={run.id} namespace={data.namespace} onclose={() => (evidenceDialogOpen = false)} />
		{/await}
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

<style>
	.run-grid {
		display: grid;
		grid-template-columns: minmax(0, 1fr) 340px;
		gap: 20px;
		align-items: start;
		margin-bottom: 24px;
	}
	.run-values {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(min(100%, 22rem), 1fr));
		gap: 20px;
		margin-bottom: 20px;
	}
	.run-col {
		display: grid;
		grid-template-columns: minmax(0, 1fr);
		gap: 20px;
		min-width: 0;
	}
	.run-aside {
		display: flex;
		flex-direction: column;
		gap: 16px;
		min-width: 0;
	}
	@media (max-width: 1180px) {
		.run-grid {
			grid-template-columns: minmax(0, 1fr);
		}
	}
	.legend {
		display: flex;
		flex-wrap: wrap;
		gap: 6px 16px;
		padding: 10px 16px;
		border-top: 1px solid var(--line);
		font-size: 13.5px;
		color: var(--ink-2);
	}
	.legend-i {
		display: inline-flex;
		align-items: center;
		gap: 6px;
	}
	.legend-note {
		display: inline-flex;
		align-items: center;
		margin-left: auto;
	}
	.legend-note:empty {
		display: none;
	}
	.tlkey {
		display: inline-flex;
		flex-wrap: wrap;
		gap: 4px 12px;
		margin-left: 8px;
		font-size: 13px;
		color: var(--ink-3);
	}
	.tlkey > span {
		display: inline-flex;
		align-items: center;
		gap: 6px;
	}
	.tlkey i {
		display: inline-block;
		width: 16px;
		height: 8px;
		border-radius: 99px;
	}
	.k-r {
		background: var(--ok-mark);
	}
	.k-run {
		background: var(--run);
	}
	.k-w {
		background: repeating-linear-gradient(135deg, var(--wait-mark) 0 3px, transparent 3px 6px);
		box-shadow: inset 0 0 0 1px var(--wait-mark);
	}
	.k-c {
		background: var(--surface);
		box-shadow: inset 0 0 0 2px var(--fail-mark);
	}
	.guide {
		display: flex;
		flex-wrap: wrap;
		justify-content: space-between;
		gap: 4px 12px;
		padding: 8px 16px;
		border-bottom: 1px solid var(--line);
		background: var(--surface-2);
		font-size: 13.5px;
		color: var(--ink-3);
	}
	.guide b {
		font-weight: 600;
		color: var(--ink-2);
	}
	.tasks-note {
		margin: 0;
		padding: 8px 16px;
		border-bottom: 1px solid var(--line);
		font-size: 13.5px;
		color: var(--ink-2);
	}
	.trow {
		position: relative;
		display: grid;
		grid-template-columns: clamp(160px, 28%, 260px) minmax(0, 1fr) 176px;
		gap: 16px;
		align-items: center;
		padding: 10px 16px;
		border-bottom: 1px solid var(--line);
		font-size: 14px;
		content-visibility: auto;
		contain-intrinsic-size: auto 68px;
	}
	.trow:global([data-selected]) {
		background: var(--hover);
		content-visibility: visible;
	}
	.trow:global([data-selected]) .tname {
		font-weight: 750;
	}
	@media (forced-colors: active) {
		.trow:global([data-selected]) {
			outline: 2px solid Highlight;
			outline-offset: -2px;
		}
	}
	@media (max-width: 640px) {
		.tname {
			min-height: 40px;
		}
	}
	.trow.pinned {
		margin-top: -1px;
		border: 1px solid;
		border-radius: var(--r-sm);
	}
	.thead {
		padding-block: 8px;
		font-size: 13px;
		font-weight: 600;
		color: var(--ink-3);
		content-visibility: visible;
	}
	.tl-axis {
		position: relative;
		display: block;
		height: 18px;
		font-variant-numeric: tabular-nums;
		font-weight: 400;
	}
	.tl-axis span {
		position: absolute;
		top: 0;
		transform: translateX(-50%);
	}
	.tl-axis span.first {
		transform: none;
	}
	.rico {
		flex: none;
		width: 16px;
		height: 16px;
		color: var(--ink-3);
	}
	.rows-chunk {
		content-visibility: auto;
		contain-intrinsic-size: auto 13600px;
	}
	.tl {
		position: relative;
		display: block;
		min-width: 0;
		height: 22px;
	}
	.tl::before {
		content: '';
		position: absolute;
		left: 0;
		right: 0;
		top: 50%;
		border-top: 1px dashed var(--line-2);
	}
	.tl i {
		position: absolute;
		top: 6px;
		height: 10px;
		min-width: 4px;
		border-radius: 99px;
	}
	.tl-wait {
		background: repeating-linear-gradient(135deg, var(--wait-mark) 0 3px, transparent 3px 6px);
		box-shadow: inset 0 0 0 1px var(--wait-mark);
	}
	.tl-ran {
		background: var(--ok-mark);
	}
	.tl-running {
		background: var(--run);
	}
	.tl-failed {
		background: var(--surface);
		box-shadow: inset 0 0 0 2px var(--fail-mark);
	}
	.tname-col {
		display: flex;
		flex-direction: column;
		gap: 2px;
		min-width: 0;
	}
	.tname {
		display: inline-flex;
		align-items: center;
		gap: 7px;
		min-height: 24px;
		padding: 2px 0;
		border: 0;
		border-radius: 4px;
		background: none;
		color: inherit;
		font: inherit;
		font-weight: 600;
		overflow-wrap: anywhere;
		text-align: left;
		cursor: pointer;
	}
	.tname:hover {
		text-decoration: underline;
		text-underline-offset: 3px;
	}
	.tsub {
		padding-left: 23px;
		font-size: 13.5px;
		color: var(--ink-3);
		overflow-wrap: anywhere;
	}
	.torker {
		display: inline-block;
		max-width: 100%;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
		vertical-align: bottom;
	}
	.tsub.clips {
		display: block;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}
	.tsub.clips .torker {
		display: inline;
	}
	.tsub :global(.retry) {
		margin-left: 4px;
	}
	.pin-dot {
		display: inline-block;
		width: 8px;
		height: 8px;
		margin-right: 4px;
		border-radius: 50%;
	}
	.stc {
		display: flex;
		flex-wrap: wrap;
		justify-content: flex-end;
		align-items: center;
		gap: 4px 8px;
	}
	.tdur {
		min-width: 34px;
		font-size: 13px;
		color: var(--ink-3);
		text-align: right;
	}
	.xpand {
		display: grid;
		grid-template-columns: minmax(0, 1fr);
		gap: 10px;
		padding: 14px 16px;
		border-bottom: 1px solid var(--line);
		background: var(--surface-2);
		min-width: 0;
	}
	.xbody {
		display: grid;
		grid-template-columns: minmax(0, 1fr);
		gap: 10px;
		min-width: 0;
	}
	.xbody :global(.fstrip > div:first-child) {
		padding-left: 0;
	}
	[data-run-log] {
		overflow: hidden;
		border: 1px solid var(--line);
		border-radius: var(--r);
	}
	.rows-chunk:global([data-open]) {
		content-visibility: visible;
	}
	.plate-open {
		display: inline-flex;
		max-width: 100%;
		border-radius: 6px;
		cursor: pointer;
	}
	.plate-open:hover :global(.plate) {
		border-color: var(--line-strong);
	}
	.xfoot {
		display: flex;
		justify-content: flex-end;
	}
	/* On a phone the quiet actions take one row and the run's own the next. */
	@media (max-width: 640px) {
		.act-break {
			display: block;
			flex-basis: 100%;
			width: auto;
			height: 0;
			margin: 0;
			background: none;
		}
	}
	.xhead {
		margin: 0;
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 4px 12px;
		font-size: 13px;
	}
	.xhead-end {
		display: inline-flex;
		align-items: center;
		gap: 8px;
		margin-left: auto;
	}
	.phline {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 6px 14px;
	}
	.phline .pbar {
		flex: 1 1 200px;
	}
	.pkey-in {
		display: inline-flex;
		gap: 12px;
		font-size: 12.5px;
		color: var(--ink-2);
	}
	.pkey-in span {
		display: inline-flex;
		align-items: center;
		gap: 5px;
	}
	.pkey-in i {
		display: inline-block;
		width: 10px;
		height: 10px;
		border-radius: 2px;
	}
	.log-none {
		margin: 0;
		padding: 14px 16px;
		background: var(--log-bg);
		color: var(--log-ink-2);
		font-size: 13.5px;
	}
	.why-line {
		display: block;
		padding: 8px 10px;
		border: 1px solid var(--line);
		border-radius: 6px;
		background: var(--surface-2);
		font-size: 12.5px;
		line-height: 1.5;
		overflow-wrap: anywhere;
	}
	.tmap {
		overflow: hidden;
		margin: 6px 8px 0;
	}
	.tmap-key {
		padding: 0 16px 12px;
	}
	.pad {
		display: grid;
		gap: 8px;
		margin: 0;
		padding: 12px 16px;
		font-size: 13.5px;
	}
	.kv-stack {
		grid-template-columns: minmax(0, 1fr);
	}
	.kv-stack :global(dt) {
		padding-bottom: 0;
		border-bottom: 0;
		font-size: 13px;
	}
	.kv-stack :global(dd) {
		padding: 2px 16px 9px;
		text-align: left;
		overflow-wrap: anywhere;
	}
	.valbox {
		max-height: 224px;
		margin: 0;
		padding: 12px 16px;
		overflow: auto;
		font-family: var(--font-code);
		font-size: 12.5px;
		color: var(--ink-2);
	}
	.insp-row {
		margin: 0;
	}
	.insp-more {
		display: flex;
		justify-content: flex-end;
	}
	@media (max-width: 900px) {
		.trow {
			grid-template-columns: minmax(0, 1fr) auto;
		}
		.trow > .tl,
		.thead > .tl-axis {
			display: none;
		}
		.legend-note {
			margin-left: 0;
		}
	}
	@media (forced-colors: active) {
		.tlkey i,
		.pkey-in i,
		.tl i {
			forced-color-adjust: none;
		}
	}
	.rh {
		display: grid;
		grid-template-columns: minmax(0, 1fr) auto;
		column-gap: 20px;
		margin-bottom: 20px;
	}
	.rh-title {
		grid-column: 1;
		grid-row: 1;
		align-self: center;
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 6px 12px;
		min-width: 0;
		overflow-wrap: anywhere;
	}
	.rh-actions {
		grid-column: 2;
		grid-row: 1;
		align-self: center;
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 8px;
	}
	.rh-plates {
		grid-column: 1 / -1;
		grid-row: 2;
		display: flex;
		flex-wrap: wrap;
		gap: 6px 12px;
		margin-top: 8px;
	}
	.rh-facts {
		grid-column: 1 / -1;
		grid-row: 3;
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 8px 16px;
		margin-top: 10px;
	}
	/* On a phone the ring and its words share one line, so the drawing stays on the first screen. */
	@media (max-width: 640px) {
		.rh-facts :global(.fstrip > div:has([data-testid='run-progress'])) {
			grid-column: 1 / -1;
		}
	}
	@media (max-width: 1099px) {
		.rh {
			grid-template-columns: minmax(0, 1fr);
		}
		.rh-actions {
			grid-column: 1;
			grid-row: 2;
			justify-self: start;
			margin-top: 10px;
		}
		.rh-plates {
			grid-row: 3;
		}
		.rh-facts {
			grid-row: 4;
		}
	}
</style>
