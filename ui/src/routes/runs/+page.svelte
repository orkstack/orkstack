<script lang="ts">
	import { goto, invalidateAll } from '$app/navigation';
	import { onMount, tick } from 'svelte';
	import ErrorPanel from '#lib/components/ErrorPanel.svelte';
	import CliPopover from '#lib/components/CliPopover.svelte';
	import FactsStrip, { type Fact } from '#lib/components/FactsStrip.svelte';
	import LiveControl from '#lib/components/LiveControl.svelte';
	import Plate from '#lib/components/Plate.svelte';
	import Principal from '#lib/components/Principal.svelte';
	import ProgressRing from '#lib/components/ProgressRing.svelte';
	import TimeAgo from '#lib/components/TimeAgo.svelte';
	import { runProgress } from '#lib/progress.ts';
	import { dayBars, runCount } from '#lib/stats.ts';
	import { relativeTime } from '#lib/when.ts';
	import { filterFlags, filterOf, FilterOperator } from '#lib/listFilter.ts';
	import { timestampDate } from '@bufbuild/protobuf/wkt';
	import ListFilter from '#lib/components/ListFilter.svelte';
	import Pagination from '#lib/components/Pagination.svelte';
	import Select from '#lib/components/Select.svelte';
	import StatusPill from '#lib/components/StatusPill.svelte';
	import { listHref, pageStackKey, readPageStack, writePageStack } from '#lib/browsing.ts';
	import { UNRECORDED, UNRECORDED_CAPTION, originKindSlug } from '#lib/origin.ts';
	import { RunOriginKind } from '#lib/gen/orkstack/v1/run_pb.ts';
	import { namespaceLabel, withNamespace } from '#lib/namespace.ts';
	import { shellQuote } from '#lib/runDetail.ts';
	import { equalityOf, type FieldFilter } from '#lib/listFilter.ts';
	import { ListRunsRequestSchema } from '#lib/gen/orkstack/v1/run_service_pb.ts';
	import type { PageProps } from './$types';
	import { untrack } from 'svelte';
	import Icon from '#lib/components/Icon.svelte';
	import { Status } from '#lib/gen/orkstack/v1/common_pb.ts';
	import type { Run } from '#lib/gen/orkstack/v1/run_pb.ts';
	import { runClient } from '#lib/clients/runs.ts';
	import { heldLabel, loadHeld, markAllSeen, observeList, saveHeld, type ListedRun } from '#lib/heldRuns.ts';
	import { registerProvider } from '#lib/palette/context.ts';
	import { formatDuration } from '#lib/runDetail.ts';
	import { isTerminal } from '#lib/status.ts';
	import { pollWhileVisible } from '#lib/visibility.ts';
	import { shortRunId } from '#lib/runDetail.ts';

	let { data }: PageProps = $props();

	// The order of this page's rows. The list itself is always newest first; this
	// orders only what has been read.
	type RowOrder = 'newest' | 'oldest' | 'longest';
	const ROW_ORDERS = [
		{ value: 'newest', label: 'newest first' },
		{ value: 'oldest', label: 'oldest first' },
		{ value: 'longest', label: 'longest first' }
	];
	let rowOrder = $state<RowOrder>('newest');
	const orderedRuns = $derived.by(() => {
		if (rowOrder === 'newest') return data.runs;
		if (rowOrder === 'oldest') return [...data.runs].reverse();
		const lasted = (r: (typeof data.runs)[number]) => {
			const from = r.startedAt ?? r.createdAt;
			return from ? (r.finishedAt ? Number(r.finishedAt.seconds) : Date.now() / 1000) - Number(from.seconds) : 0;
		};
		return [...data.runs].sort((a, b) => lasted(b) - lasted(a));
	});

	// "Runs created each day": a bar is every run its bucket counted, failed
	// on top; labelled with the local date of its start, and the bucket that
	// holds now is today so far.
	// What a running row has taken so far moves with the page, no faster than a tile reads.
	let nowMs = $state(Date.now());
	$effect(() => {
		const timer = setInterval(() => (nowMs = Date.now()), 5_000);
		return () => clearInterval(timer);
	});
	const nowStamp = $derived({ seconds: BigInt(Math.floor(nowMs / 1000)), nanos: 0 });

	const sizeOptions = [10, 25, 50, 100].map((size) => ({ value: String(size), label: String(size) }));

	// One Orkflow, when the filters name exactly one: the header says so and
	// Submit a run goes to its page. The standalone Submit Run page is gone —
	// a run is started from the Orkflow it belongs to — so a list that is not
	// narrowed to one can only offer the picker.
	const orkflowName = $derived(equalityOf(data.applied, 'orkflow_name') ?? '');
	const submitHref = $derived(
		withNamespace(orkflowName ? `/orkflows/${encodeURIComponent(orkflowName)}` : '/orkflows', data.namespace)
	);
	const applyHref = $derived(withNamespace('/apply', data.namespace));

	let retrying = $state(false);
	async function retry() {
		retrying = true;
		try {
			await invalidateAll();
		} finally {
			retrying = false;
		}
	}
	// The coordinates of what this list is showing, exactly as the address bar
	// spells them, and one builder over them. listHref only ever `set`s a
	// scalar, and writes the filters once each in canonical order, so no
	// gesture here can leave two of the same coordinate in the URL.
	const coordinates = $derived({ ns: namespaceLabel(data.namespace), size: String(data.size), cursor: data.cursor, page: String(data.page) });
	const applied = $derived({ filter: data.applied });
	function href(changes: Record<string, string>, filters: Record<string, FieldFilter[]> = applied) {
		return listHref('/runs', { ...coordinates, ...changes }, filters);
	}

	// The cursors BEHIND this page: per filter set, per tab, in sessionStorage
	// rather than in the URL, which used to grow by one escaped engine cursor
	// per Next click (docs/UI_CLI_PARITY.md "Paging in the browser").
	//
	// Read in an `$effect` and not a `$derived`: the first render is the
	// server's, where there is no sessionStorage, and a derived computed there
	// would keep its empty answer through hydration.
	const stackKey = $derived(pageStackKey('/runs', coordinates, applied));
	let stack = $state<string[]>([]);
	$effect(() => {
		stack = readPageStack(stackKey);
	});
	// Next records the path it is about to take before the router follows the
	// href: this page's cursor at this page's index, the token being followed
	// at the next one. Holes are filled with '' so the stored array stays a
	// list of strings.
	function next() {
		const ahead = Array.from({ length: Math.max(stack.length, data.page + 1) }, (_, at) => stack[at] ?? '');
		[ahead[data.page - 1], ahead[data.page]] = [data.cursor, data.nextPageToken];
		writePageStack(stackKey, ahead);
		stack = ahead;
	}
	// Previous is offered only when the stack describes THIS page. "There is an
	// earlier page" was never "the stack is non-empty": a pasted page-3 link
	// has an earlier page and no way to name its cursor, and it is handed
	// First page instead of a Previous that would have to invent one.
	const walked = $derived(data.page > 1 && stack[data.page - 1] === data.cursor);
	const previousHref = $derived(walked ? href({ cursor: stack[data.page - 2] ?? '', page: String(data.page - 1) }) : undefined);
	const nextHref = $derived(data.nextPageToken ? href({ cursor: data.nextPageToken, page: String(data.page + 1) }) : undefined);
	const firstHref = $derived(href({ cursor: '', page: '' }));

	// Page size is a field of the filter form and applies with Apply filters,
	// like every other field of it: one form never applies some fields on
	// change and others on submit. A new filter set (or size) is a new page 1
	// of a new listing, so the path back through the old one is dropped
	// rather than left to be trusted later.
	function sizeFromUrl() {
		return String(data.size);
	}
	let sizeDraft = $state(sizeFromUrl());
	$effect(() => {
		sizeDraft = sizeFromUrl();
	});
	function applyFilters(filters: FieldFilter[]) {
		writePageStack(stackKey, []);
		void goto(href({ size: sizeDraft || '25', cursor: '', page: '' }, { filter: filters }), { reset: false });
	}
	const clearHref = $derived(href({ cursor: '', page: '' }, {}));

	// ---- The register, live ----
	//
	// The page polls the same listing every LIST_POLL_MS while it is visible
	// and patches the status (and, once finished, the duration) of the rows
	// it already shows, in place: it never inserts, removes or reorders a
	// row, and never re-runs the loader or resets a half-typed filter. Newer
	// runs on page 1 are announced, and shown only when asked.
	const LIST_POLL_MS = 15_000;
	let patched = $state.raw(new Map<string, Pick<Run, 'status' | 'finishedAt'>>());
	let newer = $state(0);
	const statusOf = (run: Run) => patched.get(run.id)?.status ?? run.status;
	const finishedOf = (run: Run) => patched.get(run.id)?.finishedAt ?? run.finishedAt;

	// ---- Changed-run marks (this browser only, #lib/heldRuns.ts) ----
	let held = $state.raw(new Map<string, string>());
	const ms = (t: Run['createdAt']) => (t ? Number(t.seconds) * 1000 + Math.floor(t.nanos / 1e6) : 0);
	function listed(runs: Run[]): ListedRun[] {
		// The held-runs store keeps a status by its enum name, never by the word
		// a page shows: the word can change with the design, the name cannot.
		return runs.map((run) => ({ id: run.id, status: Status[statusOf(run)] ?? 'UNSPECIFIED', createdMs: ms(run.createdAt), orkflow: run.orkflowName }));
	}
	function observe(poll: boolean): void {
		const result = observeList(loadHeld(), namespaceLabel(data.namespace), listed(data.runs), Date.now(), { poll });
		saveHeld(result.store);
		held = result.held;
	}
	// Each answer of the loader is a visit: off the path that draws the rows.
	$effect(() => {
		void data.runs;
		patched = new Map();
		newer = 0;
		const t = setTimeout(() => untrack(() => observe(false)), 0);
		return () => clearTimeout(t);
	});
	function markAll(): void {
		saveHeld(markAllSeen(loadHeld(), namespaceLabel(data.namespace), Date.now()));
		held = new Map();
	}
	// The live control pauses this page's polling; the runs keep going, and
	// resuming reads at once to catch up.
	let paused = $state(false);
	$effect(() => {
		if (paused || data.error || data.runs.length === 0) return;
		const namespace = data.namespace;
		const size = data.size;
		const cursor = data.cursor;
		const filters = data.applied;
		const firstPage = data.page === 1;
		return pollWhileVisible(() => {
			void runClient(fetch)
				.listRuns({ namespace, pageSize: size, pageToken: cursor, filters })
				.then((res) => {
					const shown = new Set(data.runs.map((r) => r.id));
					const next = new Map(patched);
					for (const r of res.runs) if (shown.has(r.id)) next.set(r.id, { status: r.status, finishedAt: r.finishedAt });
					patched = next;
					if (firstPage) {
						const top = Math.max(...data.runs.map((r) => ms(r.createdAt)));
						newer = res.runs.filter((r) => !shown.has(r.id) && ms(r.createdAt) > top).length;
					}
					observe(true);
					readLive();
				})
				.catch(() => {
					// A missed poll changes nothing: the rows stay as last read.
				});
		}, LIST_POLL_MS, { immediate: false });
	});
	const heldCount = $derived(data.runs.filter((r) => held.has(r.id)).length);


	$effect(() =>
		registerProvider({
			id: 'runs-list',
			commands: () => [
				...(data.applied.length
					? [{ id: 'runs-clear-filters', group: 'Actions' as const, label: 'Clear the run filters', action: { kind: 'navigate' as const, href: clearHref }, href: clearHref }]
					: []),
				...(heldCount
					? [{ id: 'runs-mark-seen', group: 'Changed runs' as const, label: 'Mark all runs seen', action: { kind: 'page' as const, run: markAll } }]
					: [])
			]
		})
	);
	const ns = $derived(namespaceLabel(data.namespace));
	const command = $derived(['orkctl', '-n', shellQuote(ns), 'run', 'list']);
	const cliCommands = $derived([
		{ label: data.applied.length ? 'List these runs' : 'List the runs', command: [...command, ...filterFlags(data.applied)].join(' ') },
		{ label: 'The numbers below, for 7 days', command: `orkctl -n ${shellQuote(ns)} stats --since 168h` },
		{ label: 'Failures by reason', command: `orkctl -n ${shellQuote(ns)} stats failures --since 168h --group-by code,category` },
		{ label: 'Tasks that needed a retry', command: `orkctl -n ${shellQuote(ns)} stats flakes --since 168h --group-by orkflow,task` }
	]);

	// ---- The 24-hour tiles: they filter the list by status ----
	const TILES = [
		{ key: 'running', status: 'RUNNING', label: 'Running', mark: 'mark-dot' },
		{ key: 'succeeded', status: 'SUCCEEDED', label: 'Succeeded', mark: 'mark-square' },
		{ key: 'failed', status: 'FAILED', label: 'Failed', mark: 'mark-tri' },
		{ key: 'canceled', status: 'CANCELED', label: 'Canceled', mark: 'mark-strike' }
	] as const;
	const statusShown = $derived(equalityOf(data.applied, 'status'));
	const others = (path: string) => data.applied.filter((f) => f.field !== path);
	let countLine = $state<HTMLElement>();
	async function filterTo(path: string, value: string | null): Promise<void> {
		const next = value ? [...others(path), filterOf(path, FilterOperator.EQUALS, [value])] : others(path);
		writePageStack(stackKey, []);
		await goto(href({ cursor: '', page: '' }, { filter: next }), { reset: false });
	}
	async function tileFilter(status: string): Promise<void> {
		await filterTo('status', statusShown === status ? null : status);
		await tick();
		countLine?.scrollIntoView({ block: 'start' });
		countLine?.focus();
	}

	// ---- Search: a run id opens that run; any other words narrow by Orkflow ----
	let search = $state('');
	function onSearch(event: SubmitEvent): void {
		event.preventDefault();
		const text = search.trim();
		if (!text) return;
		if (/^ork-/.test(text)) {
			void goto(withNamespace(`/runs/${encodeURIComponent(text)}`, data.namespace));
			return;
		}
		writePageStack(stackKey, []);
		void goto(href({ cursor: '', page: '' }, { filter: [...others('orkflow_name'), filterOf('orkflow_name', FilterOperator.CONTAINS, [text])] }), { reset: false });
	}

	// ---- The charts: collapsed on a phone, open elsewhere ----
	let chartsOpen = $state(true);
	onMount(() => {
		if (matchMedia('(max-width: 640px)').matches) chartsOpen = false;
	});

	const originWords: Record<number, string> = {
		[RunOriginKind.SUBMIT]: 'Submit',
		[RunOriginKind.TRIGGER]: 'Trigger',
		[RunOriginKind.SCHEDULE]: 'Schedule',
		[RunOriginKind.PARENT_RUN]: 'Parent run',
		[RunOriginKind.RUN_AGAIN]: 'Run again'
	};
	const originOptions = [
		{ value: '', label: 'any' },
		...['SUBMIT', 'TRIGGER', 'SCHEDULE', 'PARENT_RUN', 'RUN_AGAIN'].map((v) => ({ value: v, label: originWords[RunOriginKind[v as keyof typeof RunOriginKind]] }))
	];
	const statusOptions = [{ value: '', label: 'any' }, ...['RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELED', 'PENDING'].map((v) => ({ value: v, label: v[0] + v.slice(1).toLowerCase() }))];

	// A run still going is listed from the cluster's index, with no tasks: its
	// progress is read from the run itself, for the rows on this page only,
	// on load and with each poll.
	let liveTasks = $state.raw(new Map<string, Run['taskRuns']>());
	// A run read while it went on is read once more after it ends, so its ring
	// never keeps a partial reading beside its final pill.
	const finalRead = new Set<string>();
	function readLive(): void {
		const going = data.runs
			.filter((r) => r.taskRuns.length === 0 && (!isTerminal(statusOf(r)) || (liveTasks.has(r.id) && !finalRead.has(r.id))))
			.slice(0, 25);
		for (const r of going) {
			void runClient(fetch)
				.getRun({ namespace: data.namespace, runId: r.id })
				.then((res) => {
					if (!res.run) return;
					if (isTerminal(res.run.status)) finalRead.add(r.id);
					liveTasks = new Map(liveTasks).set(r.id, res.run.taskRuns);
				})
				.catch(() => {});
		}
	}
	$effect(() => {
		void data.runs;
		liveTasks = new Map();
		untrack(readLive);
	});

	/** The one line under a failed or canceled run's pill. */
	function reasonOf(run: Run): { kind: 'fail' | 'canceled'; code?: string; task?: string; principal?: string } | null {
		const status = statusOf(run);
		if (status === Status.FAILED) {
			const task = run.taskRuns.find((t) => t.status === Status.FAILED);
			return { kind: 'fail', code: task?.error?.code || run.error?.code, task: task?.taskId };
		}
		if (status === Status.CANCELED && run.canceledBy) return { kind: 'canceled', principal: run.canceledBy };
		return null;
	}
</script>

<svelte:head><title>Runs — OrkStack</title></svelte:head>

{#snippet weekRuns()}
	{#await data.insights.week}<span class="text-ink-3">…</span>{:then week}{#if week.value}{@const c = week.value.runsByStatus}<span data-testid="runs-week-count"
				>{runCount((c.succeeded ?? 0n) + (c.failed ?? 0n) + (c.canceled ?? 0n) + (c.running ?? 0n))}</span
			>{:else}<span class="text-ink-3">—</span>{/if}{/await}
{/snippet}

<div class="grid gap-5">
	<header class="ph">
		<div class="min-w-0">
			<h1 class="page-h1" data-testid="runs-heading">{orkflowName ? `Runs for ${orkflowName}` : 'Runs'}</h1>
			<div class="fline">
				<FactsStrip
					facts={[
						{ label: 'Last 7 days', value: weekRuns }
					] satisfies Fact[]}
				/>
				<LiveControl {paused} ontoggle={() => (paused = !paused)} testid="runs-live" />
			</div>
		</div>
		<div class="actions">
			<a href={applyHref} class="btn" data-testid="runs-apply">Apply an Orkflow</a>
			<a href={submitHref} data-testid="runs-submit-run" class="btn btn-primary"><Icon name="play" />Submit a run</a>
			<CliPopover testid="runs-cli" title="Command line: runs" commands={cliCommands} />
		</div>
	</header>

	{#await data.insights.latestFailure then latest}
		{@const run = latest.value}
		{#if run && run.finishedAt && Date.now() - timestampDate(run.finishedAt).getTime() < 86_400_000}
			{@const task = run.taskRuns.find((t) => t.status === Status.FAILED)}
			<div class="alert alert-fail" role="status" data-testid="runs-latest-failure">
				<span class="alert-ic"><Icon name="alert" /></span>
				<div class="min-w-0 flex-[1_1_15rem]">
					<b>{run.orkflowName} failed {relativeTime(timestampDate(run.finishedAt))}</b>
					<p class="mt-0.5 text-ink-2">
						{#if task}Task <code>{task.taskId}</code> ended{/if}
						{#if task?.error?.code || run.error?.code}<code>{task?.error?.code || run.error?.code}</code>{/if}{#if task?.exitCode}, exit {task.exitCode}{/if}.
					</p>
				</div>
				<div class="ml-auto flex gap-2">
					{#if task}<a class="btn btn-sm btn-quiet" href={withNamespace(`/runs/${run.id}?task=${encodeURIComponent(task.taskId)}`, data.namespace)}>View logs</a>{/if}
					<a class="btn btn-sm" href={withNamespace(`/runs/${run.id}`, data.namespace)} data-testid="runs-latest-failure-open">Open run</a>
				</div>
			</div>
		{/if}
	{/await}

	<section aria-labelledby="tiles-cap" class="grid gap-2">
		<h2 id="tiles-cap" class="text-sm font-normal text-ink-3">Last 24 hours · filter the list</h2>
		{#await data.insights.day}
			<div class="tiles" aria-hidden="true">{#each TILES as t (t.key)}<div class="tile"><span class="lbl">{t.label}</span><b class="text-ink-3">…</b></div>{/each}</div>
		{:then day}
			{#if day.error}
				<p class="text-sm text-ink-3" data-testid="runs-tiles-error">The last 24 hours could not be read: {day.error}</p>
			{:else}
				<div class="tiles" role="group" aria-labelledby="tiles-cap" data-testid="runs-tiles">
					{#each TILES as t (t.key)}
						{@const count = Number(day.value?.runsByStatus[t.key] ?? 0n)}
						<button
							type="button"
							class="tile"
							aria-pressed={statusShown === t.status}
							data-testid="runs-tile-{t.key}"
							onclick={() => void tileFilter(t.status)}
							><span class="lbl"><span class={t.mark === 'mark-dot' ? 'mark-dot' : `${t.mark} inline-flex`} aria-hidden="true"></span>{t.label}</span><b class="num">{count}</b></button
						>
					{/each}
				</div>
			{/if}
		{/await}
	</section>

	<section class="grid gap-3" aria-labelledby="insights-h" data-testid="runs-insights">
		<div class="flex flex-wrap items-center gap-x-3 gap-y-2">
			<h2 id="insights-h" class="insights-h">Last 7 days</h2>
			{#await data.insights.week then week}{#if week.value?.computedAt}<span class="text-dense text-ink-3"
						>measured <TimeAgo at={week.value.computedAt} /></span
					>{/if}{/await}
			<button
				type="button"
				class="btn btn-sm btn-quiet ml-auto"
				aria-expanded={chartsOpen}
				aria-controls="insights-body"
				data-testid="runs-insights-toggle"
				onclick={() => (chartsOpen = !chartsOpen)}>Charts<Icon name="chev" class="size-4 transition-transform {chartsOpen ? '' : '-rotate-90'}" /></button
			>
		</div>
		<div id="insights-body" hidden={!chartsOpen}>
			<!-- The charts load with their own code, after the list: they sit below it on a phone and are collapsible. -->
			{#await Promise.all([import('#lib/components/RunsInsights.svelte'), data.insights.week, data.insights.failures, data.insights.flakes])}
				<p class="text-dense text-ink-3">Reading the last 7 days…</p>
			{:then [{ default: RunsInsights }, week, failures, flakes]}
				<RunsInsights week={week.value} weekError={week.error} failures={failures.value} flakes={flakes.value} days={dayBars(week.value)} />
			{:catch err}
				<p class="text-dense text-ink-3" data-testid="runs-insights-error">The charts could not load: {String(err)}</p>
			{/await}
		</div>
	</section>

	<section class="card" aria-labelledby="runs-list-h">
		<h2 id="runs-list-h" class="sr">All runs</h2>
		<div class="filters">
			<form class="search min-w-48 max-w-90 flex-[1_1_15rem]" role="search" aria-label="Filter the runs" onsubmit={onSearch}>
				<Icon name="search" />
				<input bind:value={search} aria-label="Filter by run id or Orkflow" placeholder="Filter by run id or Orkflow" data-testid="runs-search" autocomplete="off" spellcheck="false" />
			</form>
			<Select variant="chip" testid="runs-chip-status" label="Status" value={statusShown ?? ''} options={statusOptions} onchange={(v) => void filterTo('status', v || null)} />
			{#await data.insights.orkflowNames then names}
				<Select
					variant="chip"
					testid="runs-chip-orkflow"
					label="Orkflow"
					value={equalityOf(data.applied, 'orkflow_name') ?? ''}
					options={[{ value: '', label: 'any' }, ...(names.value ?? []).map((n) => ({ value: n, label: n }))]}
					onchange={(v) => void filterTo('orkflow_name', v || null)}
				/>
			{/await}
			<Select variant="chip" testid="runs-chip-origin" label="Origin" value={equalityOf(data.applied, 'origin.kind') ?? ''} options={originOptions} onchange={(v) => void filterTo('origin.kind', v || null)} />
			<!-- The table's head is hidden on a phone: this says, and sets, what its rows read in. -->
			<span class="sort-chip"><Select variant="chip" testid="runs-sort" label="Sort" value={rowOrder} options={ROW_ORDERS} onchange={(v) => (rowOrder = v as RowOrder)} /></span>
			<ListFilter request={ListRunsRequestSchema} applied={data.applied} testid="run" onapply={applyFilters} {clearHref} />
		</div>

		{#if data.error}
			<div class="p-4">
				<ErrorPanel testid="engine-error" retryTestid="runs-retry" subject="the runs" message={data.error} busy={retrying} onretry={retry} />
			</div>
		{:else if retrying}
			<p class="px-4 py-6 text-sm text-ink-3" data-testid="runs-loading">Loading runs…</p>
		{:else if data.runs.length === 0}
			<p class="px-6 py-12 text-center text-dense text-ink-3" data-testid="runs-empty">
				{#if data.applied.length > 0}
					No runs match these filters.
				{:else}
					No runs yet.
				{/if}
			</p>
		{:else}
			<div class="guide" aria-live="polite">
				<p bind:this={countLine} id="runs-count" tabindex="-1" data-testid="runs-count">
					Runs <b class="num">{(data.page - 1) * data.size + 1}–{(data.page - 1) * data.size + data.runs.length}</b> · newest first
				</p>
				{#if heldCount > 0}
					<p data-testid="runs-held-summary" class="inline-flex items-center gap-1.5 text-ink">
						<Icon name="updated" class="size-3.5 text-ink-2" />
						{heldCount} {heldCount === 1 ? 'run' : 'runs'} changed since you last looked
					</p>
					<button type="button" data-testid="runs-held-clear" class="btn btn-sm" onclick={markAll}>Mark all seen</button>
				{/if}
				{#if newer > 0}
					<button type="button" data-testid="runs-newer" class="btn btn-sm" onclick={() => void retry()}>{newer} newer {newer === 1 ? 'run' : 'runs'} — show</button>
				{/if}
			</div>
			<div class="overflow-x-auto scroll-panel">
				<table class="register runs-table" data-testid="run-list">
					<thead>
						<tr>
							<th scope="col">Run</th>
							<th scope="col">Orkflow</th>
							<th scope="col">Status</th>
							<th scope="col">Progress</th>
							<th scope="col">Origin</th>
							<th scope="col" aria-sort="descending">Created <Icon name="chev" class="size-3 text-ink-3" /></th>
							<th scope="col" class="text-right"><span data-tip="From submission to finish">Duration</span></th>
						</tr>
					</thead>
					<tbody>
						{#each orderedRuns as run (run.id)}
							{@const status = statusOf(run)}
							{@const from = held.get(run.id)}
							{@const reason = reasonOf(run)}
							{@const progress = runProgress(liveTasks.get(run.id) ?? run.taskRuns)}
							<tr
								class={from !== undefined ? (status === Status.FAILED ? 'bg-fail-tint' : 'bg-surface-2') : ''}
								data-testid="run-row-{run.id}"
								data-held={from !== undefined ? 'true' : undefined}
								data-held-from={from}
							>
								<th scope="row" class="rid">
									{#if from !== undefined}<span role="img" aria-label={heldLabel(from)} data-testid="run-row-held-{run.id}" class="mr-1.5 inline-flex align-middle text-fail"
											><Icon name="updated" class="size-3.5" /></span
										>{/if}<a href={withNamespace(`/runs/${run.id}`, data.namespace)} data-tip={run.id} class="font-mono text-sm text-ink"
										><span class="sr">{run.id}</span><span aria-hidden="true">{shortRunId(run.id)}</span></a
									>
								</th>
								<td class="wf"><span class="mb-1 block font-semibold">{run.orkflowName}</span><Plate key="version" value={run.orkflowVersion} small /></td>
								<td>
									<StatusPill {status} testid="run-row-status-{run.id}" />
									{#if reason?.kind === 'fail'}
										<div class="reason text-fail">{#if reason.code}<code>{reason.code}</code>{/if}{#if reason.task}{' '}in {reason.task}{/if}</div>
									{:else if reason?.kind === 'canceled' && reason.principal}
										<div class="reason text-ink-3">by <Principal name={reason.principal} testid="run-row-canceled-by-{run.id}" /></div>
									{/if}
								</td>
								<td>{#if progress.total > 0}<ProgressRing {progress} {status} small />{:else}<span class="text-ink-3" data-tip="Not read yet">—</span>{/if}</td>
								<td>
									{#if run.origin && originKindSlug(run.origin.kind)}
										<span data-testid="run-row-origin-{run.id}"
											>{originWords[run.origin.kind] ?? originKindSlug(run.origin.kind)}{#if run.origin.id}<span class="reason block font-mono text-sm text-ink-3"
													>{run.origin.kind === RunOriginKind.PARENT_RUN || run.origin.kind === RunOriginKind.RUN_AGAIN ? shortRunId(run.origin.id) : run.origin.id}</span
												>{/if}</span
										>
									{:else}
										<span data-testid="run-row-origin-{run.id}" class="text-ink-3" data-tip={UNRECORDED_CAPTION}>{UNRECORDED}</span>
									{/if}
								</td>
								<td class="whitespace-nowrap"><TimeAgo at={run.createdAt} /></td>
								<td class="num whitespace-nowrap text-right">{finishedOf(run) ? formatDuration(run.createdAt, finishedOf(run)) : isTerminal(status) ? '—' : formatDuration(run.createdAt, nowStamp)}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
			<div class="border-t border-line px-4 pb-3">
				<Pagination testid="runs" noun="run" page={data.page} count={data.runs.length} {previousHref} {nextHref} {firstHref} onnext={next} previousLabel="Newer" nextLabel="Older">
					<span class="flex items-center gap-2 text-sm">
						<label class="font-semibold text-ink-3" for="run-filter-size">Per page</label>
						<span class="w-20"
							><Select
								id="run-filter-size"
								testid="run-filter-size"
								value={sizeDraft}
								options={sizeOptions}
								onchange={(next) => {
									sizeDraft = next;
									writePageStack(stackKey, []);
									void goto(href({ size: next, cursor: '', page: '' }), { reset: false });
								}}
							/></span
						>
					</span>
				</Pagination>
			</div>
		{/if}
	</section>
</div>

<style>
	.insights-h {
		font-size: 16px;
		font-weight: 650;
	}
	.ph {
		display: grid;
		grid-template-columns: minmax(0, 1fr) auto;
		gap: 12px 20px;
		align-items: start;
	}
	.actions {
		display: flex;
		flex-wrap: wrap;
		gap: 8px;
	}
	.fline {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 8px 16px;
		margin-top: 10px;
	}
	.tiles {
		display: grid;
		grid-template-columns: repeat(4, minmax(0, 1fr));
		gap: 12px;
	}
	.tile .lbl {
		display: flex;
		align-items: center;
		gap: 8px;
		font-size: 13px;
		font-weight: 600;
		color: var(--ink-2);
	}
	.tile b {
		font-size: 26px;
		line-height: 1.15;
		font-weight: 700;
		letter-spacing: -0.02em;
	}
	.sort-chip {
		display: none;
	}
	@media (max-width: 640px) {
		.sort-chip {
			display: inline-block;
		}
	}
	.filters {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 8px;
		padding: 12px 16px;
		border-bottom: 1px solid var(--line);
	}
	.guide {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 4px 12px;
		padding: 8px 16px;
		border-bottom: 1px solid var(--line);
		background: var(--surface-2);
		font-size: 13.5px;
		color: var(--ink-3);
	}
	.guide p {
		margin: 0;
	}
	.guide b {
		color: var(--ink-2);
		font-weight: 600;
	}
	#runs-count:focus-visible {
		outline: 2px solid var(--focus);
		outline-offset: 2px;
	}
	.reason {
		margin-top: 4px;
		font-size: 13.5px;
	}
	.reason code {
		font-size: 12.5px;
	}
	@media (max-width: 1099px) {
		.ph {
			grid-template-columns: minmax(0, 1fr);
		}
	}
	@media (max-width: 900px) {
		.tiles {
			grid-template-columns: repeat(2, minmax(0, 1fr));
		}
	}
	/* On a phone each row is a card, and the whole card is one link. */
	/* One line with its mark, and a link whose middle is the link: a wrapped id's is the gap between its lines. */
	.runs-table th.rid {
		white-space: nowrap;
	}
	.runs-table th.rid :global(a) {
		display: inline-block;
	}
	@media (max-width: 640px) {
		.runs-table thead {
			display: none;
		}
		.runs-table,
		.runs-table tbody,
		.runs-table tr,
		.runs-table td,
		.runs-table th {
			display: block;
		}
		.runs-table tr {
			position: relative;
			display: grid;
			grid-template-columns: minmax(0, 1fr) auto;
			grid-template-areas: 'wf dur' 'st cr' 'tk tk' 'or id';
			gap: 6px 12px;
			padding: 12px 16px;
			border-bottom: 1px solid var(--line);
		}
		.runs-table td,
		.runs-table th {
			padding: 0 !important;
			border: 0 !important;
			background: none !important;
		}
		.runs-table tr > :nth-child(1) {
			grid-area: id;
			justify-self: end;
			align-self: center;
		}
		.runs-table tr > :nth-child(2) {
			grid-area: wf;
			white-space: normal;
			display: flex;
			flex-direction: column;
			align-items: flex-start;
			gap: 4px;
		}
		.runs-table tr > :nth-child(3) {
			grid-area: st;
		}
		.runs-table tr > :nth-child(4) {
			grid-area: tk;
		}
		.runs-table tr > :nth-child(5) {
			grid-area: or;
			font-size: 13.5px;
			color: var(--ink-3);
		}
		.runs-table tr > :nth-child(6) {
			grid-area: cr;
			justify-self: end;
			color: var(--ink-3);
		}
		.runs-table tr > :nth-child(7) {
			grid-area: dur;
			justify-self: end;
		}
		.runs-table th.rid :global(a)::after {
			content: '';
			position: absolute;
			inset: 0;
		}
		.runs-table th.rid :global(a:focus-visible) {
			outline: none;
		}
		.runs-table th.rid :global(a:focus-visible)::after {
			outline: 2px solid var(--focus);
			outline-offset: -3px;
			border-radius: 6px;
		}
	}
</style>
