<script lang="ts">
	import { goto, invalidateAll } from '$app/navigation';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import ListFilter from '$lib/components/ListFilter.svelte';
	import Pagination from '$lib/components/Pagination.svelte';
	import Select from '$lib/components/Select.svelte';
	import StatusPill from '$lib/components/StatusPill.svelte';
	import { listHref, pageStackKey, readPageStack, writePageStack } from '$lib/browsing';
	import { fmtTimestamp } from '$lib/status';
	import { UNRECORDED, UNRECORDED_CAPTION, originKindSlug } from '$lib/origin';
	import { namespaceLabel, withNamespace } from '$lib/namespace';
	import { shellQuote } from '$lib/runDetail';
	import { equalityOf, type FieldFilter } from '$lib/listFilter';
	import { ListRunsRequestSchema } from '$lib/gen/orkstack/v1/run_service_pb';
	import type { PageProps } from './$types';
	import { untrack } from 'svelte';
	import Icon from '$lib/components/Icon.svelte';
	import SheetHeader from '$lib/components/SheetHeader.svelte';
	import { Status } from '$lib/gen/orkstack/v1/common_pb';
	import type { Run } from '$lib/gen/orkstack/v1/run_pb';
	import { runClient } from '$lib/clients/runs';
	import { heldLabel, loadHeld, markAllSeen, observeList, saveHeld, type ListedRun } from '$lib/heldRuns';
	import { registerProvider } from '$lib/palette/context';
	import { formatDuration } from '$lib/runDetail';
	import { setTitleBlock } from '$lib/sheet.svelte';
	import { statusLabel } from '$lib/status';
	import { pollWhileVisible } from '$lib/visibility';

	let { data }: PageProps = $props();

	const sizeOptions = [10, 25, 50, 100].map((size) => ({ value: String(size), label: String(size) }));

	// One Orkflow, when the filters name exactly one: the header says so and
	// Submit a run goes to its page. The standalone Submit Run page is gone —
	// a run is started from the Orkflow it belongs to — so a list that is not
	// narrowed to one can only offer the picker.
	const orkflowName = $derived(equalityOf(data.applied, 'orkflow_name') ?? '');
	const submitHref = $derived(
		withNamespace(orkflowName ? `/orkflows/${encodeURIComponent(orkflowName)}` : '/orkflows', data.namespace)
	);

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
		void goto(href({ size: sizeDraft || '25', cursor: '', page: '' }, { filter: filters }), { keepFocus: true, noScroll: true });
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

	// ---- Changed-run marks (this browser only, $lib/heldRuns) ----
	let held = $state.raw(new Map<string, string>());
	const ms = (t: Run['createdAt']) => (t ? Number(t.seconds) * 1000 + Math.floor(t.nanos / 1e6) : 0);
	function listed(runs: Run[]): ListedRun[] {
		return runs.map((run) => ({ id: run.id, status: statusLabel(statusOf(run)), createdMs: ms(run.createdAt), orkflow: run.orkflowName }));
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
	$effect(() => {
		if (data.error || data.runs.length === 0) return;
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
				})
				.catch(() => {
					// A missed poll changes nothing: the rows stay as last read.
				});
		}, LIST_POLL_MS);
	});
	const heldCount = $derived(data.runs.filter((r) => held.has(r.id)).length);

	setTitleBlock(() => [
		{ label: 'PAGE', value: 'Runs' },
		{ label: 'NAMESPACE', value: namespaceLabel(data.namespace) },
		{ label: 'FILTERS', value: data.applied.length ? String(data.applied.length) : 'none' },
		{ label: 'SHOWING', value: `page ${data.page} · ${data.runs.length} ${data.runs.length === 1 ? 'run' : 'runs'}` }
	]);

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
	const command = $derived(['orkctl', '-n', shellQuote(namespaceLabel(data.namespace)), 'run', 'list']);
</script>

<svelte:head><title>Runs — OrkStack</title></svelte:head>

<SheetHeader title={orkflowName ? `Runs for ${orkflowName}` : 'Runs'} testid="runs-heading">
	{#snippet actions()}
		<a href={submitHref} data-testid="runs-submit-run" class="btn btn-primary">
			{orkflowName ? 'Submit a run' : 'Browse orkflows'}
		</a>
	{/snippet}
</SheetHeader>

<ListFilter request={ListRunsRequestSchema} applied={data.applied} testid="run" {command} onapply={applyFilters} {clearHref}>
	<div class="flex flex-col gap-1.5 text-sm">
		<label class="field-label mb-0" for="run-filter-size">Page size</label>
		<div class="w-24"><Select id="run-filter-size" testid="run-filter-size" value={sizeDraft} options={sizeOptions} onchange={(next) => (sizeDraft = next)} /></div>
	</div>
</ListFilter>

{#if data.error}
	<ErrorPanel
		testid="engine-error"
		retryTestid="runs-retry"
		subject="the runs in namespace {namespaceLabel(data.namespace)}"
		message={data.error}
		busy={retrying}
		onretry={retry}
	/>
{:else if retrying}
	<p class="text-sm text-ink-muted" data-testid="runs-loading">Loading runs…</p>
{:else if data.runs.length === 0}
	<p class="border border-dashed border-rule-strong px-6 py-12 text-center text-sm text-ink-muted" data-testid="runs-empty">
		{#if data.applied.length > 0}
			No runs match these filters in namespace {namespaceLabel(data.namespace)}.
		{:else}
			No runs in namespace {namespaceLabel(data.namespace)} yet.
		{/if}
	</p>
{:else}
	<div class="mb-2 flex min-h-8 flex-wrap items-center gap-x-4 gap-y-2" aria-live="polite">
		{#if heldCount > 0}
			<p data-testid="runs-held-summary" class="inline-flex items-center gap-1.5 text-sm text-ink">
				<Icon name="delta" class="size-3.5 text-red" />
				{heldCount} {heldCount === 1 ? 'run' : 'runs'} changed since you last looked
			</p>
			<button type="button" data-testid="runs-held-clear" class="btn btn-sm" onclick={markAll}>Mark all seen</button>
		{/if}
		{#if newer > 0}
			<button type="button" data-testid="runs-newer" class="btn btn-sm" onclick={() => void retry()}
				>{newer} newer {newer === 1 ? 'run' : 'runs'} — show</button
			>
		{/if}
	</div>
	<div class="overflow-x-auto border-t-2 border-ink">
		<table class="register min-w-[760px]" data-testid="run-list">
			<thead>
				<tr>
					<th scope="col">Run</th>
					<th scope="col">Orkflow</th>
					<th scope="col">Origin</th>
					<th scope="col">Status</th>
					<th scope="col">Created</th>
					<th scope="col" title="From submission to finish">Duration</th>
				</tr>
			</thead>
			<tbody>
				{#each data.runs as run (run.id)}
					{@const status = statusOf(run)}
					{@const from = held.get(run.id)}
					<tr
						class="group h-11 {from !== undefined
							? status === Status.FAILED
								? 'bg-red-tint'
								: 'bg-paper-sunken'
							: 'hover:bg-hover'}"
						data-testid="run-row-{run.id}"
						data-held={from !== undefined ? 'true' : undefined}
						data-held-from={from}
					>
						<td class="whitespace-nowrap">
							{#if from !== undefined}<span
									role="img"
									aria-label={heldLabel(from)}
									data-testid="run-row-held-{run.id}"
									class="mr-1.5 inline-flex align-middle text-red"><Icon name="delta" class="size-3.5" /></span
								>{/if}<a
								href={withNamespace(`/runs/${run.id}`, data.namespace)}
								class="font-mono text-xs font-medium text-ink group-hover:underline hover:text-blue">{run.id}</a
							>
						</td>
						<td class="text-ink-secondary">{run.orkflowName}@{run.orkflowVersion}</td>
						<td class="text-ink-secondary">
							{#if run.origin && originKindSlug(run.origin.kind)}
								<span data-testid="run-row-origin-{run.id}"
									>{originKindSlug(run.origin.kind)}{#if run.origin.id}{' '}<span class="font-mono text-xs"
											>{run.origin.id}</span
										>{/if}</span
								>
							{:else}
								<span data-testid="run-row-origin-{run.id}" class="text-ink-muted" title={UNRECORDED_CAPTION}
									>{UNRECORDED}</span
								>
							{/if}
						</td>
						<td><StatusPill {status} /></td>
						<td class="figure text-xs whitespace-nowrap text-ink-secondary">{fmtTimestamp(run.createdAt)}</td>
						<td class="figure text-xs whitespace-nowrap text-ink-secondary">{finishedOf(run) ? formatDuration(run.createdAt, finishedOf(run)) : '—'}</td>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
	<Pagination testid="runs" noun="run" page={data.page} count={data.runs.length} {previousHref} {nextHref} {firstHref} onnext={next} />
{/if}
