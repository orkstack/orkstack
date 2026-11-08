<script lang="ts">
	import VersionText from '#lib/components/VersionText.svelte';
	import { goto, invalidateAll } from '$app/navigation';
	import ErrorPanel from '#lib/components/ErrorPanel.svelte';
	import HoldBanner from '#lib/components/HoldBanner.svelte';
	import ListFilter from '#lib/components/ListFilter.svelte';
	import Pagination from '#lib/components/Pagination.svelte';
	import Select from '#lib/components/Select.svelte';
	import SheetHeader from '#lib/components/SheetHeader.svelte';
	import { listHref, pageStackKey, readPageStack, writePageStack } from '#lib/browsing.ts';
	import { fmtTimestamp } from '#lib/status.ts';
	import { namespaceLabel, releaseHoldCli, withNamespace } from '#lib/namespace.ts';
	import { shellQuote } from '#lib/runDetail.ts';
	import { filterFlags, type FieldFilter } from '#lib/listFilter.ts';
	import { ListOrkflowsRequestSchema } from '#lib/gen/orkstack/v1/orkflow_service_pb.ts';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();
	const sizeOptions = [10, 25, 50, 100].map((size) => ({ value: String(size), label: String(size) }));

	// The coordinates of what this list is showing, and one builder over them —
	// the same shape the Runs page keeps. listHref only ever `set`s: the four
	// page-size links this replaces concatenated `&size=N` onto a query that
	// already carried `size=25`, so the URL read `size=25&size=50` and
	// `searchParams.get` answered with the size the reader had just left.
	const coordinates = $derived({ ns: namespaceLabel(data.namespace), size: String(data.size), cursor: data.cursor, page: String(data.page) });
	const applied = $derived({ filter: data.applied });
	function href(changes: Record<string, string>, filters: Record<string, FieldFilter[]> = applied) {
		return listHref('/orkflows', { ...coordinates, ...changes }, filters);
	}

	// The cursors BEHIND this page, per filter set and per tab; see the same
	// block in runs/+page.svelte and docs/UI_CLI_PARITY.md "Paging in the
	// browser" for why they are not in the URL.
	const stackKey = $derived(pageStackKey('/orkflows', coordinates, applied));
	let stack = $state<string[]>([]);
	$effect(() => {
		stack = readPageStack(stackKey);
	});
	function next() {
		const ahead = Array.from({ length: Math.max(stack.length, data.page + 1) }, (_, at) => stack[at] ?? '');
		[ahead[data.page - 1], ahead[data.page]] = [data.cursor, data.nextPageToken];
		writePageStack(stackKey, ahead);
		stack = ahead;
	}
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
		void goto(href({ size: sizeDraft, cursor: '', page: '' }, { filter: filters }), { reset: false });
	}
	const clearHref = $derived(href({ cursor: '', page: '' }, {}));
	const cli = $derived([
		{ label: 'The Orkflows', command: ['orkctl', '-n', shellQuote(namespaceLabel(data.namespace)), 'orkflow', 'list', ...filterFlags(data.applied)].join(' ') },
		...(data.namespaceHold ? [{ label: 'Release the hold', command: releaseHoldCli(namespaceLabel(data.namespace)) }] : [])
	]);

	// The first eight hex digits of a definition digest, as the title block
	// shows one; the whole digest is the cell's title.
	function digest8(digest: string): string {
		const hex = digest.includes(':') ? digest.slice(digest.indexOf(':') + 1) : digest;
		return hex.slice(0, 8);
	}
	const labelPairs = (labels: Record<string, string>) => Object.entries(labels).sort(([a], [b]) => a.localeCompare(b));


	// A failed list is a failed request, not a permanent state of the world:
	// the retry re-issues it in place instead of leaving the reload button as
	// the only way out.
	let retrying = $state(false);
	async function retry() {
		retrying = true;
		try {
			await invalidateAll();
		} finally {
			retrying = false;
		}
	}
</script>

<svelte:head><title>Orkflows — OrkStack</title></svelte:head>

{#if data.namespaceHold}
	<HoldBanner
		namespace={namespaceLabel(data.namespace)}
		hold={data.namespaceHold}
		testid="orkflows-hold-banner"
	/>
{/if}

<SheetHeader title="Orkflows" commands={cli}>
	{#snippet actions()}
		<a href={withNamespace('/apply', data.namespace)} class="btn btn-primary">Apply Orkflow</a>
	{/snippet}
</SheetHeader>

<div class="mb-3"><ListFilter request={ListOrkflowsRequestSchema} applied={data.applied} testid="orkflow" onapply={applyFilters} {clearHref}>
	<div class="flex items-center gap-2 text-sm">
		<label class="field-label mb-0" for="orkflow-page-size">Page size</label>
		<div class="w-20"><Select id="orkflow-page-size" testid="orkflow-page-size" value={sizeDraft} options={sizeOptions} onchange={(next) => (sizeDraft = next)} /></div>
	</div>
</ListFilter></div>

{#if data.error}
	<ErrorPanel
		testid="engine-error"
		retryTestid="orkflows-retry"
		subject="the Orkflows"
		message={data.error}
		busy={retrying}
		onretry={retry}
	/>
{:else if retrying}
	<p class="text-sm text-ink-muted" data-testid="orkflows-loading">Loading Orkflows…</p>
{:else if data.orkflows.length === 0 && data.applied.length > 0}
	<p class="border border-dashed border-rule-strong px-6 py-12 text-center text-sm text-ink-muted" data-testid="orkflows-empty">
		No Orkflows match these filters.
	</p>
{:else if data.orkflows.length === 0}
	<p class="border border-dashed border-rule-strong px-6 py-12 text-center text-sm text-ink-muted" data-testid="orkflows-empty">
		No Orkflows yet. Apply one from a YAML document to get
		started.
	</p>
{:else}
	<!-- One link per row: the NAME. Everything else in a row is text, so a
		row is one place to go and a reader's click on it is never ambiguous. -->
	<div class="overflow-x-auto border-t-2 border-ink">
		<!-- Below 640 px a row stacks: name and version, digest and when it was
			applied. An Orkflow's labels sit under its name and description. -->
		<table class="register register-stacked sm:min-w-[560px]" data-testid="orkflow-list">
			<thead class="max-sm:sr-only">
				<tr>
					<th scope="col">Name</th>
					<th scope="col">Version</th>
					<th scope="col">Digest</th>
					<th scope="col">Applied</th>
				</tr>
			</thead>
			<tbody>
				{#each data.orkflows as wf (wf.name + '@' + wf.version)}
					<tr class="transition-colors hover:bg-hover" data-testid="orkflow-row-{wf.name}">
						<td class="max-w-md">
							<a href={withNamespace(`/orkflows/${wf.name}`, data.namespace)} class="link font-semibold">{wf.name}</a>
							{#if wf.description}<p class="truncate text-xs text-ink-muted" data-tip={wf.description}>{wf.description}</p>{/if}
							{#if labelPairs(wf.labels).length > 0}
								<span class="mt-1 flex flex-wrap gap-1" data-testid="orkflow-labels-{wf.name}">
									{#each labelPairs(wf.labels) as [key, value] (key)}<span class="tag">{key}={value}</span>{/each}
								</span>
							{/if}
						</td>
						<td class="figure text-ink max-sm:col-start-2 max-sm:row-start-1 max-sm:justify-self-end"><VersionText version={wf.version} /></td>
						<td class="figure text-ink-secondary max-sm:row-start-2 max-sm:text-xs" data-tip={wf.definitionDigest || undefined} data-testid="orkflow-digest-{wf.name}"
							>{wf.definitionDigest ? digest8(wf.definitionDigest) : '—'}</td
						>
						<td class="figure text-xs text-ink-secondary max-sm:col-start-2 max-sm:row-start-2 max-sm:justify-self-end">{fmtTimestamp(wf.createdAt)}</td>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
	<Pagination testid="orkflows" noun="Orkflow" page={data.page} count={data.orkflows.length} {previousHref} {nextHref} {firstHref} onnext={next} />
{/if}
