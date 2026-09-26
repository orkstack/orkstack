<script lang="ts">
	import { goto, invalidateAll } from '$app/navigation';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import HoldBanner from '$lib/components/HoldBanner.svelte';
	import Pagination from '$lib/components/Pagination.svelte';
	import Select from '$lib/components/Select.svelte';
	import { listHref, pageStackKey, readPageStack, writePageStack } from '$lib/browsing';
	import { fmtTimestamp } from '$lib/status';
	import { namespaceLabel, withNamespace } from '$lib/namespace';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();
	const sizeOptions = [10, 25, 50, 100].map((size) => ({ value: String(size), label: String(size) }));

	// The coordinates of what this list is showing, and one builder over them —
	// the same shape the Runs page keeps. listHref only ever `set`s: the four
	// page-size links this replaces concatenated `&size=N` onto a query that
	// already carried `size=25`, so the URL read `size=25&size=50` and
	// `searchParams.get` answered with the size the reader had just left.
	const coordinates = $derived({ ns: namespaceLabel(data.namespace), size: String(data.size), cursor: data.cursor, page: String(data.page) });
	function href(changes: Record<string, string>) { return listHref('/orkflows', { ...coordinates, ...changes }); }

	// The cursors BEHIND this page, per filter set and per tab; see the same
	// block in runs/+page.svelte and §3.2 of
	// docs/design/pagination-and-timerange-ui.md for why they are not in the URL.
	const stackKey = $derived(pageStackKey('/orkflows', coordinates));
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

	// Page size applies ON CHANGE here, and waits for Apply filters on the Runs
	// page. That is not an inconsistency to tidy away: there, page size is one
	// field of a filter form that is a draft of the URL, and a form that
	// applied one field on change and the rest on submit would be two
	// mechanisms. This page has no form, so the control is the gesture.
	//
	// A new size is a new page 1 of a new filter set, so the path back through
	// the old one is dropped rather than left to be trusted later.
	function setSize(size: string) {
		writePageStack(stackKey, []);
		void goto(href({ size, cursor: '', page: '' }));
	}

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

<div class="mb-6 flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
	<h1 class="text-xl font-semibold tracking-tight text-fg">Orkflows</h1>
	<a
		href={withNamespace('/apply', data.namespace)}
		class="inline-flex h-9 shrink-0 items-center justify-center gap-1.5 rounded-md bg-brand-600 px-3.5 text-sm font-medium text-white shadow-xs transition-colors duration-150 hover:bg-brand-700 active:bg-brand-800"
	>
		Apply orkflow
	</a>
</div>

{#if data.error}
	<ErrorPanel
		testid="engine-error"
		retryTestid="orkflows-retry"
		subject="the orkflows in namespace {namespaceLabel(data.namespace)}"
		message={data.error}
		busy={retrying}
		onretry={retry}
	/>
{:else if retrying}
	<p class="text-sm text-fg-muted" data-testid="orkflows-loading">Loading orkflows…</p>
{:else if data.orkflows.length === 0}
	<p
		class="rounded-lg border border-dashed border-border px-6 py-12 text-center text-sm text-fg-muted"
		data-testid="orkflows-empty"
	>
		No orkflows in namespace {namespaceLabel(data.namespace)} yet. Apply one from a YAML document
		to get started.
	</p>
{:else}
	<div class="overflow-x-auto rounded-lg border border-border bg-surface shadow-xs">
		<table class="w-full min-w-[640px] border-collapse text-left text-sm" data-testid="orkflow-list">
			<thead class="bg-surface-sunken">
				<tr>
					<th class="h-10 px-4 pl-5 text-xs font-medium text-fg-muted">Name</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">Version</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">Description</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">Tasks</th>
					<th class="h-10 px-4 pr-5 text-xs font-medium text-fg-muted">Applied</th>
				</tr>
			</thead>
			<tbody>
				{#each data.orkflows as wf (wf.name + '@' + wf.version)}
					<tr
						class="border-t border-border-subtle transition-colors duration-150 hover:bg-surface-hover"
						data-testid="orkflow-row-{wf.name}"
					>
						<td class="h-11 px-4 pl-5 align-middle font-medium">
							<a
								href={withNamespace(`/orkflows/${wf.name}`, data.namespace)}
								class="text-fg hover:text-brand-700">{wf.name}</a
							>
						</td>
						<td class="h-11 px-4 align-middle text-fg-secondary">{wf.version}</td>
						<td class="h-11 max-w-md truncate px-4 align-middle text-fg-secondary"
							>{wf.description}</td
						>
						<td class="h-11 px-4 align-middle text-fg-secondary tabular-nums" data-testid="orkflow-taskcount-{wf.name}"
							>{wf.tasks.length}</td
						>
						<td class="h-11 px-4 pr-5 align-middle text-fg-muted tabular-nums"
							>{fmtTimestamp(wf.createdAt)}</td
						>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
	<Pagination testid="orkflows" noun="orkflow" page={data.page} count={data.orkflows.length} {previousHref} {nextHref} {firstHref} onnext={next}>
		<div class="flex items-center gap-2 text-sm"><label class="font-medium text-fg" for="orkflow-page-size">Page size</label><div class="w-20"><Select id="orkflow-page-size" testid="orkflow-page-size" value={String(data.size)} options={sizeOptions} onchange={setSize} /></div></div>
	</Pagination>
{/if}
