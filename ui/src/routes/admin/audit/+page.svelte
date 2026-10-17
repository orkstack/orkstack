<script lang="ts">
	// Admin → Audit: the control-plane audit trail, read back.
	//
	// Three things this page owes a reader, each stated on the page rather than
	// left to be inferred:
	//   - WHAT it is showing: the scope line says whether it is every namespace
	//     (the default, deployment-wide writes included), only the
	//     deployment-wide writes (`namespace=`), or one namespace. The trail is
	//     a deployment-wide listing: the namespace switcher does not filter it,
	//     a `namespace` filter does — so it is said out loud;
	//   - WHO did it is an assertion: the caption under every rendering, empty
	//     page included, says the principal is what a gateway asserted and that
	//     OrkStack does not authenticate it; an absent assertion reads
	//     `unattributed`, never a blank that could pass for a name;
	//   - a failed read is a failure, never an empty table. A refused filter or
	//     a scan the engine refused for its budget is the engine's message,
	//     verbatim; narrowing the `at` window is the fix for the second.
	//
	// The filters are the one filter component's (ListFilter): what may be
	// filtered is the ListAuditRecordsRequest allowlist. No write exists on
	// this page, so there is nothing to confirm.
	import { goto, invalidateAll } from '$app/navigation';
	import CliEquivalent from '$lib/components/CliEquivalent.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import ListFilter from '$lib/components/ListFilter.svelte';
	import Pagination from '$lib/components/Pagination.svelte';
	import {
		AUDIT_CAPTION,
		auditCliCommand,
		auditHref,
		auditPrincipalLabel,
		auditResultLabel,
		auditScopeSentence,
		auditStackKey
	} from '$lib/audit';
	import { readPageStack, writePageStack } from '$lib/browsing';
	import { Status } from '$lib/gen/orkstack/v1/common_pb';
	import { ListAuditRecordsRequestSchema } from '$lib/gen/orkstack/v1/audit_pb';
	import type { FieldFilter } from '$lib/listFilter';
	import { fmtTimestamp, statusClasses } from '$lib/status';
	import { timestampDate } from '@bufbuild/protobuf/wkt';
	import type { Timestamp } from '@bufbuild/protobuf/wkt';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	const c = $derived(data.coordinates);
	const records = $derived(data.audit?.records ?? []);

	// The window the answer covers, resolved on the engine's clock, so "the
	// last week" is a pair of instants a reader can read.
	const iso = (ts: Timestamp | undefined) => (ts ? timestampDate(ts).toISOString().replace('.000Z', 'Z') : '');
	const resolvedSince = $derived(iso(data.audit?.since));
	const resolvedUntil = $derived(iso(data.audit?.until));

	// Any filter change drops the cursor and the page number: a token is bound
	// to the filter set it was issued for.
	function applyFilters(filters: FieldFilter[]) {
		void goto(auditHref(c, { filters, cursor: '', page: 1 }));
	}
	const clearHref = $derived(auditHref(c, { filters: [], cursor: '', page: 1 }));

	let retrying = $state(false);
	async function retry() {
		retrying = true;
		try {
			await invalidateAll();
		} finally {
			retrying = false;
		}
	}

	// The cursors BEHIND this page, per filter set, per tab — the runs list's
	// sessionStorage stack (lib/browsing.ts), so the URL carries one cursor.
	const stackKey = $derived(auditStackKey(c));
	let stack = $state<string[]>([]);
	$effect(() => {
		stack = readPageStack(stackKey);
	});
	function next() {
		const ahead = Array.from({ length: Math.max(stack.length, c.page + 1) }, (_, at) => stack[at] ?? '');
		[ahead[c.page - 1], ahead[c.page]] = [c.cursor, data.audit?.nextPageToken ?? ''];
		writePageStack(stackKey, ahead);
		stack = ahead;
	}
	const walked = $derived(c.page > 1 && stack[c.page - 1] === c.cursor);
	const previousHref = $derived(
		walked ? auditHref(c, { cursor: stack[c.page - 2] ?? '', page: c.page - 1 }) : undefined
	);
	const nextHref = $derived(
		data.audit?.nextPageToken ? auditHref(c, { cursor: data.audit.nextPageToken, page: c.page + 1 }) : undefined
	);
	const firstHref = $derived(auditHref(c, { cursor: '', page: 1 }));
	const refused = (ok: boolean) => statusClasses(ok ? Status.SUCCEEDED : Status.FAILED);
</script>

<svelte:head><title>Admin — Audit — OrkStack</title></svelte:head>

<div class="mb-4 flex flex-wrap items-center justify-between gap-3">
	<p class="text-sm text-fg-muted" data-testid="audit-scope">
		{auditScopeSentence(c.filters)}{#if data.audit}
			<span class="font-mono text-xs" data-testid="audit-window">{resolvedSince} to {resolvedUntil}</span>.{/if}
	</p>
	<CliEquivalent testid="audit-cli" command={auditCliCommand(c)} label="orkctl admin audit for this listing" />
</div>

<ListFilter
	request={ListAuditRecordsRequestSchema}
	applied={c.filters}
	testid="audit"
	command={['orkctl', 'admin', 'audit']}
	onapply={applyFilters}
	{clearHref}
/>

{#if data.auditError}
	<ErrorPanel
		testid="audit-error"
		subject="the audit trail"
		message={data.auditError}
		busy={retrying}
		onretry={retry}
	/>
{:else if records.length === 0}
	<p class="text-sm text-fg-muted" data-testid="audit-empty">
		{#if c.filters.length > 0 && c.page === 1}No audit records match these filters.{:else}No audit records in this window{c.page > 1 ? ' past the previous page' : ''}.{/if}
	</p>
	{#if c.page > 1}
		<!-- A full page hands out a token even when nothing older matches, so a
		     later page can be empty; it still needs its way back. -->
		<Pagination testid="audit" noun="record" page={c.page} count={0} {previousHref} {firstHref} />
	{/if}
{:else}
	<div class="overflow-x-auto rounded-md border border-border bg-surface">
		<table class="w-full min-w-[820px] text-left text-sm" data-testid="audit-table">
			<thead class="bg-surface-sunken text-xs text-fg-muted">
				<tr>
					<th class="p-3 font-medium">When</th>
					<th class="p-3 font-medium">Principal</th>
					<th class="p-3 font-medium">Action</th>
					<th class="p-3 font-medium">Namespace</th>
					<th class="p-3 font-medium">Target</th>
					<th class="p-3 font-medium">Result</th>
				</tr>
			</thead>
			<tbody>
				{#each records as rec (rec.id)}
					<tr class="border-t border-border-subtle align-top" data-testid="audit-row" data-record-id={rec.id}>
						<td class="p-3 text-xs whitespace-nowrap text-fg-secondary" data-testid="audit-when">{fmtTimestamp(rec.at)}</td>
						<td
							class="p-3 {rec.principal ? 'text-fg' : 'italic text-fg-muted'}"
							data-testid="audit-principal">{auditPrincipalLabel(rec.principal)}</td
						>
						<td class="p-3 font-mono text-xs" data-testid="audit-action">{rec.action}</td>
						<td class="p-3 text-xs" data-testid="audit-namespace">{rec.namespace || '-'}</td>
						<td class="p-3 break-all font-mono text-xs" data-testid="audit-target">
							{rec.target || '-'}
							{#if rec.detail}
								<span class="mt-1 block font-sans text-xs text-fg-muted" data-testid="audit-detail">{rec.detail}</span>
							{/if}
						</td>
						<td class="p-3">
							<span
								data-testid="audit-result"
								class="inline-flex items-center rounded-full border px-2 py-0.5 text-2xs font-medium tracking-caps whitespace-nowrap leading-4 {refused(rec.succeeded)}"
								>{auditResultLabel(rec)}</span
							>
						</td>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
	<Pagination
		testid="audit"
		noun="record"
		page={c.page}
		count={records.length}
		{previousHref}
		{nextHref}
		{firstHref}
		onnext={next}
	/>
{/if}

<p class="mt-5 text-xs text-fg-muted" data-testid="audit-caption">{AUDIT_CAPTION}</p>
