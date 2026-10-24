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
	import { fmtTimestamp } from '$lib/status';
	import { stampClass } from '$lib/statusMarks';
	import ScopeNote from '$lib/components/ScopeNote.svelte';
	import SheetHeader from '$lib/components/SheetHeader.svelte';
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
	const refused = (ok: boolean) => stampClass(ok ? Status.SUCCEEDED : Status.FAILED);
</script>

<svelte:head><title>Admin — Audit — OrkStack</title></svelte:head>

<SheetHeader title="Audit">
	{#snippet actions()}
		<CliEquivalent testid="audit-cli" command={auditCliCommand(c)} label="orkctl admin audit for this listing" />
	{/snippet}
</SheetHeader>

<ScopeNote testid="audit-scope">
	{auditScopeSentence(c.filters)}{#if data.audit}{' '}<span class="font-mono text-xs" data-testid="audit-window">{resolvedSince} to {resolvedUntil}</span>.{/if}
</ScopeNote>

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
	<p class="text-sm text-ink-muted" data-testid="audit-empty">
		{#if c.filters.length > 0 && c.page === 1}No audit records match these filters.{:else}No audit records in this window{c.page > 1 ? ' past the previous page' : ''}.{/if}
	</p>
	{#if c.page > 1}
		<!-- A full page hands out a token even when nothing older matches, so a
		     later page can be empty; it still needs its way back. -->
		<Pagination testid="audit" noun="record" page={c.page} count={0} {previousHref} {firstHref} />
	{/if}
{:else}
	<div class="overflow-x-auto border-t-2 border-ink">
		<table class="register min-w-[820px]" data-testid="audit-table">
			<thead>
				<tr>
					<th scope="col">When</th>
					<th scope="col">Principal <span class="font-normal normal-case tracking-normal">(asserted)</span></th>
					<th scope="col">Action</th>
					<th scope="col">Namespace</th>
					<th scope="col">Target</th>
					<th scope="col">Result</th>
				</tr>
			</thead>
			<tbody>
				{#each records as rec (rec.id)}
					<tr class="align-top {rec.succeeded ? '' : 'bg-red-tint'}" data-testid="audit-row" data-record-id={rec.id}>
						<td class="figure text-xs whitespace-nowrap text-ink-secondary" data-testid="audit-when">{fmtTimestamp(rec.at)}</td>
						<td>
							{#if rec.principal}<span class="block text-2xs text-ink-muted">asserted by</span>{/if}
							<span
								class={rec.principal ? 'font-mono text-xs text-ink' : 'italic text-ink-muted'}
								data-testid="audit-principal">{auditPrincipalLabel(rec.principal)}</span
							>
						</td>
						<td class="font-mono text-xs" data-testid="audit-action">{rec.action}</td>
						<td class="font-mono text-xs whitespace-nowrap" data-testid="audit-namespace">{rec.namespace || '-'}</td>
						<td class="break-all font-mono text-xs" data-testid="audit-target">
							{rec.target || '-'}
							{#if rec.detail}
								<span class="mt-1 block font-sans text-xs text-ink-muted" data-testid="audit-detail">{rec.detail}</span>
							{/if}
						</td>
						<td>
							<span
								data-testid="audit-result"
								class={refused(rec.succeeded)}
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

<p class="mt-5 max-w-prose text-xs text-ink-muted" data-testid="audit-caption">{AUDIT_CAPTION}</p>
