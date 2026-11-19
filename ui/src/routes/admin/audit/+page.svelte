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
	import ErrorPanel from '#lib/components/ErrorPanel.svelte';
	import { principalMark } from '#lib/principal.ts';

	// The decision a request was made under (access control on): allowed,
	// denied, or would-deny in observe mode, which served it anyway.
	function decisionWord(d: string): string {
		return d === 'would-deny' ? 'Would deny (observed)' : d === 'denied' ? 'Denied' : d === 'allowed' ? 'Allowed' : d;
	}
	import ListFilter from '#lib/components/ListFilter.svelte';
	import Pagination from '#lib/components/Pagination.svelte';
	import {
		AUDIT_CAPTION,
		auditCliCommand,
		auditHref,
		auditPrincipalLabel,
		auditResultLabel,
		auditScopeSentence,
		auditShortcuts,
		auditStackKey
	} from '#lib/audit.ts';
	import { readPageStack, writePageStack } from '#lib/browsing.ts';
	import { Status } from '#lib/gen/orkstack/v1/common_pb.ts';
	import { ListAuditRecordsRequestSchema } from '#lib/gen/orkstack/v1/audit_pb.ts';
	import type { FieldFilter } from '#lib/listFilter.ts';
	import { fmtTimestamp } from '#lib/status.ts';
	import { stampClass } from '#lib/statusMarks.ts';
	import ScopeNote from '#lib/components/ScopeNote.svelte';
	import SheetHeader from '#lib/components/SheetHeader.svelte';
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
	// The questions access control raises of the trail, one click each; only
	// where the deployment authenticates, since an asserted deployment records
	// no decision and verifies no principal.
	const shortcuts = $derived(data.authMode === 'observe' || data.authMode === 'enforce' ? auditShortcuts(c.filters) : []);

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

<SheetHeader title="Audit" commands={[{ label: 'This listing', command: auditCliCommand(c) }]} />

<ScopeNote testid="audit-scope">
	{auditScopeSentence(c.filters)}{#if data.audit}{' '}<span class="font-mono text-xs" data-testid="audit-window">{resolvedSince} to {resolvedUntil}</span>.{/if}
</ScopeNote>

<div class="mb-3">
	<ListFilter request={ListAuditRecordsRequestSchema} applied={c.filters} testid="audit" onapply={applyFilters} {clearHref} />
</div>

{#if shortcuts.length > 0}
	<!-- Independent toggles, not one choice: each is its own pill, and the one
		in force is marked current and removes itself when pressed again. -->
	<div class="mb-3 flex flex-wrap items-center gap-2" role="group" aria-label="Access shortcuts" data-testid="audit-shortcuts">
		<span class="caps text-ink-muted" aria-hidden="true">Access</span>
		{#each shortcuts as s (s.testid)}
			<a
				href={auditHref(c, { filters: s.filters, cursor: '', page: 1 })}
				data-testid="audit-shortcut-{s.testid}"
				aria-current={s.active ? 'true' : undefined}
				class="shortcut"
				>{s.label}{#if s.active}<span class="sr"> (on; press to remove)</span>{/if}</a
			>
		{/each}
	</div>
{/if}

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
					<th scope="col">Principal</th>
					<th scope="col">Action</th>
					<th scope="col">Namespace</th>
					<th scope="col" class="w-[38%]">Target</th>
					<th scope="col">Decision</th>
					<th scope="col">Result</th>
				</tr>
			</thead>
			<tbody>
				{#each records as rec (rec.id)}
					<tr class="align-top {rec.succeeded ? '' : 'bg-red-tint'}" data-testid="audit-row" data-record-id={rec.id}>
						<td class="figure text-xs whitespace-nowrap text-ink-secondary" data-testid="audit-when">{fmtTimestamp(rec.at)}</td>
						<td>
							{#if rec.principal}<span class="block text-2xs text-ink-muted" data-principal-mark={principalMark(rec.principalVerified, rec.authMethod).word}>{principalMark(rec.principalVerified, rec.authMethod).word}{rec.authMethod ? ` (${rec.authMethod})` : ''} by</span>{/if}
							<span
								class={rec.principal ? 'font-mono text-xs text-ink' : 'italic text-ink-muted'}
								data-testid="audit-principal">{auditPrincipalLabel(rec.principal)}</span
							>
						</td>
						<td class="font-mono text-xs" data-testid="audit-action">{rec.action}</td>
						<td class="font-mono text-xs whitespace-nowrap" data-testid="audit-namespace">{rec.namespace || '-'}</td>
						<td class="target break-all font-mono text-xs" data-testid="audit-target">
							{rec.target || '-'}
							{#if rec.detail}
								<span class="mt-1 block font-sans text-xs text-ink-muted [overflow-wrap:anywhere] [word-break:normal]" data-testid="audit-detail">{rec.detail}</span>
							{/if}
						</td>
						<td class="text-xs whitespace-nowrap" data-testid="audit-decision">
							{#if rec.decision}<span>{decisionWord(rec.decision)}</span>{#if rec.rule}<span class="block font-mono text-2xs text-ink-muted">{rec.rule}</span>{/if}{:else}<span class="text-ink-muted">Not decided</span>{/if}
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

<style>
	/* An access shortcut: a full pill, as chips are (ui/DESIGN.md), 32 px and
	   40 px on phones; the one in force is marked as "you are here" is. Scoped
	   here, so the shell's stylesheet does not grow. */
	.shortcut {
		display: inline-flex;
		align-items: center;
		height: 2rem;
		padding: 0 0.75rem;
		border: 1px solid var(--color-rule-strong);
		border-radius: 9999px;
		background: var(--color-paper);
		color: var(--color-ink-secondary);
		font-size: var(--text-sm);
		text-decoration: none;
		transition: background-color 120ms, color 120ms;
	}
	.shortcut:hover {
		background: var(--color-hover);
		color: var(--color-ink);
	}
	.shortcut[aria-current='true'] {
		border-color: var(--color-blue-strong);
		background: var(--color-blue-tint);
		color: var(--color-blue-strong);
		font-weight: 600;
	}
	/* The target and its detail break anywhere, so on their own they would
	   let the table squeeze them to a word a line beside the fixed columns:
	   the column keeps room for a readable line. */
	.target {
		min-width: 18rem;
	}
	@media (max-width: 639px) {
		.shortcut {
			height: 2.5rem;
		}
	}
	@media (forced-colors: active) {
		.shortcut[aria-current='true'] {
			outline: 2px solid Highlight;
			outline-offset: -2px;
		}
	}
</style>
