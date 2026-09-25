<script lang="ts">
	// Admin → Audit: the control-plane audit trail, read back.
	//
	// Three things this page owes a reader, each stated on the page rather than
	// left to be inferred:
	//   - WHAT it is showing: the scope line says whether it is every namespace
	//     (the default, deployment-wide writes included) or one namespace. The
	//     namespace switcher is a FILTER here, applied only when asked for — a
	//     third behaviour beside "scoped" and "ignored", so it is said out loud;
	//   - WHO did it is an assertion: the caption under every rendering, empty
	//     page included, says the principal is what a gateway asserted and that
	//     OrkStack does not authenticate it; an absent assertion reads
	//     `unattributed`, never a blank that could pass for a name;
	//   - a failed read is a failure, never an empty table. A scan the engine
	//     refused for its budget says so beside the window inputs, because
	//     narrowing the window is the fix.
	//
	// No write exists on this page, so there is nothing to confirm.
	import { goto, invalidateAll } from '$app/navigation';
	import CliEquivalent from '$lib/components/CliEquivalent.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import Pagination from '$lib/components/Pagination.svelte';
	import Select from '$lib/components/Select.svelte';
	import {
		AUDIT_ACTION_OPTIONS,
		AUDIT_CAPTION,
		auditCliCommand,
		auditHref,
		auditPrincipalLabel,
		auditResultLabel,
		auditStackKey,
		type AuditCoordinates
	} from '$lib/audit';
	import { readPageStack, writePageStack } from '$lib/browsing';
	import { Status } from '$lib/gen/orkstack/v1/common_pb';
	import { fmtTimestamp, statusClasses } from '$lib/status';
	import { timestampDate } from '@bufbuild/protobuf/wkt';
	import type { Timestamp } from '@bufbuild/protobuf/wkt';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	const c = $derived(data.coordinates);
	const records = $derived(data.audit?.records ?? []);

	const scopeOptions = $derived([
		{ value: 'all', label: 'All namespaces' },
		{ value: 'namespace', label: `Namespace ${c.namespace}` }
	]);

	// The window the answer covers, resolved on the engine's clock: what the
	// inputs show when the URL names none, so "the last week" is a pair of
	// instants a reader can read.
	const iso = (ts: Timestamp | undefined) => (ts ? timestampDate(ts).toISOString().replace('.000Z', 'Z') : '');
	const resolvedSince = $derived(iso(data.audit?.since));
	const resolvedUntil = $derived(iso(data.audit?.until));

	// The filter row is a DRAFT of the URL (the runs list's rule): it holds
	// what has been typed but not applied, and any navigation refills it from
	// what the page loaded.
	function draftFromData() {
		return {
			action: c.action,
			principal: c.principal,
			since: c.since || resolvedSince,
			until: c.until || resolvedUntil
		};
	}
	let draft = $state(draftFromData());
	$effect(() => {
		draft = draftFromData();
	});

	function go(changes: Partial<AuditCoordinates>) {
		// Any filter change drops the cursor and the page number: a token is
		// bound to the filters it was issued for.
		void goto(auditHref(c, { cursor: '', page: 1, ...changes }));
	}
	function apply(event: SubmitEvent) {
		event.preventDefault();
		// A window left exactly as the engine resolved it stays unset, so an
		// unfiltered listing keeps meaning "the last week, up to now".
		go({
			action: draft.action,
			principal: draft.principal.trim(),
			since: draft.since === resolvedSince && !c.since ? '' : draft.since.trim(),
			until: draft.until === resolvedUntil && !c.until ? '' : draft.until.trim()
		});
	}

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
		{#if c.scope === 'namespace'}
			Showing namespace <span class="font-medium text-fg">{c.namespace}</span>. Deployment-wide writes
			(retention, webhooks) carry no namespace and are listed only under all namespaces.
		{:else}
			Showing every namespace, deployment-wide writes included. The namespace selector does not
			filter this page unless you choose it here.
		{/if}
	</p>
	<div class="flex items-center gap-2">
		<Select
			testid="audit-scope-select"
			value={c.scope}
			options={scopeOptions}
			onchange={(value) => go({ scope: value === 'namespace' ? 'namespace' : 'all' })}
		/>
		<CliEquivalent testid="audit-cli" command={auditCliCommand(c)} label="orkctl admin audit for this listing" />
	</div>
</div>

<form
	class="mb-5 rounded-lg border border-border bg-surface p-4 shadow-xs"
	data-testid="audit-filters"
	onsubmit={apply}
>
	<div class="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
		<div class="flex flex-col gap-1.5 text-sm">
			<span class="font-medium text-fg">Action</span>
			<Select
				testid="audit-filter-action"
				value={draft.action}
				options={AUDIT_ACTION_OPTIONS}
				onchange={(value) => go({ action: value, principal: draft.principal.trim() })}
			/>
		</div>
		<label class="flex flex-col gap-1.5 text-sm">
			<span class="font-medium text-fg">Principal</span>
			<input
				data-testid="audit-filter-principal"
				bind:value={draft.principal}
				placeholder="exact asserted name"
				class="h-9 rounded-md border border-border bg-surface px-3 text-sm"
			/>
		</label>
		<label class="flex flex-col gap-1.5 text-sm">
			<span class="font-medium text-fg">Since <span class="font-normal text-fg-muted">(inclusive)</span></span>
			<input
				data-testid="audit-filter-since"
				bind:value={draft.since}
				placeholder="2026-01-01T00:00:00Z"
				class="h-9 rounded-md border border-border bg-surface px-3 font-mono text-xs"
			/>
		</label>
		<label class="flex flex-col gap-1.5 text-sm">
			<span class="font-medium text-fg">Until <span class="font-normal text-fg-muted">(exclusive)</span></span>
			<input
				data-testid="audit-filter-until"
				bind:value={draft.until}
				placeholder="now"
				class="h-9 rounded-md border border-border bg-surface px-3 font-mono text-xs"
			/>
		</label>
	</div>
	{#if data.auditErrorCode === 'resource_exhausted' || data.auditErrorCode === 'invalid_argument'}
		<p class="mt-3 text-sm text-danger-700" data-testid="audit-window-error">{data.auditError}</p>
	{/if}
	<div class="mt-4 flex items-center gap-2">
		<button
			type="submit"
			data-testid="audit-filter-apply"
			class="inline-flex h-9 items-center rounded-md bg-brand-600 px-3.5 text-sm font-medium text-white shadow-xs hover:bg-brand-700"
		>
			Apply
		</button>
		<a
			href={auditHref(c, { action: '', principal: '', since: '', until: '', cursor: '', page: 1 })}
			data-testid="audit-filter-clear"
			class="inline-flex h-9 items-center rounded-md border border-border bg-surface px-3.5 text-sm font-medium text-fg shadow-xs hover:bg-surface-hover"
		>
			Clear
		</a>
	</div>
</form>

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
		No audit records in this window{c.page > 1 ? ' past the previous page' : ''}.
	</p>
	{#if c.page > 1}
		<!-- A full page hands out a token even when nothing older matches, so a
		     later page can be empty; it still needs its way back. -->
		<Pagination testid="audit" noun="record" page={c.page} count={0} {previousHref} {firstHref} />
	{/if}
{:else}
	<div class="overflow-x-auto rounded-lg border border-border bg-surface shadow-xs">
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
								class="inline-flex items-center rounded-full border px-2 py-0.5 text-[11px] font-medium tracking-wide whitespace-nowrap leading-4 {refused(rec.succeeded)}"
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
