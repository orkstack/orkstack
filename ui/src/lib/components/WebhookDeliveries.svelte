<script lang="ts">
	// Admin → Webhooks → Deliveries: what each delivery actually did.
	//
	// A delivery record is a LOG, never a queue: one immutable record per
	// settled delivery, read here to answer an operator and by nothing that
	// decides what to send. So this panel owes a reader three things, stated
	// rather than inferred:
	//   - WHAT it shows: the caption names the window the engine resolved and
	//     whether every namespace or a namespace filter's selection is shown;
	//     the page's namespace selector does not filter it (the webhook surface
	//     is deployment-wide);
	//   - what a record IS: every attempt, the signature that was sent and the
	//     body's SHA-256 — the body itself is not stored;
	//   - a failed read is a failure, never an empty table. A refused filter or
	//     a scan the engine refused for its budget is the engine's message,
	//     verbatim.
	// The filters are the one filter component's (ListFilter, `deliveryFilter`
	// parameters): what may be filtered is the ListDeliveriesRequest allowlist.
	// Redeliver is one POST, now, behind a confirmation that says exactly that.
	import { goto, invalidateAll } from '$app/navigation';
	import CliEquivalent from '$lib/components/CliEquivalent.svelte';
	import ConfirmDialog from '$lib/components/ConfirmDialog.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import ListFilter from '$lib/components/ListFilter.svelte';
	import Pagination from '$lib/components/Pagination.svelte';
	import { readPageStack, writePageStack } from '$lib/browsing';
	import { errorMessage } from '$lib/client';
	import { webhookClient } from '$lib/clients/webhooks';
	import {
		DELIVERIES_CAPTION,
		deliveriesCliCommand,
		deliveriesHref,
		deliveriesNamespaceScope,
		deliveriesStackKey,
		instantMs,
		instantSeconds,
		lastAttemptLabel,
		millis,
		redeliveryResult,
		redeliverySentence,
		runDeliveriesFilters,
		type DeliveryCoordinates
	} from '$lib/deliveries';
	import { Status } from '$lib/gen/orkstack/v1/common_pb';
	import { EventType } from '$lib/gen/orkstack/v1/run_pb';
	import { ListDeliveriesRequestSchema, type Delivery, type ListDeliveriesResponse, type Webhook } from '$lib/gen/orkstack/v1/webhook_service_pb';
	import type { FieldFilter } from '$lib/listFilter';
	import { fmtTimestamp, statusClasses } from '$lib/status';

	interface Props {
		coordinates: DeliveryCoordinates;
		response: ListDeliveriesResponse | null;
		error: string | null;
		webhooks: Webhook[];
	}
	let { coordinates: c, response, error, webhooks }: Props = $props();

	const eventName = (event: EventType) => EventType[event] ?? '';
	const deliveries = $derived(response?.deliveries ?? []);
	const resolvedSince = $derived(instantSeconds(response?.since));
	const resolvedUntil = $derived(instantSeconds(response?.until));

	// Any filter change drops the cursor and the page: a token is bound to the
	// filter set it was issued for.
	function applyFilters(filters: FieldFilter[]) {
		void goto(deliveriesHref(c, { filters, cursor: '', page: 1 }, 'deliveries'), { noScroll: true, keepFocus: true });
	}
	const clearHref = $derived(deliveriesHref(c, { filters: [], cursor: '', page: 1 }, 'deliveries'));

	let retrying = $state(false);
	async function retry() {
		retrying = true;
		try {
			await invalidateAll();
		} finally {
			retrying = false;
		}
	}

	// Cursors BEHIND this page, per filter set, per tab (lib/browsing.ts).
	const stackKey = $derived(deliveriesStackKey(c));
	let stack = $state<string[]>([]);
	$effect(() => {
		stack = readPageStack(stackKey);
	});
	function next() {
		const ahead = Array.from({ length: Math.max(stack.length, c.page + 1) }, (_, at) => stack[at] ?? '');
		[ahead[c.page - 1], ahead[c.page]] = [c.cursor, response?.nextPageToken ?? ''];
		writePageStack(stackKey, ahead);
		stack = ahead;
	}
	const walked = $derived(c.page > 1 && stack[c.page - 1] === c.cursor);
	const previousHref = $derived(
		walked ? deliveriesHref(c, { cursor: stack[c.page - 2] ?? '', page: c.page - 1 }, 'deliveries') : undefined
	);
	const nextHref = $derived(
		response?.nextPageToken ? deliveriesHref(c, { cursor: response.nextPageToken, page: c.page + 1 }, 'deliveries') : undefined
	);
	const firstHref = $derived(deliveriesHref(c, { cursor: '', page: 1 }, 'deliveries'));

	let expanded = $state<string[]>([]);
	const toggle = (id: string) => {
		expanded = expanded.includes(id) ? expanded.filter((value) => value !== id) : [...expanded, id];
	};

	let confirming = $state<Delivery | null>(null);
	let redelivering = $state(false);
	let redeliverError = $state('');
	let results = $state<Record<string, { text: string; delivered: boolean }>>({});
	async function redeliver() {
		if (!confirming) return;
		const original = confirming;
		redelivering = true;
		redeliverError = '';
		try {
			const out = await webhookClient(fetch).redeliverWebhook({ id: original.id });
			const rec = out.delivery;
			if (rec) results = { ...results, [original.id]: { text: redeliveryResult(rec), delivered: rec.delivered } };
			confirming = null;
			await invalidateAll();
		} catch (err) {
			redeliverError = errorMessage(err);
		} finally {
			redelivering = false;
		}
	}
	const outcomeClasses = (ok: boolean) => statusClasses(ok ? Status.SUCCEEDED : Status.FAILED);
</script>

<section id="deliveries" class="mt-8" data-testid="deliveries">
	<div class="mb-3 flex flex-wrap items-center justify-between gap-3">
		<h2 class="text-sm font-semibold text-fg">Deliveries</h2>
		<CliEquivalent testid="deliveries-cli" command={deliveriesCliCommand(c)} label="orkctl webhook deliveries for this listing" />
	</div>
	<p class="mb-4 text-sm text-fg-muted" data-testid="deliveries-scope">
		{DELIVERIES_CAPTION}
		{deliveriesNamespaceScope(c.filters)}{#if response},
			<span class="font-mono text-xs" data-testid="deliveries-window">{resolvedSince} to {resolvedUntil}</span>{/if}.
		The page's namespace selector does not filter this panel.
	</p>

	<ListFilter
		request={ListDeliveriesRequestSchema}
		applied={c.filters}
		testid="deliveries"
		command={['orkctl', 'webhook', 'deliveries']}
		onapply={applyFilters}
		{clearHref}
	/>

	{#if error}
		<ErrorPanel testid="deliveries-error" subject="the delivery records" message={error} busy={retrying} onretry={retry} />
	{:else if deliveries.length === 0}
		<p class="text-sm text-fg-muted" data-testid="deliveries-empty">
			{#if c.filters.length > 0 && c.page === 1}No deliveries match these filters.{:else}No deliveries in this window{c.page > 1 ? ' past the previous page' : ''}.{/if}
		</p>
		{#if c.page > 1}
			<Pagination testid="deliveries" noun="delivery record" page={c.page} count={0} {previousHref} {firstHref} />
		{/if}
	{:else}
		<div class="overflow-x-auto rounded-lg border border-border bg-surface shadow-xs">
			<table class="w-full min-w-[900px] text-left text-sm" data-testid="deliveries-table">
				<thead class="bg-surface-sunken text-xs text-fg-muted">
					<tr>
						<th class="p-3 font-medium">When</th>
						<th class="p-3 font-medium">Webhook</th>
						<th class="p-3 font-medium">Event</th>
						<th class="p-3 font-medium">Run</th>
						<th class="p-3 font-medium">Task</th>
						<th class="p-3 font-medium">Outcome</th>
						<th class="p-3 font-medium">Attempts</th>
						<th class="p-3 font-medium">Last</th>
					</tr>
				</thead>
				<tbody>
					{#each deliveries as d (d.id)}
						<tr
							id="delivery-{d.id}"
							class="cursor-pointer border-t border-border-subtle align-top hover:bg-surface-hover"
							data-testid="delivery-{d.id}"
							aria-expanded={expanded.includes(d.id)}
							onclick={() => toggle(d.id)}
						>
							<td class="p-3 text-xs whitespace-nowrap text-fg-secondary" title={instantMs(d.createdAt)}>{fmtTimestamp(d.createdAt)}</td>
							<td class="p-3 font-mono text-xs">{d.webhookId}</td>
							<td class="p-3 font-mono text-xs" data-testid="delivery-event-{d.id}">{eventName(d.event)}</td>
							<td class="p-3 font-mono text-xs">
								<a
									href="/runs/{encodeURIComponent(d.runId)}?ns={encodeURIComponent(d.namespace)}"
									class="text-brand-700 hover:underline"
									onclick={(event) => event.stopPropagation()}>{d.runId}</a
								>
							</td>
							<td class="p-3 font-mono text-xs">{d.taskId || '-'}</td>
							<td class="p-3">
								<span
									data-testid="delivery-outcome-{d.id}"
									class="inline-flex items-center rounded-full border px-2 py-0.5 text-[11px] font-medium leading-4 tracking-wide whitespace-nowrap {outcomeClasses(d.delivered)}"
									>{d.delivered ? 'delivered' : 'failed'}</span
								>
								{#if d.redeliveryOf}<span class="ml-1 text-[11px] text-fg-muted" data-testid="delivery-is-redelivery-{d.id}">redelivery</span>{/if}
							</td>
							<td class="p-3 text-xs tabular-nums" data-testid="delivery-attempt-count-{d.id}">{d.attempts.length}</td>
							<td class="p-3 font-mono text-xs" data-testid="delivery-last-{d.id}">{lastAttemptLabel(d)}</td>
						</tr>
						{#if expanded.includes(d.id)}
							<tr class="border-t border-border-subtle bg-surface-sunken" data-testid="delivery-detail-{d.id}">
								<td colspan="8" class="p-4">
									<dl class="grid gap-x-6 gap-y-1.5 text-xs sm:grid-cols-[auto_1fr]">
										<dt class="font-medium text-fg-muted">Delivery</dt>
										<dd class="font-mono break-all">{d.id}</dd>
										<dt class="font-medium text-fg-muted">URL</dt>
										<dd class="break-all">{d.url}</dd>
										<dt class="font-medium text-fg-muted">Signature</dt>
										<dd class="font-mono break-all" data-testid="delivery-signature-{d.id}">{d.signature || 'unsigned'}</dd>
										<dt class="font-medium text-fg-muted">Body</dt>
										<dd data-testid="delivery-body-{d.id}">
											sha256 <span class="font-mono break-all" data-testid="delivery-sha256-{d.id}">{d.bodySha256}</span>,
											{d.bodyBytes} bytes — body not stored; the run is readable on its page.
										</dd>
										{#if d.taskId}
											<dt class="font-medium text-fg-muted">Task</dt>
											<dd class="font-mono">{d.taskId}{d.attempt ? `, attempt ${d.attempt}` : ''}</dd>
										{/if}
										{#if d.redeliveryOf}
											<dt class="font-medium text-fg-muted">Redelivery of</dt>
											<dd>
												<a
													data-testid="delivery-redelivery-of-{d.id}"
													class="font-mono text-brand-700 hover:underline"
													href={deliveriesHref(c, { filters: runDeliveriesFilters(d.runId, d.webhookId), cursor: '', page: 1 }, `delivery-${d.redeliveryOf}`)}
													>{d.redeliveryOf}</a
												>
											</dd>
										{/if}
										<dt class="font-medium text-fg-muted">Event at / queued / settled</dt>
										<dd class="font-mono">{instantMs(d.eventAt)} / {instantMs(d.createdAt)} / {instantMs(d.settledAt)}</dd>
									</dl>
									<table class="mt-3 w-full text-left text-xs" data-testid="delivery-attempts-{d.id}">
										<thead class="text-fg-muted">
											<tr><th class="py-1 pr-3">#</th><th class="py-1 pr-3">At</th><th class="py-1 pr-3">Status</th><th class="py-1 pr-3">Duration</th><th class="py-1 pr-3">Backoff</th><th class="py-1">Error</th></tr>
										</thead>
										<tbody>
											{#each d.attempts as a (a.number)}
												<tr class="border-t border-border-subtle" data-testid="delivery-attempt-{d.id}-{a.number}">
													<td class="py-1 pr-3 tabular-nums">{a.number}</td>
													<td class="py-1 pr-3 font-mono">{instantMs(a.at)}</td>
													<td class="py-1 pr-3 font-mono">{a.statusCode || '-'}</td>
													<td class="py-1 pr-3 tabular-nums">{millis(a.duration)}</td>
													<td class="py-1 pr-3 tabular-nums">{a.backoff && (Number(a.backoff.seconds) || a.backoff.nanos) ? millis(a.backoff) : '-'}</td>
													<td class="py-1 break-all">{a.error || '-'}</td>
												</tr>
											{/each}
										</tbody>
									</table>
									<div class="mt-3 flex flex-wrap items-center gap-3">
										<button
											type="button"
											data-testid="redeliver-{d.id}"
											onclick={() => {
												confirming = d;
												redeliverError = '';
											}}
											class="inline-flex h-8 items-center rounded-md border border-border bg-surface px-3 text-xs font-medium text-fg shadow-xs hover:bg-surface-hover"
											>Redeliver</button
										>
										{#if results[d.id]}
											<span
												data-testid="redeliver-result-{d.id}"
												class="text-xs {results[d.id].delivered ? 'text-success-700' : 'text-danger-700'}">{results[d.id].text}</span
											>
										{/if}
									</div>
								</td>
							</tr>
						{/if}
					{/each}
				</tbody>
			</table>
		</div>
		<Pagination
			testid="deliveries"
			noun="delivery record"
			page={c.page}
			count={deliveries.length}
			{previousHref}
			{nextHref}
			{firstHref}
			onnext={next}
		/>
	{/if}
</section>

{#if confirming}
	<ConfirmDialog
		testid="redeliver-confirm"
		title="Redeliver {eventName(confirming.event)} for {confirming.runId}?"
		acceptLabel="Redeliver once"
		acceptTestid="redeliver-accept"
		cancelTestid="redeliver-cancel"
		busy={redelivering}
		error={redeliverError}
		onaccept={redeliver}
		oncancel={() => {
			confirming = null;
			redeliverError = '';
		}}
	>
		{#snippet body()}
			<p data-testid="redeliver-sentence">
				{redeliverySentence(webhooks.find((hook) => hook.id === confirming?.webhookId)?.url ?? confirming?.url ?? '')}
			</p>
		{/snippet}
	</ConfirmDialog>
{/if}
