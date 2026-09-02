<script lang="ts">
	// Execution retention (proto/orkstack/v1/service.proto's
	// GetExecutionRetention/UpdateExecutionRetention): view and change how
	// long Temporal keeps a finished workflow's history for THIS
	// DEPLOYMENT'S ONE Temporal cluster namespace.
	//
	// This is deliberately its own route, not a tab inside /admin/namespaces:
	// that page manages orkstack.v1.Namespace, the OrkStack registry entity
	// owning orkflow names, runs and pool defaults — a namespace this
	// deployment can have many of. This page is the opposite shape, a single
	// cluster-wide setting, and every label on it says so.
	import { invalidateAll } from '$app/navigation';
	import { navigating, page } from '$app/state';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import { errorMessage, orkstack } from '$lib/client';
	import { hoursToMs, parseRetentionHours } from '$lib/retentionForm';
	import { fmtDuration } from '$lib/stats';
	import { durationFromMs } from '@bufbuild/protobuf/wkt';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	let retrying = $state(false);
	async function retry() {
		retrying = true;
		try {
			await invalidateAll();
		} finally {
			retrying = false;
		}
	}

	// navigatingHere covers a re-entry into this same URL a delayed RPC
	// could stall on, the same shape every other Admin page's Retry uses.
	const navigatingHere = $derived(
		navigating.to !== null && navigating.to.url.pathname === page.url.pathname
	);
	const busy = $derived(retrying || navigatingHere);

	// ── change the retention ────────────────────────────────────────────
	// The form asks for hours rather than replicating Go's
	// time.ParseDuration syntax (what `orkctl admin retention set` accepts)
	// in the browser: a plain, validated number is less to get wrong here,
	// and the RPC carries a google.protobuf.Duration either way.
	let hours = $state('');
	let setBusy = $state(false);
	let setError = $state('');
	let setSuccess = $state('');

	function resetForm() {
		hours = '';
	}

	async function submitSet() {
		setError = '';
		setSuccess = '';
		const result = parseRetentionHours(hours);
		if ('error' in result) {
			setError = result.error;
			return;
		}

		setBusy = true;
		try {
			const res = await orkstack(fetch).system.updateExecutionRetention({
				retention: durationFromMs(hoursToMs(result.hours))
			});
			setSuccess = `Set to ${fmtDuration(res.retention)} on Temporal namespace ${res.temporalNamespace}.`;
			resetForm();
			await invalidateAll();
		} catch (err) {
			// A non-positive duration never reaches here (refused above), so
			// whatever comes back is the CLUSTER's own refusal — for example
			// Temporal's server-side minimum-retention rejection — shown
			// verbatim, never translated into a generic message that would
			// hide what an operator needs to change.
			setError = errorMessage(err);
		} finally {
			setBusy = false;
		}
	}
</script>

<svelte:head><title>Admin — Execution retention — OrkStack</title></svelte:head>

<p class="mb-4 text-sm text-zinc-500" data-testid="retention-scope-caption">
	This is ONE Temporal cluster setting for the whole deployment, not scoped by the namespace
	selector above and not a property of an orkstack.v1.Namespace — see
	<code class="rounded bg-zinc-800 px-1 py-0.5 text-xs text-zinc-300">/admin/namespaces</code> for
	that separate, per-tenant concept.
</p>

{#if data.retentionError}
	<ErrorPanel
		testid="retention-error"
		retryTestid="retention-retry"
		subject="this deployment's execution retention"
		message={data.retentionError}
		busy={retrying}
		onretry={retry}
	/>
{:else if busy}
	<p class="text-sm text-zinc-400" data-testid="retention-loading">Loading execution retention…</p>
{:else if !data.retention}
	<p class="text-sm text-zinc-400" data-testid="retention-empty">
		The engine returned no execution retention.
	</p>
{:else}
	<section class="mb-8" data-testid="retention-info">
		<div class="overflow-hidden rounded-lg border border-zinc-800">
			<dl class="divide-y divide-zinc-800 text-sm">
				<div class="grid grid-cols-[10rem_1fr] px-4 py-2.5">
					<dt class="text-zinc-400">Temporal namespace</dt>
					<dd class="text-zinc-100" data-testid="retention-temporal-namespace">
						{data.retention.temporalNamespace || '—'}
					</dd>
				</div>
				<div class="grid grid-cols-[10rem_1fr] px-4 py-2.5">
					<dt class="text-zinc-400">Execution retention</dt>
					<dd class="text-zinc-100" data-testid="retention-current">
						{fmtDuration(data.retention.retention)}
					</dd>
				</div>
			</dl>
		</div>
		<p class="mt-2 text-xs text-zinc-500">
			Read live from the cluster (Temporal's DescribeNamespace) — see
			<code class="text-zinc-400">orkctl admin retention get</code>.
		</p>
	</section>

	<section class="rounded-lg border border-zinc-800 p-4">
		<h2 class="mb-3 text-sm font-semibold text-zinc-200">Change execution retention</h2>
		<form
			class="flex flex-wrap items-end gap-3"
			onsubmit={(e) => {
				e.preventDefault();
				void submitSet();
			}}
		>
			<label class="flex flex-col gap-1 text-sm">
				<span class="text-zinc-400">New retention (hours)</span>
				<input
					data-testid="retention-set-hours"
					bind:value={hours}
					inputmode="decimal"
					placeholder="168"
					class="w-32 rounded-md border border-zinc-700 bg-zinc-900 px-2 py-1.5 text-zinc-100"
				/>
			</label>
			<button
				type="submit"
				data-testid="retention-set-submit"
				disabled={setBusy}
				class="rounded-md bg-emerald-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-emerald-500 disabled:opacity-50"
			>
				{setBusy ? 'Setting…' : 'Set retention'}
			</button>
		</form>
		{#if setError}
			<p
				class="mt-3 rounded-md border border-rose-500/40 bg-rose-500/10 px-3 py-2 text-sm text-rose-300"
				data-testid="retention-set-error"
			>
				{setError}
			</p>
		{/if}
		{#if setSuccess}
			<p class="mt-3 text-sm text-emerald-300" data-testid="retention-set-success">
				{setSuccess}
			</p>
		{/if}
		<p class="mt-3 text-xs text-zinc-500">
			This changes Temporal's history retention for the WHOLE deployment's one cluster
			namespace, taking effect for every OrkStack namespace this deployment has.
		</p>
	</section>
{/if}
