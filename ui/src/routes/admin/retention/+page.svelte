<script lang="ts">
	// Maximum run retention (proto/orkstack/v1/service.proto's
	// GetRetentionPolicy/UpdateRetentionPolicy): view and change the ceiling
	// on how long any namespace may keep a finished run, and what a
	// namespace that sets none inherits.
	//
	// This is deliberately its own route, not a tab inside /admin/namespaces:
	// that page manages each orkstack.v1.Namespace's OWN run retention — a
	// value this page only bounds, never sets directly. This page is the
	// single deployment-wide ceiling, and every label on it says so.
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

	// ── change the maximum ──────────────────────────────────────────────
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
			const res = await orkstack(fetch).system.updateRetentionPolicy({
				maxRunRetention: durationFromMs(hoursToMs(result.hours))
			});
			setSuccess = `Maximum run retention is now ${fmtDuration(res.policy?.maxRunRetention)}.`;
			resetForm();
			await invalidateAll();
		} catch (err) {
			// A non-positive duration never reaches here (refused above), so
			// whatever comes back is a genuine refusal — for example a
			// namespace whose own run retention is above the requested
			// maximum — shown verbatim, never translated into a generic
			// message that would hide what an operator needs to change.
			setError = errorMessage(err);
		} finally {
			setBusy = false;
		}
	}
</script>

<svelte:head><title>Admin — Maximum run retention — OrkStack</title></svelte:head>

<p class="mb-4 text-sm text-fg-muted" data-testid="retention-scope-caption">
	The maximum any namespace may keep a finished run, and what a namespace that sets none
	inherits. Deployment-wide: the namespace selector above does not scope it. Per-namespace values
	are set on <code>/admin/namespaces</code>.
</p>

{#if data.retentionError}
	<ErrorPanel
		testid="retention-error"
		retryTestid="retention-retry"
		subject="this deployment's maximum run retention"
		message={data.retentionError}
		busy={retrying}
		onretry={retry}
	/>
{:else if busy}
	<p class="text-sm text-fg-muted" data-testid="retention-loading">
		Loading maximum run retention…
	</p>
{:else if !data.retention}
	<p class="text-sm text-fg-muted" data-testid="retention-empty">
		The engine returned no retention policy.
	</p>
{:else}
	<section class="mb-8" data-testid="retention-info">
		<div class="overflow-hidden rounded-lg border border-border bg-surface shadow-xs">
			<dl class="divide-y divide-border-subtle text-sm">
				<div class="grid grid-cols-[10rem_1fr] px-4 py-2.5">
					<dt class="text-fg-muted">Maximum run retention</dt>
					<dd class="text-fg" data-testid="retention-current">
						{fmtDuration(data.retention.policy?.maxRunRetention)}
					</dd>
				</div>
				<div class="grid grid-cols-[10rem_1fr] px-4 py-2.5">
					<dt class="text-fg-muted">Enforcement</dt>
					<dd class="text-fg" data-testid="retention-enforcement">
						{#if data.retention.namespaceRetentionEnforced}
							Enforced per namespace
						{:else}
							Not enforced — every run is kept for the maximum plus the enforcement margin
							(ORK_CLEANUP_ENABLED is off)
						{/if}
					</dd>
				</div>
			</dl>
		</div>
		<p class="mt-2 text-xs text-fg-muted">
			Stored with the deployment, so every replica answers the same — see
			<code>orkctl admin retention get</code>.
		</p>
	</section>

	<section class="rounded-lg border border-border bg-surface p-5 shadow-xs">
		<h2 class="mb-3 text-sm font-semibold text-fg">Change the maximum</h2>
		<form
			class="flex flex-wrap items-end gap-3"
			onsubmit={(e) => {
				e.preventDefault();
				void submitSet();
			}}
		>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="font-medium text-fg">New maximum (hours)</span>
				<input
					data-testid="retention-set-hours"
					bind:value={hours}
					inputmode="decimal"
					placeholder="168"
					class="h-9 w-32 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
				/>
			</label>
			<button
				type="submit"
				data-testid="retention-set-submit"
				disabled={setBusy}
				class="inline-flex h-9 items-center justify-center gap-1.5 rounded-md bg-brand-600 px-3.5 text-sm font-medium text-white shadow-xs transition-colors duration-150 hover:bg-brand-700 active:bg-brand-800 disabled:pointer-events-none disabled:opacity-50"
			>
				{setBusy ? 'Setting…' : 'Set retention'}
			</button>
		</form>
		{#if setError}
			<p
				class="mt-3 rounded-md border border-danger-200 bg-danger-50 px-3 py-2 text-sm text-danger-700"
				data-testid="retention-set-error"
			>
				{setError}
			</p>
		{/if}
		{#if setSuccess}
			<p class="mt-3 text-sm text-succeeded-fg" data-testid="retention-set-success">
				{setSuccess}
			</p>
		{/if}
		<p class="mt-3 text-xs text-fg-muted">
			Namespaces that set no run retention follow the new maximum at once; a namespace whose own
			value is above it must be lowered first.
		</p>
	</section>
{/if}
