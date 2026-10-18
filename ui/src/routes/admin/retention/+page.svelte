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
	import { untrack } from 'svelte';
	import { invalidateAll } from '$app/navigation';
	import { navigating, page } from '$app/state';
	import ConfirmDialog from '$lib/components/ConfirmDialog.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import ScopeNote from '$lib/components/ScopeNote.svelte';
	import SheetHeader from '$lib/components/SheetHeader.svelte';
	import { errorMessage } from '$lib/client';
	import { systemClient } from '$lib/clients/system';
	import {
		confirmMatches,
		hoursToMs,
		isLowering,
		lowerConsequence,
		parseRetentionHours,
		retentionHours
	} from '$lib/retentionForm';
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
	//
	// The field starts at the maximum in force, and is reset to it whenever
	// the page re-reads it. LOWERING the maximum retires stored runs on the
	// next cleanup pass, so a lower value opens a destructive confirmation
	// that names both values and what is reclaimed, and Accept waits for the
	// new value to be typed; raising it is applied at once.
	const current = $derived(data.retention?.policy?.maxRunRetention);
	let hours = $state(retentionHours(untrack(() => current)));
	$effect(() => {
		hours = retentionHours(current);
	});
	let setBusy = $state(false);
	let setError = $state('');
	let setSuccess = $state('');
	// The lower value awaiting confirmation, and what has been typed for it.
	let pendingLower = $state<number | null>(null);
	let confirmTyped = $state('');

	async function submitSet() {
		setError = '';
		setSuccess = '';
		const result = parseRetentionHours(hours);
		if ('error' in result) {
			setError = result.error;
			return;
		}
		if (isLowering(current, result.hours)) {
			pendingLower = result.hours;
			confirmTyped = '';
			return;
		}
		await applySet(result.hours);
	}

	async function applySet(next: number) {
		setBusy = true;
		try {
			const res = await systemClient(fetch).updateRetentionPolicy({
				maxRunRetention: durationFromMs(hoursToMs(next))
			});
			setSuccess = `Maximum run retention is now ${fmtDuration(res.policy?.maxRunRetention)}.`;
			pendingLower = null;
			await invalidateAll();
		} catch (err) {
			// A non-positive duration never reaches here (refused above), so
			// whatever comes back is a genuine refusal — for example a
			// namespace whose own run retention is above the requested
			// maximum — shown verbatim, never translated into a generic
			// message that would hide what an operator needs to change.
			setError = errorMessage(err);
			pendingLower = null;
		} finally {
			setBusy = false;
		}
	}
</script>

<svelte:head><title>Admin — Maximum run retention — OrkStack</title></svelte:head>

<SheetHeader title="Maximum run retention" />

<ScopeNote testid="retention-scope-caption">
	The maximum any namespace may keep a finished run, and what a namespace that sets none
	inherits. Deployment-wide: the namespace selector above does not scope it. Per-namespace values
	are set on <code>/admin/namespaces</code>.
</ScopeNote>

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
	<p class="text-sm text-ink-muted" data-testid="retention-loading">
		Loading maximum run retention…
	</p>
{:else if !data.retention}
	<p class="text-sm text-ink-muted" data-testid="retention-empty">
		The engine returned no retention policy.
	</p>
{:else}
	<section class="mb-10" data-testid="retention-info">
		<!-- The policy in the title block's anatomy: caps label over value. -->
		<dl class="grid border-t-(length:--rule-frame) border-b border-ink sm:grid-cols-[minmax(14rem,1fr)_3fr]">
				<div class="border-b border-rule px-3 py-2 sm:border-r sm:border-b-0">
					<dt class="caps text-ink-muted">Maximum run retention</dt>
					<dd class="figure mt-0.5 text-lg font-semibold text-ink" data-testid="retention-current">
						{fmtDuration(data.retention.policy?.maxRunRetention)}
					</dd>
				</div>
				<div class="px-3 py-2">
					<dt class="caps text-ink-muted">Enforcement</dt>
					<dd class="mt-1 text-sm text-ink" data-testid="retention-enforcement">
						{#if data.retention.namespaceRetentionEnforced}
							Enforced per namespace
						{:else}
							Not enforced — every run is kept for the maximum plus the enforcement margin
							(ORK_CLEANUP_ENABLED is off)
						{/if}
					</dd>
				</div>
		</dl>
		<p class="mt-2 text-xs text-ink-muted">
			Stored with the deployment, so every replica answers the same — see
			<code>orkctl admin retention get</code>.
		</p>
	</section>

	<section class="region pt-3">
		<h2 class="mb-3 text-md font-semibold text-ink">Change the maximum</h2>
		<form
			class="flex flex-wrap items-end gap-3"
			onsubmit={(e) => {
				e.preventDefault();
				void submitSet();
			}}
		>
			<label class="text-sm">
				<span class="field-label">New maximum (hours)</span>
				<input
					data-testid="retention-set-hours"
					bind:value={hours}
					inputmode="decimal"
					placeholder="168"
					class="field figure h-9 w-32 px-3"
				/>
			</label>
			<button
				type="submit"
				data-testid="retention-set-submit"
				disabled={setBusy}
				class="btn btn-primary"
			>
				{setBusy ? 'Setting…' : 'Set retention'}
			</button>
		</form>
		{#if setError}
			<p
				class="mt-3 max-w-prose border border-red bg-red-tint px-3 py-2 text-sm text-red-strong"
				role="alert"
				data-testid="retention-set-error"
			>
				{setError}
			</p>
		{/if}
		{#if setSuccess}
			<p class="confirm mt-3 text-sm text-ink" role="status" data-testid="retention-set-success">
				{setSuccess}
			</p>
		{/if}
		<p class="mt-3 max-w-prose text-xs text-ink-muted">
			Namespaces that set no run retention follow the new maximum at once; a namespace whose own
			value is above it must be lowered first. Lowering it asks for confirmation.
		</p>
	</section>
{/if}

{#if pendingLower !== null}
	{@const next = pendingLower}
	<ConfirmDialog
		testid="retention-lower-confirm"
		title="Lower the maximum run retention to {next}h?"
		acceptLabel="Lower to {next}h"
		acceptTestid="retention-lower-accept"
		cancelTestid="retention-lower-cancel"
		busy={setBusy}
		error={setError}
		destructive
		acceptDisabled={!confirmMatches(confirmTyped, next)}
		onaccept={() => void applySet(next)}
		oncancel={() => {
			pendingLower = null;
			confirmTyped = '';
		}}
	>
		{#snippet body()}
			<p data-testid="retention-lower-consequence">{lowerConsequence(fmtDuration(current), next)}</p>
			<label class="mt-3 flex flex-col gap-1.5 text-sm">
				<span class="font-medium text-ink">Type <code>{next}</code> to confirm</span>
				<input
					data-testid="retention-lower-typed"
					bind:value={confirmTyped}
					inputmode="decimal"
					autocomplete="off"
					class="field figure h-9 w-32 px-3"
				/>
			</label>
		{/snippet}
	</ConfirmDialog>
{/if}
