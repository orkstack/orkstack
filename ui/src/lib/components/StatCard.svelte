<script lang="ts">
	import { UNAVAILABLE } from '$lib/stats';

	// One number on the dashboard, with the one rule the dashboard exists to
	// keep: a number that could not be answered says so. `value` is the
	// rendered string, and when it is UNAVAILABLE the card renders it in the
	// muted "no answer" face — never as a figure a reader would act on.

	interface Props {
		label: string;
		value: string;
		note?: string;
		testid: string;
		tone?: 'plain' | 'good' | 'bad' | 'busy';
	}

	let { label, value, note = '', testid, tone = 'plain' }: Props = $props();

	const missing = $derived(value === UNAVAILABLE);
	const valueClass = $derived(
		missing
			? 'font-normal italic text-fg-muted'
			: tone === 'good'
				? 'text-succeeded-fg'
				: tone === 'bad'
					? 'text-failed-fg'
					: tone === 'busy'
						? 'text-running-fg'
						: 'text-fg'
	);
</script>

<div class="rounded-lg border border-border bg-surface p-4 shadow-xs">
	<div class="text-xs font-medium text-fg-muted">{label}</div>
	<div
		data-testid={testid}
		data-unavailable={missing ? 'true' : 'false'}
		class="mt-1.5 text-2xl font-semibold tracking-tight tabular-nums {valueClass}"
	>
		{value}
	</div>
	{#if note}
		<div class="mt-1 text-xs text-fg-muted" data-testid="{testid}-note">{note}</div>
	{/if}
</div>
