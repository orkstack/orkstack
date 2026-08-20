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
			? 'text-zinc-500 italic'
			: tone === 'good'
				? 'text-emerald-300'
				: tone === 'bad'
					? 'text-rose-300'
					: tone === 'busy'
						? 'text-sky-300'
						: 'text-zinc-100'
	);
</script>

<div class="rounded-lg border border-zinc-800 bg-zinc-900/40 p-4">
	<div class="text-xs uppercase tracking-wide text-zinc-500">{label}</div>
	<div
		data-testid={testid}
		data-unavailable={missing ? 'true' : 'false'}
		class="mt-1 text-2xl font-semibold {valueClass}"
	>
		{value}
	</div>
	{#if note}
		<div class="mt-1 text-xs text-zinc-500" data-testid="{testid}-note">{note}</div>
	{/if}
</div>
