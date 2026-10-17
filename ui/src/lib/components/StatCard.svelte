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
	// One alarm colour in this world: a bad figure is red ink, a busy one
	// drafting blue, every other figure plain ink. A good figure needs no
	// colour to be good.
	const valueClass = $derived(
		missing
			? 'font-normal italic text-ink-muted'
			: tone === 'bad'
				? 'text-red'
				: tone === 'busy'
					? 'text-blue'
					: 'text-ink'
	);
</script>

<div class="border-l border-rule px-4 py-2 first:border-l-0 first:pl-0">
	<div class="caps text-ink-muted">{label}</div>
	<div
		data-testid={testid}
		data-unavailable={missing ? 'true' : 'false'}
		class="figure mt-1 text-lg font-semibold {valueClass}"
	>
		{value}
	</div>
	{#if note}
		<div class="mt-0.5 text-xs text-ink-muted" data-testid="{testid}-note">{note}</div>
	{/if}
</div>
