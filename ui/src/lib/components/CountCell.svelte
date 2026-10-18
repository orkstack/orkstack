<script lang="ts">
	// One cell of a count band: a tracked-caps label over a figure, the way a
	// drawing's title block states a fact — never a hero number. The figure
	// is ink at the title block's value size at most; a status count carries
	// its status's drawn mark beside its label, the channel a stamp uses,
	// rather than a coloured number.
	//
	// The one rule the band exists to keep: a number that could not be
	// answered says so. When `value` is UNAVAILABLE the figure is set in the
	// muted "no answer" face, never as a figure a reader would act on.
	import type { Status } from '$lib/gen/orkstack/v1/common_pb';
	import { UNAVAILABLE } from '$lib/stats';
	import { statusMark } from '$lib/statusMarks';

	interface Props {
		label: string;
		value: string;
		/** A sub-line under the figure; it wraps rather than hides. */
		note?: string;
		/** The longer account a pointer reveals; the note when absent. */
		explain?: string;
		testid: string;
		/** A count of runs in one status: its mark is drawn beside the label. */
		status?: Status;
		/** Where the band places it: the rule a wrapped line starts with. */
		class?: string;
	}

	let { label, value, note = '', explain = '', testid, status = undefined, class: place = '' }: Props = $props();

	const missing = $derived(value === UNAVAILABLE);
	const mark = $derived(status === undefined ? null : statusMark(status));
</script>

<div class="min-w-0 border-l border-rule px-1.5 py-1 first:border-l-0 first:pl-0 sm:px-3 sm:first:pl-0 {place}" title={explain || note || undefined}>
	<div class="caps flex items-center gap-1 text-ink-muted sm:gap-1.5 max-sm:tracking-normal {mark && !mark.inner ? mark.className : ''}">
		{#if mark?.inner}<span class={mark.className} aria-hidden="true"></span>{/if}<span class="min-w-0 wrap-anywhere">{label}</span>
	</div>
	<div
		data-testid={testid}
		data-unavailable={missing ? 'true' : 'false'}
		class="figure truncate text-lg font-semibold {missing ? 'font-normal text-ink-muted italic' : 'text-ink'}"
	>
		{value}
	</div>
	{#if note}
		<div class="text-2xs text-balance text-ink-muted" data-testid="{testid}-note">{note}</div>
	{/if}
</div>
