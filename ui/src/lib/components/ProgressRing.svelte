<script lang="ts">
	// A run's progress: a ring and its percentage, the counts in the tooltip
	// and in screen-reader text (#lib/progress.ts gives the basis). The ring's
	// colour carries the status: blue while Running, green once Succeeded, red
	// for Failed, grey for Canceled, so a failed run at 100% never reads as a
	// success — and the status pill beside it says so in words.
	import { Status } from '#lib/gen/orkstack/v1/common_pb.ts';
	import { progressWords, type Progress } from '#lib/progress.ts';

	let { progress, status, small = false, testid }: { progress: Progress; status?: Status; small?: boolean; testid?: string } = $props();

	const C = 2 * Math.PI * 8;
	const words = $derived(progressWords(progress));
</script>

<!-- A tab stop on purpose: the counts in its tip must reach a keyboard (WCAG 1.4.13). -->
<!-- svelte-ignore a11y_no_noninteractive_tabindex -->
<span
	tabindex="0"
	class="pring"
	class:pring-sm={small}
	class:is-running={status === Status.RUNNING}
	class:is-succeeded={status === Status.SUCCEEDED}
	class:is-failed={status === Status.FAILED}
	class:is-canceled={status === Status.CANCELED}
	data-tip={words}
	data-testid={testid}
	><svg viewBox="0 0 20 20" aria-hidden="true"
		><circle class="trk" cx="10" cy="10" r="8" /><circle
			class="val"
			cx="10"
			cy="10"
			r="8"
			stroke-dasharray="{((C * progress.percent) / 100).toFixed(2)} {C.toFixed(2)}"
			transform="rotate(-90 10 10)"
		/></svg
	><b>{progress.percent}%</b><span class="sr"> done, {words}</span></span
>
