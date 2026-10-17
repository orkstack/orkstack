<script lang="ts">
	// A status, stamped: a 1 px frame, tracked caps, and the status's drawn
	// mark before its word ($lib/statusMarks.ts). The mark is CSS on the stamp
	// itself — RUNNING and AWAITING alone draw theirs as one inner span — so a
	// run page of thousands of stamps adds no element per stamp. The text is
	// exactly the status word and nothing else: lists read it as the cell's
	// whole text.
	import { Status } from '$lib/gen/orkstack/v1/common_pb';
	import { statusClasses, statusLabel } from '$lib/status';
	import { statusMark } from '$lib/statusMarks';

	let { status, testid }: { status: Status | undefined; testid?: string } = $props();

	const mark = $derived(statusMark(status));
</script>

<span data-testid={testid} class="stamp {statusClasses(status)} {mark.inner ? '' : mark.className}"
	>{#if mark.inner}<span
			class="{mark.className} {status === Status.RUNNING ? 'animate-live-dot motion-reduce:animate-none' : ''}"
			aria-hidden="true"
		></span>{/if}{statusLabel(status)}</span
>
