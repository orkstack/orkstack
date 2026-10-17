<script lang="ts">
	// A status, stamped: a 1 px frame, tracked caps, and the status's drawn
	// mark before its word ($lib/statusMarks.ts). The mark is CSS on the stamp
	// itself — RUNNING and AWAITING alone draw theirs as one inner span — so a
	// run page of thousands of stamps adds no element per stamp, and no
	// reactive node beyond its props. The text is exactly the status word and
	// nothing else: lists read it as the cell's whole text.
	import { Status } from '$lib/gen/orkstack/v1/common_pb';
	import { statusLabel } from '$lib/status';
	import { stampClass } from '$lib/statusMarks';

	let { status, testid }: { status: Status | undefined; testid?: string } = $props();
</script>

<span data-testid={testid} class={stampClass(status)}
	>{#if status === Status.RUNNING}<span class="mark-dot animate-live-dot motion-reduce:animate-none" aria-hidden="true"
		></span>{:else if status === Status.AWAITING}<span class="mark-ring" aria-hidden="true"></span>{/if}{statusLabel(
		status
	)}</span
>
