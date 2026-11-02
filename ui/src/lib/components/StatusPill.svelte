<script lang="ts">
	// A status pill: the status's drawn mark, its word, then its hue
	// (#lib/statusMarks.ts). The mark is CSS on the pill itself — Running and
	// Awaiting alone draw theirs as one inner span — so a run page of
	// thousands of pills adds no element per pill, and no reactive node beyond
	// its props. The text is exactly the status word and nothing else: lists
	// read it as the cell's whole text.
	import { Status } from '#lib/gen/orkstack/v1/common_pb.ts';
	import { statusLabel } from '#lib/status.ts';
	import { stampClass } from '#lib/statusMarks.ts';

	let { status, testid, small = false }: { status: Status | undefined; testid?: string; small?: boolean } = $props();
</script>

<span data-testid={testid} class="{stampClass(status)}{small ? ' stamp-sm' : ''}"
	>{#if status === Status.RUNNING}<span class="mark-dot" aria-hidden="true"
		></span>{:else if status === Status.AWAITING}<span class="mark-diamond" aria-hidden="true"></span>{/if}{statusLabel(
		status
	)}</span
>
