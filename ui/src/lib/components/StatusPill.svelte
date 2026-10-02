<script lang="ts">
	import { Status } from '$lib/gen/orkstack/v1/common_pb';
	import { statusClasses, statusLabel } from '$lib/status';

	let { status, testid }: { status: Status | undefined; testid?: string } = $props();
</script>

<span
	data-testid={testid}
	class="inline-flex items-center gap-1.5 rounded-full border px-2 py-0.5 text-[11px] font-medium tracking-wide whitespace-nowrap uppercase leading-4 {statusClasses(
		status
	)}"
>
	{#if status === Status.RUNNING}
		<span
			class="size-1.5 rounded-full bg-running-solid animate-pulse-dot motion-reduce:animate-none"
			aria-hidden="true"
		></span>
	{:else if status === Status.AWAITING}
		<!-- Static, no pulse: an awaiting task is waiting, not working. -->
		<span class="size-1.5 rounded-full bg-awaiting-solid" aria-hidden="true"></span>
	{/if}
	{statusLabel(status)}
</span>
