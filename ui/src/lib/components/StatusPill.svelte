<script lang="ts">
	import type { Status } from '$lib/gen/orkstack/v1/common_pb';
	import { statusClasses, statusLabel } from '$lib/status';

	let { status, testid }: { status: Status | undefined; testid?: string } = $props();
</script>

<span
	data-testid={testid}
	class="inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-semibold tracking-wide ring-1 {statusClasses(
		status
	)}"
>
	{statusLabel(status)}
</span>
