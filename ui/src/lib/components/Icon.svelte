<script lang="ts">
	// One icon of the Harbour set (#lib/icons.ts), drawn in currentColor and
	// hidden from assistive technology: the control it sits in carries the
	// name. It points at the symbol IconSprite drew once for the whole app.
	import type { IconName } from '#lib/icons.ts';

	let { name, class: className = 'size-4' }: { name: IconName; class?: string } = $props();
</script>

<svg viewBox="0 0 16 16" aria-hidden="true" focusable="false" class="shrink-0 {className}"><use href="#i-{name}" /></svg>
